"""An in-memory `JobQueue`, and the reference semantics for the Protocol.

Here rather than in `core/` for the same reason `MockChatRepository` is: nothing
that ships imports it. It is more load-bearing than that module, though — phase 2
runs one contract suite against three implementations, and this is the one that
always runs, so it is where the contract's *intended* answer is written down.

What it cannot be faithful about is the thing that makes those answers true
elsewhere: a single-threaded dict has no races to lose, so `SKIP LOCKED` under a
genuine concurrent claim and RabbitMQ's prefetch window are covered by the
marked sections of `tests/test_job_queue.py`, not here. A passing memory run
proves the semantics are coherent, not that Postgres implements them.
"""

import asyncio
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from core.jobs import Job, JobRequest, JobState, Lease, StaleLease

# How long `reserve` sleeps between scans while waiting. Small, because this
# implementation's whole purpose is tests: a slow tick would add latency to
# every lease-expiry assertion in the suite.
_TICK = timedelta(milliseconds=5)


@dataclass
class _Row:
    """Mutable job state. The `jobs` table of `docs/jobs.md`, in a dict."""

    job: Job
    run_after: datetime
    created_at: datetime
    lease_id: uuid.UUID | None = None
    lease_expires_at: datetime | None = None
    lease_owner: str | None = None


def _now() -> datetime:
    return datetime.now(UTC)


class MemoryJobQueue:
    """`JobQueue` in dictionaries, for a suite that must not need containers.

    Leases are checked against real elapsed time rather than an injected clock,
    which is deliberate: the same suite runs against Postgres, where `now()` is
    the database's and cannot be advanced by a test. Uniform semantics are worth
    more than fast expiry tests, so the suite uses short real leases.
    """

    def __init__(self) -> None:
        self._rows: dict[uuid.UUID, _Row] = {}
        self._keys: dict[tuple[uuid.UUID | None, str], uuid.UUID] = {}

        # Every lease handed back by `discard`, in order. Recorded rather than
        # ignored for the same reason `MemoryObjectStore.written` exists: the
        # call has no observable effect here, and a claim about a caller that
        # cannot be observed is a claim nothing holds to.
        self.discarded: list[uuid.UUID] = []

    # ----------------------------------------------------------------------
    # Producing
    # ----------------------------------------------------------------------

    async def enqueue(self, request: JobRequest) -> Job:
        key = (request.session_id, request.idempotency_key)
        known = self._keys.get(key)
        if known is not None:
            # Check-then-insert, which the real implementations must not do —
            # they get the same answer from a unique index. Safe here only
            # because nothing can run between these two statements.
            return self._rows[known].job

        now = _now()
        job = Job(
            id=uuid.uuid4(),
            kind=request.kind,
            payload=dict(request.payload),
            state=JobState.QUEUED,
            attempts=0,
            max_attempts=request.max_attempts,
            session_id=request.session_id,
            idempotency_key=request.idempotency_key,
        )
        self._rows[job.id] = _Row(job=job, run_after=now + request.delay, created_at=now)
        self._keys[key] = job.id
        return job

    async def get(self, job_id: uuid.UUID) -> Job | None:
        row = self._rows.get(job_id)
        return row.job if row else None

    async def cancel(self, job_id: uuid.UUID) -> Job | None:
        row = self._rows.get(job_id)
        if row is None or row.job.state.terminal:
            # Terminal is a no-op rather than an error: cancelling twice across
            # a reconnect has to give the same answer both times.
            return row.job if row else None

        if row.job.state is JobState.RUNNING:
            # Only the asking half. Nothing here can stop a consumer that is
            # busy; it sees the flag on its next `extend` and releases the job.
            row.job = replace(row.job, cancel_requested=True)
        else:
            row.job = _replace_state(row.job, JobState.CANCELLED, cancel_requested=True)
        return row.job

    # ----------------------------------------------------------------------
    # Consuming
    # ----------------------------------------------------------------------

    async def reserve(self, *, owner: str, lease: timedelta, wait_for: timedelta) -> Lease | None:
        deadline = _now() + wait_for
        while True:
            claimed = self._claim(owner, lease)
            if claimed is not None:
                return claimed
            # Checked after the scan, not before, so `wait_for=0` still gets one
            # look at the queue. A zero-timeout reserve that never inspects
            # anything would be a surprising way to spell "non-blocking".
            if _now() >= deadline:
                return None
            await asyncio.sleep(_TICK.total_seconds())

    def _claim(self, owner: str, lease: timedelta) -> Lease | None:
        now = _now()
        self._retire_lapsed(now)
        candidates = [row for row in self._rows.values() if self._reservable(row, now)]
        if not candidates:
            return None

        # The claim query's `order by run_after, created_at`. Ties break on
        # insertion order, which is what the real index gives for rows created
        # in the same transaction-clock tick.
        row = min(candidates, key=lambda r: (r.run_after, r.created_at))

        row.job = _replace_state(row.job, JobState.RUNNING, attempts=row.job.attempts + 1)
        row.lease_id = uuid.uuid4()
        row.lease_expires_at = now + lease
        row.lease_owner = owner
        return Lease(id=row.lease_id, job=row.job, owner=owner, expires_at=row.lease_expires_at)

    def _retire_lapsed(self, now: datetime) -> None:
        """Finish jobs whose consumer died and is never coming back.

        Two kinds, both recognised by a lapsed lease on a `RUNNING` row:

        - **Out of attempts** goes to `DEAD`. A job that reliably kills whatever
          picks it up is exactly what a delivery limit exists to stop.
        - **Cancel requested** goes to `CANCELLED`, however many attempts remain.
          `nack` already rules that a cancelled job is never reported as a
          failure, and that rule cannot depend on how many attempts happened to
          be left when the consumer died. Redelivering it instead would claim a
          consumer, extend once, discover the flag and `nack` — a whole round
          trip, and a burned attempt, to arrive at the same state.

        `nack` cannot do either job: it is the *crashing* consumer that never
        calls it. Without this the row sits at `RUNNING` forever — no terminal
        state, nothing to tell a client polling `get`, and a state a reader
        cannot distinguish from healthy, which is the same failure the
        `streaming` row was in phase 1b. Excluding it from `_reservable` is not
        enough on its own: invisible is not the same as finished.

        Done on the claim path rather than in a sweeper for the same reason
        expiry is a predicate: the only thing that has to notice is already
        looking, so there is no background process whose failure would be
        silent.
        """
        for row in self._rows.values():
            if row.job.state is not JobState.RUNNING:
                continue
            if row.lease_expires_at is None or row.lease_expires_at > now:
                continue
            if row.job.cancel_requested:
                # No error, and not the one below: the user asked for this and
                # already knows what happened.
                row.job = _replace_state(row.job, JobState.CANCELLED)
            elif row.job.attempts >= row.job.max_attempts:
                row.job = _replace_state(
                    row.job,
                    JobState.DEAD,
                    error=row.job.error or "lease expired with no attempts remaining",
                )
            else:
                continue
            self._release(row)

    @staticmethod
    def _reservable(row: _Row, now: datetime) -> bool:
        """Expiry as a predicate, not a sweeper — `docs/jobs.md`.

        A running job whose lease has lapsed is reservable on the spot, so there
        is no window between expiry and recovery and no background process whose
        failure would be invisible.

        The last two clauses restate what `_retire_lapsed` has already acted on
        by the time this runs — a lapsed lease that is out of attempts or has
        been cancelled is no longer `RUNNING`. They are kept because this
        predicate is the specification of what is reservable, and the Postgres
        side spells the same two exclusions into its index predicate; a reader
        should be able to answer "would this be handed out?" from here alone.
        Neither is load-bearing in this call path, and neither is a substitute
        for the sweep: excluding a row only makes it invisible, and invisible is
        not the same as finished.
        """
        if row.job.state in (JobState.QUEUED, JobState.FAILED):
            return row.run_after <= now
        if row.job.state is JobState.RUNNING:
            return (
                row.lease_expires_at is not None
                and row.lease_expires_at <= now
                and row.job.attempts < row.job.max_attempts
                and not row.job.cancel_requested
            )
        return False

    # ----------------------------------------------------------------------
    # Finishing
    # ----------------------------------------------------------------------

    async def extend(self, lease: Lease, by: timedelta) -> Lease:
        row = self._live(lease)
        row.lease_expires_at = _now() + by
        # `row.job`, not `lease.job`: the heartbeat is also how a consumer
        # learns its job was cancelled while it was working.
        return Lease(id=lease.id, job=row.job, owner=lease.owner, expires_at=row.lease_expires_at)

    async def ack(self, lease: Lease, *, result_ref: str | None = None) -> None:
        row = self._live(lease)
        row.job = _replace_state(row.job, JobState.DONE, result_ref=result_ref)
        self._release(row)

    async def nack(self, lease: Lease, *, error: str, retry_in: timedelta | None = None) -> None:
        row = self._live(lease)
        if row.job.cancel_requested:
            state = JobState.CANCELLED
        elif row.job.attempts >= row.job.max_attempts:
            state = JobState.DEAD
        else:
            state = JobState.FAILED
        row.job = _replace_state(row.job, state, error=error)
        row.run_after = _now() + (retry_in or timedelta())
        self._release(row)

    async def discard(self, lease: Lease) -> None:
        # Nothing to settle. This queue's deliveries are rows, and a row is not
        # held by anybody — there is no per-consumer resource here to leak,
        # which is exactly the asymmetry the Protocol's docstring warns about.
        # The half that has one is asserted in `test_rabbitmq_job_queue.py`.
        #
        # Not fenced on `lease.id` and not raising on an unknown lease: the
        # contract is that this never fails, and the only thing a check could
        # protect is state this method does not touch.
        self.discarded.append(lease.id)

    def _live(self, lease: Lease) -> _Row:
        """The row this lease still holds, or `StaleLease`.

        Fencing is on `lease.id` alone, not on the deadline: an expired lease
        that nobody has claimed is still this consumer's, because the only thing
        the check has to prevent is *two* writers. Failing an unclaimed late
        lease would add a clock-skew race and protect nothing — and RabbitMQ,
        whose token lives as long as the channel, could not implement it.
        """
        row = self._rows.get(lease.job.id)
        if row is None or row.lease_id != lease.id:
            raise StaleLease(f"lease {lease.id} is no longer live for job {lease.job.id}")
        return row

    @staticmethod
    def _release(row: _Row) -> None:
        row.lease_id = None
        row.lease_expires_at = None
        row.lease_owner = None


def _replace_state(job: Job, state: JobState, **fields) -> Job:
    """`dataclasses.replace`, with `state` promoted to a positional argument.

    Spelled out because every transition in this module goes through it, and a
    bare `replace(job, state=…)` at six call sites is where a forgotten field
    hides.
    """
    return replace(job, state=state, **fields)
