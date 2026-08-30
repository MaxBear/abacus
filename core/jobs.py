"""The job seam: what a pull-based work queue is, independent of what runs it.

Kept in `core/` and expressed as a Protocol for the same reason `Responder` and
`ChatRepository` are — but with a sharper motive here. Phase 2 exists to build
this twice, on Postgres and on RabbitMQ, and to run one suite against both; that
both pass unmodified *is* the proof this is a real seam rather than a shape
traced around whichever implementation was written first. See `docs/jobs.md`.

Nothing here is transport, storage, or scheduling policy. A `JobQueue` hands out
leases and takes them back. What a `kind` means, what a `payload` contains, and
what a worker does with either are questions for phases 3 and 4.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol


class JobState(StrEnum):
    """Where a job is in its life.

    `StrEnum` for the same reason `Role` and `Status` are: a member *is* its
    value, so it binds to a Text column and compares equal to what a query
    returns without a conversion step.

    `FAILED` and `DEAD` are not the same thing and the distinction is the
    retry policy made visible. A job that raised is `FAILED` and will be tried
    again once `run_after` passes; a job that has exhausted `max_attempts` is
    `DEAD` and never runs again. Collapsing them would mean a client cannot
    tell "not yet" from "never", which is the only question it actually has.

    `CANCELLED` is separate from both for the same kind of reason: the user
    asked for it, so it is not a failure and must not be reported as one.
    """

    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD = "dead"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        """True when nothing further will happen without a new enqueue."""
        return self in (JobState.DONE, JobState.DEAD, JobState.CANCELLED)


class StaleLease(Exception):
    """The lease being used is no longer the live one for its job.

    Raised by `extend`, `ack`, and `nack` when the lease expired and another
    consumer has since claimed the job. The paused-process case is the one that
    matters: a consumer stalls past its deadline, the job is redelivered and
    completed elsewhere, and then the original wakes up and tries to `ack` a job
    someone else finished. Without a fencing check that write lands and silently
    overwrites the real result.

    It is an exception rather than a `False` return because there is no correct
    way to continue: the caller's work is void, and a caller that ignores the
    signal is the exact bug this exists to catch.
    """


@dataclass(frozen=True, slots=True)
class JobRequest:
    """What a caller asks for. Not yet a job — `enqueue` makes it one.

    Separate from `Job` because they carry different knowledge. A request is
    everything the caller can know; a `Job` additionally carries what only the
    queue can say — its id, its state, how many times it has been attempted.
    Merging them would mean constructing a Job with placeholder values for the
    fields the caller has no business setting.
    """

    # The queue treats both as opaque. `kind` selects an analysis in phase 4;
    # `payload` is the AnalysisRequest. Neither is inspected here, which is what
    # keeps the queue reusable for work that has nothing to do with analytics.
    kind: str
    payload: dict[str, Any]

    # Which chat session to report progress to. Optional because a job need not
    # have come from a conversation — a scheduled backfill has no session — and
    # because the queue must not require the chat schema to be useful.
    session_id: uuid.UUID | None = None

    # The caller's deduplication key, scoped to the session. `client_msg_id`'s
    # counterpart, and it exists for the identical reason: a client unsure
    # whether its submit landed retries, and here a duplicate costs a
    # five-minute solve. Defaulted to a fresh uuid so "I genuinely want another
    # one" needs no ceremony, while retries that mean it supply their own.
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))

    max_attempts: int = 3

    # How long to hold this back before it is reservable. Nonzero only for
    # deliberately deferred work; retry backoff is `nack`'s business, not this.
    delay: timedelta = timedelta()


@dataclass(frozen=True, slots=True)
class Job:
    """A job as the queue knows it. A snapshot, never a live handle.

    Frozen and plain, so nothing about a job's storage crosses this line — the
    same rule `ChatRepository` follows. A caller holding one of these cannot
    accidentally mutate queue state, and a stale copy is obviously stale rather
    than quietly wrong.
    """

    id: uuid.UUID
    kind: str
    payload: dict[str, Any]
    state: JobState
    attempts: int
    max_attempts: int
    session_id: uuid.UUID | None = None
    idempotency_key: str = ""
    # Where the artifact landed. Object storage arrives in phase 3; until then
    # nothing sets this, which is why it is nullable rather than absent.
    result_ref: str | None = None
    # The last failure's message. Kept across a retry deliberately: a job that
    # failed twice for different reasons is a different situation from one that
    # failed the same way twice, and losing the text hides that.
    error: str | None = None
    # Set by `cancel` on a job a consumer already holds. A running job cannot be
    # stopped from the outside — something has to tell a busy process, and the
    # process has to notice — so this is the asking half, and a consumer polls
    # it. See `JobQueue.cancel`.
    cancel_requested: bool = False


@dataclass(frozen=True, slots=True)
class Lease:
    """A claim on a job, valid until `expires_at`.

    A lease is a *timestamp*, not a held lock. Nothing in the database or the
    broker is blocked while a consumer works; the claim is a row that has been
    marked, and it lapses on its own if the consumer dies. That is what lets a
    five-minute solve survive the process that started it — and it is the whole
    reason this is a lease-based queue rather than a `SELECT … FOR UPDATE` held
    open across the work, which would pin a connection for five minutes and lose
    the job outright if the consumer's network blinked.

    `id` is the fencing token. It changes on every reservation, so a consumer
    whose lease expired and was claimed elsewhere presents a token the queue no
    longer recognizes and gets `StaleLease` instead of a successful write. Using
    the job id alone would make the two indistinguishable.
    """

    id: uuid.UUID
    job: Job
    owner: str
    expires_at: datetime

    def expired(self, now: datetime) -> bool:
        """Advisory only. The queue's own check at write time is what decides.

        Useful for a consumer that wants to stop work it already knows is void,
        but never sufficient: a consumer's clock is not the queue's, and the
        answer can change between asking and acting.
        """
        return now >= self.expires_at


class JobQueue(Protocol):
    """A pull-based work queue with leases.

    Pull, not push: a consumer takes work only when it has capacity, which makes
    backpressure a property of the shape rather than a feature added later. It
    is also the only shape implementable on both backings without one of them
    hiding a loop behind a callback — see `docs/jobs.md`.

    Implementations own their own transactions and their own waiting strategy.
    A caller never sees a session, a channel, or a delivery tag.
    """

    async def enqueue(self, request: JobRequest) -> Job:
        """Create the job, or return the one this key already created.

        Idempotent on `(session_id, idempotency_key)`, enforced by a unique
        index rather than a check-then-insert — the same argument
        `docs/persistence.md` makes about `client_msg_id`.

        A duplicate returns the existing job rather than reporting that it was a
        duplicate, and the caller does not need to be told: it wanted a job id to
        watch, and the existing one is the right answer whatever its state. That
        is the difference from `record_user_message`, where a second call had to
        be prevented from starting a second turn.
        """
        ...

    async def reserve(self, *, owner: str, lease: timedelta, wait_for: timedelta) -> Lease | None:
        """Claim one job, waiting up to `wait_for` for one to exist.

        Returns `None` on timeout — an ordinary outcome, not an error: an idle
        queue is the normal state of this system, and a consumer loop should be
        able to poll a `None` without exception handling.

        At most one consumer may hold a live lease on a job. How that is
        achieved is the implementation's business and is exactly what phase 2
        measures: `FOR UPDATE SKIP LOCKED` on one side, a prefetch window of one
        on the other.

        `owner` identifies the consumer for observability only. Correctness
        rests on `Lease.id`, never on this.
        """
        ...

    async def extend(self, lease: Lease, by: timedelta) -> Lease:
        """Push the deadline out. Raises `StaleLease` if the claim already lapsed.

        Called by a consumer that is still working and expects to exceed its
        original lease. Returns a new `Lease` rather than mutating: the token is
        the caller's proof of claim, and a mutable one is a token that can be
        held past the moment it stopped being true.

        The returned lease carries a *fresh* `job`, which is what makes the
        heartbeat double as the cancellation check — a consumer that extends
        learns `cancel_requested` in the same round trip instead of needing a
        second call it would have to remember to make.
        """
        ...

    async def ack(self, lease: Lease, *, result_ref: str | None = None) -> None:
        """Terminal success. The job reaches `DONE` and is never redelivered.

        Raises `StaleLease` if this consumer no longer holds the claim, which is
        the case that protects a result someone else already recorded.
        """
        ...

    async def nack(self, lease: Lease, *, error: str, retry_in: timedelta | None = None) -> None:
        """Give the job back, failed or voluntarily released.

        `retry_in` is the backoff: the job is not reservable until it elapses.
        `None` means immediately, which is right for a voluntary release —
        shutting down, out of capacity — and wrong for a failure, since an
        instant retry of a deterministic error is a hot loop.

        A job with `cancel_requested` set reaches `CANCELLED` instead of
        `FAILED` or `DEAD`, whatever the consumer passes as `error`: the user
        asked for this, and reporting it as a failure would put an error in
        front of someone who already knows what happened.

        A job whose `attempts` have reached `max_attempts` goes to `DEAD` rather
        than back to the queue. That decision belongs here and not in the
        consumer: a consumer that crashes cannot make it, and the redelivered
        attempt has to be counted the same way whether it ended in a `nack` or a
        lapsed lease.

        Raises `StaleLease` on a claim that already lapsed — without it, a slow
        consumer's failure would mark a job failed that another consumer is
        midway through completing.
        """
        ...

    async def discard(self, lease: Lease) -> None:
        """Let go of a claim without deciding the job's outcome.

        The third way to be done with a lease, and the only one that writes
        nothing: `ack` says the job succeeded, `nack` says it should go round
        again, and this says the consumer has no standing to say either. The
        case it exists for is a lease that was taken while its holder was still
        working — the row belongs to whoever holds it now, and any write here
        would either raise `StaleLease` or, worse, land on a job someone else is
        midway through.

        So the row is deliberately untouched. What this releases is whatever the
        *implementation* is still holding on the consumer's behalf, which on
        RabbitMQ is an unacked delivery and at `prefetch=1` is the consumer's
        only slot. A backing with nothing of the kind implements this as
        nothing, and that asymmetry is the whole reason it is on the Protocol: a
        caller cannot know which one it has, and the one that leaks does so
        silently — a consumer that goes on reserving and is never offered
        anything reads as a broker fault from every angle except this one.

        Never raises, including on a lease that is already gone, and safe to
        call twice. Every path that reaches it is a path where something has
        already failed, and a cleanup that can fail is a cleanup every caller
        has to wrap.
        """
        ...

    async def cancel(self, job_id: uuid.UUID) -> Job | None:
        """Ask for a job to stop. Idempotent; returns the job, or `None` if unknown.

        Two unrelated things happen under one name, and which one depends on
        whether a consumer has the job yet:

        - **Not yet reserved** — the job moves straight to `CANCELLED` and no
          consumer ever claims it. This is the common case, since the reason a
          user cancels is usually that the job has been sitting in a queue.
        - **Already running** — only `cancel_requested` is set. The consumer
          sees it on its next `extend` and releases the job with `nack`, which
          reaches `CANCELLED` rather than `FAILED`.

        Already-terminal jobs are returned unchanged rather than raising: a
        client that cancels twice across a reconnect gets the same answer both
        times, which is the whole point of the operation being a transition.

        What this deliberately does *not* do is interrupt a worker blocked
        inside one long computation, which checks nothing and can only be
        stopped by killing the process holding it. That needs a real process, a
        signal path into it, and a decision about what a half-written artifact
        means — all three are phase 3. Polling a flag is the slower, duller
        mechanism that survives that rewrite.
        """
        ...

    async def get(self, job_id: uuid.UUID) -> Job | None:
        """Current state, for a client asking what happened to its job.

        A queue cannot generally answer this — RabbitMQ knows nothing about a
        message it has delivered — which is why `docs/jobs.md` makes the `jobs`
        table the system of record on both sides rather than only where it comes
        free.
        """
        ...
