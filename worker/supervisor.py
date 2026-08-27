"""The supervisor: one lease, one child, and the six ways a solve can end.

This is step 3 of `docs/worker.md`'s work order and the phase's centre. Steps 1
and 2 built the two halves it holds apart — an object store the child cannot
reach and a process the loop can kill — and everything here is the arbitration
between them: reserve, spawn, extend while it runs, write, then `ack` or `nack`.

Three properties are worth stating before the code, because each one is a line
of it that looks arbitrary otherwise.

**Nothing in this module computes.** It runs on the event loop that holds the
AMQP heartbeat and the lease, so every wait here is an `await` on a timer, a
pipe read that lives on a thread, or a query. That is the whole reason the child
exists, and the reason `worker/process.py` refuses to read a pipe inline.

**The lease is defended, not assumed.** `extend` is on a timer and its three
outcomes are three different situations that a single `except` would flatten
into one: the ordinary renewal (which also carries `cancel_requested`, so the
heartbeat doubles as the cancellation check), a `StaleLease` that means this
worker's output is already void, and an unreachable database that means nothing
at all yet. Only the second is a reason to stop believing the lease.

**Once the supervisor decides to stop the child, that decision is the outcome.**
A cancel, a cap, or a lost lease discards whatever the child reports on the way
down, even a complete artifact. The alternative — honouring a `Solved` that
arrived inside the grace period — makes a job's terminal state depend on which
of two processes the scheduler ran last, and the value recovered is a result
whose requester has already been told it stopped.

What is deliberately *not* here is progress. `websocket.md` gives the worker
progress events in step 4, and the child's contract is still one message; adding
a second one now would mean designing a protocol before the exchange it
publishes to exists.
"""

import asyncio
import enum
import logging
import os
import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from core.artifacts import ObjectStore, artifact_key
from core.jobs import Job, JobQueue, Lease, StaleLease
from worker.config import WorkerConfig
from worker.process import SolveProcess
from worker.solve import Died, Failed, Outcome, Solved, SolveRequest

log = logging.getLogger(__name__)


class Ending(enum.Enum):
    """Why the supervisor stopped watching a child.

    Separate from the `Outcome` the child reported, and the distinction is the
    one this module is organised around: an outcome is what the work did, an
    ending is what this worker decided about it. A killed child reports `Died`
    whether it was cancelled, capped, or fenced out, and those three are not the
    same job state.
    """

    COMPLETED = enum.auto()
    """The child reported. The outcome decides `ack` or `nack`."""

    CANCELLED = enum.auto()
    """`cancel_requested` came back on an `extend`. The row makes it `cancelled`."""

    TIMED_OUT = enum.auto()
    """The wall-clock cap. A real failure, with a real error on the row."""

    STALE = enum.auto()
    """The lease was taken. Nothing is written and nothing is reported."""

    UNDEFENDABLE = enum.auto()
    """The lease could not be renewed before it lapsed. Redelivery's problem now."""

    RELEASED = enum.auto()
    """The worker is shutting down and gave the job back for someone else."""


@dataclass(frozen=True, slots=True)
class Handled:
    """What one turn of the loop did. Returned for tests and for step 4's metrics.

    The run loop ignores it. It exists because "the job failed" and "the job
    failed *because its lease was cancelled mid-solve*" are the same row and
    different events, and only the supervisor is ever in a position to say which.
    """

    lease: Lease
    outcome: Outcome
    ending: Ending


def default_owner() -> str:
    """Who this worker says it is. Observability only — fencing is `Lease.id`.

    Host and pid, because those are the two facts that turn a log line into a
    container and a process. `JobQueue.reserve` is explicit that correctness
    never rests on this, which is what makes a best-effort hostname acceptable.
    """
    return f"{socket.gethostname()}/{os.getpid()}"


class _Unreachable:
    """The third `extend` outcome, as a type rather than as a `None` with a flag."""

    def __repr__(self) -> str:  # pragma: no cover - debugging only
        return "<database unreachable>"


_UNREACHABLE = _Unreachable()


class Supervisor:
    """Consumes one job at a time, forever, and never computes anything itself.

    One at a time and not by accident: `prefetch=1` is the broker-side half of
    the same decision, and both exist because a five-minute solve held in a
    buffer behind another five-minute solve is work that a second worker could
    have started five minutes ago. Concurrency here is another container.
    """

    def __init__(
        self,
        *,
        queue: JobQueue,
        store: ObjectStore,
        config: WorkerConfig | None = None,
        owner: str | None = None,
    ) -> None:
        self._queue = queue
        self._store = store
        self._config = config or WorkerConfig()
        self._owner = owner or default_owner()
        self._stopping = asyncio.Event()

    @property
    def owner(self) -> str:
        return self._owner

    def stop(self) -> None:
        """Ask the worker to finish. Safe to call from a signal handler.

        Synchronous and idempotent, which is what makes it usable as a
        `loop.add_signal_handler` callback: SIGTERM arrives, this returns
        immediately, and the loop it interrupted is the one that acts on it. An
        in-flight solve is not waited out — it is killed and the job is given
        back — because a rollout's grace period is tens of seconds and a solve
        is minutes, so waiting would mean being SIGKILLed mid-write with a lease
        still held.
        """
        self._stopping.set()

    async def run(self) -> None:
        """Reserve and solve until `stop`. The whole of a worker's life."""
        log.info("worker %s consuming", self._owner)
        while not self._stopping.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Reaching here means the failure was not the job's — `run_once`
                # turns everything a solve can do into a row state. A broker
                # that is down is the plausible one, and the pause is what stops
                # this from becoming a retry storm against it.
                log.exception("worker %s could not take a job", self._owner)
                await asyncio.sleep(self._config.error_pause.total_seconds())
        log.info("worker %s stopped", self._owner)

    async def run_once(self) -> Handled | None:
        """One job, start to terminal state. `None` if nothing was reservable.

        An empty queue is the normal state of this system, so returning `None`
        is an ordinary answer rather than a timeout to handle — the same
        reasoning `JobQueue.reserve` gives for not raising.
        """
        lease = await self._queue.reserve(
            owner=self._owner,
            lease=self._config.lease,
            wait_for=self._config.reserve_wait,
        )
        if lease is None:
            return None

        job = lease.job
        log.info("reserved job %s kind=%s attempt=%s", job.id, job.kind, job.attempts)
        proc = SolveProcess.start(_request(job))
        try:
            handled = await self._supervise(lease, proc)
            await self._finish(handled)
        finally:
            # Unconditional, and the reason `stop` is idempotent: whatever
            # decided the ending above, no path out of this method may leave a
            # child holding a CPU with nobody to reap it.
            await proc.aclose()
        log.info("job %s ended %s", job.id, handled.ending.name.lower())
        return handled

    # ----------------------------------------------------------------------
    # Watching the child
    # ----------------------------------------------------------------------

    async def _supervise(self, lease: Lease, proc: SolveProcess) -> Handled:
        """Hold the lease while the child works. Returns as soon as either ends.

        The shape is one loop rather than a set of racing tasks because there
        are only two things to wait for and three deadlines to check, and a
        `wait` with a timeout says that directly. The solve is a task only so
        that it can be *observed* without being consumed: `SolveProcess.outcome`
        shields its collector, so cancelling this wrapper at the end leaves the
        child still being reaped, which is what `aclose` then finishes.
        """
        loop = asyncio.get_running_loop()
        cap = loop.time() + self._config.solve_timeout.total_seconds()
        solve = asyncio.ensure_future(proc.outcome())
        stopping = asyncio.ensure_future(self._stopping.wait())
        try:
            while True:
                # Whichever comes first, the heartbeat or the cap. Without the
                # second term a cap that falls between two beats is enforced a
                # whole interval late.
                await asyncio.wait(
                    {solve, stopping},
                    timeout=min(
                        self._config.extend_every.total_seconds(),
                        max(cap - loop.time(), 0.0),
                    ),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if solve.done():
                    return Handled(lease, _reported(solve), Ending.COMPLETED)

                if stopping.done():
                    log.info("releasing job %s: worker is shutting down", lease.job.id)
                    return Handled(lease, await self._kill(proc), Ending.RELEASED)

                if loop.time() >= cap:
                    log.warning(
                        "job %s passed its %s wall-clock cap",
                        lease.job.id,
                        self._config.solve_timeout,
                    )
                    return Handled(lease, await self._kill(proc), Ending.TIMED_OUT)

                renewed = await self._extend(lease)
                if renewed is None:
                    return Handled(lease, await self._kill(proc), Ending.STALE)
                if renewed is _UNREACHABLE:
                    if not lease.expired(datetime.now(UTC)):
                        # The database is unreachable; the lease may be
                        # perfectly valid. Nothing has been lost yet, so the
                        # child keeps working and the next beat tries again.
                        continue
                    log.error(
                        "job %s: lease lapsed while the database was unreachable", lease.job.id
                    )
                    return Handled(lease, await self._kill(proc), Ending.UNDEFENDABLE)

                lease = renewed
                if lease.job.cancel_requested:
                    log.info("job %s was cancelled; stopping the child", lease.job.id)
                    return Handled(lease, await self._kill(proc), Ending.CANCELLED)
        finally:
            # The collector is shielded, so this cancels the *watching* and not
            # the reaping. `run_once`'s `aclose` is what actually waits.
            solve.cancel()
            stopping.cancel()

    async def _extend(self, lease: Lease) -> Lease | None | _Unreachable:
        """Renew the claim. `None` means it is gone; `_UNREACHABLE` means unknown.

        Three returns for `docs/worker.md`'s three cases, and the third is the
        one worth the sentinel: a database that cannot be reached says nothing
        about who holds the lease, and treating it as a `StaleLease` would kill
        a child whose claim was never in doubt. Conflating them is the failure
        this signature exists to make unspellable.
        """
        try:
            return await asyncio.wait_for(
                self._queue.extend(lease, self._config.lease),
                # An extend still running when the next one is due is already
                # late, and the retry that follows is the same call again. No
                # separate knob, because there is no separate decision.
                self._config.extend_every.total_seconds(),
            )
        except StaleLease:
            log.warning("job %s: lease %s was taken by someone else", lease.job.id, lease.id)
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - anything that is not StaleLease is "unknown"
            log.warning(
                "job %s: extend failed (%s); the lease may still be ours", lease.job.id, exc
            )
            return _UNREACHABLE

    async def _kill(self, proc: SolveProcess) -> Outcome:
        return await proc.stop(grace=self._config.grace)

    # ----------------------------------------------------------------------
    # Reporting
    # ----------------------------------------------------------------------

    async def _finish(self, handled: Handled) -> None:
        """Turn an ending into a terminal row, or into deliberate silence."""
        lease, outcome, ending = handled.lease, handled.outcome, handled.ending

        match ending:
            case Ending.STALE:
                # Nothing at all, on purpose. The row belongs to whoever took
                # the lease, the artifact was never written, and every write
                # this worker could attempt would either raise `StaleLease` or,
                # worse, land on a job someone else is midway through.
                log.warning("job %s: discarding a solve whose lease is void", lease.job.id)

            case Ending.UNDEFENDABLE:
                # Best-effort, because the reason we are here is that the
                # database was unreachable a moment ago. If it answers now the
                # job is released properly; if not, the lapsed lease is what
                # redelivers it, which is the mechanism this case exists inside.
                await self._release(lease, error="worker lost contact with the job store")

            case Ending.CANCELLED:
                # The row computes `cancelled` from `cancel_requested`, whatever
                # is passed here, so this text is for a log rather than for a
                # user — `nack` is explicit that a cancelled job is never
                # reported as a failure.
                await self._nack(lease, error="cancelled while running")

            case Ending.RELEASED:
                # No backoff: a voluntary release wants the job taken up by the
                # next worker immediately, which is exactly what `retry_in=None`
                # means in the contract.
                await self._nack(lease, error="released by a worker that is shutting down")

            case Ending.TIMED_OUT:
                await self._nack(
                    lease,
                    error=f"solve exceeded its {self._config.solve_timeout} wall-clock cap",
                    retry_in=self._config.retry_in,
                )

            case Ending.COMPLETED:
                await self._report(lease, outcome)

    async def _report(self, lease: Lease, outcome: Outcome) -> None:
        match outcome:
            case Solved():
                await self._install(lease, outcome)
            case Failed():
                # `permanent` is deliberately not acted on. `docs/worker.md`
                # fears that retrying an unknown `kind` "burns three solves'
                # worth of lease time", but the registry is consulted in the
                # child before any work happens, so what it actually burns is
                # three spawns and two backoffs. That is not worth a verb on
                # `JobQueue` that every implementation would have to grow —
                # and phase 4, which gives `kind` meaning, is where the answer
                # stops being hypothetical. Logged distinctly so the choice is
                # visible in an incident rather than only in this comment.
                if outcome.permanent:
                    log.error("job %s failed permanently: %s", lease.job.id, outcome.error)
                else:
                    log.warning(
                        "job %s failed: %s\n%s", lease.job.id, outcome.error, outcome.traceback
                    )
                await self._nack(lease, error=outcome.error, retry_in=self._config.retry_in)
            case Died():
                # Nobody asked for this one — the endings that do are handled
                # above and never reach here — so it is an ordinary failure with
                # an exit status for a message.
                log.error("job %s: %s", lease.job.id, outcome.error)
                await self._nack(lease, error=outcome.error, retry_in=self._config.retry_in)

    async def _install(self, lease: Lease, solved: Solved) -> None:
        """Write the artifact, then point the row at it. Never the other way.

        The order is `docs/worker.md`'s and the asymmetry is the argument for
        it: a crash between these two leaves an object nobody references, which
        is a cost, while the reverse leaves a row referencing an object that
        does not exist, which is a broken download three days later.

        The key carries the lease id, so this attempt cannot collide with a
        competing one — there is no last-writer-wins here because there is no
        shared destination.
        """
        key = artifact_key(lease.job.id, lease.id)
        try:
            await self._store.put(key, solved.data, content_type=solved.content_type)
        except Exception as exc:  # noqa: BLE001 - a failed write is a failed attempt, not a crash
            log.exception("job %s: writing the artifact failed", lease.job.id)
            await self._nack(
                lease, error=f"writing the artifact failed: {exc}", retry_in=self._config.retry_in
            )
            return

        try:
            await self._queue.ack(lease, result_ref=key)
        except StaleLease:
            # The one place an orphan is created knowingly: the bytes are
            # durable and nothing will ever point at them. `worker.md` files
            # reaping them as an open question and as a cost rather than a
            # correctness problem — the row is already someone else's result.
            log.warning("job %s: lease lost before ack; artifact %s is orphaned", lease.job.id, key)
            return
        log.info("job %s done, artifact at %s", lease.job.id, key)

    async def _nack(self, lease: Lease, *, error: str, retry_in: timedelta | None = None) -> None:
        try:
            await self._queue.nack(lease, error=error, retry_in=retry_in)
        except StaleLease:
            # Someone else owns the job's outcome now, and it is not this
            # worker's to fail. Swallowed rather than raised because there is
            # nothing left to do about it and the run loop must keep going.
            log.warning("job %s: lease lost before nack could report %r", lease.job.id, error)

    async def _release(self, lease: Lease, *, error: str) -> None:
        """`_nack`, for the case where the job store may not answer at all."""
        try:
            await self._nack(lease, error=error)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the lapsed lease is the fallback, and it is enough
            log.warning("job %s: could not release the lease; letting it lapse", lease.job.id)


def _request(job: Job) -> SolveRequest:
    """What the child is allowed to know: the question, never the history.

    `SolveRequest` rather than the `Job` itself, for the reason that dataclass
    documents — a solver that could read its own `attempts` would be a solver
    whose redelivery is not the same computation.
    """
    return SolveRequest(kind=job.kind, payload=job.payload, job_id=job.id)


def _reported(solve: asyncio.Future[Outcome]) -> Outcome:
    """The child's outcome, or the collector's failure dressed as one.

    `.result()` can only raise if the collector itself failed — an outcome that
    would not unpickle is the plausible one. Converted rather than allowed to
    escape, because an exception out of `_supervise` leaves the lease held by a
    supervisor that has stopped watching it, and the row sits `running` until it
    lapses.
    """
    try:
        return solve.result()
    except Exception as exc:  # noqa: BLE001 - reported as this attempt's failure
        log.exception("could not collect the child's outcome")
        return Failed(error=f"the solve child's outcome could not be read: {exc}", traceback="")
