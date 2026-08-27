"""The supervisor: what it does with a lease it holds, and what it does when it loses one.

Everything here runs in `make test` with nothing up. That is not a compromise —
the supervisor takes a `JobQueue` and an `ObjectStore` and never learns which
implementations it has, so a memory pair exercises the whole of its decision
making. What needs real infrastructure is the *redelivery* half of the
acceptance ("kill a worker mid-solve, the job completes elsewhere"), which is
step 5's script and phase 2's already-passing
`test_a_consumer_cut_off_mid_job_has_its_work_finished_elsewhere`.

The tests `docs/worker.md` names are the ones with real children in them, and
they are deliberately not written against a cooperative fake. A solve that
checks a flag cancels trivially; the ones below burn a CPU inside a process that
has installed `SIG_IGN`, which is the case the document insists a mock cannot
represent. Every wait is bounded, because a test asserting "and then it is
killed" hangs rather than fails when it is not.
"""

import asyncio
import json
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from core.artifacts import artifact_key
from core.jobs import Job, JobRequest, JobState, StaleLease
from tests.MemoryJobQueue import MemoryJobQueue
from tests.MemoryObjectStore import MemoryObjectStore
from worker.config import WorkerConfig
from worker.solve import Died, Failed, Solved
from worker.supervisor import Ending, Supervisor, _request

# Longer than any test here should take, and an order of magnitude over what a
# healthy run costs. `spawn` boots a fresh interpreter, so the floor on anything
# with a child in it is a couple of hundred milliseconds.
PATIENT = timedelta(seconds=20)

# What a solve that is meant to be interrupted asks for. Far beyond any bound in
# this file, so a test that fails to stop it fails on its own deadline rather
# than by waiting for the burn to end.
FOREVER = 60.0


def a_config(**overrides) -> WorkerConfig:
    """The production shape with every duration collapsed.

    The ratios are what matter and they are preserved: the heartbeat is well
    inside the lease, the cap is well beyond a normal solve, and the grace is
    long enough for a child to exit on SIGTERM. Only the scale changes.
    """
    return WorkerConfig(
        **{
            "lease": timedelta(seconds=5),
            "extend_every": timedelta(milliseconds=100),
            "solve_timeout": timedelta(seconds=15),
            "grace": timedelta(milliseconds=250),
            "reserve_wait": timedelta(milliseconds=200),
            "retry_in": timedelta(),
            "error_pause": timedelta(milliseconds=10),
            **overrides,
        }
    )


@pytest.fixture
def queue() -> MemoryJobQueue:
    return MemoryJobQueue()


@pytest.fixture
def store() -> MemoryObjectStore:
    return MemoryObjectStore()


@pytest.fixture
def worker(queue, store) -> Supervisor:
    return Supervisor(queue=queue, store=store, config=a_config(), owner="test-worker")


async def enqueue(queue, *, kind: str = "synthetic", max_attempts: int = 3, **payload):
    return await queue.enqueue(JobRequest(kind=kind, payload=payload, max_attempts=max_attempts))


async def until(predicate, *, within: timedelta = PATIENT, what: str = "the condition"):
    """Wait for something to become true, or fail. Never hang.

    Polling rather than an event because what is being waited on is a row's
    state, which is exactly what a second worker would have to poll for too.
    """
    deadline = time.monotonic() + within.total_seconds()
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def running(queue, job_id):
    async def check() -> bool:
        job = await queue.get(job_id)
        return job.state is JobState.RUNNING

    return check


async def handled(worker) -> object:
    """One turn of the loop, bounded."""
    return await asyncio.wait_for(worker.run_once(), PATIENT.total_seconds())


# ----------------------------------------------------------------------
# The ordinary path
# ----------------------------------------------------------------------


async def test_an_empty_queue_is_an_ordinary_answer(worker):
    """`None`, not an exception and not a spawn. An idle queue is the normal state."""
    assert await handled(worker) is None


async def test_a_solved_job_is_written_then_acked(worker, queue, store):
    job = await enqueue(queue, padding_bytes=32)

    result = await handled(worker)

    assert result.ending is Ending.COMPLETED
    assert isinstance(result.outcome, Solved)

    row = await queue.get(job.id)
    assert row.state is JobState.DONE
    # The key carries the lease, which is the whole mechanism behind "two
    # executions, one effect": a second attempt would write somewhere else.
    assert row.result_ref == artifact_key(job.id, result.lease.id)
    assert row.result_ref in store
    assert store.content_type(row.result_ref) == "application/json"
    assert json.loads(await store.get(row.result_ref))["solver"] == "synthetic"


async def test_the_artifact_is_durable_before_the_row_points_at_it(store):
    """Write, then ack — the order `docs/worker.md` settles and the reason for it.

    A crash between them leaves an orphan, which is a cost. The reverse leaves a
    row pointing at an object that does not exist, which is a broken download
    three days later. Asserted by looking at the bucket from inside `ack`,
    because after the fact the two orders are indistinguishable.
    """

    class _WatchesTheOrder:
        def __init__(self, queue, store):
            self._queue, self._store = queue, store
            self.artifact_present_at_ack: bool | None = None

        async def ack(self, lease, *, result_ref=None):
            self.artifact_present_at_ack = result_ref in self._store
            return await self._queue.ack(lease, result_ref=result_ref)

        def __getattr__(self, name):
            return getattr(self._queue, name)

    queue = _WatchesTheOrder(MemoryJobQueue(), store)
    await enqueue(queue)
    worker = Supervisor(queue=queue, store=store, config=a_config())

    await handled(worker)

    assert queue.artifact_present_at_ack is True


async def test_a_solver_that_raises_leaves_the_job_failed_with_its_error(worker, queue, store):
    job = await enqueue(queue, raise_error="the matrix was singular")

    result = await handled(worker)

    assert isinstance(result.outcome, Failed)
    row = await queue.get(job.id)
    assert row.state is JobState.FAILED
    assert "the matrix was singular" in row.error
    # Nothing was written, so there is nothing to reap: the artifact only exists
    # when the solve produced one.
    assert not store.written
    assert row.result_ref is None


async def test_a_child_that_dies_without_reporting_is_nacked_rather_than_acked(
    worker, queue, store
):
    """A segfaulting solve must not become a `done` row with no artifact behind it."""
    job = await enqueue(queue, crash=True)

    result = await handled(worker)

    assert isinstance(result.outcome, Died)
    row = await queue.get(job.id)
    assert row.state is JobState.FAILED
    assert "status 9" in row.error
    assert row.result_ref is None
    assert not store.written


async def test_an_unknown_kind_fails_the_attempt_rather_than_the_worker(worker, queue):
    """`docs/worker.md` leaves the policy open; this is the one the supervisor took.

    A permanent failure is nacked like any other, so an unknown `kind` uses up
    its attempts and reaches `dead`. The document worries that costs "three
    solves' worth of lease time" — it does not, because the registry is consulted
    in the child before any work happens, so what it actually costs is three
    spawns. Cheap enough not to be worth a new verb on `JobQueue`, and phase 4 is
    where `kind` stops being opaque anyway.
    """
    job = await enqueue(queue, kind="does-not-exist", max_attempts=1)

    result = await handled(worker)

    assert isinstance(result.outcome, Failed)
    assert result.outcome.permanent
    row = await queue.get(job.id)
    assert row.state is JobState.DEAD
    assert "does-not-exist" in row.error


async def test_a_write_that_fails_leaves_the_job_retryable_and_unpointed(worker, queue, store):
    """The artifact is the last thing that can go wrong before an `ack`.

    A store that refuses must not produce a `done` row: the pointer is the only
    thing that means anything, and one installed against a failed write is the
    dangling reference the write/ack order exists to prevent.
    """
    store.fail_with = RuntimeError("the bucket said no")
    job = await enqueue(queue)

    await handled(worker)

    row = await queue.get(job.id)
    assert row.state is JobState.FAILED
    assert row.result_ref is None
    assert "the bucket said no" in row.error


# ----------------------------------------------------------------------
# Holding the lease, and losing it
# ----------------------------------------------------------------------


async def test_the_lease_is_extended_for_as_long_as_the_child_runs(queue, store):
    """The heartbeat, with the broker's half of it removed.

    A solve longer than the lease completes anyway, which is only true if
    something moved the deadline: with the extend loop removed the lease lapses
    mid-solve and the `ack` raises `StaleLease`. A sleep rather than a burn, so
    what is being timed is the supervisor's clock and not the child's CPU.
    """
    config = a_config(lease=timedelta(milliseconds=400), extend_every=timedelta(milliseconds=80))
    worker = Supervisor(queue=queue, store=store, config=config)
    job = await enqueue(queue, sleep_seconds=1.2)

    claimed_at = datetime.now(UTC)
    result = await handled(worker)

    assert result.ending is Ending.COMPLETED
    assert (await queue.get(job.id)).state is JobState.DONE
    # The deadline it finished holding is past where the original claim put it,
    # which is the only way a solve three times the lease reaches `done`.
    assert result.lease.expires_at > claimed_at + config.lease


async def test_a_stale_lease_mid_solve_kills_the_child_and_installs_no_pointer(queue, store):
    """The case `docs/worker.md` calls the only correct action.

    Someone else holds the job, so this child's output is void before it exists.
    Letting it finish would spend a CPU another worker is also spending and end
    in an artifact nobody will ever point at. The solver ignores SIGTERM, so what
    stops it is layer 3 — which is the half a cooperative fake cannot show.
    """
    worker = Supervisor(queue=queue, store=store, config=a_config(), owner="loser")
    job = await enqueue(queue, burn_seconds=FOREVER, ignore_sigterm=True)

    turn = asyncio.ensure_future(worker.run_once())
    await until(running(queue, job.id), what="the worker to claim the job")

    # Another consumer takes it: force the lease past its deadline, then reserve
    # for real, so the theft is a genuine claim with a genuine new token rather
    # than a poked field.
    queue._rows[job.id].lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    thief = await queue.reserve(owner="thief", lease=timedelta(seconds=30), wait_for=timedelta())
    assert thief is not None

    result = await asyncio.wait_for(turn, PATIENT.total_seconds())

    assert result.ending is Ending.STALE
    assert isinstance(result.outcome, Died)
    assert result.outcome.signal is signal.SIGKILL
    # Nothing written and nothing said: the row is the thief's now, and the
    # assertion is on `result_ref` rather than on the bucket, per `worker.md`.
    assert not store.written
    row = await queue.get(job.id)
    assert row.result_ref is None
    assert row.state is JobState.RUNNING
    # And the thief still holds a live lease — the loser touched nothing.
    await queue.extend(thief, timedelta(seconds=30))


async def test_an_unreachable_database_is_retried_inside_the_lease(store):
    """Not the same as `StaleLease`, and must not be treated as it.

    The lease may be perfectly valid; the database is merely unreachable. Killing
    the child here would throw away a solve for a claim that was never in doubt,
    so the supervisor keeps working and tries again on the next beat.
    """

    class _Unreachable:
        def __init__(self, queue, failures: int):
            self._queue, self._left = queue, failures
            self.attempts = 0

        async def extend(self, lease, by):
            self.attempts += 1
            if self._left > 0:
                self._left -= 1
                raise ConnectionError("no route to the database")
            return await self._queue.extend(lease, by)

        def __getattr__(self, name):
            return getattr(self._queue, name)

    queue = _Unreachable(MemoryJobQueue(), failures=3)
    worker = Supervisor(queue=queue, store=store, config=a_config())
    job = await enqueue(queue, sleep_seconds=0.8)

    result = await handled(worker)

    assert queue.attempts >= 3, "the test did not actually exercise a failing extend"
    assert result.ending is Ending.COMPLETED
    assert (await queue.get(job.id)).state is JobState.DONE


async def test_a_lease_that_cannot_be_defended_stops_the_child(store):
    """Give up only when the lease is genuinely spent, and then kill the child.

    Continuing to compute against a lease that can no longer be defended is how
    two workers write two artifacts. The row is left `running` with a lapsed
    lease, which is precisely the state redelivery exists to find — this worker
    cannot say anything about it, because the thing it would say it to is the
    thing that is unreachable.
    """

    class _Gone:
        """A job store that stops answering the moment the solve starts."""

        def __init__(self, queue):
            self._queue = queue

        async def extend(self, lease, by):
            raise ConnectionError("no route to the database")

        async def nack(self, lease, *, error, retry_in=None):
            raise ConnectionError("no route to the database")

        def __getattr__(self, name):
            return getattr(self._queue, name)

    queue = _Gone(MemoryJobQueue())
    config = a_config(lease=timedelta(milliseconds=400), extend_every=timedelta(milliseconds=80))
    worker = Supervisor(queue=queue, store=store, config=config)
    job = await enqueue(queue, burn_seconds=FOREVER, ignore_sigterm=True)

    result = await handled(worker)

    assert result.ending is Ending.UNDEFENDABLE
    assert isinstance(result.outcome, Died)
    assert result.outcome.signal is signal.SIGKILL
    assert not store.written
    row = await queue.get(job.id)
    assert row.state is JobState.RUNNING
    assert row.result_ref is None


# ----------------------------------------------------------------------
# Stopping work that is not listening
# ----------------------------------------------------------------------


async def test_cancelling_a_solve_inside_a_long_call_still_ends_the_job(queue, store):
    """The test `docs/worker.md` insists on: the solver ignores the flag.

    Layer 1 is not consulted — this child checks nothing — and layer 2's SIGTERM
    is discarded, so what ends it is SIGKILL. The row reaches `cancelled` rather
    than `failed` because `nack` computes that from `cancel_requested` and not
    from anything the worker says, which is what keeps a user's own cancellation
    from being reported back to them as an error.
    """
    worker = Supervisor(queue=queue, store=store, config=a_config())
    job = await enqueue(queue, burn_seconds=FOREVER, ignore_sigterm=True)

    turn = asyncio.ensure_future(worker.run_once())
    await until(running(queue, job.id), what="the worker to claim the job")
    await queue.cancel(job.id)

    result = await asyncio.wait_for(turn, PATIENT.total_seconds())

    assert result.ending is Ending.CANCELLED
    assert isinstance(result.outcome, Died)
    assert result.outcome.signal is signal.SIGKILL
    row = await queue.get(job.id)
    assert row.state is JobState.CANCELLED
    assert row.error is None or "cancel" in row.error.lower()
    assert row.result_ref is None
    assert not store.written


async def test_a_wedged_child_hits_the_cap_and_fails_with_a_real_error(queue, store):
    """The bound the lease cannot express.

    A healthy supervisor extends a wedged child forever and the row looks
    permanently `running` — `_LAPSED` cannot tell "this worker is alive" from
    "this job will finish". So the cap is the supervisor's own clock, and past
    it the job fails with something a user can read.
    """
    config = a_config(solve_timeout=timedelta(milliseconds=500))
    worker = Supervisor(queue=queue, store=store, config=config)
    job = await enqueue(queue, burn_seconds=FOREVER, ignore_sigterm=True)

    started = time.monotonic()
    result = await handled(worker)
    elapsed = time.monotonic() - started

    assert result.ending is Ending.TIMED_OUT
    assert isinstance(result.outcome, Died)
    assert result.outcome.signal is signal.SIGKILL
    assert elapsed < 10, "the cap was not enforced anywhere near when it was due"
    row = await queue.get(job.id)
    assert row.state is JobState.FAILED
    assert "cap" in row.error
    assert row.result_ref is None


async def test_a_shutting_down_worker_gives_its_job_back_at_once(queue, store):
    """A rollout's grace is tens of seconds and a solve is minutes.

    So a worker asked to stop does not wait out its child — it kills it and
    releases the job with no backoff, which is what `retry_in=None` means in the
    contract. The job is immediately reservable by whoever is left, instead of
    sitting `running` until its lease lapses.
    """
    worker = Supervisor(queue=queue, store=store, config=a_config())
    job = await enqueue(queue, burn_seconds=FOREVER, ignore_sigterm=True)

    turn = asyncio.ensure_future(worker.run_once())
    await until(running(queue, job.id), what="the worker to claim the job")
    worker.stop()

    result = await asyncio.wait_for(turn, PATIENT.total_seconds())

    assert result.ending is Ending.RELEASED
    assert isinstance(result.outcome, Died)
    assert not store.written
    # Reservable again on the spot, which is the whole difference between
    # releasing a job and abandoning it.
    successor = await queue.reserve(
        owner="the-next-worker", lease=timedelta(seconds=30), wait_for=timedelta()
    )
    assert successor is not None
    assert successor.job.id == job.id


async def test_the_run_loop_returns_when_it_is_asked_to_stop(worker):
    """`stop` is safe from a signal handler, so it only sets a flag; `run` reads it."""
    loop = asyncio.ensure_future(worker.run())
    worker.stop()

    await asyncio.wait_for(loop, PATIENT.total_seconds())


async def test_a_heartbeat_at_the_lease_boundary_is_refused():
    """A configuration error caught at construction rather than in production.

    An extend interval at or past the lease renews a claim that has already
    lapsed. It works until another worker is quick enough to take the job, and
    then loses a solve in the shape of a broker fault — the disguise this phase
    keeps running into.
    """
    with pytest.raises(ValueError, match="inside lease"):
        WorkerConfig(lease=timedelta(seconds=10), extend_every=timedelta(seconds=10))


async def test_a_stale_lease_at_ack_time_leaves_an_orphan_rather_than_an_exception(store):
    """The one place an orphan is made knowingly.

    The bytes are durable and the pointer cannot be installed, so nothing will
    ever reference them. `worker.md` files that as a cost rather than a
    correctness problem, and the requirement here is only that the worker
    survives it: an exception out of `run_once` would take the run loop with it.
    """

    class _StolenAtTheLastMoment:
        def __init__(self, queue):
            self._queue = queue

        async def ack(self, lease, *, result_ref=None):
            raise StaleLease("someone else finished it")

        def __getattr__(self, name):
            return getattr(self._queue, name)

    queue = _StolenAtTheLastMoment(MemoryJobQueue())
    worker = Supervisor(queue=queue, store=store, config=a_config())
    job = await enqueue(queue)

    result = await handled(worker)

    assert result.ending is Ending.COMPLETED
    # Written, and unreferenced — which is exactly what an orphan is.
    assert store.written == [artifact_key(job.id, result.lease.id)]
    assert (await queue.get(job.id)).result_ref is None


def test_a_job_id_is_all_the_correlation_the_child_gets():
    """`SolveRequest` is built from the row, and carries nothing else.

    A solver that could read its own `attempts` is a solver whose redelivery is
    not the same computation, so what crosses the boundary is the question and
    never the history of asking it.
    """
    job = Job(
        id=uuid.uuid4(),
        kind="synthetic",
        payload={"burn_seconds": 1},
        state=JobState.RUNNING,
        attempts=2,
        max_attempts=3,
        result_ref="jobs/somewhere/else",
        error="a previous attempt failed",
    )

    request = _request(job)

    assert request.kind == job.kind
    assert request.payload == job.payload
    assert request.job_id == job.id
    assert not hasattr(request, "attempts")
