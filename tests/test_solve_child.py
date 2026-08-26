"""The solve child: that it is a real process, and that it can be stopped.

Everything here runs in `make test` with nothing up. That is the point of
building step 2 before the supervisor — the process boundary, the payload
contract, and both violent halves of cancellation are answerable with no broker,
no database, and no bucket, and a suite that needs none of them is one that
actually gets run.

Two tests carry the weight, and they are the two `docs/worker.md` names. **The
loop keeps scheduling through a burn** is the one that fails the moment someone
simplifies the child back into `run_in_executor`, which the document calls the
most likely future regression in this phase. **A solver that ignores SIGTERM
still dies** is the one that separates layer 3 from layer 2; without it the
suite would prove only that a cooperative child stops when asked, which
`jobs.md` already warned is the mock's version of cancellation and not the real
one.

Every wait is bounded, for the reason this suite keeps rediscovering: a test
asserting "and then it is killed" hangs rather than fails when it is not.
"""

import ast
import asyncio
import json
import os
import signal
import time
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

from worker import process as process_module
from worker.process import SolveProcess
from worker.solve import Died, Failed, Solved, SolveRequest

WORKER = Path(process_module.__file__).parent

# Enough for a fresh interpreter to boot, import the child, and reach the
# solver's first statement. `spawn` execs a new Python, which is the expensive
# part and takes a couple of hundred milliseconds here; this is the margin over
# that. It matters in exactly one place — the child that installs `SIG_IGN` has
# to have installed it before the SIGTERM arrives, or the test proves layer 2
# twice and layer 3 never.
SETTLE = timedelta(milliseconds=750)

# The grace between SIGTERM and SIGKILL in the tests that want to see the
# escalation. Short because the escalation is the assertion, not the patience.
BRIEF_GRACE = timedelta(milliseconds=250)

# An upper bound on any single await here. Generous by an order of magnitude
# against what a healthy run takes, so a failure is a failure rather than a hang.
PATIENT = timedelta(seconds=20)


def a_request(**payload) -> SolveRequest:
    return SolveRequest(kind="synthetic", payload=payload, job_id=uuid.uuid4())


async def outcome_of(request: SolveRequest) -> Solved | Failed | Died:
    """Run one solve to completion, bounded."""
    proc = SolveProcess.start(request)
    try:
        return await asyncio.wait_for(proc.outcome(), PATIENT.total_seconds())
    finally:
        await proc.aclose()


def test_the_start_method_is_spawn_and_not_fork():
    """A settled decision, asserted because a regression to `fork` would pass.

    `docs/worker.md` gives the reason: a forked child inherits the parent's
    asyncio loop, its AMQP socket, and its Postgres pool, in a state no library
    promises to survive duplication. None of that fails immediately — it fails
    under load, in a deployed worker, wearing a disguise. So the choice is
    checked here where it is cheap.
    """
    assert process_module._CONTEXT.get_start_method() == "spawn"


async def test_the_solve_happens_in_another_process():
    outcome = await outcome_of(a_request())

    assert isinstance(outcome, Solved)
    assert outcome.content_type == "application/json"
    assert json.loads(outcome.data)["pid"] != os.getpid()


async def test_the_event_loop_keeps_scheduling_while_the_child_burns():
    """The heartbeat test, without a broker in it.

    `worker.md` asks for a burn longer than two heartbeat intervals and an
    assertion that the connection survived. The connection is step 3's; what is
    falsifiable here is the property underneath it — that this loop is still
    running other tasks while a CPU is held flat — and it is falsifiable in a
    second rather than in two minutes.

    A burn, deliberately, not a sleep: a `time.sleep` in a child would leave
    this loop free too, so only real CPU held in another process can tell a
    `spawn` apart from an executor.
    """
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    beat = asyncio.create_task(ticker())
    started = time.monotonic()
    try:
        outcome = await outcome_of(a_request(burn_seconds=0.8))
    finally:
        beat.cancel()

    elapsed = time.monotonic() - started
    assert isinstance(outcome, Solved)
    assert elapsed >= 0.8, "the child returned before it could have burned"
    # A free loop reaches ~80 ticks in 0.8s. Asserting a quarter of that leaves
    # room for a loaded machine while still failing outright on a loop that was
    # blocked, which would score close to zero.
    assert ticks >= 20, f"the event loop stalled during the solve: {ticks} ticks in {elapsed:.2f}s"


async def test_an_artifact_larger_than_the_pipe_buffer_arrives_whole():
    """Bigger than the ~64KB a pipe holds before a write blocks.

    The size at which the naive parent-side read deadlocks: the child blocks
    mid-`send` waiting for someone to drain, and a parent that joins before it
    reads waits for a child that is waiting for it. Nothing about the small case
    reveals that, which is why this is its own test with a number in it.
    """
    outcome = await outcome_of(a_request(padding_bytes=200_000))

    assert isinstance(outcome, Solved)
    assert len(outcome.data) > 200_000
    assert json.loads(outcome.data)["padding"] == "x" * 200_000


async def test_an_unknown_kind_is_permanent_rather_than_a_crash():
    """The fact the supervisor needs; the policy stays open.

    `worker.md` leaves "is an unknown kind dead immediately or retried" open,
    and this is deliberately only the input to that decision. What the child
    owes is the observation that no retry will introduce a solver — burning
    three leases to rediscover it is the cost of not reporting it.
    """
    outcome = await outcome_of(SolveRequest(kind="does-not-exist", payload={}))

    assert isinstance(outcome, Failed)
    assert outcome.permanent
    assert "does-not-exist" in outcome.error


async def test_a_solver_that_raises_comes_back_with_its_traceback():
    outcome = await outcome_of(a_request(raise_error="the matrix was singular"))

    assert isinstance(outcome, Failed)
    # Not permanent: this one may well succeed on another box or another day,
    # which is exactly the distinction `permanent` exists to draw.
    assert not outcome.permanent
    assert "the matrix was singular" in outcome.error
    # Formatted in the child, because a traceback object does not survive the
    # pickle and a parent holding only the message cannot locate the failure.
    assert "worker/solvers.py" in outcome.traceback


async def test_a_child_that_dies_without_reporting_is_not_a_silent_success():
    """`os._exit` in the solver: no flush, no farewell, closed pipe.

    The supervisor must read this as `Died` and never as an empty `Solved`. A
    parent that treated a closed pipe as "nothing to report, so nothing went
    wrong" would ack a job whose artifact does not exist, which is precisely the
    dangling pointer `worker.md` orders write-then-ack to prevent.
    """
    outcome = await outcome_of(a_request(crash=True))

    assert isinstance(outcome, Died)
    assert outcome.exitcode == 9
    assert outcome.signal is None  # exited under its own power, however abruptly
    assert "status 9" in outcome.error


async def test_sigterm_stops_a_solver_that_has_no_opinion_about_it():
    """Layer 2: the child is signalled, and the default disposition ends it."""
    proc = SolveProcess.start(a_request(burn_seconds=60))
    outcome = await asyncio.wait_for(proc.stop(grace=PATIENT), PATIENT.total_seconds())

    assert isinstance(outcome, Died)
    assert outcome.signal is signal.SIGTERM


async def test_a_solver_that_ignores_sigterm_is_killed_anyway():
    """Layer 3, and the only test here that proves the process boundary earns itself.

    The solver installs `SIG_IGN` and burns for a minute — standing in for the
    long call inside numpy that checks nothing, which `worker.md` insists is the
    case a cooperative mock cannot represent. Layer 1's flag is not consulted,
    layer 2's SIGTERM is discarded, and the job still ends, because the point of
    a separate process is that SIGKILL always works.
    """
    proc = SolveProcess.start(a_request(burn_seconds=60, ignore_sigterm=True))
    # The handler has to be installed before the signal arrives, or this test
    # quietly becomes a second copy of the one above.
    await asyncio.sleep(SETTLE.total_seconds())

    started = time.monotonic()
    outcome = await asyncio.wait_for(proc.stop(grace=BRIEF_GRACE), PATIENT.total_seconds())
    elapsed = time.monotonic() - started

    assert isinstance(outcome, Died)
    assert outcome.signal is signal.SIGKILL
    assert "SIGKILL" in outcome.error
    # It survived the grace rather than dying to the SIGTERM, which is the half
    # of this that distinguishes layer 3 from layer 2.
    assert elapsed >= BRIEF_GRACE.total_seconds()
    # And it did not survive much past it: the escalation is prompt, not a
    # second timeout stacked on the first.
    assert elapsed < 5


async def test_stopping_a_child_that_already_finished_returns_its_real_outcome():
    """A cancel that loses its race must not manufacture a `Died`.

    The supervisor's shape is to race the solve against its extend loop, so
    `stop` arriving just after a child reported is ordinary rather than
    exceptional — and reporting that job as cancelled would throw away an
    artifact that exists.
    """
    proc = SolveProcess.start(a_request())
    first = await asyncio.wait_for(proc.outcome(), PATIENT.total_seconds())
    second = await asyncio.wait_for(proc.stop(grace=BRIEF_GRACE), PATIENT.total_seconds())

    assert isinstance(first, Solved)
    assert second == first


async def test_the_outcome_is_collected_once_however_often_it_is_awaited():
    """Two awaits, one `waitpid`.

    Not a convenience: `stop` and `outcome` are routinely outstanding at the
    same moment, and two threads reaping one child is a race whose loser reads a
    process that no longer exists.
    """
    proc = SolveProcess.start(a_request())
    try:
        both = await asyncio.wait_for(
            asyncio.gather(proc.outcome(), proc.outcome()), PATIENT.total_seconds()
        )
    finally:
        await proc.aclose()

    assert both[0] == both[1]
    assert isinstance(both[0], Solved)


async def test_a_cancelled_await_does_not_abandon_the_child():
    """The collector is shielded, so losing a race does not leak a process.

    The supervisor races this solve against an extend loop and will cancel one
    side. If that cancellation tore down the collector, the child would be left
    running with nobody to reap it — a CPU held by work whose lease is already
    someone else's.
    """
    proc = SolveProcess.start(a_request(burn_seconds=0.5))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(proc.outcome(), 0.05)

    outcome = await asyncio.wait_for(proc.outcome(), PATIENT.total_seconds())
    assert isinstance(outcome, Solved)


async def test_a_sleeping_solve_is_awaited_the_same_as_a_burning_one():
    """`sleep_seconds` is the I/O-bound shape, and the extend loop cannot tell.

    Worth one test because step 3's heartbeat must be driven by the clock rather
    than by anything the child is doing; a supervisor that only ever extended
    while a CPU was busy would strand exactly this job.
    """
    started = time.monotonic()
    outcome = await outcome_of(a_request(sleep_seconds=0.3))

    assert isinstance(outcome, Solved)
    assert time.monotonic() - started >= 0.3
    assert json.loads(outcome.data)["slept_seconds"] == 0.3


def test_the_child_module_imports_no_supervisor_machinery():
    """The child's import surface, enforced rather than remembered.

    `spawn` re-imports whatever the target needs in a process that holds no
    lease, so an import reachable from `worker/child.py` is code that can act on
    the system while its claim is void. The rule is the same one
    `test_layering.py` keeps for `core/`, applied to the other boundary this
    phase introduces.
    """
    source = WORKER / "child.py"
    roots = set()
    for node in ast.walk(ast.parse(source.read_text())):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])

    banned = {"asyncio", "aio_pika", "aiobotocore", "botocore", "sqlalchemy", "adapters", "api"}
    assert not (roots & banned), (
        f"worker/child.py imports {sorted(roots & banned)}. It runs in a process "
        f"that holds no lease and must not be able to reach the broker, the "
        f"database, or the object store."
    )


def _guards_main(source: str) -> bool:
    """True if the module has a top-level `if __name__ == "__main__":`.

    By AST rather than by substring, so that quoting style, spacing, or a
    comment mentioning the idiom cannot make an unguarded module look guarded.
    """
    for node in ast.parse(source).body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__"
        ):
            return True
    return False


def test_a_worker_entrypoint_guards_its_main():
    """Anything runnable under `worker/` must guard its main, because `spawn` re-imports it.

    The child reconstructs its target by importing the parent's `__main__`
    module. Unguarded, that import re-runs whatever started the process, which
    starts another process, which imports again — so `multiprocessing` refuses
    outright with "an attempt has been made to start a new process before the
    current process has finished its bootstrapping phase".

    Written before there is an entrypoint to check, and skipping until there is,
    because of *when* the unguarded version breaks. Not at import, not at
    startup, not under any test that does not spawn: it breaks the first time a
    job is actually solved, in a worker that has been sitting there looking
    healthy since it rolled out. That is far too late to find out, and far too
    easy to omit — this suite's own demo of the failure was written by omitting
    it.
    """
    entrypoints = [p for p in WORKER.glob("*.py") if p.name == "__main__.py"]
    if not entrypoints:
        pytest.skip("worker/ has no entrypoint yet — step 3's supervisor adds it")

    unguarded = [p.name for p in entrypoints if not _guards_main(p.read_text())]
    assert not unguarded, (
        f"{unguarded} starts a process without an `if __name__ == '__main__':` "
        f"guard. Under `spawn` the child re-imports this module, so an unguarded "
        f"entrypoint recurses until multiprocessing refuses — and it does so on "
        f"the first solve, not at startup."
    )
