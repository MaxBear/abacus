"""The supervisor's handle on one solve child: start it, await it, kill it.

This is the parent half of the boundary `docs/worker.md` draws. Everything here
runs on the event loop that also holds the AMQP heartbeat and the lease, so the
single rule this module exists to keep is that **nothing in it blocks for longer
than a syscall**. A five-minute solve is awaited, never waited on.

`spawn`, never `fork`, and the context is module-level so that it is one
decision rather than one per call site. `worker.md` settles it: a forked child
inherits the parent's asyncio loop, its open AMQP socket, and its Postgres pool,
in a state no library promises to survive duplication — and the child needs none
of the three.

**Why the result crosses a pipe in a thread.** The child sends one pickled
`Outcome`, which for a successful solve contains the artifact itself. A pipe
holds about 64KB before a write blocks, so any artifact larger than that arrives
in pieces and `Connection.recv()` blocks between them — on the event loop, that
is the exact stall this phase is built to avoid. Reading it on a worker thread
costs one parked thread per concurrent solve, and `prefetch=1` makes that number
one. The alternative, `add_reader` on the pipe's fd, is event-driven right up
until a partial message arrives and `recv()` blocks anyway, which is the same
bug with more machinery in front of it.

**Whatever runs this needs `if __name__ == "__main__":`.** Not this module — it
is a library and is never `__main__` — but the entrypoint that constructs a
supervisor, which is step 3's to write. `spawn` reconstructs the child's target
by importing the parent's `__main__`, so an unguarded entrypoint re-runs its own
startup in the child, which spawns again, until `multiprocessing` refuses. The
reason it is worth a paragraph rather than a footnote is when it surfaces: not
at import, not at startup, but on the first solve, in a worker that has looked
healthy since it rolled out. `test_a_worker_entrypoint_guards_its_main` fails
the moment `worker/__main__.py` appears without one.
"""

import asyncio
import logging
import multiprocessing
from datetime import timedelta

from worker.child import run_child
from worker.solve import Died, Outcome, SolveRequest

log = logging.getLogger(__name__)

# One context, one start method, asserted by the suite. A regression to `fork`
# would pass most tests and fail in production under load, which is why it is a
# named module constant rather than a string at a call site.
_CONTEXT = multiprocessing.get_context("spawn")

# How long `stop` waits between SIGTERM and SIGKILL — `worker.md`'s layer 2
# before its layer 3. Long enough for a solver that installed a handler to
# finish what it is doing, short enough that a cancelling user is not watching a
# spinner while a process that will never yield is asked politely. The
# supervisor will pass its own from `Settings` in step 3; this is the default so
# that step 2 is testable without configuration.
DEFAULT_GRACE = timedelta(seconds=5)

# How long the collector waits for an already-reported child to exit before
# reaping it outright. The child's only remaining work after `send` is to return
# from `run_child`, so a child still alive here is wedged in a way no further
# patience resolves — a non-daemon thread a solver started, most likely.
_REAP_AFTER = timedelta(seconds=5)


class SolveProcess:
    """One `spawn`ed solve, from `start` to an `Outcome`.

    Single-use. A second solve is a second process, because the whole value of
    the boundary is that a killed child leaves nothing behind to reset.
    """

    def __init__(self, process: multiprocessing.process.BaseProcess, conn) -> None:
        self._process = process
        self._conn = conn
        # The one collector. Created lazily and shared, so that `outcome()` and
        # `stop()` waiting at the same time is two awaits on one result rather
        # than two threads calling `waitpid` on one child.
        self._collector: asyncio.Task[Outcome] | None = None

    @classmethod
    def start(cls, request: SolveRequest) -> "SolveProcess":
        """Spawn the child. Returns as soon as it is started, never on completion.

        Synchronous on purpose. There is nothing to await — `Process.start()`
        forks and execs and returns — and making it `async` would suggest the
        expensive part happens here rather than in `outcome()`.
        """
        # `duplex=False` gives a one-way pipe and, more usefully, two distinct
        # ends: the child cannot read, so a solver cannot invent a protocol.
        recv_end, send_end = _CONTEXT.Pipe(duplex=False)
        process = _CONTEXT.Process(
            target=run_child,
            args=(request, send_end),
            name=f"solve-{request.job_id}",
            # Daemonic, so a supervisor that dies takes its children with it
            # rather than leaving an orphan burning a CPU with no lease behind
            # it and nobody to write its result.
            daemon=True,
        )
        process.start()
        # The parent's copy of the write end, closed immediately and not
        # optionally. While it is open the pipe has a writer, so the child's
        # death never produces EOF and the collector waits forever on a process
        # that is already gone.
        send_end.close()
        log.info("spawned solve child pid=%s job=%s", process.pid, request.job_id)
        return cls(process, recv_end)

    @property
    def pid(self) -> int | None:
        return self._process.pid

    async def outcome(self) -> Outcome:
        """Await what the child reported, or `Died` if it reported nothing.

        Safe to await more than once and safe to have its await cancelled: the
        collector is shielded, so a cancelled caller leaves the child still being
        reaped rather than abandoning a process and a thread. That matters
        because the supervisor's normal shape is to race this against an extend
        loop, and losing that race must not leak a solve.
        """
        return await asyncio.shield(self._collect())

    async def stop(self, *, grace: timedelta = DEFAULT_GRACE) -> Outcome:
        """SIGTERM, then SIGKILL after `grace`. Returns once the child is gone.

        `worker.md`'s layers 2 and 3, and the reason they are one method: layer 3
        exists only because layer 2 is insufficient, and a caller that had to
        remember to escalate would be a caller that sometimes did not. A solver
        that wants to handle SIGTERM gets `grace` to do it in; one that ignores
        it — `solvers.synthetic` can, on purpose — reaches SIGKILL, which is the
        point of a separate process.

        Idempotent, and safe on a child that already exited: `terminate` on a
        reaped process is a no-op, so cancelling a solve that just finished
        returns its real outcome rather than racing it.
        """
        if self._process.exitcode is None:
            log.info("terminating solve child pid=%s", self._process.pid)
            self._process.terminate()

        collector = self._collect()
        try:
            return await asyncio.wait_for(asyncio.shield(collector), grace.total_seconds())
        except TimeoutError:
            log.warning("solve child pid=%s ignored SIGTERM; killing", self._process.pid)
            self._process.kill()
            return await asyncio.shield(collector)

    async def aclose(self) -> None:
        """Ensure the child is gone and the pipe is closed. Cheap if it already is."""
        await self.stop()

    def _collect(self) -> asyncio.Task[Outcome]:
        """The single task that reads the pipe and reaps the process."""
        if self._collector is None:
            self._collector = asyncio.create_task(asyncio.to_thread(self._read_and_reap))
        return self._collector

    def _read_and_reap(self) -> Outcome:
        """Runs on a worker thread. The only place this child is ever waited on."""
        try:
            outcome = self._conn.recv()
        except EOFError:
            # The write end closed with nothing on it: the child died before it
            # could report. Expected after a SIGKILL, and the reason `Died`
            # exists as an outcome rather than an error.
            outcome = None
        finally:
            self._conn.close()

        self._process.join(_REAP_AFTER.total_seconds())
        if self._process.exitcode is None:
            log.warning("solve child pid=%s outlived its report; killing", self._process.pid)
            self._process.kill()
            self._process.join()

        if outcome is None:
            # `exitcode` cannot be None here — the join above guarantees it —
            # but a defaulted 0 keeps a wedged-then-killed child from crashing
            # the supervisor on the way to reporting the failure.
            return Died(exitcode=self._process.exitcode or 0)
        return outcome
