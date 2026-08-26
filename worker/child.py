"""What runs in the spawned process. The whole of the child's life is here.

Kept in its own module, and small, for a reason that is structural rather than
tidy: `spawn` starts a fresh interpreter and imports whatever it needs to
reconstruct the target, so every import reachable from here is paid on every
solve — and, worse, is code that now runs in a process holding no lease. This
module imports a registry and the contract. It must never grow an import of the
supervisor, an AMQP client, or the object store, because a child that can reach
any of those is a child that can act on the system while its lease is void.

The child's contract is one message. It sends exactly one `Outcome` and exits,
or it dies without sending and the parent reads the closed pipe as `Died`. There
is no protocol beyond that — no acknowledgement, no heartbeat, no progress yet
(that is step 4's fanout) — because anything richer would need the child to
survive the supervisor deciding it should not.
"""

import logging
import signal
import traceback
from multiprocessing.connection import Connection

from worker.solve import Failed, Solved, SolveRequest, UnknownKind
from worker.solvers import resolve

log = logging.getLogger(__name__)

# Restored to their defaults on entry, not left as inherited. `spawn` execs a
# fresh interpreter, which resets handlers — but `SIG_IGN` is the exception that
# survives an exec, so a supervisor started under a process manager that ignores
# SIGTERM would hand every child an inherited immunity to layer 2. That failure
# is invisible until the day a cancellation is needed, so it is fixed here
# rather than assumed away.
_RESET = (signal.SIGTERM, signal.SIGINT)


def run_child(request: SolveRequest, conn: Connection) -> None:
    """The `spawn` target. Solve, report once, close.

    Module-level and importable by name because that is what `spawn` requires:
    the target is pickled by qualified name and re-imported in the child, so a
    closure or a bound method cannot be one.
    """
    for sig in _RESET:
        signal.signal(sig, signal.SIG_DFL)

    # `conn.close()` in a `finally`, but the `send` deliberately not: a
    # `BaseException` on the way through here means the process is being told to
    # stop, and the parent should read that as `Died` rather than as whatever
    # half-formed outcome this frame happened to be holding. Closing regardless
    # is what makes the parent's read return promptly either way, instead of
    # leaving a supervisor to wait out its wall-clock cap on a process that has
    # already gone.
    try:
        conn.send(_solve(request))
    finally:
        conn.close()


def _solve(request: SolveRequest) -> Solved | Failed:
    """Run the solver, converting anything it does into something sendable."""
    try:
        solver = resolve(request.kind)
    except UnknownKind as exc:
        # Permanent: no amount of retrying introduces a solver. The supervisor
        # owns what happens next; this only reports that waiting will not help.
        return Failed(error=str(exc), traceback="", permanent=True)

    log.info("solving job %s kind=%s", request.job_id, request.kind)
    try:
        return solver(request.payload)
    except Exception as exc:
        # `Exception`, not `BaseException`: a `KeyboardInterrupt` or a
        # `SystemExit` is the process being told to stop, and dressing that up
        # as a solver failure would report `failed` for a job that was cancelled.
        # Those propagate, the process ends, and the parent reads `Died`.
        return Failed(
            error=f"{type(exc).__name__}: {exc}",
            # Formatted here because the frames do not survive the pickle: a
            # traceback object is not picklable, and a parent that received the
            # exception alone would log the failure with no way to locate it.
            traceback=traceback.format_exc(),
        )
