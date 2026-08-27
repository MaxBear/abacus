"""The registry of one, and the synthetic solver phase 3 tests against.

`docs/worker.md` leaves what `kind` selects to phase 4 and says the worker
"needs a registry of one" until then. This is that registry, plus the one entry:
a solver whose cost is an *input* rather than a property of real work, so every
test in this phase can state the duration it needs instead of discovering it.

A solver is synchronous and knows nothing about processes, pipes, leases, or
object storage. It takes a payload and returns bytes. That narrowness is what
makes it the same shape phase 4's real analysis will be, and it is why the
interesting parts of this phase — signals, redelivery, the write/ack order —
are all testable against something that computes nothing.

**The synthetic solver can misbehave on purpose.** `worker.md` is specific that a
cancellation suite proving only layer 1 has proved nothing, because a
cooperative mock cancels trivially and real numpy does not. So `ignore_sigterm`
and `crash` exist to make this solver the adversary those tests need: one that
refuses to die politely, and one that dies without a word. A knob that only ever
behaves is a knob that tests the easy half.
"""

import json
import os
import signal
import time
from collections.abc import Callable
from typing import Any

from worker.solve import Solved, UnknownKind

# A solver: payload in, artifact out. Sync, because it runs in the child, and
# the child exists precisely so that something may block for five minutes.
Solver = Callable[[dict[str, Any]], Solved]

# How many iterations of arithmetic run between two clock readings. Large enough
# that `burn` is dominated by the work rather than by `monotonic()`, small enough
# that the loop notices its deadline promptly.
_BURN_BATCH = 10_000


def synthetic(payload: dict[str, Any]) -> Solved:
    """Consume a stated amount of time, then return a stated number of bytes.

    Every knob defaults to the cheapest possible behaviour, so a bare
    `{"kind": "synthetic", "payload": {}}` is a solve that finishes immediately —
    the right default for a test that cares about the surrounding machinery and
    not about duration.

    - `sleep_seconds` — wall time spent not computing. The lease must survive it
      the same as a burn, which is what makes it worth having separately: it is
      the shape of an I/O-bound solve, and it proves the supervisor's extend loop
      is timing-driven rather than accidentally coupled to CPU.
    - `burn_seconds` — wall time spent holding a CPU. `worker.md`'s heartbeat
      test needs this specifically; a `sleep` in a child would keep an event loop
      alive too, so only a burn can falsify "someone simplified this back into an
      executor".
    - `padding_bytes` — how large the artifact is, for the one thing that
      genuinely changes behaviour at size: the result crosses a pipe.
    - `ignore_sigterm` — install `SIG_IGN` and become layer 3's problem.
    - `raise_error` — fail with this message instead of succeeding.
    - `crash` — leave immediately by `os._exit`, reporting nothing at all.
    """
    if payload.get("ignore_sigterm"):
        # A solver that "wants a handler" per `worker.md`'s layer 2, playing the
        # part of a long call inside a C extension that checks nothing. The
        # supervisor's grace period expiring into a SIGKILL is the only thing
        # that ends this process, which is exactly what layer 3 claims.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    if payload.get("crash"):
        # `os._exit`, not `sys.exit`: no cleanup, no atexit, and critically no
        # flush of the pipe. This is a segfaulting BLAS call's observable
        # behaviour, and the supervisor should read it as `Died`.
        os._exit(9)

    started = time.monotonic()
    if sleep_seconds := float(payload.get("sleep_seconds", 0.0)):
        time.sleep(sleep_seconds)
    if burn_seconds := float(payload.get("burn_seconds", 0.0)):
        _burn(burn_seconds)

    if error := payload.get("raise_error"):
        raise RuntimeError(str(error))

    body = {
        "solver": "synthetic",
        "pid": os.getpid(),
        "elapsed_seconds": round(time.monotonic() - started, 4),
        "slept_seconds": sleep_seconds,
        "burned_seconds": burn_seconds,
        # Padding rather than a random blob, so a size assertion reads as a
        # size assertion and a failure diff stays legible.
        "padding": "x" * int(payload.get("padding_bytes", 0)),
    }
    return Solved(data=json.dumps(body).encode(), content_type="application/json")


def _burn(seconds: float) -> None:
    """Hold a CPU for `seconds`. Not a sleep, and the difference is the point."""
    deadline = time.monotonic() + seconds
    x = 0
    while time.monotonic() < deadline:
        for _ in range(_BURN_BATCH):
            x = (x * 31 + 7) % 1_000_003


# Phase 4 replaces this with something that maps a `kind` to a real analysis.
# Until then one entry, and a lookup that fails loudly rather than defaulting:
# a registry that silently substitutes a no-op for an unrecognised kind would
# report `done` for work nobody performed.
_SOLVERS: dict[str, Solver] = {"synthetic": synthetic}


def resolve(kind: str) -> Solver:
    """The solver for `kind`, or `UnknownKind`.

    Called in the child rather than the supervisor, deliberately. The supervisor
    could check the registry before spawning and fail a job a whole process
    earlier — but then the registry would have to be identical in two processes
    that phase 6 may well deploy from two images, and a job's fate would depend
    on which one looked. The child is the process that would have to run it, so
    the child is the authority on whether it can.
    """
    try:
        return _SOLVERS[kind]
    except KeyError:
        raise UnknownKind(f"no solver registered for kind {kind!r}") from None
