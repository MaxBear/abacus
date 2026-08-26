"""The supervisor/child contract: what crosses a process boundary, and nothing else.

`docs/worker.md` makes the boundary the phase's central decision — the event loop
must keep scheduling while a solve runs, and a solve must be killable, and one
`spawn`ed process buys both. This module is that boundary's vocabulary. It is
pure data with no imports from `adapters/`, no client, and no event loop, because
both sides import it and only one of them has any of those.

**The child does not write the artifact.** It returns bytes and a content type;
the supervisor writes them and then acks, in that order. `worker.md:25` reads the
other way — "computes, writes its artifact" — and the work order at `:199` is the
one this follows, because it puts `write` in the supervisor's loop between the
extend loop and the `ack`. Two things settle it beyond the prose. The only
`ObjectStore` is `aiobotocore`-backed and therefore async, which a `spawn`ed
child computing inside numpy has no loop to drive; and a child that wrote would
need its own S3 credentials, which is a permission this design otherwise never
grants it. The cost is that an artifact crosses a pipe — see `process.py`, which
is where that is paid for.

Everything here is picklable, and that is a requirement rather than an
observation: `spawn` pickles the target's arguments and the child pickles its
outcome back. A field that is not picklable fails at the boundary, at runtime,
in the child, which is the worst place in this system to learn anything.
"""

import signal
import uuid
from dataclasses import dataclass, field
from typing import Any


class UnknownKind(Exception):
    """No solver is registered for that `kind`.

    Distinct from an ordinary solver failure because the retry answer differs
    and only this type carries it: an unknown kind will never succeed, so the
    three attempts `max_attempts` allows would burn three leases' worth of time
    to reach the answer the first one already had. `docs/worker.md` leaves the
    policy open under "What `kind` selects"; this exception is the fact the
    supervisor needs to decide it, reported as `Failed.permanent`.
    """


@dataclass(frozen=True, slots=True)
class SolveRequest:
    """What the supervisor hands a child. Everything it is allowed to know.

    Deliberately not a `Job`. A `Job` carries `attempts`, `state`, and a
    `result_ref` — queue bookkeeping that the child must not be able to read,
    because a solver that branches on its own attempt count is a solver whose
    redelivery is no longer the same computation. What a child gets is the
    question, never the history of asking it.
    """

    kind: str

    # Opaque to everything here; phase 4 gives it meaning. Must be picklable,
    # which in practice means the JSON-shaped dict that arrived on the wire.
    payload: dict[str, Any]

    # Correlation only, so a child's log line can be found next to the
    # supervisor's. Nothing computes with it, and the artifact key it also
    # appears in is built parent-side by `core.artifacts.artifact_key` — the
    # child never learns where its output lands, which is what keeps the write
    # a decision the lease holder makes.
    job_id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True, slots=True)
class Solved:
    """The solver returned. Bytes, and what they are.

    The content type travels with the artifact rather than being encoded in its
    key, for the reason `core.artifacts.artifact_key` gives: a key that claims
    `.json` is a claim nothing enforces, while an object's content type is one a
    reader can actually consult.
    """

    data: bytes
    content_type: str


@dataclass(frozen=True, slots=True)
class Failed:
    """The child ran and reported that the work did not succeed.

    Distinct from `Died` because the difference is the whole reason the child
    reports at all: a `Failed` carries a diagnosis the supervisor can put in
    `Job.error`, where a user eventually reads it, while a `Died` carries only
    an exit status.
    """

    error: str

    # The child's traceback, formatted there because the frames do not survive
    # the pickle. Kept separate from `error` so a log can have the whole thing
    # and the row can have the one line that fits in a UI.
    traceback: str

    # True when retrying cannot change the answer — an unknown `kind` today.
    # The supervisor decides what to do about it; this only says that waiting
    # will not help, which is a fact about the work rather than a policy.
    permanent: bool = False


@dataclass(frozen=True, slots=True)
class Died:
    """The child exited without reporting anything.

    The expected shape of a successful cancellation, not only of a crash: layers
    2 and 3 of `docs/worker.md`'s cancellation ladder work by signalling a
    process that is not listening, and a process that is not listening does not
    get to send a farewell. The supervisor knows whether it asked for this — it
    is the one that called `stop` — so this deliberately does not guess.
    """

    # `multiprocessing` convention: negative means killed by signal `-exitcode`.
    exitcode: int

    @property
    def signal(self) -> signal.Signals | None:
        """The signal that ended it, or `None` if it exited under its own power."""
        if self.exitcode >= 0:
            return None
        try:
            return signal.Signals(-self.exitcode)
        except ValueError:  # a signal number this platform does not name
            return None

    @property
    def error(self) -> str:
        """A line fit for `Job.error`, for the case where nobody asked for this."""
        if (sig := self.signal) is not None:
            return f"solve child killed by {sig.name}"
        return f"solve child exited with status {self.exitcode}"


# What `SolveProcess.outcome()` resolves to. Three cases and no fourth: the
# child reported success, the child reported failure, or the child is gone.
Outcome = Solved | Failed | Died
