"""Run one solve child by hand and print what came back.

The phase 3 counterpart to `dev/chat.html`: a thing a person runs and reads,
not a thing that asserts. `tests/test_solve_child.py` already pins the
behaviour; this exists for the times you want to see an `Outcome` rather than
learn that it matched.

    uv run python -m dev.solve                          # cheapest possible solve
    uv run python -m dev.solve '{"burn_seconds": 2}'    # hold a CPU first
    uv run python -m dev.solve '{"raise_error": "boom"}' # -> Failed
    uv run python -m dev.solve '{"crash": true}'         # -> Died
    uv run python -m dev.solve '{"padding_bytes": 200000}'  # the multi-chunk pipe

The payload is `worker.solvers.synthetic`'s, verbatim — its docstring is the
list of knobs.

`-m` rather than a path, so that the repo root lands on `sys.path` and
`worker` imports; run it from the repo root for the same reason.
"""

import asyncio
import json
import os
import sys
import uuid

from worker.process import SolveProcess
from worker.solve import Died, Failed, Solved, SolveRequest

# Long enough that a burn or a sleep the caller asked for finishes, short enough
# that a wedged child ends the script instead of the afternoon.
DEADLINE_SECONDS = 60.0


async def main(payload: dict) -> None:
    request = SolveRequest(kind="synthetic", payload=payload, job_id=uuid.uuid4())
    proc = SolveProcess.start(request)
    print(f"job {request.job_id}  parent pid {os.getpid()}  child pid {proc.pid}")

    try:
        outcome = await asyncio.wait_for(proc.outcome(), DEADLINE_SECONDS)
    finally:
        # `aclose` regardless, including after a timeout: an abandoned child
        # holds a CPU and nobody is left to reap it.
        await proc.aclose()

    match outcome:
        case Solved(data=data, content_type=content_type):
            print(f"Solved  {content_type}  {len(data)} bytes")
            # Pretty-print when it is the JSON the synthetic solver produces,
            # and stay useful when a future solver returns something else.
            try:
                print(json.dumps(json.loads(data), indent=2))
            except (UnicodeDecodeError, json.JSONDecodeError):
                print(data[:200], "..." if len(data) > 200 else "")
        case Failed(error=error, traceback=tb, permanent=permanent):
            print(f"Failed  permanent={permanent}  {error}")
            print(tb)
        case Died() as died:
            print(f"Died    exitcode={died.exitcode}  signal={died.signal}")
            print(died.error)


# `spawn` re-imports `__main__` in the child, so an unguarded script would spawn
# a solve on its way to spawning a solve. `worker/process.py`'s docstring is the
# long version.
if __name__ == "__main__":
    raw = sys.argv[1] if len(sys.argv) > 1 else "{}"
    asyncio.run(main(json.loads(raw)))
