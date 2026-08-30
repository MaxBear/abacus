"""Put one job on the real queue and watch what a worker does with it.

The producer side of `dev/solve.py`, and the piece that makes step 3 testable by
hand: nothing in `api/` enqueues yet — the wire from a conversation to a job is
phase 4's — so without this there is no way to give a running worker something to
do except by writing a script every time.

    make up && make migrate                  # postgres, rabbitmq, minio, worker
    docker compose logs -f worker            # in another terminal

    uv run python -m dev.enqueue                                  # instant solve
    uv run python -m dev.enqueue '{"burn_seconds": 20}'           # hold a CPU
    uv run python -m dev.enqueue '{"raise_error": "boom"}'        # -> failed, retried
    uv run python -m dev.enqueue --kind nope '{}'                 # -> unknown kind
    uv run python -m dev.enqueue --cancel '{"burn_seconds": 60, "ignore_sigterm": true}'

That last one is the interesting one, and the reason `--cancel` exists here
rather than as a second script. It is `docs/worker.md`'s cancellation ladder end
to end against real infrastructure: the flag reaches the row, the supervisor
learns it on its next `extend`, the child ignores SIGTERM, and the grace period
expires into a SIGKILL. The worker's log is where you watch it happen; this
process only reports what the row says afterwards.

The payload is `worker.solvers.synthetic`'s, verbatim — its docstring is the
list of knobs.

**Reads the same `Settings` as everything else**, which from the host means the
`.env` pointing at `localhost`. A worker inside compose reaches the same broker
and the same database by their service names, so both sides agree on the row
without agreeing on the URL.

`-m` rather than a path, so the repo root lands on `sys.path`; run it from the
repo root.
"""

import argparse
import asyncio
import json
import sys
import time
import uuid

from adapters.postgres.db import Database
from adapters.postgres.job_store import PostgresJobStore
from adapters.rabbitmq.job_queue import RabbitMQJobQueue
from core.config import get_settings
from core.jobs import Job, JobRequest, JobState

# How long to follow a job before giving up on it. Longer than the default
# `worker_solve_timeout_seconds` would be pointless — past its own cap the
# worker fails the job itself — and this is well short of that so a wedged
# stack ends the script rather than the afternoon.
FOLLOW_SECONDS = 300.0

# How long to let the worker pick the job up before asking for a cancellation.
# `--cancel` is meant to exercise the *running* half of `JobQueue.cancel`, and
# cancelling a job still sitting in the queue takes the other path entirely —
# a state transition with no consumer and no signal in it.
BEFORE_CANCEL_SECONDS = 3.0

# How often to re-read the row while following. Frequent enough to see
# `running` before a short solve is over, cheap enough to leave running.
POLL_SECONDS = 0.5


async def main(args: argparse.Namespace) -> int:
    settings = get_settings()
    db = Database(settings)

    # A full `RabbitMQJobQueue`, not a bare publisher, because `enqueue` is a
    # dual write: the row first, then the message, then `published_at`. Anything
    # that only published would leave rows this script created looking orphaned
    # to the worker's sweep.
    #
    # Note what this also does, and what phase 5 will have to decide about:
    # `start()` registers a *consumer* and runs a maintenance loop. Here that is
    # harmless — this process exits in a moment, and its unacked deliveries go
    # back to the broker when the connection drops — but an API replica doing
    # the same would quietly take a share of the work queue and never reserve
    # any of it.
    queue = await RabbitMQJobQueue.start(store=PostgresJobStore(db), url=settings.broker_url)
    try:
        job = await queue.enqueue(_request(args))
        print(f"enqueued {job.id}  kind={job.kind}  payload={json.dumps(job.payload)}")
        print(f"watch it: docker compose logs -f worker   (or grep for {job.id})")

        if args.cancel:
            await _wait_until_running(queue, job.id)
            print(f"\ncancelling {job.id}")
            cancelled = await queue.cancel(job.id)
            # `cancel_requested` rather than `cancelled` is the interesting
            # answer: it means a consumer holds the job and has to be told,
            # which is the half that needs a signal rather than a transition.
            print(f"  -> state={cancelled.state}  cancel_requested={cancelled.cancel_requested}")

        final = await _follow(queue, job.id)
    finally:
        await queue.close()
        await db.dispose()

    print(f"\n{final.state.upper()}  attempts={final.attempts}/{final.max_attempts}")
    if final.result_ref:
        print(f"  artifact  {final.result_ref}")
        bucket = get_settings().s3_bucket
        print(f"  read it:  docker compose exec minio mc cat local/{bucket}/{final.result_ref}")
    if final.error:
        print(f"  error     {final.error}")

    # A non-zero exit for anything that is not a completed job, so this is
    # usable from a shell script — `verify-phase0.sh`'s habit.
    return 0 if final.state is JobState.DONE else 1


def _request(args: argparse.Namespace) -> JobRequest:
    return JobRequest(
        kind=args.kind,
        payload=args.payload,
        max_attempts=args.max_attempts,
        # A fresh key every run, so repeated invocations are repeated jobs.
        # Supplying one by hand is how you would demonstrate the opposite:
        # two runs with the same `--idempotency-key` return one job id.
        idempotency_key=args.idempotency_key or str(uuid.uuid4()),
    )


async def _wait_until_running(queue, job_id: uuid.UUID) -> None:
    """Give a worker time to claim it, so `--cancel` exercises the hard half."""
    deadline = time.monotonic() + BEFORE_CANCEL_SECONDS
    while time.monotonic() < deadline:
        job = await queue.get(job_id)
        if job is not None and job.state is JobState.RUNNING:
            return
        await asyncio.sleep(POLL_SECONDS)
    print(
        f"  (nobody claimed it within {BEFORE_CANCEL_SECONDS:.0f}s — is a worker running? "
        f"cancelling anyway, which takes the unreserved path)"
    )


async def _follow(queue, job_id: uuid.UUID) -> Job:
    """Print each state change until the job is terminal, or until patience runs out."""
    deadline = time.monotonic() + FOLLOW_SECONDS
    last: JobState | None = None
    while time.monotonic() < deadline:
        job = await queue.get(job_id)
        if job is None:
            raise SystemExit(f"job {job_id} vanished from the jobs table")
        if job.state is not last:
            print(f"  {time.strftime('%H:%M:%S')}  {job.state}")
            last = job.state
        if job.state.terminal:
            return job
        await asyncio.sleep(POLL_SECONDS)
    raise SystemExit(
        f"job {job_id} was still {last} after {FOLLOW_SECONDS:.0f}s — "
        f"check that a worker is up: docker compose ps worker"
    )


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m dev.enqueue", description=__doc__.splitlines()[0]
    )
    parser.add_argument(
        "payload",
        nargs="?",
        default="{}",
        help="JSON for worker.solvers.synthetic (burn_seconds, sleep_seconds, "
        "padding_bytes, raise_error, crash, ignore_sigterm)",
    )
    parser.add_argument("--kind", default="synthetic", help="solver to select (default: synthetic)")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument(
        "--idempotency-key",
        default=None,
        help="reuse across runs to prove enqueue is idempotent: both return one job id",
    )
    parser.add_argument(
        "--cancel",
        action="store_true",
        help=f"cancel the job once a worker holds it (~{BEFORE_CANCEL_SECONDS:.0f}s in)",
    )
    args = parser.parse_args(argv)
    args.payload = json.loads(args.payload)
    return args


if __name__ == "__main__":
    sys.exit(asyncio.run(main(_parse(sys.argv[1:]))))
