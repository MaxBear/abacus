"""The supervisor on real infrastructure: the two things a memory pair cannot prove.

`test_supervisor.py` covers every decision the supervisor makes, because it takes
two Protocols and never learns which implementations it has. What it cannot cover
is what those implementations *do* — and two of this phase's claims are entirely
about that:

- **A solve outlives its lease only because something renewed it.** Against
  `MemoryJobQueue` a missing heartbeat is invisible, since fencing is on
  `lease_id` alone and an `ack` on a lapsed-but-unclaimed lease still succeeds.
  The proof has to be the row's `lease_expires_at` moving, read from Postgres
  while the child burns.
- **The artifact is really in a bucket.** A dict cannot fail to store bytes. MinIO
  can, and `result_ref` is worth nothing if what it names is not readable.

Both skip themselves when the stack is down — `make up && make migrate` turns
them on, with no flag to remember. Every wait is bounded, for the reason this
suite keeps rediscovering.
"""

import asyncio
import json
import time
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from adapters.postgres.db import Database
from adapters.postgres.tables import jobs
from core.config import get_settings
from core.jobs import JobRequest, JobState
from worker.config import WorkerConfig
from worker.solve import Solved
from worker.supervisor import Ending, Supervisor

pytestmark = [pytest.mark.rabbitmq, pytest.mark.s3]

# Generous against what a healthy run costs — a `spawn`, a burn, and a handful of
# round trips to two services. Large enough that a slow CI box is not a failure,
# small enough that a hang is reported as one.
PATIENT = timedelta(seconds=60)


def a_config(**overrides) -> WorkerConfig:
    """Production ratios, collapsed. The heartbeat stays well inside the lease."""
    return WorkerConfig(
        **{
            "lease": timedelta(seconds=2),
            "extend_every": timedelta(milliseconds=300),
            "solve_timeout": timedelta(seconds=30),
            "grace": timedelta(milliseconds=500),
            "reserve_wait": timedelta(seconds=2),
            "retry_in": timedelta(),
            **overrides,
        }
    )


async def _deadline(db: Database, job_id) -> datetime | None:
    """The row's `lease_expires_at`, which no Protocol exposes.

    Read straight from the table on purpose. `Job` deliberately carries no lease
    deadline — a consumer holds that in its `Lease` — so the only way to assert
    that a heartbeat reached Postgres is to look at what the heartbeat writes.
    """
    async with db.session() as s:
        stmt = select(jobs.c.lease_expires_at).where(jobs.c.id == job_id)
        return (await s.execute(stmt)).scalar_one_or_none()


async def _claimed(db: Database, job_id) -> datetime:
    """Wait until the supervisor has actually taken the job, then give its deadline."""
    limit = time.monotonic() + PATIENT.total_seconds()
    while time.monotonic() < limit:
        if (at := await _deadline(db, job_id)) is not None:
            return at
        await asyncio.sleep(0.02)
    raise AssertionError("the supervisor never claimed the job")


async def test_a_job_taken_from_the_broker_lands_in_the_bucket(rabbitmq_queue, store, written_keys):
    """Enqueue on RabbitMQ, solve in a child, read the artifact back out of MinIO.

    The end-to-end shape of everything this phase built, with no fakes in it: a
    real message, a real lease, a real `spawn`, a real `PUT`, and a `result_ref`
    that resolves to the bytes the child produced.
    """
    worker = Supervisor(queue=rabbitmq_queue, store=store, config=a_config(), owner="live-test")
    job = await rabbitmq_queue.enqueue(
        JobRequest(kind="synthetic", payload={"padding_bytes": 1024})
    )

    handled = await asyncio.wait_for(worker.run_once(), PATIENT.total_seconds())

    assert handled is not None
    # Registered before the assertions, so a failure below still cleans up: the
    # bucket is shared across runs and nothing lists it, so a leaked object is
    # one no later run could even find.
    written_keys.append(f"jobs/{job.id}/{handled.lease.id}")

    assert handled.ending is Ending.COMPLETED
    assert isinstance(handled.outcome, Solved)

    row = await rabbitmq_queue.get(job.id)
    assert row.state is JobState.DONE
    assert row.result_ref == written_keys[-1]

    # The pointer is worth nothing if what it names is not there. This is the
    # half `MemoryObjectStore` cannot fail at.
    body = json.loads(await store.get(row.result_ref))
    assert body["solver"] == "synthetic"
    assert len(body["padding"]) == 1024


async def test_the_lease_is_extended_while_a_real_child_burns(rabbitmq_queue, store, written_keys):
    """`docs/worker.md`'s heartbeat test, with the row as the witness.

    A three-second burn under a two-second lease: it can only reach `done` if
    something moved the deadline while a CPU was held flat in another process.
    Asserting on `lease_expires_at` rather than on the final state is deliberate
    — fencing is on `lease_id` alone, so an `ack` against a lapsed lease nobody
    else claimed still succeeds, and a `done` row would prove nothing.

    This is the test that fails the moment someone simplifies the child back into
    `run_in_executor`: the extend loop would stop being scheduled, the lease
    would lapse, and the connection would go with it.
    """
    worker = Supervisor(queue=rabbitmq_queue, store=store, config=a_config(), owner="live-test")
    job = await rabbitmq_queue.enqueue(JobRequest(kind="synthetic", payload={"burn_seconds": 3.0}))

    db = Database(get_settings())
    try:
        turn = asyncio.ensure_future(worker.run_once())
        first = await _claimed(db, job.id)
        # Several beats, well inside the burn, so both samples are taken while
        # the child is still holding a CPU.
        await asyncio.sleep(1.0)
        second = await _deadline(db, job.id)
        handled = await asyncio.wait_for(turn, PATIENT.total_seconds())
    finally:
        await db.dispose()

    written_keys.append(f"jobs/{job.id}/{handled.lease.id}")

    assert second is not None, "the lease was released while the child was still burning"
    assert second > first, "the deadline never moved: nothing extended the lease"

    row = await rabbitmq_queue.get(job.id)
    assert row.state is JobState.DONE
    # One attempt: the job was never redelivered, so no second worker was ever
    # offered the work this one was doing.
    assert row.attempts == 1
    # And the AMQP connection survived a CPU-bound solve, which is the property
    # the whole two-process design exists to buy.
    assert not rabbitmq_queue._connection.is_closed
