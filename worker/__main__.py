"""The worker's composition root: `python -m worker`.

The same wheel the API is built from, started at a different entrypoint — which
is `docs/worker.md`'s point about the image: "the worker runs the same code the
API imports" is a build property here rather than a promise, because there is
one `Dockerfile` and compose overrides the command.

This module is where every dependency is chosen, exactly as `api/main.py`'s
lifespan is for the API, and for the same reason: an adapter picked at a call
site is an adapter no test can replace. `Supervisor` takes a `JobQueue` and an
`ObjectStore` and never learns that they are RabbitMQ and S3.

**The `if __name__` guard at the bottom is load-bearing.** `spawn` reconstructs
the child by importing the parent's `__main__`, so without it every solve would
start a worker, which would start a solve, until `multiprocessing` refused — and
it would do so on the first job rather than at startup, in a container that had
looked healthy since it rolled out. `test_a_worker_entrypoint_guards_its_main`
now has something to check.
"""

import asyncio
import logging
import signal

from adapters.postgres.db import Database
from adapters.postgres.job_store import PostgresJobStore
from adapters.rabbitmq.job_queue import RabbitMQJobQueue
from adapters.s3.object_store import S3ObjectStore
from core.config import get_settings
from worker.config import WorkerConfig
from worker.supervisor import Supervisor

log = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    log.info("starting worker (env=%s)", settings.env)

    db = Database(settings)
    store = await S3ObjectStore.start(settings)
    # Locally the bucket is nobody's job; in phase 6 it is Terraform's and a
    # worker that creates buckets holds a permission it should not. So the
    # composition root asks, and only the local one asks.
    if settings.env == "local":
        await store.ensure_bucket()

    # `prefetch` is left at the adapter's default of 1 deliberately — this
    # supervisor solves one job at a time, and a larger window would hand it
    # several five-minute jobs to hold in a buffer while other workers idle.
    queue = await RabbitMQJobQueue.start(store=PostgresJobStore(db), url=settings.broker_url)

    supervisor = Supervisor(queue=queue, store=store, config=WorkerConfig.from_settings(settings))

    # SIGTERM is how a container is asked to stop, so it is the signal that has
    # to reach the run loop. Registered on the loop rather than with
    # `signal.signal`, because a handler that runs between bytecodes cannot
    # safely touch an `asyncio.Event`; `add_signal_handler` schedules it on the
    # loop instead. The child gets its own disposition reset in `worker/child.py`
    # and is stopped by the supervisor, never by this handler.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, supervisor.stop)

    try:
        await supervisor.run()
    finally:
        # Ordered the way they were built, in reverse. The queue first: closing
        # it drops the channel, which requeues whatever it still held — the
        # broker-side half of redelivery, and the reason nothing is acked on the
        # way out.
        await queue.close()
        await store.close()
        await db.dispose()


if __name__ == "__main__":
    asyncio.run(main())
