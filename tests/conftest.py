"""Fixtures shared across test modules.

Two seams live here, and both are here for the same reason: more than one module
wants them. `test_chat_repository.py` runs the persistence contract against
Postgres and `test_chat_ws.py`'s acceptance test needs the real database behind
a real socket; `test_job_queue.py` runs the queue contract against RabbitMQ and
`test_rabbitmq_job_queue.py` asserts what only that implementation can be asked
about.

Everything else stays in the module that uses it. A conftest is a namespace
every test file silently imports, so what goes in it should be what more than
one of them actually wants.
"""

import uuid
from datetime import timedelta

import aio_pika
import pytest
from sqlalchemy import delete, func, select

from adapters.postgres.chat_repository import PostgresChatRepository
from adapters.postgres.db import Database
from adapters.postgres.job_store import PostgresJobStore
from adapters.postgres.tables import chat_sessions, jobs
from adapters.rabbitmq.job_queue import RabbitMQJobQueue
from core.config import get_settings


@pytest.fixture
def created_sessions() -> list[uuid.UUID]:
    """Session ids a test made, for the postgres fixture to clean up after.

    A list rather than a return value so a test and its fixtures can all append
    to the same registry: `postgres_repo` reads it during teardown, by which
    point whoever created a session is long finished.
    """
    return []


@pytest.fixture
async def postgres_repo(created_sessions):
    """A repository on the real database, or a skip when none is reachable.

    Skipping rather than failing is what keeps `make test` container-free while
    leaving one command that covers both situations — `make up && make migrate`
    turns these on with no flag to remember.
    """
    db = Database(get_settings())
    try:
        await db.ping()
    except Exception:  # noqa: BLE001 - any failure to reach it means "not available"
        await db.dispose()
        pytest.skip("no postgres reachable — `make up && make migrate` enables these")

    try:
        yield PostgresChatRepository(db)
    finally:
        # chat_messages goes with it: the FK is ON DELETE CASCADE.
        if created_sessions:
            async with db.session() as s:
                stmt = delete(chat_sessions).where(chat_sessions.c.id.in_(created_sessions))
                await s.execute(stmt)
                await s.commit()
        await db.dispose()


# How often the RabbitMQ adapter's maintenance loop runs under test. Far tighter
# than the 30 seconds `docs/jobs.md` proposes for production, because that loop
# is what notices a lapsed lease here — the broker has no per-message deadline —
# and the contract suite asserts redelivery within a few hundred milliseconds.
_MAINTENANCE = timedelta(milliseconds=40)

# The orphan threshold: how long a committed-but-unpublished job waits before the
# sweep republishes it. Production wants this well above a normal publish latency
# so a broker blip is not mistaken for a lost job; a test wants it short enough
# to observe. Both are the same guess at different scales — see `docs/jobs.md`'s
# first open question.
_ORPHAN_AFTER = timedelta(milliseconds=150)

# The suite drives one queue object as several competing consumers, so its
# prefetch window has to cover them. `prefetch=1` is the phase-3 worker's
# setting and the adapter's default; raising it here is fixture configuration,
# not a change to what the contract asserts.
_PREFETCH = 16


class _SeedsSessions:
    """A `JobQueue` that creates the chat session a request names, first.

    The contract suite invents session ids inline — `a_request(session_id=
    uuid.uuid4())` — because the memory implementation has no schema to satisfy.
    Postgres does: `jobs.session_id` carries a foreign key to `chat_sessions`,
    which is what makes deleting a session take its jobs with it.

    Seeding here rather than loosening the constraint, and here rather than in
    the adapter, because the constraint is right and the looseness is the test's:
    in the running system a session exists long before any job references it —
    `api/chat.py` calls `ensure_session` on connect. Delegating everything else
    keeps this a harness detail that no assertion can see.
    """

    def __init__(self, queue, repo, created_sessions: list[uuid.UUID]) -> None:
        self._queue = queue
        self._repo = repo
        self._created = created_sessions

    async def enqueue(self, request):
        if request.session_id is not None:
            await self._repo.ensure_session(request.session_id)
            self._created.append(request.session_id)
        return await self._queue.enqueue(request)

    def __getattr__(self, name):
        return getattr(self._queue, name)


@pytest.fixture
async def rabbitmq_queue(created_sessions):
    """The RabbitMQ queue on real infrastructure, or a skip when either is missing.

    Both services are required, and that is the point rather than an
    inconvenience: this implementation is a broker *and* the `jobs` table, so a
    run with only one of them up would be testing something that does not exist.

    Every run gets its own exchange and queues. Purging shared ones between
    tests would be the alternative, and it races — a message published by the
    test that just finished can arrive after the purge and be reserved by the
    next one, which fails somewhere unrelated and only sometimes.
    """
    settings = get_settings()
    db = Database(settings)
    try:
        await db.ping()
    except Exception:  # noqa: BLE001 - any failure to reach it means "not available"
        await db.dispose()
        pytest.skip("no postgres reachable — `make up && make migrate` enables these")

    try:
        conn = await aio_pika.connect_robust(settings.broker_url, timeout=2)
        await conn.close()
    except Exception:  # noqa: BLE001 - same
        await db.dispose()
        pytest.skip("no rabbitmq reachable — `make up` enables these")

    # The database's clock, not this process's: `created_at` is written by
    # `now()`, and teardown deletes by comparing against it.
    async with db.session() as s:
        started = (await s.execute(select(func.now()))).scalar_one()

    queue = await RabbitMQJobQueue.start(
        store=PostgresJobStore(db),
        url=settings.broker_url,
        namespace=f"test-{uuid.uuid4().hex[:12]}",
        prefetch=_PREFETCH,
        maintenance_interval=_MAINTENANCE,
        orphan_after=_ORPHAN_AFTER,
    )
    try:
        yield _SeedsSessions(queue, PostgresChatRepository(db), created_sessions)
    finally:
        await queue.close(delete_queues=True)
        async with db.session() as s:
            # Session-less jobs are most of them, so the cascade is not enough on
            # its own; `created_at` is what identifies this test's rows.
            await s.execute(delete(jobs).where(jobs.c.created_at >= started))
            if created_sessions:
                await s.execute(
                    delete(chat_sessions).where(chat_sessions.c.id.in_(created_sessions))
                )
            await s.commit()
        await db.dispose()
