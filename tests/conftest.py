"""Fixtures shared across test modules.

Only the Postgres seam lives here, because it is the one thing two modules
need: `test_chat_repository.py` runs the persistence contract against it, and
`test_chat_ws.py`'s acceptance test needs the real database behind a real
socket — the whole point of that test being that durable state and the
transport agree, which a mock cannot demonstrate about itself.

Everything else stays in the module that uses it. A conftest is a namespace
every test file silently imports, so what goes in it should be what more than
one of them actually wants.
"""

import uuid

import pytest
from sqlalchemy import delete

from adapters.postgres.chat_repository import PostgresChatRepository
from adapters.postgres.db import Database
from adapters.postgres.tables import chat_sessions
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
