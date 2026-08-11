from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import Settings


class Database:
    """Owns the engine, its session factory, and their lifecycle.

    Constructed once in api/main.py:lifespan and injected from there. Nothing
    reaches for a module global, so a worker or an Alembic env can build its
    own instance without the API being involved.
    """

    def __init__(self, settings: Settings) -> None:
        # Opens no socket. Construction still validates the URL (syntax,
        # dialect, driver import), so a malformed DATABASE_URL fails at startup
        # rather than surfacing as a 503 from /readyz much later.
        self._engine = create_async_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
        )
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)

    async def ping(self) -> None:
        """Raise if Postgres is not reachable. Used by /readyz."""
        async with self._engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """A unit of work. Nothing calls this until phase 1 adds real queries.

        It lives here rather than arriving with the first repository because the
        factory is derived from the engine — same object, same lifecycle.
        """
        async with self._sessionmaker() as s:
            yield s

    async def dispose(self) -> None:
        """Close the pool. Called from lifespan on graceful shutdown."""
        await self._engine.dispose()
