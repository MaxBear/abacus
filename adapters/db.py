from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy import text

from core.config import get_settings

_engine: AsyncEngine | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            get_settings().database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
        )
    return _engine


async def ping() -> None:
    """Raise if Postgres is not reachable. Used by /readyz."""
    async with get_engine().connect() as conn:
        await conn.execute(text("SELECT 1"))


async def dispose() -> None:
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
