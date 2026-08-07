import aio_pika

from core.config import get_settings


async def ping() -> None:
    """Raise if the broker is not reachable. Used by /readyz.

    Deliberately opens and closes a connection rather than holding a pooled one:
    readiness should measure whether a *new* consumer could connect right now,
    which is the thing that actually matters after a broker restart.
    """
    conn = await aio_pika.connect_robust(get_settings().broker_url, timeout=2)
    await conn.close()
