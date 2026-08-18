import aio_pika

from core.config import Settings


async def ping(settings: Settings) -> None:
    """Raise if the broker is not reachable. Used by /readyz.

    Deliberately opens and closes a connection rather than holding a pooled one:
    readiness should measure whether a *new* consumer could connect right now,
    which is the thing that actually matters after a broker restart.
    """
    conn = await aio_pika.connect_robust(settings.broker_url, timeout=2)
    await conn.close()
