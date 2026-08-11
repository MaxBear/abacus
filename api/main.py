import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from adapters import db
from api import health
from core.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Composition root: every dependency is built here, before anything serves.

    The engine is constructed eagerly so a malformed DATABASE_URL kills the
    process at startup instead of surfacing as a mystery 503 later. It is
    deliberately *not* connected here: create_async_engine opens no socket, so
    a pod whose Postgres is down still starts and reports not-ready, rather
    than crash-looping. Same reasoning as the liveness/readiness split.
    """
    settings = get_settings()
    logging.getLogger(__name__).info("starting %s (env=%s)", settings.app_name, settings.env)

    app.state.settings = settings
    app.state.db = db.Database(settings)

    yield

    # Graceful shutdown: uvicorn is PID 1 and gets SIGTERM directly (exec-form
    # ENTRYPOINT), so this actually runs before the container exits.
    await app.state.db.dispose()


app = FastAPI(title="abacus", lifespan=lifespan)
app.include_router(health.router)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "abacus", "docs": "/docs"}
