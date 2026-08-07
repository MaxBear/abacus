import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from adapters import db
from api import health
from core.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.getLogger(__name__).info("starting %s (env=%s)", settings.app_name, settings.env)
    yield
    # Graceful shutdown: uvicorn is PID 1 and gets SIGTERM directly (exec-form
    # ENTRYPOINT), so this actually runs before the container exits.
    await db.dispose()


app = FastAPI(title="abacus", lifespan=lifespan)
app.include_router(health.router)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "abacus", "docs": "/docs"}
