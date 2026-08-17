import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from adapters import db
from api import chat, health
from core.config import get_settings
from core.protocol import Error, ErrorCode
from core.responder import StubResponder
from core.ws import ConnectionRegistry

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
    app.state.chat_registry = ConnectionRegistry(
        max_per_session=settings.ws_max_connections_per_session,
        drain_timeout_seconds=settings.ws_drain_timeout_seconds,
    )
    # Phase 4 swaps this for the LLM gateway. Because it is chosen here rather
    # than imported at a call site, that swap is one line in one file.
    app.state.responder = StubResponder()

    yield

    # Graceful shutdown: uvicorn is PID 1 and gets SIGTERM directly (exec-form
    # ENTRYPOINT), so this actually runs before the container exits.
    #
    # A backstop, not the drain path — measured, not assumed. Uvicorn closes
    # every open WebSocket itself *before* lifespan shutdown runs
    # (websockets_impl.WebSocketProtocol.shutdown: fail_connection(1012), then
    # transport.close()), and waits for those connections to finish first. So
    # under uvicorn this registry is already empty and drain() returns
    # immediately; clients get a bare 1012 close from the server, with no
    # going_away frame ahead of it, because there is no longer a socket to write
    # it to.
    #
    # That is *almost* the documented behavior — 1012 is the right code and
    # clients reconnect correctly — but the graceful half of docs/websocket.md's
    # drain sequence needs a hook that runs before uvicorn starts closing, which
    # ASGI lifespan does not provide. Phase 6 resolves it with a preStop hook.
    # Kept because drain() is correct when something does call it in time, and
    # because a non-uvicorn server may order these the other way.
    await app.state.chat_registry.drain(
        Error(code=ErrorCode.GOING_AWAY, message="server is shutting down", retryable=True)
    )
    await app.state.db.dispose()


app = FastAPI(title="abacus", lifespan=lifespan)
app.include_router(health.router)
app.include_router(chat.router)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "abacus", "docs": "/docs"}
