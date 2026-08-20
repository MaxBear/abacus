from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration.

    Everything arrives from the environment. Nothing is baked into the image —
    see the note in the Dockerfile about ENV persisting in image history.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "abacus"
    env: str = "local"

    database_url: str = "postgresql+asyncpg://abacus:abacus@postgres:5432/abacus"
    broker_url: str = "amqp://abacus:abacus@rabbitmq:5672/"

    # Readiness must fail fast: a slow /readyz blocks rollouts and makes the
    # kubelet's probe timeout the thing that decides liveness.
    readiness_timeout_seconds: float = 2.0

    # --- WebSocket transport (docs/websocket.md) ---

    # Browser origins allowed to open a socket. WebSockets are exempt from the
    # same-origin policy and CORS does not apply to them, so without this check
    # any page on the internet can open a connection here and the browser will
    # attach cookies. Empty is permissive only when env == "local"; anywhere
    # else an empty list denies every browser origin, so a forgotten value
    # fails closed.
    ws_allowed_origins: tuple[str, ...] = ()

    # Per-connection outbound buffer. Beyond this the connection is dropped
    # rather than buffered — see the backpressure note in core/ws.py.
    ws_send_queue_size: int = 64
    ws_max_connections_per_session: int = 8
    ws_max_concurrent_turns: int = 4

    # How much history one `resume` may replay. A client further behind than
    # this gets `resume_too_old` and reloads over plain HTTP instead: unbounded
    # replay turns a week-old tab into a full history dump on a single
    # reconnect, down a socket whose send queue is 64 frames deep.
    ws_resume_max_messages: int = 200

    # Must stay well inside the container's stop_grace_period, or the drain is
    # cut short by SIGKILL and accomplishes nothing.
    ws_drain_timeout_seconds: float = 5.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
