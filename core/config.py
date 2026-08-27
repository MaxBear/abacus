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

    # --- Object storage (docs/worker.md) ---

    # MinIO under compose, real S3 in phase 6. `None` is what selects AWS: it
    # lets botocore resolve the regional endpoint itself, which is the one thing
    # a hardcoded URL cannot do correctly across regions.
    s3_endpoint_url: str | None = "http://minio:9000"
    s3_region: str = "us-east-1"

    # Local credentials, matching the pattern DATABASE_URL and BROKER_URL
    # already set: the compose stack's values are the defaults so a fresh
    # checkout runs, and a deployment supplies its own. Phase 6 replaces both
    # with a role rather than a longer string.
    s3_access_key: str = "abacus"
    s3_secret_key: str = "abacus-local-secret"  # noqa: S105 - a compose default, not a credential

    s3_bucket: str = "abacus-artifacts"

    # --- The worker (docs/worker.md) ---

    # How long a claim is good for. The supervisor extends it on a timer while a
    # child runs, so this is not the length of a solve — it is how long the job
    # is unavailable to anyone else after this worker stops saying anything at
    # all. Short enough that a dead worker's job is picked up promptly, long
    # enough that a database blip is not immediately a lost lease.
    worker_lease_seconds: float = 60.0

    # The heartbeat, comfortably inside the lease so a slow `extend` is not
    # itself the thing that loses the job: two consecutive failures still leave
    # a third attempt before the deadline.
    worker_extend_interval_seconds: float = 20.0

    # The wall-clock cap the lease cannot express. A supervisor that is healthy
    # while its child is wedged extends forever and the row looks permanently
    # `running`; past this the child is killed and the job fails with a real
    # error. Twice the five-minute solve `docs/worker.md` sizes everything else
    # against.
    worker_solve_timeout_seconds: float = 600.0

    # Between SIGTERM and SIGKILL — layer 2 before layer 3.
    worker_grace_seconds: float = 5.0

    # How long one `reserve` waits before returning empty-handed. The loop asks
    # again immediately, so this is only how often an idle worker speaks.
    worker_reserve_wait_seconds: float = 5.0

    # The backoff on a failed solve. Nonzero for the reason `JobQueue.nack`
    # gives: an instant retry of a deterministic error is a hot loop.
    worker_retry_backoff_seconds: float = 30.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
