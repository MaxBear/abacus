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


@lru_cache
def get_settings() -> Settings:
    return Settings()
