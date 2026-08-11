import asyncio
import logging

from fastapi import APIRouter, Response, status

from adapters import broker
from api.deps import DatabaseDep, SettingsDep

log = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get("/livez")
async def livez() -> dict[str, str]:
    """Liveness. Checks nothing but that the process can serve a request.

    Deliberately dependency-free — it takes no injected dependencies at all.
    If this checked Postgres, a database blip would make the kubelet kill and
    restart every pod, turning a recoverable dependency outage into a crash
    loop. Liveness answers "is this process wedged?", not "is the system
    healthy?".
    """
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(response: Response, db: DatabaseDep, settings: SettingsDep) -> dict[str, object]:
    """Readiness. Fails when this pod cannot usefully serve traffic.

    Checks are run concurrently and bounded — a hung dependency must not turn
    into a hung probe.
    """
    timeout = settings.readiness_timeout_seconds
    checks = {"postgres": db.ping(), "rabbitmq": broker.ping(settings)}

    async def run(name: str, coro) -> tuple[str, str | None]:
        try:
            await asyncio.wait_for(coro, timeout=timeout)
            return name, None
        except Exception as exc:  # noqa: BLE001 - report any failure to the probe
            log.warning("readiness check failed: %s: %s", name, exc)
            return name, f"{type(exc).__name__}: {exc}"

    results = await asyncio.gather(*(run(n, c) for n, c in checks.items()))
    failures = {name: err for name, err in results if err is not None}

    if failures:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unavailable", "failed": failures}

    return {"status": "ready", "checks": list(checks)}
