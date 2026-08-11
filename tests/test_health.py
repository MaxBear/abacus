import httpx
import pytest
from httpx import ASGITransport

from api import deps
from api.main import app
from core.config import Settings


class FakeDatabase:
    """Stands in for adapters.db.Database.

    Injected through dependency_overrides rather than monkeypatching an import
    path by string, which is the point of constructing Database in lifespan and
    passing it down.
    """

    def __init__(self, *, healthy: bool = True) -> None:
        self._healthy = healthy
        self.pings = 0

    async def ping(self) -> None:
        self.pings += 1
        if not self._healthy:
            raise ConnectionError("postgres is down")


@pytest.fixture
async def client():
    """A client with dependencies injected directly.

    ASGITransport does not run lifespan, so app.state is never populated —
    which is fine: overriding the providers is the seam these tests want.
    Individual tests re-override get_db to vary database health.
    """
    app.dependency_overrides[deps.get_db] = lambda: FakeDatabase()
    app.dependency_overrides[deps.get_settings] = lambda: Settings()

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    app.dependency_overrides.clear()


async def test_livez_has_no_dependencies(client, monkeypatch):
    """Liveness must stay green even when every dependency is down.

    This is the regression guard for the mistake the Dockerfile/k8s critique
    flagged: if liveness checked Postgres, a database outage would crash-loop
    every pod instead of just removing them from the load balancer.
    """

    async def boom(*_args) -> None:
        raise ConnectionError("rabbitmq is down")

    app.dependency_overrides[deps.get_db] = lambda: FakeDatabase(healthy=False)
    monkeypatch.setattr("adapters.broker.ping", boom)

    resp = await client.get("/livez")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_readyz_reports_503_when_a_dependency_is_down(client, monkeypatch):
    async def fine(*_args) -> None:
        return None

    app.dependency_overrides[deps.get_db] = lambda: FakeDatabase(healthy=False)
    monkeypatch.setattr("adapters.broker.ping", fine)

    resp = await client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unavailable"
    assert "postgres" in body["failed"]
    assert "rabbitmq" not in body["failed"]


async def test_readyz_ok_when_all_dependencies_are_up(client, monkeypatch):
    async def fine(*_args) -> None:
        return None

    monkeypatch.setattr("adapters.broker.ping", fine)

    resp = await client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


async def test_readyz_uses_the_injected_database(client, monkeypatch):
    """The Database reaches the handler by injection, not by a module global."""
    fake = FakeDatabase()
    app.dependency_overrides[deps.get_db] = lambda: fake

    async def fine(*_args) -> None:
        return None

    monkeypatch.setattr("adapters.broker.ping", fine)

    resp = await client.get("/readyz")
    assert resp.status_code == 200
    assert fake.pings == 1
