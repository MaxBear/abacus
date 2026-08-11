import httpx
import pytest
from httpx import ASGITransport

from api.main import app


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_livez_has_no_dependencies(client, monkeypatch):
    """Liveness must stay green even when every dependency is down.

    This is the regression guard for the mistake the Dockerfile/k8s critique
    flagged: if liveness checked Postgres, a database outage would crash-loop
    every pod instead of just removing them from the load balancer.
    """

    async def boom() -> None:
        raise ConnectionError("postgres is down")

    monkeypatch.setattr("adapters.db.ping", boom)
    monkeypatch.setattr("adapters.broker.ping", boom)

    resp = await client.get("/livez")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_readyz_reports_503_when_a_dependency_is_down(client, monkeypatch):
    async def boom() -> None:
        raise ConnectionError("postgres is down")

    async def fine() -> None:
        return None

    monkeypatch.setattr("adapters.db.ping", boom)
    monkeypatch.setattr("adapters.broker.ping", fine)

    resp = await client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unavailable"
    assert "postgres" in body["failed"]
    assert "rabbitmq" not in body["failed"]


async def test_readyz_ok_when_all_dependencies_are_up(client, monkeypatch):
    async def fine() -> None:
        return None

    monkeypatch.setattr("adapters.db.ping", fine)
    monkeypatch.setattr("adapters.broker.ping", fine)

    resp = await client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"
