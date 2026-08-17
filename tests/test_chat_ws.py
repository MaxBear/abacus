"""Phase 1a acceptance: the frame protocol, and the transport rules around it.

No containers and no LLM — the responder is a stub behind the Protocol phase 4
will implement, and the connection plumbing is exercised directly with a fake
socket where a real one would only add nondeterminism.
"""

import asyncio
import contextlib
import json
import uuid

import anyio
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect, WebSocketState

from api import deps
from api.main import app
from core.chat_handler import ConnectionHandler
from core.config import Settings
from core.protocol import PROTOCOL_VERSION, Error, ErrorCode, Pong
from core.responder import StubResponder
from core.ws import (
    WS_NORMAL,
    WS_SERVICE_RESTART,
    WS_TRY_AGAIN_LATER,
    Connection,
    ConnectionRegistry,
)

SESSION = str(uuid.uuid4())
URL = f"/ws/chat/{SESSION}"


@pytest.fixture
def client():
    """A client whose lifespan has actually run.

    Unlike the health tests' ASGITransport, this needs app.state populated: the
    registry and the responder are built in lifespan, and the point of building
    them there is that the socket route finds them without importing anything.
    """
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# --------------------------------------------------------------------------
# The happy path
# --------------------------------------------------------------------------


def test_a_user_message_streams_ack_then_deltas_then_done(client):
    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": "hello world", "client_msg_id": "c1"})

        ack = ws.receive_json()
        assert ack["type"] == "ack"
        assert ack["client_msg_id"] == "c1"
        assert ack["v"] == PROTOCOL_VERSION
        message_id = ack["message_id"]

        deltas = []
        while (frame := ws.receive_json())["type"] != "done":
            assert frame["type"] == "delta"
            assert frame["message_id"] == message_id
            deltas.append(frame)

        assert frame["message_id"] == message_id
        # Several chunks, not one: the multi-frame streaming path is the thing
        # the stub exists to exercise before there is a model behind it.
        assert len(deltas) > 1
        assert [d["chunk_index"] for d in deltas] == list(range(len(deltas)))
        assert "".join(d["text"] for d in deltas) == "echo: hello world "


def test_a_reply_longer_than_the_send_queue_still_arrives(client):
    """A long reply must not trip the backpressure bound.

    The queue exists to drop a slow *reader*; a healthy client must never reach
    it. That requires the producer to let the writer run between frames — a turn
    that fills the queue in one uninterrupted burst starves the only consumer
    that could drain it, and every message past ws_send_queue_size words dies
    with 1013 no matter how fast the client is.
    """
    text = " ".join(f"w{i}" for i in range(100))  # comfortably over the 64-frame default

    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": text, "client_msg_id": "c1"})
        assert ws.receive_json()["type"] == "ack"

        deltas = []
        while (frame := ws.receive_json())["type"] != "done":
            deltas.append(frame)

    assert [d["chunk_index"] for d in deltas] == list(range(len(deltas)))
    assert "".join(d["text"] for d in deltas) == f"echo: {text} "


def test_ping_is_answered_with_pong(client):
    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "ping"})
        assert ws.receive_json() == {"type": "pong", "v": PROTOCOL_VERSION}


# --------------------------------------------------------------------------
# Errors are frames, not closes
# --------------------------------------------------------------------------


def test_a_malformed_frame_errors_without_closing_the_socket(client):
    """Closing here would turn one bad message into a reconnect loop."""
    with client.websocket_connect(URL) as ws:
        ws.send_text('{"type": "user_message"}')  # missing text and client_msg_id

        err = ws.receive_json()
        assert err["type"] == "error"
        assert err["code"] == ErrorCode.BAD_FRAME
        assert err["retryable"] is False

        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


def test_frame_types_from_later_phases_are_rejected_not_ignored(client):
    """`cancel` is specified in docs/websocket.md but not implemented until phase 2.

    It must not parse. A cancel that is silently accepted and does nothing is,
    from the client's side, indistinguishable from one that worked.
    """
    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "cancel", "job_id": "j1"})
        err = ws.receive_json()
        assert err["type"] == "error"
        assert err["code"] == ErrorCode.BAD_FRAME


def test_an_unknown_field_is_rejected(client):
    """extra="forbid", so a typo'd field is loud rather than silently dropped."""
    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "ping", "txet": "typo"})
        assert ws.receive_json()["code"] == ErrorCode.BAD_FRAME


def test_a_frame_from_a_future_protocol_version_is_rejected(client):
    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "ping", "v": PROTOCOL_VERSION + 1})
        err = ws.receive_json()
        assert err["code"] == ErrorCode.UNSUPPORTED_VERSION


# --------------------------------------------------------------------------
# Handshake refusals — before accept, so the client's connect fails outright
# --------------------------------------------------------------------------


def test_a_disallowed_origin_is_refused_at_the_handshake(client):
    app.dependency_overrides[deps.get_settings] = lambda: Settings(
        ws_allowed_origins=("https://app.example.com",)
    )
    with (
        pytest.raises(WebSocketDisconnect),
        client.websocket_connect(URL, headers={"Origin": "https://evil.example.com"}),
    ):
        pass


def test_an_allowed_origin_connects(client):
    app.dependency_overrides[deps.get_settings] = lambda: Settings(
        ws_allowed_origins=("https://app.example.com",)
    )
    with client.websocket_connect(URL, headers={"Origin": "https://app.example.com"}) as ws:
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


def test_an_empty_allowlist_denies_browser_origins_outside_local(client):
    """An unset allowlist in production must not quietly mean "any origin".

    Overriding the dependency is enough: lifespan reads core.config.get_settings
    directly, while the route resolves settings through deps.get_settings, so
    the override applies however the client was built.
    """
    app.dependency_overrides[deps.get_settings] = lambda: Settings(env="prod")
    with (
        pytest.raises(WebSocketDisconnect),
        client.websocket_connect(URL, headers={"Origin": "https://evil.example.com"}),
    ):
        pass


def test_a_future_protocol_version_is_refused_at_the_handshake(client):
    with (
        pytest.raises(WebSocketDisconnect),
        client.websocket_connect(f"{URL}?v={PROTOCOL_VERSION + 1}"),
    ):
        pass


def test_a_non_uuid_session_id_is_refused(client):
    with pytest.raises(WebSocketDisconnect), client.websocket_connect("/ws/chat/not-a-uuid"):
        pass


@pytest.mark.parametrize(
    "spelling",
    [
        SESSION.upper(),
        SESSION.replace("-", ""),
        "{" + SESSION + "}",
        f"urn:uuid:{SESSION}",
    ],
    ids=["uppercase", "undashed", "braced", "urn"],
)
def test_a_non_canonical_uuid_spelling_is_refused(client, spelling):
    """uuid.UUID() parses all of these, but they are not the same registry key.

    The path string is what the registry buckets connections by, so admitting a
    second spelling of one session would silently fan a message out to only the
    clients that happened to spell it the same way.
    """
    with pytest.raises(WebSocketDisconnect), client.websocket_connect(f"/ws/chat/{spelling}"):
        pass


# --------------------------------------------------------------------------
# Limits
# --------------------------------------------------------------------------


def test_connections_per_session_are_capped(client):
    registry = ConnectionRegistry(max_per_session=1, drain_timeout_seconds=1.0)
    app.dependency_overrides[deps.get_registry] = lambda: registry

    # The first connection must stay open while the second is attempted.
    with client.websocket_connect(URL), client.websocket_connect(URL) as second:
        # Nudge the socket before reading. TestClient's receive has no timeout,
        # so if the cap ever stops being enforced this would block forever on a
        # healthy connection with nothing to say. With a ping in flight there is
        # always a frame to read: `pong` if the cap is broken, which fails the
        # assertion instead of hanging CI.
        #
        # Nothing reads this one — the route refused and returned before it ever
        # called receive — and it needs no guard: TestClient's send only puts a
        # message on a queue, so a closed socket cannot make it raise.
        second.send_json({"type": "ping"})

        err = second.receive_json()
        assert err["type"] == "error"
        assert err["code"] == ErrorCode.TOO_MANY_CONNECTIONS
        assert err["retryable"] is True

        # The refusal is a frame *and* a close — a connection left open past its
        # own error would count against the cap forever. 1013 is the code that
        # tells the client to retry later rather than treat this as fatal.
        with pytest.raises(WebSocketDisconnect) as refused:
            second.receive_json()
        assert refused.value.code == WS_TRY_AGAIN_LATER


def test_concurrent_turns_are_capped(client):
    """The second message is dispatched strictly after the first is registered.

    `_dispatch` adds the turn to the set synchronously before returning to the
    read loop, so this needs no sleep on the test side to be deterministic — only
    a responder slow enough that the first turn has not finished.
    """

    class SlowResponder:
        async def respond(self, text: str):
            await asyncio.sleep(30)
            yield text  # pragma: no cover - the test never lets it get here

    app.dependency_overrides[deps.get_responder] = SlowResponder
    app.dependency_overrides[deps.get_settings] = lambda: Settings(ws_max_concurrent_turns=1)

    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": "first", "client_msg_id": "c1"})
        assert ws.receive_json()["type"] == "ack"

        ws.send_json({"type": "user_message", "text": "second", "client_msg_id": "c2"})
        err = ws.receive_json()
        assert err["code"] == ErrorCode.TOO_MANY_TURNS
        assert err["retryable"] is True


# --------------------------------------------------------------------------
# Connection plumbing, against a fake socket
# --------------------------------------------------------------------------


class FakeWebSocket:
    """Records what reached the wire. A real socket would only add scheduling noise."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.close_code: int | None = None
        self.client_state = WebSocketState.CONNECTED

    async def send_text(self, text: str) -> None:
        self.sent.append(text)

    async def close(self, code: int = 1000) -> None:
        self.close_code = code
        self.client_state = WebSocketState.DISCONNECTED


async def test_cancelling_serve_leaves_no_task_behind():
    """serve() must not orphan its reader and writer when cancelled from outside.

    Nothing in asyncio links a task to the one that created it, so a cancel
    aimed at the ASGI task — which is what uvicorn does once
    timeout_graceful_shutdown is exceeded — reaches serve() at its
    `asyncio.wait`, and asyncio.wait does not cancel what it awaits. Without
    the cleanup in serve()'s finally, ws-reader and ws-writer would keep
    running with nothing left holding a reference to them.
    """

    class SilentWebSocket(FakeWebSocket):
        async def receive_text(self) -> str:
            await asyncio.Event().wait()  # a peer that never sends and never leaves
            raise AssertionError("unreachable")

    conn = Connection(SilentWebSocket(), "s", send_queue_size=8)
    serving = asyncio.create_task(ConnectionHandler(conn, StubResponder(), Settings()).serve())
    await asyncio.sleep(0)  # let serve() spawn both tasks and park in asyncio.wait

    serving.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await serving

    leaked = [t for t in asyncio.all_tasks() if t.get_name() in {"ws-reader", "ws-writer"}]
    assert leaked == []


async def test_send_queue_overflow_drops_the_connection():
    """A slow reader must not be able to grow this process's memory.

    Dropping is only the right answer because durability lives elsewhere: from
    1b the client reconnects and replays what it missed.
    """
    conn = Connection(FakeWebSocket(), "s", send_queue_size=2)
    for _ in range(6):
        conn.send(Pong())

    # wait_for, not a bare await: if the bound is ever removed, run_writer blocks
    # on an empty queue forever. That must surface as a failure, not as a CI job
    # that hangs until its timeout.
    assert await asyncio.wait_for(conn.run_writer(), timeout=2.0) == WS_TRY_AGAIN_LATER


async def test_drain_sends_going_away_then_closes_with_service_restart():
    """Kubernetes does not drain WebSockets; this is what stands in for it."""
    fake = FakeWebSocket()
    conn = Connection(fake, "s", send_queue_size=8)
    registry = ConnectionRegistry(max_per_session=4, drain_timeout_seconds=1.0)
    assert registry.add(conn)

    async def serve() -> int:
        code = await conn.run_writer()
        conn.finished.set()  # what the route's finally does
        return code

    serving = asyncio.create_task(serve())
    await registry.drain(
        Error(code=ErrorCode.GOING_AWAY, message="server is shutting down", retryable=True)
    )

    # Bounded for the same reason as the overflow test: a drain that stops
    # closing connections would otherwise hang here instead of failing.
    assert await asyncio.wait_for(serving, timeout=2.0) == WS_SERVICE_RESTART
    assert json.loads(fake.sent[0])["code"] == ErrorCode.GOING_AWAY


@pytest.mark.parametrize(
    "exc",
    [
        # Starlette's state check, when a close was already sent.
        RuntimeError('Cannot call "send" once a close message has been sent.'),
        # What Starlette raises when the *write* fails: it catches the OSError
        # and re-raises it as a disconnect, so the send path can surface the
        # exception the receive path is named for (starlette/websockets.py).
        WebSocketDisconnect(code=1006),
        anyio.ClosedResourceError(),
        anyio.BrokenResourceError(),
    ],
    ids=["runtime", "disconnect", "closed", "broken"],
)
async def test_a_write_that_loses_the_race_with_a_disconnect_closes_normally(exc):
    """A peer that vanishes mid-write is routine, not a failure.

    Each of these means the same thing — the socket is gone — so each must end
    the writer with WS_NORMAL. Letting one escape instead closes the connection
    with WS_INTERNAL_ERROR and logs a stack trace for an ordinary disconnect,
    which is noise in exactly the logs you read during a rollout.
    """

    class FailingWebSocket(FakeWebSocket):
        async def send_text(self, text: str) -> None:
            raise exc

    conn = Connection(FailingWebSocket(), "s", send_queue_size=8)
    conn.send(Pong())

    assert await asyncio.wait_for(conn.run_writer(), timeout=2.0) == WS_NORMAL


async def test_drain_does_not_wait_past_its_timeout():
    """A connection that will not flush must not hold the pod past its grace period."""
    conn = Connection(FakeWebSocket(), "s", send_queue_size=8)  # nothing ever drains it
    registry = ConnectionRegistry(max_per_session=4, drain_timeout_seconds=0.05)
    registry.add(conn)

    await asyncio.wait_for(
        registry.drain(Error(code=ErrorCode.GOING_AWAY, message="bye", retryable=True)),
        timeout=2.0,
    )


def test_the_registry_refuses_a_connection_past_the_cap():
    """The cap itself, with no socket in the way to turn a regression into a hang."""
    registry = ConnectionRegistry(max_per_session=1, drain_timeout_seconds=1.0)

    assert registry.add(Connection(FakeWebSocket(), "s", send_queue_size=8)) is True
    assert registry.add(Connection(FakeWebSocket(), "s", send_queue_size=8)) is False
    # A different session is unaffected — the cap is per session, not per replica.
    assert registry.add(Connection(FakeWebSocket(), "other", send_queue_size=8)) is True


def test_the_registry_forgets_sessions_it_no_longer_holds():
    """Otherwise a long-lived process accumulates an entry per session ever seen."""
    registry = ConnectionRegistry(max_per_session=2, drain_timeout_seconds=1.0)
    conn = Connection(FakeWebSocket(), "s", send_queue_size=8)

    registry.add(conn)
    assert registry.for_session("s") == [conn]
    registry.remove(conn)

    assert len(registry) == 0
    assert registry.for_session("s") == []
