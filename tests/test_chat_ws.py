"""Phase 1a acceptance: the frame protocol, and the transport rules around it.

No containers and no LLM — the responder is a stub behind the Protocol phase 4
will implement, and the connection plumbing is exercised directly with a mock
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
from core.chat_repository import Role, Status
from core.config import Settings
from core.frames import PROTOCOL_VERSION, Error, ErrorCode, Pong
from core.jobs import JobState
from core.responder import StubResponder
from core.ws import (
    WS_NORMAL,
    WS_SERVICE_RESTART,
    WS_TRY_AGAIN_LATER,
    Connection,
    ConnectionRegistry,
)
from tests.MockChatRepository import MockChatRepository

SESSION = str(uuid.uuid4())
URL = f"/ws/chat/{SESSION}"


@pytest.fixture
def repository():
    """The mock behind the route, also readable by a test that asserts on rows."""
    return MockChatRepository()


@pytest.fixture
def client(repository):
    """A client whose lifespan has actually run.

    Unlike the health tests' ASGITransport, this needs app.state populated: the
    registry and the responder are built in lifespan, and the point of building
    them there is that the socket route finds them without importing anything.

    The repository is a mock, and a *fresh* one per test: these tests share one
    SESSION and reuse `client_msg_id` "c1", so a repository that outlived a test
    would answer the next one's first message as a duplicate.
    """
    app.dependency_overrides[deps.get_chat_repository] = lambda: repository
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
        # The first message in a fresh session, so its row is seq 1 and the
        # reply it opens is seq 2.
        assert ack["seq"] == 1
        # Two rows, two identities: the ack names the user's message, the
        # stream names the assistant's. Binding deltas to ack["message_id"]
        # would match nothing.
        assert ack["reply_message_id"] != ack["message_id"]
        message_id = ack["reply_message_id"]

        deltas = []
        while (frame := ws.receive_json())["type"] != "done":
            assert frame["type"] == "delta"
            assert frame["message_id"] == message_id
            deltas.append(frame)

        assert frame["message_id"] == message_id
        assert frame["seq"] == 2
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


def test_a_resubmitted_message_is_re_acked_without_a_second_turn(client):
    """A client unsure its message landed resends it under the same key.

    The answer must be the row that already exists — same `seq`, same
    `message_id` — and no second turn, or every uncertain reconnect silently
    doubles the user's question. Here the first turn has already finished, so
    its entry in the live map is gone and there is no stream to name: the reply
    is a completed row, which is resume's job to hand back rather than the
    ack's.
    """
    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": "hello", "client_msg_id": "c1"})
        first = ws.receive_json()
        while ws.receive_json()["type"] != "done":
            pass

        ws.send_json({"type": "user_message", "text": "hello", "client_msg_id": "c1"})
        again = ws.receive_json()

        # No second turn: a ping is answered immediately, with no deltas ahead
        # of the pong.
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"

    assert again["type"] == "ack"
    assert again["seq"] == first["seq"]
    assert again["message_id"] == first["message_id"]
    assert again["reply_message_id"] is None


def test_a_resubmit_while_the_turn_is_live_is_re_acked_with_the_same_stream(client):
    """The other half: the original turn is still running on this connection.

    There *is* a stream to name, and it must be the one already in flight — a
    client that reconnected its bubble to a new id would then never see the
    deltas it is about to receive.
    """

    class SlowResponder:
        async def respond(self, text: str):
            await asyncio.sleep(30)
            yield text  # pragma: no cover - the test never lets it get here

    app.dependency_overrides[deps.get_responder] = SlowResponder

    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": "hello", "client_msg_id": "c1"})
        first = ws.receive_json()

        ws.send_json({"type": "user_message", "text": "hello", "client_msg_id": "c1"})
        again = ws.receive_json()

    assert again["seq"] == first["seq"]
    assert again["reply_message_id"] == first["reply_message_id"]


def test_a_turn_that_raises_leaves_the_row_failed_not_streaming(client, repository):
    """An error frame is not enough — the row has to reach a terminal state.

    Left at `streaming`, the reply is indistinguishable on the next connection
    from one still in flight, and the client renders a half-sentence under a
    spinner that never resolves.
    """

    class FailingResponder:
        async def respond(self, text: str):
            raise RuntimeError("boom")
            yield ""  # pragma: no cover - makes this an async generator

    app.dependency_overrides[deps.get_responder] = FailingResponder

    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": "hello", "client_msg_id": "c1"})
        assert ws.receive_json()["type"] == "ack"

        err = ws.receive_json()
        assert err["code"] == ErrorCode.INTERNAL
        assert err["retryable"] is True

    rows = asyncio.run(repository.log_since(uuid.UUID(SESSION), 0, 10))
    assert [(r.role, r.status) for r in rows] == [
        (Role.USER, Status.COMPLETE),
        (Role.ASSISTANT, Status.FAILED),
    ]


# --------------------------------------------------------------------------
# Resume
# --------------------------------------------------------------------------


def _say_and_settle(ws, text: str, client_msg_id: str) -> dict:
    """Send one message and drain to `done`. Returns the ack."""
    ws.send_json({"type": "user_message", "text": text, "client_msg_id": client_msg_id})
    ack = ws.receive_json()
    while ws.receive_json()["type"] != "done":
        pass
    return ack


def _resume(ws, last_seq: int) -> list[dict]:
    """Resume, and collect the replay up to a trailing ping's pong.

    The ping is a terminator, not decoration. `docs/persistence.md` records the
    trap: a test that reads a fixed number of frames *hangs* rather than fails
    when replay returns too few, because a healthy socket with nothing to say is
    simply silent. Measuring "nothing came" needs something that must come.

    Sound because the read loop awaits each dispatch to completion, so the
    resume is fully replayed before the ping is even parsed — the pong can never
    overtake a frame the replay owes us.
    """
    ws.send_json({"type": "resume", "last_seq": last_seq})
    ws.send_json({"type": "ping"})
    frames = []
    while (frame := ws.receive_json())["type"] != "pong":
        frames.append(frame)
    return frames


def test_resume_replays_stored_messages_in_seq_order(client):
    """The reconnect path: a cursor of 0 asks for the session from the start.

    Whole messages, not the deltas the turn originally streamed — the accumulated
    text reaches the same rendered state in one frame instead of one per chunk.
    """
    with client.websocket_connect(URL) as ws:
        _say_and_settle(ws, "hello", "c1")
        replayed = _resume(ws, 0)

    assert [f["type"] for f in replayed] == ["message", "message"]
    assert [f["seq"] for f in replayed] == [1, 2]

    user, assistant = replayed
    assert user["role"] == Role.USER
    assert user["text"] == "hello"
    assert assistant["role"] == Role.ASSISTANT
    assert assistant["status"] == Status.COMPLETE
    # The whole reply in one frame, not the four the stub streamed as deltas.
    assert assistant["text"] == "echo: hello "


def test_resume_replays_job_transitions_between_the_messages_they_happened_between(
    client, repository
):
    """The fan-out's safety net, and the reason it is allowed to be lossy.

    A `job_status` sent live to a socket that was not connected is simply gone.
    What makes that acceptable is this: the transition is a numbered row in the
    same log as the messages, so a reconnecting client is told about it by the
    same cursor that replays everything else — in the position it actually
    happened in, not at the end.
    """
    job_id = uuid.uuid4()
    with client.websocket_connect(URL) as ws:
        _say_and_settle(ws, "run the backtest", "c1")  # seq 1 and 2
        repository.record_job_event(uuid.UUID(SESSION), job_id, JobState.QUEUED)
        repository.record_job_event(uuid.UUID(SESSION), job_id, JobState.RUNNING)

        replayed = _resume(ws, 0)

    assert [f["type"] for f in replayed] == ["message", "message", "job_status", "job_status"]
    assert [f["seq"] for f in replayed] == [1, 2, 3, 4]

    transitions = replayed[2:]
    # The job's own identity, not the message's: one message spawns several
    # transitions, and the client watches the job.
    assert {f["job_id"] for f in transitions} == {job_id.hex}
    assert [f["state"] for f in transitions] == [JobState.QUEUED, JobState.RUNNING]
    # No progress key on either — not a null one. A percentage is not a durable
    # fact and does not ship here; `job_progress` is phase 5, unnumbered, and
    # never replayed. A key appearing here would mean the split had collapsed.
    assert all("progress" not in f for f in transitions)


def test_a_transition_counts_against_the_resume_bound_like_any_other_entry(client, repository):
    """The bound is on the log, not on the messages in it.

    Counting only messages would let a solve's transitions push a replay past a
    limit that exists to stop one — and the client would be handed a gap it
    cannot see, which is the exact thing `resume_too_old` refuses to do.
    """
    app.dependency_overrides[deps.get_settings] = lambda: Settings(ws_resume_max_messages=2)

    with client.websocket_connect(URL) as ws:
        _say_and_settle(ws, "hello", "c1")  # two messages: at the bound
        repository.record_job_event(uuid.UUID(SESSION), uuid.uuid4(), JobState.QUEUED)

        (err,) = _resume(ws, 0)

    assert err["type"] == "error"
    assert err["code"] == ErrorCode.RESUME_TOO_OLD


def test_resume_is_exclusive_of_the_clients_cursor(client):
    """`last_seq` is what the client already applied, so replay starts after it.

    Inclusive would redeliver the message the client is holding — harmless only
    because clients dedupe on `seq`, and leaning on that to paper over an
    off-by-one is how the dedupe stops being a safety net.
    """
    with client.websocket_connect(URL) as ws:
        _say_and_settle(ws, "hello", "c1")
        replayed = _resume(ws, 1)

    assert [f["seq"] for f in replayed] == [2]
    assert replayed[0]["role"] == Role.ASSISTANT


def test_resume_at_the_head_replays_nothing(client):
    """A client that missed nothing gets nothing, and must not wait for it."""
    with client.websocket_connect(URL) as ws:
        _say_and_settle(ws, "hello", "c1")

        assert _resume(ws, 2) == []


def test_a_resume_further_back_than_the_bound_is_refused_not_truncated(client):
    """Truncating would hand the client a gap it cannot detect.

    It would advance its cursor past frames it never received and never ask for
    them again. `resume_too_old` says the cursor is unusable, which is the one
    answer that sends the client to a full reload instead.
    """
    app.dependency_overrides[deps.get_settings] = lambda: Settings(ws_resume_max_messages=2)

    with client.websocket_connect(URL) as ws:
        _say_and_settle(ws, "one", "c1")  # seq 1, 2
        _say_and_settle(ws, "two", "c2")  # seq 3, 4
        replayed = _resume(ws, 0)  # four messages, bound is two

    assert [f["type"] for f in replayed] == ["error"]
    assert replayed[0]["code"] == ErrorCode.RESUME_TOO_OLD
    # Not retryable: the same cursor fails the same way forever. The client has
    # to reload over HTTP, not back off and ask again.
    assert replayed[0]["retryable"] is False


def test_resume_exactly_at_the_bound_still_replays(client):
    """The boundary the `limit + 1` query exists to get right.

    Off by one here and a client sitting exactly on the bound is told to reload
    for history the server was perfectly willing to send.
    """
    app.dependency_overrides[deps.get_settings] = lambda: Settings(ws_resume_max_messages=2)

    with client.websocket_connect(URL) as ws:
        _say_and_settle(ws, "hello", "c1")  # exactly two rows
        replayed = _resume(ws, 0)

    assert [f["type"] for f in replayed] == ["message", "message"]


def test_resume_hands_back_a_failed_reply_rather_than_hiding_it(client):
    """A turn that died is a fact the client needs, not one to omit.

    Replaying only completed rows would leave the client waiting forever for a
    reply that is never coming — the spinner that never resolves, moved from the
    original socket onto the reconnected one.
    """

    class FailingResponder:
        async def respond(self, text: str):
            raise RuntimeError("boom")
            yield ""  # pragma: no cover - makes this an async generator

    app.dependency_overrides[deps.get_responder] = FailingResponder

    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": "hello", "client_msg_id": "c1"})
        assert ws.receive_json()["type"] == "ack"
        assert ws.receive_json()["type"] == "error"
        replayed = _resume(ws, 1)

    assert [f["seq"] for f in replayed] == [2]
    assert replayed[0]["status"] == Status.FAILED


def test_resume_is_answered_even_at_the_turn_cap(client):
    """Replay starts no turn, so the turn cap must not gate it.

    A client that reconnected mid-solve is both the most likely to be at the cap
    and the most in need of what it missed; refusing there would deny history to
    exactly the session that lost some.
    """

    class SlowResponder:
        async def respond(self, text: str):
            await asyncio.sleep(30)
            yield text  # pragma: no cover - the test never lets it get here

    app.dependency_overrides[deps.get_responder] = SlowResponder
    app.dependency_overrides[deps.get_settings] = lambda: Settings(ws_max_concurrent_turns=1)

    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "user_message", "text": "hello", "client_msg_id": "c1"})
        assert ws.receive_json()["type"] == "ack"

        # At the cap: a second message is refused...
        ws.send_json({"type": "user_message", "text": "again", "client_msg_id": "c2"})
        assert ws.receive_json()["code"] == ErrorCode.TOO_MANY_TURNS

        replayed = _resume(ws, 0)  # ...but resume is not a turn.

    assert [f["type"] for f in replayed] == ["message", "message"]


def test_a_negative_resume_cursor_is_a_bad_frame(client):
    """`seq` starts at 1, so 0 already means "I have nothing".

    A negative cursor can only be a client bug, and quietly widening the range
    for it would hide that rather than surface it.
    """
    with client.websocket_connect(URL) as ws:
        ws.send_json({"type": "resume", "last_seq": -1})
        assert ws.receive_json()["code"] == ErrorCode.BAD_FRAME


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
    """uuid.UUID() parses all of these; the server still refuses them.

    Policy, not correctness: the registry keys on the parsed UUID, so every
    spelling already lands in one bucket and the per-session cap holds either
    way. What the check buys is that one session never appears under five
    different URLs in the logs — and that a client sending a spelling the server
    never mints hears about it, rather than being quietly accommodated.
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

    The read loop awaits `_dispatch` to completion before pulling the next
    frame, and `_dispatch` adds the turn to the set before it returns, so this
    needs no sleep on the test side to be deterministic — only a responder slow
    enough that the first turn has not finished.
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
# Connection plumbing, against a mock socket
# --------------------------------------------------------------------------


class MockWebSocket:
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

    class SilentWebSocket(MockWebSocket):
        async def receive_text(self) -> str:
            await asyncio.Event().wait()  # a peer that never sends and never leaves
            raise AssertionError("unreachable")

    conn = Connection(SilentWebSocket(), uuid.uuid4(), send_queue_size=8)
    serving = asyncio.create_task(
        ConnectionHandler(conn, MockChatRepository(), StubResponder(), Settings()).serve()
    )
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
    conn = Connection(MockWebSocket(), uuid.uuid4(), send_queue_size=2)
    for _ in range(6):
        conn.send(Pong())

    # wait_for, not a bare await: if the bound is ever removed, run_writer blocks
    # on an empty queue forever. That must surface as a failure, not as a CI job
    # that hangs until its timeout.
    assert await asyncio.wait_for(conn.run_writer(), timeout=2.0) == WS_TRY_AGAIN_LATER


async def test_drain_sends_going_away_then_closes_with_service_restart():
    """Kubernetes does not drain WebSockets; this is what stands in for it."""
    mock = MockWebSocket()
    conn = Connection(mock, uuid.uuid4(), send_queue_size=8)
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
    assert json.loads(mock.sent[0])["code"] == ErrorCode.GOING_AWAY


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

    class FailingWebSocket(MockWebSocket):
        async def send_text(self, text: str) -> None:
            raise exc

    conn = Connection(FailingWebSocket(), uuid.uuid4(), send_queue_size=8)
    conn.send(Pong())

    assert await asyncio.wait_for(conn.run_writer(), timeout=2.0) == WS_NORMAL


async def test_drain_does_not_wait_past_its_timeout():
    """A connection that will not flush must not hold the pod past its grace period."""
    conn = Connection(MockWebSocket(), uuid.uuid4(), send_queue_size=8)  # nothing ever drains it
    registry = ConnectionRegistry(max_per_session=4, drain_timeout_seconds=0.05)
    registry.add(conn)

    await asyncio.wait_for(
        registry.drain(Error(code=ErrorCode.GOING_AWAY, message="bye", retryable=True)),
        timeout=2.0,
    )


def test_the_registry_refuses_a_connection_past_the_cap():
    """The cap itself, with no socket in the way to turn a regression into a hang."""
    registry = ConnectionRegistry(max_per_session=1, drain_timeout_seconds=1.0)
    session = uuid.uuid4()

    assert registry.add(Connection(MockWebSocket(), session, send_queue_size=8)) is True
    assert registry.add(Connection(MockWebSocket(), session, send_queue_size=8)) is False
    # A different session is unaffected — the cap is per session, not per replica.
    assert registry.add(Connection(MockWebSocket(), uuid.uuid4(), send_queue_size=8)) is True


def test_the_registry_forgets_sessions_it_no_longer_holds():
    """Otherwise a long-lived process accumulates an entry per session ever seen."""
    registry = ConnectionRegistry(max_per_session=2, drain_timeout_seconds=1.0)
    session = uuid.uuid4()
    conn = Connection(MockWebSocket(), session, send_queue_size=8)

    registry.add(conn)
    assert registry.for_session(session) == [conn]
    registry.remove(conn)

    assert len(registry) == 0
    assert registry.for_session(session) == []


# --------------------------------------------------------------------------
# Phase 1b acceptance, against the real database
# --------------------------------------------------------------------------


@pytest.fixture
def pg_client(postgres_chat_repo):
    """A client with no repository override, so the app's own wiring is used.

    The acceptance test is the one place a mock proves nothing: what it checks
    is that durable state and the transport agree, and a mock only ever agrees
    with itself. `lifespan` already builds a real `PostgresChatRepository`, so
    the way to get one here is to override nothing.

    `postgres_chat_repo` is requested for the two things it does *not* hand over —
    the skip when no database answers, and the row cleanup afterwards. Its pool
    belongs to the fixture's event loop while TestClient runs the app on its
    own, and an asyncpg connection cannot cross loops: passing that repository
    into the app fails on the first query, loudly and confusingly.
    """
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.mark.postgres
def test_a_socket_killed_mid_turn_loses_nothing(pg_client, created_sessions):
    """roadmap.md's sentence for the whole phase, executed.

    A solve runs 30 seconds to five minutes. If a dropped connection can lose
    one, no amount of reconnect logic repairs the product — so every frame the
    client received has to be reconstructible from Postgres alone.

    What survives the kill, and what does not, is the honest 1b answer: the
    user's question is durable and replays verbatim; a completed reply replays
    whole; and the reply that was mid-flight comes back `failed` rather than
    `streaming`, so the client renders a failure instead of a spinner that never
    resolves. The *solve itself* surviving its socket is phase 2's worker, not
    this phase's transport.
    """
    session = uuid.uuid4()
    created_sessions.append(session)
    url = f"/ws/chat/{session}"

    # One turn that finishes normally, so the replay has something whole in it.
    with pg_client.websocket_connect(url) as ws:
        _say_and_settle(ws, "first", "c1")

    # A second turn that will still be running when the socket dies.
    class SlowResponder:
        async def respond(self, text: str):
            await asyncio.sleep(30)
            yield text  # pragma: no cover - the kill lands first

    app.dependency_overrides[deps.get_responder] = SlowResponder

    with pg_client.websocket_connect(url) as ws:
        ws.send_json({"type": "user_message", "text": "second", "client_msg_id": "c2"})
        ack = ws.receive_json()
        assert ack["type"] == "ack"
        assert ack["reply_message_id"] is not None  # a turn really did start
        # Leaving the block kills the socket with that turn in flight.

    app.dependency_overrides.pop(deps.get_responder, None)

    # A new socket, which shares nothing with the dead one but the session id.
    with pg_client.websocket_connect(url) as ws:
        replayed = _resume(ws, 0)

    assert [(f["role"], f["status"]) for f in replayed] == [
        (Role.USER, Status.COMPLETE),
        (Role.ASSISTANT, Status.COMPLETE),
        (Role.USER, Status.COMPLETE),
        (Role.ASSISTANT, Status.FAILED),
    ]
    assert [f["seq"] for f in replayed] == [1, 2, 3, 4]
    # The questions are verbatim, and the finished answer is whole.
    assert replayed[0]["text"] == "first"
    assert replayed[1]["text"] == "echo: first "
    assert replayed[2]["text"] == "second"
