"""The chat frame protocol: what a frame *is*, in both directions.

The wire format has exactly one definition — these models. `api/chat.py` builds
no ad-hoc dicts, so a frame that type-checks here is the frame that ships, and
adding a field is a change in one place rather than three.

Nothing here knows how a frame reaches a socket (`core/ws.py`), which frame
answers which (`core/chat_handler.py`), or what fills a reply
(`core/responder.py`). Those depend on this module; it depends on none of them.

Only the frames the server actually honors are defined. `cancel` and
`job_status` (2) are specified in docs/websocket.md but deliberately absent
here: a `cancel` that parses and silently does nothing is indistinguishable,
from the client's side, from one that worked. An unknown `type` is rejected as
a bad frame, which is a truthful answer until the phase that implements it.
"""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

# Role and Status are spelled once, in the persistence seam, and reused on the
# wire rather than restated here — the same "one list, not two" the tables
# module follows. `message` replays a stored row, so its vocabulary is the
# row's; inventing a parallel set of strings is how the two drift.
from core.repository import Role, Status

# Bumped only for breaking changes. It rides on every frame from the first
# commit because retrofitting a version field requires a flag day: there is no
# way to interpret an unversioned frame once a second version exists.
PROTOCOL_VERSION = 1

# A ceiling, not a product limit. Without one, a single client can hand the
# event loop an arbitrarily large string to parse and echo.
MAX_MESSAGE_CHARS = 8_000


class ErrorCode(StrEnum):
    """Stable identifiers. Clients branch on these; `message` is for humans."""

    BAD_FRAME = "bad_frame"
    UNSUPPORTED_VERSION = "unsupported_version"
    TOO_MANY_TURNS = "too_many_turns"
    TOO_MANY_CONNECTIONS = "too_many_connections"
    GOING_AWAY = "going_away"
    INTERNAL = "internal"
    RESUME_TOO_OLD = "resume_too_old"


# --------------------------------------------------------------------------
# Client → server
# --------------------------------------------------------------------------


class _ClientFrame(BaseModel):
    # extra="forbid" so a typo'd field is a loud bad_frame rather than a value
    # silently ignored — the failure mode where a client "sends" a field for
    # weeks and nobody notices the server never read it.
    model_config = ConfigDict(extra="forbid")

    v: int = PROTOCOL_VERSION


class UserMessage(_ClientFrame):
    type: Literal["user_message"]
    text: str = Field(min_length=1, max_length=MAX_MESSAGE_CHARS)
    # The client's idempotency key. Unused in 1a — there is nothing durable to
    # deduplicate against yet — but echoed in the ack, so the client-side
    # retry logic 1b needs can be written and tested against the real server now.
    client_msg_id: str = Field(min_length=1, max_length=64)


class Ping(_ClientFrame):
    type: Literal["ping"]


class Resume(_ClientFrame):
    """Everything this session recorded after `last_seq`, please.

    Sent immediately after reconnect, before anything else: the client cannot
    know whether the frames it missed matter until it has them, and a message
    submitted first would interleave its ack into the replay.

    `last_seq` is the highest `seq` the client has applied, so the reply starts
    at `last_seq + 1`. Zero is the honest opening bid from a client with no
    history — a fresh tab, or one that dropped its state — and asks for the
    session from the beginning.
    """

    type: Literal["resume"]
    # Not negative: `seq` starts at 1, so 0 already means "I have nothing".
    # A negative cursor could only be a client bug, and silently widening the
    # range for it would hide that.
    last_seq: int = Field(ge=0)


ClientFrame = Annotated[UserMessage | Ping | Resume, Field(discriminator="type")]

# Built once at import. TypeAdapter compiles a validator; constructing one per
# frame would put that cost on every inbound message.
CLIENT_FRAME_ADAPTER: TypeAdapter[ClientFrame] = TypeAdapter(ClientFrame)


# --------------------------------------------------------------------------
# Server → client
# --------------------------------------------------------------------------


class _ServerFrame(BaseModel):
    v: int = PROTOCOL_VERSION


class Ack(_ServerFrame):
    """The message is durable: it is a row in this session, at this `seq`.

    Two identities, because a turn is two rows. `message_id` names the user's
    message — the thing being acknowledged. `reply_message_id` names the
    assistant row that `delta` and `done` will carry, which is what lets the
    client bind the stream to the bubble it drew before either row existed.

    `reply_message_id` is absent when the key was already recorded and no turn
    for it is running *on this connection* — which covers three situations, not
    one: the turn finished here already, it died with the socket that started
    it, or it is running on another of this session's sockets. The frame does
    not distinguish them, because the client's next move is the same in all
    three: stop waiting for deltas here and ask `resume` what the row says.
    """

    type: Literal["ack"] = "ack"
    client_msg_id: str
    seq: int
    message_id: str
    reply_message_id: str | None = None


class Delta(_ServerFrame):
    """One chunk of an assistant turn.

    Addressed by (message_id, chunk_index) rather than a session-wide sequence
    number, which is what lets several turns stream concurrently on one socket
    without the client having to guess which reply a chunk belongs to.
    """

    type: Literal["delta"] = "delta"
    message_id: str
    chunk_index: int
    text: str


class Done(_ServerFrame):
    """The reply is complete and its text is final.

    Carries the assistant row's `seq`, allocated when that row was opened rather
    than now — so `seq` orders messages by creation, and interleaved turns still
    sort into a readable transcript. `delta` carries none: a chunk is a
    rendering detail, not a durable fact.
    """

    type: Literal["done"] = "done"
    message_id: str
    seq: int


class Error(_ServerFrame):
    """An application error. Never a reason to close the socket.

    Closing on a bad message conflates "this request was wrong" with "this
    connection is unusable" — and since clients reconnect on close, that
    conflation turns one malformed frame into a reconnect loop.
    """

    type: Literal["error"] = "error"
    code: ErrorCode
    message: str
    retryable: bool


class Pong(_ServerFrame):
    type: Literal["pong"] = "pong"


class Message(_ServerFrame):
    """One stored message, replayed whole. Only resume sends this.

    Replay hands over accumulated text rather than re-streaming tokens: the
    client renders by appending, so one frame reaches the same end state as four
    hundred deltas would, and a 500-token reply stays 2 rows and 2 `seq`
    allocations instead of 500 of each.

    `status` is on the wire because a replayed assistant row is not always
    finished. A reply whose socket died mid-turn comes back `failed`, and a
    client that assumed every replayed row was `complete` would render a
    truncated answer as though the model had meant to stop there.
    """

    type: Literal["message"] = "message"
    seq: int
    message_id: str
    role: Role
    status: Status
    text: str


ServerFrame = Ack | Delta | Done | Error | Pong | Message
