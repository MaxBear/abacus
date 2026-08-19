"""The chat frame protocol: what a frame *is*, in both directions.

The wire format has exactly one definition — these models. `api/chat.py` builds
no ad-hoc dicts, so a frame that type-checks here is the frame that ships, and
adding a field is a change in one place rather than three.

Nothing here knows how a frame reaches a socket (`core/ws.py`), which frame
answers which (`core/chat_handler.py`), or what fills a reply
(`core/responder.py`). Those depend on this module; it depends on none of them.

Only the frames phase 1a actually honors are defined. `resume` (1b), `cancel`
and `job_status` (2) are specified in docs/websocket.md but deliberately absent
here: a `cancel` that parses and silently does nothing is indistinguishable,
from the client's side, from one that worked. An unknown `type` is rejected as
a bad frame, which is a truthful answer until the phase that implements it.
"""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

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


ClientFrame = Annotated[UserMessage | Ping, Field(discriminator="type")]

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
    for it is running on this connection — a resubmit after a reconnect. There
    is no live stream to name: the original turn died with the socket that
    started it, and the row it left behind is what resume delivers.
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


ServerFrame = Ack | Delta | Done | Error | Pong
