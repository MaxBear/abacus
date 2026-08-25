"""The persistence seam: what the chat protocol needs from a database.

Kept in `core/` and expressed as a Protocol for the same reason `Responder` is:
the handler must be exercisable without a container. `adapters/` supplies the
Postgres implementation, tests supply a fake, and neither the handler nor its
tests import SQLAlchemy.

Narrow on purpose. Every method here is one short transaction that returns
plain data — no sessions, no rows, no ORM objects cross this line. That is what
lets `docs/persistence.md`'s rule hold: the `seq` allocator takes a row lock
held to commit, so no LLM call and no socket write may sit inside one.
"""

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


class Role(StrEnum):
    """Who produced a message.

    `StrEnum`, so a member *is* its value: it binds to a Text column without a
    conversion, compares equal to the plain string a query returns, and needs no
    special casing to serialize.
    """

    USER = "user"
    ASSISTANT = "assistant"


class Status(StrEnum):
    """Where a message is in its life.

    Only assistant rows ever leave `COMPLETE`: a user message is whole the
    moment it arrives.
    """

    STREAMING = "streaming"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class StoredMessage:
    """One durable message, as the wire needs to see it.

    `seq` and `message_id` are both here because they answer different
    questions: `seq` is this session's ordering and the resume cursor,
    `message_id` is the opaque identity the client uses to address a stream.
    """

    seq: int
    message_id: uuid.UUID
    role: Role
    status: Status
    text: str


@dataclass(frozen=True, slots=True)
class RecordedUserMessage:
    """A stored user message, plus whether *this* call is what stored it.

    `created=False` means the `client_msg_id` was already recorded, so the
    caller must re-`ack` the existing row rather than start a second turn. The
    flag is the whole point of the return type: the row alone cannot tell a
    first submit from a replayed one.
    """

    message: StoredMessage
    created: bool


class ChatRepository(Protocol):
    """Durable storage for one chat session's messages.

    Implementations own their own transactions. A caller never sees a session,
    never commits, and cannot hold a lock open across an await it does not
    control.
    """

    async def ensure_session(self, session_id: uuid.UUID) -> None:
        """Create the session row if it is not there yet.

        Idempotent, because it runs on every connect and a session outlives its
        connections. Nothing here decides whether a session *may* exist — the
        route already settled that.
        """
        ...

    async def record_user_message(
        self, session_id: uuid.UUID, client_msg_id: str, text: str
    ) -> RecordedUserMessage:
        """Store a user message, or recover the one this key already stored.

        Allocates the next `seq` and inserts in one transaction, so a rejected
        duplicate consumes no number. See `docs/persistence.md` — this is the
        one place where check-then-insert would be a real bug rather than a
        style problem.
        """
        ...

    async def start_assistant_message(self, session_id: uuid.UUID) -> StoredMessage:
        """Open an assistant row as `streaming`, with empty text.

        Allocated and inserted *before* the first chunk exists, so the row is
        already numbered and already findable when a client reconnects mid-turn.
        Allocating at `done` instead would make `seq` encode completion order,
        and interleaved turns would then sort wrong.
        """
        ...

    async def complete_assistant_message(self, message_id: uuid.UUID, text: str) -> None:
        """Store the finished reply and mark the row complete.

        One write at the end, not one per chunk. Appending on every delta would
        be 500 updates and 500 dead tuples for a 500-token reply — the same cost
        the two-rows-per-turn design exists to avoid.

        The price is that a reconnect mid-turn currently finds empty text. That
        is deliberate for now: the client is re-sent nothing rather than
        something stale. A periodic checkpoint is the fix, and it belongs with
        `resume` where its cadence can be argued against a real requirement.
        """
        ...

    async def fail_assistant_message(self, message_id: uuid.UUID) -> None:
        """Mark a turn that raised as failed.

        Without this a crashed turn leaves a row at `streaming` forever, which a
        client cannot distinguish from one still in flight — it renders a
        half-sentence under a spinner that never resolves.
        """
        ...

    async def messages_since(
        self, session_id: uuid.UUID, after_seq: int, limit: int
    ) -> list[StoredMessage]:
        """History after `after_seq`, in `seq` order, at most `limit`.

        Every row, in whatever state it holds — not just completed ones. A reply
        whose socket died mid-turn is `failed`, and resume has to hand that back
        rather than hide it: a client told nothing would wait forever for a turn
        that is never coming.

        The bound is not politeness: unbounded replay turns a week-old tab into
        a full history dump on a single reconnect.
        """
        ...
