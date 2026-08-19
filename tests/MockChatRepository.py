"""An in-memory stand-in for the `ChatRepository` Protocol in `core/`.

Here rather than in `core/` because it is a test double, not a second
supported implementation: nothing that ships imports this module.
"""

import uuid
from dataclasses import replace

from core.repository import RecordedUserMessage, Role, Status, StoredMessage


class MockChatRepository:
    """`ChatRepository` in dictionaries, for tests that must not need a container.

    Faithful about ordering, gap-freeness, and idempotency *outcomes*. It cannot
    be faithful about the two things that make those outcomes true in Postgres —
    the row lock that serializes concurrent allocation, and `on conflict`'s
    atomicity — because a single-threaded dict has no races to lose. The tests
    that cover those live in the postgres-only section of
    `tests/test_chat_repository.py`, and a passing mock proves nothing about them.
    """

    def __init__(self) -> None:
        self._next_seq: dict[uuid.UUID, int] = {}
        self._rows: dict[uuid.UUID, tuple[uuid.UUID, StoredMessage]] = {}
        self._client_keys: dict[tuple[uuid.UUID, str], uuid.UUID] = {}

    async def ensure_session(self, session_id: uuid.UUID) -> None:
        self._next_seq.setdefault(session_id, 1)

    async def record_user_message(
        self, session_id: uuid.UUID, client_msg_id: str, text: str
    ) -> RecordedUserMessage:
        known = self._client_keys.get((session_id, client_msg_id))
        if known is not None:
            # Checked before allocating, which is exactly the check-then-insert
            # the real implementation may not do. Same answer here only because
            # nothing else can run between these two lines.
            return RecordedUserMessage(message=self._rows[known][1], created=False)

        message = StoredMessage(
            seq=self._allocate(session_id),
            message_id=uuid.uuid4(),
            role=Role.USER,
            status=Status.COMPLETE,
            text=text,
        )
        self._rows[message.message_id] = (session_id, message)
        self._client_keys[session_id, client_msg_id] = message.message_id
        return RecordedUserMessage(message=message, created=True)

    async def start_assistant_message(self, session_id: uuid.UUID) -> StoredMessage:
        message = StoredMessage(
            seq=self._allocate(session_id),
            message_id=uuid.uuid4(),
            role=Role.ASSISTANT,
            status=Status.STREAMING,
            text="",
        )
        self._rows[message.message_id] = (session_id, message)
        return message

    async def complete_assistant_message(self, message_id: uuid.UUID, text: str) -> None:
        self._update(message_id, status=Status.COMPLETE, text=text)

    async def fail_assistant_message(self, message_id: uuid.UUID) -> None:
        self._update(message_id, status=Status.FAILED)

    async def messages_since(
        self, session_id: uuid.UUID, after_seq: int, limit: int
    ) -> list[StoredMessage]:
        rows = [
            message
            for row_session_id, message in self._rows.values()
            if row_session_id == session_id and message.seq > after_seq
        ]
        return sorted(rows, key=lambda m: m.seq)[:limit]

    # -- internals ---------------------------------------------------------

    def _allocate(self, session_id: uuid.UUID) -> int:
        if session_id not in self._next_seq:
            raise LookupError(f"no such chat session: {session_id}")
        seq = self._next_seq[session_id]
        self._next_seq[session_id] = seq + 1
        return seq

    def _update(self, message_id: uuid.UUID, **changes: object) -> None:
        session_id, message = self._rows[message_id]
        self._rows[message_id] = (session_id, replace(message, **changes))
