"""An in-memory stand-in for the `ChatRepository` Protocol in `core/`.

Here rather than in `core/` because it is a test double, not a second
supported implementation: nothing that ships imports this module.
"""

import heapq
import itertools
import uuid
from dataclasses import replace

from core.chat_repository import RecordedUserMessage, Role, Status, StoredMessage
from core.jobs import JobEvent, JobState


class MockChatRepository:
    """`ChatRepository` in dictionaries, for tests that must not need a container.

    Faithful about ordering, gap-freeness, idempotency *outcomes*, and the merge
    of the two sources the session log has had since phase 3. It cannot
    be faithful about the two things that make those outcomes true in Postgres —
    the row lock that serializes concurrent allocation, and `on conflict`'s
    atomicity — because a single-threaded dict has no races to lose. The tests
    that cover those live in the postgres-only section of
    `tests/test_chat_repository.py`, and a passing mock proves nothing about them.
    """

    def __init__(self) -> None:
        self._next_seq: dict[uuid.UUID, int] = {}
        self._chat_messages: dict[uuid.UUID, tuple[uuid.UUID, StoredMessage]] = {}
        self._client_keys: dict[tuple[uuid.UUID, str], uuid.UUID] = {}
        self._job_events: list[tuple[uuid.UUID, JobEvent]] = []

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
            return RecordedUserMessage(message=self._chat_messages[known][1], created=False)

        message = StoredMessage(
            seq=self._allocate(session_id),
            message_id=uuid.uuid4(),
            role=Role.USER,
            status=Status.COMPLETE,
            text=text,
        )
        self._chat_messages[message.message_id] = (session_id, message)
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
        self._chat_messages[message.message_id] = (session_id, message)
        return message

    async def complete_assistant_message(self, message_id: uuid.UUID, text: str) -> None:
        self._update(message_id, status=Status.COMPLETE, text=text)

    async def fail_assistant_message(self, message_id: uuid.UUID) -> None:
        self._update(message_id, status=Status.FAILED)

    async def log_since(
        self, session_id: uuid.UUID, after_seq: int, limit: int
    ) -> list[StoredMessage | JobEvent]:
        messages = self._since(
            ((sid, m) for sid, m in self._chat_messages.values()), session_id, after_seq, limit
        )
        events = self._since(self._job_events, session_id, after_seq, limit)
        # Two sorted lists merged on seq, which is the whole of what the real
        # implementation does once its two queries have returned. What this
        # cannot imitate is the snapshot they share — see `log_since` there.
        merged = heapq.merge(messages, events, key=lambda row: row.seq)
        return list(itertools.islice(merged, limit))

    # -- test affordances --------------------------------------------------

    def record_job_event(
        self, session_id: uuid.UUID, job_id: uuid.UUID, state: JobState
    ) -> JobEvent:
        """Put a job transition in the log, as `PostgresJobStore` would.

        Not on the `ChatRepository` Protocol, and deliberately: nothing writes
        `job_events` through the repository — the job store does, inside the
        transaction that changes the state. This is the seam a test needs to
        stand a transition next to a message without a queue, and it draws from
        the same allocator so the interleaving it produces is the real one.
        """
        event = JobEvent(seq=self._allocate(session_id), job_id=job_id, state=state)
        self._job_events.append((session_id, event))
        return event

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _since(rows, session_id: uuid.UUID, after_seq: int, limit: int) -> list:
        kept = [row for sid, row in rows if sid == session_id and row.seq > after_seq]
        return sorted(kept, key=lambda row: row.seq)[:limit]

    def _allocate(self, session_id: uuid.UUID) -> int:
        if session_id not in self._next_seq:
            raise LookupError(f"no such chat session: {session_id}")
        seq = self._next_seq[session_id]
        self._next_seq[session_id] = seq + 1
        return seq

    def _update(self, message_id: uuid.UUID, **changes: object) -> None:
        session_id, message = self._chat_messages[message_id]
        self._chat_messages[message_id] = (session_id, replace(message, **changes))
