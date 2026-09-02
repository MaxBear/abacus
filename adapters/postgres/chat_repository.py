"""The Postgres implementation of `core.chat_repository.ChatRepository`.

Infrastructure, so it lives here rather than in `core/`: the Protocol is domain,
the SQL is not. This is what `adapters/postgres/db.py`'s `session()` has been waiting
for since phase 0.

Every method is one transaction, opened and closed inside the method. The
`seq` allocator takes a row lock held until commit, so a caller that could keep
a transaction open would be able to stall every other turn on that session
behind an LLM call.

Written with SQLAlchemy Core against `adapters/postgres/tables.py`, not the ORM:
these are a handful of statements with no object graph, no identity map, and no
lazy loads to want.

Since phase 3 one of them reads `job_events`, a table this file's name does not
advertise. `log_since` says why.
"""

import heapq
import itertools
import uuid

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from adapters.postgres.db import Database
from adapters.postgres.seq import allocate_seq
from adapters.postgres.tables import chat_messages, chat_sessions, job_events
from core.chat_repository import RecordedUserMessage, Role, Status, StoredMessage
from core.jobs import JobEvent, JobState

# The columns a StoredMessage is built from, in one place so the select list and
# the insert's `returning` cannot drift apart.
_MESSAGE_COLUMNS = (
    chat_messages.c.seq,
    chat_messages.c.message_id,
    chat_messages.c.role,
    chat_messages.c.status,
    chat_messages.c.text,
)

_EVENT_COLUMNS = (
    job_events.c.seq,
    job_events.c.job_id,
    job_events.c.state,
)


def _to_event(row) -> JobEvent:
    return JobEvent(seq=row.seq, job_id=row.job_id, state=JobState(row.state))


def _to_message(row) -> StoredMessage:
    return StoredMessage(
        seq=row.seq,
        message_id=row.message_id,
        # Coerced, not cast: a value the check constraint should have made
        # impossible raises here rather than travelling on as a bare string.
        role=Role(row.role),
        status=Status(row.status),
        text=row.text,
    )


class PostgresChatRepository:
    """`ChatRepository` over the tables in `adapters/postgres/tables.py`."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def ensure_session(self, session_id: uuid.UUID) -> None:
        stmt = (
            pg_insert(chat_sessions)
            .values(id=session_id)
            # `do nothing` rather than a no-op `do update`: this runs on every
            # connect, and for an existing session the update form would write a
            # dead tuple each time for no gain.
            .on_conflict_do_nothing(index_elements=[chat_sessions.c.id])
        )
        async with self._db.session() as s:
            await s.execute(stmt)
            await s.commit()

    async def record_user_message(
        self, session_id: uuid.UUID, client_msg_id: str, text: str
    ) -> RecordedUserMessage:
        stmt = (
            pg_insert(chat_messages)
            .values(
                session_id=session_id,
                message_id=uuid.uuid4(),
                role=Role.USER,
                # A user message is whole the moment it arrives; there is
                # nothing to stream and nothing to finish.
                status=Status.COMPLETE,
                text=text,
                client_msg_id=client_msg_id,
                completed_at=func.now(),
            )
            .on_conflict_do_nothing(
                # Names the partial index from tables.py, predicate included —
                # without `index_where` Postgres cannot match a partial index and
                # rejects the statement rather than silently using another one.
                index_elements=[chat_messages.c.session_id, chat_messages.c.client_msg_id],
                index_where=chat_messages.c.client_msg_id.isnot(None),
            )
            .returning(*_MESSAGE_COLUMNS)
        )

        async with self._db.session() as s:
            seq = await allocate_seq(s, session_id)
            row = (await s.execute(stmt.values(seq=seq))).first()

            if row is None:
                # The key was already recorded. Roll back *before* reading, so
                # the number this transaction allocated is released: a duplicate
                # that consumed a `seq` would tear a gap in a log whose whole
                # contract is that it has none.
                await s.rollback()
                existing = await self._by_client_msg_id(s, session_id, client_msg_id)
                return RecordedUserMessage(message=existing, created=False)

            await s.commit()
            return RecordedUserMessage(message=_to_message(row), created=True)

    async def start_assistant_message(self, session_id: uuid.UUID) -> StoredMessage:
        stmt = (
            chat_messages.insert()
            .values(
                session_id=session_id,
                message_id=uuid.uuid4(),
                role=Role.ASSISTANT,
                status=Status.STREAMING,
                # client_msg_id stays NULL: the check constraint in tables.py
                # ties the key to the role in both directions.
            )
            .returning(*_MESSAGE_COLUMNS)
        )
        async with self._db.session() as s:
            seq = await allocate_seq(s, session_id)
            row = (await s.execute(stmt.values(seq=seq))).one()
            await s.commit()
            return _to_message(row)

    async def complete_assistant_message(self, message_id: uuid.UUID, text: str) -> None:
        await self._finish(message_id, status=Status.COMPLETE, text=text)

    async def fail_assistant_message(self, message_id: uuid.UUID) -> None:
        await self._finish(message_id, status=Status.FAILED)

    async def log_since(
        self, session_id: uuid.UUID, after_seq: int, limit: int
    ) -> list[StoredMessage | JobEvent]:
        """Two bounded range scans, merged on `seq`.

        Rather than one query returning a union of two row shapes padded with
        nulls: both inputs are already sorted and already bounded, so merging
        here is three lines, where `union all` buys a select list that has to be
        maintained in two places every time either row shape grows.

        Each side asks for the caller's whole `limit` and the merge truncates —
        the first n of a merge of two sorted streams can only come from the
        first n of each, so neither side needs more.
        """
        async with self._db.session() as s:
            # One snapshot for both statements, which is the one thing this
            # merge needs that a single-table read never did. Under read
            # committed each statement sees its own snapshot, so a message
            # committed between them is missing while a *later* transition is
            # present — a hole in the middle of a replay, and the client
            # advances its cursor past it and never asks again. Read-only, so
            # repeatable read costs nothing here and cannot serialization-fail.
            await s.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            messages = await self._messages_since(s, session_id, after_seq, limit)
            events = await self._job_events_since(s, session_id, after_seq, limit)

        # No tie-break: both tables draw from one allocator, so the two streams
        # interleave with no collisions.
        merged = heapq.merge(messages, events, key=lambda row: row.seq)
        return list(itertools.islice(merged, limit))

    # -- internals ---------------------------------------------------------

    async def _messages_since(
        self, s: AsyncSession, session_id: uuid.UUID, after_seq: int, limit: int
    ) -> list[StoredMessage]:
        """The message half of the log. Private since `log_since` took over.

        It was on the Protocol until phase 3, and lost its last production
        caller when resume started asking for the merged log instead. A Protocol
        method only tests call is the dilution `log_since` exists to avoid.
        """
        stmt = (
            select(*_MESSAGE_COLUMNS)
            .where(chat_messages.c.session_id == session_id, chat_messages.c.seq > after_seq)
            # Served as a single range scan by uq_chat_messages_session_id_seq —
            # equality column first, range column second — with no sort node,
            # because the index already supplies the order.
            .order_by(chat_messages.c.seq)
            .limit(limit)
        )
        return [_to_message(row) for row in (await s.execute(stmt)).all()]

    async def _job_events_since(
        self, s: AsyncSession, session_id: uuid.UUID, after_seq: int, limit: int
    ) -> list[JobEvent]:
        """The other half, served by uq_job_events_session_id_seq the same way.

        Reading `job_events` from the chat repository crosses a domain line the
        name does not advertise. Tolerable because both live in
        `adapters/postgres` and share `tables.py` — the line is a naming one,
        not a transaction or deployment one — but it is a cost, and
        `docs/persistence.md` names it rather than leaving it to be discovered.
        """
        stmt = (
            select(*_EVENT_COLUMNS)
            .where(job_events.c.session_id == session_id, job_events.c.seq > after_seq)
            .order_by(job_events.c.seq)
            .limit(limit)
        )
        return [_to_event(row) for row in (await s.execute(stmt)).all()]

    async def _by_client_msg_id(
        self, s: AsyncSession, session_id: uuid.UUID, client_msg_id: str
    ) -> StoredMessage:
        stmt = select(*_MESSAGE_COLUMNS).where(
            chat_messages.c.session_id == session_id,
            chat_messages.c.client_msg_id == client_msg_id,
        )
        return _to_message((await s.execute(stmt)).one())

    async def _finish(
        self, message_id: uuid.UUID, *, status: Status, text: str | None = None
    ) -> None:
        values = {"status": status, "completed_at": func.now()}
        if text is not None:
            values["text"] = text
        stmt = (
            update(chat_messages).where(chat_messages.c.message_id == message_id).values(**values)
        )
        async with self._db.session() as s:
            await s.execute(stmt)
            await s.commit()
