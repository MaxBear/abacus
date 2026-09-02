"""The per-session `seq` allocator, in one place because two tables draw from it.

`chat_messages` had it to itself until phase 3. `job_events` now takes numbers
from the same counter — that is what makes a job transition replayable by the
same cursor that replays messages — so the UPDATE that mints them belongs to
neither table's module. Copying it into the second caller would be two row
locks that have to stay identical forever, which is the drift this file exists
to prevent.

See `docs/persistence.md`. The cost of the sharing is recorded there too: the
job store now takes a lock live turns take.
"""

import uuid

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from adapters.postgres.tables import chat_sessions


async def allocate_seq(s: AsyncSession, session_id: uuid.UUID) -> int:
    """Take the next `seq` for this session, gap-free.

    A Postgres sequence is the wrong tool: it is global rather than per-session,
    and rollbacks burn values. This UPDATE takes a row lock held to commit, so
    concurrent writers on one session serialize here and cannot mint the same
    number, and a rolled-back transaction consumes nothing.

    That lock is also why `seq` order *is* commit order: nothing can commit a
    higher number while a lower one is still in flight. `log_since` leans on it
    — a reader that sees `seq` n has already been able to see everything below
    it, so a merge of two tables cannot open a hole.

    Runs in the caller's transaction, deliberately. The number and the row that
    carries it have to commit together, or a crash between them tears the gap
    the whole contract is that there are none.
    """
    stmt = (
        update(chat_sessions)
        .where(chat_sessions.c.id == session_id)
        .values(next_seq=chat_sessions.c.next_seq + 1, last_seen_at=func.now())
        .returning(chat_sessions.c.next_seq - 1)
    )
    row = (await s.execute(stmt)).first()
    if row is None:
        # Every caller runs behind ensure_session, or behind a foreign key that
        # says the session exists, so this is a bug rather than a condition to
        # recover from — but an explicit error beats the NOT NULL violation the
        # insert would raise two lines later.
        raise LookupError(f"no such chat session: {session_id}")
    return row[0]
