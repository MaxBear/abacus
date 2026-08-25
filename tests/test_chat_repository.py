"""Phase 1b: the persistence contract, run against both implementations.

Every test in the first section runs twice — once against the in-memory mock,
once against real Postgres if one is reachable. That is the point: a mock the
handler's tests trust is only useful if something proves it does not lie.

The second section is postgres-only, and covers the two claims
`docs/persistence.md` says a mock cannot falsify — gap-free allocation under
concurrency, and the atomicity of the idempotent insert. Both are properties of
a row lock and of `on conflict`, neither of which a dict has.

Postgres tests skip rather than fail when no database is reachable, so
`make test` stays container-free; `make up && make migrate` turns them on with
no flag to remember.
"""

import asyncio
import contextlib
import uuid

import pytest
from sqlalchemy import text

from core.chat_repository import Role, Status
from tests.MockChatRepository import MockChatRepository


@pytest.fixture(params=["mock", "postgres"])
def repo(request):
    """The contract under test, once per implementation.

    `getfixturevalue` rather than requesting `postgres_repo` outright: naming it
    in the signature would build a database connection for the `mock` run too,
    and skip both when none is reachable.
    """
    if request.param == "mock":
        return MockChatRepository()
    return request.getfixturevalue("postgres_repo")


@pytest.fixture
async def session_id(repo, created_sessions) -> uuid.UUID:
    sid = uuid.uuid4()
    created_sessions.append(sid)
    await repo.ensure_session(sid)
    return sid


# --------------------------------------------------------------------------
# The contract, both implementations
# --------------------------------------------------------------------------


async def test_ensure_session_is_idempotent(repo, session_id):
    # Runs on every connect, and a session outlives its connections.
    await repo.ensure_session(session_id)
    await repo.ensure_session(session_id)

    recorded = await repo.record_user_message(session_id, "c1", "hi")
    assert recorded.message.seq == 1, "a second ensure_session must not reset the allocator"


async def test_a_turn_takes_two_contiguous_seqs(repo, session_id):
    user = await repo.record_user_message(session_id, "c1", "hi")
    assistant = await repo.start_assistant_message(session_id)
    second_user = await repo.record_user_message(session_id, "c2", "again")

    assert [user.message.seq, assistant.seq, second_user.message.seq] == [1, 2, 3]
    assert user.message.message_id != assistant.message_id


async def test_user_and_assistant_rows_differ_in_role_and_status(repo, session_id):
    user = await repo.record_user_message(session_id, "c1", "hi")
    assistant = await repo.start_assistant_message(session_id)

    assert (user.message.role, user.message.status) == (Role.USER, Status.COMPLETE)
    assert (assistant.role, assistant.status) == (Role.ASSISTANT, Status.STREAMING)
    # Opened before the first chunk exists, so a mid-turn reconnect finds a row.
    assert assistant.text == ""


async def test_a_replayed_client_msg_id_returns_the_original_row(repo, session_id):
    first = await repo.record_user_message(session_id, "c7", "solve this")
    again = await repo.record_user_message(session_id, "c7", "solve this")

    assert first.created is True
    assert again.created is False
    # The client must be able to treat the second ack as the first one arriving
    # late, so both identities have to match.
    assert again.message.seq == first.message.seq
    assert again.message.message_id == first.message.message_id


async def test_a_replayed_message_consumes_no_seq(repo, session_id):
    await repo.record_user_message(session_id, "c7", "solve this")
    await repo.record_user_message(session_id, "c7", "solve this")
    following = await repo.record_user_message(session_id, "c8", "next")

    # 3 would mean the rejected duplicate burned a number, tearing a gap in a
    # log whose whole contract is that it has none.
    assert following.message.seq == 2


async def test_the_same_key_in_another_session_is_a_different_message(repo, created_sessions):
    one, two = uuid.uuid4(), uuid.uuid4()
    created_sessions.extend([one, two])
    await repo.ensure_session(one)
    await repo.ensure_session(two)

    first = await repo.record_user_message(one, "c1", "hi")
    second = await repo.record_user_message(two, "c1", "hi")

    assert second.created is True
    assert second.message.message_id != first.message.message_id


async def test_completing_a_turn_stores_the_accumulated_text(repo, session_id):
    assistant = await repo.start_assistant_message(session_id)
    await repo.complete_assistant_message(assistant.message_id, "Hello there")

    (stored,) = await repo.messages_since(session_id, after_seq=0, limit=10)
    assert stored.status == Status.COMPLETE
    assert stored.text == "Hello there"


async def test_a_failed_turn_is_marked_not_left_streaming(repo, session_id):
    assistant = await repo.start_assistant_message(session_id)
    await repo.fail_assistant_message(assistant.message_id)

    (stored,) = await repo.messages_since(session_id, after_seq=0, limit=10)
    # Left at streaming, a client renders a half-sentence under a spinner that
    # never resolves, indistinguishable from a turn still in flight.
    assert stored.status == Status.FAILED


async def test_messages_since_is_ordered_exclusive_and_bounded(repo, session_id):
    for n in range(5):
        await repo.record_user_message(session_id, f"c{n}", f"m{n}")

    assert [m.seq for m in await repo.messages_since(session_id, 0, 10)] == [1, 2, 3, 4, 5]
    assert [m.seq for m in await repo.messages_since(session_id, 2, 10)] == [3, 4, 5]
    assert [m.seq for m in await repo.messages_since(session_id, 0, 2)] == [1, 2]
    assert await repo.messages_since(session_id, 5, 10) == []


async def test_another_sessions_messages_are_never_returned(repo, created_sessions):
    mine, theirs = uuid.uuid4(), uuid.uuid4()
    created_sessions.extend([mine, theirs])
    await repo.ensure_session(mine)
    await repo.ensure_session(theirs)
    await repo.record_user_message(theirs, "c1", "not yours")

    assert await repo.messages_since(mine, 0, 10) == []


async def test_an_unknown_session_raises_rather_than_inventing_one(repo):
    with pytest.raises(LookupError):
        await repo.record_user_message(uuid.uuid4(), "c1", "hi")


# --------------------------------------------------------------------------
# Postgres only: what the mock cannot falsify
# --------------------------------------------------------------------------


@pytest.fixture
def pg(postgres_repo):
    """The shared postgres fixture, under the short name these tests read with."""
    return postgres_repo


@pytest.fixture
async def pg_session(pg, created_sessions) -> uuid.UUID:
    sid = uuid.uuid4()
    created_sessions.append(sid)
    await pg.ensure_session(sid)
    return sid


async def _warm_pool(repo, n: int) -> None:
    """Hold n connections open at once, so the pool has really created them.

    A session opens no connection until a statement runs on it, so this has to
    execute something. Without it the racing tasks below serialize on connection
    establishment instead of on the row lock, and each one reads a database the
    previous task has already committed to — the race under test never happens,
    and the test passes against an implementation that has the bug.
    """
    async with contextlib.AsyncExitStack() as stack:
        sessions = await asyncio.gather(
            *(stack.enter_async_context(repo._db.session()) for _ in range(n))
        )
        await asyncio.gather(*(s.execute(text("select 1")) for s in sessions))


@pytest.mark.postgres
async def test_concurrent_allocation_is_gap_free(pg, pg_session):
    """The row-lock argument, falsifiable.

    Without the lock two turns read the same `next_seq` and both insert it —
    which the unique constraint on (session_id, seq) turns into a crash, not
    silent corruption, but a crash on the submit path all the same.
    """
    recorded = await asyncio.gather(
        *(pg.record_user_message(pg_session, f"c{n}", f"m{n}") for n in range(40))
    )

    seqs = sorted(r.message.seq for r in recorded)
    assert seqs == list(range(1, 41)), "distinct and contiguous, in any interleaving"


@pytest.mark.postgres
async def test_racing_the_same_client_msg_id_stores_one_row(pg, pg_session):
    """Two reconnecting tabs replaying one message — the race `on conflict` exists for.

    Check-then-insert passes every sequential test above and loses precisely
    here: both reads find nothing before either write lands.
    """
    await _warm_pool(pg, 8)
    recorded = await asyncio.gather(
        *(pg.record_user_message(pg_session, "c7", "hi") for _ in range(8))
    )

    assert sum(r.created for r in recorded) == 1
    assert len({r.message.message_id for r in recorded}) == 1
    assert len({r.message.seq for r in recorded}) == 1

    stored = await pg.messages_since(pg_session, 0, 10)
    assert len(stored) == 1
    assert stored[0].seq == 1, "the seven losers must not have burned numbers"
