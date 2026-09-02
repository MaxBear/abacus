"""Phase 3: one `job_events` row per transition, asserted where it can be.

This module exists because the contract suite cannot. `job_events` sits *below*
the `JobQueue` Protocol — `MemoryJobQueue` has no `chat_sessions`, no allocator,
and no session log to write into — so `test_job_queue.py` runs over both
implementations and never asks about events. That is a knowing divergence and
`docs/persistence.md` argues it, but it leaves a hole with a name: add a seventh
transition to `PostgresJobStore`, forget its event row, and the 35-test suite
stays green.

These assertions are what closes it, which is why the document lists them as an
obligation rather than a note. All six transitions are covered here —
`insert_or_get`, `claim`, `ack`, `nack`, `cancel`, `retire_lapsed` — against the
real database, with no broker: the store is the row half, and none of this needs
anything delivered.

Postgres-marked, so they skip when no database answers.
"""

import asyncio
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import select

from adapters.postgres.job_store import PostgresJobStore
from adapters.postgres.tables import job_events
from core.jobs import JobRequest, JobState

pytestmark = pytest.mark.postgres

# Long enough that a lease does not lapse mid-assertion on a loaded box, short
# enough that the expiry tests stay quick — `test_job_queue.py`'s UNIT, at the
# same scale and for the same reason.
UNIT = timedelta(milliseconds=120)

OWNER = "worker-under-test"


@pytest.fixture
def job_store(postgres_job_store) -> PostgresJobStore:
    """The shared fixture, under the short name these tests read with."""
    return postgres_job_store


@pytest.fixture
async def session_id(postgres_chat_repo, created_sessions) -> uuid.UUID:
    """A real session, cleaned up by `postgres_chat_repo` afterwards.

    The repository is here for `ensure_session` and for its teardown: deleting
    the session cascades to its jobs and to their events, so nothing in this
    module has to remember what it wrote.
    """
    sid = uuid.uuid4()
    created_sessions.append(sid)
    await postgres_chat_repo.ensure_session(sid)
    return sid


async def _log(postgres_db, session_id: uuid.UUID) -> list[tuple[int, uuid.UUID, str]]:
    """Every event for this session, in `seq` order: what resume would replay."""
    stmt = (
        select(job_events.c.seq, job_events.c.job_id, job_events.c.state)
        .where(job_events.c.session_id == session_id)
        .order_by(job_events.c.seq)
    )
    async with postgres_db.session() as s:
        return [(r.seq, r.job_id, r.state) for r in (await s.execute(stmt)).all()]


async def _enqueue(job_store, session_id, **kwargs):
    job, _created = await job_store.insert_or_get(
        JobRequest(kind="synthetic", payload={}, session_id=session_id, **kwargs)
    )
    return job


# --------------------------------------------------------------------------
# The six
# --------------------------------------------------------------------------


async def test_enqueue_numbers_the_first_transition(job_store, postgres_db, session_id):
    job = await _enqueue(job_store, session_id)

    assert await _log(postgres_db, session_id) == [(1, job.id, JobState.QUEUED)]


async def test_a_duplicate_enqueue_is_not_a_second_transition(job_store, postgres_db, session_id):
    """The row is the same row, so there is nothing new to tell anyone.

    Numbering it would say the same job was queued twice, and burn a `seq`
    saying it — which is the failure `record_user_message` rolls back to avoid.
    """
    job = await _enqueue(job_store, session_id, idempotency_key="k")
    again, created = await job_store.insert_or_get(
        JobRequest(kind="synthetic", payload={}, session_id=session_id, idempotency_key="k")
    )

    assert created is False
    assert again.id == job.id
    assert await _log(postgres_db, session_id) == [(1, job.id, JobState.QUEUED)]


async def test_a_full_life_is_three_rows_at_three_numbers(job_store, postgres_db, session_id):
    job = await _enqueue(job_store, session_id)
    lease = await job_store.claim(job.id, owner=OWNER, lease=UNIT * 8)
    await job_store.ack(lease, result_ref="jobs/whatever")

    assert await _log(postgres_db, session_id) == [
        (1, job.id, JobState.QUEUED),
        (2, job.id, JobState.RUNNING),
        (3, job.id, JobState.DONE),
    ]


async def test_a_nack_records_what_the_row_became_not_what_the_consumer_said(
    job_store, postgres_db, session_id
):
    """`failed` and `dead` are different frames, and the row decides which.

    The consumer passes the same `error` either way; whether the job goes round
    again is `nack`'s arithmetic on `attempts`, so reading the state back out of
    the statement is the only way the event cannot disagree with the row.
    """
    retried = await _enqueue(job_store, session_id, idempotency_key="retried", max_attempts=3)
    lease = await job_store.claim(retried.id, owner=OWNER, lease=UNIT * 8)
    await job_store.nack(lease, error="boom", retry_in=UNIT)

    last = await _enqueue(job_store, session_id, idempotency_key="last", max_attempts=1)
    lease = await job_store.claim(last.id, owner=OWNER, lease=UNIT * 8)
    await job_store.nack(lease, error="boom")

    assert await _log(postgres_db, session_id) == [
        (1, retried.id, JobState.QUEUED),
        (2, retried.id, JobState.RUNNING),
        (3, retried.id, JobState.FAILED),
        (4, last.id, JobState.QUEUED),
        (5, last.id, JobState.RUNNING),
        (6, last.id, JobState.DEAD),
    ]


async def test_cancelling_an_unreserved_job_is_a_transition(job_store, postgres_db, session_id):
    job = await _enqueue(job_store, session_id)
    await job_store.cancel(job.id)

    assert await _log(postgres_db, session_id) == [
        (1, job.id, JobState.QUEUED),
        (2, job.id, JobState.CANCELLED),
    ]


async def test_cancelling_twice_says_it_once(job_store, postgres_db, session_id):
    """Terminal is a no-op, so a client that cancels across a reconnect gets
    one `cancelled` frame rather than one per attempt."""
    job = await _enqueue(job_store, session_id)
    await job_store.cancel(job.id)
    await job_store.cancel(job.id)

    assert [state for _seq, _id, state in await _log(postgres_db, session_id)] == [
        JobState.QUEUED,
        JobState.CANCELLED,
    ]


async def test_cancelling_a_running_job_records_nothing_until_the_consumer_answers(
    job_store, postgres_db, session_id
):
    """The half of `cancel` that is a request rather than a transition.

    A running job keeps its state and its lease; all that changed is a flag the
    consumer will see on its next `extend`. Writing `cancelled` here would tell
    a client the solve had stopped while a process was still inside numpy —
    and the row would then transition a second time when it really did stop.
    """
    job = await _enqueue(job_store, session_id)
    lease = await job_store.claim(job.id, owner=OWNER, lease=UNIT * 8)
    await job_store.cancel(job.id)

    assert [state for _seq, _id, state in await _log(postgres_db, session_id)] == [
        JobState.QUEUED,
        JobState.RUNNING,
    ]

    # And when the consumer does answer, `nack` reaches `cancelled` — not
    # `failed`, whatever error it passes.
    await job_store.nack(lease, error="stopped")
    assert (await _log(postgres_db, session_id))[-1] == (3, job.id, JobState.CANCELLED)


async def test_retire_lapsed_numbers_the_transition_nobody_is_watching(
    job_store, postgres_db, session_id
):
    """The one a publisher bolted on further out would drop.

    Its consumer is gone: there is no `nack` coming, no session in scope, and
    nothing else in the system will ever say what happened to this job. A client
    waiting on it is exactly the client that most needs the frame.
    """
    job = await _enqueue(job_store, session_id, max_attempts=1)
    await job_store.claim(job.id, owner=OWNER, lease=UNIT)
    await asyncio.sleep(UNIT.total_seconds() * 2)

    # `in`, not `==`: the sweep is global, and another module's lapsed row is
    # entitled to be retired by the same call.
    assert job.id in await job_store.retire_lapsed()
    assert await _log(postgres_db, session_id) == [
        (1, job.id, JobState.QUEUED),
        (2, job.id, JobState.RUNNING),
        (3, job.id, JobState.DEAD),
    ]


async def test_a_lapsed_lease_on_a_cancelled_job_retires_as_cancelled(
    job_store, postgres_db, session_id
):
    job = await _enqueue(job_store, session_id, max_attempts=3)
    await job_store.claim(job.id, owner=OWNER, lease=UNIT)
    await job_store.cancel(job.id)
    await asyncio.sleep(UNIT.total_seconds() * 2)
    await job_store.retire_lapsed()

    # Not `failed`, and not `dead` with attempts to spare: the user asked for
    # this, and the consumer that crashed is the one that never says so.
    assert (await _log(postgres_db, session_id))[-1] == (3, job.id, JobState.CANCELLED)


# --------------------------------------------------------------------------
# What the events are numbered in
# --------------------------------------------------------------------------


async def test_a_job_with_no_session_writes_no_events(job_store, postgres_db):
    """`jobs.session_id` is nullable; `job_events.session_id` is not.

    A scheduled backfill has no conversation, so its transitions are facts about
    nothing anyone can be told. The store has to notice that rather than
    inventing a session — which is what the FK would refuse anyway, loudly and
    at the wrong moment.
    """
    job, _created = await job_store.insert_or_get(JobRequest(kind="synthetic", payload={}))
    lease = await job_store.claim(job.id, owner=OWNER, lease=UNIT * 8)
    await job_store.ack(lease)

    stmt = select(job_events.c.id).where(job_events.c.job_id == job.id)
    async with postgres_db.session() as s:
        assert (await s.execute(stmt)).all() == []


async def test_transitions_and_messages_share_one_gap_free_sequence(
    job_store, postgres_chat_repo, session_id
):
    """The coupling this phase bought, and the thing it bought it for.

    The worker allocates in the chat sequence space — `PostgresJobStore` takes
    the same row lock a live turn takes — and this is what that buys: one
    ordering, so `log_since` can merge the two tables with no tie-break and a
    client can replay them as one conversation.
    """
    await postgres_chat_repo.record_user_message(session_id, "c1", "run the backtest")
    job = await _enqueue(job_store, session_id)
    assistant = await postgres_chat_repo.start_assistant_message(session_id)
    lease = await job_store.claim(job.id, owner=OWNER, lease=UNIT * 8)
    await job_store.ack(lease)

    entries = await postgres_chat_repo.log_since(session_id, 0, 10)

    assert [e.seq for e in entries] == [1, 2, 3, 4, 5], "one allocator, no gaps"
    assert entries[2].message_id == assistant.message_id
    assert [getattr(e, "state", None) for e in entries] == [
        None,
        JobState.QUEUED,
        None,
        JobState.RUNNING,
        JobState.DONE,
    ]
