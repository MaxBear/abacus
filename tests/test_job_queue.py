"""Phase 2: the queue contract, run once per implementation.

Parametrized over `queue` so that a second implementation is a param and a
fixture, not a second copy of this file. That both implementations pass this
suite *unmodified* is the phase's actual deliverable — a Protocol nobody has
implemented twice is a guess, and a suite rewritten to accommodate the second
implementation has stopped being evidence. See `docs/jobs.md`.

`memory` always runs; `rabbitmq` skips itself when no broker or database
answers, so `make test` stays container-free and `make up` turns the rest on with
no flag to remember. What the memory run cannot demonstrate — competing
consumers under a genuine race, and redelivery after a consumer is cut off
mid-job — is asserted against the real implementation in
`test_rabbitmq_job_queue.py`, exactly as `test_chat_repository.py` does for
gap-free `seq`.

Leases here are short *real* durations rather than an injected clock, because
Postgres' `now()` is the database's and no test can advance it. Uniform
semantics across implementations are worth a few hundred milliseconds.
"""

import asyncio
import uuid
from datetime import timedelta

import pytest

from core.jobs import JobRequest, JobState, StaleLease
from tests.MemoryJobQueue import MemoryJobQueue

# Long enough that a lease does not lapse mid-assertion on a loaded CI box,
# short enough that the expiry tests stay quick. Every duration below is a
# multiple of this so the ratios stay legible when one of them has to change.
UNIT = timedelta(milliseconds=120)

# `reserve` on a queue that is expected to be empty. Non-zero so a bug that
# makes a job reservable slightly late still fails the assertion it should,
# rather than passing because the test looked too early.
BRIEF = UNIT


@pytest.fixture(params=["memory", pytest.param("rabbitmq", marks=pytest.mark.rabbitmq)])
def queue(request):
    if request.param == "memory":
        return MemoryJobQueue()
    return request.getfixturevalue(f"{request.param}_queue")


def a_request(**overrides) -> JobRequest:
    """A minimal enqueueable request. The queue treats kind and payload as opaque."""
    return JobRequest(kind="noop", payload={"n": 1}, **overrides)


async def reserve(queue, *, owner="w1", lease=UNIT * 8, wait_for=BRIEF):
    return await queue.reserve(owner=owner, lease=lease, wait_for=wait_for)


# --------------------------------------------------------------------------
# Producing
# --------------------------------------------------------------------------


async def test_enqueue_creates_a_queued_job(queue):
    job = await queue.enqueue(a_request())

    assert job.state is JobState.QUEUED
    assert job.attempts == 0
    assert await queue.get(job.id) == job


async def test_get_is_none_for_an_unknown_job(queue):
    # A client may hold a job id from a session that was since deleted. `None`
    # rather than a raise: "no such job" is an answer, not a failure.
    assert await queue.get(uuid.uuid4()) is None


async def test_enqueue_is_idempotent_on_the_key(queue):
    session = uuid.uuid4()
    first = await queue.enqueue(a_request(session_id=session, idempotency_key="k"))
    second = await queue.enqueue(a_request(session_id=session, idempotency_key="k"))

    # The same job, not a second one: a client unsure whether its submit landed
    # retries, and here a duplicate costs a five-minute solve.
    assert second.id == first.id
    assert await reserve(queue) is not None
    assert await reserve(queue) is None


async def test_the_key_is_scoped_to_the_session(queue):
    # Two clients that both call their message "1" are not submitting the same
    # job. Global keys would silently drop one of them.
    a = await queue.enqueue(a_request(session_id=uuid.uuid4(), idempotency_key="1"))
    b = await queue.enqueue(a_request(session_id=uuid.uuid4(), idempotency_key="1"))

    assert a.id != b.id


async def test_an_unset_key_never_deduplicates(queue):
    # The default is a fresh uuid, so "run this again" needs no ceremony while
    # retries that mean it supply their own key.
    a = await queue.enqueue(a_request())
    b = await queue.enqueue(a_request())

    assert a.id != b.id


async def test_delay_holds_a_job_back(queue):
    await queue.enqueue(a_request(delay=UNIT * 3))

    assert await reserve(queue) is None
    assert await reserve(queue, wait_for=UNIT * 5) is not None


# --------------------------------------------------------------------------
# Reserving
# --------------------------------------------------------------------------


async def test_reserve_claims_the_job_and_counts_the_attempt(queue):
    job = await queue.enqueue(a_request())

    lease = await reserve(queue, owner="w1")

    assert lease is not None
    assert lease.job.id == job.id
    assert lease.job.state is JobState.RUNNING
    assert lease.job.attempts == 1
    assert lease.owner == "w1"


async def test_reserve_returns_none_on_an_empty_queue(queue):
    # An idle queue is this system's normal state, so a timeout is an ordinary
    # outcome and a consumer loop should not need exception handling for it.
    assert await reserve(queue) is None


async def test_a_leased_job_is_not_reserved_twice(queue):
    await queue.enqueue(a_request())

    assert await reserve(queue, owner="w1", lease=UNIT * 20) is not None
    assert await reserve(queue, owner="w2") is None


async def test_an_expired_lease_makes_the_job_reservable_again(queue):
    await queue.enqueue(a_request())
    first = await reserve(queue, owner="w1", lease=UNIT)
    assert first is not None

    second = await reserve(queue, owner="w2", wait_for=UNIT * 5)

    # The lease is a timestamp, not a held lock — which is the whole reason a
    # five-minute solve can survive the process that claimed it.
    assert second is not None
    assert second.job.id == first.job.id
    assert second.job.attempts == 2
    assert second.id != first.id


async def test_reserve_prefers_the_job_that_became_ready_first(queue):
    # `order by run_after, created_at`. Asserted as "the ready one first" rather
    # than strict FIFO, which no competing-consumer queue actually offers.
    late = await queue.enqueue(a_request(delay=UNIT * 3))
    ready = await queue.enqueue(a_request())

    lease = await reserve(queue)

    assert lease is not None and lease.job.id == ready.id
    assert late.id != ready.id


async def test_reserve_hands_out_ready_jobs_in_ready_order(queue):
    # Both jobs are reservable by the time anyone asks, so `_reservable` cannot
    # decide this one and the claim query's `order by` has to. Every other test
    # in this file passes with that ordering inverted.
    ready = await queue.enqueue(a_request())
    later = await queue.enqueue(a_request(delay=UNIT * 2))
    await asyncio.sleep((UNIT * 3).total_seconds())

    first = await reserve(queue, owner="w1", lease=UNIT * 20)
    second = await reserve(queue, owner="w2", lease=UNIT * 20)

    assert first is not None and second is not None
    assert [first.job.id, second.job.id] == [ready.id, later.id]


async def test_run_after_outranks_creation_order(queue):
    # The two halves of `order by run_after, created_at` disagree here: the
    # delayed job was created first, the prompt one became ready first. Readiness
    # wins, which is what makes the ordering key two columns and not one — and an
    # implementation that dropped the `order by` altogether would hand back the
    # older row instead.
    delayed = await queue.enqueue(a_request(delay=UNIT * 2))
    prompt = await queue.enqueue(a_request())
    await asyncio.sleep((UNIT * 3).total_seconds())

    lease = await reserve(queue, owner="w1", lease=UNIT * 20)

    assert lease is not None and lease.job.id == prompt.id
    assert delayed.id != prompt.id


async def test_concurrent_consumers_each_get_a_different_job(queue):
    ids = {(await queue.enqueue(a_request())).id for _ in range(4)}

    leases = await asyncio.gather(
        *(reserve(queue, owner=f"w{i}", wait_for=UNIT * 5) for i in range(4))
    )

    # The memory implementation cannot lose a race it never runs; this is the
    # coherence check, and the real one is the postgres-marked `SKIP LOCKED`
    # test that arrives with that implementation.
    assert {lease.job.id for lease in leases} == ids


# --------------------------------------------------------------------------
# Finishing
# --------------------------------------------------------------------------


async def test_ack_is_terminal(queue):
    job = await queue.enqueue(a_request())
    lease = await reserve(queue)

    await queue.ack(lease, result_ref="s3://bucket/key")

    stored = await queue.get(job.id)
    assert stored.state is JobState.DONE
    assert stored.result_ref == "s3://bucket/key"
    assert await reserve(queue) is None


async def test_extend_pushes_the_deadline_out(queue):
    await queue.enqueue(a_request())
    lease = await reserve(queue, lease=UNIT)

    extended = await queue.extend(lease, UNIT * 20)

    assert extended.expires_at > lease.expires_at
    # The job that would have lapsed by now does not, which is what lets a
    # five-minute solve that needs seven minutes keep its claim.
    assert await reserve(queue, owner="w2", wait_for=UNIT * 3) is None


async def test_nack_holds_the_job_back_for_the_backoff(queue):
    job = await queue.enqueue(a_request())
    lease = await reserve(queue)

    await queue.nack(lease, error="boom", retry_in=UNIT * 3)

    assert (await queue.get(job.id)).state is JobState.FAILED
    assert await reserve(queue) is None
    retried = await reserve(queue, wait_for=UNIT * 6)
    assert retried is not None and retried.job.attempts == 2


async def test_nack_without_a_backoff_returns_the_job_immediately(queue):
    # The voluntary release: shutting down, or out of capacity. Immediate is
    # wrong for a failure and right for this.
    await queue.enqueue(a_request())
    lease = await reserve(queue)

    await queue.nack(lease, error="draining", retry_in=None)

    assert await reserve(queue, owner="w2") is not None


async def test_a_job_that_kills_its_consumer_stops_being_redelivered(queue):
    # The poison-message case, and the one `nack` cannot cover: a consumer that
    # crashes never reports anything, so the attempts bound has to be enforced
    # where the job is reclaimed. Without it, a job that reliably kills whatever
    # picks it up is redelivered forever and occupies a consumer each time.
    job = await queue.enqueue(a_request(max_attempts=2))

    for _ in range(2):
        lease = await reserve(queue, lease=UNIT, wait_for=UNIT * 5)
        assert lease is not None  # reserved, then the consumer "dies" — no ack, no nack
        assert lease.job.attempts <= lease.job.max_attempts

    assert await reserve(queue, wait_for=UNIT * 5) is None
    stored = await queue.get(job.id)
    assert stored.state is JobState.DEAD
    assert stored.attempts == stored.max_attempts


async def test_exhausted_attempts_reach_dead_rather_than_looping(queue):
    job = await queue.enqueue(a_request(max_attempts=2))

    for _ in range(2):
        lease = await reserve(queue, wait_for=UNIT * 5)
        await queue.nack(lease, error="boom", retry_in=None)

    stored = await queue.get(job.id)
    assert stored.state is JobState.DEAD
    assert stored.error == "boom"
    # A poison job that kept being redelivered would occupy a consumer forever
    # and starve everything behind it.
    assert await reserve(queue) is None


# --------------------------------------------------------------------------
# Fencing
# --------------------------------------------------------------------------


async def test_ack_on_a_stolen_lease_is_refused(queue):
    await queue.enqueue(a_request())
    stalled = await reserve(queue, owner="w1", lease=UNIT)
    stolen = await reserve(queue, owner="w2", wait_for=UNIT * 5, lease=UNIT * 20)
    assert stolen is not None

    # The consumer that stalled past its deadline wakes up and tries to record a
    # result for a job someone else is already working. Without the fencing
    # token that write lands and overwrites the real one.
    with pytest.raises(StaleLease):
        await queue.ack(stalled, result_ref="stale")

    assert (await queue.get(stalled.job.id)).state is JobState.RUNNING


async def test_nack_on_a_stolen_lease_is_refused(queue):
    await queue.enqueue(a_request())
    stalled = await reserve(queue, owner="w1", lease=UNIT)
    assert await reserve(queue, owner="w2", wait_for=UNIT * 5, lease=UNIT * 20) is not None

    # Symmetrical, and the more dangerous direction: a slow consumer's failure
    # marking a job failed that another consumer is midway through completing.
    with pytest.raises(StaleLease):
        await queue.nack(stalled, error="boom")


async def test_extend_on_a_stolen_lease_is_refused(queue):
    await queue.enqueue(a_request())
    stalled = await reserve(queue, owner="w1", lease=UNIT)
    assert await reserve(queue, owner="w2", wait_for=UNIT * 5, lease=UNIT * 20) is not None

    with pytest.raises(StaleLease):
        await queue.extend(stalled, UNIT * 20)


async def test_a_late_lease_nobody_took_is_still_usable(queue):
    # Fencing is on the token, not the deadline: the only thing the check has to
    # prevent is *two* writers. Failing an unclaimed late lease would add a
    # clock-skew race and protect nothing.
    job = await queue.enqueue(a_request())
    lease = await reserve(queue, lease=UNIT)
    await asyncio.sleep((UNIT * 2).total_seconds())

    await queue.ack(lease)

    assert (await queue.get(job.id)).state is JobState.DONE


# --------------------------------------------------------------------------
# Cancellation
# --------------------------------------------------------------------------


async def test_cancel_before_reserve_stops_the_job_reaching_a_consumer(queue):
    job = await queue.enqueue(a_request())

    cancelled = await queue.cancel(job.id)

    # The common case, and the one phase 2 can honestly do: the reason a user
    # cancels is usually that the job has been sitting in a queue.
    assert cancelled.state is JobState.CANCELLED
    assert await reserve(queue) is None


async def test_cancel_is_idempotent(queue):
    job = await queue.enqueue(a_request())

    first = await queue.cancel(job.id)
    second = await queue.cancel(job.id)

    # A client that cancels twice across a reconnect gets the same answer both
    # times — the whole point of this being a transition.
    assert first.state is second.state is JobState.CANCELLED


async def test_cancel_is_none_for_an_unknown_job(queue):
    assert await queue.cancel(uuid.uuid4()) is None


async def test_cancel_of_a_running_job_only_raises_the_flag(queue):
    job = await queue.enqueue(a_request())
    lease = await reserve(queue, lease=UNIT * 20)

    await queue.cancel(job.id)

    # Nothing can stop a consumer from out here. The job is still running and
    # still leased; all that has happened is that someone asked.
    stored = await queue.get(job.id)
    assert stored.state is JobState.RUNNING
    assert stored.cancel_requested is True
    assert await reserve(queue, owner="w2") is None
    assert lease.job.cancel_requested is False  # the snapshot taken at reserve


async def test_extend_is_how_a_consumer_learns_it_was_cancelled(queue):
    job = await queue.enqueue(a_request())
    lease = await reserve(queue, lease=UNIT * 20)
    await queue.cancel(job.id)

    beating = await queue.extend(lease, UNIT * 20)

    # The heartbeat doubles as the cancellation check, so a consumer needs no
    # second call it would have to remember to make.
    assert beating.job.cancel_requested is True


async def test_a_cancelled_job_released_by_its_consumer_is_not_a_failure(queue):
    job = await queue.enqueue(a_request())
    lease = await reserve(queue, lease=UNIT * 20)
    await queue.cancel(job.id)

    await queue.nack(lease, error="cancelled", retry_in=None)

    # Not FAILED, and not retried: the user asked for this, and putting an error
    # in front of someone who already knows what happened is not reporting.
    stored = await queue.get(job.id)
    assert stored.state is JobState.CANCELLED
    assert await reserve(queue, wait_for=UNIT * 3) is None


async def test_cancel_does_not_disturb_a_finished_job(queue):
    job = await queue.enqueue(a_request())
    lease = await reserve(queue)
    await queue.ack(lease, result_ref="s3://bucket/key")

    cancelled = await queue.cancel(job.id)

    # The cancel arrived late — the work is already done and paid for. Rewriting
    # a completed job's state would throw away a result that exists.
    assert cancelled.state is JobState.DONE
    assert cancelled.result_ref == "s3://bucket/key"


async def test_a_cancelled_job_whose_consumer_dies_is_still_not_a_failure(queue):
    # The crash counterpart of the test above it. `nack` settles that a cancelled
    # job is never reported as a failure, and the consumer that crashes is
    # precisely the one that never calls `nack` — so the claim path has to apply
    # the same rule, or the user who asked for this gets an error instead.
    job = await queue.enqueue(a_request(max_attempts=1))
    lease = await reserve(queue, lease=UNIT)
    assert lease is not None

    await queue.cancel(job.id)  # flag raised, then the consumer dies: no ack, no nack

    assert await reserve(queue, wait_for=UNIT * 5) is None
    stored = await queue.get(job.id)
    assert stored.state is JobState.CANCELLED
    # Not `dead`, and carrying no lease-expiry message: the outcome a client sees
    # must not depend on how the consumer happened to stop.
    assert stored.error is None


async def test_a_cancelled_job_is_not_redelivered_to_burn_its_remaining_attempts(queue):
    # Same crash with attempts to spare. Redelivering would claim a consumer,
    # extend once, discover the flag and `nack` — a round trip to reach the state
    # the queue could already name.
    job = await queue.enqueue(a_request(max_attempts=3))
    lease = await reserve(queue, lease=UNIT)
    assert lease is not None

    await queue.cancel(job.id)

    assert await reserve(queue, wait_for=UNIT * 5) is None
    stored = await queue.get(job.id)
    assert stored.state is JobState.CANCELLED
    assert stored.attempts == 1


async def test_cancel_of_a_failed_job_stops_the_retry(queue):
    # `failed` is "queued, backing off", so it is cancellable for the same reason
    # `queued` is: no consumer holds it, and the retry has not happened yet.
    job = await queue.enqueue(a_request())
    lease = await reserve(queue)
    await queue.nack(lease, error="boom", retry_in=UNIT)
    assert (await queue.get(job.id)).state is JobState.FAILED

    cancelled = await queue.cancel(job.id)

    assert cancelled.state is JobState.CANCELLED
    assert await reserve(queue, wait_for=UNIT * 5) is None


async def test_a_consumer_that_finishes_before_seeing_the_flag_still_wins(queue):
    # The race `cancel` cannot close: the work is done and paid for by the time
    # the flag is read. `ack` does not consult it, for the same reason cancelling
    # an already-`done` job is a no-op.
    job = await queue.enqueue(a_request())
    lease = await reserve(queue)

    await queue.cancel(job.id)
    await queue.ack(lease, result_ref="s3://bucket/key")

    stored = await queue.get(job.id)
    assert stored.state is JobState.DONE
    assert stored.result_ref == "s3://bucket/key"
