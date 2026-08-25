"""What only the RabbitMQ implementation can be asked about.

`test_job_queue.py` runs the contract against every implementation and is the
phase's deliverable; these are the claims a fake cannot falsify, for the same
reason `test_chat_repository.py` keeps its gap-free `seq` assertions on the real
database. Most are about the seam between a broker that has already delivered a
message and a row that decides what that message is worth; the last is about
what this object owes the broker on the way out, which is only a question
because a real connection carries a real consumer.

Everything that needs the stack skips itself when it is not up — `make up &&
make migrate`. The last test needs neither: it is a claim about the maintenance
loop's own arithmetic, which is answerable from an unstarted object.
"""

import inspect
import uuid
from datetime import timedelta

import pytest

from adapters.rabbitmq.job_queue import RabbitMQJobQueue
from core.config import get_settings
from core.jobs import JobRequest, JobState, StaleLease

pytestmark = pytest.mark.rabbitmq

# A lease short enough that the maintenance loop reaches it inside a test, and
# comfortably longer than a claim round trip on a loaded box.
LEASE = timedelta(milliseconds=150)

# Long enough to cover a lease lapsing plus a maintenance tick plus a broker
# round trip. Generous on purpose: `docs/websocket.md` records the trap this
# suite keeps hitting, that a test asserting "and then it is redelivered" hangs
# rather than fails when redelivery never happens, so every wait here is bounded
# and every bound is one a healthy system beats by an order of magnitude.
PATIENT = timedelta(seconds=3)

# `reserve` on a queue whose message is expected not to exist yet, sized against
# `_ORPHAN_AFTER` in `conftest.py` — 150ms, the fixture's grace before the sweep
# republishes a job the broker never got. Waiting less than that is what makes
# an empty reserve mean "never published" rather than "the sweep was slow".
#
# The bound is the point of the name. Raise this past that threshold and the
# sweep gets there first, the reserve succeeds, and the assertion fails
# reporting a broken sweep when the only broken thing is this number.
BEFORE_THE_SWEEP = timedelta(milliseconds=60)


def a_request(**overrides) -> JobRequest:
    return JobRequest(kind="noop", payload={"n": 1}, **overrides)


async def test_the_sweep_runs_a_job_whose_publish_never_happened(rabbitmq_queue, monkeypatch):
    """The dual write's one failure that can actually lose work, and its repair.

    `insert` and `basic_publish` are two systems with no transaction spanning
    them. Published-twice is harmless and published-before-inserted cannot arise,
    but inserted-and-never-published is silent, permanent, and indistinguishable
    from a job legitimately waiting behind a busy consumer — which is exactly why
    `published_at` exists: it makes "orphan" an exact question instead of a
    guess about how long is too long.
    """
    # `._queue` reaches past the fixture's session-seeding wrapper to the adapter
    # itself, which is what has to fail.
    adapter = rabbitmq_queue._queue

    async def publish_fails(*_args, **_kwargs):
        raise ConnectionResetError("broker went away between the commit and the publish")

    monkeypatch.setattr(adapter, "_publish", publish_fails)
    with pytest.raises(ConnectionResetError):
        await rabbitmq_queue.enqueue(a_request())
    monkeypatch.undo()

    # The row is committed and nothing is on the wire. Without the sweep this is
    # where the job stays forever, reported to a client as `queued`.
    assert await adapter.reserve(owner="w1", lease=LEASE, wait_for=BEFORE_THE_SWEEP) is None

    lease = await adapter.reserve(owner="w1", lease=PATIENT, wait_for=PATIENT)
    assert lease is not None
    assert lease.job.kind == "noop"
    assert lease.job.attempts == 1


async def test_a_consumer_cut_off_mid_job_has_its_work_finished_elsewhere(rabbitmq_queue):
    """Phase 2's half of phase 3's acceptance criterion, and a finding with it.

    A consumer dies holding a job: no `ack`, no `nack`, and its channel goes with
    the process. RabbitMQ requeues everything that channel held without Postgres
    being consulted, which is the one redelivery guarantee that is genuinely the
    broker's and the reason deliveries are held unacked at all.

    Note what the assertions below actually establish, because it is not the
    obvious thing. The redelivery is immediate; the *claim* is not. A surviving
    consumer receives the requeued message straight away and the row refuses it,
    because the dead consumer's lease is still live — so time-to-redelivery is
    set by the lease, not by the channel closing. The broker's guarantee is
    necessary and not sufficient — anything asking how fast a killed job comes
    back is measuring the lease, not the socket.
    """
    settings = get_settings()
    adapter = rabbitmq_queue._queue
    job = await rabbitmq_queue.enqueue(a_request())

    dying = await adapter.reserve(owner="dying", lease=LEASE, wait_for=PATIENT)
    assert dying is not None and dying.job.id == job.id

    # A killed process takes its maintenance loop with it, so this consumer
    # contributes nothing to its own recovery from here on.
    adapter._maintenance.cancel()
    await adapter._connection.close()

    survivor = await RabbitMQJobQueue.start(
        store=adapter._store,
        url=settings.broker_url,
        # The same topology, which is the point: competing consumers on one
        # quorum queue, not two queues that happen to look alike.
        namespace=adapter._namespace,
        prefetch=4,
        maintenance_interval=timedelta(milliseconds=40),
        orphan_after=timedelta(seconds=30),
    )
    try:
        picked = await survivor.reserve(owner="survivor", lease=PATIENT, wait_for=PATIENT)

        assert picked is not None
        assert picked.job.id == job.id
        # The redelivered attempt is counted the same as one that ended in a
        # `nack`, which is what keeps `max_attempts` meaningful against exactly
        # the consumer that never reports anything.
        assert picked.job.attempts == 2
        assert picked.id != dying.id

        await survivor.ack(picked, result_ref="s3://bucket/key")
        finished = await survivor.get(job.id)
        assert finished.state is JobState.DONE
        assert finished.result_ref == "s3://bucket/key"
    finally:
        # The dead adapter cannot delete the queues it declared — its channel is
        # gone — so the survivor tears the topology down for both of them.
        await survivor.close(delete_queues=True)


async def test_losing_the_lease_race_does_not_cost_a_consumer_its_prefetch_slot(rabbitmq_queue):
    """The other end of the steal: what happens to the delivery the loser holds.

    The overlap this design accepts — a lease in Postgres and a delivery in
    RabbitMQ, expiring on different clocks — means a consumer can still be alive
    and working when its lease lapses and the maintenance loop offers its job to
    somebody else. That much is intended: the claim is a conditional update, so
    one writer wins and `ack` tells the loser it lost by raising `StaleLease`.

    What is *not* intended is the loser's delivery outliving the exception. It
    is void — the winner finished the job and the row is terminal — but nothing
    reclaims it: `retire_lapsed` and `lapsed_reservable` both want `running`,
    and the row is `done` by the time anyone could look. So it stays unacked
    until the channel closes, and at `prefetch=1`, which is the phase-3 worker's
    setting, the loser's only slot is spent for the life of the process. It goes
    on reserving and is never offered anything, which reads as a broker fault
    from every angle except this one.

    The assertion is therefore the second reserve, not the exception.
    """
    settings = get_settings()
    store = rabbitmq_queue._queue._store

    # A topology of this test's own: the fixture's queue is a consumer too, and
    # the last assertion needs to know which consumer a delivery went to.
    namespace = f"test-{uuid.uuid4().hex[:12]}"

    # Slow, not dead — its channel stays up, which is what puts this outside the
    # redelivery guarantee the broker gives for free. Its maintenance interval is
    # long enough never to fire here: a sweep of its own would release the
    # delivery through `lapsed_reservable` before the winner claimed, which is
    # the narrow window in which this bug hides itself.
    slow = await RabbitMQJobQueue.start(
        store=store,
        url=settings.broker_url,
        namespace=namespace,
        prefetch=1,
        maintenance_interval=timedelta(seconds=30),
        orphan_after=timedelta(seconds=30),
    )
    try:
        thief = await RabbitMQJobQueue.start(
            store=store,
            url=settings.broker_url,
            namespace=namespace,
            prefetch=4,
            maintenance_interval=timedelta(milliseconds=40),
            orphan_after=timedelta(seconds=30),
        )
        try:
            job = await slow.enqueue(a_request())
            lost = await slow.reserve(owner="slow", lease=LEASE, wait_for=PATIENT)
            assert lost is not None and lost.job.id == job.id

            # The thief's sweep notices the lapsed lease and republishes. The
            # message can only come here: `slow` is at its prefetch limit,
            # holding the delivery it has not settled.
            stolen = await thief.reserve(owner="thief", lease=PATIENT, wait_for=PATIENT)
            assert stolen is not None and stolen.job.id == job.id
            assert stolen.id != lost.id
            await thief.ack(stolen, result_ref="s3://bucket/key")
        finally:
            # Out of the way, so the next delivery has one place to go. Its
            # queues stay: `slow` is still consuming from them.
            await thief.close()

        with pytest.raises(StaleLease):
            await slow.ack(lost)
        assert (await slow.get(job.id)).state is JobState.DONE

        # The point of all of it. Before the `finally` in `ack`, the delivery
        # for the lost lease is still unacked here and this reserve returns
        # `None` — for this process, forever.
        second = await slow.enqueue(a_request())
        picked = await slow.reserve(owner="slow", lease=PATIENT, wait_for=PATIENT)
        assert picked is not None and picked.job.id == second.id
    finally:
        await slow.close(delete_queues=True)


async def test_a_teardown_that_fails_still_drops_the_connection(rabbitmq_queue, monkeypatch):
    """`close` reports the failure, but not instead of closing.

    A leaked connection here is not an idle socket. It carries this object's
    registered consumer, so the broker goes on dispatching to a queue nobody is
    reading, and every one of those deliveries is unacked and unreachable for the
    life of the process — the same wedge a stale delivery causes, arrived at from
    the other end. The teardown that failed is exactly the moment it happens,
    because whatever made `queue.delete` raise is usually still true.

    The only caller passing `delete_queues=True` is the fixture in
    `conftest.py`, which means the blast radius is one leaked consumer per test
    and a stale consumer on a queue a later test declares by the same name. That
    hangs rather than fails, which is the failure mode this suite is built to
    refuse.
    """
    settings = get_settings()
    namespace = f"test-{uuid.uuid4().hex[:12]}"
    adapter = await RabbitMQJobQueue.start(
        store=rabbitmq_queue._queue._store,
        url=settings.broker_url,
        namespace=namespace,
        prefetch=1,
        maintenance_interval=timedelta(seconds=30),
        orphan_after=timedelta(seconds=30),
    )

    class RefusesToBeDeleted:
        """A queue whose `delete` fails, which is a broker having a bad day.

        Substituted for the queue rather than patched onto it: `RobustQueue`
        makes its attributes read-only, and the adapter holds `_work` as its own
        plain attribute — the same seam `_publish` is replaced through above.
        """

        async def delete(self, *_args, **_kwargs):
            raise ConnectionResetError("broker went away mid-teardown")

    monkeypatch.setattr(adapter, "_work", RefusesToBeDeleted())
    try:
        with pytest.raises(ConnectionResetError):
            await adapter.close(delete_queues=True)
        assert adapter._connection.is_closed
    finally:
        monkeypatch.undo()
        # The queues outlived the teardown that was supposed to remove them, so
        # a second adapter redeclares the same topology and takes it down.
        undertaker = await RabbitMQJobQueue.start(
            store=rabbitmq_queue._queue._store,
            url=settings.broker_url,
            namespace=namespace,
            prefetch=1,
            maintenance_interval=timedelta(seconds=30),
            orphan_after=timedelta(seconds=30),
        )
        await undertaker.close(delete_queues=True)


def test_the_republish_damper_outlasts_the_loop_it_damps():
    """A damper shorter than the loop it damps is not a damper.

    `_sweep` skips a republish when `now - last < quiet`, and two passes are one
    `maintenance_interval` apart — so a `quiet` smaller than that interval can
    never suppress anything, and the damper, the `_republished` dict, and the
    prune that empties it are all dead weight. That is what the five-second
    constant this replaces was at the thirty seconds `start()` ships: it worked
    only under the fixture's 40ms interval, the one place five seconds was the
    bigger number, which is the worst way for a constant to be wrong — visibly
    exercised by the suite and inert everywhere else.

    Asserted against `start`'s own default rather than a number copied here,
    because the regression to guard is not someone editing the damper. It is
    someone raising the interval past it.
    """
    shipped = inspect.signature(RabbitMQJobQueue.start).parameters["maintenance_interval"].default
    for interval in (shipped, timedelta(milliseconds=40), timedelta(minutes=5)):
        # Constructed, not started: no connection, no topology, no store touched.
        unstarted = RabbitMQJobQueue(
            None,
            namespace="unstarted",
            prefetch=1,
            maintenance_interval=interval,
            orphan_after=timedelta(minutes=2),
        )
        assert unstarted._republish_quiet > interval
