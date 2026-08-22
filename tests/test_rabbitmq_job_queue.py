"""What only the RabbitMQ implementation can be asked about.

`test_job_queue.py` runs the contract against every implementation and is the
phase's deliverable; these are the claims a fake cannot falsify, for the same
reason `test_chat_repository.py` keeps its gap-free `seq` assertions on the real
database. Both tests here are about the seam between a broker that has already
delivered a message and a row that decides what that message is worth.

Everything skips itself when the stack is not up — `make up && make migrate`.
"""

from datetime import timedelta

import pytest

from adapters.rabbitmq.job_queue import RabbitMQJobQueue
from core.config import get_settings
from core.jobs import JobRequest, JobState

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

# Shorter than the fixture's orphan threshold, so a reserve inside this window
# proves the message was never published rather than that the sweep was slow.
BRIEF = timedelta(milliseconds=60)


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
    assert await adapter.reserve(owner="w1", lease=LEASE, wait_for=BRIEF) is None

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
    necessary and not sufficient, and `docs/benchmark.md` should measure the
    lease rather than the socket.
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
