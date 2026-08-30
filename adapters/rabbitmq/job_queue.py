"""The RabbitMQ implementation of `core.jobs.JobQueue`.

The broker moves job *ids*; `PostgresJobStore` holds everything else. That is
not a shortcut — `docs/jobs.md` settles it on both sides of the comparison,
because RabbitMQ cannot answer "what happened to my job?" about a message it has
already delivered, and phase 5's UI has to ask exactly that.

What the split costs is the interesting part, and it is all visible here:

- **enqueue is a dual write.** `insert` and `basic_publish` are two systems with
  no transaction spanning them. Publisher confirms do not help; a confirm is
  still not atomic with a Postgres commit. Repaired with `published_at` and the
  sweep in `_maintain`, which is an outbox collapsed into the `jobs` table.
- **`extend` has no broker half at all.** A five-minute solve that needs seven
  minutes has no way to tell RabbitMQ so, so the lease is the row's timestamp
  and the delivery just stays unacked.
- **Nothing notices a lapsed lease**, for the same reason, so `_maintain` polls
  Postgres for them and republishes. That loop and the sweep are two periodic
  database queries that a `SKIP LOCKED` implementation would not need — which is
  `docs/jobs.md`'s thesis showing up as code rather than as an opinion, and the
  reason that document's benchmark was never worth running: the cost it set out
  to measure is legible here without a number attached to it.

The delivery is held unacked from `reserve` until `ack`/`nack` deliberately. It
buys the one redelivery guarantee that is genuinely the broker's: a consumer
whose process dies drops its channel, and RabbitMQ requeues everything it held
without Postgres being consulted. The lease covers the case the broker cannot
see — a consumer still alive but past its deadline — and the two overlapping
means a crash can produce two deliveries for one job. That is the harmless
failure of the three: the claim is a conditional update, so one wins and the
loser discards.
"""

import asyncio
import contextlib
import logging
import uuid
from datetime import timedelta

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from adapters.postgres.job_store import PostgresJobStore
from core.jobs import Job, JobRequest, JobState, Lease

log = logging.getLogger(__name__)

# The routing key every job message carries. One key, one queue: this is a work
# queue with competing consumers, not a topic, and the fanout that phase 3 adds
# for progress events is a different exchange entirely.
_WORK_KEY = "work"

# Quorum queues, not classic — `docs/jobs.md` settles it. They are RabbitMQ's
# modern default and the comparison is only worth having against the real
# default; most "we compared them" writeups quietly used classic on one node.
#
# `x-delivery-limit` is the broker's version of the `attempts` column, and it is
# a backstop rather than the enforcement: `attempts` is per job and this is per
# queue, so the row is what decides when a job is `dead`. This catches the one
# case the row cannot see — a message requeued over and over by channel
# closures, each of which is a consumer that died before it could claim anything.
_QUORUM = {"x-queue-type": "quorum", "x-delivery-limit": 20}

# How long a republished-and-still-unclaimed job is left alone before the
# maintenance loop offers it again, counted in maintenance passes. Duplicates
# are harmless but not free, and one delivery is enough: the message waits in
# the broker until someone claims it, so republishing every tick would only pile
# up deliveries to discard.
#
# Passes rather than seconds, because the damper has to be longer than the loop
# it damps and only the loop knows its own period. The five seconds this
# replaces did not: at the thirty-second interval `start()` defaults to,
# `now - last` was always the larger number, so the `continue` never ran, the
# prune emptied `_republished` on every pass, and the whole mechanism — dict,
# damper, prune — did nothing. It was load-bearing only under the test fixture's
# 40ms interval, which is the one place five seconds was the bigger number.
_REPUBLISH_QUIET_PASSES = 4


class RabbitMQJobQueue:
    """`JobQueue` over a quorum work queue, with `jobs` as the system of record.

    Built with `start()` rather than a constructor: the topology has to be
    declared and the consumer running before the first `reserve`, and none of
    that can happen in `__init__`.
    """

    def __init__(
        self,
        store: PostgresJobStore,
        *,
        namespace: str,
        prefetch: int,
        maintenance_interval: timedelta,
        orphan_after: timedelta,
    ) -> None:
        self._store = store
        self._namespace = namespace
        self._prefetch = prefetch
        self._maintenance_interval = maintenance_interval
        self._republish_quiet = maintenance_interval * _REPUBLISH_QUIET_PASSES
        self._orphan_after = orphan_after

        # Deliveries the consumer has taken but nobody has reserved yet. Bounded
        # without being a bounded queue: everything in here is unacked, so
        # `prefetch` is its ceiling. That is what makes backpressure a property
        # of the shape rather than a feature — a consumer holding its quota is
        # simply not offered more work.
        self._incoming: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()

        # The delivery behind each live lease, by job id, so `ack`/`nack` can
        # settle with the broker. Keyed by job rather than by lease because the
        # maintenance loop has to drop a delivery whose lease it just retired,
        # and at that point the lease id is the row's business, not ours.
        self._held: dict[uuid.UUID, tuple[uuid.UUID, AbstractIncomingMessage]] = {}
        self._republished: dict[uuid.UUID, float] = {}

        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        self._work: aio_pika.abc.AbstractQueue | None = None
        self._delay: aio_pika.abc.AbstractQueue | None = None
        self._maintenance: asyncio.Task | None = None

    @classmethod
    async def start(
        cls,
        *,
        store: PostgresJobStore,
        url: str,
        namespace: str = "abacus",
        prefetch: int = 1,
        maintenance_interval: timedelta = timedelta(seconds=30),
        orphan_after: timedelta = timedelta(minutes=2),
    ) -> "RabbitMQJobQueue":
        """Declare the topology, start consuming, and start the maintenance loop.

        `prefetch=1` is the phase-3 worker's setting and the reason the two
        implementations are comparable at all: with a larger window RabbitMQ
        hands one consumer several five-minute jobs at once, they sit in its
        buffer while others idle, and if it dies they all wait for the channel to
        close. It is an argument rather than a constant because this object is
        one consumer — a caller that reserves concurrently is several, and has to
        say so.
        """
        self = cls(
            store,
            namespace=namespace,
            prefetch=prefetch,
            maintenance_interval=maintenance_interval,
            orphan_after=orphan_after,
        )
        self._connection = await aio_pika.connect_robust(url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=prefetch)

        self._exchange = await self._channel.declare_exchange(
            f"{namespace}.jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )
        self._work = await self._channel.declare_queue(
            f"{namespace}.jobs.work", durable=True, arguments=_QUORUM
        )
        await self._work.bind(self._exchange, routing_key=_WORK_KEY)

        # The parking queue: nothing consumes it, messages expire out of it, and
        # the dead-letter exchange puts them on the work queue at that moment.
        # This is what `nack(retry_in=…)` and `JobRequest.delay` are built from —
        # one column update in Postgres, a whole queue and an exchange here.
        #
        # Classic rather than quorum, and per-message TTL rather than a queue of
        # them: arbitrary per-job backoff needs the TTL on the message, which is
        # a classic-queue idiom and carries a documented hazard — messages expire
        # only from the head, so a 30-second delay queued behind a 30-minute one
        # waits the full half hour. `docs/jobs.md` records the production answer
        # as a ladder of fixed-TTL queues (30s / 5m / 30m); it is left open here
        # because the ladder cannot express an arbitrary duration and the
        # contract's `retry_in` is arbitrary.
        self._delay = await self._channel.declare_queue(
            f"{namespace}.jobs.delay",
            durable=True,
            arguments={
                "x-dead-letter-exchange": self._exchange.name,
                "x-dead-letter-routing-key": _WORK_KEY,
            },
        )

        await self._work.consume(self._on_delivery)
        self._maintenance = asyncio.create_task(self._maintain())
        return self

    async def close(self, *, delete_queues: bool = False) -> None:
        """Stop consuming and drop the connection.

        Whatever is still held goes back to the broker when the channel closes,
        which is the redelivery path this design keeps the deliveries unacked
        for. Nothing is acked on the way out on purpose.

        The connection goes in a `finally` because everything above it can fail
        against a broker that is having a bad day, and the connection is what
        registers this object's *consumer*. Leaked, it is not an idle socket: the
        broker goes on delivering to a consumer no one is reading, and each of
        those deliveries is unacked and unreachable until the process exits. A
        teardown that raises would leave a queue looking healthy and answering
        nobody, which is worse than the failure it is reporting.
        """
        try:
            if self._maintenance is not None:
                self._maintenance.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._maintenance
            if delete_queues and self._channel is not None and not self._channel.is_closed:
                for queue in (self._work, self._delay):
                    if queue is not None:
                        await queue.delete(if_unused=False, if_empty=False)
                if self._exchange is not None:
                    await self._exchange.delete()
        finally:
            if self._connection is not None:
                await self._connection.close()

    # ----------------------------------------------------------------------
    # Producing
    # ----------------------------------------------------------------------

    async def enqueue(self, request: JobRequest) -> Job:
        job, created = await self._store.insert_or_get(request)
        if created:
            # The dual write, in the only order that leaves a repairable state:
            # the row first, so the failure mode is a job that exists and has not
            # been published — findable, because `published_at` is null — rather
            # than a message referring to a row that does not exist.
            await self._publish(job.id, delay=request.delay)
            await self._store.mark_published(job.id)
        return job

    async def get(self, job_id: uuid.UUID) -> Job | None:
        return await self._store.get(job_id)

    async def cancel(self, job_id: uuid.UUID) -> Job | None:
        """Entirely the row's business; the broker is not told.

        A message for a job that has just been cancelled stays on the queue and
        is discarded when someone tries to claim it — there is no way to retract
        a published message, and no need for one when the claim is conditional.
        """
        return await self._store.cancel(job_id)

    # ----------------------------------------------------------------------
    # Consuming
    # ----------------------------------------------------------------------

    async def reserve(self, *, owner: str, lease: timedelta, wait_for: timedelta) -> Lease | None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_for.total_seconds()
        while True:
            message = await self._next_delivery(deadline)
            if message is None:
                return None

            job_id = uuid.UUID(message.body.decode())
            claimed = await self._store.claim(job_id, owner=owner, lease=lease)
            if claimed is None:
                # A delivery the row refuses: cancelled, finished elsewhere,
                # still inside its backoff, or a duplicate of one already leased.
                # The row is the authority, so this is discarded rather than
                # requeued — requeuing would spin it straight back at us.
                await _settle(message)
                continue

            self._held[job_id] = (claimed.id, message)
            return claimed

    async def _next_delivery(self, deadline: float) -> AbstractIncomingMessage | None:
        """One delivery, or `None` once `wait_for` is spent.

        The non-blocking look comes first so that `wait_for=0` still inspects the
        queue: a zero-timeout reserve that never looks at anything would be a
        surprising way to spell "non-blocking".
        """
        try:
            return self._incoming.get_nowait()
        except asyncio.QueueEmpty:
            pass
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return None
        try:
            return await asyncio.wait_for(self._incoming.get(), remaining)
        except TimeoutError:
            return None

    async def _on_delivery(self, message: AbstractIncomingMessage) -> None:
        """Hand the delivery to `reserve`, unacked.

        No `message.process()` context: that would ack when this returns, and the
        whole point is that the delivery outlives the callback and is settled by
        whoever finishes the job.
        """
        self._incoming.put_nowait(message)

    # ----------------------------------------------------------------------
    # Finishing
    # ----------------------------------------------------------------------

    async def extend(self, lease: Lease, by: timedelta) -> Lease:
        """The row's deadline moves; the broker is not involved and cannot be.

        This is the asymmetry `docs/jobs.md` calls out as the place the folklore
        inverts. There is no AMQP way to say "still working" about one delivery,
        so a *slow* consumer here is safe only because Postgres holds the lease,
        and a *hung* one holds its delivery until its channel closes.
        """
        return await self._store.extend(lease, by)

    async def ack(self, lease: Lease, *, result_ref: str | None = None) -> None:
        # Postgres first, so a stolen lease raises before the delivery is
        # settled: the fencing check is the thing protecting a result another
        # consumer already recorded, and acking first would throw away the
        # redelivery that consumer is relying on.
        #
        # Settled either way, though, and that is what the `finally` is for. A
        # `StaleLease` means this delivery is for a job somebody else has already
        # finished, so it is void — and nothing else will ever come back for it:
        # both queries in `_sweep` are predicated on `state == running`, and by
        # now the row is terminal. Left in `_held` it is an unacked delivery the
        # broker counts against this channel until the process exits, which at
        # `prefetch=1` is the whole consumer.
        try:
            await self._store.ack(lease, result_ref=result_ref)
        finally:
            await self._release(lease.job.id, lease.id)

    async def nack(self, lease: Lease, *, error: str, retry_in: timedelta | None = None) -> None:
        # Same shape as `ack`, and the republish below stays inside the success
        # path: a lease this consumer no longer holds is not its job to reoffer.
        try:
            job = await self._store.nack(lease, error=error, retry_in=retry_in)
        finally:
            await self._release(lease.job.id, lease.id)
        if job.state is JobState.FAILED:
            # Republished rather than requeued: `basic_nack(requeue=True)` puts
            # the message back at the head with no delay at all, which turns a
            # deterministic failure into a hot loop between one consumer and the
            # broker. The backoff has to be delivery-time, so it goes through the
            # parking queue.
            await self._publish(job.id, delay=retry_in)
            await self._store.mark_published(job.id)

    async def discard(self, lease: Lease) -> None:
        """Hand back the delivery for a claim that is void. No Postgres at all.

        The row is not this consumer's to touch — that is what made the claim
        void — so the only thing owed to anyone is the prefetch slot. The
        `lease_id` guard is what makes that safe to say without a round trip: if
        this process has since re-reserved the same job, the delivery in
        `_held` belongs to the newer lease and stays put.
        """
        await self._release(lease.job.id, lease.id)

    # ----------------------------------------------------------------------
    # Publishing, and the periodic repairs the dual write needs
    # ----------------------------------------------------------------------

    async def _publish(self, job_id: uuid.UUID, *, delay: timedelta | None = None) -> None:
        """Put a job id on the wire — straight to work, or via the parking queue.

        The body is the id and nothing else. The payload a worker actually needs
        is on the row, which is what lets the message be this small and what
        makes `published_at` a sufficient outbox: there is no second table
        because there is no intent to record beyond "this row is not on the wire
        yet".
        """
        body = str(job_id).encode()
        if delay is not None and delay > timedelta():
            message = aio_pika.Message(
                body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                expiration=delay,
            )
            await self._channel.default_exchange.publish(message, routing_key=self._delay.name)
            return
        message = aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self._exchange.publish(message, routing_key=_WORK_KEY)

    async def _maintain(self) -> None:
        """The two periodic queries RabbitMQ needs and Postgres would not.

        Kept as one loop rather than two timers because they are one idea: both
        exist because the broker's view and the row's can disagree, and both are
        cheap partial-index reads that should normally return nothing.

        Failures are logged and the loop continues. A maintenance pass that dies
        silently would leave orphaned jobs unpublished forever, which is the
        exact failure the sweep exists to prevent.
        """
        interval = self._maintenance_interval.total_seconds()
        while True:
            await asyncio.sleep(interval)
            try:
                await self._sweep()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("job queue maintenance pass failed")

    async def _sweep(self) -> None:
        for job_id in await self._store.retire_lapsed():
            # The consumer that held this is gone and the job is finished. Its
            # delivery is void, and dropping it now returns the prefetch slot
            # instead of leaving this consumer one job poorer until it restarts.
            await self._release(job_id)

        now = asyncio.get_running_loop().time()
        quiet = self._republish_quiet.total_seconds()
        for job_id in await self._store.lapsed_reservable():
            await self._release(job_id)
            if now - self._republished.get(job_id, float("-inf")) < quiet:
                continue
            # Note what is *not* done here: the row is left alone. A lease that
            # lapsed while nobody wanted the job is still its holder's, so the
            # attempt is counted and the token minted by whoever claims next —
            # never by the loop that merely offers the job again.
            await self._publish(job_id)
            self._republished[job_id] = now

        for job_id in await self._store.unpublished(self._orphan_after):
            await self._publish(job_id)
            await self._store.mark_published(job_id)

        self._republished = {
            job_id: at for job_id, at in self._republished.items() if now - at < quiet
        }

    async def _release(self, job_id: uuid.UUID, lease_id: uuid.UUID | None = None) -> None:
        """Settle the delivery this consumer holds for a job, if it still holds one.

        `lease_id` guards the `ack`/`nack` path: a consumer settling its own work
        must not drop a delivery belonging to the lease that superseded it. The
        maintenance loop passes none, because there the job is finished or being
        reoffered and every delivery for it is void.
        """
        held = self._held.get(job_id)
        if held is None:
            return
        if lease_id is not None and held[0] != lease_id:
            return
        del self._held[job_id]
        await _settle(held[1])


async def _settle(message: AbstractIncomingMessage) -> None:
    """`basic_ack`, tolerating a delivery the broker has already taken back.

    A closed channel has requeued everything on it, so the ack has nothing to
    settle and its failure means the redelivery already happened — which is the
    outcome this would have been asking for anyway.
    """
    try:
        await message.ack()
    except Exception:  # noqa: BLE001 - the channel is gone; the broker requeued it
        log.debug("could not ack delivery %s; channel is gone", message.delivery_tag)
