"""The `jobs` table, and every statement that touches it.

Deliberately **not** a `JobQueue`. `docs/jobs.md` settles that `jobs` is the
system of record in both worlds — the queue moves a job *id*, the job's state
lives in Postgres either way — so the row half and the movement half are two
different objects. This is the row half: it knows SQL and nothing about waiting,
delivery, or channels.

That split is what makes the RabbitMQ adapter honest rather than a second copy
of the semantics. It is also what would make phase 2's other implementation
cheap if it is ever built: a Postgres `JobQueue` is this store plus a polling
loop and a `SKIP LOCKED` scan, with no statement here rewritten.

One transaction per method, opened and closed inside it, as in
`chat_repository.py`. Nothing here holds a lock across anything a caller does —
a lease is a timestamp in a row, never a held lock, which is the whole reason a
five-minute solve can outlive the process that claimed it.
"""

import uuid
from datetime import timedelta

from sqlalchemy import (
    Interval,
    and_,
    case,
    func,
    literal,
    null,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from adapters.postgres.db import Database
from adapters.postgres.tables import jobs
from core.jobs import Job, JobRequest, JobState, Lease, StaleLease

# The columns a `Job` is built from, in one place so that a select list and an
# `update … returning` cannot drift apart — the same reason
# `chat_repository.py` keeps `_MESSAGE_COLUMNS`.
_JOB_COLUMNS = (
    jobs.c.id,
    jobs.c.kind,
    jobs.c.payload,
    jobs.c.state,
    jobs.c.attempts,
    jobs.c.max_attempts,
    jobs.c.session_id,
    jobs.c.idempotency_key,
    jobs.c.result_ref,
    jobs.c.error,
    jobs.c.cancel_requested,
)

# What a lapsed lease with nothing left to try records, when the consumer that
# held it never said anything. Spelled once because `docs/jobs.md` quotes it.
_LEASE_EXPIRED = "lease expired with no attempts remaining"

# `state in ('queued', 'failed') and run_after <= now()` — the reservable half of
# the predicate. `failed` is here because it *is* "queued, backing off": the
# claim path treats the two identically, which is why `ix_jobs_reservable` is
# partial on both and why `JobState.terminal` excludes `failed`.
_WAITING = and_(
    jobs.c.state.in_([JobState.QUEUED, JobState.FAILED]),
    jobs.c.run_after <= func.now(),
)

# The other half: a running job whose consumer stopped answering. Expiry is a
# predicate, not a daemon (`docs/jobs.md`) — there is no window between a lease
# lapsing and the job being available, and no sweeper whose failure is silent.
#
# The two exclusions are load-bearing. `attempts < max_attempts` is what stops a
# job that reliably kills its consumer from being redelivered forever, and
# enforcing it only in `nack` would leave exactly the crashing consumer
# uncovered. `not cancel_requested` keeps `retire_lapsed` from being second-
# guessed: a cancelled job is finished, not reservable.
_LAPSED = and_(
    jobs.c.state == JobState.RUNNING,
    jobs.c.lease_expires_at < func.now(),
    jobs.c.attempts < jobs.c.max_attempts,
    jobs.c.cancel_requested.is_(False),
)


def _interval(delta: timedelta):
    """A bound INTERVAL parameter, so durations are the database's arithmetic.

    `now() + $1` rather than a Python-computed timestamp throughout: the lease
    deadline has to be on the same clock as the predicate that later decides it
    lapsed, and that clock is Postgres'. A consumer's own `datetime.now()` is
    close enough to look right and skewed enough to be a race.
    """
    return literal(delta, Interval())


def _to_job(row) -> Job:
    return Job(
        id=row.id,
        kind=row.kind,
        payload=row.payload,
        # Coerced, not cast: a value the check constraint should have made
        # impossible raises here rather than travelling on as a bare string.
        state=JobState(row.state),
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        session_id=row.session_id,
        idempotency_key=row.idempotency_key,
        result_ref=row.result_ref,
        error=row.error,
        cancel_requested=row.cancel_requested,
    )


class PostgresJobStore:
    """Every statement against `jobs`, and no policy about how work is handed out."""

    def __init__(self, db: Database) -> None:
        self._db = db

    # ----------------------------------------------------------------------
    # Producing
    # ----------------------------------------------------------------------

    async def insert_or_get(self, request: JobRequest) -> tuple[Job, bool]:
        """Create the job, or return the one this key already created.

        The bool is "we inserted it", and the RabbitMQ adapter needs it: a
        duplicate `enqueue` must not put a second message on the wire. It is not
        part of the `JobQueue` contract, where a caller wanting a job id to watch
        is given the right answer either way.

        `on conflict do nothing` rather than a check-then-insert, the same
        argument `docs/persistence.md` makes about `client_msg_id` — two callers
        retrying the same submit concurrently is precisely the case a
        check-then-insert loses.
        """
        stmt = (
            pg_insert(jobs)
            .values(
                id=uuid.uuid4(),
                session_id=request.session_id,
                idempotency_key=request.idempotency_key,
                kind=request.kind,
                payload=request.payload,
                state=JobState.QUEUED,
                max_attempts=request.max_attempts,
                run_after=func.now() + _interval(request.delay),
            )
            .on_conflict_do_nothing(constraint="uq_jobs_session_id_idempotency_key")
            .returning(*_JOB_COLUMNS)
        )
        async with self._db.session() as s:
            row = (await s.execute(stmt)).first()
            if row is not None:
                await s.commit()
                return _to_job(row), True

            # The key already exists. `is not distinct from` rather than `=`
            # because session_id is nullable and `null = null` is null — which
            # would find nothing for exactly the session-less callers the
            # constraint's NULLS NOT DISTINCT was added to cover.
            existing = (
                await s.execute(
                    select(*_JOB_COLUMNS).where(
                        jobs.c.session_id.is_not_distinct_from(request.session_id),
                        jobs.c.idempotency_key == request.idempotency_key,
                    )
                )
            ).one()
            return _to_job(existing), False

    async def mark_published(self, job_id: uuid.UUID) -> None:
        """Record that the broker has the job. RabbitMQ only.

        `published_at is null` on a committed row is the one dual-write failure
        that can actually lose work, and this is what makes "orphan" exact.
        Without it a sweep could only ask "has this been queued a while?", which
        a job waiting behind four busy consumers answers identically.
        """
        async with self._db.session() as s:
            await s.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(published_at=func.now(), updated_at=func.now())
            )
            await s.commit()

    async def get(self, job_id: uuid.UUID) -> Job | None:
        async with self._db.session() as s:
            row = (await s.execute(select(*_JOB_COLUMNS).where(jobs.c.id == job_id))).first()
            return _to_job(row) if row else None

    async def cancel(self, job_id: uuid.UUID) -> Job | None:
        """Ask for a job to stop. Two operations under one name — see `JobQueue.cancel`.

        Unreserved, this is the transition and the job never reaches a consumer.
        Reserved, it only raises the flag: nothing here can stop a process that
        is already inside a computation, so the row keeps its state and its
        lease and the consumer finds out on its next `extend`.
        """
        stmt = (
            update(jobs)
            .where(
                jobs.c.id == job_id,
                # Terminal is a no-op, not an error: a client that cancels twice
                # across a reconnect has to get the same answer both times, and
                # rewriting a `done` job would throw away a result that exists.
                jobs.c.state.notin_([JobState.DONE, JobState.DEAD, JobState.CANCELLED]),
            )
            .values(
                cancel_requested=True,
                state=case(
                    (jobs.c.state == JobState.RUNNING, literal(JobState.RUNNING.value)),
                    else_=literal(JobState.CANCELLED.value),
                ),
                updated_at=func.now(),
            )
            .returning(*_JOB_COLUMNS)
        )
        async with self._db.session() as s:
            row = (await s.execute(stmt)).first()
            await s.commit()
            if row is not None:
                return _to_job(row)
        # Either unknown or already terminal, and the caller wants those told
        # apart by the value rather than by an exception.
        return await self.get(job_id)

    # ----------------------------------------------------------------------
    # Consuming
    # ----------------------------------------------------------------------

    async def claim(self, job_id: uuid.UUID, *, owner: str, lease: timedelta) -> Lease | None:
        """Take this specific job if it is reservable. `None` if it is not.

        Note what is *not* here: the `order by … for update skip locked` scan of
        `docs/jobs.md`'s claim query. Under RabbitMQ the broker already chose
        which job, so this is a primary-key lookup with the reservability
        predicate as a filter — and the 80x `OR`-plan problem that document
        measures does not arise, because there is no `order by` to satisfy and
        one row to test. That cost belongs to the Postgres queue, not to this
        table.

        `None` is ordinary, not exceptional. A delivery for a job that was
        cancelled, finished elsewhere, or is already leased is the normal way a
        duplicate publish ends, and the row is the authority that says so.
        """
        lease_id = uuid.uuid4()
        stmt = (
            update(jobs)
            .where(jobs.c.id == job_id, or_(_WAITING, _LAPSED))
            .values(
                state=JobState.RUNNING,
                # Incremented on the claim, not on the outcome: a redelivery
                # after a crash has to be counted the same as one after a `nack`,
                # and the crashing consumer reports nothing.
                attempts=jobs.c.attempts + 1,
                lease_id=lease_id,
                lease_expires_at=func.now() + _interval(lease),
                lease_owner=owner,
                updated_at=func.now(),
            )
            .returning(*_JOB_COLUMNS, jobs.c.lease_expires_at)
        )
        async with self._db.session() as s:
            row = (await s.execute(stmt)).first()
            await s.commit()
            if row is None:
                return None
            return Lease(
                id=lease_id, job=_to_job(row), owner=owner, expires_at=row.lease_expires_at
            )

    async def lapsed_reservable(self) -> list[uuid.UUID]:
        """Jobs whose lease lapsed and which should go round again.

        Postgres finds these for free as part of its claim query; RabbitMQ
        cannot, because it has no per-message deadline and a delivery it has
        already made is not something it will reconsider. So the caller polls
        this and republishes — which is the concrete shape of `docs/jobs.md`'s
        finding that making RabbitMQ safe means running a small Postgres queue
        beside it. This query and the sweep are that queue, and there are only
        the two of them.

        Deliberately read-only. The row is left exactly as it is so that a
        consumer whose lease lapsed while nobody wanted the job can still `ack`
        it: fencing is on the token, and nothing has taken the token yet.
        """
        async with self._db.session() as s:
            rows = (await s.execute(select(jobs.c.id).where(_LAPSED))).all()
            return [row.id for row in rows]

    async def retire_lapsed(self) -> list[uuid.UUID]:
        """Finish jobs whose consumer died and is never coming back.

        Excluding them from `_LAPSED` is not enough on its own: that only makes
        them invisible, and invisible is not finished — a row stuck at `running`
        is a state a reader cannot tell from healthy, which is the failure the
        `streaming` row was in phase 1b.

        Two kinds. Out of attempts reaches `dead`, which is what stops a job
        that reliably kills whatever picks it up. Cancel-requested reaches
        `cancelled` however many attempts remain, because `nack` already rules
        that a cancelled job is never reported as a failure and the consumer
        that *crashes* is precisely the one that never calls `nack`. Note the
        `error` arm: a cancelled job must not inherit the lease-expiry message,
        since not putting an error in front of someone who asked for this is the
        entire point.
        """
        stmt = (
            update(jobs)
            .where(
                jobs.c.state == JobState.RUNNING,
                jobs.c.lease_expires_at < func.now(),
                or_(jobs.c.attempts >= jobs.c.max_attempts, jobs.c.cancel_requested),
            )
            .values(
                state=case(
                    (jobs.c.cancel_requested, literal(JobState.CANCELLED.value)),
                    else_=literal(JobState.DEAD.value),
                ),
                error=case(
                    (jobs.c.cancel_requested, jobs.c.error),
                    else_=func.coalesce(jobs.c.error, _LEASE_EXPIRED),
                ),
                lease_id=null(),
                lease_expires_at=null(),
                lease_owner=null(),
                updated_at=func.now(),
            )
            .returning(jobs.c.id)
        )
        async with self._db.session() as s:
            rows = (await s.execute(stmt)).all()
            await s.commit()
            return [row.id for row in rows]

    async def unpublished(self, older_than: timedelta) -> list[uuid.UUID]:
        """Committed jobs the broker never got — the dual write's one real failure.

        `run_after` rather than `created_at` as the age test, which is what lets
        one query cover both holes: a fresh enqueue has `run_after = created_at`,
        and a `nack`ed retry pushes it out by the backoff, so a job still inside
        its backoff is not mistaken for one whose republish was lost.

        No leader election and no `for update skip locked`, both for the same
        reason: a duplicate publish is the harmless failure of the three
        (`docs/jobs.md`), absorbed by the claim being a conditional update. Two
        API replicas sweeping the same row costs one discarded delivery.
        """
        stmt = select(jobs.c.id).where(
            jobs.c.published_at.is_(None),
            jobs.c.state.in_([JobState.QUEUED, JobState.FAILED]),
            jobs.c.run_after < func.now() - _interval(older_than),
        )
        async with self._db.session() as s:
            return [row.id for row in (await s.execute(stmt)).all()]

    # ----------------------------------------------------------------------
    # Finishing
    # ----------------------------------------------------------------------

    async def extend(self, lease: Lease, by: timedelta) -> Lease:
        stmt = (
            update(jobs)
            .where(*_fencing(lease))
            .values(lease_expires_at=func.now() + _interval(by), updated_at=func.now())
            .returning(*_JOB_COLUMNS, jobs.c.lease_expires_at)
        )
        async with self._db.session() as s:
            row = (await s.execute(stmt)).first()
            await s.commit()
            if row is None:
                raise StaleLease(f"lease {lease.id} is no longer live for job {lease.job.id}")
            # The row's job, not the lease's: the heartbeat is also how a
            # consumer learns it was cancelled while it was working, which saves
            # it a second call it would have to remember to make.
            return Lease(
                id=lease.id, job=_to_job(row), owner=lease.owner, expires_at=row.lease_expires_at
            )

    async def ack(self, lease: Lease, *, result_ref: str | None = None) -> None:
        stmt = (
            update(jobs)
            .where(*_fencing(lease))
            .values(
                state=JobState.DONE,
                result_ref=result_ref,
                **_RELEASED,
            )
            .returning(jobs.c.id)
        )
        async with self._db.session() as s:
            row = (await s.execute(stmt)).first()
            await s.commit()
            if row is None:
                raise StaleLease(f"lease {lease.id} is no longer live for job {lease.job.id}")

    async def nack(self, lease: Lease, *, error: str, retry_in: timedelta | None = None) -> Job:
        """Give the job back. Returns what it became, which decides republishing.

        The state is computed in SQL rather than read-then-written because the
        inputs are the row's, not the caller's: `cancel_requested` may have been
        set since this consumer last looked, and `attempts` was incremented by
        whoever claimed it. A read-modify-write here would be a lost update on
        exactly the cancel it is supposed to honour.
        """
        # `failed` is the only outcome that goes round again — and the only one
        # whose `published_at` must be cleared, so a republish lost to a crash
        # between this commit and `basic_publish` is found by `unpublished`.
        # Clearing it unconditionally would park every dead and cancelled row in
        # `ix_jobs_unpublished` forever, which is a partial index whose whole
        # value is that it is normally empty.
        returning_to_queue = and_(
            jobs.c.cancel_requested.is_(False),
            jobs.c.attempts < jobs.c.max_attempts,
        )
        stmt = (
            update(jobs)
            .where(*_fencing(lease))
            .values(
                state=case(
                    (jobs.c.cancel_requested, literal(JobState.CANCELLED.value)),
                    (jobs.c.attempts >= jobs.c.max_attempts, literal(JobState.DEAD.value)),
                    else_=literal(JobState.FAILED.value),
                ),
                error=error,
                run_after=func.now() + _interval(retry_in or timedelta()),
                published_at=case((returning_to_queue, null()), else_=jobs.c.published_at),
                **_RELEASED,
            )
            .returning(*_JOB_COLUMNS)
        )
        async with self._db.session() as s:
            row = (await s.execute(stmt)).first()
            await s.commit()
            if row is None:
                raise StaleLease(f"lease {lease.id} is no longer live for job {lease.job.id}")
            return _to_job(row)


def _fencing(lease: Lease):
    """Match the row *and* the token this consumer was given.

    Fencing is on `lease_id` alone and never on the deadline. The only thing the
    check has to prevent is *two* writers, so an expired lease nobody has taken
    is still its holder's; failing it would add a clock-skew race that protects
    nothing — and RabbitMQ, whose delivery is valid as long as the channel is,
    could not implement the stricter rule anyway.
    """
    return (jobs.c.id == lease.job.id, jobs.c.lease_id == lease.id)


# Every terminal write drops the lease. Spelled once because the `lease_is_whole`
# check constraint rejects half of one, and a forgotten `lease_expires_at` would
# leave a finished job looking like a live claim to `_LAPSED`.
_RELEASED = {
    "lease_id": null(),
    "lease_expires_at": null(),
    "lease_owner": null(),
    "updated_at": func.now(),
}
