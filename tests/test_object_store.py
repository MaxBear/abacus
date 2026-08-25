"""The object store contract, and the key discipline the whole phase rests on.

Two halves, and the split is the point. `artifact_key` is pure and its tests run
in `make test` with nothing running, because the rule it encodes — two attempts
at one job never share a destination — is the mechanism behind
`docs/worker.md`'s "two executions, one effect" and deserves to be falsifiable
without infrastructure. Everything below it needs a real bucket, because what is
being asserted is what MinIO does rather than what this code intends.
"""

import uuid

import pytest
from botocore.exceptions import EndpointConnectionError

from adapters.s3.object_store import S3ObjectStore
from core.artifacts import ArtifactNotFound, artifact_key
from core.config import get_settings


def test_two_attempts_at_one_job_never_share_a_key():
    job = uuid.uuid4()
    first, second = artifact_key(job, uuid.uuid4()), artifact_key(job, uuid.uuid4())

    assert first != second
    # Same job, so the same prefix: an orphan reaper or a lifecycle rule keyed
    # on the job has to be able to find both, which is the other half of why the
    # job id leads.
    assert first.startswith(f"jobs/{job}/")
    assert second.startswith(f"jobs/{job}/")


def test_a_key_is_the_same_every_time_it_is_asked_for():
    """A retry of the *same* attempt must overwrite, not accumulate.

    A key with anything time-varying in it would make a supervisor that retries
    its own upload leave one orphan per try, and nothing would ever collect them
    — they belong to a lease that did install a pointer.
    """
    job, lease = uuid.uuid4(), uuid.uuid4()
    assert artifact_key(job, lease) == artifact_key(job, lease)


@pytest.fixture
async def store():
    """A store on real MinIO, or a skip when nothing answers.

    Kept here rather than in `conftest.py` because one module wants it — that
    file's own rule. It moves the day the supervisor's tests need it too.
    """
    settings = get_settings()
    store = await S3ObjectStore.start(settings)
    written: list[str] = []
    # Everything after `start` is inside this, including the skip: the client
    # holds an aiohttp session, and the reachability probe is not the only thing
    # that can fail here. `head_bucket` under scoped credentials answers
    # `AccessDenied`, which is a `ClientError` and would otherwise leave the
    # session open for the rest of the run.
    try:
        try:
            await store.ensure_bucket()
        except (EndpointConnectionError, OSError):
            pytest.skip("no object store reachable — `make up`, and S3_ENDPOINT_URL in .env")

        _put = store.put

        async def put(key, data, *, content_type="application/octet-stream"):
            written.append(key)
            await _put(key, data, content_type=content_type)

        store.put = put
        yield store
    finally:
        # The bucket is shared across runs and nothing lists it, so a test that
        # does not clean up after itself leaves an object no later run can even
        # find. Cheaper than a bucket per run, and unlike the RabbitMQ fixture's
        # topology there is nothing here for a leftover to race with.
        for key in written:
            await store.delete(key)
        await store.close()


@pytest.mark.s3
async def test_put_then_get_round_trips_the_bytes(store):
    key = artifact_key(uuid.uuid4(), uuid.uuid4())
    await store.put(key, b"\x00result\xff", content_type="application/json")

    assert await store.get(key) == b"\x00result\xff"


@pytest.mark.s3
async def test_get_of_a_ref_that_was_never_written_raises(store):
    """The failure a dangling `result_ref` would produce, named so it is legible.

    `docs/worker.md` orders the write before the `ack` so this cannot happen to a
    real job. That it raises something a caller can catch by type, rather than a
    `ClientError` with a string code inside, is what keeps that argument checkable
    from `core/`.
    """
    with pytest.raises(ArtifactNotFound):
        await store.get(artifact_key(uuid.uuid4(), uuid.uuid4()))


@pytest.mark.s3
async def test_two_attempts_at_one_job_leave_two_objects(store):
    """The key discipline, end to end rather than as string arithmetic.

    This is the redelivery case: one job, two leases, two workers that both
    finish. Neither overwrites the other, so whichever loses the fenced `ack` has
    written garbage rather than damage.
    """
    job = uuid.uuid4()
    first, second = artifact_key(job, uuid.uuid4()), artifact_key(job, uuid.uuid4())
    await store.put(first, b"first", content_type="text/plain")
    await store.put(second, b"second", content_type="text/plain")

    assert await store.get(first) == b"first"
    assert await store.get(second) == b"second"


@pytest.mark.s3
async def test_delete_removes_the_object_and_deleting_again_is_fine(store):
    key = artifact_key(uuid.uuid4(), uuid.uuid4())
    await store.put(key, b"x", content_type="text/plain")

    await store.delete(key)
    with pytest.raises(ArtifactNotFound):
        await store.get(key)

    # The reaper this exists for cannot know what another pass already took.
    await store.delete(key)


@pytest.mark.s3
async def test_ensure_bucket_is_idempotent(store):
    """It runs on every local start, so the second call is the normal one."""
    await store.ensure_bucket()
    await store.ensure_bucket()
