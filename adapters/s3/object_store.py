"""The S3 implementation of `core.artifacts.ObjectStore`.

`aiobotocore` rather than `boto3`, for the reason that decides most of this
phase: the supervisor's event loop must keep scheduling while a solve runs, and
a synchronous `put_object` on that loop is a multi-second stall in the same
place `docs/worker.md` refuses to put a five-minute one. The child process is
free to be synchronous; nothing that shares a loop with an AMQP heartbeat is.

What crosses this line is bytes and a ref. `botocore`'s exceptions do not — a
`ClientError` with a string code inside is exactly the kind of storage detail
`core/` is arranged not to know, and the one distinction a caller can act on
(`ArtifactNotFound`) is worth translating by hand.
"""

import logging
from contextlib import AsyncExitStack

from aiobotocore.session import get_session
from botocore.config import Config
from botocore.exceptions import ClientError

from core.artifacts import ArtifactNotFound
from core.config import Settings

log = logging.getLogger(__name__)

# The error codes S3 uses for "that key is not there". Two of them, because a
# `head`/`get` on a missing key answers `NoSuchKey` while a missing *bucket* or
# an unauthorized prefix can surface as `404`, and MinIO is not identical to AWS
# here. Matching on both is what keeps the translation honest across the two.
_MISSING = {"NoSuchKey", "NoSuchBucket", "404"}


class S3ObjectStore:
    """Owns one S3 client and its lifecycle.

    Constructed by whichever composition root needs it — phase 3's worker
    entrypoint, and phase 5's API when it starts serving artifacts back — and
    never reached for as a module global, matching `Database` and
    `RabbitMQJobQueue`.
    """

    def __init__(self, bucket: str, client, stack: AsyncExitStack) -> None:
        self._bucket = bucket
        self._client = client
        # The client is an async context manager, so something has to hold its
        # exit. Kept as a stack rather than the client itself because `close`
        # then unwinds whatever `start` entered, however that grows.
        self._stack = stack

    @classmethod
    async def start(cls, settings: Settings) -> "S3ObjectStore":
        """Open the client. Does not create the bucket — see `ensure_bucket`.

        The retry configuration is explicit rather than defaulted because the
        default is `legacy`, which retries three times and does not back off on
        throttling. `standard` is the one that handles a 503 the way a worker
        holding a lease needs it handled: this is the last write before an `ack`,
        and a failure here costs the whole solve.
        """
        stack = AsyncExitStack()
        client = await stack.enter_async_context(
            get_session().create_client(
                "s3",
                endpoint_url=settings.s3_endpoint_url,
                region_name=settings.s3_region,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                config=Config(
                    retries={"max_attempts": 3, "mode": "standard"},
                    # MinIO serves one host and routes by key prefix; virtual
                    # host addressing would resolve `bucket.minio` and fail
                    # inside compose's DNS. AWS accepts path style too.
                    s3={"addressing_style": "path"},
                ),
            )
        )
        return cls(settings.s3_bucket, client, stack)

    async def ensure_bucket(self) -> None:
        """Create the bucket if it is not there. Idempotent.

        Deliberately not called by `start`, and this is the whole reason it is a
        separate method: locally the bucket is nobody's job and this is the
        cheapest way to have one, while in phase 6 it is Terraform's, and a
        worker that creates buckets needs a permission it should never hold. A
        composition root that wants it says so; a deployed one does not.
        """
        try:
            await self._client.head_bucket(Bucket=self._bucket)
        except ClientError as exc:
            if _code(exc) not in _MISSING:
                raise
            log.info("creating bucket %s", self._bucket)
            await self._client.create_bucket(Bucket=self._bucket)

    async def put(self, key: str, data: bytes, *, content_type: str) -> str:
        await self._client.put_object(
            Bucket=self._bucket, Key=key, Body=data, ContentType=content_type
        )
        # The key, not a URI. The bucket is deployment configuration and is
        # already in the environment of everything that could read this back;
        # copying it into every `jobs` row would mean a rename rewrites history
        # to describe objects that did not move, and a ref naming a foreign
        # bucket is an access-control question phase 3 has no answer to.
        return key

    async def get(self, ref: str) -> bytes:
        try:
            response = await self._client.get_object(Bucket=self._bucket, Key=ref)
        except ClientError as exc:
            if _code(exc) in _MISSING:
                raise ArtifactNotFound(ref) from exc
            raise
        async with response["Body"] as body:
            return await body.read()

    async def delete(self, ref: str) -> None:
        # `delete_object` on a missing key succeeds on both S3 and MinIO, so
        # idempotence is the backing store's here rather than something this
        # method has to arrange.
        await self._client.delete_object(Bucket=self._bucket, Key=ref)

    async def close(self) -> None:
        await self._stack.aclose()


def _code(exc: ClientError) -> str:
    """The error code botocore buried, as a plain string."""
    return str(exc.response.get("Error", {}).get("Code", ""))
