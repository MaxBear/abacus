"""The artifact seam: where a solve's output goes, independent of what stores it.

Kept in `core/` and expressed as a Protocol for the same reason `JobQueue` and
`ChatRepository` are — the worker must be exercisable without a bucket, and
`adapters/s3/` supplies the one implementation this phase needs.

Narrow on purpose, and narrower than an object store generally is. There is no
`list`, and that absence is load-bearing rather than an omission: `docs/worker.md`
settles that the row's `result_ref` is the only pointer that means anything, and
a `list` here is the first step of the design where two attempts race to be found
by a scan. Nothing enumerates the bucket, so nothing can mistake a loser's output
for a result.
"""

import uuid
from typing import Protocol


class ArtifactNotFound(Exception):
    """No object lives at that key.

    A distinct type because the caller can act on it: a `result_ref` that
    resolves to nothing is a bug in this system — the write precedes the `ack`
    precisely so it cannot happen — while any other storage failure is the
    ordinary bad day that a retry may well survive.
    """


def artifact_key(job_id: uuid.UUID, lease_id: uuid.UUID) -> str:
    """Where one *attempt* at a job writes. Two attempts never share a key.

    The lease id is in the key rather than only the job id, and that is the
    whole mechanism behind `docs/worker.md`'s "two executions, one effect": a
    redelivered job is a second attempt with a second lease, so it writes
    somewhere else and there is no last-writer-wins to reason about. The job id
    stays in front so every attempt at one job shares a prefix — which is what a
    lifecycle rule or an orphan reaper would key on, both of which the document
    leaves open.

    No extension. What the bytes are is phase 4's decision and travels with the
    object as its content type, where a reader can actually consult it; a key
    that claims `.json` is a claim nothing enforces.
    """
    return f"jobs/{job_id}/{lease_id}"


class ObjectStore(Protocol):
    """Durable storage for artifacts, addressed by key.

    Implementations own their own client and its lifecycle. A caller never sees
    a bucket, a session, or a botocore exception.
    """

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        """Store the bytes under `key`.

        One request, never multipart. `docs/worker.md` rests "no partial
        artifact is ever visible as a finished one" on the atomicity of a
        single-object `PUT` — the object does not exist until the upload
        completes — and a multipart upload would move that guarantee into the
        caller's error handling, where a crash mid-sequence is the caller's
        problem to notice. The cost is that an artifact is held in memory whole,
        which is the right trade at phase 4's result sizes and the thing to
        revisit before it is not.

        Returns nothing, because there is nothing to return that the caller does
        not already hold. The key *is* the ref: `Job.result_ref` stores exactly
        this string, and `get` takes exactly it back. An opaque handle minted
        here would be the shape to reach for if a ref could ever be something
        else — a bucket-qualified URI, a signed URL — and `S3ObjectStore.put`
        settles that it cannot.
        """
        ...

    async def get(self, key: str) -> bytes:
        """The bytes at `key`, or `ArtifactNotFound`."""
        ...

    async def delete(self, key: str) -> None:
        """Remove the object. Idempotent: deleting nothing is not an error.

        Not on any correctness path — no artifact is ever overwritten or
        replaced. It exists for the two housekeeping jobs that would otherwise
        need `list`: a test cleaning up after itself, and the orphan reaper
        `docs/worker.md` leaves open.
        """
        ...
