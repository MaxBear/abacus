"""An in-memory `ObjectStore`, so the supervisor is testable with nothing up.

Here rather than in `core/` for the same reason `MemoryJobQueue` and
`MockChatRepository` are: nothing that ships imports it. What it exists to make
cheap is the assertion this phase actually cares about — *whether* an artifact
was written, and *when* relative to the `ack` — neither of which needs MinIO to
answer. What MinIO does with the bytes is `test_object_store.py`'s question and
is asked against the real thing.

`fail_with` is the one piece of dishonesty on purpose. A write that fails is a
supervisor path with a job state at the end of it, and a store that can only
succeed would leave it untested.
"""

from core.artifacts import ArtifactNotFound


class MemoryObjectStore:
    """`ObjectStore` in a dict, with a record of what happened in what order."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str]] = {}

        # Keys in write order, kept after a delete. The store's contents answer
        # "is it there now?"; this answers "was it ever written?", which is the
        # question a test about orphans has to ask.
        self.written: list[str] = []

        # Set to an exception to make the next `put` raise it.
        self.fail_with: Exception | None = None

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self._objects[key] = (data, content_type)
        self.written.append(key)

    async def get(self, key: str) -> bytes:
        try:
            return self._objects[key][0]
        except KeyError:
            raise ArtifactNotFound(key) from None

    async def delete(self, key: str) -> None:
        self._objects.pop(key, None)

    def content_type(self, key: str) -> str:
        return self._objects[key][1]

    def __contains__(self, key: str) -> bool:
        return key in self._objects

    def __len__(self) -> int:
        return len(self._objects)
