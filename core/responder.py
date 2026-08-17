"""The seam the LLM gateway will later fill.

Kept apart from `core/protocol.py` because the two change for unrelated reasons:
the frame models move when the wire format does — a new frame type, a new field,
a version bump — while this moves when the thing *producing* replies changes.
Phase 4 replaces the implementation below without touching a frame, and 1b adds
frames without touching this.

Deliberately narrow. A responder is handed text and yields text; it never sees a
Connection, a session id, or a frame, so the handler stays the only place that
knows how a reply becomes deltas on a socket.
"""

from collections.abc import AsyncIterator
from typing import Protocol


class Responder(Protocol):
    """Turns a user message into a stream of reply chunks.

    The phase-4 LLM gateway implements this exact signature, which is the whole
    reason phase 1 is built against a stub: swapping the implementation must not
    change a frame type, an endpoint, or a test's expectations about either.
    """

    def respond(self, text: str) -> AsyncIterator[str]:
        """Yield the reply in chunks. Chunk boundaries carry no meaning."""
        ...


class StubResponder:
    """Echoes the user's message back a word at a time.

    Deliberately streams in several chunks rather than returning one string:
    the point of having a stub at all is to exercise the multi-frame delta path
    — ordering, chunk indices, interleaving between concurrent turns — before
    there is a model behind it.
    """

    async def respond(self, text: str) -> AsyncIterator[str]:
        yield "echo: "
        for word in text.split():
            yield word + " "
