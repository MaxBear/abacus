"""The chat protocol driven over one connection.

Sits between `core/protocol.py`, which defines what a frame *is*, and
`core/ws.py`, which knows how to move bytes and nothing about their meaning.
Everything here is the part in the middle: which frame gets which reply, when a
turn may start, and how the two sides are shut down together.

Deliberately free of FastAPI — it takes a Connection, a Responder, and Settings,
so the whole protocol can be exercised without a route or an ASGI server.

The design and its reasoning live in docs/websocket.md.
"""

import asyncio
import logging
import uuid

from pydantic import ValidationError

from core.config import Settings
from core.protocol import (
    CLIENT_FRAME_ADAPTER,
    PROTOCOL_VERSION,
    Ack,
    Delta,
    Done,
    Error,
    ErrorCode,
    Ping,
    Pong,
)
from core.responder import Responder
from core.ws import WS_INTERNAL_ERROR, WS_NORMAL, Connection

log = logging.getLogger(__name__)


class ConnectionHandler:
    """Drives one accepted connection: reader, writer, and the turns in flight.

    Scoped to a single socket, not to a chat session — a session outlives its
    connections and can hold several at once, so nothing here may be reused
    across reconnects.
    """

    def __init__(self, conn: Connection, responder: Responder, settings: Settings) -> None:
        self._conn = conn
        self._responder = responder
        self._settings = settings
        self._turns: set[asyncio.Task] = set()

    async def serve(self) -> None:
        """Run the reader and the writer until either stops, then close once."""
        writer = asyncio.create_task(self._conn.run_writer(), name="ws-writer")
        reader = asyncio.create_task(self._read_frames(), name="ws-reader")

        # Bound before the try so the close below always has a code. WS_NORMAL
        # is also the right answer for the ordinary case: a reader that ended
        # because the peer disconnected.
        code = WS_NORMAL
        try:
            done, _pending = await asyncio.wait(
                {writer, reader}, return_when=asyncio.FIRST_COMPLETED
            )

            # The writer decides the close code — it is the side that knows
            # whether the queue overflowed or a drain was requested.
            for task in done:
                exc = task.exception()
                if exc is not None:
                    log.error(
                        "websocket task failed (session=%s)", self._conn.session_id, exc_info=exc
                    )
                    code = WS_INTERNAL_ERROR
                elif task is writer:
                    code = task.result()
        finally:
            # asyncio has no parent/child link between tasks, so whatever unwinds
            # this coroutine has to stop these two itself or they outlive it.
            # In the ordinary path one of them is already done and cancelling it
            # is a no-op. The path that needs this is a cancellation from
            # outside — uvicorn cancels the ASGI task once
            # timeout_graceful_shutdown is exceeded, and asyncio.wait does not
            # cancel what it awaits, so both would be left running with nothing
            # holding a reference to close them.
            for task in (writer, reader):
                task.cancel()
            await asyncio.gather(writer, reader, return_exceptions=True)

        await self._conn.close(code)

    async def _read_frames(self) -> None:
        """Read until the peer goes away, dispatching each frame.

        Turns run as their own tasks so a five-minute solve cannot stop this loop
        from answering a ping — the thing that would let an intermediary conclude
        the connection is idle and reap it mid-answer.
        """
        try:
            async for raw in self._conn:
                self._dispatch(raw)
        finally:
            for task in self._turns:
                task.cancel()
            await asyncio.gather(*self._turns, return_exceptions=True)

    def _dispatch(self, raw: str) -> None:
        try:
            frame = CLIENT_FRAME_ADAPTER.validate_json(raw)
        except ValidationError as exc:
            # An error frame, not a close: the connection is fine, this one message
            # was not. An unimplemented type (cancel, resume) lands here too, which
            # is the truthful answer until the phase that implements it.
            self._conn.send(
                Error(
                    code=ErrorCode.BAD_FRAME,
                    message=f"{exc.error_count()} validation error(s)",
                    retryable=False,
                )
            )
            return

        if frame.v > PROTOCOL_VERSION:
            self._conn.send(
                Error(
                    code=ErrorCode.UNSUPPORTED_VERSION,
                    message=f"this server speaks v{PROTOCOL_VERSION}",
                    retryable=False,
                )
            )
            return

        if isinstance(frame, Ping):
            self._conn.send(Pong())
            return

        if len(self._turns) >= self._settings.ws_max_concurrent_turns:
            self._conn.send(
                Error(
                    code=ErrorCode.TOO_MANY_TURNS,
                    message="too many turns in flight on this connection",
                    retryable=True,
                )
            )
            return

        message_id = uuid.uuid4().hex
        self._conn.send(Ack(client_msg_id=frame.client_msg_id, message_id=message_id))

        task = asyncio.create_task(self._run_turn(frame.text, message_id))
        self._turns.add(task)
        # Strong reference until completion: a bare create_task can be garbage
        # collected mid-flight, which loses the turn with no error anywhere.
        task.add_done_callback(self._turns.discard)

    async def _run_turn(self, text: str, message_id: str) -> None:
        try:
            index = 0
            async for chunk in self._responder.respond(text):
                self._conn.send(Delta(message_id=message_id, chunk_index=index, text=chunk))
                index += 1
                # Hand the loop to the writer between frames. A responder that
                # never awaits — the stub, and any future one that yields from a
                # buffer — would otherwise produce the whole reply in a single
                # uninterrupted burst, filling the send queue while the only task
                # that drains it never gets scheduled. The bound is there to drop
                # a slow reader, not a long reply.
                await asyncio.sleep(0)
            self._conn.send(Done(message_id=message_id))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - one failed turn must not kill the socket
            log.exception(
                "turn failed (session=%s message_id=%s)", self._conn.session_id, message_id
            )
            self._conn.send(
                Error(code=ErrorCode.INTERNAL, message="the turn failed", retryable=True)
            )
