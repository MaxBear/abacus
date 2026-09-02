"""The chat protocol driven over one connection.

Sits between `core/frames.py`, which defines what a frame *is*, and
`core/ws.py`, which knows how to move bytes and nothing about their meaning.
Everything here is the part in the middle: which frame gets which reply, when a
turn may start, and how the two sides are shut down together.

Deliberately free of FastAPI — it takes a Connection, a Responder, and Settings,
so the whole protocol can be exercised without a route or an ASGI server.

The design and its reasoning live in docs/websocket.md.
"""

import asyncio
import contextlib
import logging
import uuid

from pydantic import ValidationError

from core.chat_repository import ChatRepository, StoredMessage
from core.config import Settings
from core.frames import (
    CLIENT_FRAME_ADAPTER,
    PROTOCOL_VERSION,
    Ack,
    Delta,
    Done,
    Error,
    ErrorCode,
    JobStatus,
    Message,
    Ping,
    Pong,
    Resume,
    ServerFrame,
    UserMessage,
)
from core.jobs import JobEvent
from core.responder import Responder
from core.ws import WS_INTERNAL_ERROR, WS_NORMAL, Connection

log = logging.getLogger(__name__)


class ConnectionHandler:
    """Drives one accepted connection: reader, writer, and the turns in flight.

    Scoped to a single socket, not to a chat session — a session outlives its
    connections and can hold several at once, so nothing here may be reused
    across reconnects.
    """

    def __init__(
        self,
        conn: Connection,
        repository: ChatRepository,
        responder: Responder,
        settings: Settings,
    ) -> None:
        self._conn = conn
        self._repo = repository
        self._responder = responder
        self._settings = settings
        self._turns: set[asyncio.Task] = set()
        # client_msg_id -> the assistant row a turn on *this* connection is
        # streaming into, so a resubmitted message can be re-acked with the
        # stream it already has. Bounded by ws_max_concurrent_turns: an entry is
        # evicted the moment its turn ends, which is also what keeps it honest —
        # it never names a stream that is no longer running.
        self._live_replies: dict[str, uuid.UUID] = {}

    async def serve(self) -> None:
        """Run the reader and the writer until either stops, then close once."""
        try:
            # Before either task starts: every message stored below allocates
            # its seq from this session's row, so a turn that ran ahead of it
            # dies in the allocator — `no such chat session`, from a write the
            # client was told would be durable.
            await self._repo.ensure_session(self._conn.session_id)
        except Exception:
            log.exception("could not open session (session=%s)", self._conn.session_id)
            # No writer task yet, so an error frame would only sit in the queue
            # unread. 1011 is the code that tells the client to reconnect with
            # backoff, which is the right answer to a database that is down.
            await self._conn.close(WS_INTERNAL_ERROR)
            return

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
                await self._dispatch(raw)
        finally:
            for task in self._turns:
                task.cancel()
            await asyncio.gather(*self._turns, return_exceptions=True)

    async def _dispatch(self, raw: str) -> None:
        try:
            frame = CLIENT_FRAME_ADAPTER.validate_json(raw)
        except ValidationError as exc:
            # An error frame, not a close: the connection is fine, this one message
            # was not. An unimplemented type (cancel) lands here too, which is the
            # truthful answer until the phase that implements it.
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

        if isinstance(frame, Resume):
            # Ahead of the turn cap deliberately: replaying history starts no
            # turn, and a client that reconnected mid-solve is both the most
            # likely to be at the cap and the most in need of the frames it
            # missed. Refusing it there would deny history to exactly the
            # session that lost some.
            try:
                await self._resume(frame)
            except Exception:  # noqa: BLE001 - a database blip must not kill the socket
                log.exception("could not resume (session=%s)", self._conn.session_id)
                self._conn.send(
                    Error(
                        code=ErrorCode.INTERNAL,
                        message="the session could not be replayed",
                        retryable=True,
                    )
                )
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

        try:
            await self._start_turn(frame)
        except Exception:  # noqa: BLE001 - a database blip must not kill the socket
            # A message that could not be stored was not accepted, and saying so
            # is the whole point of `ack` meaning durable. Retryable, because the
            # client resends under the same client_msg_id and the idempotency
            # key makes that safe however far the first attempt got.
            log.exception("could not record message (session=%s)", self._conn.session_id)
            self._conn.send(
                Error(
                    code=ErrorCode.INTERNAL,
                    message="the message was not stored",
                    retryable=True,
                )
            )

    async def _resume(self, frame: Resume) -> None:
        """Replay what this session recorded after `last_seq`.

        Whole messages, not token streams: the accumulated text reaches the same
        rendered state as re-streaming would, in one frame instead of one per
        chunk. Rows come back in `seq` order, so the client applies them by
        appending and its cursor advances monotonically.

        Since phase 3 the log has a second source, and the handler cannot tell:
        `log_since` returns messages and job transitions already merged on the
        `seq` both draw from. That is what makes the `chat.events` fan-out an
        optimisation — a live `job_status` nobody was connected to receive is
        found here instead.
        """
        limit = self._settings.ws_resume_max_messages
        # One past the bound, so "further behind than we will replay" is
        # answerable from this query rather than from a second count(*).
        missed = await self._repo.log_since(self._conn.session_id, frame.last_seq, limit + 1)

        if len(missed) > limit:
            # Truncating instead would hand the client a gap it cannot see: it
            # would advance its cursor past frames it never received and never
            # ask for them again. Better to say the cursor is unusable.
            self._conn.send(
                Error(
                    code=ErrorCode.RESUME_TOO_OLD,
                    message=f"more than {limit} entries since seq {frame.last_seq}",
                    retryable=False,
                )
            )
            return

        for entry in missed:
            self._conn.send(_replayed(entry))

    async def _start_turn(self, frame: UserMessage) -> None:
        """Store the message, open a reply, ack both, and spawn the turn.

        Both writes happen inline, before the turn is spawned, so `seq` follows
        the order messages were sent and the two rows of a turn stay adjacent.
        They are allowed on the read loop because they are two single statements
        behind one row lock; the thing that may not sit here is the reply.
        """
        recorded = await self._repo.record_user_message(
            self._conn.session_id, frame.client_msg_id, frame.text
        )

        if recorded.created:
            # Opened before the ack rather than inside the turn: the ack names
            # the stream, so the row it names has to exist first.
            assistant = await self._repo.start_assistant_message(self._conn.session_id)
            self._live_replies[frame.client_msg_id] = assistant.message_id
            reply_id = assistant.message_id
        else:
            # The key was already stored, so this is a replay, not a second
            # question: re-ack the row that exists and start nothing. The stream
            # is named only if that turn is still running here — a replay after
            # a reconnect reaches a handler that never started it, and its turn
            # died with the socket that did.
            assistant = None
            reply_id = self._live_replies.get(frame.client_msg_id)

        # One ack for both paths. Only the named stream differs; the other three
        # fields describe the stored user message, which is the same row either
        # way — and this frame gains fields as the protocol version grows.
        self._conn.send(
            Ack(
                client_msg_id=frame.client_msg_id,
                seq=recorded.message.seq,
                message_id=recorded.message.message_id.hex,
                reply_message_id=reply_id.hex if reply_id is not None else None,
            )
        )

        if assistant is None:  # a replay: the ack was the whole of the answer
            return

        task = asyncio.create_task(self._run_turn(frame.text, assistant))
        self._turns.add(task)

        def _finished(task: asyncio.Task) -> None:
            self._turns.discard(task)
            # Evicted only now, so the entry never outlives the stream it names.
            # _run_turn has already written the row's terminal state by here, so
            # a replay that misses the map finds a resolved row instead — which
            # is the answer resume gives it.
            self._live_replies.pop(frame.client_msg_id, None)

        # Strong reference until completion: a bare create_task can be garbage
        # collected mid-flight, which loses the turn with no error anywhere.
        task.add_done_callback(_finished)

    async def _run_turn(self, text: str, assistant: StoredMessage) -> None:
        message_id = assistant.message_id
        chunks: list[str] = []
        try:
            index = 0
            async for chunk in self._responder.respond(text):
                chunks.append(chunk)
                self._conn.send(Delta(message_id=message_id.hex, chunk_index=index, text=chunk))
                index += 1
                # Hand the loop to the writer between frames. A responder that
                # never awaits — the stub, and any future one that yields from a
                # buffer — would otherwise produce the whole reply in a single
                # uninterrupted burst, filling the send queue while the only task
                # that drains it never gets scheduled. The bound is there to drop
                # a slow reader, not a long reply.
                await asyncio.sleep(0)

            # Written before `done` is queued, not after: the frame says the text
            # is final, and a client that reconnects on the strength of it has to
            # find that text in the log rather than an empty streaming row.
            await self._repo.complete_assistant_message(message_id, "".join(chunks))
            self._conn.send(Done(message_id=message_id.hex, seq=assistant.seq))
        except asyncio.CancelledError:
            # The socket is going away and this turn dies with it. Shielded so
            # the row still reaches a terminal state: left at `streaming` it is
            # indistinguishable, to the next connection, from one still in
            # flight. Best effort only: if a second cancellation arrives before
            # this write lands, the row stays `streaming`, and closing that gap
            # needs a sweep at resume time rather than anything available here.
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(self._mark_failed(message_id))
            raise
        except Exception:  # noqa: BLE001 - one failed turn must not kill the socket
            log.exception(
                "turn failed (session=%s message_id=%s)", self._conn.session_id, message_id
            )
            await self._mark_failed(message_id)
            self._conn.send(
                Error(code=ErrorCode.INTERNAL, message="the turn failed", retryable=True)
            )

    async def _mark_failed(self, message_id: uuid.UUID) -> None:
        """Mark a turn failed without letting that write become the failure.

        The caller is already handling something that went wrong; a database
        error raised from here would replace it, and would escape a task nobody
        awaits — which asyncio reports, much later, as an unretrieved exception.
        """
        try:
            await self._repo.fail_assistant_message(message_id)
        except Exception:  # noqa: BLE001 - see above
            log.exception("could not mark message failed (message_id=%s)", message_id)


def _replayed(entry: StoredMessage | JobEvent) -> ServerFrame:
    """The frame for one row of the session log.

    A function rather than a method on the rows themselves: `core/frames.py`
    imports the row types, so the dependency runs frames → repository and must
    not run back. Mapping here is what keeps that direction, and it is the same
    reason `log_since` returns rows instead of frames.
    """
    if isinstance(entry, JobEvent):
        return JobStatus(seq=entry.seq, job_id=entry.job_id.hex, state=entry.state)
    return Message(
        seq=entry.seq,
        message_id=entry.message_id.hex,
        role=entry.role,
        status=entry.status,
        text=entry.text,
    )
