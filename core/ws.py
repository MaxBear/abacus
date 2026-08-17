"""WebSocket connection plumbing: both directions of one socket, and the
registry that owns every live connection on this replica.

Kept apart from `api/chat.py` so the transport mechanics — backpressure, close
codes, drain-on-shutdown — can be read and tested without the chat protocol in
the way. Nothing here knows what a frame means.
"""

import asyncio
import logging
from collections.abc import AsyncIterator

import anyio
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from core.protocol import ServerFrame

log = logging.getLogger(__name__)

# RFC 6455 plus the IANA-registered additions. Named because a bare 1013 at a
# call site tells the next reader nothing. docs/websocket.md carries the table
# of what each one obliges the client to do.
WS_NORMAL = 1000
WS_POLICY_VIOLATION = 1008
WS_INTERNAL_ERROR = 1011
WS_SERVICE_RESTART = 1012
WS_TRY_AGAIN_LATER = 1013


class _Close:
    """Writer control token: "flush what is ahead of me, then stop."

    A class rather than a bare `object()` so the queue can be typed as what it
    actually holds. `isinstance` then narrows the other branch to ServerFrame,
    which is what lets run_writer call `.model_dump_json()` without an ignore.
    """


_CLOSE = _Close()

# A write that loses the race with a disconnect. Starlette's send path has two
# shapes for this and neither is intuitive: its state check raises RuntimeError
# ("Cannot call send once a close message has been sent"), while a transport
# failure is caught as OSError and re-raised as WebSocketDisconnect(1006) —
# a *send* raising the exception the receive side is named for (websockets.py:
# WebSocket.send). anyio's pair covers the servers that surface a closed stream
# directly instead. OSError is kept for a transport that reaches us unwrapped.
#
# An explicit tuple rather than `except Exception`: a serialization bug must
# still surface as an internal error, not be misreported as a clean close.
_PEER_GONE = (
    RuntimeError,
    OSError,
    WebSocketDisconnect,
    anyio.ClosedResourceError,
    anyio.BrokenResourceError,
)


class Connection:
    """One socket, with a bounded outbound queue in front of it.

    Sends are non-blocking by construction. A client on hotel wifi cannot make
    a producer wait, and cannot make this process buffer without limit — the
    two ways one slow reader takes down every other session on the replica.
    """

    def __init__(self, websocket: WebSocket, session_id: str, *, send_queue_size: int) -> None:
        self._ws = websocket
        self.session_id = session_id
        self._out: asyncio.Queue[ServerFrame | _Close] = asyncio.Queue(maxsize=send_queue_size)
        self._overflowed = False
        self._close_code = WS_NORMAL
        # Set by the route handler once it has stopped serving. drain() waits on
        # this rather than guessing how long a flush takes.
        self.finished = asyncio.Event()

    def send(self, frame: ServerFrame) -> None:
        """Queue a frame. Never blocks, never raises.

        On overflow the connection is abandoned rather than buffered: the client
        reconnects and, from 1b, replays what it missed. Dropping a slow reader
        is only an acceptable answer because durability lives elsewhere.
        """
        if self._overflowed:
            return
        if not self._enqueue(frame):
            log.warning("send queue overflow, dropping connection (session=%s)", self.session_id)

    def request_close(self, code: int, frame: ServerFrame | None = None) -> None:
        """Ask the writer to flush and stop. Used by drain-on-shutdown.

        The sentinel is queued even on an already-overflowed connection, unlike
        `send`: this is the writer's stop signal, not a frame for the client.
        """
        if frame is not None:
            self.send(frame)
        self._close_code = code
        self._enqueue(_CLOSE)

    def _enqueue(self, item: ServerFrame | _Close) -> bool:
        """Queue an item, marking the connection overflowed if it will not fit.

        The one place the bound is enforced. Deliberately does not log: what a
        full queue *means* differs between a dropped frame and a stop signal,
        so the caller says it.
        """
        try:
            self._out.put_nowait(item)
            return True
        except asyncio.QueueFull:
            self._overflowed = True
            return False

    async def run_writer(self) -> int:
        """Drain the queue onto the socket. Returns the close code to use.

        Sole writer to the socket: interleaved `send_text` from concurrent turns
        would corrupt the frame stream, so every producer goes through the queue.
        """
        while True:
            frame = await self._out.get()
            if isinstance(frame, _Close):
                return self._close_code
            if self._overflowed:
                return WS_TRY_AGAIN_LATER
            try:
                await self._ws.send_text(frame.model_dump_json())
            except _PEER_GONE:
                # The peer went away between the queue and the wire. The reader
                # side ends its iteration and drives the teardown.
                return WS_NORMAL

    def __aiter__(self) -> AsyncIterator[str]:
        """Yield inbound text frames until the peer goes away.

        A disconnect ends the iteration rather than raising: it is the ordinary
        way a socket finishes, not an error, and containing it here keeps
        Starlette's exception types from reaching the protocol layer.
        """
        return self._incoming()

    async def _incoming(self) -> AsyncIterator[str]:
        try:
            while True:
                yield await self._ws.receive_text()
        except WebSocketDisconnect:
            log.debug("peer disconnected (session=%s)", self.session_id)

    async def close(self, code: int) -> None:
        """Close if still open. Safe to call after the peer has already gone."""
        if self._ws.client_state is WebSocketState.DISCONNECTED:
            return
        try:
            await self._ws.close(code=code)
        except RuntimeError as exc:  # already closing, or never accepted
            log.debug("close on a socket that was already gone: %s", exc)


class ConnectionRegistry:
    """Every connection this replica currently holds, indexed by session.

    In-memory and therefore replica-local, which is the whole reason phase 3
    needs a broker fanout: the pod holding a session's socket is rarely the pod
    that learns a job finished.
    """

    def __init__(self, *, max_per_session: int, drain_timeout_seconds: float) -> None:
        self._max_per_session = max_per_session
        self._drain_timeout = drain_timeout_seconds
        self._by_session: dict[str, set[Connection]] = {}

    def add(self, conn: Connection) -> bool:
        """Register a connection. False when the session is already at its cap."""
        conns = self._by_session.setdefault(conn.session_id, set())
        if len(conns) >= self._max_per_session:
            return False
        conns.add(conn)
        return True

    def remove(self, conn: Connection) -> None:
        conns = self._by_session.get(conn.session_id)
        if conns is None:
            return
        conns.discard(conn)
        # Drop the empty set: otherwise a long-lived process accumulates one
        # entry per session it has ever seen.
        if not conns:
            del self._by_session[conn.session_id]

    def for_session(self, session_id: str) -> list[Connection]:
        """Connections held here for a session. Phase 3's fanout delivers via this."""
        return list(self._by_session.get(session_id, ()))

    def __len__(self) -> int:
        return sum(len(c) for c in self._by_session.values())

    async def drain(self, going_away: ServerFrame) -> None:
        """Close every connection ahead of shutdown.

        Kubernetes does not drain WebSockets; without this, a rolling deploy
        severs live sockets mid-frame when the grace period expires. Bounded,
        because a connection that will not flush must not hold up the whole pod
        past terminationGracePeriodSeconds.
        """
        conns = [c for s in self._by_session.values() for c in s]
        if not conns:
            return

        log.info("draining %d websocket connection(s)", len(conns))
        for conn in conns:
            conn.request_close(WS_SERVICE_RESTART, going_away)

        waiters = [asyncio.create_task(c.finished.wait()) for c in conns]
        done, pending = await asyncio.wait(waiters, timeout=self._drain_timeout)
        for task in pending:
            task.cancel()
        if pending:
            log.warning("%d connection(s) did not drain in time", len(pending))
