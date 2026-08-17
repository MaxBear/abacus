"""The chat WebSocket route.

The endpoint and its handshake policy, nothing else: what may connect, and the
wiring that hands an accepted socket to a ConnectionHandler. The protocol itself
lives in core/chat_handler.py, per the layering in README.md.

The design and its reasoning live in docs/websocket.md.
"""

import logging
import uuid

from fastapi import APIRouter, WebSocket

from api.deps import RegistryDep, ResponderDep, SettingsDep
from core.chat_handler import ConnectionHandler
from core.config import Settings
from core.protocol import PROTOCOL_VERSION, Error, ErrorCode
from core.ws import WS_POLICY_VIOLATION, WS_TRY_AGAIN_LATER, Connection

log = logging.getLogger(__name__)
router = APIRouter(tags=["chat"])


@router.websocket("/ws/chat/{session_id}")
async def chat_socket(
    websocket: WebSocket,
    session_id: str,
    settings: SettingsDep,
    registry: RegistryDep,
    responder: ResponderDep,
) -> None:
    refusal = _handshake_refusal(websocket, session_id, settings)
    if refusal is not None:
        # Refused *before* accept(): the ASGI server turns a close-before-accept
        # into an HTTP 403, so the client's connect fails outright. Accepting and
        # then closing instead looks to the client like a working connection that
        # dropped — which reconnect logic retries, forever, at full speed.
        log.warning("refusing websocket handshake (session=%s): %s", session_id, refusal)
        await websocket.close(code=WS_POLICY_VIOLATION, reason=refusal)
        return

    await websocket.accept()
    conn = Connection(websocket, session_id, send_queue_size=settings.ws_send_queue_size)

    if not registry.add(conn):
        # No writer task yet, so this send is unambiguously ordered before the close.
        await websocket.send_text(
            Error(
                code=ErrorCode.TOO_MANY_CONNECTIONS,
                message="too many connections for this session",
                retryable=True,
            ).model_dump_json()
        )
        await websocket.close(code=WS_TRY_AGAIN_LATER)
        return

    try:
        await ConnectionHandler(conn, responder, settings).serve()
    finally:
        registry.remove(conn)
        conn.finished.set()


def _handshake_refusal(websocket: WebSocket, session_id: str, settings: Settings) -> str | None:
    """Why this handshake must be rejected, or None to proceed."""
    origin = websocket.headers.get("origin")
    # A missing Origin means a non-browser client (curl, a test, a service).
    # Browsers always send one on a WebSocket handshake, and they are the only
    # clients that carry ambient credentials — so the check that matters is on
    # the value when present, not on its presence.
    if origin is not None and not _origin_allowed(origin, settings):
        return "origin not allowed"

    requested = websocket.query_params.get("v")
    if requested is not None:
        # Negotiated at the handshake, not per frame: a client that cannot be
        # understood should fail to connect, before any session state exists to
        # reconcile.
        try:
            if int(requested) > PROTOCOL_VERSION:
                return "unsupported protocol version"
        except ValueError:
            return "malformed protocol version"

    try:
        # Canonical form only. uuid.UUID() also accepts braces, a urn: prefix,
        # uppercase, and the undashed hex — all distinct strings that name the
        # same session. The raw path value is the registry key, so accepting the
        # variants would split one session across several fan-out buckets.
        # Server-minted ids are always canonical; anything else is a bug or a
        # probe, and better surfaced here than repaired silently.
        if str(uuid.UUID(session_id)) != session_id:
            return "session_id must be a canonical uuid"
    except ValueError:
        return "session_id must be a uuid"

    return None


def _origin_allowed(origin: str, settings: Settings) -> bool:
    if settings.ws_allowed_origins:
        return origin in settings.ws_allowed_origins
    # Fails closed everywhere but local: an unset allowlist in production must
    # not silently mean "any origin".
    return settings.env == "local"
