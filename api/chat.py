"""The chat WebSocket route.

The endpoint and its handshake policy, nothing else: what may connect, and the
wiring that hands an accepted socket to a ConnectionHandler. The protocol itself
lives in core/chat_handler.py, per the layering in README.md.

The design and its reasoning live in docs/websocket.md.
"""

import logging
import uuid

from fastapi import APIRouter, WebSocket

from api.deps import ChatRepositoryDep, RegistryDep, ResponderDep, SettingsDep
from core.chat_handler import ConnectionHandler
from core.config import Settings
from core.frames import PROTOCOL_VERSION, Error, ErrorCode
from core.ws import WS_POLICY_VIOLATION, WS_TRY_AGAIN_LATER, Connection

log = logging.getLogger(__name__)
router = APIRouter(tags=["chat"])


class _HandshakeRefused(Exception):
    """This handshake may not proceed. The message is the client-visible reason.

    Raised rather than returned so the vetting helper has one return type — the
    session — and the route reads as the straight line it is on every connection
    that succeeds.
    """


@router.websocket("/ws/chat/{session_id}")
async def chat_socket(
    websocket: WebSocket,
    session_id: str,
    settings: SettingsDep,
    registry: RegistryDep,
    responder: ResponderDep,
    repository: ChatRepositoryDep,
) -> None:
    try:
        session = _vet_handshake(websocket, session_id, settings)
    except _HandshakeRefused as refusal:
        # Refused *before* accept(): the ASGI server turns a close-before-accept
        # into an HTTP 403, so the client's connect fails outright. Accepting and
        # then closing instead looks to the client like a working connection that
        # dropped — which reconnect logic retries, forever, at full speed.
        log.warning("refusing websocket handshake (session=%s): %s", session_id, refusal)
        await websocket.close(code=WS_POLICY_VIOLATION, reason=str(refusal))
        return

    await websocket.accept()
    conn = Connection(websocket, session, send_queue_size=settings.ws_send_queue_size)

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
        await ConnectionHandler(conn, repository, responder, settings).serve()
    finally:
        registry.remove(conn)
        conn.finished.set()


def _vet_handshake(websocket: WebSocket, session_id: str, settings: Settings) -> uuid.UUID:
    """The session this handshake is for. Raises _HandshakeRefused if it may not.

    The parse is the return value rather than a discarded validation step: the
    id reaches this process as text exactly once, and everything downstream —
    the registry key, the repository — wants the UUID.
    """
    origin = websocket.headers.get("origin")
    # A missing Origin means a non-browser client (curl, a test, a service).
    # Browsers always send one on a WebSocket handshake, and they are the only
    # clients that carry ambient credentials — so the check that matters is on
    # the value when present, not on its presence.
    if origin is not None and not _origin_allowed(origin, settings):
        raise _HandshakeRefused("origin not allowed")

    requested = websocket.query_params.get("v")
    if requested is not None:
        # Negotiated at the handshake, not per frame: a client that cannot be
        # understood should fail to connect, before any session state exists to
        # reconcile.
        # Only the parse is guarded: a refusal raised inside the try would be
        # caught and remapped the day _HandshakeRefused gains a ValueError base.
        try:
            version = int(requested)
        except ValueError:
            raise _HandshakeRefused("malformed protocol version") from None
        if version > PROTOCOL_VERSION:
            raise _HandshakeRefused("unsupported protocol version")

    try:
        session = uuid.UUID(session_id)
    except ValueError:
        raise _HandshakeRefused("session_id must be a uuid") from None

    # Canonical form only. uuid.UUID() also accepts braces, a urn: prefix,
    # uppercase, and the undashed hex — all distinct strings that name the same
    # session. Nothing downstream depends on the spelling any more: the registry
    # keys on the parsed UUID, so every variant lands in the same fan-out
    # bucket. This is a policy check rather than a correctness one — server-minted
    # ids are always canonical, so anything else is a bug or a probe, and better
    # surfaced here than accepted silently.
    if str(session) != session_id:
        raise _HandshakeRefused("session_id must be a canonical uuid")

    return session


def _origin_allowed(origin: str, settings: Settings) -> bool:
    if settings.ws_allowed_origins:
        return origin in settings.ws_allowed_origins
    # Fails closed everywhere but local: an unset allowlist in production must
    # not silently mean "any origin".
    return settings.env == "local"
