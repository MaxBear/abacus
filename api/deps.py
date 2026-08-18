"""Dependency providers: the seam between the composition root and handlers.

Handlers never reach for module globals — they declare what they need and get
it from app.state, which api/main.py:lifespan populated. That keeps adapters
free of hidden process state and lets tests inject fakes instead of
monkeypatching import paths by string.
"""

from typing import Annotated

from fastapi import Depends, Request
from starlette.requests import HTTPConnection

from adapters.db import Database
from core.config import Settings
from core.repository import ChatRepository
from core.responder import Responder
from core.ws import ConnectionRegistry


# All of these are `async def` deliberately. FastAPI runs *sync* dependencies in
# a threadpool (fastapi/dependencies/utils.py: run_in_threadpool), which would
# cost a thread dispatch per request just to read an attribute. Async ones
# resolve inline on the event loop.
async def get_db(request: Request) -> Database:
    return request.app.state.db


# HTTPConnection, not Request: it is the common base of Request and WebSocket,
# and FastAPI's solver resolves it for both (dependencies/utils.py:
# add_non_field_param_to_dependency). One provider therefore serves /readyz and
# the chat socket, instead of a near-duplicate pair differing only in a type.
async def get_settings(conn: HTTPConnection) -> Settings:
    return conn.app.state.settings


async def get_registry(conn: HTTPConnection) -> ConnectionRegistry:
    return conn.app.state.chat_registry


async def get_responder(conn: HTTPConnection) -> Responder:
    return conn.app.state.responder


async def get_chat_repository(conn: HTTPConnection) -> ChatRepository:
    return conn.app.state.chat_repository


DatabaseDep = Annotated[Database, Depends(get_db)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
RegistryDep = Annotated[ConnectionRegistry, Depends(get_registry)]
ResponderDep = Annotated[Responder, Depends(get_responder)]
ChatRepositoryDep = Annotated[ChatRepository, Depends(get_chat_repository)]
