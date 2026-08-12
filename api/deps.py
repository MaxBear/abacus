"""Dependency providers: the seam between the composition root and handlers.

Handlers never reach for module globals — they declare what they need and get
it from app.state, which api/main.py:lifespan populated. That keeps adapters
free of hidden process state and lets tests inject fakes instead of
monkeypatching import paths by string.
"""

from typing import Annotated

from fastapi import Depends, Request

from adapters.db import Database
from core.config import Settings


# Both are `async def` deliberately. FastAPI runs *sync* dependencies in a
# threadpool (fastapi/dependencies/utils.py: run_in_threadpool), which would
# cost a thread dispatch per request just to read an attribute. Async ones
# resolve inline on the event loop.
async def get_db(request: Request) -> Database:
    return request.app.state.db


async def get_settings(request: Request) -> Settings:
    return request.app.state.settings


DatabaseDep = Annotated[Database, Depends(get_db)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
