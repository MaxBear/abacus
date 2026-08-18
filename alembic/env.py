"""Alembic's entry point, wired to this project's settings rather than to the ini.

The database URL is read from `Settings`, never from `alembic.ini`. Credentials
in a committed ini file is the failure this avoids; it also means migrations and
the app cannot disagree about which database they are pointed at, since both
resolve `DATABASE_URL` the same way.

`adapters/db.py` anticipates this: its `Database` is constructed from settings by
whoever needs it, "so a worker or an Alembic env can build its own instance
without the API being involved". This env builds its own engine for the same
reason — importing `api.main` here would drag the whole ASGI app, its lifespan,
and the broker into a migration run.
"""

import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from adapters.tables import metadata
from alembic import context
from core.config import get_settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set at runtime, not in alembic.ini — see the module docstring.
config.set_main_option("sqlalchemy.url", get_settings().database_url)

target_metadata = metadata


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of running it — `alembic upgrade --sql`.

    Kept because it is how a migration gets reviewed before it touches a
    database that matters, and how one is handed to a DBA who will not run
    Python against production.
    """
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        # Without this a column changing from, say, text to varchar(64) is
        # invisible to autogenerate: only added and dropped columns are noticed.
        compare_type=True,
        # Same, for a server_default appearing or changing.
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        # NullPool: a migration run is one connection, once. A pool would hold
        # the process open after the work is done.
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
