"""Everything that talks to Postgres: the engine, the schema, the repositories.

One directory per external system, so each is a thing that can be swapped or
read whole. The Protocols these implement stay in `core/` — the dependency runs
adapters → core and never back, which is what keeps `core/` importable without
a database.
"""
