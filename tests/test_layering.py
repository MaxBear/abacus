"""The dependency rule, enforced rather than remembered.

`core/` holds domain: the frame protocol, the handler that drives it, and the
Protocols that `adapters/` implements. It must stay importable without a
database, a broker, or an ASGI app — that is what lets the whole suite run in a
second with no containers, and what makes a fake repository a legitimate stand-in
rather than a hopeful one.

Directory layout used to say this on its own. Once `adapters/` grew
subpackages the rule became easier to break by accident, so it is a test.
"""

import ast
from pathlib import Path

import pytest

CORE = Path(__file__).resolve().parent.parent / "core"

# starlette is deliberately absent: core/ws.py wraps `starlette.websockets` on
# purpose, because something has to own the transport primitive and that module
# is where "moves bytes, knows nothing about their meaning" lives.
#
# fastapi *is* banned, and that is the existing rule in core/chat_handler.py's
# docstring: "Deliberately free of FastAPI — it takes a Connection, a Responder,
# and Settings, so the whole protocol can be exercised without a route or an
# ASGI server."
BANNED = {
    "sqlalchemy",
    "alembic",
    "aio_pika",
    "aiobotocore",
    "botocore",
    "fastapi",
    "adapters",
    "api",
}


def _imported_roots(source: str) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return roots


@pytest.mark.parametrize("module", sorted(CORE.glob("*.py")), ids=lambda p: p.name)
def test_core_imports_no_infrastructure(module: Path):
    offending = _imported_roots(module.read_text()) & BANNED
    assert not offending, (
        f"core/{module.name} imports {sorted(offending)}. The dependency runs "
        f"adapters → core and never back; putting infrastructure here means the "
        f"handler can no longer be tested without it."
    )
