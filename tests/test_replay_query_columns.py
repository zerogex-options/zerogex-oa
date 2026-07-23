"""Schema-contract test for the replay *at-timestamp* queries.

The endpoint tests in ``test_replay_endpoint.py`` stub the DB layer with
``AsyncMock``, so a query that selects a column ``gex_summary`` does not
have sails straight through them and only blows up in production as
``column "spot_price" does not exist``. That is exactly what shipped: the
replay ``get_gex_summary_at_ts`` / ``get_gex_by_strike_at_ts`` queries
selected ``spot_price`` from ``gex_summary``, but spot has never lived on
that table — it is carried on ``underlying_quotes``.

This test closes the gap without needing a live Postgres. It builds
throwaway SQLite tables whose columns are the *authoritative* ones parsed
from ``setup/database/schema.sql``, then prepares the real query text (read
straight out of ``src/api/database.py``) against them. SQLite resolves every
column reference at prepare time, so a phantom column raises
``OperationalError`` — the failure the mocks could never surface.

Only column *resolution* is exercised, not Postgres semantics: the tables
are empty, so no row-level expression (``AT TIME ZONE``, arithmetic, …)
ever evaluates. The queries under test use portable ANSI constructs
(CTEs, correlated sub-selects), which SQLite prepares faithfully.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCHEMA_SQL = (_ROOT / "setup" / "database" / "schema.sql").read_text()
_DATABASE_PY = (_ROOT / "src" / "api" / "database.py").read_text()

# Tables the replay at-ts queries read from.
_TABLES = ("gex_summary", "underlying_quotes", "gex_by_strike")

# First tokens that introduce a table-level constraint, not a column.
_CONSTRAINT_KEYWORDS = {"PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT", "EXCLUDE"}


def _columns_of(table: str) -> list[str]:
    """Authoritative column names for ``table`` from the Postgres DDL.

    Reads the ``CREATE TABLE IF NOT EXISTS <table> ( ... );`` body plus any
    later ``ALTER TABLE <table> ADD COLUMN IF NOT EXISTS <col>`` migrations
    (deployments that pre-date a column), skipping comment lines and
    table-level constraints.
    """
    create = re.search(
        rf"CREATE TABLE IF NOT EXISTS {re.escape(table)}\b\s*\((.*?)\n\);",
        _SCHEMA_SQL,
        re.DOTALL,
    )
    assert create, f"{table}: CREATE TABLE not found in schema.sql"

    cols: list[str] = []
    for raw in create.group(1).splitlines():
        line = raw.split("--", 1)[0].strip()  # drop inline / comment-only text
        if not line:
            continue
        token = re.match(r"([a-z_][a-z0-9_]*)", line)
        if not token or token.group(1).upper() in _CONSTRAINT_KEYWORDS:
            continue
        cols.append(token.group(1))

    for m in re.finditer(
        rf"ALTER TABLE {re.escape(table)}\b\s+ADD COLUMN IF NOT EXISTS\s+" r"([a-z_][a-z0-9_]*)",
        _SCHEMA_SQL,
    ):
        if m.group(1) not in cols:
            cols.append(m.group(1))

    assert cols, f"{table}: no columns parsed from schema.sql"
    return cols


def _extract_query(method: str) -> str:
    """Pull the ``query = \"\"\"...\"\"\"`` SQL out of ``method``'s body."""
    start = _DATABASE_PY.index(f"async def {method}(")
    m = re.search(r'query = """(.*?)"""', _DATABASE_PY[start:], re.DOTALL)
    assert m, f"{method}: no query string found in src/api/database.py"
    return m.group(1)


@pytest.fixture()
def schema_db():
    """In-memory SQLite mirror of the real tables (column names only)."""
    conn = sqlite3.connect(":memory:")
    try:
        for table in _TABLES:
            col_defs = ", ".join(f'"{c}"' for c in _columns_of(table))
            conn.execute(f"CREATE TABLE {table} ({col_defs})")
        yield conn
    finally:
        conn.close()


def _prepare(conn: sqlite3.Connection, sql: str) -> None:
    """Run the query against the empty schema mirror.

    asyncpg ``$n`` placeholders are rewritten to SQLite ``?``. With empty
    tables the query returns zero rows, so only column *resolution* runs —
    an unknown column raises ``OperationalError`` at prepare time.
    """
    translated = re.sub(r"\$\d+", "?", sql)
    conn.execute(translated, [1] * translated.count("?")).fetchall()


@pytest.mark.parametrize("method", ["get_gex_summary_at_ts", "get_gex_by_strike_at_ts"])
def test_replay_at_ts_query_columns_exist(schema_db, method):
    """Every column the replay at-ts queries name must exist on the tables.

    Regression guard for the ``spot_price`` bug: before the fix this raised
    ``no such column: spot_price`` for ``get_gex_summary_at_ts`` and
    ``get_gex_by_strike_at_ts`` alike.
    """
    _prepare(schema_db, _extract_query(method))


def test_schema_mirror_rejects_phantom_gex_summary_spot_price(schema_db):
    """Guard the guard: the exact original defect must still fail here.

    If ``gex_summary`` ever grows a real ``spot_price`` column (or the
    mirror drifts from the schema), this stops raising and the contract
    test above would be silently toothless.
    """
    assert "spot_price" not in _columns_of("gex_summary")
    with pytest.raises(sqlite3.OperationalError):
        schema_db.execute("SELECT spot_price FROM gex_summary").fetchall()
