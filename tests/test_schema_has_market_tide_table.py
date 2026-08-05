"""Regression: setup/database/schema.sql must create the market_tide_snapshots
table the API reads from and the refresh/backfill write to.

``/api/flow/market-tide`` (a pure read via
``DatabaseManager._get_latest_market_tide_snapshot``) and the writers
(``src.tools.market_tide_refresh`` / ``src.tools.market_tide_backfill`` ->
``DatabaseManager.write_market_tide_snapshots`` ->
``_upsert_market_tide_snapshot``) all target ``market_tide_snapshots``. If the
table were missing from schema.sql, every request would fall back to the live
compute and read/write paths would raise ``UndefinedTableError`` on a DB
provisioned from the file.

These checks pin the contract so code can never reference a column or conflict
target the schema does not create:

* the ``CREATE TABLE IF NOT EXISTS`` statement is present;
* every column the writer inserts / the endpoint selects is declared;
* the PRIMARY KEY matches the ``ON CONFLICT (window_minutes, snapshot_ts)``
  target the upsert relies on (a mismatch would make the INSERT raise instead
  of updating in place).

Pure/hermetic: parses the SQL text, no database required.
"""

from __future__ import annotations

import re
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "setup" / "database" / "schema.sql"

# Columns _upsert_market_tide_snapshot INSERTs + get_market_tide reads
# (payload) + created_at/updated_at (rely on the column DEFAULT).
_SNAPSHOT_COLUMNS = {
    "snapshot_ts",
    "window_minutes",
    "session_date",
    "score",
    "label",
    "participation_pct",
    "source_ts",
    "payload",
    "created_at",
    "updated_at",
}


def _table_body(sql: str, table: str) -> str:
    """Return the parenthesized body of ``CREATE TABLE IF NOT EXISTS <table>``."""
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS\s+" + re.escape(table) + r"\s*\((.*?)\n\);",
        sql,
        re.S | re.I,
    )
    assert m, f"schema.sql is missing CREATE TABLE for {table!r}"
    return m.group(1)


def test_schema_defines_market_tide_snapshots_with_required_columns():
    sql = SCHEMA_PATH.read_text()

    body = _table_body(sql, "market_tide_snapshots")
    for col in _SNAPSHOT_COLUMNS:
        assert re.search(rf"^\s*{col}\b", body, re.M), (
            f"market_tide_snapshots missing column {col!r} that "
            f"src/api/database.py reads or writes"
        )


def test_market_tide_primary_key_matches_on_conflict_target():
    """The upsert uses ON CONFLICT (window_minutes, snapshot_ts); that tuple
    must be the primary key or the INSERT raises instead of updating."""
    sql = SCHEMA_PATH.read_text()

    body = _table_body(sql, "market_tide_snapshots")
    m = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", body, re.I)
    assert m, "market_tide_snapshots has no PRIMARY KEY"
    pk = [c.strip() for c in m.group(1).split(",")]
    assert pk == ["window_minutes", "snapshot_ts"], (
        f"market_tide_snapshots PK {pk} must match the upsert's "
        f"ON CONFLICT (window_minutes, snapshot_ts)"
    )
