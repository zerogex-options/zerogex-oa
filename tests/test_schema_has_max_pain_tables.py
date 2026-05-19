"""Regression: setup/database/schema.sql must create the max-pain snapshot
tables that the API actually reads from and writes to.

``/api/max-pain/current`` (a pure read) and the scheduled refresh
(``src.tools.max_pain_refresh`` -> ``DatabaseManager.refresh_max_pain_snapshots``
-> ``_refresh_max_pain_snapshot``) both target ``max_pain_oi_snapshot`` and
``max_pain_oi_snapshot_expiration``.  Those tables were never added to
schema.sql, so on any DB provisioned from it every request raised
``UndefinedTableError`` -> the endpoint logged a continual stream of
``GET /api/max-pain/current failed`` 500s and the refresh logged
"non-fatal, will retry" forever.

These checks pin the contract so code can never again reference a max-pain
table the schema does not create:

* both ``CREATE TABLE IF NOT EXISTS`` statements are present;
* every column the writer inserts / the endpoint selects is declared;
* each PRIMARY KEY matches the ``ON CONFLICT`` target the upsert relies on
  (a mismatch would make the upsert raise instead of updating in place).

Pure/hermetic: parses the SQL text, no database required.
"""

from __future__ import annotations

import re
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "setup" / "database" / "schema.sql"

# Columns the production table has carried since 2026-03-04 and that
# src/api/database.py reads/writes.  max_pain_oi_snapshot: the
# _refresh_max_pain_snapshot INSERT column list + the get_max_pain_current
# SELECT list + created_at/updated_at (never in the INSERT column list, so
# both depend on the column DEFAULT).  *_expiration: the sync-expirations
# INSERT column list + the endpoint's expiration SELECT + the timestamps.
_SNAPSHOT_COLUMNS = {
    "symbol",
    "as_of_date",
    "source_timestamp",
    "underlying_price",
    "max_pain",
    "difference",
    "expirations",
    "created_at",
    "updated_at",
}
_EXPIRATION_COLUMNS = {
    "symbol",
    "as_of_date",
    "source_timestamp",
    "expiration",
    "max_pain",
    "difference_from_underlying",
    "strikes",
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


def test_schema_defines_max_pain_snapshot_tables_with_required_columns():
    sql = SCHEMA_PATH.read_text()

    snap = _table_body(sql, "max_pain_oi_snapshot")
    for col in _SNAPSHOT_COLUMNS:
        assert re.search(rf"^\s*{col}\b", snap, re.M), (
            f"max_pain_oi_snapshot missing column {col!r} that "
            f"src/api/database.py reads or writes"
        )

    exp = _table_body(sql, "max_pain_oi_snapshot_expiration")
    for col in _EXPIRATION_COLUMNS:
        assert re.search(rf"^\s*{col}\b", exp, re.M), (
            f"max_pain_oi_snapshot_expiration missing column {col!r} that "
            f"src/api/database.py reads or writes"
        )


def test_primary_keys_match_on_conflict_targets():
    """The upserts use ON CONFLICT (symbol, as_of_date) and
    ON CONFLICT (symbol, as_of_date, expiration); those tuples must be a
    unique/primary key or the INSERT raises instead of updating in place."""
    sql = SCHEMA_PATH.read_text()

    snap = _table_body(sql, "max_pain_oi_snapshot")
    m = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", snap, re.I)
    assert m, "max_pain_oi_snapshot has no PRIMARY KEY"
    pk = [c.strip() for c in m.group(1).split(",")]
    assert pk == ["symbol", "as_of_date"], (
        f"max_pain_oi_snapshot PK {pk} must match the upsert's " f"ON CONFLICT (symbol, as_of_date)"
    )

    exp = _table_body(sql, "max_pain_oi_snapshot_expiration")
    m = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", exp, re.I)
    assert m, "max_pain_oi_snapshot_expiration has no PRIMARY KEY"
    pk = [c.strip() for c in m.group(1).split(",")]
    assert pk == ["symbol", "as_of_date", "expiration"], (
        f"max_pain_oi_snapshot_expiration PK {pk} must match the upsert's "
        f"ON CONFLICT (symbol, as_of_date, expiration)"
    )
