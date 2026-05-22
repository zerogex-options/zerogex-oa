"""Tests for the incremental flow_series_5min upsert.

``SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2`` refreshes only the prev + curr
5-min bars instead of recomputing the whole session through the 8-CTE
canonical pipeline. The math is algebraically equivalent (closed bars
are window-invariant), but two structural properties must hold for the
refactor to be safe:

  1. The incremental SQL must always upsert into the same column set as
     the full SNAPSHOT_UPSERT_PSYCOPG2, with the same IS DISTINCT FROM
     guard, against the same (symbol, bar_start) primary key.
  2. The engine's dispatch logic must pick the incremental path during
     steady-state cycles AND fall back to the full backfill on
     cold-start / gap-fill cycles (or the cumulative invariant breaks --
     the incremental form requires prev_bar to already exist).

Value-level SQL equivalence (the incremental rows matching what the
canonical CTE would have produced for the same session) is only
verifiable against a real Postgres and belongs in
``tests/test_flow_series_parity.py`` (integration-marked).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine
from src.flow_series_sql import (
    FLOW_SERIES_COLUMNS,
    SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2,
    SNAPSHOT_UPSERT_PSYCOPG2,
)

ET = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# SQL structural properties
# ---------------------------------------------------------------------------


def test_incremental_sql_inserts_full_column_set():
    """Same target columns as the full form -- otherwise mixing the two
    forms (cold-start then steady-state) would leave incomplete rows."""
    for col in FLOW_SERIES_COLUMNS:
        assert (
            col in SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
        ), f"incremental SQL is missing column {col!r}"
    # symbol is the only column not in FLOW_SERIES_COLUMNS but still
    # required (PK component).
    assert "symbol" in SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2


def test_incremental_sql_uses_same_conflict_and_distinct_guard_as_full():
    """The IS DISTINCT FROM no-op suppression must apply identically so
    the write semantics don't diverge between the two forms."""
    assert "ON CONFLICT (symbol, bar_start) DO UPDATE SET" in (SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2)
    assert "IS DISTINCT FROM" in SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
    # Every non-PK column appears in the distinct guard.
    for col in FLOW_SERIES_COLUMNS:
        if col == "bar_start":
            continue
        assert (
            f"flow_series_5min.{col}" in SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
        ), f"DISTINCT guard missing for column {col!r}"


def test_incremental_sql_parameter_set_is_minimal():
    """Only %(symbol)s, %(prev_bar)s, %(curr_bar)s -- no session window
    threading, no filter args. Locks down the calling contract."""
    for name in ("symbol", "prev_bar", "curr_bar"):
        assert f"%({name})s" in SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
    for name in ("session_start", "session_end", "strikes", "expirations"):
        assert (
            f"%({name})s" not in SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
        ), f"incremental SQL unexpectedly references the {name!r} parameter"


def test_incremental_sql_aggregates_flow_by_contract_directly():
    """The whole point of the refactor: read flow_by_contract directly
    instead of running the canonical 8-CTE LAG-and-recum pipeline."""
    sql = SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
    assert "FROM flow_by_contract" in sql or "flow_by_contract fbc" in sql
    # The big CTE machinery from the canonical form must NOT appear here.
    for marker in (
        "WITH filtered AS",
        "contract_deltas AS",
        "ROWS UNBOUNDED PRECEDING",
        "LAG(",
        "generate_series(",
    ):
        assert (
            marker not in sql
        ), f"incremental SQL unexpectedly contains canonical CTE marker {marker!r}"


# ---------------------------------------------------------------------------
# Column-existence guards
# ---------------------------------------------------------------------------
#
# Background: shipping SQL that references columns that don't exist on
# the referenced table is a class of bug that static-marker tests don't
# catch and that requires a real Postgres to detect at runtime.  After
# the May-21 incident (``COUNT(fbc.option_symbol)`` -- flow_by_contract
# has no option_symbol column -- crashed every analytics cycle), we
# parse the canonical schema and assert that every ``fbc.<col>``
# reference resolves to a real flow_by_contract column.  Cheap, static,
# no DB needed, and would have caught the original bug.


def _columns_of(table: str) -> set[str]:
    """Return the column names declared on ``table`` by parsing
    setup/database/schema.sql.  Conservative -- only reads the
    initial CREATE TABLE block; the ALTER TABLE ADD COLUMN
    follow-ups in the same file are also picked up for completeness."""
    import re
    from pathlib import Path

    schema = (
        Path(__file__).resolve().parent.parent / "setup" / "database" / "schema.sql"
    ).read_text()
    cols: set[str] = set()
    # CREATE TABLE [IF NOT EXISTS] <table> ( ... );
    m = re.search(
        rf"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?{re.escape(table)}\s*\(([^;]+?)\)\s*;",
        schema,
        re.DOTALL | re.IGNORECASE,
    )
    if m:
        body = m.group(1)
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            # Skip table-level constraints (PRIMARY KEY, FOREIGN KEY, CHECK, ...)
            # and empty/comment lines.
            if not line or line.startswith("--"):
                continue
            up = line.upper()
            if up.startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "CONSTRAINT")):
                continue
            # Column definition: first token is the column name.
            name = line.split()[0].strip('"')
            if name.isidentifier():
                cols.add(name)
    # ALTER TABLE <table> ADD COLUMN [IF NOT EXISTS] <name> ...
    for am in re.finditer(
        rf"ALTER TABLE\s+{re.escape(table)}\s+ADD COLUMN\s+(?:IF NOT EXISTS\s+)?(\w+)",
        schema,
        re.IGNORECASE,
    ):
        cols.add(am.group(1))
    return cols


def test_incremental_sql_references_only_real_flow_by_contract_columns():
    """Every ``fbc.<col>`` reference must resolve to a real
    flow_by_contract column.  Regression guard for the May-21
    ``COUNT(fbc.option_symbol)`` runtime crash -- option_symbol is on
    option_chains, NOT on flow_by_contract."""
    import re

    sql = SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
    fbc_cols = _columns_of("flow_by_contract")
    assert fbc_cols, "could not parse flow_by_contract columns from schema.sql"

    referenced = set(re.findall(r"\bfbc\.(\w+)", sql))
    bogus = referenced - fbc_cols
    assert not bogus, (
        f"SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2 references columns that do "
        f"not exist on flow_by_contract: {sorted(bogus)}. "
        f"Real columns are: {sorted(fbc_cols)}"
    )


def test_incremental_sql_references_only_real_underlying_quotes_columns():
    """Same guard for the underlying_quotes table (used for the
    underlying_price subquery)."""
    import re

    sql = SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
    uq_cols = _columns_of("underlying_quotes")
    assert uq_cols, "could not parse underlying_quotes columns from schema.sql"

    referenced = set(re.findall(r"\buq\.(\w+)", sql))
    bogus = referenced - uq_cols
    assert not bogus, (
        f"SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2 references columns that do "
        f"not exist on underlying_quotes: {sorted(bogus)}. "
        f"Real columns are: {sorted(uq_cols)}"
    )


# ---------------------------------------------------------------------------
# Engine dispatch
# ---------------------------------------------------------------------------


def _fixed_ts():
    """A timestamp comfortably inside a regular trading session."""
    # 14:32 UTC = 10:32 ET = mid-morning, ~1h into the session.
    return datetime(2026, 5, 21, 14, 32, tzinfo=timezone.utc)


def _engine() -> AnalyticsEngine:
    eng = AnalyticsEngine(underlying="SPY")
    # Force the flow-cache refresh on so _refresh_flow_series_snapshot
    # doesn't short-circuit on the disabled-flag path.
    eng._analytics_flow_cache_refresh_enabled = True
    return eng


def _mock_conn_with_prev_bar(prev_bar_exists: bool):
    """Mock db_connection() with the probe row returning either a hit
    (prev_bar row already in flow_series_5min, steady-state) or a miss
    (cold-start / gap, must run the full backfill)."""
    cursor = MagicMock()
    cursor.rowcount = 2 if prev_bar_exists else 80
    cursor.fetchone.return_value = (1,) if prev_bar_exists else None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, conn, cursor


def _executed_sqls(cursor):
    return [c[0][0] for c in cursor.execute.call_args_list]


def test_steady_state_cycle_uses_incremental_upsert():
    """Steady-state: prev_bar's row already exists -> incremental form."""
    engine = _engine()
    cm, _, cursor = _mock_conn_with_prev_bar(prev_bar_exists=True)
    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_flow_series_snapshot(_fixed_ts())

    sqls = _executed_sqls(cursor)
    # First execute is the probe; second is the upsert.
    assert any(
        "SELECT 1 FROM flow_series_5min" in s for s in sqls
    ), "expected the prev_bar probe SELECT before dispatch"
    upserts = [s for s in sqls if "INSERT INTO flow_series_5min" in s]
    assert len(upserts) == 1
    # Incremental form: references prev_bar/curr_bar parameters, NOT the
    # canonical CTE markers.
    upsert_sql = upserts[0]
    assert "%(prev_bar)s" in upsert_sql
    assert "%(curr_bar)s" in upsert_sql
    assert (
        "ROWS UNBOUNDED PRECEDING" not in upsert_sql
    ), "steady-state cycle must NOT execute the canonical CTE"


def test_cold_start_cycle_uses_full_upsert():
    """Cold-start: no prev_bar row in flow_series_5min -> full backfill,
    so all bars between session_open and curr_bar get seeded."""
    engine = _engine()
    cm, _, cursor = _mock_conn_with_prev_bar(prev_bar_exists=False)
    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_flow_series_snapshot(_fixed_ts())

    sqls = _executed_sqls(cursor)
    upserts = [s for s in sqls if "INSERT INTO flow_series_5min" in s]
    assert len(upserts) == 1
    upsert_sql = upserts[0]
    # Full form: references session_start/session_end, contains the CTE.
    assert "%(session_start)s" in upsert_sql
    assert "%(session_end)s" in upsert_sql
    assert "ROWS UNBOUNDED PRECEDING" in upsert_sql, (
        "cold-start cycle must execute the full canonical CTE so closed "
        "bars between session_open and prev_bar are seeded"
    )


def test_refresh_short_circuits_when_flow_cache_disabled(monkeypatch):
    """The cache-disabled flag still short-circuits the whole path."""
    engine = _engine()
    engine._analytics_flow_cache_refresh_enabled = False

    cm, _, cursor = _mock_conn_with_prev_bar(prev_bar_exists=True)
    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_flow_series_snapshot(_fixed_ts())
    assert cursor.execute.call_count == 0, "disabled flag must skip DB activity entirely"


def test_first_bar_of_session_falls_back_to_full_backfill():
    """If curr_bar == session_start (we're at 09:30 ET sharp), prev_bar
    clamps to session_start and there's no earlier bar for the
    incremental form to ride on -- use the full backfill regardless of
    what the probe returns.

    The bar boundaries inside the engine come from ``datetime.now(utc)``,
    not the ``timestamp`` argument (which only supplies the ET date), so
    triggering this edge case requires patching ``datetime.now``.
    """
    engine = _engine()
    open_ts = datetime(2026, 5, 21, 13, 30, tzinfo=timezone.utc)  # 09:30 ET

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return open_ts.astimezone(tz) if tz is not None else open_ts.replace(tzinfo=None)

    cm, _, cursor = _mock_conn_with_prev_bar(prev_bar_exists=True)
    with (
        patch.object(main_engine, "db_connection", return_value=cm),
        patch.object(main_engine, "datetime", _FrozenDateTime),
    ):
        engine._refresh_flow_series_snapshot(open_ts)

    sqls = _executed_sqls(cursor)
    upserts = [s for s in sqls if "INSERT INTO flow_series_5min" in s]
    assert len(upserts) == 1
    assert (
        "%(session_start)s" in upserts[0]
    ), "the open-of-session edge case must take the full-backfill path"
