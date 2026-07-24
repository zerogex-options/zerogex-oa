"""Tests for the flow_series_5min snapshot path.

Two layers, mirroring the existing flow-series tests:

  * **Single-source / no-drift** — the canonical CTE is stored once and
    rendered to two driver dialects (asyncpg ``$1..$5`` for the API,
    psycopg2 ``%(name)s`` for the engine + backfill). These tests assert
    the two rendered forms are equivalent modulo the placeholder dialect.
    That is the structural guarantee that the snapshot write and the live
    read can never silently diverge.

  * **Read-path plumbing parity** — drive ``get_flow_series`` with canned
    rows through both the CTE path and the snapshot path and assert the
    returned dicts, ordering, ``intervals`` slicing, caching and
    404/empty semantics are byte-identical. Value-level SQL equivalence
    (the snapshot rows actually matching the CTE's arithmetic) is only
    provable against a real Postgres and lives in
    ``tests/test_flow_series_parity.py`` (integration-marked, skipped in
    CI) — it is the gating step of the phase-1 → phase-2 migration.
"""

from __future__ import annotations

import asyncio
import re
from datetime import date

from src.api.database import _expected_flow_series_bars
from src.flow_series_sql import (
    FLOW_SERIES_COLUMNS,
    FLOW_SERIES_CTE_ASYNCPG,
    FLOW_SERIES_CTE_PSYCOPG2,
    SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2,
    SNAPSHOT_SELECT_ASYNCPG,
    SNAPSHOT_UPSERT_PSYCOPG2,
)

# Reuse the canned-connection harness from the existing suite.
from tests.test_api_flow_series import (
    _CannedConn,
    _make_db,
    _mock_session_resolution_rows,
    _bar_ts,
)

_INNER_CTES = (
    "WITH filtered AS (",
    "contract_deltas AS (",
    "per_bar AS (",
    "underlying_by_bar AS (",
    "timeline AS (",
    "joined AS (",
    "carry AS (",
)

_PARAM_DIALECT = (
    ("$1", "%(symbol)s"),
    ("$2", "%(session_start)s"),
    ("$3", "%(session_end)s"),
    ("$4", "%(strikes)s"),
    ("$5", "%(expirations)s"),
)


# ---------------------------------------------------------------------------
# Single source of truth / no-drift
# ---------------------------------------------------------------------------


def test_cte_renders_to_both_dialects_with_no_leftover_tokens():
    for sql in (FLOW_SERIES_CTE_ASYNCPG, FLOW_SERIES_CTE_PSYCOPG2):
        for name in ("symbol", "session_start", "session_end", "strikes", "expirations"):
            assert f":{name}" not in sql, f"unrendered :{name} token in {sql[:80]!r}"
    for tok in ("$1", "$2", "$3", "$4", "$5"):
        assert tok in FLOW_SERIES_CTE_ASYNCPG
    for _, named in _PARAM_DIALECT:
        assert named in FLOW_SERIES_CTE_PSYCOPG2


def test_asyncpg_and_psycopg2_cte_are_equivalent_modulo_dialect():
    """The anti-drift guarantee: both driver forms come from the same
    template, so mapping the positional placeholders onto the named ones
    must reproduce the psycopg2 form exactly."""
    mapped = FLOW_SERIES_CTE_ASYNCPG
    for positional, named in _PARAM_DIALECT:
        mapped = mapped.replace(positional, named)
    assert mapped == FLOW_SERIES_CTE_PSYCOPG2


def test_cte_pipeline_structure_present_in_both_forms():
    for sql in (FLOW_SERIES_CTE_ASYNCPG, FLOW_SERIES_CTE_PSYCOPG2):
        for cte in _INNER_CTES:
            assert cte in sql
        assert "WINDOW w_cum AS (ORDER BY bar_start ROWS UNBOUNDED PRECEDING)" in sql
        assert "ORDER BY bar_start DESC" in sql


def test_snapshot_select_projects_canonical_columns_without_symbol():
    sql = SNAPSHOT_SELECT_ASYNCPG
    assert "FROM flow_series_5min" in sql
    assert "WHERE symbol = $1" in sql
    assert "bar_start >= $2" in sql and "bar_start <= $3" in sql
    assert sql.strip().endswith("ORDER BY bar_start DESC")
    select_clause = sql[sql.index("SELECT") + len("SELECT") : sql.index("FROM flow_series_5min")]
    for col in FLOW_SERIES_COLUMNS:
        assert col in select_clause
    # symbol is a PK component but is never returned to the API, so the
    # dict(row) keys match the CTE path exactly.
    assert "symbol" not in select_clause


def test_snapshot_upsert_wraps_canonical_cte_and_is_idempotent():
    sql = SNAPSHOT_UPSERT_PSYCOPG2
    assert "INSERT INTO flow_series_5min (" in sql
    assert FLOW_SERIES_CTE_PSYCOPG2 in sql  # runs the exact canonical query
    assert "ON CONFLICT (symbol, bar_start) DO UPDATE SET" in sql
    # IS DISTINCT FROM guard suppresses no-op rewrites of closed bars.
    assert "IS DISTINCT FROM flow_series_5min.call_premium_cum" in sql
    for col in FLOW_SERIES_COLUMNS:
        if col == "bar_start":
            continue
        assert f"{col} = EXCLUDED.{col}" in sql


def test_expected_flow_series_bars_inclusive_and_clamped():
    from datetime import datetime, timedelta, timezone

    start = datetime(2026, 4, 24, 13, 30, tzinfo=timezone.utc)
    # A full RTH session is 09:30..16:15 = 6h45m = 81 steps + 1 = 82 bars.
    assert _expected_flow_series_bars(start, start + timedelta(hours=6, minutes=45)) == 82
    assert _expected_flow_series_bars(start, start) == 1
    assert _expected_flow_series_bars(start, start - timedelta(minutes=5)) == 0


# ---------------------------------------------------------------------------
# Read-path plumbing parity
# ---------------------------------------------------------------------------


def _snapshot_rows(n: int):
    """n canned snapshot rows, newest-first (as the SELECT returns)."""
    return [
        {
            "bar_start": _bar_ts(5 * (n - 1 - i)),
            "call_premium_cum": 1000.0 + i,
            "put_premium_cum": 900.0 + i,
            "call_volume_cum": 10 + i,
            "put_volume_cum": 8 + i,
            "net_volume_cum": 2,
            "raw_volume_cum": 18 + i,
            "call_position_cum": 5,
            "put_position_cum": 4,
            "net_premium_cum": 1900.0 + 2 * i,
            "put_call_ratio": 0.8,
            "underlying_price": 710.0 + i,
            "contract_count": 2,
            "is_synthetic": False,
        }
        for i in range(n)
    ]


def test_flag_off_unfiltered_uses_cte():
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=[])
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = False

    asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    query, args = conn.fetch_calls[0]
    assert query == FLOW_SERIES_CTE_ASYNCPG
    assert len(args) == 5  # symbol, start, end, strikes, expirations


def test_flag_on_but_filtered_still_uses_cte():
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=[])
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    asyncio.run(db.get_flow_series(symbol="SPY", session="current", strikes=[700.0]))

    query, args = conn.fetch_calls[0]
    assert query == FLOW_SERIES_CTE_ASYNCPG
    assert args[3] == [700.0]  # strike filter forwarded to the CTE path


def test_flag_on_unfiltered_reads_snapshot():
    rows = _snapshot_rows(3)
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=rows)
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    result = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    query, args = conn.fetch_calls[0]
    assert query == SNAPSHOT_SELECT_ASYNCPG
    assert len(args) == 3  # symbol, session_start, session_end — no filters
    assert args[0] == "SPY"
    # Rows pass straight through, newest-first preserved.
    assert [r["bar_start"] for r in result] == [r["bar_start"] for r in rows]
    assert result[0]["call_premium_cum"] == rows[0]["call_premium_cum"]


def test_snapshot_and_cte_paths_are_byte_identical_shape():
    """Same canned rows through both paths -> identical Python result.
    Proves the dict shaping / ordering / slicing is path-independent."""
    rows = _snapshot_rows(4)

    cte_conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=rows)
    cte_db = _make_db(cte_conn)
    cte_db._flow_endpoint_cache_ttl_seconds = 0.0
    cte_db._flow_series_use_snapshot = False
    cte_result = asyncio.run(cte_db.get_flow_series(symbol="SPY", session="current"))

    snap_conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=rows)
    snap_db = _make_db(snap_conn)
    snap_db._flow_endpoint_cache_ttl_seconds = 0.0
    snap_db._flow_series_use_snapshot = True
    snap_result = asyncio.run(snap_db.get_flow_series(symbol="SPY", session="current"))

    assert snap_result == cte_result


def test_snapshot_path_intervals_slicing_matches_cte():
    rows = _snapshot_rows(6)
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=rows)
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    result = asyncio.run(db.get_flow_series(symbol="SPY", session="current", intervals=2))

    assert len(result) == 2  # leading N (most-recent N) bars
    assert result == [dict(r) for r in rows[:2]]


def test_snapshot_path_empty_returns_empty_list_not_none():
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=[])
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    result = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))
    assert result == []


def test_snapshot_path_unknown_symbol_returns_none_before_switch():
    conn = _CannedConn(fetchval_sequence=[None])  # EXISTS -> None
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    result = asyncio.run(db.get_flow_series(symbol="ZZZZZ", session="current"))
    assert result is None
    assert conn.fetch_calls == []  # never reached the snapshot SELECT


def test_snapshot_path_prior_with_no_prior_data_returns_empty():
    # EXISTS -> 1, current_date -> a date, prior_date -> None.
    conn = _CannedConn(fetchval_sequence=[1, date(2026, 4, 24), None])
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    result = asyncio.run(db.get_flow_series(symbol="SPY", session="prior"))
    assert result == []
    assert conn.fetch_calls == []  # has_data=False short-circuits before the SELECT


def test_snapshot_shortfall_logs_warning_but_still_serves(caplog):
    # current_date in the past => window is the full closed RTH session
    # => _expected_flow_series_bars == 82, deterministic. Returning only a
    # few rows is a shortfall: warn, but still return what we have.
    rows = _snapshot_rows(3)
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(date(2026, 4, 24)),
        fetch_rows=rows,
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    with caplog.at_level("WARNING"):
        result = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    assert len(result) == 3  # still served — no CTE fallback
    assert any("flow_series_5min shortfall" in r.message for r in caplog.records)


def test_snapshot_full_window_does_not_warn(caplog):
    rows = _snapshot_rows(82)  # exactly the expected bar count
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(date(2026, 4, 24)),
        fetch_rows=rows,
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0
    db._flow_series_use_snapshot = True

    with caplog.at_level("WARNING"):
        result = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    assert len(result) == 82
    assert not any("shortfall" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Directional-sign regression guard — the "flow mirror" bug (Jul 2026)
# ---------------------------------------------------------------------------
#
# net_premium_cum and net_volume_cum are DIRECTIONAL: bullish flow is
# positive, bearish flow is negative. Buying puts is bearish, so the put
# leg must be SUBTRACTED, never added:
#
#     net_premium_cum = call_premium_cum - put_premium_cum
#     net_volume_cum  = (calls bought-sold) - (puts bought-sold)
#
# The original code ADDED the legs, which inverted the headline whenever
# put flow dominated — it rendered as the mirror image of the true
# directional signal (and of the documented "Cumulative Net Premium"
# definition). These static guards lock the sign in every rendered form so
# the bug cannot silently return. Value-level correctness is covered by the
# integration parity harness (tests/test_flow_series_parity.py).


def _collapse_ws(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


def test_net_premium_cum_subtracts_the_put_leg_in_every_form():
    for sql in (FLOW_SERIES_CTE_ASYNCPG, FLOW_SERIES_CTE_PSYCOPG2):
        norm = _collapse_ws(sql)
        assert (
            "- SUM(put_premium_delta) OVER w_cum) AS net_premium_cum" in norm
        ), "net_premium_cum must SUBTRACT the put premium leg (directional)"
        assert (
            "+ SUM(put_premium_delta) OVER w_cum) AS net_premium_cum" not in norm
        ), "net_premium_cum must not ADD the put leg — that is the mirror bug"
    inc = _collapse_ws(SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2)
    assert "a.call_premium_cum - a.put_premium_cum) AS net_premium_cum" in inc
    assert "a.call_premium_cum + a.put_premium_cum) AS net_premium_cum" not in inc


def test_net_volume_cum_is_directional_calls_minus_puts():
    for sql in (FLOW_SERIES_CTE_ASYNCPG, FLOW_SERIES_CTE_PSYCOPG2):
        norm = _collapse_ws(sql)
        assert (
            "THEN net_volume_delta ELSE -net_volume_delta END" in norm
        ), "net_volume_cum must negate the put leg (directional net volume)"
    inc = _collapse_ws(SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2)
    assert (
        "THEN fbc.net_volume ELSE -fbc.net_volume END" in inc
    ), "incremental net_volume_cum must negate the put leg to match the CTE"
