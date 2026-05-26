"""Tests for the ``option_chains_latest`` cache read path in the
analytics snapshot.

Gated on ``ANALYTICS_USE_LATEST_CACHE`` (default off).  When enabled,
``_get_snapshot`` reads latest-per-contract rows from the maintained
cache instead of running ``DISTINCT ON`` over ``option_chains`` history.
The cache is populated by ingestion's dual-UPSERT.

These tests pin down:

  * Flag default OFF: the cache query never runs; behavior unchanged.
  * Flag ON, cache populated: cache query runs; the legacy DISTINCT ON
    query does NOT run for that cycle.
  * Flag ON, cache empty: a warning is logged and the legacy DISTINCT ON
    query runs as the fallback for that cycle (so a too-early flag flip
    cannot leave analytics blind).
  * Flag ON, cache read raises: same fallback to the legacy path.
  * Cache SQL shape: reads from ``option_chains_latest``, no DISTINCT ON,
    same parameter contract as the history query.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _row(option_symbol, strike, expiration, option_type, quote_ts, *, gamma=0.01, oi=100):
    """15-tuple matching the column order in _SNAPSHOT_QUERY / _SNAPSHOT_QUERY_CACHE."""
    return (
        option_symbol,
        strike,
        expiration,
        option_type,
        1.0,  # last
        0.99,  # bid
        1.01,  # ask
        10,  # volume
        oi,
        0.5,  # delta
        gamma,
        -0.05,  # theta
        0.1,  # vega
        0.2,  # implied_volatility
        quote_ts,
    )


def _mock_db_connection(latest_ts, underlying_price, snapshot_rows):
    """Three-query mock matching the _get_snapshot call shape.

    Snapshot row(s) are returned by ``fetchall()`` -- the same call site
    serves both the cache query and the history query, so the mock is
    agnostic to which path the engine takes.
    """
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (latest_ts,),  # query 1: latest option_chains timestamp
        (underlying_price, latest_ts),  # query 2: underlying close + ts
    ]
    cursor.fetchall.return_value = snapshot_rows

    conn = MagicMock()
    conn.cursor.return_value = cursor

    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, cursor


def _execute_sqls(cursor):
    """Return all SQL strings executed on the cursor, in call order."""
    return [call.args[0] for call in cursor.execute.call_args_list]


def _snapshot_ts():
    return ET.localize(datetime(2026, 5, 26, 14, 30)).astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Flag default off: cache is invisible.
# ---------------------------------------------------------------------------

def test_default_off_flag_skips_cache_query_entirely():
    """Without the env var set, _get_snapshot runs the legacy history
    query and never touches option_chains_latest."""
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.use_latest_cache is False, (
        "default must be off so just-deploying the code is a no-op"
    )

    snapshot_ts = _snapshot_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260520C00500000", 500.0, expiration, "C", snapshot_ts)]
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()

    sqls = _execute_sqls(cursor)
    assert not any("option_chains_latest" in s for s in sqls), (
        "cache query must NOT run when flag is off"
    )
    # Legacy history query still ran.
    assert any("DISTINCT ON" in s for s in sqls)
    assert result is not None and len(result["options"]) == 1


# ---------------------------------------------------------------------------
# Flag on, cache populated: cache query runs; history query does NOT.
# ---------------------------------------------------------------------------

def test_cache_populated_skips_history_query():
    """When the cache returns rows, the legacy DISTINCT ON query
    must NOT run for that cycle -- otherwise we haven't saved any work."""
    engine = AnalyticsEngine(underlying="SPY")
    engine.use_latest_cache = True

    snapshot_ts = _snapshot_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260520C00500000", 500.0, expiration, "C", snapshot_ts)]
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()

    sqls = _execute_sqls(cursor)
    # Cache query ran.
    assert any("option_chains_latest" in s for s in sqls), (
        "cache query must run when flag is on"
    )
    # Legacy DISTINCT ON path did NOT run -- the whole point.
    assert not any("DISTINCT ON" in s for s in sqls), (
        "history DISTINCT ON must not run when cache returns rows"
    )
    assert result is not None and len(result["options"]) == 1


# ---------------------------------------------------------------------------
# Flag on, cache empty: warning + fallback to history.
# ---------------------------------------------------------------------------

def test_cache_empty_falls_back_to_history(caplog):
    """An empty cache (warm-up, stale weekend, etc.) must transparently
    fall through to the legacy DISTINCT ON path; the cycle must succeed
    with the history result."""
    engine = AnalyticsEngine(underlying="SPY")
    engine.use_latest_cache = True

    snapshot_ts = _snapshot_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    history_rows = [_row("SPY260520C00500000", 500.0, expiration, "C", snapshot_ts)]

    # First fetchall() = cache (empty); second fetchall() = history (one row).
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (snapshot_ts,),
        (500.0, snapshot_ts),
    ]
    cursor.fetchall.side_effect = [[], history_rows]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False

    with patch.object(main_engine, "db_connection", return_value=cm):
        with caplog.at_level("WARNING"):
            result = engine._get_snapshot()

    sqls = _execute_sqls(cursor)
    assert any("option_chains_latest" in s for s in sqls), "cache attempt"
    assert any("DISTINCT ON" in s for s in sqls), "fallback to history"

    # Fallback yields the history row.
    assert result is not None and len(result["options"]) == 1
    # And a clear operator-facing warning was emitted.
    assert any(
        "option_chains_latest cache returned 0 rows" in r.message for r in caplog.records
    ), "operator must see the cache miss warning"


# ---------------------------------------------------------------------------
# Flag on, cache read raises: warning + fallback to history.
# ---------------------------------------------------------------------------

def test_cache_read_error_falls_back_to_history(caplog):
    """If the cache query raises (transient connection blip, table
    locked by maintenance), the cycle falls back to the history query
    rather than failing the whole cycle."""
    engine = AnalyticsEngine(underlying="SPY")
    engine.use_latest_cache = True

    snapshot_ts = _snapshot_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    history_rows = [_row("SPY260520C00500000", 500.0, expiration, "C", snapshot_ts)]

    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (snapshot_ts,),
        (500.0, snapshot_ts),
    ]
    # First fetchall() raises (cache); second fetchall() returns history.
    cursor.fetchall.side_effect = [RuntimeError("simulated cache outage"), history_rows]

    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False

    with patch.object(main_engine, "db_connection", return_value=cm):
        with caplog.at_level("WARNING"):
            result = engine._get_snapshot()

    # Engine rolled back the failed cache transaction and ran history.
    conn.rollback.assert_called()
    assert result is not None and len(result["options"]) == 1
    assert any(
        "option_chains_latest cache read failed" in r.message for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Cache SQL shape.
# ---------------------------------------------------------------------------

def test_cache_sql_reads_from_option_chains_latest_no_distinct_on():
    """The cache query targets the cache table and does NOT use
    DISTINCT ON -- the whole point is that dedup happened at write time."""
    sql = AnalyticsEngine._SNAPSHOT_QUERY_CACHE
    assert "FROM option_chains_latest" in sql
    assert "DISTINCT ON" not in sql, "cache query must not re-dedup"
    assert "gamma IS NOT NULL" in sql, "cache query must filter unusable rows"


def test_cache_query_receives_same_param_contract_as_history():
    """Both queries take (underlying, ts, lookback_start, min_expiration,
    row_cap) -- callers can switch between them transparently."""
    engine = AnalyticsEngine(underlying="SPY")
    engine.use_latest_cache = True

    snapshot_ts = _snapshot_ts()
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    # Stub cache to a no-error empty list so we observe its params then
    # fall through to history.  Use side_effect so the second fetchall
    # (history) also returns [].
    cursor.fetchall.side_effect = [[], []]

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    # Find the cache-query execute() call.
    cache_call = None
    for call in cursor.execute.call_args_list:
        sql = call.args[0]
        if "FROM option_chains_latest" in sql:
            cache_call = call
            break
    assert cache_call is not None, "cache query should have been issued"

    params = cache_call.args[1]
    # (underlying, timestamp, lookback_start, min_expiration, row_cap)
    assert len(params) == 5
    assert params[0] == engine.db_symbol
    assert params[1] == snapshot_ts
    # lookback_start = timestamp - lookback_hours
    assert params[2] == snapshot_ts - timedelta(hours=engine.snapshot_lookback_hours)
    # min_expiration is a date (today-1 in-session before 16:15)
    assert hasattr(params[3], "year")
    # row_cap is an int
    assert isinstance(params[4], int) and params[4] > 0


# ---------------------------------------------------------------------------
# Cold-start latch interaction.
# ---------------------------------------------------------------------------

def test_first_cycle_latch_flips_even_when_cache_returns_rows():
    """``_snapshot_cold_start_consumed`` must flip on cycle 1 regardless
    of which path served the rows.  A later fallback (cache→history on
    cycle 2) then doesn't unnecessarily re-enter the cold-start scan."""
    engine = AnalyticsEngine(underlying="SPY")
    engine.use_latest_cache = True
    assert engine._snapshot_cold_start_consumed is False

    snapshot_ts = _snapshot_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260520C00500000", 500.0, expiration, "C", snapshot_ts)]
    cm, _ = _mock_db_connection(snapshot_ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    assert engine._snapshot_cold_start_consumed is True, (
        "first cycle must consume the cold-start latch on the cache path too"
    )
