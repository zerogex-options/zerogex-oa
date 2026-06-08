"""Regression tests for the /api/gex/strike-profile-timeseries query.

This endpoint backs the Strike Profile chart's rewind feature: it returns
a fully aligned per-bucket timeseries of OHLC + gamma-flip / call-wall /
put-wall + per-strike gamma exposure + open interest.  The chart's rewind
collapses to direct indexing into the returned array — so the contract
this test pins is:

  * the SQL has the same right-edge anchor + bucket-rep + JOIN-on-rep_ts
    shape every historical GEX endpoint uses (no full-window scan of
    gex_by_strike — that would be tens of millions of rows on long
    windows);
  * for cash indices (SPX / NDX / RUT) the cash-session filter is applied
    to BOTH the window anchor and the bucket-rep CTE — same rationale as
    get_historical_gex / get_gex_heatmap;
  * the expiration filter is a single fixed-shape predicate
    ``$3::date IS NULL OR gbs.expiration = $3::date`` so PG plans the
    query once for both "all" and per-expiration modes;
  * OHLC is bucketed against the SAME bucket expression every other
    historical endpoint uses, so the candle and the GEX surface always
    line up on the time axis;
  * the response is ASCENDING by bucket time (most recent last) — the
    chart's rewindIndex indexes directly into this array.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import date

from src.api.database import DatabaseManager


class _RecordingConn:
    """Captures queries + args and returns canned rows."""

    def __init__(self, fetch_rows=None):
        self._fetch_rows = fetch_rows or []
        self.queries = []
        self.args = []

    async def fetch(self, query, *args, timeout=None):
        self.queries.append(query)
        self.args.append(args)
        return list(self._fetch_rows)


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def _run(symbol, timeframe="1min", window_units=78, expiration=None, rows=None):
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=rows or [])
    _install_conn(db, conn)
    result = asyncio.run(
        db.get_strike_profile_timeseries(symbol, timeframe, window_units, expiration)
    )
    return {
        "query": conn.queries[0] if conn.queries else "",
        "args": conn.args[0] if conn.args else (),
        "result": result,
    }


# ---------------------------------------------------------------------------
# Query shape: anchor + bucket-rep + rep_ts JOIN
# ---------------------------------------------------------------------------


def test_query_anchors_on_gex_summary_and_picks_bucket_representative():
    """The right edge must be anchored on MAX(gex_summary.timestamp) and
    the per-bucket representative selected with DISTINCT ON.  Same shape
    get_gex_heatmap uses — keeps the chart's freshness coupled to the
    analytics writer rather than the underlying-quotes feed."""
    captured = _run("SPY")
    sql = captured["query"]

    # Window anchor is gex_summary, not underlying_quotes.
    assert "FROM gex_summary" in sql
    latest_idx = sql.index("latest AS")
    bucket_reps_idx = sql.index("bucket_reps AS")
    assert latest_idx < bucket_reps_idx
    assert "DISTINCT ON" in sql

    # bucket_reps must come BEFORE strikes — strikes JOINs on bucket_reps.rep_ts.
    strikes_idx = sql.index("strikes AS")
    assert bucket_reps_idx < strikes_idx


def test_strikes_join_on_rep_ts_not_window_scan():
    """The core anti-regression: gex_by_strike must be JOINed at the
    per-bucket representative timestamps (g.timestamp = br.rep_ts), NOT
    range-scanned across the whole window.  A full window scan of
    gex_by_strike is the highest-cardinality read on the API and the
    timeout that previously took down the heatmap."""
    captured = _run("SPY")
    sql = captured["query"]

    assert "JOIN gex_by_strike gbs" in sql
    assert "gbs.timestamp  = br.rep_ts" in sql or "gbs.timestamp = br.rep_ts" in sql

    # gex_by_strike must NOT have a timestamp >= ... start_ts predicate —
    # that would degenerate the JOIN into a window scan.
    gbs_idx = sql.index("gex_by_strike gbs")
    tail = sql[gbs_idx:]
    # The tail can carry the bounds CTE only by name, never as a direct
    # predicate on gbs.timestamp.
    assert "gbs.timestamp BETWEEN" not in tail
    assert "gbs.timestamp >=" not in tail


# ---------------------------------------------------------------------------
# Cash-index session filter
# ---------------------------------------------------------------------------


def test_etf_has_no_session_filter():
    """ETFs / equities (SPY, QQQ, AAPL) trade extended hours legitimately.
    The query and bound params stay clean: no session predicate, four args
    only (symbol, window_units, expiration, plus the cash-index holidays
    bound only appears for cash indices)."""
    for sym in ("SPY", "QQQ", "AAPL"):
        captured = _run(sym)
        sql = captured["query"]

        assert "America/New_York" not in sql, sym
        assert "EXTRACT(DOW" not in sql, sym
        assert "$4" not in sql, sym
        # Param shape: symbol, window_units, expiration.
        assert len(captured["args"]) == 3, sym
        assert captured["args"][0] == sym


def test_cash_index_restricts_to_regular_session():
    """SPX / NDX / RUT charts can't surface overnight gex_summary rows —
    they have no candlestick to align with.  Filter applied to BOTH the
    anchor and the bucket-rep CTE (the two-template trick
    get_historical_gex uses)."""
    captured = _run("SPX")
    sql = captured["query"]

    assert "EXTRACT(DOW FROM" in sql
    assert "America/New_York" in sql
    assert "BETWEEN TIME '09:30' AND TIME '16:00'" in sql
    assert "<> ALL($4::date[])" in sql

    # Filter is applied in BOTH gex_summary scans.
    assert sql.count("EXTRACT(DOW") == 2

    # bucket_reps aliases gex_summary as ``gs`` — its session predicate
    # must reference gs.timestamp, not the bare column (PG won't resolve).
    bucket_reps_idx = sql.index("bucket_reps AS")
    bucket_reps_block = sql[bucket_reps_idx:]
    assert "gs.timestamp AT TIME ZONE" in bucket_reps_block

    # Param shape: symbol, window_units, expiration, holidays[].
    assert len(captured["args"]) == 4
    assert captured["args"][0] == "SPX"
    assert isinstance(captured["args"][3], list)


# ---------------------------------------------------------------------------
# Expiration filter is fixed-shape (single predicate, single bind)
# ---------------------------------------------------------------------------


def test_expiration_filter_is_fixed_shape_predicate():
    """One SQL plan for both modes: ``$3::date IS NULL OR gbs.expiration =
    $3::date``.  The query must look identical regardless of whether the
    caller passed an expiration date or 'all'."""
    captured_all = _run("SPY", expiration=None)
    captured_one = _run("SPY", expiration=date(2026, 6, 19))
    assert captured_all["query"] == captured_one["query"]
    assert "$3::date IS NULL OR gbs.expiration = $3::date" in captured_all["query"]
    assert captured_all["args"][2] is None
    assert captured_one["args"][2] == date(2026, 6, 19)


# ---------------------------------------------------------------------------
# OHLC bucketed against the SAME bucket expression
# ---------------------------------------------------------------------------


def test_ohlc_uses_same_bucket_expression_as_gex():
    """OHLC and GEX must bucket on the same expression so the candle and
    the per-strike surface always line up on the time axis, even across
    DST transitions and half-day sessions."""
    captured = _run("SPY", timeframe="5min")
    sql = captured["query"]

    # Both buckets reference the 5-minute boundary expression.
    # _bucket_expr('5min') is defined as a date_trunc('hour') + 5-minute
    # floor in _sql_helpers.py.
    assert "ohlc AS" in sql
    assert sql.count("FLOOR(EXTRACT(MINUTE FROM timestamp) / 5)") >= 2


# ---------------------------------------------------------------------------
# Response ordering + grouping
# ---------------------------------------------------------------------------


def test_query_orders_ascending_by_bucket_time():
    """The rewind chart's rewindIndex indexes directly into the response
    array; ASCENDING ordering (most recent last) is the contract the
    frontend relies on for ``buckets[rewindIndex]``."""
    sql = _run("SPY")["query"]
    # ORDER BY ASCENDING (no DESC) — different from heatmap which is DESC.
    assert "ORDER BY br.bucket_ts ASC, s.strike ASC" in sql


def test_response_groups_flat_rows_into_per_bucket_dicts():
    """Flat (timestamp, strike, ...) SQL rows must be grouped into one
    dict per timestamp, carrying OHLC / flip / walls once and a
    ``strikes`` array of the per-row gamma values."""
    rows = [
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 512.30,
            "high": 513.10,
            "low": 511.85,
            "close": 512.80,
            "gamma_flip": 510.0,
            "call_wall": 515.0,
            "put_wall": 505.0,
            "strike": 505.0,
            "call_gex": 1234.5,
            "put_gex": -2345.6,
            "net_gex": -1111.1,
            "call_oi": 8200,
            "put_oi": 9100,
        },
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 512.30,
            "high": 513.10,
            "low": 511.85,
            "close": 512.80,
            "gamma_flip": 510.0,
            "call_wall": 515.0,
            "put_wall": 505.0,
            "strike": 510.0,
            "call_gex": 5555.5,
            "put_gex": -1111.1,
            "net_gex": 4444.4,
            "call_oi": 3300,
            "put_oi": 2200,
        },
    ]
    captured = _run("SPY", rows=rows)
    result = captured["result"]

    assert len(result) == 1
    bucket = result[0]
    assert bucket["timestamp"] == "2026-06-08T14:30:00+00:00"
    assert bucket["symbol"] == "SPY"
    assert bucket["open"] == 512.30
    assert bucket["close"] == 512.80
    assert bucket["gamma_flip"] == 510.0
    assert bucket["call_wall"] == 515.0
    assert bucket["put_wall"] == 505.0
    assert len(bucket["strikes"]) == 2
    # Values map by name: call_gamma <- call_gex, etc.
    assert bucket["strikes"][0]["strike"] == 505.0
    assert bucket["strikes"][0]["call_gamma"] == 1234.5
    assert bucket["strikes"][0]["put_gamma"] == -2345.6
    assert bucket["strikes"][0]["net_gamma"] == -1111.1
    assert bucket["strikes"][0]["call_oi"] == 8200
    assert bucket["strikes"][1]["strike"] == 510.0


def test_response_omits_strikes_with_zero_values_only():
    """A bucket with no actual gex/oi data at a strike (all zeros) is
    just noise on the panels — drop it to keep the payload lean for
    long-window rewinds."""
    rows = [
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "gamma_flip": None, "call_wall": None, "put_wall": None,
            "strike": 500.0,
            "call_gex": 0, "put_gex": 0, "net_gex": 0,
            "call_oi": 0, "put_oi": 0,
        },
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "gamma_flip": None, "call_wall": None, "put_wall": None,
            "strike": 510.0,
            "call_gex": 100, "put_gex": 0, "net_gex": 100,
            "call_oi": 0, "put_oi": 0,
        },
    ]
    captured = _run("SPY", rows=rows)
    result = captured["result"]

    assert len(result) == 1
    # Only the 510 row survives — the 500 row was all-zero noise.
    assert [s["strike"] for s in result[0]["strikes"]] == [510.0]


def test_response_keeps_buckets_with_no_strikes():
    """A bucket whose representative gex_summary timestamp had no
    gex_by_strike rows (rare — typically when ingestion lagged on that
    cycle) still appears, carrying OHLC + walls and an empty strikes
    array.  The chart renders the candle and flip line without the
    per-strike surface for that bucket rather than dropping the bucket
    entirely (which would misalign the rewindIndex grid)."""
    rows = [
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
            "gamma_flip": 100.0, "call_wall": 102.0, "put_wall": 98.0,
            "strike": None,
            "call_gex": None, "put_gex": None, "net_gex": None,
            "call_oi": None, "put_oi": None,
        },
    ]
    captured = _run("SPY", rows=rows)
    result = captured["result"]

    assert len(result) == 1
    assert result[0]["close"] == 100.5
    assert result[0]["gamma_flip"] == 100.0
    assert result[0]["strikes"] == []


# ---------------------------------------------------------------------------
# Window-units clamp + cache hit
# ---------------------------------------------------------------------------


def test_window_units_clamped_to_safe_range():
    """Anti-DoS bound: window_units cannot grow without bound (e.g. a
    rogue client passing 100_000 would tip the JOIN into millions of
    rows).  Clamp to 480 — that's already 8 hours of 1-minute buckets
    or 40 hours of 5-minute buckets, well past the rewind depth the
    frontend needs."""
    captured = _run("SPY", window_units=10_000)
    # Param 2 (1-indexed: $2) is window_units; should be clamped.
    assert captured["args"][1] == 480

    captured_low = _run("SPY", window_units=0)
    # Lower clamp keeps at least 1 bucket.
    assert captured_low["args"][1] == 1
