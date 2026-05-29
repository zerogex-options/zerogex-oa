"""Regression tests for the /api/gex/heatmap query shape and behavior.

History:
  v1 (slow)  — AVG(net_gex) over the whole window of raw gex_by_strike,
               spot±50 filtered AFTER the GROUP BY.
  v2 (still slow for 1day) — same, but spot±50 pushed before the GROUP
               BY.  Cut the aggregate's working set but NOT the scan:
               timeframe=1day with window_units=N still scanned an
               N-day range of the highest-cardinality table (~14s).
  v3 (fast)  — pick ONE representative (latest) snapshot per bucket from
               the lightweight gex_summary, then read gex_by_strike ONLY
               at those ~window_units timestamps (the get_historical_gex
               pattern).  Cells are the bucket-close GEX surface, not a
               within-bucket average.
  v4         — anchor the window's right edge on MAX(gex_summary.timestamp)
               (with the cash-index session filter applied to the anchor
               too) instead of MAX(underlying_quotes.timestamp).  Decouples
               heatmap freshness from the TradeStation Stream Bars feed
               so an underlying-quotes stall no longer freezes the chart
               while analytics is still writing rows.  ``underlying_quotes``
               remains the source of ``spot_close`` for the strike band.

These tests pin v4 so a refactor can't regress to scanning the full
window of gex_by_strike or re-coupling the chart to underlying_quotes
freshness.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from unittest.mock import patch

from src.api.database import DatabaseManager


class _RecordingConn:
    """Captures queries and returns canned rows."""

    def __init__(self, fetch_rows=None):
        self._fetch_rows = fetch_rows or []
        self.queries = []

    async def fetch(self, query, *_args):
        self.queries.append(query)
        return list(self._fetch_rows)


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def test_heatmap_reads_gex_by_strike_only_at_representative_timestamps():
    """The core anti-regression: gex_by_strike must be JOINed on the
    per-bucket representative timestamps (g.timestamp = br.rep_ts), NOT
    range-scanned across the whole window."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)

    asyncio.run(db.get_gex_heatmap("SPY", "1day", 10))

    assert conn.queries, "heatmap query was never executed"
    sql = conn.queries[0]

    # Representative snapshot per bucket comes from the lightweight
    # gex_summary, not the per-strike table.
    assert "bucket_reps AS" in sql
    assert "FROM gex_summary" in sql
    assert "DISTINCT ON" in sql

    # gex_by_strike is joined on the representative timestamp, never
    # range-scanned.  If a future edit reintroduces a windowed scan of
    # gex_by_strike (timestamp >= start_time), this fails.
    assert "JOIN gex_by_strike g" in sql
    assert "g.timestamp = br.rep_ts" in sql
    gbs_idx = sql.index("gex_by_strike g")
    # No "timestamp >= ... start_time" predicate attached to the
    # gex_by_strike read.
    tail = sql[gbs_idx:]
    assert "start_time" not in tail, "gex_by_strike must not be window-scanned"

    # The v1/v2 shapes must be gone.
    assert "recent_data AS" not in sql
    assert "filtered_data" not in sql
    assert "latest_price_timestamp AS" not in sql
    assert "latest_price AS" not in sql


def test_heatmap_anchor_is_gex_summary_not_underlying_quotes():
    """v4 pin: the window's right edge (max_ts) is selected from
    gex_summary, not underlying_quotes.  Anchoring on underlying_quotes
    coupled chart freshness to the TradeStation Stream Bars feed — any
    cause of stalled underlying writes (stream-cap eviction, single-
    symbol bar-feed outages, vendor reset glitches) froze the heatmap
    even while analytics kept writing gex_summary rows.

    The spot price for the strike-band predicate is still sourced from
    underlying_quotes (no GEX-side equivalent), but that's a far softer
    dependency: a stale spot just sizes the band against the level the
    analytics engine has also been computing against, so the band stays
    centered on the heatmap data instead of going dark.
    """
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60))
    sql = conn.queries[0]

    # New CTE names.
    assert "latest_summary AS" in sql
    assert "spot AS" in sql
    # The old underlying_quotes-rooted anchor is gone.
    assert "latest_quote AS" not in sql

    # max_ts comes out of gex_summary inside latest_summary.
    anchor_cte = sql[sql.index("latest_summary AS") : sql.index("spot AS")]
    assert "FROM gex_summary" in anchor_cte
    assert "FROM underlying_quotes" not in anchor_cte

    # spot_close (used by the strike-band predicate only) still comes
    # from underlying_quotes inside its own CTE.
    spot_cte = sql[sql.index("spot AS") : sql.index("time_window AS")]
    assert "FROM underlying_quotes" in spot_cte
    assert "close" in spot_cte
    assert "FROM gex_summary" not in spot_cte

    # The time window's end_time is the gex_summary-derived max_ts, never
    # the underlying_quote timestamp.
    tw = sql[sql.index("time_window AS") : sql.index("bucket_reps AS")]
    assert "FROM latest_summary" in tw
    assert "FROM latest_quote" not in tw


def test_heatmap_keeps_strike_band_around_underlying_spot():
    """The strike band stays anchored to underlying_quotes.close (via the
    ``spot`` CTE) and remains proportional to that spot — the old fixed
    ±50 absolute band is gone."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60))
    sql = conn.queries[0]

    assert "spot AS" in sql
    assert (
        "ABS(g.strike - (SELECT spot_close FROM spot)) "
        "<= (SELECT spot_close FROM spot) * 0.08" in sql
    )
    assert "<= 50" not in sql
    # Newest-first, strike ascending — the documented row order.
    assert "ORDER BY br.bucket_ts DESC, g.strike ASC" in sql


def test_heatmap_surfaces_gamma_flip_from_its_own_buckets():
    """gamma_flip must ride the heatmap's own (RTH-filtered, over-fetched)
    bucket timestamps so the frontend's primary path uses it instead of
    falling back to the short, separately-windowed /api/gex/historical
    call. Pin: the representative gex_summary row carries gamma_flip_point
    and it's projected once per bucket (lowest-strike row, NULL elsewhere)
    so the payload doesn't repeat it across every strike."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap("SPX", "5min", 60))
    sql = conn.queries[0]

    # gamma_flip_point is pulled from the representative gex_summary
    # snapshot selected inside bucket_reps.  (Since v4, gex_summary is
    # also scanned earlier inside the latest_summary CTE — look for
    # "FROM gex_summary" only AFTER the bucket_reps marker so the slice
    # captures the right region.)
    bucket_reps_idx = sql.index("bucket_reps AS")
    reps_from_idx = sql.index("FROM gex_summary", bucket_reps_idx)
    reps = sql[bucket_reps_idx:reps_from_idx]
    assert "gamma_flip_point AS gamma_flip" in reps

    # Emitted once per bucket (lowest strike), NULL on the other strikes.
    assert "MIN(g.strike) OVER (PARTITION BY br.bucket_ts)" in sql
    assert "THEN MAX(br.gamma_flip)" in sql
    assert "END AS gamma_flip" in sql


def test_heatmap_returns_mapped_rows():
    db = DatabaseManager()
    rows = [
        {"timestamp": "2026-05-15T20:00:00Z", "strike": 585.0, "net_gex": 1.2e9},
        {"timestamp": "2026-05-15T20:00:00Z", "strike": 586.0, "net_gex": -3.4e8},
    ]
    conn = _RecordingConn(fetch_rows=rows)
    _install_conn(db, conn)

    result = asyncio.run(db.get_gex_heatmap("spy", "5min", 60))
    # get_gex_heatmap groups the flat (timestamp, strike) DB rows into one
    # object per bucket: gamma_flip / gamma_flip_span_used carried once
    # (None here -- these mock rows omit those columns) plus a ``heatmap``
    # array of {strike, net_gex} in the query's strike-ascending order.
    assert result == [
        {
            "timestamp": "2026-05-15T20:00:00Z",
            "gamma_flip": None,
            "gamma_flip_span_used": None,
            "heatmap": [
                {"strike": 585.0, "net_gex": 1.2e9},
                {"strike": 586.0, "net_gex": -3.4e8},
            ],
        }
    ]


def test_heatmap_window_units_clamped_to_300():
    db = DatabaseManager()
    captured = {}

    class _Conn(_RecordingConn):
        async def fetch(self, query, *args):
            captured["args"] = args
            return []

    conn = _Conn()
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap("SPY", "1day", 99999))
    # (symbol, window_units) — window_units clamped to 300.
    assert captured["args"] == ("SPY", 300)


def _run_and_capture(symbol, timeframe="5min", window_units=60):
    db = DatabaseManager()
    captured = {}

    class _Conn(_RecordingConn):
        async def fetch(self, query, *args):
            captured["query"] = query
            captured["args"] = args
            return []

    conn = _Conn()
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap(symbol, timeframe, window_units))
    return captured


def test_etf_heatmap_has_no_cash_session_filter():
    """ETFs / equities genuinely trade extended hours — the query and its
    params must stay exactly as before (no session predicate, two args)."""
    captured = _run_and_capture("SPY")
    sql = captured["query"]

    assert "America/New_York" not in sql
    assert "EXTRACT(DOW" not in sql
    assert "$3" not in sql
    # Unchanged param shape: (symbol, window_units) only.
    assert captured["args"] == ("SPY", 60)


def test_cash_index_heatmap_restricts_to_regular_session():
    """SPX (a cash index) must restrict BOTH the window anchor
    (latest_summary) AND the per-bucket representatives (bucket_reps) to
    the regular cash session so extended-hours / overnight buckets never
    reach the heatmap AND the chart's right edge lands on the most recent
    RTH analytics row instead of whatever overnight cycle ran last.  The
    NYSE-holiday list is bound as the 3rd param and the same predicate
    fragment is interpolated into both CTEs."""
    captured = _run_and_capture("SPX")
    sql = captured["query"]

    # Weekday + 09:30–16:00 ET band, mirroring get_session_closes.
    assert "EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 1 AND 5" in sql
    assert "BETWEEN TIME '09:30' AND TIME '16:00'" in sql
    # NYSE holidays excluded via a bound date[] param.
    assert "<> ALL($3::date[])" in sql

    # The session predicate is attached to BOTH gex_summary scans — the
    # window anchor in latest_summary AND the per-bucket representative
    # selection in bucket_reps — but never to the gex_by_strike join.
    join_idx = sql.index("JOIN gex_by_strike g")
    assert sql.count("EXTRACT(DOW") == 2
    extract_positions = [i for i in range(len(sql)) if sql.startswith("EXTRACT(DOW", i)]
    # Both EXTRACT(DOW occurrences precede the join, neither follows it.
    assert all(idx < join_idx for idx in extract_positions)
    # First occurrence is inside latest_summary; second is inside bucket_reps.
    summary_anchor_idx = sql.index("FROM gex_summary")  # first one — latest_summary
    bucket_reps_idx = sql.index("bucket_reps AS")
    assert summary_anchor_idx < extract_positions[0] < bucket_reps_idx
    bucket_reps_summary_idx = sql.index("FROM gex_summary", bucket_reps_idx)
    assert bucket_reps_summary_idx < extract_positions[1] < join_idx

    # symbol, window_units, then the holiday list.
    assert captured["args"][0] == "SPX"
    assert captured["args"][1] == 60
    assert isinstance(captured["args"][2], list)


def test_strike_band_is_proportional_for_every_underlying():
    """A fixed ±50 was ≈±8.5% of SPY but only ≈±0.7% of a ~$7400 index,
    collapsing the index heatmap into a thin strip inside the frontend's
    price-cropped y-axis. Strikes are now scoped proportionally to spot
    for ETFs and cash indices alike — no bare fixed-50 band anywhere."""
    for sym in ("SPY", "QQQ", "SPX", "NDX", "AAPL"):
        sql = _run_and_capture(sym)["query"]
        assert (
            "ABS(g.strike - (SELECT spot_close FROM spot)) "
            "<= (SELECT spot_close FROM spot) * 0.08" in sql
        ), sym
        assert "<= 50" not in sql, sym


def test_strike_band_pct_config_default_and_bounds():
    """GEX_HEATMAP_STRIKE_BAND_PCT defaults to 0.08 and is clamped to
    [0.005, 0.5] so a misconfigured env var can't return zero strikes or
    scan the whole chain."""
    from src.config import _getenv_float

    def band(env):
        with patch.dict(os.environ, env, clear=False):
            return _getenv_float("GEX_HEATMAP_STRIKE_BAND_PCT", 0.08, min=0.005, max=0.5)

    assert band({}) == 0.08
    assert band({"GEX_HEATMAP_STRIKE_BAND_PCT": "0.03"}) == 0.03
    assert band({"GEX_HEATMAP_STRIKE_BAND_PCT": "0"}) == 0.005  # clamped up
    assert band({"GEX_HEATMAP_STRIKE_BAND_PCT": "9"}) == 0.5  # clamped down


def test_strike_band_pct_is_config_driven():
    """The band fraction comes from GEX_HEATMAP_STRIKE_BAND_PCT (bounded
    in config), not a hard-coded literal — overriding the instance
    attribute changes the rendered predicate."""
    db = DatabaseManager()
    db._gex_heatmap_strike_band_pct = 0.05
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60))
    sql = conn.queries[0]
    assert "(SELECT spot_close FROM spot) * 0.05" in sql
    assert "* 0.08" not in sql


def test_cash_index_detection_is_case_insensitive():
    """Lowercased index symbols still get the session filter (symbol is
    upper-cased before the cash-index check)."""
    sql = _run_and_capture("spx")["query"]
    assert "America/New_York" in sql
