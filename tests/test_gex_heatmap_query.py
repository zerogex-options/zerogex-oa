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
  v5         — move the gex_by_strike read into a correlated LATERAL with
               its own GROUP BY, in a ``strikes`` CTE.  v3's rep_ts join
               only *expressed* the ~window_units-probe plan; the planner
               could still fall back to a hash/merge join, and the
               redundant window bound that capped that fallback capped it
               at the WINDOW, which is ~390x the rep set at timeframe=1day.
               A lateral reference is evaluated per outer row and the
               grouping blocks pull-up, so the bound is now structural.

These tests pin v5 so a refactor can't regress to scanning the full
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


def _sql_only(sql: str) -> str:
    """Strip ``--`` comment lines so a structural assertion can only be
    satisfied by executable SQL.

    The strikes CTE carries a long comment explaining which read shapes are
    forbidden and why — naming the plain join it replaced.  Asserting against
    the raw query string let these guards pass on the prose alone, which is
    exactly backwards for a regression test.
    """
    return "\n".join(
        line.split("--", 1)[0] if "--" in line else line for line in sql.splitlines()
    )


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def test_heatmap_read_is_a_correlated_lateral_fenced_to_the_rep_timestamps():
    """The core anti-regression: the gex_by_strike read must be BOUNDED to the
    ~window_units bucket-representative timestamps, by construction.

    The read keys on the per-bucket representative timestamps
    (``g.timestamp = br.rep_ts``), so the *intended* plan is ~window_units
    index point-lookups.  Expressed as a plain join, that was only an
    intention: ``time_window`` / ``bucket_reps`` are optimisation-fence CTEs
    whose row counts the planner cannot estimate, so under production stats it
    periodically abandoned the nested loop for a merge/hash join reading the
    ENTIRE gex_by_strike table — the 15s "GEX heatmap query timed out ...
    returning empty" path.

    A logically-redundant ``g.timestamp BETWEEN start_time AND end_time``
    bound (commit 81586d8) capped that fallback at the window's rows.  Not
    enough, because the window is not the rep set: each bucket contributes ONE
    rep_ts out of every snapshot inside it, so the window holds ~1x the read's
    timestamps at 1min but ~5x at 5min, ~60x at 1hr and ~390x at 1day — and
    ``timeframe=1day&window_units=300`` is a request this endpoint accepts.
    The strike band does not save it: the band caps how many rows SURVIVE, not
    how many the fallback reads before filtering.

    So the fence is structural now.  The read is a LATERAL subquery correlated
    on ``br.rep_ts`` carrying its own GROUP BY: a lateral reference is
    evaluated per outer row, and the grouping blocks subquery pull-up, so PG
    cannot re-plan it as a hash/merge join over the window or the table.

    Asserted against comment-stripped SQL — the CTE's comment names the join
    shape this test forbids, so matching the raw string would pass on prose.
    """
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)

    asyncio.run(db.get_gex_heatmap("SPY", "1day", 10))

    assert conn.queries, "heatmap query was never executed"
    sql = _sql_only(conn.queries[0])

    # Representative snapshot per bucket comes from the lightweight
    # gex_summary, not the per-strike table.
    assert "bucket_reps AS" in sql
    assert "FROM gex_summary" in sql
    assert "DISTINCT ON" in sql

    strikes_cte = sql[sql.index("strikes AS (") : sql.index(") cell")]

    # Correlated LATERAL read: cannot be re-planned as a hash/merge join.
    assert "CROSS JOIN LATERAL" in strikes_cte
    assert "FROM gex_by_strike g" in strikes_cte
    # Still keyed on the per-bucket representative (the point-lookup path).
    assert "g.timestamp = br.rep_ts" in strikes_cte
    # The lateral aggregates, which is what blocks PG from pulling it up into
    # the parent and losing the nested loop.
    assert "GROUP BY g.strike" in strikes_cte[strikes_cte.index("CROSS JOIN LATERAL") :]
    # NOT re-planned back into a plain join on the table.
    assert "JOIN gex_by_strike" not in strikes_cte.replace("CROSS JOIN LATERAL", "")

    # Window bound retained as defence in depth.
    assert "g.timestamp BETWEEN" in strikes_cte
    assert "start_time FROM time_window" in strikes_cte
    assert "end_time FROM time_window" in strikes_cte

    # The v1/v2 shapes must be gone.
    assert "recent_data AS" not in sql
    assert "filtered_data" not in sql
    assert "latest_price_timestamp AS" not in sql
    assert "latest_price AS" not in sql


def test_heatmap_cells_stay_one_avg_per_bucket_strike():
    """Cell values are unchanged by the lateral rewrite.

    The cross-expiration combination per (bucket, strike) is still ``AVG`` over
    that bucket's representative snapshot.  Grouping by strike *inside* the
    lateral is identical to the old ``GROUP BY br.bucket_ts, g.strike``,
    because ``bucket_reps`` is DISTINCT ON the bucket — one rep_ts per
    bucket_ts — so the lateral runs against exactly one timestamp per group.

    And unlike the strike-profile CTE, this one must NOT drop zero cells: a
    zero is a real neutral cell on a heatmap, so a HAVING here would punch a
    hole in the surface rather than trim a payload.
    """
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60))
    sql = _sql_only(conn.queries[0])
    strikes_cte = sql[sql.index("strikes AS (") : sql.index(") cell")]

    assert "AVG(g.net_gex) AS net_gex" in strikes_cte
    assert "GROUP BY g.strike" in strikes_cte
    # One row per (bucket, strike): no second grouping key smuggled in.
    assert "GROUP BY g.strike," not in strikes_cte
    # No zero-cell filter.
    assert "HAVING" not in strikes_cte


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
    sql = _sql_only(conn.queries[0])

    assert "spot AS" in sql
    assert (
        "ABS(g.strike - (SELECT spot_close FROM spot)) "
        "<= (SELECT spot_close FROM spot) * 0.08" in sql
    )
    assert "<= 50" not in sql
    # Applied inside the lateral, so the band prunes rows at the index probe
    # rather than after the per-bucket read.
    strikes_cte = sql[sql.index("strikes AS (") : sql.index(") cell")]
    assert "ABS(g.strike - (SELECT spot_close FROM spot))" in strikes_cte
    # Newest-first, strike ascending — the documented row order.
    assert "ORDER BY s.bucket_ts DESC, s.strike ASC" in sql


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
    sql = _sql_only(conn.queries[0])

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
    # The window function now runs over the strikes CTE, which is already one
    # row per (bucket, strike) — so the flip is projected directly instead of
    # being collapsed through a GROUP BY with MAX().
    assert "MIN(s.strike) OVER (PARTITION BY s.bucket_ts)" in sql
    assert "THEN s.gamma_flip" in sql
    assert "END AS gamma_flip" in sql
    assert "THEN s.gamma_flip_span_used" in sql


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
    """SPX (a cash index) must restrict every gex_summary scan to the
    regular cash session so extended-hours / overnight buckets never
    reach the heatmap AND the chart's right edge lands on the most recent
    RTH analytics row instead of whatever overnight cycle ran last.  The
    NYSE-holiday list is bound as the 3rd param and the same predicate
    fragment is interpolated into the window anchor (latest_summary), the
    bucket-floor subquery in time_window (so ``window_units`` counts only
    RTH buckets), and the per-bucket representatives (bucket_reps)."""
    captured = _run_and_capture("SPX")
    sql = _sql_only(captured["query"])

    # Weekday + 09:30–16:00 ET band, mirroring get_session_closes.
    assert "EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 1 AND 5" in sql
    assert "BETWEEN TIME '09:30' AND TIME '16:00'" in sql
    # NYSE holidays excluded via a bound date[] param.
    assert "<> ALL($3::date[])" in sql

    # The session predicate is attached to all three gex_summary scans —
    # the latest_summary anchor, the bucket-floor subquery in time_window,
    # and bucket_reps — but never to the gex_by_strike read.
    join_idx = sql.index("FROM gex_by_strike g")
    assert sql.count("EXTRACT(DOW") == 3
    extract_positions = [i for i in range(len(sql)) if sql.startswith("EXTRACT(DOW", i)]
    # All EXTRACT(DOW occurrences precede the join, none follow it.
    assert all(idx < join_idx for idx in extract_positions)
    # Each occurrence lands in its corresponding CTE — anchor, then
    # time_window bucket-floor, then bucket_reps.
    summary_anchor_idx = sql.index("FROM gex_summary")
    time_window_idx = sql.index("time_window AS")
    bucket_reps_idx = sql.index("bucket_reps AS")
    assert summary_anchor_idx < extract_positions[0] < time_window_idx
    assert time_window_idx < extract_positions[1] < bucket_reps_idx
    bucket_reps_summary_idx = sql.index("FROM gex_summary", bucket_reps_idx)
    assert bucket_reps_summary_idx < extract_positions[2] < join_idx

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
