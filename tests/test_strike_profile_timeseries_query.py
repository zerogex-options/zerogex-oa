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
    they have no candlestick to align with.  Filter applied to all three
    gex_summary scans: the ``latest`` anchor, the ``bounds`` bucket-floor
    subquery (so ``window_units`` counts only RTH buckets), and the
    ``bucket_reps`` per-bucket selector."""
    captured = _run("SPX")
    sql = captured["query"]

    assert "EXTRACT(DOW FROM" in sql
    assert "America/New_York" in sql
    assert "BETWEEN TIME '09:30' AND TIME '16:00'" in sql
    assert "<> ALL($4::date[])" in sql

    # Filter applied to all three gex_summary scans: latest anchor,
    # bucket-floor subquery, and bucket_reps. Counting RTH-only buckets
    # for the floor matches what bucket_reps will surface — anything
    # less would re-introduce the wall-clock sparsity bug for cash
    # indices on coarser intervals.
    assert sql.count("EXTRACT(DOW") == 3

    # bucket_reps aliases gex_summary as ``gs`` — its session predicate
    # must reference gs.timestamp, not the bare column (PG won't resolve).
    bucket_reps_idx = sql.index("bucket_reps AS")
    bucket_reps_block = sql[bucket_reps_idx:]
    assert "gs.timestamp AT TIME ZONE" in bucket_reps_block

    # The bucket-floor subquery sits inside ``bounds`` and scans
    # gex_summary without aliasing it, so its session predicate must
    # reference the bare ``timestamp`` column.
    bounds_idx = sql.index("bounds AS")
    bounds_block = sql[bounds_idx:bucket_reps_idx]
    assert "EXTRACT(DOW FROM timestamp AT TIME ZONE" in bounds_block
    assert "LIMIT $2" in bounds_block

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
            "strike": 505.0,
            "call_gamma_raw": 0.0,
            "put_gamma_raw": 80.0,
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
            "strike": 515.0,
            "call_gamma_raw": 50.0,
            "put_gamma_raw": 0.0,
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
    # Walls are computed from the bucket's own (filtered, summed) gamma
    # rows against the bucket's close (512.80) — the only call_gamma_raw
    # above spot sits at 515; the only put_gamma_raw below spot sits at
    # 505.
    assert bucket["call_wall"] == 515.0
    assert bucket["put_wall"] == 505.0
    assert len(bucket["strikes"]) == 2
    # Values map by name: call_gamma <- call_gex, etc.
    assert bucket["strikes"][0]["strike"] == 505.0
    assert bucket["strikes"][0]["call_gamma"] == 1234.5
    assert bucket["strikes"][0]["put_gamma"] == -2345.6
    assert bucket["strikes"][0]["net_gamma"] == -1111.1
    assert bucket["strikes"][0]["call_oi"] == 8200
    assert bucket["strikes"][1]["strike"] == 515.0
    # Raw-gamma fields are wall-computation inputs only; they must not
    # leak into the response payload.
    assert "call_gamma_raw" not in bucket["strikes"][0]
    assert "put_gamma_raw" not in bucket["strikes"][0]


def test_response_omits_strikes_with_zero_values_only():
    """A bucket with no actual gex/oi data at a strike (all zeros) is
    just noise on the panels — drop it to keep the payload lean for
    long-window rewinds."""
    rows = [
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "gamma_flip": None,
            "strike": 500.0,
            "call_gamma_raw": 0.0,
            "put_gamma_raw": 0.0,
            "call_gex": 0,
            "put_gex": 0,
            "net_gex": 0,
            "call_oi": 0,
            "put_oi": 0,
        },
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "gamma_flip": None,
            "strike": 510.0,
            "call_gamma_raw": 1.0,
            "put_gamma_raw": 0.0,
            "call_gex": 100,
            "put_gex": 0,
            "net_gex": 100,
            "call_oi": 0,
            "put_oi": 0,
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
    cycle) still appears, carrying OHLC + flip and an empty strikes
    array.  The chart renders the candle and flip line without the
    per-strike surface for that bucket rather than dropping the bucket
    entirely (which would misalign the rewindIndex grid).  Walls are
    NULL because there are no strikes to compute them from — the
    persisted ``gex_summary.call_wall`` is no longer carried through;
    /api/gex/summary remains the source for the aggregate-basis walls
    when callers want them independent of the chart's filter."""
    rows = [
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "gamma_flip": 100.0,
            "strike": None,
            "call_gamma_raw": None,
            "put_gamma_raw": None,
            "call_gex": None,
            "put_gex": None,
            "net_gex": None,
            "call_oi": None,
            "put_oi": None,
        },
    ]
    captured = _run("SPY", rows=rows)
    result = captured["result"]

    assert len(result) == 1
    assert result[0]["close"] == 100.5
    assert result[0]["gamma_flip"] == 100.0
    assert result[0]["strikes"] == []
    assert result[0]["call_wall"] is None
    assert result[0]["put_wall"] is None


def test_walls_per_bucket_follow_the_summed_gamma_basis():
    """Walls must agree with the bars in the same bucket.  When the
    request filter aggregates expirations (``expirations=all``), the
    summed-by-strike gamma is what the chart renders; the wall must
    point at the strike with the largest summed gamma, not a
    single-expiration outlier.  This is the user-visible bug the
    helper fix targets.
    """
    # Spot 100, three strikes above spot.  Per-strike summed call gamma:
    #   105 -> 90 (two expirations, 45 + 45 not modeled here — SQL has
    #              already SUMmed by strike before returning)
    #   110 -> 70
    #   115 -> 80
    # Below spot, the put side has 95 -> 90, 90 -> 60, 85 -> 70.
    # Walls must be 105 / 95 — the strikes with the largest aggregated
    # gamma on each side of spot.
    base = {
        "timestamp": "2026-06-08T14:30:00+00:00",
        "open": 100.0,
        "high": 100.0,
        "low": 100.0,
        "close": 100.0,
        "gamma_flip": None,
        "call_oi": 0,
        "put_oi": 0,
    }
    rows = []
    for strike, cg, pg in (
        (105.0, 90.0, 0.0),
        (110.0, 70.0, 0.0),
        (115.0, 80.0, 0.0),
        (95.0, 0.0, 90.0),
        (90.0, 0.0, 60.0),
        (85.0, 0.0, 70.0),
    ):
        rows.append(
            {
                **base,
                "strike": strike,
                "call_gamma_raw": cg,
                "put_gamma_raw": pg,
                # Dollar GEX values just need to be non-zero so the
                # row isn't dropped as noise; the wall computation
                # only reads call_gamma_raw / put_gamma_raw.
                "call_gex": max(cg, 1.0),
                "put_gex": -max(pg, 1.0),
                "net_gex": cg - pg,
            }
        )
    captured = _run("SPY", rows=rows)
    bucket = captured["result"][0]
    assert bucket["call_wall"] == 105.0
    assert bucket["put_wall"] == 95.0


def test_walls_use_bucket_close_as_spot():
    """The spot used to split strikes into call/put regions is the
    bucket's own close — same convention the dollar-GEX scaling already
    uses.  This keeps the wall basis consistent with the candle shown
    in the bucket, even on historical buckets whose close diverges from
    the trailing live spot.
    """
    rows = [
        # Close = 100; 99 should be eligible only as a put wall, 101
        # only as a call wall.
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 100.0,
            "high": 100.0,
            "low": 100.0,
            "close": 100.0,
            "gamma_flip": None,
            "strike": 99.0,
            "call_gamma_raw": 50.0,
            "put_gamma_raw": 40.0,
            "call_gex": 50.0,
            "put_gex": -40.0,
            "net_gex": 10.0,
            "call_oi": 0,
            "put_oi": 0,
        },
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": 100.0,
            "high": 100.0,
            "low": 100.0,
            "close": 100.0,
            "gamma_flip": None,
            "strike": 101.0,
            "call_gamma_raw": 30.0,
            "put_gamma_raw": 70.0,
            "call_gex": 30.0,
            "put_gex": -70.0,
            "net_gex": -40.0,
            "call_oi": 0,
            "put_oi": 0,
        },
    ]
    captured = _run("SPY", rows=rows)
    bucket = captured["result"][0]
    # 101 is the only above-spot strike; 99 the only below-spot strike.
    # The spot filter dominates the gamma ranking.
    assert bucket["call_wall"] == 101.0
    assert bucket["put_wall"] == 99.0


def test_walls_null_when_close_is_missing():
    """A bucket whose underlying tape was missing has a NULL close.
    Without a spot reference the above/below-spot split is undefined,
    so walls must be NULL too — the chart already treats NULL walls as
    "no level drawn".  Failing closed here is cheaper than fabricating a
    wall from the wrong spot.
    """
    rows = [
        {
            "timestamp": "2026-06-08T14:30:00+00:00",
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "gamma_flip": None,
            "strike": 100.0,
            "call_gamma_raw": 50.0,
            "put_gamma_raw": 50.0,
            "call_gex": 0.0,
            "put_gex": 0.0,
            "net_gex": 0.0,
            "call_oi": 10,
            "put_oi": 10,
        },
    ]
    captured = _run("SPY", rows=rows)
    bucket = captured["result"][0]
    assert bucket["call_wall"] is None
    assert bucket["put_wall"] is None


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


# ---------------------------------------------------------------------------
# Cache TTL — long-lived for this endpoint specifically
# ---------------------------------------------------------------------------


def test_response_cached_with_dedicated_ttl():
    """This endpoint's response is cached with the dedicated
    ``_strike_profile_timeseries_cache_ttl_seconds`` TTL (default 30s) —
    NOT the shared 5s analytics TTL.  The query JOINs ~720K rows on a
    480-bucket request; sharing the analytics TTL meant every 1Hz poll
    paid that full cost.  With the dedicated TTL only the first poll in
    each TTL window does, and bounded staleness is fine because the
    analytics cycle is ~60s anyway."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    # First call hits the DB and populates the cache.
    asyncio.run(db.get_strike_profile_timeseries("SPY", "1min", 78, None))
    assert len(conn.queries) == 1
    # Inspect the cache entry directly so we don't have to mock the clock —
    # the entry must exist and carry a TTL strictly greater than the shared
    # analytics TTL.  Either condition failing means the endpoint reverted to
    # the shared TTL and the rewind chart's polling cost will collapse.
    matching_keys = [k for k in db._read_cache if k.startswith("strike_profile_ts:")]
    assert matching_keys, "endpoint did not cache its response"
    expires_at, _ = db._read_cache[matching_keys[0]]
    assert db._strike_profile_timeseries_cache_ttl_seconds > db._analytics_cache_ttl_seconds

    # Second call hits the cache, no extra DB round-trip.
    asyncio.run(db.get_strike_profile_timeseries("SPY", "1min", 78, None))
    assert len(conn.queries) == 1
