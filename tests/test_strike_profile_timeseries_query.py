"""Regression tests for the /api/gex/strike-profile-timeseries query.

This endpoint backs the Strike Profile chart's rewind feature: it returns
a fully aligned per-bucket timeseries of OHLC + gamma-flip / call-wall /
put-wall + per-strike gamma exposure + open interest.  The chart's rewind
collapses to direct indexing into the returned array — so the contract
this test pins is:

  * the SQL has the same right-edge anchor + bucket-rep + JOIN-on-rep_ts
    shape every historical GEX endpoint uses, and the gex_by_strike read is
    fenced to the window — JOIN-on-rep_ts PLUS a logically-redundant
    ``timestamp BETWEEN start_ts AND end_ts`` bound — so no query plan can
    scan the whole 90-day table (the timeout this endpoint used to hit);
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

import pytest

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


def _sql_only(sql: str) -> str:
    """Strip ``--`` comment lines so a structural assertion can only be
    satisfied by executable SQL.

    The strikes CTE carries a long comment explaining which join shapes are
    forbidden and why — including the literal text of the shape it replaced.
    Asserting against the raw query string let those assertions pass on the
    prose alone, which is exactly backwards for a regression guard.
    """
    return "\n".join(
        line.split("--", 1)[0] if "--" in line else line for line in sql.splitlines()
    )


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def _run(symbol, timeframe="1min", window_units=78, expirations=None, rows=None):
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=rows or [])
    _install_conn(db, conn)
    result = asyncio.run(
        db.get_strike_profile_timeseries(symbol, timeframe, window_units, expirations)
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


def test_strikes_join_keyed_on_rep_ts_and_fenced_to_window():
    """The core anti-regression: the gex_by_strike read must be BOUNDED to the
    ~window_units bucket-representative timestamps, by construction.

    gex_by_strike is the highest-cardinality table on the API — one row per
    strike x expiration every ~60s analytics cycle, tens of millions of rows
    across the 90-day retention.  The strikes CTE keys the read on the
    per-bucket representative timestamps (``gbs.timestamp = br.rep_ts``), so
    the *intended* plan is ~window_units index point-lookups.

    That intent used to be expressed as a plain JOIN, which the planner was
    free to ignore: ``bucket_reps`` / ``bounds`` are optimisation-fence CTEs
    whose row counts it cannot estimate, so under production stats it
    periodically abandoned the point-lookup nested loop for a merge/hash join
    reading the ENTIRE table.  That was the 15s "Strike-profile timeseries
    query timed out ... returning empty" path (reproduced with EXPLAIN ANALYZE:
    an 8.5M-row table read end to end in ~1.9s; linear in table size, so a real
    multi-underlying / 90-day table crosses the timeout).

    A logically-redundant ``gbs.timestamp BETWEEN start_ts AND end_ts`` bound
    (commit c4d6463) capped that fallback to the window's rows.  It was not
    enough, because the window is not the rep set: each bucket contributes ONE
    rep_ts out of every snapshot inside it, so at 5min the window holds ~5x the
    timestamps the CTE reads (at 15min, ~15x).  window_units=78 at 1min is a
    78-minute window and the fallback fit inside the timeout; the same 78
    buckets at 5min span a 390-minute session and it did not — which is why the
    alert that prompted this fires on timeframe=5min only.

    So the fence is now structural.  The read is a LATERAL subquery correlated
    on ``br.rep_ts`` that carries its own GROUP BY: a lateral reference is
    evaluated per outer row, and the grouping blocks subquery pull-up, so PG
    cannot re-plan it as a hash/merge join over the table or over the window.
    The read is ~window_units equality probes whatever else the planner does —
    flat in the window's span, so 15min costs what 1min costs.  The redundant
    window bound stays as defence in depth.

    Asserted against comment-stripped SQL: the CTE's comment quotes the very
    join shape this test forbids, so matching the raw string would pass on the
    prose.
    """
    sql = _sql_only(_run("SPY")["query"])

    start = sql.index("strikes AS (")
    end = sql.index(") sx", start)
    strikes_cte = sql[start:end]

    # Correlated LATERAL read: cannot be re-planned as a hash/merge join.
    assert "CROSS JOIN LATERAL" in strikes_cte
    assert "FROM gex_by_strike gbs" in strikes_cte
    # Keyed on the per-bucket representative (the point-lookup path).
    assert "gbs.timestamp  = br.rep_ts" in strikes_cte

    # The lateral subquery must aggregate, which is what blocks PG from
    # pulling it up into the parent and losing the nested loop.
    lateral = strikes_cte[strikes_cte.index("CROSS JOIN LATERAL") :]
    assert "GROUP BY gbs.strike" in lateral

    # NOT re-planned back into a plain join on the table.
    assert "JOIN gex_by_strike" not in strikes_cte.replace("CROSS JOIN LATERAL", "")

    # Window bound retained as defence in depth.
    assert "gbs.timestamp  BETWEEN" in strikes_cte
    assert "start_ts FROM bounds" in strikes_cte
    assert "end_ts FROM bounds" in strikes_cte


def test_strikes_cte_drops_all_zero_strikes_in_sql():
    """All-zero (bucket, strike) aggregates must be dropped by the query, not
    just by the Python grouping.

    gex_by_strike holds a row for every (strike, expiration) the chain quotes,
    including the zero-OI wings that are the bulk of an SPX chain.  The Python
    grouping already discards a strike whose gex and both OI legs are zero, so
    without a HAVING those rows are fetched, decoded and thrown away — on the
    largest response this API serves.

    The SQL predicate is deliberately NARROWER than the Python one (it cannot
    see the bucket's close, which zeroes a whole bucket's dollar-GEX when the
    underlying tape is missing), so what it drops is a strict subset of what
    Python drops and the response is unchanged.
    """
    sql = _sql_only(_run("SPY")["query"])
    strikes_cte = sql[sql.index("strikes AS (") : sql.index(") sx")]

    having = strikes_cte[strikes_cte.index("HAVING") :]
    for col in ("call_gamma", "put_gamma", "call_oi", "put_oi"):
        assert f"SUM(COALESCE(gbs.{col}, 0)) <> 0" in having, col
    # ORed, not ANDed: a strike survives if ANY leg carries data.
    assert having.count("OR ") == 3
    assert "AND " not in having


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
    """One SQL plan for every mode: ``$3::date[] IS NULL OR gbs.expiration =
    ANY($3::date[])``.  The query must look identical whether the caller
    passed 'all' (None) or a set of expiration dates — only the bound
    ``date[]`` param differs.  Selecting a set binds a sorted, de-duped
    ``date[]``; 'all' binds NULL so the IS NULL branch fires."""
    captured_all = _run("SPY", expirations=None)
    captured_set = _run("SPY", expirations=[date(2026, 6, 20), date(2026, 6, 19)])
    assert captured_all["query"] == captured_set["query"]
    assert (
        "$3::date[] IS NULL OR gbs.expiration = ANY($3::date[])"
        in captured_all["query"]
    )
    assert captured_all["args"][2] is None
    # Normalised to a sorted, de-duplicated list before binding.
    assert captured_set["args"][2] == [date(2026, 6, 19), date(2026, 6, 20)]


# ---------------------------------------------------------------------------
# Per-strike gamma is SUMMED across expirations (never collapsed to one)
# ---------------------------------------------------------------------------


def test_strikes_cte_sums_gamma_across_expirations_not_collapse():
    """Per-(bucket, strike) gamma must be a SUM across every admitted
    expiration — the aggregate a multi-expiration Strike-Profile selection
    renders.  The regression this guards against is a *collapse* that reduces
    multiple expirations to a single one per strike (a ``DISTINCT ON (strike)``
    "latest row per group", an average, or a JOIN-then-dedup): that makes the
    chart's per-strike gamma SHRINK when a second expiration is added instead
    of growing — the reported symptom (an isolated 0DTE per-strike max ~3.6B
    dropping to the next day's value ALONE ~350.7M once a second date is
    added).  ``call_gamma``/``put_gamma`` are stored as non-negative
    ``sum(γ × OI)`` (see ``_calculate_gex_by_strike``), so a true SUM is
    monotone in the expiration set while a collapse is not.

    Pinned at the SQL-shape level so it runs in CI without a database; the
    value-level invariant is exercised against a real Postgres in
    ``tests/test_strike_profile_timeseries_expiration_sum.py``.
    """
    sql = _sql_only(_run("SPY")["query"])

    # Isolate the ``strikes`` CTE (header -> the close of its lateral) so an
    # assertion can't be satisfied by an unrelated part of the surrounding
    # query.  Comment-stripped, because the CTE's own comment spells out the
    # collapse shapes this test forbids.
    start = sql.index("strikes AS (")
    end = sql.index(") sx", start)
    strikes_cte = sql[start:end]

    # Gamma (and OI) are SUMmed across the admitted expirations.
    assert "SUM(COALESCE(gbs.call_gamma, 0))" in strikes_cte
    assert "SUM(COALESCE(gbs.put_gamma, 0))" in strikes_cte
    assert "SUM(COALESCE(gbs.call_oi, 0))" in strikes_cte
    assert "SUM(COALESCE(gbs.put_oi, 0))" in strikes_cte

    # Grouped by strike ONLY, inside a lateral that runs at a single rep_ts:
    # expiration is summed OVER, never a grouping key, so one output row per
    # (bucket, strike) carries every selected expiration's gamma.  Grouping by
    # strike within one rep_ts is identical to grouping by (bucket_ts, strike)
    # — bucket_reps is DISTINCT ON the bucket, so bucket_ts <-> rep_ts is 1:1.
    # (``expiration`` still appears earlier in the lateral's WHERE — that's the
    # set predicate, not a grouping key — so this check is scoped to the text
    # after GROUP BY.)
    assert "GROUP BY gbs.strike" in strikes_cte
    assert "expiration" not in strikes_cte.split("GROUP BY")[1]

    # No collapse: the strikes CTE must not reduce to a single row per strike
    # via a DISTINCT ON / latest-expiration ordering.  Either would keep only
    # one expiration per strike and reintroduce the shrink-on-add regression.
    assert "DISTINCT ON" not in strikes_cte
    assert "ORDER BY" not in strikes_cte
    assert "AVG(" not in strikes_cte


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
# Gamma-flip scope follows the expiration filter
# ---------------------------------------------------------------------------


def _flip_rows():
    """Three strikes around spot 100 whose cumulative net gamma
    (call_gamma_raw - put_gamma_raw, ascending) crosses zero once:

        95  -> net -40, cum -40
        100 -> net -10, cum -50
        105 -> net +80, cum +30   (crossing between 100 and 105)

    Crossing price = 100 + 5 * 50 / 80 = 103.125.  ``gamma_flip`` on every
    row is a deliberately-different aggregate value (510) so the test can
    tell "kept persisted" from "recomputed from strikes" apart.
    """
    rows = []
    for strike, cg, pg in ((95.0, 10.0, 50.0), (100.0, 30.0, 40.0), (105.0, 90.0, 10.0)):
        rows.append(
            {
                "timestamp": "2026-06-08T14:30:00+00:00",
                "open": 100.0,
                "high": 100.0,
                "low": 100.0,
                "close": 100.0,
                "gamma_flip": 510.0,
                "strike": strike,
                "call_gamma_raw": cg,
                "put_gamma_raw": pg,
                "call_gex": cg,
                "put_gex": -pg,
                "net_gex": cg - pg,
                "call_oi": 0,
                "put_oi": 0,
            }
        )
    return rows


def test_flip_scoped_to_subset_recomputed_from_strikes():
    """A specific expiration set recomputes the bucket flip from the
    summed-by-strike gamma (cumulative net-GEX zero crossing) against the
    bucket's own close — the aggregate spot-shift flip can't be rebuilt for
    a subset, so the persisted value must NOT leak through."""
    bucket = _run("SPY", expirations=[date(2026, 6, 19)], rows=_flip_rows())["result"][0]
    assert bucket["gamma_flip"] == pytest.approx(103.125)


def test_flip_all_keeps_persisted_aggregate():
    """``expirations=None`` (All) keeps the canonical persisted
    ``gex_summary.gamma_flip_point`` verbatim so the chart's "All" flip
    stays in parity with /api/gex/summary and the headline metric — no
    regression from the subset path."""
    bucket = _run("SPY", expirations=None, rows=_flip_rows())["result"][0]
    assert bucket["gamma_flip"] == 510.0


def test_flip_scoped_subset_null_when_curve_one_signed():
    """A subset whose cumulative net-GEX curve never crosses zero (one-signed
    book) yields a NULL flip rather than falling back to the aggregate — the
    chart draws no flip line in that case."""
    rows = []
    for strike, cg, pg in ((95.0, 50.0, 0.0), (100.0, 60.0, 0.0), (105.0, 90.0, 0.0)):
        rows.append(
            {
                "timestamp": "2026-06-08T14:30:00+00:00",
                "open": 100.0,
                "high": 100.0,
                "low": 100.0,
                "close": 100.0,
                "gamma_flip": 510.0,
                "strike": strike,
                "call_gamma_raw": cg,
                "put_gamma_raw": pg,
                "call_gex": cg,
                "put_gex": -pg,
                "net_gex": cg - pg,
                "call_oi": 0,
                "put_oi": 0,
            }
        )
    bucket = _run("SPY", expirations=[date(2026, 6, 19)], rows=rows)["result"][0]
    assert bucket["gamma_flip"] is None


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
    # Index rather than unpack so the assertion does not depend on the cache
    # entry's internal arity (expiry, payload, and a size-accounting field).
    expires_at = db._read_cache[matching_keys[0]][0]
    assert db._strike_profile_timeseries_cache_ttl_seconds > db._analytics_cache_ttl_seconds

    # Second call hits the cache, no extra DB round-trip.
    asyncio.run(db.get_strike_profile_timeseries("SPY", "1min", 78, None))
    assert len(conn.queries) == 1
