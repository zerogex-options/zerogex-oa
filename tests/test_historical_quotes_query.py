"""Regression tests for the /api/market/historical query.

This endpoint drives the OHLC candle layer in every price chart on the
site (Strike Profile, GEX heatmap, Max Pain timeseries, …) via the
``useMarketHistorical`` cache on the web frontend.

The contract these tests pin is:

  * ``window_units`` means "N most recent buckets that have data",
    NOT "N step_interval-units of wall-clock time".  The old wall-clock
    floor under-filled cash-index charts on the 5-min interval (48-hour
    lookback from a Monday afternoon landed between Friday's close and
    Monday's open — the entire prior session was just out of reach
    even though plenty of bucket-eligible data existed further back),
    while leaving 1-min and 15-min looking fine.  The bucket-aware
    floor gives the same "N available bars" semantics to every symbol
    and every timeframe.

  * ``start_date`` / ``end_date`` overrides still work — when the
    caller pins an explicit lower bound the bucket-floor subquery is
    skipped (COALESCE prefers the caller's value).

  * Symbol bound as $1, start_date $2, end_date $3, window_units $4 —
    the param shape ``useMarketHistorical`` and the FastAPI handler
    both rely on.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from src.api.database import DatabaseManager


class _RecordingConn:
    def __init__(self, fetch_rows=None):
        self._fetch_rows = fetch_rows or []
        self.queries = []
        self.args = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        self.args.append(args)
        return list(self._fetch_rows)


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def _run(symbol, *, start_date=None, end_date=None, window_units=192, timeframe="1min"):
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_historical_quotes(symbol, start_date, end_date, window_units, timeframe))
    return {"query": conn.queries[0], "args": conn.args[0]}


# ---------------------------------------------------------------------------
# Bucket-aware window floor (the symbol-agnostic fix for sparse charts)
# ---------------------------------------------------------------------------


def test_bounds_use_bucket_floor_not_wall_clock_subtraction():
    """The window's lower bound must be derived from the Nth most recent
    bucket that has data, not from ``max_ts - step_interval * (N - 1)``.
    The wall-clock floor breaks on any feed with gaps the lookback must
    cross (worst case: cash-index RTH-only data on the 5-min interval on
    a Monday afternoon — the 48-hour wall-clock window misses Friday's
    entire session).  Pinning the new shape so future refactors don't
    silently revert to the sparsity-prone wall-clock arithmetic.
    """
    sql = _run("SPY", timeframe="5min")["query"]

    # The wall-clock floor is gone everywhere.  If a future edit
    # re-introduces ``max_ts - INTERVAL * ($4 - 1)`` (or its 5-minute /
    # 15-minute variants), this fails — and the SPX 5-min sparsity bug
    # is back.
    assert "max_ts - (INTERVAL" not in sql
    assert "max_ts - INTERVAL" not in sql

    # The bucket-floor subquery sits inside ``bounds`` and selects the
    # MIN of the N most recent distinct bucket timestamps.
    bounds_idx = sql.index("bounds AS")
    base_idx = sql.index("base AS")
    bounds_block = sql[bounds_idx:base_idx]
    assert "(SELECT MIN(bucket_ts) FROM (" in bounds_block
    assert "SELECT DISTINCT" in bounds_block
    assert "AS bucket_ts" in bounds_block
    assert "ORDER BY bucket_ts DESC" in bounds_block
    assert "LIMIT $4" in bounds_block


def test_bucket_floor_uses_underlying_quotes_for_the_same_symbol():
    """The bucket-floor subquery counts buckets in the SAME source the
    main aggregation reads (``underlying_quotes`` for the same symbol),
    so the count agrees with what the response will surface."""
    sql = _run("SPY")["query"]
    bounds_idx = sql.index("bounds AS")
    base_idx = sql.index("base AS")
    bounds_block = sql[bounds_idx:base_idx]
    assert "FROM underlying_quotes" in bounds_block
    assert "WHERE symbol = $1" in bounds_block


def test_start_date_override_short_circuits_bucket_floor():
    """When the caller pins ``start_date`` explicitly, the COALESCE
    upstream of the bucket-floor takes the caller's value first — so
    the subquery is logically skipped and behaviour matches the
    historical contract for explicit date-range requests."""
    sql = _run("SPY", start_date=datetime(2026, 6, 1, tzinfo=timezone.utc))["query"]
    bounds_idx = sql.index("bounds AS")
    base_idx = sql.index("base AS")
    bounds_block = sql[bounds_idx:base_idx]

    # COALESCE picks $2::timestamptz when the caller passed start_date,
    # so the bucket-floor subquery becomes dead branch but still emits
    # the same fixed-shape SQL (one query plan for both modes).
    assert "COALESCE($2::timestamptz" in bounds_block


def test_end_date_override_caps_the_bucket_floor_lookback():
    """When ``end_date`` is provided, the bucket-floor subquery must
    only consider buckets at or before that date — otherwise the
    floor could land later than the caller's end bound."""
    sql = _run("SPY", end_date=datetime(2026, 6, 1, tzinfo=timezone.utc))["query"]
    bounds_idx = sql.index("bounds AS")
    base_idx = sql.index("base AS")
    bounds_block = sql[bounds_idx:base_idx]

    # The subquery's ``timestamp <=`` expression has to COALESCE through
    # the same end-date parameter so the upper bound on the bucket-floor
    # walk matches the upper bound on the final aggregation.
    assert "AND timestamp <= COALESCE($3::timestamptz" in bounds_block


# ---------------------------------------------------------------------------
# Param shape — frontend (useMarketHistorical) and FastAPI handler depend on it
# ---------------------------------------------------------------------------


def test_param_shape_is_symbol_start_end_window():
    """The handler at /api/market/historical binds (symbol, start_date,
    end_date, window_units) and the frontend cache requests
    ``window_units=576``.  Cash-index detection happens inside the query
    via the underlying data shape, NOT via a 5th bind parameter — so
    the symbol is the only ticker-specific input."""
    captured = _run("SPY", window_units=576)
    assert captured["args"] == ("SPY", None, None, 576)


def test_window_units_is_clamped_to_safe_upper_bound():
    """A rogue caller passing window_units=10_000 would tip the bucket-
    floor subquery into a very wide scan.  Clamp to 576 — that's already
    9.6h at 1-min, 48h at 5-min, ~144h (6d) at 15-min.  Below 1 we clamp
    up to 1 so the LIMIT clause stays well-formed."""
    captured = _run("SPY", window_units=10_000)
    assert captured["args"][3] == 576

    captured_low = _run("SPY", window_units=0)
    assert captured_low["args"][3] == 1


# ---------------------------------------------------------------------------
# Bucket / interval consistency
# ---------------------------------------------------------------------------


def test_bucket_expression_matches_timeframe():
    """The bucket-floor subquery and the main aggregation must group on
    the same bucket expression — otherwise the floor would slice the
    timeline differently than the aggregation and the LIMIT could trim
    the wrong rows."""
    sql_5 = _run("SPY", timeframe="5min")["query"]
    sql_15 = _run("SPY", timeframe="15min")["query"]

    # 5-min buckets floor on ``FLOOR(... / 5)`` — three times: once in
    # the bucket-floor subquery, once in each of the two ROW_NUMBER
    # window specs that pick first-open / last-close per bucket.
    assert sql_5.count("FLOOR(EXTRACT(MINUTE FROM timestamp) / 5)") >= 3
    # 15-min uses the /15 variant.
    assert sql_15.count("FLOOR(EXTRACT(MINUTE FROM timestamp) / 15)") >= 3
