"""Regression tests for the cash-index session filter on
``get_historical_gex`` (/api/gex/historical) and
``get_max_pain_timeseries`` (/api/max-pain/timeseries).

Both endpoints feed cash-index charts (gamma-flip overlay and max-pain
timeseries respectively). Their data source is ``gex_summary``, which
is written around the clock because SPX/NDX/RUT options trade extended
hours.  Without a session filter the cash-index charts would surface
overnight rows that have no candlestick to align with — and would also
drift past the heatmap's right edge (the heatmap clamps via the same
filter in ``get_gex_heatmap``).

These tests pin that:
  * for cash indices the filter is applied to BOTH the window-anchor
    CTE (``latest``) and the bucket-selection CTE (``bucketed`` /
    ``ranked``);
  * for ETFs (SPY, QQQ, AAPL …) the filter is absent and param shape is
    unchanged so existing callers don't see a new bound parameter.
"""

import asyncio
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager


class _RecordingConn:
    """Captures queries + args and returns canned rows."""

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


# ---------------------------------------------------------------------------
# /api/gex/historical
# ---------------------------------------------------------------------------


def _run_historical(symbol, timeframe="5min", window_units=60):
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_historical_gex(symbol, None, None, window_units, timeframe))
    return {"query": conn.queries[0], "args": conn.args[0]}


def test_historical_etf_has_no_session_filter():
    """ETFs / equities (SPY, QQQ, AAPL) trade extended hours legitimately,
    so the query and its bound params stay exactly as before — no session
    predicate anywhere, four args only (symbol, start_date, end_date,
    window_units)."""
    for sym in ("SPY", "QQQ", "AAPL"):
        captured = _run_historical(sym)
        sql = captured["query"]

        assert "America/New_York" not in sql, sym
        assert "EXTRACT(DOW" not in sql, sym
        assert "$5" not in sql, sym
        # Unchanged param shape.
        assert len(captured["args"]) == 4, sym
        assert captured["args"][0] == sym


def test_historical_cash_index_restricts_to_regular_session():
    """SPX (and NDX, RUT) must restrict both CTEs that scan gex_summary
    to weekdays 09:30–16:00 ET, excluding NYSE holidays."""
    captured = _run_historical("SPX")
    sql = captured["query"]

    # Weekday + 09:30–16:00 ET band, mirroring the heatmap query.
    assert "EXTRACT(DOW FROM" in sql
    assert "America/New_York" in sql
    assert "BETWEEN TIME '09:30' AND TIME '16:00'" in sql
    # NYSE holidays bound as the 5th param.
    assert "<> ALL($5::date[])" in sql

    # Filter is applied in BOTH gex_summary scans — the anchor in
    # ``latest`` AND the per-bucket selection in ``bucketed``.
    assert sql.count("EXTRACT(DOW") == 2
    extract_positions = [i for i in range(len(sql)) if sql.startswith("EXTRACT(DOW", i)]
    bucketed_idx = sql.index("bucketed AS")
    # First occurrence is inside ``latest`` (before ``bucketed``).
    assert extract_positions[0] < bucketed_idx
    # Second occurrence is inside ``bucketed`` (after that marker).
    assert extract_positions[1] > bucketed_idx
    # Neither extends into the gex_by_strike joins (strike_agg / call_walls
    # / put_walls inherit the RTH-only timestamp set transitively via
    # ``base``).
    strike_agg_idx = sql.index("strike_agg AS")
    assert all(idx < strike_agg_idx for idx in extract_positions)

    # The ``bucketed`` CTE aliases gex_summary as ``gs`` — its session
    # predicate must reference ``gs.timestamp``, not the bare column.
    bucketed_block = sql[
        bucketed_idx : (
            sql.index(") ,", bucketed_idx)
            if ") ," in sql[bucketed_idx:]
            else sql.index("base AS", bucketed_idx)
        )
    ]
    assert (
        "gs.timestamp AT TIME ZONE" in bucketed_block
    ), "session filter inside bucketed CTE must be qualified with the gs alias"

    # Param shape: symbol, start_date, end_date, window_units, holidays[].
    assert len(captured["args"]) == 5
    assert captured["args"][0] == "SPX"
    assert captured["args"][1] is None  # start_date
    assert captured["args"][2] is None  # end_date
    assert captured["args"][3] == 60  # window_units
    assert isinstance(captured["args"][4], list)


def test_historical_cash_index_detection_is_case_insensitive():
    """Lowercased index symbols still get the session filter — the
    is_cash_index() helper normalizes case before lookup."""
    sql = _run_historical("spx")["query"]
    assert "America/New_York" in sql
    assert "EXTRACT(DOW" in sql


# ---------------------------------------------------------------------------
# /api/max-pain/timeseries
# ---------------------------------------------------------------------------


def _run_max_pain(symbol, timeframe="5min", window_units=60):
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_max_pain_timeseries(symbol, timeframe, window_units))
    return {"query": conn.queries[0], "args": conn.args[0]}


def test_max_pain_etf_has_no_session_filter():
    """ETF max-pain timeseries unchanged — no session predicate, two args."""
    for sym in ("SPY", "QQQ", "AAPL"):
        captured = _run_max_pain(sym)
        sql = captured["query"]

        assert "America/New_York" not in sql, sym
        assert "EXTRACT(DOW" not in sql, sym
        assert "$3" not in sql, sym
        assert len(captured["args"]) == 2, sym
        assert captured["args"] == (sym, 60), sym


def test_max_pain_cash_index_restricts_to_regular_session():
    """SPX max-pain timeseries clamps to RTH so the right edge of the
    chart aligns with the heatmap and the candlesticks."""
    captured = _run_max_pain("SPX")
    sql = captured["query"]

    assert "EXTRACT(DOW FROM" in sql
    assert "America/New_York" in sql
    assert "BETWEEN TIME '09:30' AND TIME '16:00'" in sql
    assert "<> ALL($3::date[])" in sql

    # Filter applied in BOTH gex_summary scans (latest + ranked) — same
    # pattern as the heatmap.
    assert sql.count("EXTRACT(DOW") == 2

    # Param shape: symbol, window_units, holidays[].
    assert len(captured["args"]) == 3
    assert captured["args"][0] == "SPX"
    assert captured["args"][1] == 60
    assert isinstance(captured["args"][2], list)


def test_max_pain_cash_index_detection_is_case_insensitive():
    sql = _run_max_pain("ndx")["query"]
    assert "America/New_York" in sql
