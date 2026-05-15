"""Regression tests for the /api/gex/heatmap query shape and behavior.

Context: the heatmap is a strike×time grid that AVGs net_gex per
(bucket, strike).  The spot±50 strike band MUST be filtered inside the
aggregation CTE (before GROUP BY), not after it.  Filtering after the
GROUP BY forced the aggregate to process the entire option chain for
every snapshot in the window -- for timeframe=1day (where window_units=N
spans N days of the highest-cardinality table) that meant scanning and
aggregating millions of rows that were then discarded, producing a
~14s response while every other timeframe was sub-20ms.

Pre- vs post-aggregation strike filtering is provably equivalent (AVG is
per (bucket, strike)), so these tests pin the faster shape so a refactor
can't silently reintroduce the post-aggregation filter.
"""

import asyncio
from contextlib import asynccontextmanager

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


def test_heatmap_filters_strike_band_before_group_by():
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)

    asyncio.run(db.get_gex_heatmap("SPY", "1day", 10))

    assert conn.queries, "heatmap query was never executed"
    sql = conn.queries[0]

    # The strike band predicate must exist and sit BEFORE the GROUP BY,
    # i.e. inside recent_data, so the aggregate only touches near-spot
    # strikes.
    strike_pred = "ABS(strike - (SELECT spot_close FROM latest_quote)) <= 50"
    assert strike_pred in sql
    assert "GROUP BY 1, strike" in sql
    assert sql.index(strike_pred) < sql.index(
        "GROUP BY 1, strike"
    ), "strike band must be filtered before the GROUP BY, not after it"

    # The post-aggregation filter CTE must be gone (it was the bug).
    assert "filtered_data" not in sql

    # The two redundant latest-underlying_quotes scans are folded into one.
    assert "latest_quote AS" in sql
    assert "latest_price_timestamp AS" not in sql
    assert "latest_price AS" not in sql


def test_heatmap_returns_mapped_rows_newest_first_contract():
    db = DatabaseManager()
    rows = [
        {"timestamp": "2026-05-15T20:00:00Z", "strike": 585.0, "net_gex": 1.2e9},
        {"timestamp": "2026-05-15T20:00:00Z", "strike": 586.0, "net_gex": -3.4e8},
    ]
    conn = _RecordingConn(fetch_rows=rows)
    _install_conn(db, conn)

    result = asyncio.run(db.get_gex_heatmap("spy", "5min", 60))
    assert result == rows
    # symbol upper-cased, passed as the first bind param.
    # window_units passed as the second bind param.


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


def test_heatmap_orders_by_timestamp_desc_then_strike_asc():
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_heatmap("SPY", "1hr", 24))
    assert "ORDER BY timestamp DESC, strike ASC" in conn.queries[0]
