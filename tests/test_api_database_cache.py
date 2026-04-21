import asyncio
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager, _get_session_bounds


class _FakeConn:
    def __init__(self, row):
        self.row = row
        self.calls = 0

    async def fetchrow(self, _query, _symbol):
        self.calls += 1
        return self.row

    async def fetch(self, _query, *_args):
        self.calls += 1
        return [self.row] if self.row is not None else []


class _FakeFlowConn:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0
        self.last_query = None

    async def fetch(self, *args):
        self.calls += 1
        if args:
            self.last_query = args[0]
        return self.rows


def test_get_latest_quote_uses_short_ttl_cache():
    db = DatabaseManager()
    db._latest_quote_cache_ttl_seconds = 60.0
    conn = _FakeConn({"symbol": "SPY", "close": 500.0})

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    first = asyncio.run(db.get_latest_quote("spy"))
    second = asyncio.run(db.get_latest_quote("SPY"))

    assert first == second
    assert conn.calls == 1


def test_get_latest_gex_summary_cache_expires():
    db = DatabaseManager()
    db._latest_gex_summary_cache_ttl_seconds = 0.01
    conn = _FakeConn({"symbol": "SPY", "net_gex": 123.0})

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    first = asyncio.run(db.get_latest_gex_summary("SPY"))
    assert first is not None

    # Allow the cache entry to expire.
    asyncio.run(asyncio.sleep(0.02))
    second = asyncio.run(db.get_latest_gex_summary("SPY"))

    assert second == first
    assert conn.calls == 2


def test_get_latest_signal_score_enriched_includes_intraday_score():
    db = DatabaseManager()
    conn = _FakeConn(
        {
            "underlying": "SPY",
            "timestamp": "2026-01-01T15:30:00Z",
            "composite_score": 0.2,
            "normalized_score": 0.2,
            "direction": "bullish",
            "components": {},
            "intraday_score": 72.5,
        }
    )

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    async def _empty_calibration_history(*_args, **_kwargs):
        return []

    db._get_signal_calibration_history = _empty_calibration_history  # type: ignore[method-assign]

    row = asyncio.run(db.get_latest_signal_score_enriched("SPY"))
    assert row is not None
    assert row["intraday_score"] == 72.5


def test_get_signal_score_history_includes_intraday_score():
    db = DatabaseManager()
    conn = _FakeConn(
        {
            "underlying": "SPY",
            "timestamp": "2026-01-01T15:30:00Z",
            "composite_score": 0.2,
            "normalized_score": 0.2,
            "direction": "bullish",
            "components": {},
            "intraday_score": 68.0,
        }
    )

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    rows = asyncio.run(db.get_signal_score_history("SPY", 5))
    assert len(rows) == 1
    assert rows[0]["intraday_score"] == 68.0


def test_get_flow_by_type_uses_cache():
    db = DatabaseManager()
    db._flow_endpoint_cache_ttl_seconds = 60.0
    conn = _FakeFlowConn([{"timestamp": "2026-01-01T09:30:00Z", "symbol": "SPY"}])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    first = asyncio.run(db.get_flow_by_type("spy", "current"))
    second = asyncio.run(db.get_flow_by_type("SPY", "current"))

    assert first == second
    assert conn.calls == 1


def test_get_flow_by_strike_cache_expires():
    db = DatabaseManager()
    db._flow_endpoint_cache_ttl_seconds = 0.01
    conn = _FakeFlowConn([{"timestamp": "2026-01-01T09:30:00Z", "symbol": "SPY", "strike": 500.0}])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    first = asyncio.run(db.get_flow_by_strike("SPY", "current"))
    assert first

    asyncio.run(asyncio.sleep(0.02))
    second = asyncio.run(db.get_flow_by_strike("SPY", "current"))

    assert second == first
    assert conn.calls == 2


def test_get_flow_by_strike_query_uses_dense_buckets():
    db = DatabaseManager()
    conn = _FakeFlowConn([])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    asyncio.run(db.get_flow_by_strike("SPY", "current"))

    assert conn.last_query is not None
    assert "generate_series(" in conn.last_query
    assert "CROSS JOIN strikes" in conn.last_query
    assert "underlying_by_bucket" in conn.last_query
    assert "underlying_dense" in conn.last_query
    assert "FROM dense" in conn.last_query


def test_get_flow_by_expiration_query_uses_dense_buckets():
    db = DatabaseManager()
    conn = _FakeFlowConn([])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    asyncio.run(db.get_flow_by_expiration("SPY", "current"))

    assert conn.last_query is not None
    assert "generate_series(" in conn.last_query
    assert "CROSS JOIN expirations" in conn.last_query
    assert "underlying_by_bucket" in conn.last_query
    assert "underlying_dense" in conn.last_query
    assert "FROM dense" in conn.last_query


def test_prior_session_bounds_end_at_1615_et():
    _start, end = _get_session_bounds("prior")
    assert end.hour == 16
    assert end.minute == 15
