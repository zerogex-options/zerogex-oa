import asyncio
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager


class _FakeConn:
    def __init__(self, row):
        self.row = row
        self.calls = 0

    async def fetchrow(self, _query, _symbol):
        self.calls += 1
        return self.row


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
