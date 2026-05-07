"""Tests for volume-spike proxy substitution for cash indices.

Cash indices (SPX, NDX, RUT, DJX) carry no transactional volume of their
own, so the canonical ``unusual_volume_spikes`` view stops emitting fresh
rows for them once TradeStation's synthetic index volume drops.
``DatabaseManager.get_unusual_volume_spikes`` must route those symbols
through the proxy-based detector (mirroring ``vwap-deviation``) so the
``/api/technicals/volume-spikes`` endpoint keeps producing data for
indices.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager


class _RecordingConn:
    """Minimal asyncpg-connection stand-in that captures fetch args."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.last_query = None
        self.last_args = None
        self.calls = 0

    async def fetch(self, query, *args):
        self.calls += 1
        self.last_query = query
        self.last_args = args
        return self.rows


def _make_db_with_conn(conn: _RecordingConn) -> DatabaseManager:
    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


class TestVolumeSpikeProxyForCashIndices:
    """SPX/NDX/RUT/DJX should route through the proxy-based detector."""

    def test_spx_uses_spy_volume_proxy(self):
        conn = _RecordingConn(rows=[])
        db = _make_db_with_conn(conn)

        asyncio.run(db.get_unusual_volume_spikes("SPX", limit=20))

        assert conn.last_query is not None
        # Proxy path joins underlying_quotes with itself; the canonical
        # view name should NOT appear.
        assert "unusual_volume_spikes" not in conn.last_query
        assert "index_quotes" in conn.last_query
        assert "proxy_volume" in conn.last_query
        # SPX is the index, SPY is the configured volume proxy.
        assert "SPX" in conn.last_args
        assert "SPY" in conn.last_args

    def test_ndx_uses_qqq_volume_proxy(self):
        conn = _RecordingConn(rows=[])
        db = _make_db_with_conn(conn)

        asyncio.run(db.get_unusual_volume_spikes("NDX", limit=20))

        assert "NDX" in conn.last_args
        assert "QQQ" in conn.last_args

    def test_proxy_response_tags_volume_proxy_field(self):
        canned_row = {
            "time_et": "2026-05-07T15:59:00",
            "timestamp": "2026-05-07T19:59:00+00:00",
            "symbol": "SPX",
            "price": 7100.0,
            "current_volume": 1_500_000,
            "avg_volume": 250_000.0,
            "volume_sigma": 5.2,
            "volume_ratio": 6.0,
            "buying_pressure_pct": 60.0,
            "volume_class": "🚨 Extreme Spike",
        }
        conn = _RecordingConn(rows=[canned_row])
        db = _make_db_with_conn(conn)

        rows = asyncio.run(db.get_unusual_volume_spikes("SPX", limit=20))

        assert len(rows) == 1
        assert rows[0]["volume_proxy"] == "SPY"
        # Original fields survive untouched.
        assert rows[0]["symbol"] == "SPX"
        assert rows[0]["current_volume"] == 1_500_000


class TestVolumeSpikeNoProxyForEquitiesAndEtfs:
    """SPY/QQQ/AAPL should keep using the canonical view directly."""

    def test_spy_uses_canonical_view(self):
        conn = _RecordingConn(rows=[])
        db = _make_db_with_conn(conn)

        asyncio.run(db.get_unusual_volume_spikes("SPY", limit=20))

        assert conn.last_query is not None
        assert "unusual_volume_spikes" in conn.last_query
        assert "index_quotes" not in conn.last_query
        assert "proxy_volume" not in conn.last_query
        # Canonical path passes (symbol, limit) — no proxy positional arg.
        assert conn.last_args == ("SPY", 20)

    def test_aapl_uses_canonical_view(self):
        conn = _RecordingConn(rows=[])
        db = _make_db_with_conn(conn)

        asyncio.run(db.get_unusual_volume_spikes("AAPL", limit=10))

        assert "unusual_volume_spikes" in conn.last_query
        assert conn.last_args == ("AAPL", 10)

    def test_canonical_response_has_no_volume_proxy_field(self):
        canned_row = {
            "time_et": "2026-05-07T15:59:00",
            "timestamp": "2026-05-07T19:59:00+00:00",
            "symbol": "SPY",
            "price": 510.0,
            "current_volume": 1_200_000,
            "avg_volume": 300_000.0,
            "volume_sigma": 4.0,
            "volume_ratio": 4.0,
            "buying_pressure_pct": 55.0,
            "volume_class": "🔥 Strong Spike",
        }
        conn = _RecordingConn(rows=[canned_row])
        db = _make_db_with_conn(conn)

        rows = asyncio.run(db.get_unusual_volume_spikes("SPY", limit=20))

        assert len(rows) == 1
        assert "volume_proxy" not in rows[0]
