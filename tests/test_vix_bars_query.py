"""Unit test for DatabaseManager.get_vix_bars shaping.

Covers the async (asyncpg) VIX-bar read that replaced the volatility-gauge
router's sync psycopg2 + run_in_executor path: ascending order preserved,
timestamps normalized to the requested tz, NULL-close rows dropped, OHLC
floats (with None passthrough for non-close fields).
"""

from datetime import datetime, timezone

import pytz
import pytest

from src.api.database import DatabaseManager

ET = pytz.timezone("US/Eastern")


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, *args, **kwargs):
        return self._rows


class _FakeAcquire:
    def __init__(self, rows):
        self._rows = rows

    async def __aenter__(self):
        return _FakeConn(self._rows)

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_get_vix_bars_shapes_and_normalizes(monkeypatch):
    db = DatabaseManager()
    # TIMESTAMPTZ (UTC) rows as asyncpg would return them, ascending.
    t0 = datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 18, 14, 35, tzinfo=timezone.utc)
    rows = [
        {"timestamp": t0, "open": 18.0, "high": 18.5, "low": 17.8, "close": 18.2},
        {"timestamp": t1, "open": None, "high": None, "low": None, "close": 18.4},
    ]
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire(rows))

    bars = await db.get_vix_bars(t0, ET)

    assert [b["close"] for b in bars] == [18.2, 18.4]  # ascending preserved
    # Timestamps normalized to ET (UTC 14:30 -> 10:30 ET on 2026-05-18).
    assert bars[0]["timestamp"].tzinfo is not None
    assert bars[0]["timestamp"].hour == 10 and bars[0]["timestamp"].minute == 30
    # Non-close OHLC pass through as float or None; close always float.
    assert bars[0]["open"] == 18.0 and bars[0]["high"] == 18.5
    assert bars[1]["open"] is None and bars[1]["low"] is None
    assert isinstance(bars[1]["close"], float)


@pytest.mark.asyncio
async def test_get_vix_bars_drops_null_close(monkeypatch):
    db = DatabaseManager()
    t0 = datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc)
    rows = [
        {"timestamp": t0, "open": 18.0, "high": 18.5, "low": 17.8, "close": None},
        {"timestamp": t0, "open": 18.0, "high": 18.5, "low": 17.8, "close": 18.2},
    ]
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire(rows))

    bars = await db.get_vix_bars(t0, ET)
    assert len(bars) == 1 and bars[0]["close"] == 18.2  # NULL-close row dropped
