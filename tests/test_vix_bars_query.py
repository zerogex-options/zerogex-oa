"""Unit tests for DatabaseManager.get_volatility_index_bars.

Covers the async (asyncpg) bar reads behind /api/market/volatility for
both VIX and VXN: ascending order preserved, timestamps normalized to
the requested tz, NULL-close rows dropped, OHLC floats (with None
passthrough for non-close fields), per-ticker table dispatch, and the
allowlist that protects the table-name SQL interpolation.
"""

from datetime import datetime, timezone

import pytz
import pytest

from src.api.database import DatabaseManager

ET = pytz.timezone("US/Eastern")


class _FakeConn:
    def __init__(self, rows, captured_queries=None):
        self._rows = rows
        self._captured = captured_queries

    async def fetch(self, query, *args, **kwargs):
        if self._captured is not None:
            self._captured.append(query)
        return self._rows


class _FakeAcquire:
    def __init__(self, rows, captured_queries=None):
        self._rows = rows
        self._captured = captured_queries

    async def __aenter__(self):
        return _FakeConn(self._rows, self._captured)

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_get_volatility_index_bars_shapes_and_normalizes(monkeypatch):
    db = DatabaseManager()
    # TIMESTAMPTZ (UTC) rows as asyncpg would return them, ascending.
    t0 = datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 18, 14, 35, tzinfo=timezone.utc)
    rows = [
        {"timestamp": t0, "open": 18.0, "high": 18.5, "low": 17.8, "close": 18.2},
        {"timestamp": t1, "open": None, "high": None, "low": None, "close": 18.4},
    ]
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire(rows))

    bars = await db.get_volatility_index_bars("VIX", t0, ET)

    assert [b["close"] for b in bars] == [18.2, 18.4]  # ascending preserved
    # Timestamps normalized to ET (UTC 14:30 -> 10:30 ET on 2026-05-18).
    assert bars[0]["timestamp"].tzinfo is not None
    assert bars[0]["timestamp"].hour == 10 and bars[0]["timestamp"].minute == 30
    # Non-close OHLC pass through as float or None; close always float.
    assert bars[0]["open"] == 18.0 and bars[0]["high"] == 18.5
    assert bars[1]["open"] is None and bars[1]["low"] is None
    assert isinstance(bars[1]["close"], float)


@pytest.mark.asyncio
async def test_get_volatility_index_bars_drops_null_close(monkeypatch):
    db = DatabaseManager()
    t0 = datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc)
    rows = [
        {"timestamp": t0, "open": 18.0, "high": 18.5, "low": 17.8, "close": None},
        {"timestamp": t0, "open": 18.0, "high": 18.5, "low": 17.8, "close": 18.2},
    ]
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire(rows))

    bars = await db.get_volatility_index_bars("VIX", t0, ET)
    assert len(bars) == 1 and bars[0]["close"] == 18.2  # NULL-close row dropped


@pytest.mark.asyncio
async def test_get_volatility_index_bars_vix_targets_vix_table(monkeypatch):
    """ticker="VIX" must hit ``vix_bars`` (not vxn_bars)."""
    db = DatabaseManager()
    t0 = datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc)
    rows = [{"timestamp": t0, "open": 18.0, "high": 18.5, "low": 17.8, "close": 18.2}]
    captured: list[str] = []
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire(rows, captured))

    await db.get_volatility_index_bars("VIX", t0, ET)

    assert len(captured) == 1
    assert "FROM vix_bars" in captured[0]
    assert "FROM vxn_bars" not in captured[0]


@pytest.mark.asyncio
async def test_get_volatility_index_bars_vxn_targets_vxn_table(monkeypatch):
    """ticker="VXN" must hit ``vxn_bars`` (not vix_bars)."""
    db = DatabaseManager()
    t0 = datetime(2026, 6, 18, 14, 30, tzinfo=timezone.utc)
    rows = [{"timestamp": t0, "open": 26.5, "high": 26.7, "low": 26.3, "close": 26.4}]
    captured: list[str] = []
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire(rows, captured))

    bars = await db.get_volatility_index_bars("VXN", t0, ET)

    assert len(bars) == 1 and bars[0]["close"] == 26.4
    assert len(captured) == 1
    assert "FROM vxn_bars" in captured[0]
    assert "FROM vix_bars" not in captured[0]


@pytest.mark.asyncio
async def test_get_volatility_index_bars_rejects_unknown_ticker():
    """Tickers outside the allowlist must raise before any DB access."""
    db = DatabaseManager()
    assert "VIX" in DatabaseManager._VOLATILITY_INDEX_TABLES
    assert "VXN" in DatabaseManager._VOLATILITY_INDEX_TABLES

    with pytest.raises(ValueError, match="Unsupported volatility-index ticker"):
        await db.get_volatility_index_bars("RVX", datetime(2026, 6, 18, tzinfo=timezone.utc), ET)
