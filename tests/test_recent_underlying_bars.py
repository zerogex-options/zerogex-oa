"""Unit test for DatabaseManager.get_recent_underlying_bars shaping.

Guards the async API playbook path (context_builder) getting the same bar
history the sync UnifiedSignalEngine uses: DESC query reversed to chronological
oldest → newest, with low/high falling back to close on pre-backfill rows so the
three lists stay aligned.
"""

import pytest

from src.api.database import DatabaseManager


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
async def test_get_recent_underlying_bars_reverses_and_fills(monkeypatch):
    db = DatabaseManager()
    # As returned by the DESC query (newest first); middle row is pre-backfill
    # with NULL low/high.
    rows = [
        {"low": 11.0, "high": 13.0, "close": 12.0},
        {"low": None, "high": None, "close": 10.0},
        {"low": 8.0, "high": 9.5, "close": 9.0},
    ]
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire(rows))

    closes, lows, highs = await db.get_recent_underlying_bars("SPY")

    assert closes == [9.0, 10.0, 12.0]  # reversed to oldest → newest
    assert lows == [8.0, 10.0, 11.0]  # NULL low falls back to close (10.0)
    assert highs == [9.5, 10.0, 13.0]  # NULL high falls back to close (10.0)


@pytest.mark.asyncio
async def test_get_recent_underlying_bars_fails_closed(monkeypatch):
    db = DatabaseManager()

    def _boom():
        raise RuntimeError("pool down")

    monkeypatch.setattr(db, "_acquire_connection", _boom)
    # Errors degrade to empty lists (= prior empty-bar fallback), never raise.
    assert await db.get_recent_underlying_bars("SPY") == ([], [], [])
