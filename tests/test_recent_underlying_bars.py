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


@pytest.mark.asyncio
async def test_as_of_bounds_the_bars_at_the_card_time(monkeypatch):
    """The playbook passes the Card's timestamp so the newest close is the
    price at that minute, not whenever the API request arrived."""
    from datetime import datetime, timezone

    db = DatabaseManager()
    calls = []

    class _Conn:
        async def fetch(self, *args):
            calls.append(args)
            return [{"low": 767.75, "high": 767.99, "close": 767.99}]

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(db, "_acquire_connection", lambda: _Acquire())
    as_of = datetime(2026, 9, 23, 19, 47, tzinfo=timezone.utc)

    closes, _, _ = await db.get_recent_underlying_bars("SPY", as_of=as_of)
    sql, *params = calls[-1]
    assert "timestamp <= $3" in sql
    assert params == ["SPY", 120, as_of]
    assert closes == [767.99]

    await db.get_recent_underlying_bars("SPY")
    sql, *params = calls[-1]
    assert "timestamp <=" not in sql, "no as_of keeps the unbounded read"
    assert params == ["SPY", 120]
