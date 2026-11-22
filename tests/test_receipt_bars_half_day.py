"""The 4:05 PM receipt grades a half day's low/high from 09:30-13:00 only.

``get_underlying_bars_for_session`` read 09:30-16:00 every day, so on an
early-close day (NYSE_HALF_DAYS: 2026-11-27, 2026-12-24) an ETF's three hours
of after-hours prints counted toward the low/high the morning band was graded
against. The window now ends at the day's own close.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, time


class _Conn:
    def __init__(self):
        self.calls = []

    async def fetch(self, sql, *params):
        self.calls.append((sql, params))
        return []


def _db(conn):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


def _session_end(monkeypatch, day: date) -> time:
    monkeypatch.setenv("NYSE_HALF_DAYS", "2026-11-27,2026-12-24")
    conn = _Conn()
    asyncio.run(_db(conn).get_underlying_bars_for_session("SPY", day))
    (sql, params) = conn.calls[-1]
    assert "BETWEEN TIME '09:30' AND $3::time" in " ".join(sql.split())
    assert params[:2] == ("SPY", day)
    return params[2]


def test_half_day_window_ends_at_1300(monkeypatch):
    assert _session_end(monkeypatch, date(2026, 11, 27)) == time(13, 0)
    assert _session_end(monkeypatch, date(2026, 12, 24)) == time(13, 0)


def test_regular_day_window_still_ends_at_1600(monkeypatch):
    assert _session_end(monkeypatch, date(2026, 11, 25)) == time(16, 0)
    assert _session_end(monkeypatch, date(2026, 11, 30)) == time(16, 0)
