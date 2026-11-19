"""Early-close days: the fleet's day ends 5 minutes before a 13:00 close.

The engine's cutoffs were a fixed 15:55 ET. On an early-close day
(``NYSE_HALF_DAYS``: the Friday after Thanksgiving, Christmas Eve) the cash
session ends at 13:00, so the fleet would have kept opening for three hours
on quotes up to 15 minutes stale, held same-day positions it could no longer
price or exit, and settled SPY/QQQ same-day options against an after-hours
print instead of the 13:00 close. These pin the half-day behavior across the
live engine, the research harness and the product bot replay.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pytest

from src import market_calendar

ET = ZoneInfo("America/New_York")
HALF = date(2026, 11, 27)  # Friday after Thanksgiving
NORMAL = date(2026, 11, 25)


@pytest.fixture(autouse=True)
def half_days(monkeypatch):
    monkeypatch.setattr(market_calendar, "NYSE_HALF_DAYS", {HALF})


def _utc(day: date, hh: int, mm: int) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mm, tzinfo=ET).astimezone(timezone.utc)


def test_the_cutoff_is_five_minutes_before_the_days_close():
    from src.tradeworkz.reconciler import session_cutoff_et

    assert session_cutoff_et(NORMAL) == time(15, 55)
    assert session_cutoff_et(HALF) == time(12, 55)


def test_the_live_engine_stops_opening_at_1255_on_a_half_day():
    from src.tradeworkz.engine import _opens_allowed

    assert _opens_allowed(_utc(HALF, 12, 54)) is True
    assert _opens_allowed(_utc(HALF, 12, 55)) is False
    assert _opens_allowed(_utc(HALF, 14, 0)) is False
    # A normal day is unchanged.
    assert _opens_allowed(_utc(NORMAL, 14, 0)) is True
    assert _opens_allowed(_utc(NORMAL, 15, 54)) is True
    assert _opens_allowed(_utc(NORMAL, 15, 55)) is False


def test_the_harness_and_bot_replay_gate_the_same_way():
    from src.backtesting.bot_replay import _opens_allowed as replay_gate
    from src.tradeworkz.backtest import _opens_allowed as harness_gate

    for gate in (harness_gate, replay_gate):
        assert gate(_utc(HALF, 12, 54), time(15, 55)) is True
        assert gate(_utc(HALF, 12, 56), time(15, 55)) is False
        assert gate(_utc(NORMAL, 12, 56), time(15, 55)) is True


def test_a_same_day_position_is_time_stopped_before_a_1300_close():
    from src.tradeworkz.reconciler import _expiration_close_cap_utc

    assert _expiration_close_cap_utc(HALF) == _utc(HALF, 12, 55)
    assert _expiration_close_cap_utc(NORMAL) == _utc(NORMAL, 15, 55)


class _Cur:
    def __init__(self):
        self.params = None

    def execute(self, sql, params=None):
        self.params = params

    def fetchone(self):
        return (600.0,)


class _Conn:
    def __init__(self):
        self.cur = _Cur()

    def cursor(self):
        return self.cur


def test_settlement_reads_the_last_print_before_the_days_close():
    from src.tradeworkz.pricing import _settlement_spot

    conn = _Conn()
    assert _settlement_spot(conn, "SPY", HALF) == 600.0
    assert conn.cur.params == ("SPY", _utc(HALF, 13, 0))
    _settlement_spot(conn, "SPY", NORMAL)
    assert conn.cur.params == ("SPY", _utc(NORMAL, 16, 0))
