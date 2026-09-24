"""``in_regular_session``: the window a Playbook Card can be issued in.

The signal cycle runs 24x5, so the engine needs a precise answer to "can an
option be traded at this bar's price?" -- including the early close and the
bar-stamp convention at both edges.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone

from src import market_calendar
from src.market_calendar import in_regular_session, regular_session_close


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_close_is_four_pm_unless_the_day_closes_early(monkeypatch):
    half_day = date(2026, 11, 27)
    monkeypatch.setattr(market_calendar, "NYSE_HALF_DAYS", {half_day})
    assert regular_session_close(date(2026, 11, 25)) == time(16, 0)
    assert regular_session_close(half_day) == time(13, 0)


def test_window_is_half_open_at_both_edges():
    # 2026-09-23 is EDT: 09:30 ET = 13:30Z, 16:00 ET = 20:00Z.
    assert not in_regular_session(_utc(2026, 9, 23, 13, 29, 59))
    assert in_regular_session(_utc(2026, 9, 23, 13, 30))
    assert in_regular_session(_utc(2026, 9, 23, 19, 59, 59))
    assert not in_regular_session(_utc(2026, 9, 23, 20, 0))


def test_pre_market_and_after_hours_prints_are_outside():
    assert not in_regular_session(_utc(2026, 9, 23, 8, 0))  # 04:00 ET, card #11270
    assert not in_regular_session(_utc(2026, 9, 23, 23, 59))  # 19:59 ET


def test_naive_timestamps_are_read_as_utc():
    assert in_regular_session(datetime(2026, 9, 23, 13, 30))
    assert not in_regular_session(datetime(2026, 9, 23, 9, 30))  # 05:30 ET


def test_weekends_holidays_and_early_closes(monkeypatch):
    assert not in_regular_session(_utc(2026, 9, 26, 15, 0))  # Saturday 11:00 ET
    monkeypatch.setattr(market_calendar, "NYSE_HOLIDAYS", {date(2026, 11, 26)})
    assert not in_regular_session(_utc(2026, 11, 26, 16, 0))  # holiday 11:00 ET
    monkeypatch.setattr(market_calendar, "NYSE_HALF_DAYS", {date(2026, 11, 27)})
    assert in_regular_session(_utc(2026, 11, 27, 17, 59))  # 12:59 EST
    assert not in_regular_session(_utc(2026, 11, 27, 18, 0))  # 13:00 EST
