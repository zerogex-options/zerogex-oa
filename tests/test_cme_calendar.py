"""CME equity-index futures calendar tests (src/futures/calendar.py)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.futures.calendar import (
    CMEEquityIndexCalendar,
    SessionLabel,
)

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")


@pytest.fixture
def cal():
    return CMEEquityIndexCalendar()


def _ct(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=CT)


# -- Sunday evening open ----------------------------------------------------


def test_sunday_evening_opens_at_1700_ct(cal):
    # 2026-01-11 is a Sunday.
    assert cal.is_trading(_ct(2026, 1, 11, 17, 30)) is True
    assert cal.is_trading(_ct(2026, 1, 11, 16, 0)) is False
    # Sunday-evening session belongs to Monday's trade date.
    assert cal.get_trade_date(_ct(2026, 1, 11, 17, 30)) == "2026-01-12"


def test_saturday_is_closed(cal):
    assert cal.is_trading(_ct(2026, 1, 10, 12, 0)) is False
    assert cal.get_session(_ct(2026, 1, 10, 12, 0)).label is SessionLabel.CLOSED


# -- Trade-date rollover / overnight ---------------------------------------


def test_trade_date_rolls_at_1700_ct(cal):
    # Tue 15:00 CT -> Tuesday; Tue 17:30 CT -> Wednesday.
    assert cal.get_trade_date(_ct(2026, 1, 13, 15, 0)) == "2026-01-13"
    assert cal.get_trade_date(_ct(2026, 1, 13, 17, 30)) == "2026-01-14"


def test_midnight_overnight_belongs_to_next_trade_date(cal):
    # Wed 00:30 CT is part of Wednesday's trade date and is trading.
    dt = _ct(2026, 1, 14, 0, 30)
    assert cal.get_trade_date(dt) == "2026-01-14"
    assert cal.is_trading(dt) is True
    assert cal.get_session(dt).label is SessionLabel.GLOBEX


def test_friday_evening_maps_to_monday(cal):
    # Thu 20:00 CT -> Friday's trade date.
    assert cal.get_trade_date(_ct(2026, 1, 15, 20, 0)) == "2026-01-16"
    # Friday after 16:00 CT is closed (no Friday-evening session).
    assert cal.is_trading(_ct(2026, 1, 16, 17, 30)) is False


# -- Daily maintenance ------------------------------------------------------


def test_daily_maintenance_window(cal):
    dt = _ct(2026, 1, 13, 16, 30)  # Tue 16:30 CT
    assert cal.is_maintenance_window(dt) is True
    assert cal.is_trading(dt) is False
    assert cal.get_session(dt).label is SessionLabel.MAINTENANCE


# -- Friday close -----------------------------------------------------------


def test_friday_1600_ct_close(cal):
    assert cal.is_trading(_ct(2026, 1, 16, 15, 59)) is True
    assert cal.is_trading(_ct(2026, 1, 16, 16, 30)) is False


# -- Session labels ---------------------------------------------------------


def test_cash_overlap_label(cal):
    # Tue 10:00 CT (~11:00 ET) is inside the RTH overlap.
    assert cal.get_session(_ct(2026, 1, 13, 10, 0)).label is SessionLabel.CASH_OVERLAP
    # Tue 20:00 CT overnight is plain Globex.
    assert cal.get_session(_ct(2026, 1, 13, 20, 0)).label is SessionLabel.GLOBEX


# -- DST-safe handling ------------------------------------------------------


def test_dst_winter_and_summer_map_to_same_wall_time(cal):
    # Winter (CST, UTC-6): 2026-01-06 16:00 UTC == 10:00 CT.
    winter = datetime(2026, 1, 6, 16, 0, tzinfo=UTC)
    # Summer (CDT, UTC-5): 2026-07-07 15:00 UTC == 10:00 CT.
    summer = datetime(2026, 7, 7, 15, 0, tzinfo=UTC)
    assert cal.get_session(winter).label is SessionLabel.CASH_OVERLAP
    assert cal.get_session(summer).label is SessionLabel.CASH_OVERLAP


def test_naive_datetime_treated_as_utc(cal):
    naive = datetime(2026, 1, 6, 16, 0)  # no tzinfo -> UTC -> 10:00 CT
    assert cal.get_session(naive).label is SessionLabel.CASH_OVERLAP


# -- Holidays & early closes ------------------------------------------------


def test_christmas_is_closed(cal):
    # 2025-12-25 is a full CME holiday.
    assert cal.is_holiday(cal._trade_date_ct(_ct(2025, 12, 25, 10, 0)))
    assert cal.is_trading(_ct(2025, 12, 25, 10, 0)) is False


def test_early_close_truncates_day_session(cal):
    # 2025-12-24 early close at 12:00 CT.
    assert cal.is_trading(_ct(2025, 12, 24, 10, 0)) is True
    assert cal.is_trading(_ct(2025, 12, 24, 13, 0)) is False


def test_holiday_by_trade_date_closes_overnight(cal):
    # Evening before Christmas rolls to the 25th trade date -> closed.
    # 2025-12-24 18:00 CT -> trade date 2025-12-25 (holiday).
    dt = _ct(2025, 12, 24, 18, 0)
    assert cal.get_trade_date(dt) == "2025-12-25"
    assert cal.is_trading(dt) is False


def test_env_holiday_override(monkeypatch):
    monkeypatch.setenv("CME_HOLIDAYS", "2026-01-13")
    cal = CMEEquityIndexCalendar()
    assert cal.is_trading(_ct(2026, 1, 13, 10, 0)) is False


# -- Session boundaries -----------------------------------------------------


def test_session_open_close_boundaries(cal):
    open_dt = cal.get_session_open("2026-01-12")  # Monday
    close_dt = cal.get_session_close("2026-01-12")
    assert open_dt == _ct(2026, 1, 11, 17, 0)  # Sunday 17:00 CT
    assert close_dt == _ct(2026, 1, 12, 16, 0)  # Monday 16:00 CT


def test_settlement_window(cal):
    win = cal.get_settlement_window("2026-01-13")
    assert win is not None
    assert win.start == _ct(2026, 1, 13, 16, 0)
    assert win.end == _ct(2026, 1, 13, 17, 0)


# -- 0DTE / DTE classification ---------------------------------------------


def test_zero_dte_by_exact_expiration_instant(cal):
    now = _ct(2026, 1, 13, 10, 0)  # Tuesday morning
    exp_today = _ct(2026, 1, 13, 15, 0)  # expires today 15:00 CT
    exp_tomorrow = _ct(2026, 1, 14, 15, 0)
    assert cal.is_zero_dte(now, exp_today) is True
    assert cal.classify_dte(now, exp_today) == 0
    assert cal.classify_dte(now, exp_tomorrow) == 1
    assert cal.is_zero_dte(now, exp_tomorrow) is False


def test_already_expired_is_not_zero_dte(cal):
    now = _ct(2026, 1, 13, 16, 30)
    already = _ct(2026, 1, 13, 15, 0)  # settled earlier today
    assert cal.is_zero_dte(now, already) is False
    assert cal.classify_dte(now, already) == 0  # DTE floors at 0
