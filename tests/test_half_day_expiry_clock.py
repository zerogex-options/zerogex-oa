"""Regression: on an early-close day, today's PM-settled options expire at 13:00 ET.

``calculate_time_to_expiration`` anchored every PM-settled expiry at 16:00 ET,
so on an early-close day (2026-11-27, 2026-12-24) a 0DTE contract had three
phantom hours of life: its IV was solved too low and its charm too small all
day, and it still counted as alive after it stopped trading. The engine's own
remaining-session clock, its 16:15 roll-off of the day's expiry and the daily
ATM-IV window assumed 16:00 the same way.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from src import market_calendar as mc

HALF_DAY = date(2026, 11, 27)
NORMAL_DAY = date(2026, 11, 25)
YEAR_SECONDS = 365.0 * 86_400


@pytest.fixture(autouse=True)
def _half_day(monkeypatch):
    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {HALF_DAY})


def _et(day: date, hour: int, minute: int = 0) -> datetime:
    return mc.ET.localize(datetime(day.year, day.month, day.day, hour, minute))


def test_a_0dte_pm_contract_on_a_half_day_expires_at_1300():
    years = mc.calculate_time_to_expiration(_et(HALF_DAY, 12, 0), HALF_DAY)
    assert years * YEAR_SECONDS == pytest.approx(3600, abs=1)
    # Explicit 16:00 (what settlement_close_time_for_contract returns) too.
    years = mc.calculate_time_to_expiration(_et(HALF_DAY, 12, 0), HALF_DAY, "16:00:00")
    assert years * YEAR_SECONDS == pytest.approx(3600, abs=1)
    # After the 13:00 close it has expired.
    assert mc.calculate_time_to_expiration(_et(HALF_DAY, 13, 30), HALF_DAY) == 0.0


def test_a_normal_day_still_expires_at_1600():
    years = mc.calculate_time_to_expiration(_et(NORMAL_DAY, 12, 0), NORMAL_DAY)
    assert years * YEAR_SECONDS == pytest.approx(4 * 3600, abs=1)


def test_am_settled_contracts_are_untouched():
    # A 09:30 anchor is not the regular close, so it is left alone.
    years = mc.calculate_time_to_expiration(_et(HALF_DAY, 9, 0), HALF_DAY, "09:30:00")
    assert years * YEAR_SECONDS == pytest.approx(1800, abs=1)


def test_a_later_expiry_from_a_half_day_counts_to_its_own_close():
    # Monday 2026-11-30 is a normal day: anchored at its 16:00 close.
    monday = date(2026, 11, 30)
    years = mc.calculate_time_to_expiration(_et(HALF_DAY, 12, 0), monday)
    expected = (_et(monday, 16, 0) - _et(HALF_DAY, 12, 0)).total_seconds()
    assert years * YEAR_SECONDS == pytest.approx(expected, abs=1)


def test_the_engine_session_clock_ends_at_the_half_day_close(monkeypatch):
    from src.analytics import main_engine

    monkeypatch.setattr(main_engine, "regular_session_close", mc.regular_session_close)
    engine = main_engine.AnalyticsEngine.__new__(main_engine.AnalyticsEngine)
    remaining = engine._session_days_remaining(_et(HALF_DAY, 12, 0))
    assert remaining * 86_400 == pytest.approx(3600, abs=1)
    assert engine._session_days_remaining(_et(HALF_DAY, 13, 5)) == 0.0
    assert engine._session_days_remaining(_et(NORMAL_DAY, 12, 0)) * 86_400 == pytest.approx(
        4 * 3600, abs=1
    )
