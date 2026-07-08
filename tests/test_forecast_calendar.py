"""Tests for the OPEX / VIX-piration calendar helpers."""

from __future__ import annotations

from datetime import date

import pytest

import src.jobs.forecast_calendar as forecast_calendar
from src.jobs.forecast_calendar import (
    days_to_next_opex,
    is_monthly_opex_friday,
    is_post_opex_monday,
    is_vix_expiration_day,
    monthly_opex_date,
)


@pytest.fixture(autouse=True)
def _no_nyse_holidays(monkeypatch):
    """Pin the holiday set empty so the plain third-Friday assertions below
    are independent of the operator's ``NYSE_HOLIDAYS`` env var.

    ``forecast_calendar`` binds ``NYSE_HOLIDAYS`` at import time from
    ``src.market_calendar`` (which parses the env var once, at import). On a
    server whose ``.env`` lists 2026-06-19 (Juneteenth), June OPEX correctly
    rolls back to the 18th — but these tests assert the un-rolled third
    Friday. Neutralize the holiday roll here so the assertions test only the
    third-Friday math; the holiday-roll behavior gets its own explicit test
    below.
    """
    monkeypatch.setattr(forecast_calendar, "NYSE_HOLIDAYS", set())


def test_monthly_opex_is_third_friday():
    # June 2026: Fri 5, 12, 19 → third Friday = 19.
    assert monthly_opex_date(2026, 6) == date(2026, 6, 19)
    # July 2026: Fri 3, 10, 17 → third Friday = 17.
    assert monthly_opex_date(2026, 7) == date(2026, 7, 17)


def test_is_monthly_opex_friday_true():
    assert is_monthly_opex_friday(date(2026, 6, 19)) is True


def test_is_monthly_opex_friday_false_regular_friday():
    # First Friday, not OPEX.
    assert is_monthly_opex_friday(date(2026, 6, 5)) is False


def test_is_vix_expiration_true():
    # June 2026 third Wed = 17.
    assert is_vix_expiration_day(date(2026, 6, 17)) is True


def test_is_vix_expiration_false():
    assert is_vix_expiration_day(date(2026, 6, 18)) is False


def test_is_post_opex_monday_true():
    # Third Friday June 2026 was June 19; Monday June 22 is post-OPEX.
    assert is_post_opex_monday(date(2026, 6, 22)) is True


def test_is_post_opex_monday_false_random_monday():
    # First Monday of the month is not post-OPEX.
    assert is_post_opex_monday(date(2026, 6, 1)) is False


def test_days_to_next_opex_before_this_month():
    # June 5 → 14 days until June 19.
    assert days_to_next_opex(date(2026, 6, 5)) == 14


def test_days_to_next_opex_on_opex_day_returns_zero():
    assert days_to_next_opex(date(2026, 6, 19)) == 0


def test_days_to_next_opex_after_this_month_rolls_forward():
    # June 22 → next OPEX is July 17 (25 days).
    assert days_to_next_opex(date(2026, 6, 22)) == 25


def test_monthly_opex_rolls_back_when_third_friday_is_holiday(monkeypatch):
    # 2026-06-19 (the third Friday) is Juneteenth. With it in NYSE_HOLIDAYS,
    # June OPEX rolls back to the preceding trading day, Thursday the 18th.
    monkeypatch.setattr(forecast_calendar, "NYSE_HOLIDAYS", {date(2026, 6, 19)})
    assert monthly_opex_date(2026, 6) == date(2026, 6, 18)
    assert is_monthly_opex_friday(date(2026, 6, 18)) is True
    assert is_monthly_opex_friday(date(2026, 6, 19)) is False
    # The holiday-displaced Thursday OPEX still anchors the post-OPEX Monday.
    assert is_post_opex_monday(date(2026, 6, 22)) is True
