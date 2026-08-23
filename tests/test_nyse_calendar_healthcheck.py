"""Tests for the NYSE calendar healthcheck.

The calendar fails SILENTLY when it lapses — an unlisted date is simply
"not a holiday", so every holiday past the end of the list is graded as a
normal trading session with no log line and no failed request. This check
is the alarm; these tests are the alarm's alarm.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.tools import nyse_calendar_healthcheck as hc

# A calendar with generous runway from TODAY_2026, used as the clean baseline.
TODAY_2026 = date(2026, 8, 23)
GOOD_HOLIDAYS = {
    date(2026, 9, 7),
    date(2026, 11, 26),
    date(2026, 12, 25),
    date(2027, 1, 1),
    date(2027, 11, 25),
    date(2027, 12, 24),
}
GOOD_HALF_DAYS = {date(2026, 11, 27), date(2026, 12, 24), date(2027, 11, 26)}


def _report(holidays=None, half_days=None, today=TODAY_2026, months=6):
    return hc.evaluate(
        set(GOOD_HOLIDAYS if holidays is None else holidays),
        set(GOOD_HALF_DAYS if half_days is None else half_days),
        today=today,
        months=months,
    )


# ---------------------------------------------------------------------------
# The derivation
# ---------------------------------------------------------------------------


def test_thanksgiving_is_the_fourth_thursday():
    assert hc.thanksgiving(2026) == date(2026, 11, 26)
    assert hc.thanksgiving(2027) == date(2027, 11, 25)
    assert hc.thanksgiving(2028) == date(2028, 11, 23)
    assert hc.thanksgiving(2029) == date(2029, 11, 22)


@pytest.mark.parametrize(
    "year,expected",
    [
        # Dec 25 2026 is a Friday, so Dec 24 (Thu) is a real trading day and an
        # early close. Jul 4 2026 is a Saturday -> observed Fri Jul 3 is a FULL
        # holiday, so there is no Jul 3 early close.
        (2026, {date(2026, 11, 27), date(2026, 12, 24)}),
        # Dec 25 2027 is a SATURDAY: the holiday is observed on Fri Dec 24, a
        # full close, so 2027 has no Christmas Eve early close at all. Jul 4
        # 2027 is a Sunday -> observed Mon Jul 5, no Jul 3 close either.
        (2027, {date(2027, 11, 26)}),
        # Jul 4 2028 is a Tuesday -> Jul 3 (Mon) is an early close.
        # Dec 24 2028 is a Sunday -> no Christmas Eve close.
        (2028, {date(2028, 7, 3), date(2028, 11, 24)}),
        # Jul 4 2029 Wed, Dec 24 2029 Mon: all three rules fire.
        (2029, {date(2029, 7, 3), date(2029, 11, 23), date(2029, 12, 24)}),
    ],
)
def test_half_day_derivation_handles_the_weekend_exceptions(year, expected):
    """The exceptions are where the mistakes live: a Saturday Christmas moves
    the observance to Dec 24 (full close, not half), and a weekend Jul 4
    absorbs the adjacent weekday entirely."""
    assert hc.expected_half_days(year) == expected


# ---------------------------------------------------------------------------
# Runway
# ---------------------------------------------------------------------------


def test_a_healthy_calendar_reports_no_problems():
    assert _report().problems == []


def test_a_lapsing_holiday_calendar_is_a_problem():
    """The silent cliff: past the last entry every holiday grades as a
    trading day, in the freshness envelope and the session gates alike."""
    report = _report(holidays={date(2026, 9, 7), date(2026, 12, 25)})
    assert any("NYSE_HOLIDAYS ends" in p for p in report.problems)


def test_an_empty_holiday_calendar_is_a_problem():
    assert any("empty" in p for p in _report(holidays=set()).problems)


def test_runway_is_measured_from_today_not_from_the_list():
    """Same calendar, two dates: fine now, a problem once time catches up."""
    assert _report(today=date(2026, 8, 23)).problems == []
    assert _report(today=date(2027, 10, 1)).problems != []


# ---------------------------------------------------------------------------
# Half-day correctness
# ---------------------------------------------------------------------------


def test_a_spurious_half_day_is_caught():
    """Dec 23 2027 was a real mistake in a shipped .env.example. It would have
    graded a normal full session as a 13:00 close, SUPPRESSING a genuine
    stall for three hours — the opposite of the failure the list prevents."""
    report = _report(half_days=GOOD_HALF_DAYS | {date(2027, 12, 23)})
    assert any("2027-12-23" in p and "do not produce" in p for p in report.problems)


def test_a_missing_half_day_is_caught():
    report = _report(half_days={date(2026, 12, 24), date(2027, 11, 26)})
    assert any("missing half day 2026-11-27" in p for p in report.problems)


def test_a_half_day_already_in_the_past_is_not_reported():
    """Nothing can be done about an early close that already happened; only
    future gaps are actionable."""
    report = _report(
        half_days={date(2027, 11, 26)},
        today=date(2026, 12, 1),  # 2026-11-27 has passed
        months=1,
    )
    assert not any("2026-11-27" in p for p in report.problems)


def test_a_date_in_both_lists_is_caught():
    """Ambiguous: the holiday wins, so either the early close or the holiday
    entry is wrong."""
    report = _report(half_days=GOOD_HALF_DAYS | {date(2026, 12, 25)})
    assert any("BOTH lists" in p for p in report.problems)


def test_a_half_day_whose_year_has_no_holidays_is_caught():
    """The tell that the two lists have drifted apart — which is exactly how
    the shipped template looked before the holiday list was extended."""
    report = _report(half_days=GOOD_HALF_DAYS | {date(2029, 11, 23)})
    assert any("no NYSE_HOLIDAYS entries at all" in p for p in report.problems)


def test_an_observed_full_closure_supersedes_a_derived_half_day():
    """If a derived early close is configured as a full holiday, that is a
    deliberate observance, not a missing half day."""
    report = _report(
        holidays=GOOD_HOLIDAYS | {date(2026, 11, 27)},
        half_days={date(2026, 12, 24), date(2027, 11, 26)},
    )
    assert not any("missing half day 2026-11-27" in p for p in report.problems)


def test_evaluate_is_pure_and_takes_its_clock_as_an_argument():
    """No env, no DB, no wall clock — so the check itself is testable and its
    verdict is reproducible."""
    a = _report(today=date(2026, 8, 23))
    b = _report(today=date(2026, 8, 23))
    assert a == b
