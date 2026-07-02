"""Calendar helpers for the daily-forecast writer.

Standard-library only.  Detects the special days v1.2 forecast_range_model
branches on: monthly OPEX Friday, VIX-piration Wednesday, post-OPEX
Monday, and days-to-next-OPEX for horizon-aware weighting.

Not tied to any specific symbol — these are pure calendar predicates.
NYSE holiday rolls are honored so a Friday holiday shifts OPEX to
Thursday, following market convention.
"""

from __future__ import annotations

from datetime import date, timedelta

from src.market_calendar import NYSE_HOLIDAYS


def _third_friday(year: int, month: int) -> date:
    """Third Friday of the calendar month (standard monthly OPEX)."""
    d = date(year, month, 1)
    # weekday(): Mon=0..Sun=6.  Friday=4.
    delta = (4 - d.weekday()) % 7
    first_friday = d + timedelta(days=delta)
    return first_friday + timedelta(days=14)


def _third_wednesday(year: int, month: int) -> date:
    """Third Wednesday — VIX futures/options settle here (VIX-piration)."""
    d = date(year, month, 1)
    delta = (2 - d.weekday()) % 7  # Wednesday = 2
    first_wednesday = d + timedelta(days=delta)
    return first_wednesday + timedelta(days=14)


def _roll_forward_holiday(day: date) -> date:
    """If ``day`` is a NYSE holiday, roll BACK a day.  OPEX convention:
    if the third Friday is a holiday, options expire the preceding
    Thursday.  Applies iteratively for the (rare) two-day holiday case."""
    while day in NYSE_HOLIDAYS:
        day -= timedelta(days=1)
    return day


def monthly_opex_date(year: int, month: int) -> date:
    """Effective monthly OPEX date for a calendar month, accounting for
    holiday rolls."""
    return _roll_forward_holiday(_third_friday(year, month))


def is_monthly_opex_friday(day: date) -> bool:
    """True iff ``day`` is the effective monthly OPEX for its month."""
    return day == monthly_opex_date(day.year, day.month)


def is_vix_expiration_day(day: date) -> bool:
    """True iff ``day`` is the third Wednesday of its calendar month.
    VIX futures/options settle on the AM open of this day."""
    return day == _third_wednesday(day.year, day.month)


def is_post_opex_monday(day: date) -> bool:
    """True iff ``day`` is the Monday immediately following a monthly
    OPEX Friday.  Holiday-rolled OPEX still counts (Thursday OPEX →
    following Monday still qualifies)."""
    if day.weekday() != 0:  # Monday
        return False
    # Look back at the prior 3-5 days for the effective OPEX date.
    for delta in range(1, 6):
        candidate = day - timedelta(days=delta)
        if is_monthly_opex_friday(candidate):
            return True
    return False


def days_to_next_opex(day: date) -> int:
    """Positive integer: number of calendar days from ``day`` until the
    next effective monthly OPEX.  Returns 0 on OPEX day itself."""
    this_month = monthly_opex_date(day.year, day.month)
    if day <= this_month:
        return (this_month - day).days
    # Roll to next month.
    if day.month == 12:
        next_month = monthly_opex_date(day.year + 1, 1)
    else:
        next_month = monthly_opex_date(day.year, day.month + 1)
    return (next_month - day).days
