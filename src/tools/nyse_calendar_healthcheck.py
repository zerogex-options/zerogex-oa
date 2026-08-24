"""Verify the NYSE calendar has runway left and that its half days are real.

Catches the silent-failure mode where ``NYSE_HOLIDAYS`` simply runs out.
Nothing errors when that happens: the calendar is a set of dates, an
unlisted date is "not a holiday", and every subsequent holiday is graded
as a normal trading session — in :mod:`src.api.freshness` (which then
reports ``stale`` all day on a closed market), in ``is_market_hours`` and
the analytics off-hours gate, and in the signal engine's 24x5 run window.
There is no log line and no failed request; the calendar just quietly
stops being right.

``NYSE_HALF_DAYS`` has the same property in the other direction. A missing
early close makes every endpoint report ``stale`` for the three hours
after a real 13:00 ET close; a spurious one suppresses a genuine stall for
those same three hours.

Half days are derived, not memorised, so a typo is catchable. Three rules
produce the whole list, and the exceptions are where the mistakes live:

* the day after Thanksgiving — always;
* Dec 24, but only when Dec 24 AND Dec 25 are both weekdays.  When
  Christmas falls on a Saturday the holiday is OBSERVED on Fri Dec 24, a
  full close that belongs in ``NYSE_HOLIDAYS``;
* Jul 3, but only when Jul 4 falls Tue-Fri.  When Jul 4 lands on a
  weekend the observed holiday absorbs the adjacent weekday and there is
  no early close at all.

Ad-hoc closures (a national day of mourning) cannot be derived and are
never reported as spurious — only dates that look like a botched
application of the three rules are.

Exit codes:
    0 — calendar has runway and the half days check out.
    1 — runway is short, or a half day is missing/spurious/contradictory.
    2 — the calendar could not be parsed.

Usage:
    python -m src.tools.nyse_calendar_healthcheck
    python -m src.tools.nyse_calendar_healthcheck --months 12
    python -m src.tools.nyse_calendar_healthcheck --json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Sequence

logger = logging.getLogger(__name__)

# Default runway. Six months is comfortably longer than the gap between
# consecutive NYSE holidays, so a calendar that passes today cannot lapse
# before the next check without at least one alert in between.
DEFAULT_MONTHS = 6


@dataclass
class CalendarReport:
    today: str
    holidays_count: int
    holidays_last: str | None
    holidays_runway_days: int | None
    half_days_count: int
    half_days_last: str | None
    half_days_runway_days: int | None
    required_days: int
    problems: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def thanksgiving(year: int) -> date:
    """Fourth Thursday in November."""
    d = date(year, 11, 1)
    while d.weekday() != 3:  # Thursday
        d += timedelta(days=1)
    return d + timedelta(days=21)


def expected_half_days(year: int) -> set[date]:
    """The early closes the three rules produce for ``year``."""
    out = {thanksgiving(year) + timedelta(days=1)}
    if date(year, 12, 24).weekday() < 5 and date(year, 12, 25).weekday() < 5:
        out.add(date(year, 12, 24))
    if date(year, 7, 4).weekday() in (1, 2, 3, 4):  # Tue-Fri
        out.add(date(year, 7, 3))
    return out


def _months_out(today: date, months: int) -> date:
    """``today`` plus ``months`` calendar months, clamped to a valid day."""
    year = today.year + (today.month - 1 + months) // 12
    month = (today.month - 1 + months) % 12 + 1
    day = min(today.day, 28)  # 28 is safe in every month; runway needs no precision
    return date(year, month, day)


def evaluate(
    holidays: set[date],
    half_days: set[date],
    *,
    today: date,
    months: int = DEFAULT_MONTHS,
) -> CalendarReport:
    """Grade a configured calendar. Pure — no env, no clock, no DB."""
    horizon = _months_out(today, months)
    required_days = (horizon - today).days

    hol_last = max(holidays) if holidays else None
    half_last = max(half_days) if half_days else None
    report = CalendarReport(
        today=str(today),
        holidays_count=len(holidays),
        holidays_last=str(hol_last) if hol_last else None,
        holidays_runway_days=(hol_last - today).days if hol_last else None,
        half_days_count=len(half_days),
        half_days_last=str(half_last) if half_last else None,
        half_days_runway_days=(half_last - today).days if half_last else None,
        required_days=required_days,
    )

    # --- runway ------------------------------------------------------------
    if hol_last is None:
        report.problems.append("NYSE_HOLIDAYS is empty — every holiday grades as a trading day")
    elif hol_last < horizon:
        report.problems.append(
            f"NYSE_HOLIDAYS ends {hol_last} — under {months} months of runway "
            f"(need through {horizon}). Every holiday after that date will be "
            f"graded as a normal trading session."
        )
    if half_last is None:
        report.warnings.append("NYSE_HALF_DAYS is empty — early closes will report stale")
    elif half_last < horizon:
        report.problems.append(
            f"NYSE_HALF_DAYS ends {half_last} — under {months} months of runway "
            f"(need through {horizon})."
        )

    # --- a half day in a year the holiday calendar does not cover ----------
    # The tell that the two lists have drifted apart.
    hol_years = {d.year for d in holidays}
    for d in sorted(half_days):
        if d.year not in hol_years:
            report.problems.append(
                f"half day {d} is in {d.year}, which has no NYSE_HOLIDAYS entries at all"
            )

    # --- a date in both lists ---------------------------------------------
    both = holidays & half_days
    for d in sorted(both):
        report.problems.append(
            f"{d} is in BOTH lists — it will be graded a full holiday, so either "
            f"the early close is wrong or the holiday is"
        )

    # --- derived half days, only for years the calendar actually covers ----
    # Checking a year with no configured holidays would just restate the
    # runway problem above.
    covered = sorted(y for y in hol_years if y >= today.year)
    for year in covered:
        expected = expected_half_days(year)
        configured = {d for d in half_days if d.year == year}
        for d in sorted(expected - configured):
            if d in holidays:
                continue  # an observed full closure supersedes the early close
            if d < today:
                continue  # already passed; nothing to alert about
            report.problems.append(f"missing half day {d} ({_why(d)})")
        for d in sorted(configured - expected):
            report.problems.append(
                f"{d} is configured as a half day but the NYSE rules do not "
                f"produce one that day ({_near_miss(d)})"
            )

    return report


def _why(d: date) -> str:
    if d == thanksgiving(d.year) + timedelta(days=1):
        return "day after Thanksgiving"
    if (d.month, d.day) == (12, 24):
        return "Christmas Eve, a trading day that year"
    if (d.month, d.day) == (7, 3):
        return "Jul 4 falls Tue-Fri that year"
    return "derived by the NYSE early-close rules"


def _near_miss(d: date) -> str:
    """Explain why a configured date is NOT an early close, when we can."""
    if (d.month, d.day) == (12, 24):
        return "Dec 25 falls on a weekend, so Dec 24 is the observed full holiday"
    if (d.month, d.day) == (12, 23):
        return "Dec 23 is never an early close; check whether Dec 24 is the observed holiday"
    if (d.month, d.day) == (7, 3):
        return "Jul 4 falls on a weekend, so the observed holiday absorbs Jul 3"
    if d.weekday() >= 5:
        return "it is a weekend"
    return "not the day after Thanksgiving, Dec 24, or Jul 3"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--months",
        type=int,
        default=DEFAULT_MONTHS,
        help=f"Runway required in both calendars (default: {DEFAULT_MONTHS}).",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        # Read through market_calendar so this checks the SAME parsed sets the
        # running services use, including NYSE_HOLIDAYS_STRICT rejection.
        from src.market_calendar import NYSE_HALF_DAYS, NYSE_HOLIDAYS
    except Exception:
        logger.exception("nyse calendar healthcheck: calendar could not be loaded")
        return 2

    report = evaluate(
        set(NYSE_HOLIDAYS),
        set(NYSE_HALF_DAYS),
        today=date.today(),
        months=args.months,
    )

    if args.json:
        print(json.dumps(asdict(report), indent=2))
    else:
        logger.info(
            "NYSE_HOLIDAYS: %d dates, last %s (%s days out)",
            report.holidays_count,
            report.holidays_last,
            report.holidays_runway_days,
        )
        logger.info(
            "NYSE_HALF_DAYS: %d dates, last %s (%s days out)",
            report.half_days_count,
            report.half_days_last,
            report.half_days_runway_days,
        )
        for w in report.warnings:
            logger.warning("%s", w)
        for p in report.problems:
            logger.error("%s", p)

    if report.problems:
        logger.error("nyse calendar healthcheck FAILED with %d problem(s)", len(report.problems))
        return 1
    logger.info("nyse calendar healthcheck OK (>= %d months of runway)", args.months)
    return 0


if __name__ == "__main__":
    sys.exit(main())
