"""Shared market-calendar helpers.

Single source of truth for:

* The Eastern timezone constant.
* NYSE holiday loading (from ``NYSE_HOLIDAYS`` env var, with the
  ``NYSE_HOLIDAYS_STRICT`` fail-fast toggle introduced in Phase 1).
* Time-to-expiration math used by the Greeks and IV calculators.
* ``is_market_hours``, ``get_market_session``, ``is_engine_run_window``,
  and ``seconds_until_engine_run_window`` — previously duplicated across
  ``src/validation.py`` and ``src/api/main.py``.

Most existing call sites import these names from ``src.validation`` —
those imports still work because ``validation.py`` now re-exports the
same symbols.  New code should import directly from
``src.market_calendar``.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Optional

import pytz

from src.utils import get_logger

logger = get_logger(__name__)

# Eastern Time timezone — used everywhere the US equity and options
# markets care about wall-clock time.
ET = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# NYSE holidays
# ---------------------------------------------------------------------------

def load_nyse_holidays() -> set[date]:
    """Load holiday dates from the ``NYSE_HOLIDAYS`` env var.

    Misconfigured holidays silently classify a closed session as "open",
    which produces incorrect market-state signals downstream.  Set
    ``NYSE_HOLIDAYS_STRICT=true`` (recommended for production) to raise
    on any invalid token so the process refuses to start with a corrupt
    calendar.
    """
    raw = os.getenv("NYSE_HOLIDAYS", "")
    strict = os.getenv("NYSE_HOLIDAYS_STRICT", "false").strip().lower() == "true"
    holidays: set[date] = set()
    invalid: list[str] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            holidays.add(date.fromisoformat(token))
        except ValueError:
            invalid.append(token)
            logger.error("Invalid date in NYSE_HOLIDAYS env var: %r", token)
    if invalid and strict:
        raise ValueError(
            f"NYSE_HOLIDAYS contains {len(invalid)} invalid token(s): {invalid!r}. "
            "Fix the env var or set NYSE_HOLIDAYS_STRICT=false to tolerate."
        )
    return holidays


# Singleton — loaded once at import time.  Every module that needs the
# holiday set should read it from here (or via the ``validation``
# re-export) rather than re-parsing the env var.
NYSE_HOLIDAYS: set[date] = load_nyse_holidays()


# ---------------------------------------------------------------------------
# Time-to-expiration
# ---------------------------------------------------------------------------

_MIN_YEARS_TO_EXPIRATION = 1.0 / 525_600  # one minute, in years


def calculate_time_to_expiration(
    current_date: datetime,
    expiration_date: date,
    market_close_time: str = "16:00:00",
) -> float:
    """Return time-to-expiration in years, floored at one minute.

    The expiration date is anchored at the US equity market close in ET.
    Naive ``current_date`` values are treated as UTC and converted to
    ET.  This is the single canonical implementation used by the Greeks
    calculator, IV calculator, and analytics engine (previously three
    independent copies, all slightly different in their edge handling).
    """
    if current_date.tzinfo is None:
        current_date = pytz.UTC.localize(current_date).astimezone(ET)
    else:
        current_date = current_date.astimezone(ET)

    close_t = datetime.strptime(market_close_time, "%H:%M:%S").time()
    expiration_dt = ET.localize(datetime.combine(expiration_date, close_t))

    years = (expiration_dt - current_date).total_seconds() / 86_400 / 365.0
    return max(years, _MIN_YEARS_TO_EXPIRATION)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def _to_et(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return datetime.now(ET)
    if dt.tzinfo is None:
        return pytz.UTC.localize(dt).astimezone(ET)
    return dt.astimezone(ET)


def is_market_hours(dt: Optional[datetime] = None, check_extended: bool = False) -> bool:
    """Return True if ``dt`` (default: now) is during cash-market hours.

    Extended hours (04:00–20:00 ET) are included when
    ``check_extended=True``.  Weekends and configured NYSE holidays
    always return False.
    """
    dt = _to_et(dt)
    if dt.weekday() > 4 or dt.date() in NYSE_HOLIDAYS:
        return False

    current_time = dt.time()
    if check_extended:
        market_open = datetime.strptime("04:00:00", "%H:%M:%S").time()
        market_close = datetime.strptime("20:00:00", "%H:%M:%S").time()
    else:
        market_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
        market_close = datetime.strptime("16:00:00", "%H:%M:%S").time()
    return market_open <= current_time <= market_close


def get_market_session(dt: Optional[datetime] = None) -> str:
    """Return the generic session label for ``dt``.

    One of: ``pre-market``, ``regular``, ``after-hours``, ``closed``.

    NOTE: the API layer has its own ``get_market_session`` helper with
    INDEX-vs-EQUITY semantics and a soft-close window; that lives in
    ``src/api/main.py`` and is intentionally not consolidated here.
    """
    dt = _to_et(dt)
    if dt.weekday() > 4 or dt.date() in NYSE_HOLIDAYS:
        return "closed"

    current_time = dt.time()
    pre_market_start = datetime.strptime("04:00:00", "%H:%M:%S").time()
    regular_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
    regular_close = datetime.strptime("16:00:00", "%H:%M:%S").time()
    after_hours_end = datetime.strptime("20:00:00", "%H:%M:%S").time()

    if current_time < pre_market_start:
        return "closed"
    if current_time < regular_open:
        return "pre-market"
    if current_time < regular_close:
        return "regular"
    if current_time < after_hours_end:
        return "after-hours"
    return "closed"


# ---------------------------------------------------------------------------
# Engine run window (24x5 weekdays minus NYSE holidays)
# ---------------------------------------------------------------------------

def is_engine_run_window(dt: Optional[datetime] = None) -> bool:
    """Engines run 24x5: all hours on weekdays excluding NYSE holidays."""
    dt = _to_et(dt)
    return dt.weekday() <= 4 and dt.date() not in NYSE_HOLIDAYS


def seconds_until_engine_run_window(dt: Optional[datetime] = None) -> int:
    """Seconds until midnight ET of the next non-holiday weekday."""
    dt = _to_et(dt)
    if is_engine_run_window(dt):
        return 0

    next_open = (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    while next_open.weekday() > 4 or next_open.date() in NYSE_HOLIDAYS:
        next_open += timedelta(days=1)
    return max(int((next_open - dt).total_seconds()), 1)
