"""Single source of truth for the cash session's opening range.

The opening range is the high and low of the first 30 minutes of the cash
session: the 1-minute bars stamped 09:30 through 09:59 ET.
``underlying_quotes`` stamps each bar at the START of the minute it covers
(see ``src.ingestion.main_engine._store_underlying``), so that is exactly
``[09:30, 10:00)`` -- the same window the ``opening_range_breakout`` view in
``setup/database/schema.sql`` has always used.

Why this module exists
----------------------
Two strategies traded "the break of the opening range", and neither ever
read one:

* the ``opening_range_break`` playbook pattern used the last 120 one-minute
  bars (about two hours, reaching into SPY's pre-market and SPX's prior
  afternoon), and
* the ``OpeningRangeHunter`` TradeWorkz bot used the last 24 hours of bars.

Both then asked whether price had closed beyond the extreme of a window that
included the bar price was sitting in. A bar's close is always inside its
own high and low, so the break was impossible and neither ever fired.

Every consumer -- the live signal cycle, the ``/api/signals/action`` path
and the TradeWorkz snapshot -- reads the range through here, so they agree
on its value and on the moment it becomes available.

The range is only published once it is complete (10:00 ET) and only on the
session's own ET calendar day, so the next morning's pre-market never reads
yesterday's range. Every bar it reads is finished before any break is
tested against it, which also means a backtest rebuilding a past instant
can never see a future bar through it.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Final, Optional, Tuple
from zoneinfo import ZoneInfo

from src.validation import cash_session_date, cash_session_start_utc

_ET = ZoneInfo("America/New_York")

#: Length of the opening range, in minutes from the 09:30 ET cash open.
OPENING_RANGE_MINUTES: Final[int] = 30

#: Fewest of the window's 30 one-minute bars that must be present for the
#: range to be published. A feed gap at the open would otherwise understate
#: the range and turn ordinary movement into a "break".
MIN_BARS: Final[int] = int(os.getenv("SIGNAL_OPENING_RANGE_MIN_BARS", "20"))

#: psycopg2 placeholders: ``(symbol, window_start, window_end)``.
OPENING_RANGE_SQL: Final[str] = (
    "SELECT MAX(high), MIN(low), COUNT(*) FROM underlying_quotes "
    "WHERE symbol = %s AND timestamp >= %s AND timestamp < %s"
)

#: asyncpg placeholders, same parameters in the same order.
OPENING_RANGE_SQL_ASYNC: Final[str] = (
    "SELECT MAX(high), MIN(low), COUNT(*) FROM underlying_quotes "
    "WHERE symbol = $1 AND timestamp >= $2 AND timestamp < $3"
)


def opening_range_window(ts: datetime) -> Optional[Tuple[datetime, datetime]]:
    """UTC ``[start, end)`` of the opening range for the session ``ts`` is in.

    ``None`` while the range is still forming (before 10:00 ET) and whenever
    ``ts`` is not on the session's own ET calendar day (the next morning's
    pre-market, a weekend). Naive timestamps are treated as UTC, matching
    :func:`src.validation.cash_session_date`.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    session = cash_session_date(ts)
    start = cash_session_start_utc(session)
    end = start + timedelta(minutes=OPENING_RANGE_MINUTES)
    if ts < end or ts.astimezone(_ET).date() != session:
        return None
    return start, end


def opening_range_from_row(row: Any) -> Optional[Tuple[float, float]]:
    """``(high, low)`` from an ``OPENING_RANGE_SQL`` row, or ``None``.

    ``None`` when the row is missing or empty, or when fewer than
    :data:`MIN_BARS` bars made it into the window.
    """
    if not row:
        return None
    high, low, bars = row[0], row[1], row[2]
    if high is None or low is None or int(bars or 0) < MIN_BARS:
        return None
    high_f, low_f = float(high), float(low)
    if high_f < low_f:
        return None
    return high_f, low_f
