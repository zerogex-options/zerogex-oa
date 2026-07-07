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
from datetime import date, datetime, time, timedelta
from typing import Optional

import pytz

from src.symbols import is_cash_index, resolve_index_future
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

# Lower bound on time-to-expiration in years. The BS gamma kernel has a
# ``1/√T`` term: at T = 1 minute (1/525,600 yr) that's ~27 — so a single
# 1-minute-to-expiry ATM contract dominates every aggregate metric
# (GEX, walls, max-gamma) by ~50× relative to a 30-day strike at the same
# OI. Floor at 30 minutes by default (= 1/17,520 yr → √T ≈ 0.0076) so
# expiring contracts decay gracefully out of the metrics rather than
# spiking. ``ANALYTICS_MIN_TTE_MINUTES`` env override available for
# operators who want the prior 1-minute behavior.
_MIN_TTE_MINUTES = max(0.5, float(os.getenv("ANALYTICS_MIN_TTE_MINUTES", "30")))
_MIN_YEARS_TO_EXPIRATION = _MIN_TTE_MINUTES / (60.0 * 24.0 * 365.0)


def is_spx_am_settled_expiration(symbol: str, expiration_date: date) -> bool:
    """True for SPX/SPXpm AM-settled monthly expirations.

    SPX monthly options expire on the third Friday of the month and
    settle AM at the Special Opening Quotation (~09:30 ET).  The
    weekly SPX series (SPXW) and end-of-month series settle PM at
    16:00 ET, like SPY/QQQ/etc.

    We can't always tell the series from just (underlying, expiration)
    because TradeStation lists both SPX and SPXW under the same
    ``$SPX.X`` underlying.  Best heuristic without an option-symbol
    prefix is: ``$SPX.X`` (or canonical ``SPX``) on a 3rd-Friday is
    AM-settled.  SPXW on a 3rd-Friday is rare in production data;
    callers with ``option_symbol`` available should branch on the
    ``SPXW`` prefix and skip this helper for those rows.
    """
    # Normalize "$SPX.X" / "SPX" / "$SPX" -> "SPX".  Use explicit
    # prefix/suffix strips, NOT str.rstrip(".X") -- rstrip treats its
    # argument as a character set and would turn "SPX" into "SP".
    sym = (symbol or "").upper()
    if sym.startswith("$"):
        sym = sym[1:]
    if sym.endswith(".X"):
        sym = sym[:-2]
    if sym != "SPX":
        return False
    # Third-Friday rule: weekday() == 4 (Fri) and day-of-month in [15, 21].
    return expiration_date.weekday() == 4 and 15 <= expiration_date.day <= 21


def expiration_close_time_et(symbol: str, expiration_date: date) -> str:
    """Wall-clock time in ET at which the contract settles.

    Returns ``"09:30:00"`` for SPX AM-settled monthlies and
    ``"16:00:00"`` for everything else.  Pass to
    ``calculate_time_to_expiration`` so AM-settled contracts don't
    accumulate ~6.5 hours of phantom time value on the morning of
    expiration.
    """
    if is_spx_am_settled_expiration(symbol, expiration_date):
        return "09:30:00"
    return "16:00:00"


def settlement_close_time_for_contract(
    underlying_symbol: Optional[str],
    option_symbol: Optional[str],
    expiration: date,
) -> str:
    """Per-contract version of ``expiration_close_time_et``.

    Picks the right ET close time given an option_symbol prefix, so
    SPXW (PM-settled) and SPX (AM-settled) contracts that share an
    underlying AND an expiration date (e.g. a 3rd-Friday under SPX
    monthly chain expansion) get distinct close times.  Falls back to
    16:00 ET when called without an underlying — the most common case
    (equity ETFs / equities).
    """
    if not underlying_symbol:
        return "16:00:00"
    if (option_symbol or "").upper().startswith("SPXW"):
        return "16:00:00"
    return expiration_close_time_et(underlying_symbol, expiration)


def calculate_time_to_expiration(
    current_date: datetime,
    expiration_date: date,
    market_close_time: str = "16:00:00",
) -> float:
    """Return time-to-expiration in years, floored at one minute.

    The expiration date is anchored at the US equity market close in ET
    by default; pass ``market_close_time="09:30:00"`` for AM-settled
    contracts (or use ``expiration_close_time_et`` to derive it).  Naive
    ``current_date`` values are treated as UTC and converted to ET.
    """
    if current_date.tzinfo is None:
        current_date = pytz.UTC.localize(current_date).astimezone(ET)
    else:
        current_date = current_date.astimezone(ET)

    close_t = datetime.strptime(market_close_time, "%H:%M:%S").time()
    expiration_dt = ET.localize(datetime.combine(expiration_date, close_t))

    years = (expiration_dt - current_date).total_seconds() / 86_400 / 365.0
    # Past the settlement instant the contract has EXPIRED. Return 0 so
    # callers' ``T <= 0`` guards (BS gamma/IV solver/Greeks) drop it. The
    # floor below intentionally only protects *still-alive* near-expiry
    # contracts (0 < years < floor) from the ``1/√T`` gamma spike; applying
    # it to ``years <= 0`` fabricated ~30 min of life and produced non-NULL
    # Greeks/IV for dead contracts whose option_chains rows linger after
    # settlement (e.g. AM-settled SPX after 09:30, PM contracts 16:00-16:15).
    if years <= 0.0:
        return 0.0
    return max(years, _MIN_YEARS_TO_EXPIRATION)  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------


def _to_et(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return datetime.now(ET)
    if dt.tzinfo is None:
        return pytz.UTC.localize(dt).astimezone(ET)  # type: ignore[no-any-return]
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


def is_rth_settled(dt: Optional[datetime] = None, settle_minutes: int = 30) -> bool:
    """True iff ``dt`` (default: now) is within RTH (09:30–16:00 ET) AND at
    least ``settle_minutes`` past the 09:30 ET open.

    Used to gate "no data yet" diagnostics so that a worker restart in
    the pre-market extended-hours window (04:00–09:30 ET) or in the
    first few minutes after open doesn't false-positive when options
    ingestion / analytics has legitimately not produced any rows yet.
    Weekends and configured NYSE holidays always return False.
    """
    dt_et = _to_et(dt)
    if dt_et.weekday() > 4 or dt_et.date() in NYSE_HOLIDAYS:
        return False
    open_dt = dt_et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_dt = dt_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_dt + timedelta(minutes=settle_minutes) <= dt_et <= close_dt


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


def _feed_session_window(session_template: Optional[str]) -> tuple[time, time]:
    """ET window the TradeStation bar feed delivers data in for a given
    session template. Unknown/custom templates fail safe to the widest
    window so a genuine stall is never silenced."""
    st = (session_template or "").strip().lower()
    if st == "default":
        return time(9, 30), time(16, 0)
    if st == "useqpre":
        return time(4, 0), time(9, 30)
    # "useq24hour" and any unrecognised template -> widest (fail safe).
    return time(4, 0), time(20, 0)


def underlying_feed_expected(
    dt: Optional[datetime],
    session_template: str = "Default",
    symbol: Optional[str] = None,
) -> bool:
    """True when the underlying bar feed should be producing bars now.

    The window is the TradeStation ``session_template``'s delivery
    window, EXCEPT cash indices (SPX, NDX, …): their underlying level
    prints only during the regular cash session (09:30–16:00 ET)
    regardless of template — the *options* trade extended hours but the
    index itself does not. Weekends/holidays are always False.

    When this returns False, underlying silence is EXPECTED (post-close
    / pre-open): refuse Greeks quietly at DEBUG and do NOT warn or
    attempt a stream reconnect. When True, staleness is a real anomaly.
    """
    dt = _to_et(dt)
    if dt.weekday() > 4 or dt.date() in NYSE_HOLIDAYS:
        return False
    open_t, close_t = _feed_session_window(session_template)
    if symbol and is_cash_index(symbol):
        # A cash index has no pre/after-hours print even under a 24h
        # template — clamp to the regular cash session.
        open_t = max(open_t, time(9, 30))
        close_t = min(close_t, time(16, 0))
    return open_t <= dt.time() <= close_t


def is_underlying_active_session(
    dt: Optional[datetime] = None, symbol: Optional[str] = None
) -> bool:
    """Return True when the symbol's underlying feed is expected to print.

    Symbol-aware "freeze" window. Outside this window the underlying
    naturally trails (cash equity feed stops at 16:00 ET; cash-index feeds
    stop the same), and the system is expected to be in "frozen" state —
    APIs continue serving the last in-session snapshot persisted by the
    analytics engine, but no new live data is arriving.

    * **Stocks/ETFs**  (SPY, QQQ, AAPL …): 04:00 ET → 20:00 ET on weekdays
      (extended-hours cash trading).
    * **Cash indexes** (SPX, NDX …):       09:30 ET → 16:00 ET on weekdays
      (regular cash session only — the index level itself does not print
      pre/after-hours).

    Weekends and configured NYSE holidays always return False.

    Used by the analytics engine to decide whether to apply the tight
    underlying-staleness gate (in-session: a stuck feed is a real anomaly)
    or skip it (off-session: the gap between the latest option print and
    the cash-close underlying snapshot is structural, not a fault).
    """
    dt = _to_et(dt)
    if dt.weekday() > 4 or dt.date() in NYSE_HOLIDAYS:
        return False
    if symbol and is_cash_index(symbol):
        open_t, close_t = time(9, 30), time(16, 0)
    else:
        open_t, close_t = time(4, 0), time(20, 0)
    return open_t <= dt.time() <= close_t


# ---------------------------------------------------------------------------
# CME equity-index futures session (for the out-of-cash-session display swap)
# ---------------------------------------------------------------------------

# CME Globex equity-index futures (ES, NQ, RTY, YM) trade nearly 24h:
#   Sunday 18:00 ET  →  Friday 17:00 ET
# with a daily maintenance break 17:00–18:00 ET.  This is DISPLAY-only
# session logic — used to decide when a cash-index quote/candle surface
# should show its futures equivalent instead of the (frozen) index.
#
# CME holidays (which differ from the NYSE calendar) are intentionally NOT
# modelled here: on a CME holiday the futures feed simply returns no fresh
# bars, which the request-time fetch surfaces as a stale/last print — the
# same graceful degradation as any other quiet period.

_FUTURES_REOPEN = time(18, 0)  # Sunday open / daily reopen after maintenance
_FUTURES_DAILY_CLOSE = time(17, 0)  # Friday close / daily maintenance start


def is_futures_session_open(dt: Optional[datetime] = None) -> bool:
    """True when the CME equity-index futures session is trading at ``dt``.

    Session: Sunday 18:00 ET → Friday 17:00 ET, with a daily maintenance
    break 17:00–18:00 ET.  Weekends outside those bounds are closed.
    """
    dt = _to_et(dt)
    wd = dt.weekday()  # Mon=0 … Sun=6
    t = dt.time()

    # Daily maintenance break (17:00–18:00 ET every trading day).
    if _FUTURES_DAILY_CLOSE <= t < _FUTURES_REOPEN:
        return False
    if wd == 5:  # Saturday — closed all day
        return False
    if wd == 6:  # Sunday — opens at 18:00 ET
        return t >= _FUTURES_REOPEN
    if wd == 4:  # Friday — closes at 17:00 ET
        return t < _FUTURES_DAILY_CLOSE
    return True  # Mon–Thu — open except the 17:00–18:00 break handled above


def is_futures_display_window(dt: Optional[datetime] = None) -> bool:
    """True when the site is in the overnight futures-display window.

    The window is 18:00 ET → next 09:30 ET AND the CME futures session is
    actually trading (so the Friday 17:00 → Sunday 18:00 weekend gap and
    the daily 17:00–18:00 maintenance break are excluded).  This is the
    symbol-agnostic timing gate shared by the display swap
    (:func:`should_display_future`) and the futures ingester, so both agree
    on exactly when the flip happens.
    """
    dt = _to_et(dt)
    t = dt.time()
    in_overnight_window = t >= _FUTURES_REOPEN or t < time(9, 30)
    return in_overnight_window and is_futures_session_open(dt)


def should_display_future(symbol: Optional[str], dt: Optional[datetime] = None) -> bool:
    """True when a cash-index display surface should swap to its future.

    Policy (all times ET): show the cash index (SPX/NDX/…) during the
    regular cash session AND through the post-close / maintenance evening
    hold, then swap to the future only during the overnight futures window
    — 18:00 ET → next 09:30 ET — while the futures market is actually
    trading.  Concretely:

      * 09:30–16:00  cash session      → index (live)
      * 16:00–18:00  post-close / break→ index (frozen cash close)
      * 18:00–09:30  overnight futures → future  (when the future is open)
      * Fri 17:00 → Sun 18:00 weekend  → index (frozen Friday close)

    Requires ``symbol`` to be a cash index WITH a mapped future; returns
    False otherwise.  DISPLAY-ONLY — never used to key data, persistence,
    or analytics.
    """
    if not symbol or not is_cash_index(symbol):
        return False
    if resolve_index_future(symbol) is None:
        return False
    return is_futures_display_window(dt)


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
