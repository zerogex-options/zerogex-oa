"""CME equity-index futures market calendar (ES / NQ / RTY / YM).

The existing :mod:`src.market_calendar` models the US equity cash session and
has *display-only* CME session helpers (Sun 18:00 ET -> Fri 17:00 ET) but no
CME holidays, no Chicago-time handling, no session labels, and no
0DTE-by-exact-timestamp classification.  This module adds a first-class CME
calendar so futures analytics are never filtered through 09:30-16:00 equity
rules (spec Phase 4).

Times are resolved in **America/Chicago** (CME's home timezone) so DST
transitions are handled correctly for both Chicago and New York, using the
stdlib :mod:`zoneinfo` (as ``src/api/routers/replay.py`` already does).

Session model (Chicago time):

* Trading:   Sun 17:00 CT  ->  Fri 16:00 CT
* Daily maintenance halt:   16:00-17:00 CT every trading day
* Trade date rolls at 17:00 CT — the evening session (17:00 CT onward)
  belongs to the **next** calendar trading day.
* RTH / cash-overlap:  08:30-15:00 CT (aligns with the US cash session
  09:30-16:00 ET), surfaced as a distinct label for cash-session-sensitive
  analytics.

CME holidays differ from the NYSE calendar and are **operator-supplied**
(env ``CME_HOLIDAYS`` / ``CME_EARLY_CLOSE_DATES``, same comma-ISO format as
``NYSE_HOLIDAYS``), with a documented default set for the current window.
Authoritative dates should always be supplied by the operator — the embedded
defaults are a convenience, not a multi-decade source of truth (spec rule
"treat security-definition metadata as authoritative").
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

_REOPEN = time(17, 0)  # 17:00 CT session (re)open
_DAILY_CLOSE = time(16, 0)  # 16:00 CT settlement / maintenance start
_RTH_OPEN = time(8, 30)  # 08:30 CT ~ 09:30 ET cash open
_RTH_CLOSE = time(15, 0)  # 15:00 CT ~ 16:00 ET cash close
_DEFAULT_EARLY_CLOSE = time(12, 0)  # 12:00 CT half-day close

CALENDAR_ID = "cme_equity_index"


class SessionLabel(str, Enum):
    GLOBEX = "globex"  # normal electronic trading (outside cash overlap)
    CASH_OVERLAP = "cash_overlap"  # RTH overlap with US cash session
    MAINTENANCE = "maintenance"  # daily 16:00-17:00 CT halt
    CLOSED = "closed"  # weekend / holiday
    EXPIRING = "expiring"  # a tracked option/future expires this trade date


@dataclass(frozen=True)
class MarketSessionState:
    label: SessionLabel
    is_open: bool
    trade_date: str  # YYYY-MM-DD (CME trade date)


@dataclass(frozen=True)
class DateRange:
    start: datetime
    end: datetime


def _parse_date_set(raw: str) -> frozenset[date]:
    out: set[date] = set()
    for token in (raw or "").split(","):
        token = token.split("#", 1)[0].strip()
        if not token:
            continue
        try:
            out.add(date.fromisoformat(token))
        except ValueError:
            continue
    return frozenset(out)


# Documented default full-closure holidays CME equity-index futures observe.
# Operator env vars override/extend this; kept intentionally short (near-term
# window) rather than pretending to be authoritative for all time.
_DEFAULT_CME_HOLIDAYS: frozenset[date] = frozenset(
    {
        date(2025, 1, 1),  # New Year's Day
        date(2025, 4, 18),  # Good Friday
        date(2025, 5, 26),  # Memorial Day
        date(2025, 6, 19),  # Juneteenth
        date(2025, 7, 4),  # Independence Day
        date(2025, 9, 1),  # Labor Day
        date(2025, 11, 27),  # Thanksgiving
        date(2025, 12, 25),  # Christmas
        date(2026, 1, 1),
        date(2026, 4, 3),  # Good Friday
        date(2026, 5, 25),  # Memorial Day
        date(2026, 6, 19),
        date(2026, 7, 3),  # Independence Day (observed)
        date(2026, 9, 7),  # Labor Day
        date(2026, 11, 26),  # Thanksgiving
        date(2026, 12, 25),
    }
)

_DEFAULT_CME_EARLY_CLOSES: frozenset[date] = frozenset(
    {
        date(2025, 7, 3),
        date(2025, 11, 28),  # Black Friday
        date(2025, 12, 24),
        date(2026, 11, 27),  # Black Friday
        date(2026, 12, 24),
    }
)


class CMEEquityIndexCalendar:
    """Calendar for the CME Globex equity-index futures session.

    Instances read holiday overrides from the environment once at
    construction, so a long-running worker sees a stable calendar; tools that
    need to pick up new env values simply construct a fresh instance.
    """

    id = CALENDAR_ID

    def __init__(
        self,
        holidays: Optional[frozenset[date]] = None,
        early_closes: Optional[frozenset[date]] = None,
    ) -> None:
        env_hol = _parse_date_set(os.getenv("CME_HOLIDAYS", ""))
        env_early = _parse_date_set(os.getenv("CME_EARLY_CLOSE_DATES", ""))
        self._holidays = (holidays or _DEFAULT_CME_HOLIDAYS) | env_hol
        self._early_closes = (early_closes or _DEFAULT_CME_EARLY_CLOSES) | env_early

    # -- time helpers ------------------------------------------------------

    @staticmethod
    def _to_ct(dt: Optional[datetime]) -> datetime:
        if dt is None:
            # Callers in tests always pass an explicit dt; production callers
            # pass a real "now".  Avoid an implicit clock here so the module
            # stays deterministic under test.
            raise ValueError("CMEEquityIndexCalendar requires an explicit datetime")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(CT)

    def is_holiday(self, d: date) -> bool:
        return d in self._holidays

    def is_early_close(self, d: date) -> bool:
        return d in self._early_closes

    # -- trade date --------------------------------------------------------

    def _trade_date_ct(self, ct: datetime) -> date:
        """The CME trade date a Chicago-time instant belongs to.

        Rolls at 17:00 CT (evening session -> next day) and forward over
        **weekends** so a Friday-evening or weekend timestamp maps to the
        upcoming Monday.  Holidays are deliberately NOT rolled here: the
        nominal trade date may land on a holiday so that :meth:`is_trading`
        can detect the closure via ``is_holiday(trade_date)`` — rolling past
        it here would make every holiday look tradable.
        """
        base = ct.date()
        if ct.time() >= _REOPEN:
            base = base + timedelta(days=1)
        # Roll forward over the weekend gap only.
        for _ in range(3):  # Sat/Sun -> Mon at most
            if base.weekday() >= 5:
                base = base + timedelta(days=1)
                continue
            break
        return base

    def get_trade_date(self, dt: datetime) -> str:
        return self._trade_date_ct(self._to_ct(dt)).isoformat()

    # -- session state -----------------------------------------------------

    def is_maintenance_window(self, dt: datetime) -> bool:
        ct = self._to_ct(dt)
        return _DAILY_CLOSE <= ct.time() < _REOPEN and ct.weekday() != 5

    def is_trading(self, dt: datetime) -> bool:
        ct = self._to_ct(dt)
        t = ct.time()
        wd = ct.weekday()  # Mon=0 .. Sun=6

        # Weekend gap: Fri 16:00 CT -> Sun 17:00 CT.
        if wd == 5:  # Saturday
            return False
        if wd == 6 and t < _REOPEN:  # Sunday before reopen
            return False
        if wd == 4 and t >= _DAILY_CLOSE:  # Friday after close
            return False
        # Daily maintenance halt.
        if _DAILY_CLOSE <= t < _REOPEN:
            return False

        trade_date = self._trade_date_ct(ct)
        if self.is_holiday(trade_date):
            return False
        # Early close truncates the DAY portion of this trade date's session.
        if self.is_early_close(trade_date) and t < _REOPEN and t >= _DEFAULT_EARLY_CLOSE:
            return False
        return True

    def get_session(self, dt: datetime) -> MarketSessionState:
        ct = self._to_ct(dt)
        trade_date = self._trade_date_ct(ct)
        if not self.is_trading(dt):
            label = (
                SessionLabel.MAINTENANCE if self.is_maintenance_window(dt) else SessionLabel.CLOSED
            )
            return MarketSessionState(label=label, is_open=False, trade_date=trade_date.isoformat())

        t = ct.time()
        # Cash-overlap window (08:30-15:00 CT) on a weekday day session.
        if _RTH_OPEN <= t < _RTH_CLOSE and ct.weekday() < 5:
            label = SessionLabel.CASH_OVERLAP
        else:
            label = SessionLabel.GLOBEX
        return MarketSessionState(label=label, is_open=True, trade_date=trade_date.isoformat())

    # -- session boundaries ------------------------------------------------

    def get_session_open(self, trade_date: str | date) -> datetime:
        """The 17:00 CT open of the session for ``trade_date`` (prior evening)."""
        td = date.fromisoformat(trade_date) if isinstance(trade_date, str) else trade_date
        open_day = td - timedelta(days=1)
        # Sunday-opened week: Monday's session opens Sunday 17:00 CT.  For a
        # Monday trade date the prior day is Sunday, which is correct.
        return datetime.combine(open_day, _REOPEN, tzinfo=CT)

    def get_session_close(self, trade_date: str | date) -> datetime:
        td = date.fromisoformat(trade_date) if isinstance(trade_date, str) else trade_date
        close_t = _DEFAULT_EARLY_CLOSE if self.is_early_close(td) else _DAILY_CLOSE
        return datetime.combine(td, close_t, tzinfo=CT)

    def get_settlement_window(self, trade_date: str | date) -> Optional[DateRange]:
        """The 16:00-17:00 CT settlement/maintenance window for a trade date."""
        td = date.fromisoformat(trade_date) if isinstance(trade_date, str) else trade_date
        if self.is_holiday(td):
            return None
        return DateRange(
            start=datetime.combine(td, _DAILY_CLOSE, tzinfo=CT),
            end=datetime.combine(td, _REOPEN, tzinfo=CT),
        )

    # -- expiration / DTE --------------------------------------------------

    def classify_dte(self, now: datetime, expiration: datetime) -> int:
        """Whole trade-days between ``now`` and an exact expiration instant.

        0 == expires on the current trade date (0DTE), based on the CME trade
        date of each instant — NOT a naive calendar-day subtraction, so an
        overnight timestamp is correctly bucketed against the session it
        trades in.  Never negative (expired -> 0).
        """
        now_td = self._trade_date_ct(self._to_ct(now))
        exp_td = self._trade_date_ct(self._to_ct(expiration))
        return max(0, (exp_td - now_td).days)

    def is_zero_dte(self, now: datetime, expiration: datetime) -> bool:
        """True when the option/future expires on the current trade date.

        Uses the exact expiration instant: an option that already settled
        earlier today is not 0DTE (it's expired), so we also require the
        expiration to be in the future.
        """
        if expiration <= now:
            return False
        return self.classify_dte(now, expiration) == 0


#: Process-wide default instance (holidays from env at first import).  Tools
#: that mutate the env after import should construct their own instance.
DEFAULT_CME_CALENDAR = CMEEquityIndexCalendar()
