"""Implied cash-index spot from the index future (overnight-basis proxy).

Outside the regular cash session a cash index (SPX / NDX / …) has no live
underlying print — its most recent ``underlying_quotes`` row is the frozen
prior 16:00 cash close.  Anchoring the morning forecast (or a pre-market
bulletin) on that frozen level is wrong: SPX doesn't trade overnight, so
the number ignores wherever the futures have moved the market since the
bell.  This module projects an *implied* cash level from the mapped
continuous future using a constant-overnight-basis proxy::

    implied = prior_cash_close + (future_now - future_ref)

``future_ref`` is the future's own 16:00 ET print of the current overnight
session (``get_latest_future_quote`` derives it from the same
``current_cash_close_reference`` anchor), so the index↔future basis at the
close cancels and the projection tracks the future's overnight *move*
applied to the cash close — exactly how a trader reads "where is SPX
implied right now" off @ES.

In-session (or during the 16:00-18:00 evening hold, or on weekends) the
frozen cash close is the correct value, so the helper returns ``None`` and
callers keep the cash value untouched.  It also returns ``None`` when the
futures ingester has no usable bar yet, so an empty feed degrades
gracefully to the frozen close rather than a bad projection.

FORECAST-ANCHOR / DISPLAY ONLY — the projected level is never written back
to ``underlying_quotes``, GEX, greeks, signals or settlement.  It only
substitutes the spot the forecast/bulletin *renders*.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from src.market_calendar import current_cash_close_reference, should_display_future
from src.symbols import is_cash_index, resolve_index_future

# How many days around the measured roll the continuous future may switch
# contracts on (see futures_projection._ROLL_DAYS_BEFORE_EXPIRY: measured once,
# not the provider's published rule).
_ROLL_SLACK_DAYS = 1

logger = logging.getLogger("zerogex.index_projection")


@dataclass(frozen=True)
class ImpliedIndexSpot:
    """Result of a futures-implied cash-index spot projection.

    ``implied_price`` is what a caller substitutes for the frozen cash spot;
    the remaining fields are kept for audit/logging and for the "projected
    via @ES" indicator surfaced to users.
    """

    symbol: str
    implied_price: float
    cash_ref_close: float
    future_now: float
    future_ref: float
    future_symbol: str  # continuous future used, e.g. "@ES"

    @property
    def gap_points(self) -> float:
        """Overnight move of the future in index points."""
        return self.future_now - self.future_ref

    @property
    def gap_pct(self) -> float:
        """Overnight move of the future as a fraction of its reference close."""
        return (self.future_now - self.future_ref) / self.future_ref

    def as_audit(self) -> dict[str, Any]:
        """Compact dict for the forecast_inputs JSONB / log lines."""
        return {
            "source": "futures_implied",
            "future_symbol": self.future_symbol,
            "implied_price": self.implied_price,
            "cash_ref_close": self.cash_ref_close,
            "future_now": self.future_now,
            "future_ref": self.future_ref,
            "gap_pct": round(self.gap_pct, 6),
        }


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def continuous_future_may_have_rolled(ref_at: datetime, now_at: datetime) -> bool:
    """True when @ES / @NQ may have switched contracts between the two instants.

    The overnight move is read off the CONTINUOUS future, ``F_now - F_ref``. If
    the series rolled in between, the two prints are different contracts a
    quarter apart, and the difference is the calendar spread, not a move: on
    the Sep 2026 roll the ES basis stepped from 8.1bp to 93.7bp overnight,
    which would have centered the SPX morning band ~60 points (NDX ~250) too
    high. The roll date is a measurement, not the provider's rule, so a day
    either side of it counts too.
    """
    from src.jobs.futures_projection import _ROLL_DAYS_BEFORE_EXPIRY, active_contract_expiry

    for roll_days in range(
        _ROLL_DAYS_BEFORE_EXPIRY - _ROLL_SLACK_DAYS,
        _ROLL_DAYS_BEFORE_EXPIRY + _ROLL_SLACK_DAYS + 1,
    ):
        if active_contract_expiry(ref_at, roll_days) != active_contract_expiry(now_at, roll_days):
            return True
    return False


async def implied_index_spot(
    db: Any, symbol: str, *, at: Optional[datetime] = None
) -> Optional[ImpliedIndexSpot]:
    """Futures-implied cash spot for ``symbol``, or ``None`` when N/A.

    ``None`` (caller keeps the frozen cash close) is returned when:

      * ``symbol`` is not a cash index with a mapped future, or
      * we are inside the cash session / evening hold / weekend
        (``should_display_future`` is False — the cash value is right then),
      * the futures feed has no usable bar, or
      * the continuous future may have rolled to the next contract since the
        reference close (the difference would be the calendar spread), or
      * the prior cash close is unavailable.

    ``at`` overrides "now" for testing / backfill; live callers pass None.

    Requires ``db`` to expose ``get_latest_future_quote`` and
    ``get_previous_close`` (the ``DatabaseManager`` used by both the
    forecast writer and the bulletin does).
    """
    normalized = (symbol or "").strip().upper()
    if not is_cash_index(normalized) or resolve_index_future(normalized) is None:
        return None
    # Only project during the overnight futures window; in-session / evening
    # hold / weekend the frozen cash close is the correct number.
    if not should_display_future(normalized, at):
        return None

    reference_at = current_cash_close_reference(at)
    if continuous_future_may_have_rolled(reference_at, at or datetime.now(timezone.utc)):
        logger.info(
            "implied_index_spot: %s continuous future may have rolled since %s; "
            "keeping the cash close",
            normalized,
            reference_at.isoformat(),
        )
        return None
    fut = await db.get_latest_future_quote(normalized, reference_at)
    if not fut:
        return None
    future_now = _to_float(fut.get("close"))
    future_ref = _to_float(fut.get("reference_close"))
    if future_now is None or future_ref is None or future_ref <= 0:
        return None

    prev = await db.get_previous_close(normalized)
    cash_ref = _to_float(prev.get("previous_close")) if prev else None
    if cash_ref is None or cash_ref <= 0:
        return None

    future_symbol = str(fut.get("future_symbol") or resolve_index_future(normalized) or "")
    implied = cash_ref + (future_now - future_ref)
    return ImpliedIndexSpot(
        symbol=normalized,
        implied_price=round(implied, 4),
        cash_ref_close=cash_ref,
        future_now=future_now,
        future_ref=future_ref,
        future_symbol=future_symbol,
    )
