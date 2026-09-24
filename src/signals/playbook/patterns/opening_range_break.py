"""Pattern: ``opening_range_break`` — Early-Session Range Break.

After the market's first half hour, a decisive break of the opening range
often sets the day's direction. Between 10:00 and 11:30 ET this trades a break
of the 09:30-10:00 ET high/low in the direction of the break. It stands down
before 10:00 (the range is still forming) and after 11:30 (the edge fades).

The range comes from ``ctx.levels`` (``opening_range_high`` /
``opening_range_low``), which both context builders fill from
:mod:`src.opening_range`. It used to be the max/min of the trailing bars,
including the bar the close came from -- a close can never sit outside its own
bar, so the pattern could never fire.

Backtestable twin of the TradeWorkz Opening Range Hunter bot.
"""

from __future__ import annotations

import os
from datetime import time
from typing import Optional

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import ActionCard, ActionEnum, Entry, Leg, Stop, Target

_START_ET = time(10, 0)
_END_ET = time(11, 30)
_TARGET_PCT = float(os.getenv("PLAYBOOK_ORB_TARGET_PCT", "0.005"))


def _round_to_strike(price: float) -> float:
    return round(price)


def _opening_range(ctx: PlaybookContext) -> Optional[tuple[float, float]]:
    """``(high, low)`` of this session's 09:30-10:00 ET range, or ``None``."""
    high = ctx.level("opening_range_high")
    low = ctx.level("opening_range_low")
    if high is None or low is None or high <= 0 or low <= 0 or high < low:
        return None
    return float(high), float(low)


class OpeningRangeBreakPattern(PatternBase):
    id = "opening_range_break"
    name = "Opening Range Break"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = ("trend_expansion", "controlled_trend", "high_risk_reversal")
    preferred_regime = "trend_expansion"
    pattern_base = 0.52

    confluence_signals_for = ("range_break_imminence", "vol_expansion")
    confluence_signals_against = ("positioning_trap",)

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        opening_range = _opening_range(ctx)
        if opening_range is None or self._check_triggers(ctx):
            return None
        close = ctx.close
        range_high, range_low = opening_range

        if close > range_high:
            direction, action, right = "bullish", ActionEnum.BUY_CALL_DEBIT, "C"
            target_ref = close * (1.0 + _TARGET_PCT)
            stop_ref = range_low  # back into the range invalidates
            broken_edge = range_high
        else:
            direction, action, right = "bearish", ActionEnum.BUY_PUT_DEBIT, "P"
            target_ref = close * (1.0 - _TARGET_PCT)
            stop_ref = range_high
            broken_edge = range_low

        strike = _round_to_strike(close)
        legs = [Leg(expiry=ctx.et_date.isoformat(), strike=strike, right=right, side="BUY", qty=1)]
        confidence = self.compute_confidence(ctx, bias=direction)

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=direction,
            confidence=confidence,
            size_multiplier=0.6,
            max_hold_minutes=120,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_market"),
            target=Target(ref_price=round(target_ref, 4), kind="level", level_name="orb_extension"),
            stop=Stop(ref_price=round(stop_ref, 4), kind="level", level_name="orb_reentry"),
            rationale=(
                f"Spot ${close:.2f} broke the 09:30–10:00 opening-range "
                f"{'high' if direction == 'bullish' else 'low'} ${broken_edge:.2f} "
                f"→ trade the {direction} break."
            ),
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "range_high": range_high,
                "range_low": range_low,
                "close": close,
            },
        )

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        close = ctx.close
        if close <= 0:
            missing.append("close price unavailable")
        if not (_START_ET <= ctx.et_time <= _END_ET):
            missing.append(f"outside 10:00–11:30 ET window ({ctx.et_time} ET)")
        opening_range = _opening_range(ctx)
        if opening_range is None:
            missing.append("09:30–10:00 opening range unavailable")
        elif not (close > opening_range[0] or close < opening_range[1]):
            missing.append("price has not broken the opening range")
        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)


PATTERN: PatternBase = OpeningRangeBreakPattern()
