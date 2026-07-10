"""Pattern: ``opening_range_break`` — Early-Session Range Break.

After the market's first half hour, a decisive break of the early-session range
often sets the day's direction. Between 10:00 and 11:30 ET this trades a break
of the session's early high/low in the direction of the break. It stands down
before 10:00 (the range is still forming) and after 11:30 (the edge fades).

Backtestable twin of the TradeWorkz Opening Range Hunter bot. (Uses the recent
session high/low as the live range proxy.)
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
_MIN_BARS = int(os.getenv("PLAYBOOK_ORB_MIN_BARS", "5"))


def _round_to_strike(price: float) -> float:
    return round(price)


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
        if self._check_triggers(ctx):
            return None
        close = ctx.close
        highs = [h for h in (ctx.market.recent_highs or []) if h and h > 0]
        lows = [low for low in (ctx.market.recent_lows or []) if low and low > 0]
        range_high, range_low = max(highs), min(lows)

        if close > range_high:
            direction, action, right = "bullish", ActionEnum.BUY_CALL_DEBIT, "C"
            target_ref = close * (1.0 + _TARGET_PCT)
            stop_ref = range_low  # back into the range invalidates
        else:
            direction, action, right = "bearish", ActionEnum.BUY_PUT_DEBIT, "P"
            target_ref = close * (1.0 - _TARGET_PCT)
            stop_ref = range_high

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
                f"Spot ${close:.2f} broke the early-session "
                f"{'high' if direction == 'bullish' else 'low'} in the 10:00–11:30 window "
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
        highs = [h for h in (ctx.market.recent_highs or []) if h and h > 0]
        lows = [low for low in (ctx.market.recent_lows or []) if low and low > 0]
        if len(highs) < _MIN_BARS or len(lows) < _MIN_BARS:
            missing.append("not enough recent bars to define the opening range")
        elif not (close > max(highs) or close < min(lows)):
            missing.append("price has not broken the early-session range")
        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)


PATTERN: PatternBase = OpeningRangeBreakPattern()
