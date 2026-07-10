"""Pattern: ``vix_regime_breakout`` — Volatility-Expansion Breakout.

When VIX is elevated and net gamma is negative (short-gamma, amplifying), range
breaks tend to run as dealers hedge pro-trend. This trades a break of the recent
session extreme in the direction of the break. It stands down when volatility is
calm or gamma is positive (suppressive), where breaks tend to fail.

Backtestable twin of the TradeWorkz VIX Regime Breakout bot. (An elevated VIX
level is the live proxy for a rising-volatility regime.)
"""

from __future__ import annotations

import os
from typing import Optional

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import ActionCard, ActionEnum, Entry, Leg, Stop, Target

_VIX_MIN = float(os.getenv("PLAYBOOK_VIX_BREAKOUT_MIN", "18.0"))
_NET_GEX_CEIL = float(os.getenv("PLAYBOOK_VIX_BREAKOUT_NET_GEX_CEIL", "0.0"))
_TARGET_PCT = float(os.getenv("PLAYBOOK_VIX_BREAKOUT_TARGET_PCT", "0.005"))
# Minimum bars of recent range needed to define the extreme being broken.
_MIN_BARS = int(os.getenv("PLAYBOOK_VIX_BREAKOUT_MIN_BARS", "5"))


def _round_to_strike(price: float) -> float:
    return round(price)


class VixRegimeBreakoutPattern(PatternBase):
    id = "vix_regime_breakout"
    name = "Volatility-Expansion Breakout"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = ("trend_expansion", "high_risk_reversal")
    preferred_regime = "trend_expansion"
    pattern_base = 0.52

    confluence_signals_for = ("vol_expansion", "range_break_imminence")
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
            stop_ref = range_high  # back inside the range invalidates
        else:
            direction, action, right = "bearish", ActionEnum.BUY_PUT_DEBIT, "P"
            target_ref = close * (1.0 - _TARGET_PCT)
            stop_ref = range_low

        strike = _round_to_strike(close)
        legs = [Leg(expiry=ctx.et_date.isoformat(), strike=strike, right=right, side="BUY", qty=1)]
        confidence = self.compute_confidence(ctx, bias=direction)
        vix = ctx.market.extra.get("vix_level")

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
            target=Target(
                ref_price=round(target_ref, 4), kind="level", level_name="breakout_extension"
            ),
            stop=Stop(ref_price=round(stop_ref, 4), kind="level", level_name="range_reentry"),
            rationale=(
                f"VIX {vix} elevated + short-gamma; spot ${close:.2f} broke the recent "
                f"{'high' if direction == 'bullish' else 'low'} → ride the {direction} break."
            ),
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "vix_level": vix,
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
        if ctx.net_gex >= _NET_GEX_CEIL:
            missing.append("net_gex not negative (breakouts need short gamma)")
        vix = ctx.market.extra.get("vix_level")
        if vix is None:
            missing.append("vix_level unavailable")
        elif float(vix) < _VIX_MIN:
            missing.append(f"VIX {float(vix):.1f} < {_VIX_MIN:.1f} (calm vol)")
        highs = [h for h in (ctx.market.recent_highs or []) if h and h > 0]
        lows = [low for low in (ctx.market.recent_lows or []) if low and low > 0]
        if len(highs) < _MIN_BARS or len(lows) < _MIN_BARS:
            missing.append("not enough recent bars to define the range")
        elif not (close > max(highs) or close < min(lows)):
            missing.append("price has not broken the recent range")
        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)


PATTERN: PatternBase = VixRegimeBreakoutPattern()
