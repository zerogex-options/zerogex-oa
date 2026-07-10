"""Pattern: ``max_pain_gravitation`` — Drift Back to Max Pain.

When price is displaced from the max-pain strike in a positive-gamma (pinning)
regime, dealer hedging tends to pull it back toward max pain. This fades the
displacement — buy calls when spot sits below max pain, buy puts when it sits
above — targeting the max-pain level. It stands down in negative-gamma regimes,
where dealer hedging amplifies moves instead of pinning them.

Backtestable twin of the TradeWorkz Max Pain Gravitator bot.
"""

from __future__ import annotations

import os
from typing import Optional

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import ActionCard, ActionEnum, Entry, Leg, Stop, Target

# Minimum |spot − max_pain| / spot to consider price "displaced" enough to fade.
_DISPLACE_MIN_PCT = float(os.getenv("PLAYBOOK_MAXPAIN_DISPLACE_MIN_PCT", "0.003"))
# How far further from max pain price must move to invalidate (the stop).
_STOP_EXT_PCT = float(os.getenv("PLAYBOOK_MAXPAIN_STOP_EXT_PCT", "0.004"))
# Long-gamma floor: pinning needs a positive-gamma backdrop.
_NET_GEX_FLOOR = float(os.getenv("PLAYBOOK_MAXPAIN_NET_GEX_FLOOR", "0.0"))


def _round_to_strike(price: float) -> float:
    return round(price)


class MaxPainGravitationPattern(PatternBase):
    id = "max_pain_gravitation"
    name = "Drift Back to Max Pain"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = ("controlled_trend", "chop_range")
    preferred_regime = "chop_range"
    pattern_base = 0.53

    confluence_signals_for = ("positioning_trap",)
    confluence_signals_against = ("vol_expansion",)

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        if self._check_triggers(ctx):
            return None
        close = ctx.close
        max_pain = ctx.market.max_pain
        assert max_pain is not None  # guaranteed by _check_triggers
        direction = "bullish" if close < max_pain else "bearish"

        if direction == "bullish":
            action, right = ActionEnum.BUY_CALL_DEBIT, "C"
            stop_ref = close * (1.0 - _STOP_EXT_PCT)  # falling further from max pain invalidates
        else:
            action, right = ActionEnum.BUY_PUT_DEBIT, "P"
            stop_ref = close * (1.0 + _STOP_EXT_PCT)

        strike = _round_to_strike(close)
        legs = [Leg(expiry=ctx.et_date.isoformat(), strike=strike, right=right, side="BUY", qty=1)]
        confidence = self.compute_confidence(ctx, bias=direction)
        displaced_pct = abs(close - max_pain) / close * 100.0

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
            target=Target(ref_price=round(max_pain, 4), kind="level", level_name="max_pain"),
            stop=Stop(ref_price=round(stop_ref, 4), kind="level", level_name="max_pain_extension"),
            rationale=(
                f"Spot ${close:.2f} is {displaced_pct:.2f}% from max pain ${max_pain:.2f} "
                f"in a long-gamma regime → fade back toward max pain."
            ),
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "max_pain": max_pain,
                "displaced_pct": displaced_pct,
                "close": close,
            },
        )

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        close = ctx.close
        max_pain = ctx.market.max_pain
        if close <= 0:
            missing.append("close price unavailable")
        if max_pain is None or max_pain <= 0:
            missing.append("max_pain unavailable")
        if ctx.net_gex <= _NET_GEX_FLOOR:
            missing.append("net_gex not positive (pinning needs long gamma)")
        if close > 0 and max_pain and abs(close - max_pain) / close < _DISPLACE_MIN_PCT:
            missing.append(
                f"not displaced enough from max pain "
                f"({abs(close - max_pain) / close * 100:.2f}% < {_DISPLACE_MIN_PCT * 100:.2f}%)"
            )
        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)


PATTERN: PatternBase = MaxPainGravitationPattern()
