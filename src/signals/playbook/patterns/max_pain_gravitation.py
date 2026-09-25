"""Pattern: ``max_pain_gravitation`` — Drift Back to Max Pain.

When price is displaced from the max-pain strike in a positive-gamma (pinning)
regime, dealer hedging tends to pull it back toward max pain. This fades the
displacement — buy calls when spot sits below max pain, buy puts when it sits
above. It stands down in negative-gamma regimes, where dealer hedging
amplifies moves instead of pinning them.

The exits are sized to the move price typically makes over the hold
(``reach.py``): the target is max pain when it is within reach, otherwise a
point partway toward it; the stop is the same distance the other way. Three
in four of this pattern's Cards used to time out aiming at a max pain that
could be 1%+ away with price barely moving. No Card when the market is too
quiet to reach a worthwhile target.

Backtestable twin of the TradeWorkz Max Pain Gravitator bot.
"""

from __future__ import annotations

import os
from typing import Optional

from src.signals.playbook import reach
from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import ActionCard, ActionEnum, Entry, Leg, Stop, Target

# Minimum |spot − max_pain| / spot to consider price "displaced" enough to fade.
_DISPLACE_MIN_PCT = float(os.getenv("PLAYBOOK_MAXPAIN_DISPLACE_MIN_PCT", "0.003"))
# Stop distance when there is too little bar history to size it to volatility.
_STOP_EXT_PCT = float(os.getenv("PLAYBOOK_MAXPAIN_STOP_EXT_PCT", "0.004"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_MAXPAIN_MAX_HOLD_MIN", "120"))
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
        sizing = self._sizing(ctx, direction)
        hold, exits = sizing.hold, sizing.exits

        if direction == "bullish":
            action, right = ActionEnum.BUY_CALL_DEBIT, "C"
        else:
            action, right = ActionEnum.BUY_PUT_DEBIT, "P"
        if exits is not None:
            target_ref, target_name, stop_ref = exits.target, exits.target_name, exits.stop
        else:  # too little bar history to measure volatility: the original exits
            target_ref, target_name = max_pain, "max_pain"
            sign = 1.0 if direction == "bullish" else -1.0
            stop_ref = close * (1.0 - sign * _STOP_EXT_PCT)

        strike = _round_to_strike(close)
        legs = [Leg(expiry=ctx.et_date.isoformat(), strike=strike, right=right, side="BUY", qty=1)]
        confidence = self.compute_confidence(ctx, bias=direction)
        displaced_pct = abs(close - max_pain) / close * 100.0
        sizing_note = (
            f" Target ${target_ref:.2f}, stop ${stop_ref:.2f}: sized to the "
            f"{exits.move_pct(close):.2f}% price typically moves in {hold}m."
            if exits is not None
            else ""
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=direction,
            confidence=confidence,
            size_multiplier=0.6,
            max_hold_minutes=hold,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_market"),
            target=Target(ref_price=round(target_ref, 4), kind="level", level_name=target_name),
            stop=Stop(ref_price=round(stop_ref, 4), kind="level", level_name="max_pain_extension"),
            rationale=(
                f"Spot ${close:.2f} is {displaced_pct:.2f}% from max pain ${max_pain:.2f} "
                f"in a long-gamma regime → fade back toward max pain.{sizing_note}"
            ),
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "max_pain": max_pain,
                "displaced_pct": displaced_pct,
                "close": close,
                "expected_move_pct": (
                    round(exits.move_pct(close), 4) if exits is not None else None
                ),
            },
        )

    def _sizing(self, ctx: PlaybookContext, direction: str) -> reach.Sizing:
        return reach.plan(
            ts=ctx.timestamp,
            close=ctx.close,
            closes=ctx.market.recent_closes,
            hold_minutes=_MAX_HOLD_MIN,
            tier=self.tier,
            direction=direction,
            entry=ctx.close,
            level=ctx.market.max_pain,
            level_name="max_pain",
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
        if not missing:
            sizing = self._sizing(ctx, "bullish" if close < max_pain else "bearish")
            if sizing.too_quiet:
                missing.append(reach.too_quiet_reason(close, sizing.move, sizing.hold))
        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)


PATTERN: PatternBase = MaxPainGravitationPattern()
