"""Pattern: ``dealer_delta_pressure`` — Ride Dealer Hedging Pressure.

In a negative-gamma (amplifying) regime, a large dealer net-delta imbalance
means market makers must hedge in the direction of the move, adding fuel. This
leans into that push — the direction the ``dealer_delta_pressure`` signal is
leaning — with an ATM 0DTE debit. It stands down in positive-gamma regimes,
where dealer hedging dampens rather than accelerates a move.

Backtestable twin of the TradeWorkz Dealer Delta Pressure Rider bot.
"""

from __future__ import annotations

import os
from typing import Optional

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import ActionCard, ActionEnum, Entry, Leg, Stop, Target

# Minimum |dealer_delta_pressure signal score| (SignalSnapshot.score is [-100, 100]).
_SCORE_MIN = float(os.getenv("PLAYBOOK_DEALER_DELTA_SCORE_MIN", "30"))
# Short-gamma ceiling: amplification needs a negative-gamma backdrop.
_NET_GEX_CEIL = float(os.getenv("PLAYBOOK_DEALER_DELTA_NET_GEX_CEIL", "0.0"))
_TARGET_PCT = float(os.getenv("PLAYBOOK_DEALER_DELTA_TARGET_PCT", "0.004"))
_STOP_PCT = float(os.getenv("PLAYBOOK_DEALER_DELTA_STOP_PCT", "0.003"))


def _round_to_strike(price: float) -> float:
    return round(price)


class DealerDeltaPressurePattern(PatternBase):
    id = "dealer_delta_pressure"
    name = "Dealer Hedging Pressure"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = ("trend_expansion", "high_risk_reversal")
    preferred_regime = "trend_expansion"
    pattern_base = 0.52

    confluence_signals_for = ("order_flow_imbalance", "tape_flow_bias")
    confluence_signals_against = ("positioning_trap",)

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        if self._check_triggers(ctx):
            return None
        sig = ctx.signal("dealer_delta_pressure")
        assert sig is not None  # guaranteed by _check_triggers
        direction = "bullish" if sig.score > 0 else "bearish"
        close = ctx.close

        if direction == "bullish":
            action, right = ActionEnum.BUY_CALL_DEBIT, "C"
            target_ref = close * (1.0 + _TARGET_PCT)
            stop_ref = close * (1.0 - _STOP_PCT)
        else:
            action, right = ActionEnum.BUY_PUT_DEBIT, "P"
            target_ref = close * (1.0 - _TARGET_PCT)
            stop_ref = close * (1.0 + _STOP_PCT)

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
            max_hold_minutes=90,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_market"),
            target=Target(ref_price=round(target_ref, 4), kind="level", level_name="delta_push"),
            stop=Stop(ref_price=round(stop_ref, 4), kind="level", level_name="delta_fade"),
            rationale=(
                f"Dealer-delta pressure {sig.score:+.2f} in a short-gamma regime "
                f"→ ride the {direction} hedging push."
            ),
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "dealer_delta_score": sig.score,
                "dealer_net_delta": ctx.market.dealer_net_delta,
                "close": close,
            },
        )

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        if ctx.close <= 0:
            missing.append("close price unavailable")
        if ctx.net_gex >= _NET_GEX_CEIL:
            missing.append("net_gex not negative (amplification needs short gamma)")
        sig = ctx.signal("dealer_delta_pressure")
        if sig is None:
            missing.append("dealer_delta_pressure signal unavailable")
        elif abs(sig.score) < _SCORE_MIN:
            missing.append(f"dealer_delta_pressure |score| {abs(sig.score):.2f} < {_SCORE_MIN:.2f}")
        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)


PATTERN: PatternBase = DealerDeltaPressurePattern()
