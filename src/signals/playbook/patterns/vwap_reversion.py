"""Pattern: ``vwap_reversion`` — Mean-Reversion to VWAP.

In a positive-gamma (mean-reverting) regime, price stretched away from the
session VWAP tends to snap back. During the midday session this fades the
stretch — buy calls when price is extended below VWAP, buy puts when extended
above. It stands down in negative-gamma regimes and outside the 10:00–15:00 ET
window, where the effect is weakest.

The exits are sized to the move price typically makes over the hold
(``reach.py``): the target is VWAP when it is within reach, otherwise a point
partway back toward it; the stop is the same distance the other way. Three in
four of this pattern's Cards used to time out aiming at a VWAP that could be
well out of reach. No Card when the market is too quiet to reach a worthwhile
target.

Backtestable twin of the TradeWorkz VWAP Reversion Scalper bot.
"""

from __future__ import annotations

import os
from datetime import time
from typing import Optional

from src.signals.playbook import reach
from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import ActionCard, ActionEnum, Entry, Leg, Stop, Target

# Minimum |close − vwap| / vwap to consider price "stretched" enough to fade.
_STRETCH_MIN_PCT = float(os.getenv("PLAYBOOK_VWAP_STRETCH_MIN_PCT", "0.003"))
# Stop distance when there is too little bar history to size it to volatility.
_STOP_EXT_PCT = float(os.getenv("PLAYBOOK_VWAP_STOP_EXT_PCT", "0.004"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_VWAP_MAX_HOLD_MIN", "90"))
_NET_GEX_FLOOR = float(os.getenv("PLAYBOOK_VWAP_NET_GEX_FLOOR", "0.0"))
_START_ET = time(10, 0)
_END_ET = time(15, 0)


def _round_to_strike(price: float) -> float:
    return round(price)


class VwapReversionPattern(PatternBase):
    id = "vwap_reversion"
    name = "VWAP Mean-Reversion"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = ("controlled_trend", "chop_range")
    preferred_regime = "chop_range"
    pattern_base = 0.53

    confluence_signals_for = ("positioning_trap",)
    confluence_signals_against = ("vol_expansion", "range_break_imminence")

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        if self._check_triggers(ctx):
            return None
        close = ctx.close
        vwap = ctx.market.vwap
        assert vwap is not None  # guaranteed by _check_triggers
        direction = "bullish" if close < vwap else "bearish"
        sizing = self._sizing(ctx, direction)
        hold, exits = sizing.hold, sizing.exits

        if direction == "bullish":
            action, right = ActionEnum.BUY_CALL_DEBIT, "C"
        else:
            action, right = ActionEnum.BUY_PUT_DEBIT, "P"
        if exits is not None:
            target_ref, target_name, stop_ref = exits.target, exits.target_name, exits.stop
        else:  # too little bar history to measure volatility: the original exits
            target_ref, target_name = vwap, "vwap"
            sign = 1.0 if direction == "bullish" else -1.0
            stop_ref = close * (1.0 - sign * _STOP_EXT_PCT)

        strike = _round_to_strike(close)
        legs = [Leg(expiry=ctx.et_date.isoformat(), strike=strike, right=right, side="BUY", qty=1)]
        confidence = self.compute_confidence(ctx, bias=direction)
        stretch_pct = abs(close - vwap) / vwap * 100.0
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
            stop=Stop(ref_price=round(stop_ref, 4), kind="level", level_name="vwap_extension"),
            rationale=(
                f"Spot ${close:.2f} stretched {stretch_pct:.2f}% from VWAP ${vwap:.2f} "
                f"in a long-gamma regime → fade back toward VWAP.{sizing_note}"
            ),
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "vwap": vwap,
                "stretch_pct": stretch_pct,
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
            level=ctx.market.vwap,
            level_name="vwap",
        )

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        close = ctx.close
        vwap = ctx.market.vwap
        if close <= 0:
            missing.append("close price unavailable")
        if vwap is None or vwap <= 0:
            missing.append("vwap unavailable")
        if ctx.net_gex <= _NET_GEX_FLOOR:
            missing.append("net_gex not positive (mean-reversion needs long gamma)")
        if not (_START_ET <= ctx.et_time < _END_ET):
            missing.append(f"outside 10:00–15:00 ET window ({ctx.et_time} ET)")
        if vwap and vwap > 0 and abs(close - vwap) / vwap < _STRETCH_MIN_PCT:
            missing.append(
                f"not stretched enough from VWAP "
                f"({abs(close - vwap) / vwap * 100:.2f}% < {_STRETCH_MIN_PCT * 100:.2f}%)"
            )
        if not missing:
            sizing = self._sizing(ctx, "bullish" if close < vwap else "bearish")
            if sizing.too_quiet:
                missing.append(reach.too_quiet_reason(close, sizing.move, sizing.hold))
        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)


PATTERN: PatternBase = VwapReversionPattern()
