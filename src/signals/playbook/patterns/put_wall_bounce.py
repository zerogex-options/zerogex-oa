"""Pattern 1.2: ``put_wall_bounce`` — Bounce Off the Put Wall.

Symmetric mirror of ``call_wall_fade``: long-gamma backdrop + price
tagging the put wall + bullish flow + a corroborating advanced signal
= buy the bounce.  Per ``docs/playbook_catalog.md`` §7.1.2.
"""

from __future__ import annotations

import math
import os
from datetime import time
from typing import Optional

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import (
    ActionCard,
    ActionEnum,
    Entry,
    Leg,
    Stop,
    Target,
)

# Env-overridable thresholds (mirror call_wall_fade defaults).
_NET_GEX_FLOOR = float(os.getenv("PLAYBOOK_PWB_NET_GEX_FLOOR", "1.5e9"))
_WALL_PROXIMITY_PCT = float(os.getenv("PLAYBOOK_PWB_WALL_PROXIMITY_PCT", "0.0020"))
_FLOW_BULL_THRESHOLD = float(os.getenv("PLAYBOOK_PWB_FLOW_BULL_THRESHOLD", "20"))
_VOL_DEBIT_SWITCH = float(os.getenv("PLAYBOOK_PWB_VOL_DEBIT_SWITCH", "0.0025"))
_STOP_PCT_BELOW_WALL = float(os.getenv("PLAYBOOK_PWB_STOP_PCT", "0.0030"))
_SPREAD_WIDTH_POINTS = float(os.getenv("PLAYBOOK_PWB_SPREAD_WIDTH", "5.0"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_PWB_MAX_HOLD_MIN", "90"))
_VIX_HEADWIND = float(os.getenv("PLAYBOOK_PWB_VIX_HEADWIND", "22.0"))


def _realized_sigma_30min(closes: list[float]) -> float:
    usable = [c for c in (closes or []) if c and c > 0][-30:]
    if len(usable) < 5:
        return 0.0
    rets = [
        (usable[i] - usable[i - 1]) / usable[i - 1]
        for i in range(1, len(usable))
        if usable[i - 1] > 0
    ]
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    return math.sqrt(max(var, 0.0))


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


class PutWallBouncePattern(PatternBase):
    id = "put_wall_bounce"
    name = "Bounce Off the Put Wall"
    tier = "0DTE"
    direction = "bullish"
    valid_regimes = ("chop_range", "high_risk_reversal")
    preferred_regime = "high_risk_reversal"
    pattern_base = 0.55

    confluence_signals_for = (
        "positioning_trap",
        "dealer_delta_pressure",
        "vanna_charm_flow",
    )
    confluence_signals_against = (
        "vol_expansion",
        "range_break_imminence",
    )

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        put_wall = ctx.level("put_wall")
        max_pain = ctx.level("max_pain") or ctx.market.max_pain
        gamma_flip = ctx.market.gamma_flip
        close = ctx.close
        sigma = _realized_sigma_30min(ctx.market.recent_closes)

        # Instrument selection (mirror of call_wall_fade).
        wall_strike = _round_to_strike(put_wall, 1.0)
        expiry = self._zero_dte_expiry(ctx)
        if sigma > _VOL_DEBIT_SWITCH:
            action = ActionEnum.BUY_CALL_DEBIT
            legs = [Leg(expiry=expiry, strike=wall_strike, right="C", side="BUY", qty=1)]
        else:
            action = ActionEnum.SELL_PUT_SPREAD
            long_strike = wall_strike - _SPREAD_WIDTH_POINTS
            legs = [
                Leg(expiry=expiry, strike=wall_strike, right="P", side="SELL", qty=1),
                Leg(expiry=expiry, strike=long_strike, right="P", side="BUY", qty=1),
            ]

        target_ref, target_level_name = self._pick_target(close, max_pain, gamma_flip, wall_strike)
        stop_ref = wall_strike * (1.0 - _STOP_PCT_BELOW_WALL)

        confidence = self.compute_confidence(ctx, bias="bullish")
        vix = (ctx.market.extra or {}).get("vix_level")
        if vix is not None and float(vix) > _VIX_HEADWIND:
            confidence = max(0.20, confidence - 0.10)

        adv_aligned = [
            name
            for name in ("trap_detection", "gamma_vwap_confluence")
            if self._adv_aligned_bullish(ctx, name)
        ]
        basic_aligned = [
            name
            for name in self.confluence_signals_for
            if (snap := ctx.signal(name)) and snap.clamped_score > 0
        ]

        rationale = self._compose_rationale(
            close=close,
            put_wall=wall_strike,
            net_gex=ctx.net_gex,
            adv_aligned=adv_aligned,
            sigma=sigma,
            action=action,
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=self.direction,
            confidence=confidence,
            size_multiplier=0.6,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_touch"),
            target=Target(
                ref_price=target_ref,
                kind="level" if target_level_name else "premium_pct",
                level_name=target_level_name,
            ),
            stop=Stop(
                ref_price=stop_ref,
                kind="premium_pct",
                level_name="put_wall_break",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "put_wall": wall_strike,
                "max_pain": max_pain,
                "gamma_flip": gamma_flip,
                "vwap": ctx.market.vwap,
                "realized_sigma_30min": round(sigma, 6),
                "advanced_signals_aligned": adv_aligned,
                "basic_signals_aligned": basic_aligned,
                "vix_level": vix,
            },
        )

    # ------------------------------------------------------------------
    # Trigger checks
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []

        if ctx.net_gex is None or ctx.net_gex <= _NET_GEX_FLOOR:
            missing.append(
                f"net_gex {ctx.net_gex} <= floor {_NET_GEX_FLOOR:.0f} (need long-gamma backdrop)"
            )

        put_wall = ctx.level("put_wall")
        if put_wall is None:
            missing.append("put_wall level unavailable")
        elif ctx.close <= 0:
            missing.append("close price unavailable")
        else:
            distance_pct = abs(ctx.close - put_wall) / ctx.close
            if distance_pct > _WALL_PROXIMITY_PCT:
                missing.append(
                    f"price {distance_pct * 100:.2f}% from put_wall "
                    f"(needs <= {_WALL_PROXIMITY_PCT * 100:.2f}%)"
                )

        if ctx.et_time < time(10, 0):
            missing.append(f"too early ({ctx.et_time} ET); needs >= 10:00 ET")

        tape = ctx.signal("tape_flow_bias")
        ofi = ctx.signal("order_flow_imbalance")
        flow_ok = (tape and tape.score >= _FLOW_BULL_THRESHOLD) or (
            ofi and ofi.score >= _FLOW_BULL_THRESHOLD
        )
        if not flow_ok:
            missing.append(
                f"no bullish flow signal: tape_flow_bias={tape.score if tape else None}, "
                f"order_flow_imbalance={ofi.score if ofi else None}"
            )

        if not (
            self._adv_aligned_bullish(ctx, "trap_detection")
            or self._adv_aligned_bullish(ctx, "gamma_vwap_confluence")
        ):
            missing.append(
                "no corroborating advanced signal "
                "(trap_detection != 'bullish_fade' AND gamma_vwap_confluence != 'bullish_confluence')"
            )

        rbi = ctx.signal("range_break_imminence")
        if rbi and rbi.context_values.get("label") == "Breakout Mode":
            missing.append("range_break_imminence is in 'Breakout Mode' (trend overrides walls)")

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _adv_aligned_bullish(ctx: PlaybookContext, signal_name: str) -> bool:
        snap = ctx.signal(signal_name)
        if not snap:
            return False
        if signal_name == "trap_detection":
            return snap.signal == "bullish_fade"
        if signal_name == "gamma_vwap_confluence":
            return snap.signal == "bullish_confluence"
        return snap.clamped_score > 0

    @staticmethod
    def _pick_target(
        close: float,
        max_pain: Optional[float],
        gamma_flip: Optional[float],
        wall: float,
    ) -> tuple[Optional[float], Optional[str]]:
        """Mirror of call_wall_fade: bullish targets sit *above* close."""
        if max_pain is not None and max_pain > close:
            return float(max_pain), "max_pain"
        if gamma_flip is not None and gamma_flip > close:
            return float(gamma_flip), "gamma_flip"
        return None, None

    @staticmethod
    def _zero_dte_expiry(ctx: PlaybookContext) -> str:
        return ctx.et_date.isoformat()

    @staticmethod
    def _compose_rationale(
        close: float,
        put_wall: float,
        net_gex: float,
        adv_aligned: list[str],
        sigma: float,
        action: ActionEnum,
    ) -> str:
        parts = [
            f"Price ${close:.2f} bouncing off put wall ${put_wall:.2f}",
            f"net GEX ${net_gex / 1e9:.1f}B (long-gamma backdrop)",
        ]
        if adv_aligned:
            parts.append("confirmed by " + " + ".join(adv_aligned))
        if sigma > 0:
            parts.append(f"30-min σ {sigma * 100:.2f}%")
        instrument_word = (
            "call debit" if action == ActionEnum.BUY_CALL_DEBIT else "put credit spread"
        )
        parts.append(f"→ {instrument_word} at the wall.")
        return ", ".join(parts[:-1]) + " " + parts[-1]


PATTERN: PatternBase = PutWallBouncePattern()
