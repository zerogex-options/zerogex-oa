"""Pattern 1.1: ``call_wall_fade`` — Fade Touches of the Call Wall.

Long-gamma backdrop (positive net GEX) + price tagging the call wall +
flow turning negative + a corroborating advanced signal = sell into the
wall.  Per ``docs/playbook_catalog.md`` §7.1.1.
"""

from __future__ import annotations

import math
import os
from datetime import time, timedelta
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

# All thresholds env-overridable so PR-3 backtests can tune without code edits.
_NET_GEX_FLOOR = float(os.getenv("PLAYBOOK_CWF_NET_GEX_FLOOR", "1.5e9"))
_WALL_PROXIMITY_PCT = float(os.getenv("PLAYBOOK_CWF_WALL_PROXIMITY_PCT", "0.0020"))
_FLOW_BEAR_THRESHOLD = float(os.getenv("PLAYBOOK_CWF_FLOW_BEAR_THRESHOLD", "-20"))
_VOL_DEBIT_SWITCH = float(os.getenv("PLAYBOOK_CWF_VOL_DEBIT_SWITCH", "0.0025"))
_STOP_PCT_ABOVE_WALL = float(os.getenv("PLAYBOOK_CWF_STOP_PCT", "0.0030"))
_SPREAD_WIDTH_POINTS = float(os.getenv("PLAYBOOK_CWF_SPREAD_WIDTH", "5.0"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_CWF_MAX_HOLD_MIN", "90"))
_VIX_HEADWIND = float(os.getenv("PLAYBOOK_CWF_VIX_HEADWIND", "22.0"))


def _realized_sigma_30min(closes: list[float]) -> float:
    """Per-bar standard deviation of returns over the last 30 closes."""
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


class CallWallFadePattern(PatternBase):
    id = "call_wall_fade"
    name = "Fade Touches of the Call Wall"
    tier = "0DTE"
    direction = "bearish"
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
        # 1) Hard triggers — any miss bails out.
        miss = self._check_triggers(ctx)
        if miss:
            return None

        call_wall = ctx.level("call_wall")
        max_pain = ctx.level("max_pain") or ctx.market.max_pain
        gamma_flip = ctx.market.gamma_flip
        close = ctx.close
        sigma = _realized_sigma_30min(ctx.market.recent_closes)

        # 2) Instrument selection.
        wall_strike = _round_to_strike(call_wall, 1.0)
        expiry = self._zero_dte_expiry(ctx)
        if sigma > _VOL_DEBIT_SWITCH:
            action = ActionEnum.BUY_PUT_DEBIT
            legs = [Leg(expiry=expiry, strike=wall_strike, right="P", side="BUY", qty=1)]
            stop_kind = "premium_pct"
            stop_premium_target = "-50% premium"
        else:
            action = ActionEnum.SELL_CALL_SPREAD
            long_strike = wall_strike + _SPREAD_WIDTH_POINTS
            legs = [
                Leg(expiry=expiry, strike=wall_strike, right="C", side="SELL", qty=1),
                Leg(expiry=expiry, strike=long_strike, right="C", side="BUY", qty=1),
            ]
            stop_kind = "premium_pct"
            stop_premium_target = "200% credit lost"

        # 3) Target selection: prefer max_pain → gamma_flip → percent.
        target_ref, target_level_name = self._pick_target(close, max_pain, gamma_flip, wall_strike)

        # 4) Stop: close above wall * (1 + stop_pct).
        stop_ref = wall_strike * (1.0 + _STOP_PCT_ABOVE_WALL)

        # 5) Confidence: base * confluence * regime_fit, with VIX headwind.
        confidence = self.compute_confidence(ctx, bias="bearish")
        vix = (ctx.market.extra or {}).get("vix_level")
        if vix is not None and float(vix) > _VIX_HEADWIND:
            confidence = max(0.20, confidence - 0.10)

        # 6) Aligned signals (audit trail in Card.context).
        adv_aligned = [
            name
            for name in ("trap_detection", "gamma_vwap_confluence")
            if self._adv_aligned_bearish(ctx, name)
        ]
        basic_aligned = [
            name
            for name in self.confluence_signals_for
            if (snap := ctx.signal(name)) and snap.clamped_score < 0
        ]

        rationale = self._compose_rationale(
            close=close,
            call_wall=wall_strike,
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
                kind=stop_kind,
                level_name="call_wall_break",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "call_wall": wall_strike,
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
    # Trigger checks (also drives explain_miss for STAND_DOWN diagnostics)
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        """Return list of unmet trigger conditions; empty == all met."""
        missing: list[str] = []

        if ctx.net_gex is None or ctx.net_gex <= _NET_GEX_FLOOR:
            missing.append(
                f"net_gex {ctx.net_gex} <= floor {_NET_GEX_FLOOR:.0f} (need long-gamma backdrop)"
            )

        call_wall = ctx.level("call_wall")
        if call_wall is None:
            missing.append("call_wall level unavailable")
        elif ctx.close <= 0:
            missing.append("close price unavailable")
        else:
            distance_pct = abs(ctx.close - call_wall) / ctx.close
            if distance_pct > _WALL_PROXIMITY_PCT:
                missing.append(
                    f"price {distance_pct * 100:.2f}% from call_wall "
                    f"(needs <= {_WALL_PROXIMITY_PCT * 100:.2f}%)"
                )

        if ctx.et_time < time(10, 0):
            missing.append(f"too early ({ctx.et_time} ET); needs >= 10:00 ET")

        # At least one bearish flow signal required.
        tape = ctx.signal("tape_flow_bias")
        ofi = ctx.signal("order_flow_imbalance")
        flow_ok = (tape and tape.score <= _FLOW_BEAR_THRESHOLD) or (
            ofi and ofi.score <= _FLOW_BEAR_THRESHOLD
        )
        if not flow_ok:
            missing.append(
                f"no bearish flow signal: tape_flow_bias={tape.score if tape else None}, "
                f"order_flow_imbalance={ofi.score if ofi else None}"
            )

        # At least one corroborating advanced signal required.
        if not (
            self._adv_aligned_bearish(ctx, "trap_detection")
            or self._adv_aligned_bearish(ctx, "gamma_vwap_confluence")
        ):
            missing.append(
                "no corroborating advanced signal "
                "(trap_detection != 'bearish_fade' AND gamma_vwap_confluence != 'bearish_confluence')"
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
    def _adv_aligned_bearish(ctx: PlaybookContext, signal_name: str) -> bool:
        snap = ctx.signal(signal_name)
        if not snap:
            return False
        if signal_name == "trap_detection":
            return snap.signal == "bearish_fade"
        if signal_name == "gamma_vwap_confluence":
            return snap.signal == "bearish_confluence"
        return snap.clamped_score < 0

    @staticmethod
    def _pick_target(
        close: float,
        max_pain: Optional[float],
        gamma_flip: Optional[float],
        wall: float,
    ) -> tuple[Optional[float], Optional[str]]:
        if max_pain is not None and max_pain < close:
            return float(max_pain), "max_pain"
        if gamma_flip is not None and gamma_flip < close:
            return float(gamma_flip), "gamma_flip"
        return None, None

    @staticmethod
    def _zero_dte_expiry(ctx: PlaybookContext) -> str:
        # Use the date in ET so an after-hours UTC ts still maps to the
        # right session expiry.
        return ctx.et_date.isoformat()

    @staticmethod
    def _compose_rationale(
        close: float,
        call_wall: float,
        net_gex: float,
        adv_aligned: list[str],
        sigma: float,
        action: ActionEnum,
    ) -> str:
        parts = [
            f"Price ${close:.2f} pinned at call wall ${call_wall:.2f}",
            f"net GEX ${net_gex / 1e9:.1f}B (long-gamma backdrop)",
        ]
        if adv_aligned:
            parts.append("confirmed by " + " + ".join(adv_aligned))
        if sigma > 0:
            parts.append(f"30-min σ {sigma * 100:.2f}%")
        instrument_word = (
            "put debit" if action == ActionEnum.BUY_PUT_DEBIT else "call credit spread"
        )
        parts.append(f"→ {instrument_word} at the wall.")
        return ", ".join(parts[:-1]) + " " + parts[-1]


PATTERN: PatternBase = CallWallFadePattern()
