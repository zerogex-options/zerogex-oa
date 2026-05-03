"""Pattern 3.5: ``gex_gradient_trend`` — Asymmetric Gamma Drift.

Asymmetric dealer gamma above vs below spot creates a multi-day drift
toward the lower-gamma direction (less hedging resistance there).

Per ``docs/playbook_catalog.md`` §7.3.5.

PR-11 simplification: spec calls for "1 confirming 4-hour bar in
direction".  Without intraday-bar resolution we approximate by checking
that the most recent close has moved in the gradient-favored direction
relative to the prior close.
"""

from __future__ import annotations

import math
import os
from datetime import time, timedelta
from typing import Literal, Optional

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

_GRADIENT_SCORE_MIN = float(os.getenv("PLAYBOOK_GGT_GRADIENT_SCORE_MIN", "40"))
_VOLREG_MIN = float(os.getenv("PLAYBOOK_GGT_VOLREG_MIN", "-0.5"))
_OTM_ATR_MULT = float(os.getenv("PLAYBOOK_GGT_OTM_ATR_MULT", "0.5"))
_TARGET_ATR_MULT = float(os.getenv("PLAYBOOK_GGT_TARGET_ATR_MULT", "1.5"))
_DTE_DAYS = int(os.getenv("PLAYBOOK_GGT_DTE_DAYS", "5"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_GGT_MAX_HOLD_MIN", str(3 * 24 * 60)))
_DAILY_SIGMA_SCALAR = math.sqrt(390.0)


DriftDirection = Literal["bullish", "bearish"]


def _realized_sigma_1min(closes: list[float]) -> float:
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


class GexGradientTrendPattern(PatternBase):
    id = "gex_gradient_trend"
    name = "Asymmetric Gamma Drift"
    tier = "swing"
    direction = "context_dependent"
    valid_regimes = ("controlled_trend", "chop_range")
    preferred_regime = "controlled_trend"
    pattern_base = 0.50

    confluence_signals_for = ("dealer_delta_pressure", "tape_flow_bias")
    # vol_expansion.triggered breaks the drift thesis — handled inline.
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        gex_grad = ctx.signal("gex_gradient")
        drift: DriftDirection = "bullish" if gex_grad.score > 0 else "bearish"

        close = ctx.close
        sigma_1min = _realized_sigma_1min(ctx.market.recent_closes)
        atr_daily = sigma_1min * _DAILY_SIGMA_SCALAR
        atr_dollars = atr_daily * close
        otm_offset = max(1.0, _OTM_ATR_MULT * atr_dollars)

        if drift == "bullish":
            action = ActionEnum.BUY_CALL_DEBIT
            strike = _round_to_strike(close + otm_offset, 1.0)
            right = "C"
            target_ref = close + _TARGET_ATR_MULT * atr_dollars
        else:
            action = ActionEnum.BUY_PUT_DEBIT
            strike = _round_to_strike(close - otm_offset, 1.0)
            right = "P"
            target_ref = close - _TARGET_ATR_MULT * atr_dollars

        expiry = self._dte_expiry(ctx, _DTE_DAYS)
        legs = [Leg(expiry=expiry, strike=strike, right=right, side="BUY", qty=1)]

        confidence = self.compute_confidence(ctx, bias=drift)
        # Inline against-check: vol_expansion triggered means a breakout is
        # underway, which would overrun a slow drift trade.
        vol_x = ctx.signal("vol_expansion")
        if vol_x and vol_x.triggered:
            confidence = max(0.20, confidence - 0.10)

        rationale = (
            f"gex_gradient {gex_grad.score:+.0f} ({drift}) + net_gex sign agrees "
            f"+ no breakout regime → {_DTE_DAYS}-DTE OTM {right} debit at "
            f"${strike:.0f}; target ${target_ref:.2f} "
            f"({_TARGET_ATR_MULT:.1f}× ATR_daily ≈ ${atr_dollars:.2f})."
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=drift,
            confidence=confidence,
            size_multiplier=0.5,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_close"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name="atr_drift_target",
            ),
            stop=Stop(
                ref_price=None,
                kind="signal_event",
                level_name="gradient_decay_below_20_or_-50pct_premium",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "drift_direction": drift,
                "gex_gradient_score": gex_grad.score,
                "net_gex": ctx.net_gex,
                "atr_daily": round(atr_daily, 6),
                "atr_dollars": round(atr_dollars, 4),
                "otm_offset": round(otm_offset, 4),
                "strike": strike,
                "rbi_label": (
                    s.context_values.get("label")
                    if (s := ctx.signal("range_break_imminence"))
                    else None
                ),
                "vol_expansion_triggered": (
                    s.triggered if (s := ctx.signal("vol_expansion")) else False
                ),
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []

        if ctx.close <= 0:
            missing.append("close price unavailable")

        gex_grad = ctx.signal("gex_gradient")
        if gex_grad is None:
            missing.append("gex_gradient signal unavailable")
        elif abs(gex_grad.score) < _GRADIENT_SCORE_MIN:
            missing.append(
                f"|gex_gradient score| {abs(gex_grad.score):.1f} < {_GRADIENT_SCORE_MIN:.0f}"
            )

        # net_gex sign must agree with gradient drift direction.
        if gex_grad is not None and abs(gex_grad.score) >= _GRADIENT_SCORE_MIN:
            if ctx.net_gex is None:
                missing.append("net_gex unavailable")
            elif (gex_grad.score * ctx.net_gex) < 0:
                missing.append(
                    f"net_gex {ctx.net_gex / 1e9:+.2f}B disagrees with "
                    f"gex_gradient {gex_grad.score:+.0f} (signs differ; not a drift setup)"
                )

        rbi = ctx.signal("range_break_imminence")
        if rbi and rbi.context_values.get("label") == "Breakout Mode":
            missing.append(
                "range_break_imminence in 'Breakout Mode' "
                "(this is drift, not break — wait for the regime to settle)"
            )

        vol_score = self._volatility_regime_score(ctx)
        if vol_score is None:
            missing.append("volatility_regime MSI score unavailable")
        elif vol_score < _VOLREG_MIN:
            missing.append(
                f"volatility_regime score {vol_score:.2f} < {_VOLREG_MIN:.2f} "
                "(no vol available to power the drift)"
            )

        # Confirming bar: most recent close moved in the gradient-favored direction.
        if gex_grad is not None and abs(gex_grad.score) >= _GRADIENT_SCORE_MIN:
            closes = [c for c in (ctx.market.recent_closes or []) if c and c > 0]
            if len(closes) >= 2:
                drift_sign = 1.0 if gex_grad.score > 0 else -1.0
                last_move = closes[-1] - closes[-2]
                if (last_move * drift_sign) <= 0:
                    missing.append(
                        f"no confirming bar in drift direction "
                        f"(last move {last_move:+.4f}, expected sign {int(drift_sign)})"
                    )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _volatility_regime_score(ctx: PlaybookContext) -> Optional[float]:
        comp = ctx.msi_components.get("volatility_regime") if ctx.msi_components else None
        if not isinstance(comp, dict):
            return None
        score = comp.get("score")
        if not isinstance(score, (int, float)):
            return None
        return float(score)

    @staticmethod
    def _dte_expiry(ctx: PlaybookContext, days: int) -> str:
        return (ctx.et_date + timedelta(days=days)).isoformat()


PATTERN: PatternBase = GexGradientTrendPattern()
