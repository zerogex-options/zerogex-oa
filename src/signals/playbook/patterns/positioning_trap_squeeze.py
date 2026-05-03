"""Pattern 3.4: ``positioning_trap_squeeze`` — One-Way Crowding Squeeze.

When ``positioning_trap`` flags one-sided crowd positioning AND the
tape has started turning *against* the crowd, the squeeze produces a
multi-day move opposite the crowd's bias.

Per ``docs/playbook_catalog.md`` §7.3.4.
"""

from __future__ import annotations

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

_PTRAP_SCORE_MIN = float(os.getenv("PLAYBOOK_PTS_PTRAP_SCORE_MIN", "50"))
_VOLREG_MIN = float(os.getenv("PLAYBOOK_PTS_VOLREG_MIN", "-0.2"))
_TAPE_MAGNITUDE_MIN = float(os.getenv("PLAYBOOK_PTS_TAPE_MAG_MIN", "10"))
_SPREAD_WIDTH_POINTS = float(os.getenv("PLAYBOOK_PTS_SPREAD_WIDTH", "10.0"))
_DTE_DAYS = int(os.getenv("PLAYBOOK_PTS_DTE_DAYS", "5"))
_TARGET_RANGE_MULT = float(os.getenv("PLAYBOOK_PTS_TARGET_RANGE_MULT", "2.0"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_PTS_MAX_HOLD_MIN", str(3 * 24 * 60)))
_ENVELOPE_LOOKBACK_BARS = int(os.getenv("PLAYBOOK_PTS_ENVELOPE_BARS", "30"))


SqueezeDirection = Literal["bullish", "bearish"]


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


def _envelope(closes: list[float]) -> Optional[tuple[float, float]]:
    usable = [c for c in (closes or []) if c and c > 0]
    if len(usable) < 5:
        return None
    window = usable[-_ENVELOPE_LOOKBACK_BARS:]
    return (min(window), max(window))


class PositioningTrapSqueezePattern(PatternBase):
    id = "positioning_trap_squeeze"
    name = "One-Way Crowding Squeeze"
    tier = "swing"
    direction = "context_dependent"
    valid_regimes = ("chop_range", "controlled_trend", "high_risk_reversal")
    preferred_regime = "high_risk_reversal"
    pattern_base = 0.55

    confluence_signals_for = ("skew_delta", "dealer_delta_pressure")
    # vanna_charm_flow opposing the squeeze direction is handled inline
    # (the base helper's sign-vs-bias product would treat *any* opposing
    # vanna as against, but spec wants only meaningful opposition).
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        ptrap = ctx.signal("positioning_trap")
        # Squeeze direction is OPPOSITE the crowd: long crowd → bearish squeeze.
        squeeze: SqueezeDirection = "bearish" if ptrap.score > 0 else "bullish"

        close = ctx.close
        envelope = _envelope(ctx.market.recent_closes) or (close, close)
        envelope_low, envelope_high = envelope
        prior_range = envelope_high - envelope_low

        atm_strike = _round_to_strike(close, 1.0)
        expiry = self._dte_expiry(ctx, _DTE_DAYS)
        if squeeze == "bullish":
            action = ActionEnum.BUY_CALL_SPREAD
            short_strike = atm_strike + _SPREAD_WIDTH_POINTS
            legs = [
                Leg(expiry=expiry, strike=atm_strike, right="C", side="BUY", qty=1),
                Leg(expiry=expiry, strike=short_strike, right="C", side="SELL", qty=1),
            ]
            target_ref = close + _TARGET_RANGE_MULT * prior_range
        else:
            action = ActionEnum.BUY_PUT_SPREAD
            short_strike = atm_strike - _SPREAD_WIDTH_POINTS
            legs = [
                Leg(expiry=expiry, strike=atm_strike, right="P", side="BUY", qty=1),
                Leg(expiry=expiry, strike=short_strike, right="P", side="SELL", qty=1),
            ]
            target_ref = close - _TARGET_RANGE_MULT * prior_range

        confidence = self.compute_confidence(ctx, bias=squeeze)
        # Inline against-check: vanna_charm_flow opposing the squeeze with
        # meaningful magnitude lowers confidence.
        vcf = ctx.signal("vanna_charm_flow")
        if vcf is not None:
            sign = 1.0 if squeeze == "bullish" else -1.0
            if vcf.score * sign < 0 and abs(vcf.score) >= 30:
                confidence = max(0.20, confidence - 0.10)

        rationale = (
            f"positioning_trap {ptrap.score:+.0f} crowded against squeeze; "
            f"tape turning {squeeze} → {_DTE_DAYS}-DTE +${_SPREAD_WIDTH_POINTS:.0f} debit "
            f"spread against the crowd; target ${target_ref:.2f} "
            f"({_TARGET_RANGE_MULT:.1f}× prior range)."
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=squeeze,
            confidence=confidence,
            size_multiplier=0.5,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_close"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name="range_2x_against_crowd",
            ),
            stop=Stop(
                ref_price=None,
                kind="signal_event",
                level_name="ptrap_unwinds_30pct_or_-50pct_premium",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "squeeze_direction": squeeze,
                "positioning_trap_score": ptrap.score,
                "tape_flow_bias_score": (s.score if (s := ctx.signal("tape_flow_bias")) else None),
                "envelope_low": envelope_low,
                "envelope_high": envelope_high,
                "prior_range": round(prior_range, 4),
                "volatility_regime_score": self._volatility_regime_score(ctx),
                "vanna_charm_flow_score": (
                    s.score if (s := ctx.signal("vanna_charm_flow")) else None
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

        ptrap = ctx.signal("positioning_trap")
        if ptrap is None:
            missing.append("positioning_trap signal unavailable")
        elif abs(ptrap.score) < _PTRAP_SCORE_MIN:
            missing.append(
                f"positioning_trap |score| {abs(ptrap.score):.1f} < {_PTRAP_SCORE_MIN:.0f} "
                "(crowd not heavily positioned)"
            )

        if ptrap is not None and abs(ptrap.score) >= _PTRAP_SCORE_MIN:
            tape = ctx.signal("tape_flow_bias")
            if tape is None:
                missing.append("tape_flow_bias signal unavailable")
            else:
                # Tape must be turning AGAINST the crowd (opposite sign).
                if (tape.score * ptrap.score) >= 0:
                    missing.append(
                        f"tape_flow_bias {tape.score:+.0f} not turning against crowd "
                        f"positioning {ptrap.score:+.0f} (need opposite sign)"
                    )
                elif abs(tape.score) < _TAPE_MAGNITUDE_MIN:
                    missing.append(
                        f"tape_flow_bias |score| {abs(tape.score):.1f} < "
                        f"{_TAPE_MAGNITUDE_MIN:.0f} (tape barely turning)"
                    )

        vol_score = self._volatility_regime_score(ctx)
        if vol_score is None:
            missing.append("volatility_regime MSI score unavailable")
        elif vol_score < _VOLREG_MIN:
            missing.append(
                f"volatility_regime score {vol_score:.2f} < {_VOLREG_MIN:.2f} "
                "(no vol to fuel the squeeze)"
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


PATTERN: PatternBase = PositioningTrapSqueezePattern()
