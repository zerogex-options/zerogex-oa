"""Pattern 2.2: ``overnight_trap_continuation`` — Trap Reversal Held Overnight.

Failed end-of-day breakouts that fire ``trap_detection`` in the last
hour often extend overnight as foreign markets and after-hours flow
continue to fade the original direction.  We hold a 1DTE OTM debit
into the next morning.

Per ``docs/playbook_catalog.md`` §7.2.2.
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

_START_HOUR = int(os.getenv("PLAYBOOK_OTC_START_HOUR_ET", "14"))
_START_MIN = int(os.getenv("PLAYBOOK_OTC_START_MIN_ET", "30"))
_FLIP_DISTANCE_MAX = float(os.getenv("PLAYBOOK_OTC_FLIP_DISTANCE_MAX", "0.0"))
_OTM_SIGMA_MULT = float(os.getenv("PLAYBOOK_OTC_OTM_SIGMA_MULT", "1.0"))
# Approximate daily sigma by scaling 1-min sigma by sqrt(390 trading min).
_DAILY_SIGMA_SCALAR = math.sqrt(390.0)
# Hold from late-day entry until next-day 11:00 ET ≈ 19h = 1140 min.
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_OTC_MAX_HOLD_MIN", "1140"))


TrapDirection = Literal["bullish", "bearish"]


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


class OvernightTrapContinuationPattern(PatternBase):
    id = "overnight_trap_continuation"
    name = "Trap Reversal Held Overnight"
    tier = "1DTE"
    direction = "context_dependent"
    valid_regimes = (
        "trend_expansion",
        "controlled_trend",
        "chop_range",
        "high_risk_reversal",
    )
    preferred_regime = "high_risk_reversal"
    pattern_base = 0.55

    confluence_signals_for = ("positioning_trap", "skew_delta")
    # 0dte_position_imbalance opposing the trap direction is handled
    # inline because it requires a directional comparison rather than a
    # straight sign-vs-bias product.
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        trap = ctx.signal("trap_detection")
        # _check_triggers guarantees trap is triggered with a fade signal.
        direction: TrapDirection = "bullish" if trap.signal == "bullish_fade" else "bearish"

        close = ctx.close
        sigma_1min = _realized_sigma_1min(ctx.market.recent_closes)
        sigma_daily_proxy = sigma_1min * _DAILY_SIGMA_SCALAR
        otm_offset = max(1.0, _OTM_SIGMA_MULT * sigma_daily_proxy * close)

        if direction == "bullish":
            action = ActionEnum.BUY_CALL_DEBIT
            strike = _round_to_strike(close + otm_offset, 1.0)
            right = "C"
        else:
            action = ActionEnum.BUY_PUT_DEBIT
            strike = _round_to_strike(close - otm_offset, 1.0)
            right = "P"

        expiry = self._one_dte_expiry(ctx)
        legs = [Leg(expiry=expiry, strike=strike, right=right, side="BUY", qty=1)]

        # Target: prior intraday range midpoint at next-day open.
        # Approximation: midpoint of recent_closes range.
        target_ref = self._intraday_midpoint(ctx)

        confidence = self.compute_confidence(ctx, bias=direction)
        # Penalize when 0dte_position_imbalance opposes the trap direction
        # (still active end-of-day flow fighting the reversal).
        odpi = ctx.signal("0dte_position_imbalance")
        if odpi and odpi.triggered and abs(odpi.score) >= 30:
            opposes = (direction == "bullish" and odpi.score < 0) or (
                direction == "bearish" and odpi.score > 0
            )
            if opposes:
                confidence = max(0.20, confidence - 0.10)

        rationale = (
            f"Late-day {trap.signal} trap (no wall migration) fading "
            f"the prior breakout; 1DTE OTM ${otm_offset:.2f} ({direction}) "
            "into next-day open, target prior range midpoint "
            f"${target_ref:.2f}."
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=direction,
            confidence=confidence,
            size_multiplier=0.5,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_market"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name="prior_range_midpoint",
            ),
            stop=Stop(
                ref_price=None,
                kind="signal_event",
                level_name="wall_migration_or_-60pct_premium",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "trap_signal": trap.signal,
                "direction": direction,
                "close": close,
                "otm_offset": round(otm_offset, 4),
                "strike": strike,
                "sigma_1min": round(sigma_1min, 6),
                "sigma_daily_proxy": round(sigma_daily_proxy, 6),
                "wall_migrated_up": trap.context_values.get("wall_migrated_up"),
                "wall_migrated_down": trap.context_values.get("wall_migrated_down"),
                "intraday_range_high": (
                    max(c for c in ctx.market.recent_closes if c and c > 0)
                    if ctx.market.recent_closes
                    else None
                ),
                "intraday_range_low": (
                    min(c for c in ctx.market.recent_closes if c and c > 0)
                    if ctx.market.recent_closes
                    else None
                ),
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        et = ctx.et_time
        if et < time(_START_HOUR, _START_MIN):
            missing.append(f"too early ({et} ET); needs >= {_START_HOUR:02d}:{_START_MIN:02d} ET")

        if ctx.close <= 0:
            missing.append("close price unavailable")

        trap = ctx.signal("trap_detection")
        if trap is None:
            missing.append("trap_detection signal unavailable")
        elif not trap.triggered:
            missing.append("trap_detection not triggered")
        elif trap.signal not in ("bullish_fade", "bearish_fade"):
            missing.append(f"trap_detection.signal {trap.signal!r} is not a fade signal")
        else:
            ctx_vals = trap.context_values or {}
            if ctx_vals.get("wall_migrated_up") or ctx_vals.get("wall_migrated_down"):
                missing.append(
                    "trap_detection: wall_migrated flag set "
                    "(setup invalidated; dealers repositioning with price)"
                )

        # gamma_anchor.flip_distance_subscore <= 0.0 — price not at flip,
        # which would otherwise create a competing momentum thesis.
        flip_subscore = self._flip_distance_subscore(ctx)
        if flip_subscore is None:
            missing.append("gamma_anchor.flip_distance_subscore unavailable")
        elif flip_subscore > _FLIP_DISTANCE_MAX:
            missing.append(
                f"flip_distance_subscore {flip_subscore:.2f} > {_FLIP_DISTANCE_MAX:.2f} "
                "(price too close to gamma flip; momentum could overrun the trap)"
            )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _flip_distance_subscore(ctx: PlaybookContext) -> Optional[float]:
        ga = ctx.msi_components.get("gamma_anchor") if ctx.msi_components else None
        if not isinstance(ga, dict):
            return None
        gactx = ga.get("context")
        if not isinstance(gactx, dict):
            return None
        val = gactx.get("flip_distance_subscore")
        if not isinstance(val, (int, float)):
            return None
        return float(val)

    @staticmethod
    def _intraday_midpoint(ctx: PlaybookContext) -> float:
        closes = [c for c in (ctx.market.recent_closes or []) if c and c > 0]
        if not closes:
            return ctx.close
        return (max(closes) + min(closes)) / 2.0

    @staticmethod
    def _one_dte_expiry(ctx: PlaybookContext) -> str:
        return (ctx.et_date + timedelta(days=1)).isoformat()


PATTERN: PatternBase = OvernightTrapContinuationPattern()
