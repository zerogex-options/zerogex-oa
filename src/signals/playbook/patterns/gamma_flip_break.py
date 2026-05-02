"""Pattern 1.3: ``gamma_flip_break`` — Trade Through the Gamma Flip.

Crossing the gamma flip transitions dealers from suppressing moves
(above flip / long-gamma) to amplifying them (below flip / short-gamma).
The cross direction *is* the trade direction — buy calls on a
below-to-above cross, buy puts on an above-to-below cross.

Per ``docs/playbook_catalog.md`` §7.1.3.
"""

from __future__ import annotations

import math
import os
from datetime import time
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

# Tunable thresholds — env-overridable so PR-3+ backtests can adjust.
_FLIP_DISTANCE_MIN = float(os.getenv("PLAYBOOK_GFB_FLIP_DISTANCE_MIN", "0.6"))
_BREAK_BUFFER_PCT = float(os.getenv("PLAYBOOK_GFB_BREAK_BUFFER_PCT", "0.0005"))  # 0.05%
_PRIOR_MODE_BARS = int(os.getenv("PLAYBOOK_GFB_PRIOR_MODE_BARS", "30"))
_PRIOR_MODE_THRESHOLD = float(os.getenv("PLAYBOOK_GFB_PRIOR_MODE_THRESHOLD", "0.65"))
_RECENT_BARS = int(os.getenv("PLAYBOOK_GFB_RECENT_BARS", "5"))  # most recent bars to test for cross
_VOL_SAT_TARGET = float(os.getenv("PLAYBOOK_GFB_VOL_SAT_TARGET", "2.0"))  # 2σ multiplier for target
_LOW_SIGMA_PENALTY_FLOOR = float(os.getenv("PLAYBOOK_GFB_LOW_SIGMA_FLOOR", "0.0010"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_GFB_MAX_HOLD_MIN", "60"))


CrossDirection = Literal["bullish", "bearish"]


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


def _detect_cross(
    closes: list[float],
    flip: float,
    *,
    prior_bars: int = _PRIOR_MODE_BARS,
    recent_bars: int = _RECENT_BARS,
    threshold: float = _PRIOR_MODE_THRESHOLD,
) -> Optional[CrossDirection]:
    """Detect a flip-cross from the trailing closes.

    Strategy: look at the prior ``prior_bars`` 1-min closes (excluding the
    most recent ``recent_bars``).  If at least ``threshold`` of those sit
    on one side of the flip, that side is the prior mode.  If the most
    recent close sits on the *opposite* side, return the cross direction.
    Otherwise return None.
    """
    usable = [c for c in (closes or []) if c and c > 0]
    if len(usable) < (prior_bars // 2) + 1 or flip is None or flip <= 0:
        return None

    # Take the prior window (excluding the most recent bars).
    if len(usable) > recent_bars:
        prior_window = usable[-(prior_bars + recent_bars) : -recent_bars]
        recent_window = usable[-recent_bars:]
    else:
        return None
    if len(prior_window) < 5:
        return None

    above_prior = sum(1 for c in prior_window if c > flip)
    below_prior = len(prior_window) - above_prior
    above_share = above_prior / len(prior_window)
    below_share = below_prior / len(prior_window)

    last_close = recent_window[-1]
    if above_share >= threshold and last_close < flip:
        return "bearish"  # was above, broke below
    if below_share >= threshold and last_close > flip:
        return "bullish"  # was below, broke above
    return None


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


class GammaFlipBreakPattern(PatternBase):
    id = "gamma_flip_break"
    name = "Trade Through the Gamma Flip"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = ("trend_expansion", "controlled_trend", "chop_range", "high_risk_reversal")
    preferred_regime = "controlled_trend"
    pattern_base = 0.50

    confluence_signals_for = (
        "gex_gradient",
        "tape_flow_bias",
        "order_flow_imbalance",
    )
    # gamma_vwap_confluence's mean_reversion regime opposes the trade —
    # handled inline because it requires a non-sign-based check.
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        flip = ctx.market.gamma_flip
        close = ctx.close
        cross = _detect_cross(ctx.market.recent_closes, flip)
        if cross is None:
            # Should be impossible after _check_triggers, but defensive.
            return None

        sigma = _realized_sigma_30min(ctx.market.recent_closes)

        # Instrument: single-leg ATM 0DTE, debit, in the cross direction.
        atm_strike = _round_to_strike(close, 1.0)
        expiry = self._zero_dte_expiry(ctx)
        if cross == "bullish":
            action = ActionEnum.BUY_CALL_DEBIT
            right = "C"
        else:
            action = ActionEnum.BUY_PUT_DEBIT
            right = "P"
        legs = [Leg(expiry=expiry, strike=atm_strike, right=right, side="BUY", qty=1)]

        # Entry: break + buffer past the flip in the cross direction.
        if cross == "bullish":
            entry_ref = flip * (1.0 + _BREAK_BUFFER_PCT)
            stop_ref = flip * (1.0 - _BREAK_BUFFER_PCT)
        else:
            entry_ref = flip * (1.0 - _BREAK_BUFFER_PCT)
            stop_ref = flip * (1.0 + _BREAK_BUFFER_PCT)

        # Target: nearest of (next wall in cross direction) or (entry ± 2σ),
        # whichever is closer — limits over-extension.
        target_ref, target_level_name = self._pick_target(
            cross=cross,
            entry=entry_ref,
            sigma=sigma,
            call_wall=ctx.level("call_wall"),
            put_wall=ctx.level("put_wall"),
            max_gamma=ctx.level("max_gamma_strike"),
        )

        # Confidence: pattern_base * confluence(bias=cross) * regime_fit.
        confidence = self.compute_confidence(ctx, bias=cross)

        # Inline against-check: gvc.regime_direction == "mean_reversion"
        # opposes a momentum trade.
        gvc = ctx.signal("gamma_vwap_confluence")
        if gvc and gvc.context_values.get("regime_direction") == "mean_reversion":
            confidence = max(0.20, confidence - 0.10)

        # Low-σ penalty: no fuel for the break.
        if sigma < _LOW_SIGMA_PENALTY_FLOOR:
            confidence = max(0.20, confidence - 0.15)

        rationale = self._compose_rationale(
            cross=cross,
            flip=flip,
            close=close,
            sigma=sigma,
            target_name=target_level_name,
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=cross,  # Concrete direction at emit time, not "context_dependent".
            confidence=confidence,
            size_multiplier=0.6,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=entry_ref, trigger="on_break"),
            target=Target(
                ref_price=target_ref,
                kind="level" if target_level_name else "premium_pct",
                level_name=target_level_name,
            ),
            stop=Stop(
                ref_price=stop_ref,
                kind="level",
                level_name="gamma_flip_reversal",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "gamma_flip": flip,
                "cross_direction": cross,
                "call_wall": ctx.level("call_wall"),
                "put_wall": ctx.level("put_wall"),
                "max_gamma_strike": ctx.level("max_gamma_strike"),
                "realized_sigma_30min": round(sigma, 6),
                "advanced_signals_aligned": [
                    name
                    for name in ("vol_expansion", "range_break_imminence")
                    if (snap := ctx.signal(name)) and snap.triggered
                ],
                "gvc_regime_direction": (
                    gvc.context_values.get("regime_direction") if gvc else None
                ),
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        flip = ctx.market.gamma_flip
        close = ctx.close

        if flip is None:
            missing.append("gamma_flip unavailable")
        if close <= 0:
            missing.append("close price unavailable")

        # MSI gamma_anchor.flip_distance_subscore must indicate price near flip.
        flip_subscore = self._flip_distance_subscore(ctx)
        if flip_subscore is None:
            missing.append("gamma_anchor.flip_distance_subscore unavailable")
        elif flip_subscore < _FLIP_DISTANCE_MIN:
            missing.append(
                f"flip_distance_subscore {flip_subscore:.2f} < {_FLIP_DISTANCE_MIN:.2f} "
                "(price not near gamma_flip)"
            )

        # Cross detection on the trailing closes.
        if flip is not None:
            cross = _detect_cross(ctx.market.recent_closes, flip)
            if cross is None:
                missing.append(
                    "no flip-cross detected in last "
                    f"{_RECENT_BARS} bars vs prior {_PRIOR_MODE_BARS}-bar mode"
                )

        # range_break_imminence label gate.
        rbi = ctx.signal("range_break_imminence")
        rbi_label = rbi.context_values.get("label") if rbi else None
        if rbi_label not in ("Break Watch", "Breakout Mode"):
            missing.append(
                f"range_break_imminence.label is {rbi_label!r} "
                "(needs 'Break Watch' or 'Breakout Mode')"
            )

        # vol_expansion.triggered required (confirms momentum to power the move).
        vol_x = ctx.signal("vol_expansion")
        if not vol_x or not vol_x.triggered:
            missing.append("vol_expansion not triggered")

        if ctx.et_time < time(10, 0):
            missing.append(f"too early ({ctx.et_time} ET); needs >= 10:00 ET")

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
    def _pick_target(
        *,
        cross: CrossDirection,
        entry: float,
        sigma: float,
        call_wall: Optional[float],
        put_wall: Optional[float],
        max_gamma: Optional[float],
    ) -> tuple[Optional[float], Optional[str]]:
        """Return (target_price, level_name) — nearest of wall or 2σ move."""
        sigma_target = (
            entry * (1.0 + _VOL_SAT_TARGET * sigma)
            if cross == "bullish"
            else entry * (1.0 - _VOL_SAT_TARGET * sigma)
        )

        if cross == "bullish":
            wall_candidates: list[tuple[float, str]] = []
            if call_wall is not None and call_wall > entry:
                wall_candidates.append((float(call_wall), "call_wall"))
            if max_gamma is not None and max_gamma > entry:
                wall_candidates.append((float(max_gamma), "max_gamma_strike"))
            if not wall_candidates:
                if sigma > 0:
                    return sigma_target, "sigma_2x"
                return None, None
            # Pick the closer of the two: smallest distance from entry.
            wall_target, wall_name = min(wall_candidates, key=lambda w: w[0] - entry)
            if sigma <= 0:
                return wall_target, wall_name
            # Compare wall vs sigma; pick closer.
            if wall_target - entry <= sigma_target - entry:
                return wall_target, wall_name
            return sigma_target, "sigma_2x"
        else:  # bearish
            wall_candidates = []
            if put_wall is not None and put_wall < entry:
                wall_candidates.append((float(put_wall), "put_wall"))
            if max_gamma is not None and max_gamma < entry:
                wall_candidates.append((float(max_gamma), "max_gamma_strike"))
            if not wall_candidates:
                if sigma > 0:
                    return sigma_target, "sigma_2x"
                return None, None
            wall_target, wall_name = max(wall_candidates, key=lambda w: w[0] - entry)
            if sigma <= 0:
                return wall_target, wall_name
            if entry - wall_target <= entry - sigma_target:
                return wall_target, wall_name
            return sigma_target, "sigma_2x"

    @staticmethod
    def _zero_dte_expiry(ctx: PlaybookContext) -> str:
        return ctx.et_date.isoformat()

    @staticmethod
    def _compose_rationale(
        cross: CrossDirection,
        flip: float,
        close: float,
        sigma: float,
        target_name: Optional[str],
    ) -> str:
        direction_word = "above" if cross == "bullish" else "below"
        instrument_word = "call debit" if cross == "bullish" else "put debit"
        target_phrase = f"target {target_name}" if target_name else "target sigma-scaled"
        return (
            f"Price ${close:.2f} broke {direction_word} gamma_flip ${flip:.2f}; "
            f"30-min σ {sigma * 100:.2f}%; {target_phrase} → {instrument_word} ATM 0DTE."
        )


PATTERN: PatternBase = GammaFlipBreakPattern()
