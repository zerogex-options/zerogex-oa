"""Pattern 1.4: ``eod_pressure_drift`` — Last-Hour Hedging Drift.

In the last hour of regular trading, dealer 0DTE hedging dominates the
tape; the ``eod_pressure`` advanced signal aggregates that directional
push.  We lean into it with an ATM 0DTE debit, anchored to VWAP for
target and invalidation.

Per ``docs/playbook_catalog.md`` §7.1.4.
"""

from __future__ import annotations

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

# Tunable thresholds — env-overridable.
_EOD_SCORE_MIN = float(os.getenv("PLAYBOOK_EOD_SCORE_MIN", "30"))
_WALL_BLOCKER_PCT = float(os.getenv("PLAYBOOK_EOD_WALL_BLOCKER_PCT", "0.0030"))
_VWAP_TARGET_MULT = float(os.getenv("PLAYBOOK_EOD_VWAP_TARGET_MULT", "1.5"))
_CLOSE_BUFFER_MIN = int(os.getenv("PLAYBOOK_EOD_CLOSE_BUFFER_MIN", "5"))  # exit by 15:55 ET
_START_HOUR = int(os.getenv("PLAYBOOK_EOD_START_HOUR_ET", "15"))
_START_MIN = int(os.getenv("PLAYBOOK_EOD_START_MIN_ET", "0"))


DriftDirection = Literal["bullish", "bearish"]


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


class EodPressureDriftPattern(PatternBase):
    id = "eod_pressure_drift"
    name = "Last-Hour Hedging Drift"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = (
        "trend_expansion",
        "controlled_trend",
        "chop_range",
        "high_risk_reversal",
    )
    preferred_regime = "controlled_trend"
    pattern_base = 0.55

    confluence_signals_for = (
        "0dte_position_imbalance",
        "dealer_delta_pressure",
    )
    # tape_flow_bias only opposes when the score sign is *opposite* the drift
    # bias — base-class _confluence_multiplier already encodes that semantics.
    confluence_signals_against = ("tape_flow_bias",)

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        eod = ctx.signal("eod_pressure")
        # _check_triggers guarantees eod is non-None, triggered, and
        # |score| >= _EOD_SCORE_MIN.
        drift: DriftDirection = "bullish" if eod.score > 0 else "bearish"

        close = ctx.close
        vwap = ctx.market.vwap
        # Spec target: VWAP + 1.5 × distance-from-VWAP-at-entry.  Algebra
        # collapses to the same formula for both directions.
        target_ref = _VWAP_TARGET_MULT * close - (_VWAP_TARGET_MULT - 1.0) * vwap
        stop_ref = vwap

        atm_strike = _round_to_strike(close, 1.0)
        expiry = self._zero_dte_expiry(ctx)
        if drift == "bullish":
            action = ActionEnum.BUY_CALL_DEBIT
            right = "C"
        else:
            action = ActionEnum.BUY_PUT_DEBIT
            right = "P"
        legs = [Leg(expiry=expiry, strike=atm_strike, right=right, side="BUY", qty=1)]

        # Max hold: shrinks as we approach 15:55 ET.  Always positive at
        # this point because _check_triggers gates on minutes_to_close.
        max_hold_min = max(1, ctx.minutes_to_close - _CLOSE_BUFFER_MIN)

        confidence = self.compute_confidence(ctx, bias=drift)

        rationale = self._compose_rationale(
            drift=drift,
            close=close,
            vwap=vwap,
            eod_score=eod.score,
            target=target_ref,
            stop=stop_ref,
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=drift,
            confidence=confidence,
            size_multiplier=0.6,
            max_hold_minutes=max_hold_min,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_market"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name="vwap_extension",
            ),
            stop=Stop(
                ref_price=round(stop_ref, 4),
                kind="level",
                level_name="vwap_cross",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "drift_direction": drift,
                "eod_pressure_score": eod.score,
                "vwap": vwap,
                "close": close,
                "call_wall": ctx.level("call_wall"),
                "put_wall": ctx.level("put_wall"),
                "advanced_signals_aligned": [
                    name
                    for name in ("0dte_position_imbalance",)
                    if (snap := ctx.signal(name)) and snap.triggered
                ],
                "minutes_to_close": ctx.minutes_to_close,
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        close = ctx.close
        vwap = ctx.market.vwap
        et = ctx.et_time

        if et < time(_START_HOUR, _START_MIN):
            missing.append(f"too early ({et} ET); needs >= {_START_HOUR:02d}:{_START_MIN:02d} ET")
        # Need at least the close-buffer of session left to enter.
        if ctx.minutes_to_close <= _CLOSE_BUFFER_MIN:
            missing.append(
                f"too close to bell: {ctx.minutes_to_close}m to close "
                f"(need > {_CLOSE_BUFFER_MIN}m)"
            )

        if close <= 0:
            missing.append("close price unavailable")
        if vwap is None or vwap <= 0:
            missing.append("vwap unavailable (required for target/stop)")

        eod = ctx.signal("eod_pressure")
        if eod is None:
            missing.append("eod_pressure signal unavailable")
        elif not eod.triggered:
            missing.append(f"eod_pressure not triggered (score={eod.score})")
        elif abs(eod.score) < _EOD_SCORE_MIN:
            missing.append(f"eod_pressure |score| {abs(eod.score):.1f} < {_EOD_SCORE_MIN:.0f}")

        # Confirming bar: most recent 1-min close moved in the drift direction.
        # Skip when eod is missing — already failed above.
        if eod is not None and eod.triggered and abs(eod.score) >= _EOD_SCORE_MIN:
            drift_sign = 1.0 if eod.score > 0 else -1.0
            closes = [c for c in (ctx.market.recent_closes or []) if c and c > 0]
            if len(closes) >= 2:
                last_move = closes[-1] - closes[-2]
                if (last_move * drift_sign) <= 0:
                    missing.append(
                        f"no confirming 1-min bar in drift direction "
                        f"(last move {last_move:+.4f}, expected sign {int(drift_sign)})"
                    )

        # Opposing wall blocker: a wall too close in the drift direction
        # would absorb the move before VWAP-extension can hit.  Only
        # check when eod is valid (drift is defined).
        if eod is not None and eod.triggered and abs(eod.score) >= _EOD_SCORE_MIN and close > 0:
            drift_bullish = eod.score > 0
            if drift_bullish:
                wall = ctx.level("call_wall")
                if wall is not None and wall > close:
                    distance_pct = (wall - close) / close
                    if distance_pct <= _WALL_BLOCKER_PCT:
                        missing.append(
                            f"call_wall blocker: {distance_pct * 100:.2f}% above close "
                            f"(needs > {_WALL_BLOCKER_PCT * 100:.2f}%)"
                        )
            else:
                wall = ctx.level("put_wall")
                if wall is not None and wall < close:
                    distance_pct = (close - wall) / close
                    if distance_pct <= _WALL_BLOCKER_PCT:
                        missing.append(
                            f"put_wall blocker: {distance_pct * 100:.2f}% below close "
                            f"(needs > {_WALL_BLOCKER_PCT * 100:.2f}%)"
                        )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _zero_dte_expiry(ctx: PlaybookContext) -> str:
        return ctx.et_date.isoformat()

    @staticmethod
    def _compose_rationale(
        drift: DriftDirection,
        close: float,
        vwap: float,
        eod_score: float,
        target: float,
        stop: float,
    ) -> str:
        direction_word = "above" if drift == "bullish" else "below"
        instrument_word = "call debit" if drift == "bullish" else "put debit"
        return (
            f"EOD pressure {eod_score:+.0f} pushing price ${close:.2f} "
            f"{direction_word} VWAP ${vwap:.2f}; "
            f"target ${target:.2f}, stop on VWAP cross at ${stop:.2f} "
            f"→ {instrument_word} ATM 0DTE."
        )


PATTERN: PatternBase = EodPressureDriftPattern()
