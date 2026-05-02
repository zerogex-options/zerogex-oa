"""Pattern 1.5: ``zero_dte_imbalance_drift`` — Smart-Money 0DTE Bias.

Midday window (11:00 → 14:30 ET) where smart-money 0DTE flow leads
price by ~30s on liquid names.  When ``0dte_position_imbalance`` reads
heavily one-sided AND the corroborating flow is real (not all-expiry
fallback) AND no trap is fighting the drift, lean into it with a
narrow 0DTE debit spread.

Per ``docs/playbook_catalog.md`` §7.1.5.
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

_ODPI_SCORE_MIN = float(os.getenv("PLAYBOOK_ZDID_SCORE_MIN", "30"))
_START_HOUR = int(os.getenv("PLAYBOOK_ZDID_START_HOUR_ET", "11"))
_START_MIN = int(os.getenv("PLAYBOOK_ZDID_START_MIN_ET", "0"))
_END_HOUR = int(os.getenv("PLAYBOOK_ZDID_END_HOUR_ET", "14"))
_END_MIN = int(os.getenv("PLAYBOOK_ZDID_END_MIN_ET", "30"))
_TARGET_SIGMA_MULT = float(os.getenv("PLAYBOOK_ZDID_TARGET_SIGMA_MULT", "2.0"))
_STOP_SIGMA_MULT = float(os.getenv("PLAYBOOK_ZDID_STOP_SIGMA_MULT", "0.5"))
_SPREAD_WIDTH_POINTS = float(os.getenv("PLAYBOOK_ZDID_SPREAD_WIDTH", "5.0"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_ZDID_MAX_HOLD_MIN", "90"))
_RANGE_FADE_PENALTY = float(os.getenv("PLAYBOOK_ZDID_RANGE_FADE_PENALTY", "0.10"))


DriftDirection = Literal["bullish", "bearish"]


def _realized_sigma(closes: list[float]) -> float:
    """Per-bar standard deviation of returns over the last 30 1-min closes."""
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


class ZeroDteImbalanceDriftPattern(PatternBase):
    id = "zero_dte_imbalance_drift"
    name = "Smart-Money 0DTE Bias"
    tier = "0DTE"
    direction = "context_dependent"
    # Excluded from high_risk_reversal: in HRR the wall patterns own the
    # contrarian fade — drifting with flow there over-fights the regime.
    valid_regimes = ("trend_expansion", "controlled_trend", "chop_range")
    preferred_regime = "controlled_trend"
    pattern_base = 0.50

    confluence_signals_for = ("tape_flow_bias", "vanna_charm_flow")
    # range_break_imminence's "Range Fade" label opposes — handled inline
    # because the check is on the label, not the signal sign.
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        odpi = ctx.signal("0dte_position_imbalance")
        # _check_triggers guarantees odpi is triggered with sufficient score
        # and a real zero_dte flow source.
        drift: DriftDirection = "bullish" if odpi.score > 0 else "bearish"

        close = ctx.close
        sigma = _realized_sigma(ctx.market.recent_closes)
        # ATR-proxy in price units: σ_1min × close.  Spec calls for ATR(5min);
        # we approximate with 1-min σ scaled by the configured multiplier.
        sigma_dollars = sigma * close
        target_distance = _TARGET_SIGMA_MULT * sigma_dollars
        stop_distance = _STOP_SIGMA_MULT * sigma_dollars

        # Instrument: 0DTE +5 strike-width debit spread.
        atm_strike = _round_to_strike(close, 1.0)
        expiry = self._zero_dte_expiry(ctx)
        if drift == "bullish":
            action = ActionEnum.BUY_CALL_SPREAD
            short_strike = atm_strike + _SPREAD_WIDTH_POINTS
            legs = [
                Leg(expiry=expiry, strike=atm_strike, right="C", side="BUY", qty=1),
                Leg(expiry=expiry, strike=short_strike, right="C", side="SELL", qty=1),
            ]
        else:
            action = ActionEnum.BUY_PUT_SPREAD
            short_strike = atm_strike - _SPREAD_WIDTH_POINTS
            legs = [
                Leg(expiry=expiry, strike=atm_strike, right="P", side="BUY", qty=1),
                Leg(expiry=expiry, strike=short_strike, right="P", side="SELL", qty=1),
            ]

        # Target: σ-based extension OR first opposing wall, whichever closer.
        if drift == "bullish":
            sigma_target = close + target_distance
            wall = ctx.level("call_wall")
            target_ref, target_level_name = self._closer_target(
                cross="bullish",
                entry=close,
                sigma_target=sigma_target,
                wall=wall,
                wall_name="call_wall",
            )
            stop_ref = close - stop_distance
        else:
            sigma_target = close - target_distance
            wall = ctx.level("put_wall")
            target_ref, target_level_name = self._closer_target(
                cross="bearish",
                entry=close,
                sigma_target=sigma_target,
                wall=wall,
                wall_name="put_wall",
            )
            stop_ref = close + stop_distance

        confidence = self.compute_confidence(ctx, bias=drift)
        # Inline against-check: range_break_imminence "Range Fade" label.
        rbi = ctx.signal("range_break_imminence")
        if rbi and rbi.context_values.get("label") == "Range Fade":
            confidence = max(0.20, confidence - _RANGE_FADE_PENALTY)

        rationale = self._compose_rationale(
            drift=drift,
            close=close,
            sigma_pct=sigma * 100.0,
            odpi_score=odpi.score,
            target_name=target_level_name,
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
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_market"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name=target_level_name,
            ),
            stop=Stop(
                ref_price=round(stop_ref, 4),
                kind="level",
                level_name="atr_stop",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "drift_direction": drift,
                "odpi_score": odpi.score,
                "flow_source": odpi.context_values.get("flow_source"),
                "realized_sigma_30min": round(sigma, 6),
                "atr_proxy_dollars": round(sigma_dollars, 4),
                "call_wall": ctx.level("call_wall"),
                "put_wall": ctx.level("put_wall"),
                "rbi_label": (rbi.context_values.get("label") if rbi else None),
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        et = ctx.et_time
        start = time(_START_HOUR, _START_MIN)
        end = time(_END_HOUR, _END_MIN)
        if et < start:
            missing.append(f"too early ({et} ET); needs >= {start} (avoid open noise)")
        if et > end:
            missing.append(
                f"too late ({et} ET); needs <= {end} (EOD overlap with eod_pressure_drift)"
            )

        if ctx.close <= 0:
            missing.append("close price unavailable")

        odpi = ctx.signal("0dte_position_imbalance")
        if odpi is None:
            missing.append("0dte_position_imbalance signal unavailable")
        elif not odpi.triggered:
            missing.append(f"0dte_position_imbalance not triggered (score={odpi.score})")
        elif abs(odpi.score) < _ODPI_SCORE_MIN:
            missing.append(
                f"0dte_position_imbalance |score| {abs(odpi.score):.1f} < {_ODPI_SCORE_MIN:.0f}"
            )
        else:
            flow_source = odpi.context_values.get("flow_source")
            if flow_source != "zero_dte":
                missing.append(
                    f"flow_source is {flow_source!r}; needs 'zero_dte' "
                    "(all_expiry_fallback is inferred, not measured)"
                )

        # Trap-conflict gate: if trap_detection is active, its signal must
        # agree with the drift direction or we're fading our own bias.
        if odpi is not None and odpi.triggered and abs(odpi.score) >= _ODPI_SCORE_MIN:
            drift = "bullish" if odpi.score > 0 else "bearish"
            trap = ctx.signal("trap_detection")
            if trap and trap.triggered:
                expected = "bullish_fade" if drift == "bullish" else "bearish_fade"
                if trap.signal != expected:
                    missing.append(
                        f"trap_detection.signal {trap.signal!r} fights drift "
                        f"{drift!r} (expected {expected!r} or untriggered)"
                    )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _closer_target(
        *,
        cross: DriftDirection,
        entry: float,
        sigma_target: float,
        wall: Optional[float],
        wall_name: str,
    ) -> tuple[float, str]:
        """Return whichever of (sigma_target, wall) is closer to entry."""
        if wall is None:
            return sigma_target, "atr_2x"
        if cross == "bullish":
            if wall <= entry:
                return sigma_target, "atr_2x"
            return (
                (wall, wall_name)
                if (wall - entry) <= (sigma_target - entry)
                else (
                    sigma_target,
                    "atr_2x",
                )
            )
        # bearish
        if wall >= entry:
            return sigma_target, "atr_2x"
        return (
            (wall, wall_name)
            if (entry - wall) <= (entry - sigma_target)
            else (
                sigma_target,
                "atr_2x",
            )
        )

    @staticmethod
    def _zero_dte_expiry(ctx: PlaybookContext) -> str:
        return ctx.et_date.isoformat()

    @staticmethod
    def _compose_rationale(
        drift: DriftDirection,
        close: float,
        sigma_pct: float,
        odpi_score: float,
        target_name: str,
    ) -> str:
        instrument_word = "call debit spread" if drift == "bullish" else "put debit spread"
        return (
            f"0DTE flow imbalance {odpi_score:+.0f} drives price ${close:.2f} "
            f"{drift}; 1-min σ {sigma_pct:.2f}%; target {target_name} "
            f"→ {instrument_word} ATM 0DTE."
        )


PATTERN: PatternBase = ZeroDteImbalanceDriftPattern()
