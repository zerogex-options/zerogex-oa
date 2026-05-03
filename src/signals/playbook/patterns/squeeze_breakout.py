"""Pattern 3.1: ``squeeze_breakout`` — Vol-Compression Resolves.

Multi-day low realized vol + dense gamma + asymmetric ``gex_gradient``
charges potential energy for a swing move.  Direction is pre-revealed
by the gradient asymmetry; ``net_gex`` near the flip indicates the
dealer regime isn't entrenched, so the move can run.

Per ``docs/playbook_catalog.md`` §7.3.1.

The 2-day-sustained ``squeeze_setup.triggered`` requirement is enforced
via ``SignalSnapshot.daily_max_abs`` (PR-12 history loader): we count
the number of distinct ET trading days within the configured window in
which |squeeze score| has reached the trigger threshold and require at
least ``_SUSTAINED_DAYS_MIN``.
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

_VOL_X_SCORE_MIN = float(os.getenv("PLAYBOOK_SQB_VOL_X_SCORE_MIN", "30"))
_GRADIENT_SCORE_MIN = float(os.getenv("PLAYBOOK_SQB_GRADIENT_SCORE_MIN", "30"))
_NET_GEX_FLIP_BAND = float(os.getenv("PLAYBOOK_SQB_NET_GEX_FLIP_BAND", "1.0e9"))
_SPREAD_WIDTH_POINTS = float(os.getenv("PLAYBOOK_SQB_SPREAD_WIDTH", "10.0"))
_DTE_DAYS = int(os.getenv("PLAYBOOK_SQB_DTE_DAYS", "7"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_SQB_MAX_HOLD_MIN", str(3 * 24 * 60)))  # 3 days
_TARGET_RANGE_MULT = float(os.getenv("PLAYBOOK_SQB_TARGET_RANGE_MULT", "2.0"))
_WALL_BUFFER_PCT = float(os.getenv("PLAYBOOK_SQB_WALL_BUFFER_PCT", "0.005"))  # 0.5%
_ENVELOPE_LOOKBACK_BARS = int(os.getenv("PLAYBOOK_SQB_ENVELOPE_LOOKBACK_BARS", "30"))
_ET_START_HOUR = int(os.getenv("PLAYBOOK_SQB_START_HOUR_ET", "10"))
# Number of distinct ET trading days within squeeze_setup history that must
# reach the triggered threshold (clamped >= 0.25, i.e. score >= 25).
_SUSTAINED_DAYS_MIN = int(os.getenv("PLAYBOOK_SQB_SUSTAINED_DAYS_MIN", "2"))
_SUSTAINED_DAILY_THRESHOLD = float(os.getenv("PLAYBOOK_SQB_SUSTAINED_DAILY_THRESHOLD", "0.25"))


BreakoutDirection = Literal["bullish", "bearish"]


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


def _envelope(closes: list[float]) -> Optional[tuple[float, float]]:
    """Return (low, high) of the prior N bars excluding the last.

    Used as a proxy for the "squeeze envelope" — the recent range the
    breakout is exiting.
    """
    usable = [c for c in (closes or []) if c and c > 0]
    if len(usable) < 5:
        return None
    window = usable[-_ENVELOPE_LOOKBACK_BARS - 1 : -1] if len(usable) > 1 else []
    if len(window) < 5:
        return None
    return (min(window), max(window))


class SqueezeBreakoutPattern(PatternBase):
    id = "squeeze_breakout"
    name = "Vol-Compression Resolves"
    tier = "swing"
    direction = "context_dependent"
    valid_regimes = ("trend_expansion", "controlled_trend", "chop_range")
    preferred_regime = "controlled_trend"
    pattern_base = 0.55

    confluence_signals_for = (
        "positioning_trap",
        "tape_flow_bias",
        "dealer_delta_pressure",
    )
    # range_break_imminence "Range Fade" label opposes — handled inline.
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        gex_grad = ctx.signal("gex_gradient")
        # _check_triggers guarantees gex_grad has |score| >= threshold.
        breakout: BreakoutDirection = "bullish" if gex_grad.score > 0 else "bearish"

        close = ctx.close
        envelope = _envelope(ctx.market.recent_closes) or (close, close)
        envelope_low, envelope_high = envelope
        prior_range = envelope_high - envelope_low

        # Instrument: 5-7 DTE +10 strike-width debit spread in breakout direction.
        atm_strike = _round_to_strike(close, 1.0)
        expiry = self._dte_expiry(ctx, _DTE_DAYS)
        if breakout == "bullish":
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

        # Target: 2 × prior range OR (next major gamma level + 0.5% buffer),
        # whichever is closer.
        target_ref, target_level_name = self._pick_target(
            breakout=breakout,
            entry=close,
            prior_range=prior_range,
            call_wall=ctx.level("call_wall"),
            put_wall=ctx.level("put_wall"),
        )

        # Stop: a daily close back inside the envelope invalidates.  We
        # surface that envelope edge as the stop ref price.
        stop_ref = envelope_high if breakout == "bullish" else envelope_low

        confidence = self.compute_confidence(ctx, bias=breakout)
        # Inline against-check: range_break_imminence "Range Fade" opposes.
        rbi = ctx.signal("range_break_imminence")
        if rbi and rbi.context_values.get("label") == "Range Fade":
            confidence = max(0.20, confidence - 0.10)

        rationale = (
            f"Vol compression + gex_gradient {gex_grad.score:+.0f} "
            f"({breakout}) + net_gex within ±${_NET_GEX_FLIP_BAND / 1e9:.1f}B "
            f"of flip → {_DTE_DAYS}-DTE +${_SPREAD_WIDTH_POINTS:.0f} debit spread "
            f"in breakout direction; target {target_level_name}."
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=breakout,
            confidence=confidence,
            size_multiplier=0.5,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="on_break"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name=target_level_name,
            ),
            stop=Stop(
                ref_price=round(stop_ref, 4),
                kind="level",
                level_name="envelope_reentry",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "breakout_direction": breakout,
                "gex_gradient_score": gex_grad.score,
                "vol_expansion_score": (s.score if (s := ctx.signal("vol_expansion")) else None),
                "squeeze_setup_signal": (s.signal if (s := ctx.signal("squeeze_setup")) else None),
                "envelope_low": envelope_low,
                "envelope_high": envelope_high,
                "prior_range": round(prior_range, 4),
                "net_gex": ctx.net_gex,
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

        # Stick to regular hours only — swing entries decided pre-close.
        if ctx.et_time < time(_ET_START_HOUR, 0):
            missing.append(f"too early ({ctx.et_time} ET); needs >= {_ET_START_HOUR:02d}:00 ET")

        if ctx.close <= 0:
            missing.append("close price unavailable")

        squeeze = ctx.signal("squeeze_setup")
        if squeeze is None:
            missing.append("squeeze_setup signal unavailable")
        elif not squeeze.triggered:
            missing.append("squeeze_setup not triggered")
        else:
            # 2-day-sustained gate: count distinct ET trading days where
            # squeeze_setup reached the trigger threshold.  Falls back to
            # accepting the current trigger when no history is loaded
            # (e.g. early bootstrap or test contexts without history wired).
            sustained_days = self._count_sustained_days(squeeze)
            if sustained_days is not None and sustained_days < _SUSTAINED_DAYS_MIN:
                missing.append(
                    f"squeeze_setup triggered on only {sustained_days} ET trading "
                    f"day(s) of recent history (need >= {_SUSTAINED_DAYS_MIN})"
                )

        vol_x = ctx.signal("vol_expansion")
        if vol_x is None:
            missing.append("vol_expansion signal unavailable")
        elif vol_x.score < _VOL_X_SCORE_MIN:
            missing.append(f"vol_expansion score {vol_x.score:.1f} < {_VOL_X_SCORE_MIN:.0f}")

        gex_grad = ctx.signal("gex_gradient")
        if gex_grad is None:
            missing.append("gex_gradient signal unavailable")
        elif abs(gex_grad.score) < _GRADIENT_SCORE_MIN:
            missing.append(
                f"|gex_gradient score| {abs(gex_grad.score):.1f} < {_GRADIENT_SCORE_MIN:.0f} "
                "(no clear breakout direction)"
            )

        if ctx.net_gex is None or abs(ctx.net_gex) > _NET_GEX_FLIP_BAND:
            missing.append(
                f"|net_gex| {abs(ctx.net_gex or 0) / 1e9:.2f}B > flip-band "
                f"{_NET_GEX_FLIP_BAND / 1e9:.1f}B (dealer regime entrenched)"
            )

        # Squeeze direction must agree with gex_gradient direction.
        if squeeze and gex_grad and squeeze.signal:
            grad_dir = "bullish" if gex_grad.score > 0 else "bearish"
            squeeze_label = squeeze.signal
            expected = "bullish_squeeze" if grad_dir == "bullish" else "bearish_squeeze"
            if squeeze_label != expected:
                missing.append(
                    f"squeeze_setup.signal {squeeze_label!r} disagrees with "
                    f"gex_gradient direction {grad_dir!r}"
                )

        # Confirming bar: most recent close has exited the envelope in the
        # gradient-favored direction.
        envelope = _envelope(ctx.market.recent_closes)
        if envelope is None:
            missing.append("insufficient closes to compute squeeze envelope")
        elif gex_grad is not None:
            envelope_low, envelope_high = envelope
            grad_dir = "bullish" if gex_grad.score > 0 else "bearish"
            if grad_dir == "bullish" and ctx.close <= envelope_high:
                missing.append(
                    f"close {ctx.close:.2f} not yet above envelope high {envelope_high:.2f} "
                    "(no confirming breakout)"
                )
            elif grad_dir == "bearish" and ctx.close >= envelope_low:
                missing.append(
                    f"close {ctx.close:.2f} not yet below envelope low {envelope_low:.2f} "
                    "(no confirming breakout)"
                )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _count_sustained_days(snap) -> Optional[int]:
        """Distinct ET trading days where |squeeze score| >= threshold.

        Returns None when ``score_history`` is empty (no history loaded) so
        callers can fall back to "accept current trigger".  Returns 0 when
        history is loaded but no day clears the bar.
        """
        if not getattr(snap, "score_history", None):
            return None
        daily = snap.daily_max_abs()
        return sum(1 for _day, max_abs in daily if max_abs >= _SUSTAINED_DAILY_THRESHOLD)

    @staticmethod
    def _pick_target(
        *,
        breakout: BreakoutDirection,
        entry: float,
        prior_range: float,
        call_wall: Optional[float],
        put_wall: Optional[float],
    ) -> tuple[float, str]:
        range_target = (
            entry + _TARGET_RANGE_MULT * prior_range
            if breakout == "bullish"
            else entry - _TARGET_RANGE_MULT * prior_range
        )

        if breakout == "bullish" and call_wall is not None and call_wall > entry:
            wall_target = call_wall * (1.0 + _WALL_BUFFER_PCT)
            if wall_target - entry <= range_target - entry:
                return wall_target, "call_wall_plus_buffer"
            return range_target, "range_2x"
        if breakout == "bearish" and put_wall is not None and put_wall < entry:
            wall_target = put_wall * (1.0 - _WALL_BUFFER_PCT)
            if entry - wall_target <= entry - range_target:
                return wall_target, "put_wall_minus_buffer"
            return range_target, "range_2x"
        return range_target, "range_2x"

    @staticmethod
    def _dte_expiry(ctx: PlaybookContext, days: int) -> str:
        """Naive +N day expiry — broker integration handles calendar routing."""
        return (ctx.et_date + timedelta(days=days)).isoformat()


PATTERN: PatternBase = SqueezeBreakoutPattern()
