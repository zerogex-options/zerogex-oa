"""Intraday regime scoring component.

0DTE behavior is sharply time-of-day dependent:

  * **Open (first 30 min)** — opening range, widest uncertainty, dealer
    hedging is chaotic. Bias the composite toward neutral.
  * **Mid-session** — the dealer-hedging feedback loop dominates;
    trend persistence is highest. Let momentum lead.
  * **Power hour** — charm flow amplifies; mean reversion probability
    increases near heavy OI strikes. Fade stretched moves.
  * **Last 15 min** — gamma pin strengthens dramatically; score toward
    the nearest heavy-OI strike.

This component produces a *modest* directional contribution — it's
context, not conviction. The bulk of its value is in suppressing
over-confident composite scores during chaotic windows.
"""
from __future__ import annotations

from datetime import datetime

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_UTC,
    SESSION_OPEN_MIN_UTC,
    minute_of_day,
    pct_change_n_bar,
)


class IntradayRegimeComponent(ComponentBase):
    name = "intraday_regime"
    weight = 0.02

    def compute(self, ctx: MarketContext) -> float:
        phase = self._phase(ctx.timestamp)
        if phase == "pre_open" or phase == "post_close":
            return 0.0
        if phase == "opening_range":
            # Suppress directional scoring; small momentum lean only.
            return 0.25 * self._momentum_sign(ctx)
        if phase == "mid_session":
            # Let trend persistence through at full strength.
            return self._momentum_sign(ctx)
        if phase == "power_hour":
            # Mean-revert toward max_gamma_strike when stretched.
            return self._mean_revert_bias(ctx)
        if phase == "closing_pin":
            # Hard pin toward the dominant strike.
            return self._pin_bias(ctx)
        return 0.0

    def context_values(self, ctx: MarketContext) -> dict:
        phase = self._phase(ctx.timestamp)
        return {
            "phase": phase,
            "momentum_sign": round(self._momentum_sign(ctx), 4),
            "mean_revert_bias": round(self._mean_revert_bias(ctx), 4),
            "pin_bias": round(self._pin_bias(ctx), 4),
        }

    # ------------------------------------------------------------------
    # Phase detection
    # ------------------------------------------------------------------

    @staticmethod
    def _phase(ts: datetime | None) -> str:
        minute = minute_of_day(ts)
        if minute is None:
            return "unknown"
        if minute < SESSION_OPEN_MIN_UTC:
            return "pre_open"
        if minute >= SESSION_CLOSE_MIN_UTC:
            return "post_close"
        from_open = minute - SESSION_OPEN_MIN_UTC
        to_close = SESSION_CLOSE_MIN_UTC - minute
        if from_open < 30:
            return "opening_range"
        if to_close <= 15:
            return "closing_pin"
        if to_close <= 75:
            return "power_hour"
        return "mid_session"

    # ------------------------------------------------------------------
    # Sub-scores
    # ------------------------------------------------------------------

    @staticmethod
    def _momentum_sign(ctx: MarketContext) -> float:
        pct = pct_change_n_bar(ctx.recent_closes or [], 5)
        return max(-1.0, min(1.0, pct / 0.003))  # 0.3% saturates

    @staticmethod
    def _mean_revert_bias(ctx: MarketContext) -> float:
        """In power hour, lean *away from* an overextended VWAP move."""
        if ctx.vwap is None or ctx.vwap <= 0:
            return 0.0
        stretch = (ctx.close - ctx.vwap) / ctx.vwap
        # 0.5% stretch is considered "extended".
        normalized = max(-1.0, min(1.0, stretch / 0.005))
        # Fade the stretch (minus sign), scaled down to a half-strength bias.
        return -0.5 * normalized

    @staticmethod
    def _pin_bias(ctx: MarketContext) -> float:
        pin = ctx.extra.get("max_gamma_strike") if ctx.extra else None
        if pin is None or ctx.close <= 0:
            return 0.0
        try:
            pin_f = float(pin)
        except (TypeError, ValueError):
            return 0.0
        distance = (pin_f - ctx.close) / ctx.close
        # Pull toward pin — positive if pin is above spot (bullish bias).
        return max(-1.0, min(1.0, distance / 0.003))  # 0.3% saturates
