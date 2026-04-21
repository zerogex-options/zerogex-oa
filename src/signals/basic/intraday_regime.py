"""Intraday regime scoring component.

The component contributes a directional score to the composite while also
publishing a separate intraday "Market State Index" (0-100) that estimates
how decisive the next move setup is.

Market State Index inputs and weights:
  * Net GEX sign (+/-20)
  * Flip distance (+/-25)
  * Local gamma density (+/-20)
  * Put/call ratio (+/-15)
  * Price vs max gamma (+/-10)
  * Volatility regime (+/-10)
"""
from __future__ import annotations

from datetime import datetime
import math

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_ET,
    SESSION_OPEN_MIN_ET,
    minute_of_day_et,
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
        market_state_index = self._market_state_index(ctx)
        return {
            "phase": phase,
            "momentum_sign": round(self._momentum_sign(ctx), 4),
            "mean_revert_bias": round(self._mean_revert_bias(ctx), 4),
            "pin_bias": round(self._pin_bias(ctx), 4),
            "market_state_index": round(market_state_index, 2),
            "intraday_score": round(market_state_index, 2),
            "net_gex_sign_score": round(self._net_gex_sign_score(ctx), 4),
            "flip_distance_score": round(self._flip_distance_score(ctx), 4),
            "local_gamma_score": round(self._local_gamma_score(ctx), 4),
            "put_call_ratio_score": round(self._put_call_ratio_score(ctx), 4),
            "price_vs_max_gamma_score": round(self._price_vs_max_gamma_score(ctx), 4),
            "volatility_regime_score": round(self._volatility_regime_score(ctx), 4),
        }

    # ------------------------------------------------------------------
    # Phase detection
    # ------------------------------------------------------------------

    @staticmethod
    def _phase(ts: datetime | None) -> str:
        minute = minute_of_day_et(ts)
        if minute is None:
            return "unknown"
        if minute < SESSION_OPEN_MIN_ET:
            return "pre_open"
        if minute >= SESSION_CLOSE_MIN_ET:
            return "post_close"
        from_open = minute - SESSION_OPEN_MIN_ET
        to_close = SESSION_CLOSE_MIN_ET - minute
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

    # ------------------------------------------------------------------
    # Market State Index (0-100)
    # ------------------------------------------------------------------

    @staticmethod
    def _market_state_index(ctx: MarketContext) -> float:
        # Convert weighted signed components to a 0..100 index.
        score = 50.0
        score += 20.0 * IntradayRegimeComponent._net_gex_sign_score(ctx)
        score += 25.0 * IntradayRegimeComponent._flip_distance_score(ctx)
        score += 20.0 * IntradayRegimeComponent._local_gamma_score(ctx)
        score += 15.0 * IntradayRegimeComponent._put_call_ratio_score(ctx)
        score += 10.0 * IntradayRegimeComponent._price_vs_max_gamma_score(ctx)
        score += 10.0 * IntradayRegimeComponent._volatility_regime_score(ctx)
        return max(0.0, min(100.0, score))

    @staticmethod
    def _net_gex_sign_score(ctx: MarketContext) -> float:
        if ctx.net_gex < 0:
            return 1.0
        if ctx.net_gex > 0:
            return -1.0
        return 0.0

    @staticmethod
    def _flip_distance_score(ctx: MarketContext) -> float:
        extra = ctx.extra or {}
        fd = extra.get("flip_distance")
        if fd is None and ctx.gamma_flip and ctx.close > 0:
            try:
                fd = (ctx.close - float(ctx.gamma_flip)) / ctx.close
            except (TypeError, ValueError, ZeroDivisionError):
                fd = None
        if fd is None:
            return 0.0
        # Near flip => +1 (high potential), far => -1 (stable)
        return max(-1.0, min(1.0, 1.0 - (abs(float(fd)) / 0.02)))

    @staticmethod
    def _local_gamma_score(ctx: MarketContext) -> float:
        local_gex = float((ctx.extra or {}).get("local_gex") or 0.0)
        normalizer = float((ctx.extra or {}).get("normalizers", {}).get("local_gex") or 0.0)
        if normalizer <= 0:
            normalizer = max(abs(ctx.net_gex), 1.0)
        ratio = local_gex / max(normalizer, 1.0)
        # High local gamma => pinning/stability (-1), low local gamma => air pocket (+1)
        return max(-1.0, min(1.0, 1.0 - 2.0 * min(ratio, 1.0)))

    @staticmethod
    def _put_call_ratio_score(ctx: MarketContext) -> float:
        pcr = float(ctx.put_call_ratio or 1.0)
        # Contrarian-style: extreme fear (high PCR) can mean higher move potential.
        return max(-1.0, min(1.0, (pcr - 1.0) / 0.4))

    @staticmethod
    def _price_vs_max_gamma_score(ctx: MarketContext) -> float:
        max_gamma_strike = (ctx.extra or {}).get("max_gamma_strike")
        if max_gamma_strike is None or ctx.close <= 0:
            return 0.0
        try:
            distance = abs((ctx.close - float(max_gamma_strike)) / ctx.close)
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0
        # Close to max gamma is pinning/stable (-1); far away implies freer movement (+1).
        return max(-1.0, min(1.0, (distance / 0.01) - 1.0))

    @staticmethod
    def _volatility_regime_score(ctx: MarketContext) -> float:
        vix = (ctx.extra or {}).get("vix_level")
        if vix is not None:
            try:
                return max(-1.0, min(1.0, (float(vix) - 20.0) / 10.0))
            except (TypeError, ValueError):
                pass

        closes = ctx.recent_closes or []
        if len(closes) < 2:
            return 0.0
        rets = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            curr = closes[i]
            if prev and prev > 0:
                rets.append((curr - prev) / prev)
        if not rets:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        realized = math.sqrt(max(var, 0.0))
        return max(-1.0, min(1.0, (realized - 0.002) / 0.003))
