"""Price-vs-max-gamma component for Market State Index.

Close to max gamma implies pinning (lower move potential).
Far from max gamma implies freer movement (higher potential).

Phase 2.2: the fixed 1% threshold is now ``k × realized_sigma`` of recent
1-min bars, bounded by env-tunable floor / ceiling, with fallback to the
original 1% when ``recent_closes`` is too sparse to compute σ reliably.
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import realized_sigma


_VOL_ADAPTIVE_ENABLED = os.getenv("SIGNAL_VOL_ADAPTIVE_ENABLED", "true").lower() == "true"
_VOL_LOOKBACK_BARS = max(5, int(os.getenv("SIGNAL_VOL_LOOKBACK_BARS", "30")))
_VOL_MIN_BARS = max(5, int(os.getenv("SIGNAL_VOL_MIN_BARS", "10")))

# Pinning is a tighter regime than flip-proximity; saturation expects to
# fire closer to the max-gamma strike.  k=6 with σ=0.15% produces ~0.9%
# saturation — close to the original fixed 1%.
_MAXG_K_SIGMA = float(os.getenv("SIGNAL_PRICE_VS_MAX_GAMMA_K_SIGMA", "6.0"))
_MAXG_FALLBACK_PCT = float(os.getenv("SIGNAL_PRICE_VS_MAX_GAMMA_FALLBACK_PCT", "0.01"))
_MAXG_MIN_PCT = float(os.getenv("SIGNAL_PRICE_VS_MAX_GAMMA_MIN_PCT", "0.003"))
_MAXG_MAX_PCT = float(os.getenv("SIGNAL_PRICE_VS_MAX_GAMMA_MAX_PCT", "0.03"))


def _resolve_saturation(closes: list[float]) -> tuple[float, str]:
    if not _VOL_ADAPTIVE_ENABLED:
        return _MAXG_FALLBACK_PCT, "fixed"
    usable = [c for c in (closes or []) if c and c > 0]
    if len(usable) < _VOL_MIN_BARS:
        return _MAXG_FALLBACK_PCT, "fallback_sparse"
    sigma = realized_sigma(usable, window=_VOL_LOOKBACK_BARS)
    if sigma <= 0:
        return _MAXG_FALLBACK_PCT, "fallback_zero_sigma"
    sat = _MAXG_K_SIGMA * sigma
    sat = max(_MAXG_MIN_PCT, min(_MAXG_MAX_PCT, sat))
    return sat, "vol_adaptive"


class PriceVsMaxGammaComponent(ComponentBase):
    name = "price_vs_max_gamma"
    weight = 10.0

    def compute(self, ctx: MarketContext) -> float:
        max_gamma_strike = (ctx.extra or {}).get("max_gamma_strike")
        if max_gamma_strike is None or ctx.close <= 0:
            return 0.0
        try:
            distance = abs((ctx.close - float(max_gamma_strike)) / ctx.close)
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0
        sat, _ = _resolve_saturation(ctx.recent_closes or [])
        if sat <= 0:
            return 0.0
        # Near max-gamma => -1 (pinned); >= saturation away => +1 (free).
        return max(-1.0, min(1.0, (distance / sat) - 1.0))

    def context_values(self, ctx: MarketContext) -> dict:
        max_gamma_strike = (ctx.extra or {}).get("max_gamma_strike")
        sat, sat_source = _resolve_saturation(ctx.recent_closes or [])
        distance_pct = None
        score = 0.0
        if max_gamma_strike is not None and ctx.close > 0 and sat > 0:
            try:
                distance_pct = abs((ctx.close - float(max_gamma_strike)) / ctx.close)
                score = max(-1.0, min(1.0, (distance_pct / sat) - 1.0))
            except (TypeError, ValueError, ZeroDivisionError):
                distance_pct = None
                score = 0.0
        return {
            "close": ctx.close,
            "max_gamma_strike": max_gamma_strike,
            "distance_pct": distance_pct,
            "score": round(score, 6),
            "saturation_pct": round(sat, 6),
            "saturation_source": sat_source,
        }
