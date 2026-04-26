"""Market State component: distance to gamma flip (vol-adaptive saturation).

The pre-Phase-2.2 implementation saturated at a fixed 2% distance from
the gamma flip strike.  That treats a calm 0.3%-σ day and a volatile
2%-σ day identically, which is wrong: on calm days flip-proximity needs
to be measured tighter (a 1% gap *is* meaningful), while on volatile
days a 1% gap is just noise.

Phase 2.2 replaces the fixed 2% with ``k × realized_sigma`` over a
configurable rolling window of 1-minute bars, bounded by an env-tunable
floor and ceiling.  When ``recent_closes`` is too sparse to compute σ
(early session, missing data) the component falls back to the original
2% so behavior is conservative-equivalent.

Score convention:
  * +1.0  — at the gamma flip (max volatility / inflection potential)
  *  0.0  — half-saturation distance
  * -1.0  — beyond saturation (stable, far from flip)
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import realized_sigma


# Master switch — set to "false" to fall back to the original fixed 2%.
_VOL_ADAPTIVE_ENABLED = os.getenv("SIGNAL_VOL_ADAPTIVE_ENABLED", "true").lower() == "true"

# Realized-sigma lookback in 1-min bars.  30 ≈ last half-hour of session.
_VOL_LOOKBACK_BARS = max(5, int(os.getenv("SIGNAL_VOL_LOOKBACK_BARS", "30")))
_VOL_MIN_BARS = max(5, int(os.getenv("SIGNAL_VOL_MIN_BARS", "10")))

# k × σ saturates the score.  k=10 produces ~1.5% saturation at typical
# SPY 0.15%-per-min sigma — close to the original fixed 2%.
_FLIP_K_SIGMA = float(os.getenv("SIGNAL_FLIP_DISTANCE_K_SIGMA", "10.0"))
_FLIP_FALLBACK_PCT = float(os.getenv("SIGNAL_FLIP_DISTANCE_FALLBACK_PCT", "0.02"))
_FLIP_MIN_PCT = float(os.getenv("SIGNAL_FLIP_DISTANCE_MIN_PCT", "0.005"))
_FLIP_MAX_PCT = float(os.getenv("SIGNAL_FLIP_DISTANCE_MAX_PCT", "0.05"))


def _resolve_saturation(closes: list[float]) -> tuple[float, str]:
    """Pick the saturation pct + a short label for diagnostics."""
    if not _VOL_ADAPTIVE_ENABLED:
        return _FLIP_FALLBACK_PCT, "fixed"
    usable = [c for c in (closes or []) if c and c > 0]
    if len(usable) < _VOL_MIN_BARS:
        return _FLIP_FALLBACK_PCT, "fallback_sparse"
    sigma = realized_sigma(usable, window=_VOL_LOOKBACK_BARS)
    if sigma <= 0:
        return _FLIP_FALLBACK_PCT, "fallback_zero_sigma"
    sat = _FLIP_K_SIGMA * sigma
    sat = max(_FLIP_MIN_PCT, min(_FLIP_MAX_PCT, sat))
    return sat, "vol_adaptive"


class FlipDistanceComponent(ComponentBase):
    name = "flip_distance"
    weight = 0.25

    def compute(self, ctx: MarketContext) -> float:
        fd = (ctx.extra or {}).get("flip_distance")
        if fd is None and ctx.gamma_flip is not None and ctx.close > 0:
            try:
                fd = (ctx.close - float(ctx.gamma_flip)) / ctx.close
            except (TypeError, ValueError, ZeroDivisionError):
                return 0.0
        if fd is None:
            return 0.0
        sat, _ = _resolve_saturation(ctx.recent_closes or [])
        if sat <= 0:
            return 0.0
        # Near flip => +1 (higher volatility potential), far => -1 (stable).
        return max(-1.0, min(1.0, 1.0 - (abs(float(fd)) / sat)))

    def context_values(self, ctx: MarketContext) -> dict:
        sat, sat_source = _resolve_saturation(ctx.recent_closes or [])
        score = self.compute(ctx)
        return {
            "flip_distance": (ctx.extra or {}).get("flip_distance"),
            "score": round(score, 6),
            "weight_points": 19,  # COMPONENT_POINTS["flip_distance"] post Phase 3.1
            "contribution_points": round(score * 19.0, 4),
            "saturation_pct": round(sat, 6),
            "saturation_source": sat_source,
        }
