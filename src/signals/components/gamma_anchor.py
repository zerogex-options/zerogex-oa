"""Unified gamma-anchor component (Phase 2.1).

Replaces the over-counting that previously existed between three closely
related MSI components — ``flip_distance``, ``local_gamma``, and
``price_vs_max_gamma`` — by computing a single weighted blend of their
sub-signals.  All three measure how anchored price is to a dealer-gamma
reference point; combining them produces one vote in the composite while
each still appears in the API ``components`` dict (with weight 0) so
front-end visualizations that hard-coded their keys keep rendering.

Sign convention (consistent across all three sub-signals after Phase 2.2's
vol-adaptive saturation update):

  * +1.0 — price is "free" relative to the gamma anchor (flip is near,
           local gamma is thin, max-gamma strike is far) → expect movement.
  *  0.0 — neutral / insufficient data
  * -1.0 — price is "anchored" (far from flip, dense local gamma, sitting
           at max-gamma strike) → expect chop/pinning.

The default blend weights mirror our prior conviction about each
sub-signal's reliability:

  * flip_distance       0.45  — most predictive of regime change
  * local_gamma         0.35  — strongest pinning gauge
  * price_vs_max_gamma  0.20  — weakest standalone, partially redundant
                                  with local_gamma

All three weights are env-tunable and re-normalized to sum to 1.0 so
operators can re-balance without touching code.
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.flip_distance import FlipDistanceComponent
from src.signals.components.local_gamma import LocalGammaComponent
from src.signals.components.price_vs_max_gamma import PriceVsMaxGammaComponent


# Internal blend weights.  Re-normalized at module load so operators can
# override one without having to recompute the others.
_W_FLIP = max(0.0, float(os.getenv("SIGNAL_GAMMA_ANCHOR_W_FLIP", "0.45")))
_W_LOCAL = max(0.0, float(os.getenv("SIGNAL_GAMMA_ANCHOR_W_LOCAL", "0.35")))
_W_MAXG = max(0.0, float(os.getenv("SIGNAL_GAMMA_ANCHOR_W_MAX_GAMMA", "0.20")))

_W_TOTAL = _W_FLIP + _W_LOCAL + _W_MAXG
if _W_TOTAL <= 0:
    # Pathological config; fall back to the defaults so the component
    # still emits a usable score.
    _W_FLIP, _W_LOCAL, _W_MAXG, _W_TOTAL = 0.45, 0.35, 0.20, 1.00
_W_FLIP /= _W_TOTAL
_W_LOCAL /= _W_TOTAL
_W_MAXG /= _W_TOTAL


class GammaAnchorComponent(ComponentBase):
    name = "gamma_anchor"
    # Class-level weight is informational only — the authoritative weight
    # lives in ScoringEngine.COMPONENT_POINTS.
    weight = 0.30

    def __init__(self) -> None:
        # Reuse the existing sub-components so the actual scoring math
        # (including Phase 2.2's vol-adaptive saturation) stays in one
        # place.  These instances are never registered with the scoring
        # engine — they're delegates owned by gamma_anchor.
        self._flip = FlipDistanceComponent()
        self._local = LocalGammaComponent()
        self._maxg = PriceVsMaxGammaComponent()

    def compute(self, ctx: MarketContext) -> float:
        flip_score = float(self._flip.compute(ctx))
        local_score = float(self._local.compute(ctx))
        maxg_score = float(self._maxg.compute(ctx))
        blended = (
            _W_FLIP * flip_score
            + _W_LOCAL * local_score
            + _W_MAXG * maxg_score
        )
        return max(-1.0, min(1.0, blended))

    def context_values(self, ctx: MarketContext) -> dict:
        flip_score = float(self._flip.compute(ctx))
        local_score = float(self._local.compute(ctx))
        maxg_score = float(self._maxg.compute(ctx))
        blended = max(
            -1.0,
            min(
                1.0,
                _W_FLIP * flip_score
                + _W_LOCAL * local_score
                + _W_MAXG * maxg_score,
            ),
        )
        return {
            "score": round(blended, 6),
            "flip_distance_subscore": round(flip_score, 6),
            "local_gamma_subscore": round(local_score, 6),
            "price_vs_max_gamma_subscore": round(maxg_score, 6),
            "blend_weights": {
                "flip_distance": round(_W_FLIP, 4),
                "local_gamma": round(_W_LOCAL, 4),
                "price_vs_max_gamma": round(_W_MAXG, 4),
            },
        }
