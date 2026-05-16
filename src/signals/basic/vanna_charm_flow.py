"""Vanna/Charm flow scoring component.

Second-order greeks drive intraday dealer re-hedging in ways that gamma
alone cannot explain:

  * **Vanna** (dVega/dSpot) captures how dealer deltas change when
    volatility moves. As IV crushes through the session (typical morning
    into midday behavior), vanna-short dealers must buy underlying —
    this is the "vol-crush rally" that kills naked put sellers.
  * **Charm** (dDelta/dTime) captures the decay of short-dated deltas
    toward expiry. In the last 2 hours of an expiry session, charm flow
    accelerates dramatically — dealers short calls above spot are
    forced to sell into weakness, amplifying afternoon drift.

The analytics layer populates ``dealer_vanna_exposure`` and
``dealer_charm_exposure`` per-strike in ``gex_by_strike`` (positive =
dealer buying pressure, negative = dealer selling pressure).  Legacy
rows without the dealer columns fall back to negating the raw
``vanna_exposure``/``charm_exposure`` (market-aggregate convention).

Sign convention:
  * Positive aggregate vanna+charm => bullish tailwind (dealer buying)
  * Negative aggregate vanna+charm => bearish headwind (dealer selling)
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_ET,
    SESSION_OPEN_MIN_ET,
    minute_of_day_et,
)

# Vanna and charm are different-axis dollar exposures ($/vol-point vs
# $/day — see _calculate_gex_by_strike), so they are normalized
# INDEPENDENTLY, each by its own scale, before being blended.  This
# makes the component scale-invariant: the authoritative normalizers are
# the data-derived per-symbol p95 magnitudes in ``component_normalizer_cache``
# (``normalizer_cache_refresh`` samples SUM(dealer_vanna_exposure) and
# SUM(dealer_charm_exposure) separately), so any change to the stored
# unit is absorbed by the next cache refresh and the score is unchanged.
# The constants below are only coarse pre-cache / no-cache fallbacks.
#
# Back-compat: ``_VC_NORM`` is retained as an alias (= the vanna
# fallback) for callers/tests that imported it.
_VANNA_NORM = float(os.getenv("SIGNAL_VANNA_NORM", "1.0e7"))
_CHARM_NORM = float(os.getenv("SIGNAL_CHARM_NORM", "1.0e9"))
_VC_NORM = _VANNA_NORM

# Afternoon charm amplification kicks in after this fraction of session.
_CHARM_AMP_START = 0.6  # ~2h before close
_CHARM_AMP_MAX = 1.5


class VannaCharmFlowComponent(ComponentBase):
    name = "vanna_charm_flow"
    weight = 0.04

    def compute(self, ctx: MarketContext) -> float:
        agg = self._aggregate(ctx)
        if agg is None:
            return 0.0
        vanna = agg["vanna"]
        charm = agg["charm"]

        charm_weight = self._charm_amplification(ctx)
        norm_v, norm_c = self._field_norms(ctx)
        if norm_v <= 0 or norm_c <= 0:
            return 0.0
        # Normalize each field by ITS OWN scale (raw $/vol-point and
        # $/day are not addable), then blend the two dimensionless
        # [-1, 1] terms.  Sum-then-clamp keeps the prior behavior that
        # either field reaching its own scale can drive the score.
        v_term = max(-1.0, min(1.0, vanna / norm_v))
        c_term = max(-1.0, min(1.0, (charm * charm_weight) / norm_c))
        return max(-1.0, min(1.0, v_term + c_term))

    def context_values(self, ctx: MarketContext) -> dict:
        agg = self._aggregate(ctx)
        if agg is None:
            return {
                "vanna_total": None,
                "charm_total": None,
                "charm_amplification": round(self._charm_amplification(ctx), 3),
                "source": "unavailable",
            }
        norm_v, norm_c = self._field_norms(ctx)
        return {
            "vanna_total": round(agg["vanna"], 2),
            "charm_total": round(agg["charm"], 2),
            "charm_amplification": round(self._charm_amplification(ctx), 3),
            "vanna_norm": round(norm_v, 2),
            "charm_norm": round(norm_c, 2),
            # Back-compat key (was a single combined scale); now the
            # vanna scale so existing dashboards keep rendering.
            "vc_norm": round(norm_v, 2),
            "source": agg["source"],
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate(ctx: MarketContext) -> dict | None:
        rows = ctx.extra.get("gex_by_strike") if ctx.extra else None
        if not rows:
            return None
        vanna_total = 0.0
        charm_total = 0.0
        used_dealer = False
        saw_any = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                dv = row.get("dealer_vanna_exposure")
                dc = row.get("dealer_charm_exposure")
                if dv is not None:
                    vanna_total += float(dv)
                    used_dealer = True
                    saw_any = True
                elif (v := row.get("vanna_exposure")) is not None:
                    vanna_total += -float(v)
                    saw_any = True

                if dc is not None:
                    charm_total += float(dc)
                    used_dealer = True
                    saw_any = True
                elif (c := row.get("charm_exposure")) is not None:
                    charm_total += -float(c)
                    saw_any = True
            except (TypeError, ValueError):
                continue
        if not saw_any:
            return None
        return {
            "vanna": vanna_total,
            "charm": charm_total,
            "source": "dealer_exposure" if used_dealer else "market_exposure_negated",
        }

    @staticmethod
    def _charm_amplification(ctx: MarketContext) -> float:
        """Scale charm's contribution upward as we approach the close.

        Returns 1.0 for most of the session; ramps to _CHARM_AMP_MAX in
        the final ~2h when charm flow dominates.  Uses ET-native minute
        of day so the ramp is DST-correct year-round.
        """
        minute = minute_of_day_et(ctx.timestamp)
        if minute is None or minute <= SESSION_OPEN_MIN_ET:
            return 1.0
        if minute >= SESSION_CLOSE_MIN_ET:
            return _CHARM_AMP_MAX
        frac = (minute - SESSION_OPEN_MIN_ET) / (SESSION_CLOSE_MIN_ET - SESSION_OPEN_MIN_ET)
        if frac < _CHARM_AMP_START:
            return 1.0
        ramp = (frac - _CHARM_AMP_START) / (1.0 - _CHARM_AMP_START)
        return 1.0 + (_CHARM_AMP_MAX - 1.0) * ramp

    @staticmethod
    def _field_norms(ctx: MarketContext) -> tuple[float, float]:
        """Per-field (vanna, charm) saturation scales.

        Prefers the data-derived per-symbol magnitudes in
        ``component_normalizer_cache`` (exposed via
        ``ctx.extra['normalizers']`` keyed by ``dealer_vanna_exposure`` /
        ``dealer_charm_exposure``) so the component is scale-invariant —
        a change to the stored unit is absorbed by the next cache
        refresh.  Falls back to the coarse module constants per field.
        """
        norm_v = _VANNA_NORM
        norm_c = _CHARM_NORM
        extra = ctx.extra if isinstance(ctx.extra, dict) else {}
        normalizers = extra.get("normalizers") if isinstance(extra, dict) else None
        if isinstance(normalizers, dict):
            try:
                fv = float(normalizers.get("dealer_vanna_exposure"))
                if fv > 0:
                    norm_v = fv
            except (TypeError, ValueError):
                pass
            try:
                fc = float(normalizers.get("dealer_charm_exposure"))
                if fc > 0:
                    norm_c = fc
            except (TypeError, ValueError):
                pass
        return norm_v, norm_c
