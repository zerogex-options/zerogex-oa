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

# Normalize combined vanna+charm exposure so that a magnitude of this
# value saturates the score.  Calibrated for the dollar-scale dealer
# exposure convention (vanna × OI × 100 × S, summed across strikes &
# expirations).  For SPY-magnitude underlyings the typical NET dealer
# vanna+charm sum runs in the hundreds of millions to low billions —
# the prior 5e7 default saturated almost permanently and made the
# score flip between ±1 with every sign-change in the underlying
# (visible as a +100/−100 sawtooth in score history).  Per-symbol
# normalizers from ``component_normalizer_cache`` still override at
# runtime when populated.
_VC_NORM = float(os.getenv("SIGNAL_VANNA_CHARM_NORM", "1.0e9"))

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
        combined = vanna + charm * charm_weight
        norm = self._vc_norm(ctx)
        if norm <= 0:
            return 0.0
        normalized = max(-1.0, min(1.0, combined / norm))
        return normalized

    def context_values(self, ctx: MarketContext) -> dict:
        agg = self._aggregate(ctx)
        if agg is None:
            return {
                "vanna_total": None,
                "charm_total": None,
                "charm_amplification": round(self._charm_amplification(ctx), 3),
                "source": "unavailable",
            }
        return {
            "vanna_total": round(agg["vanna"], 2),
            "charm_total": round(agg["charm"], 2),
            "charm_amplification": round(self._charm_amplification(ctx), 3),
            "vc_norm": round(self._vc_norm(ctx), 2),
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
    def _vc_norm(ctx: MarketContext) -> float:
        """Use dynamic symbol normalizer when available; else fallback constant."""
        extra = ctx.extra if isinstance(ctx.extra, dict) else {}
        normalizers = extra.get("normalizers") if isinstance(extra, dict) else None
        if isinstance(normalizers, dict):
            v = normalizers.get("dealer_vanna_exposure")
            c = normalizers.get("dealer_charm_exposure")
            vals = []
            for raw in (v, c):
                try:
                    fv = float(raw)
                except (TypeError, ValueError):
                    continue
                if fv > 0:
                    vals.append(fv)
            if vals:
                # Combined flow scale; avoid under-normalizing from a single field.
                return max(_VC_NORM * 0.5, sum(vals))
        return _VC_NORM
