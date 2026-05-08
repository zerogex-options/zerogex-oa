"""Gamma gradient scoring component.

Decomposes per-strike gamma exposure around spot into four buckets:

  * **below_spot_gamma** — dealer gamma support beneath spot
  * **above_spot_gamma** — dealer gamma resistance above spot
  * **wing_gamma** — far-OTM concentration (pinning pressure)
  * **atm_gamma** — near-ATM concentration (short-dated convexity)

A heavy "above spot" skew means dealers are short gamma into a rally — any
up-move triggers call-unwinding (bullish). A heavy "below spot" skew is the
mirror — downside dealer shorts that accelerate a sell-off.

The score is the *asymmetry* of dealer gamma around spot, weighted by the
total notional gamma in the surrounding strike window. When gamma is flat
or the data isn't available we return 0 (the component abstains).

Input is drawn from ``ctx.extra['gex_by_strike']`` — a list of dicts
populated by ``UnifiedSignalEngine._fetch_market_context``. The component
degrades gracefully when the data isn't present.
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Strikes within this percent of spot are treated as "near" (ATM window).
_ATM_WINDOW_PCT = float(os.getenv("SIGNAL_GEX_GRADIENT_ATM_PCT", "0.015"))

# Strikes beyond this percent of spot are treated as "wings".
_WING_WINDOW_PCT = float(os.getenv("SIGNAL_GEX_GRADIENT_WING_PCT", "0.04"))

# Minimum total notional gamma (absolute sum across surveyed strikes) to
# emit a non-zero score. Prevents over-reaction when OI is thin.  Calibrated
# for the industry-standard "dollar gamma per 1% move" GEX convention
# (γ × OI × 100 × S² × 0.01); the prior 5.0e7 default was on the
# share-equivalent scale and is multiplied by ≈7 for SPY-magnitude
# underlyings.
_MIN_TOTAL_GAMMA = float(os.getenv("SIGNAL_GEX_GRADIENT_MIN_GAMMA", "3.5e8"))
_LONG_GAMMA_DAMPING = max(
    0.0,
    min(1.0, float(os.getenv("SIGNAL_GEX_GRADIENT_LONG_GAMMA_DAMPING", "0.40"))),
)


class GexGradientComponent(ComponentBase):
    name = "gex_gradient"
    weight = 0.08

    def compute(self, ctx: MarketContext) -> float:
        buckets = self._buckets(ctx)
        if buckets is None:
            return 0.0

        above = buckets["above_abs_gamma"]
        below = buckets["below_abs_gamma"]
        total = above + below
        if total <= 0:
            return 0.0

        # Positive value = more gamma concentration above spot than below.
        # Under short-gamma this is upside-amplifying; under long-gamma it is
        # resistance and should be damped rather than fully inverted in force.
        asymmetry = (above - below) / total  # in [-1, +1]
        if ctx.net_gex < 0:
            score = asymmetry
        else:
            score = -asymmetry * _LONG_GAMMA_DAMPING

        # Scale confidence by how much of the surveyed gamma is sitting at
        # the wings. Heavy wing concentration = structural pinning, which
        # dampens directional edge.
        wing_fraction = buckets["wing_fraction"]
        wing_confidence = max(0.25, 1.0 - wing_fraction)  # never fully dampen

        # Replace the previous hard cutoff at _MIN_TOTAL_GAMMA with a soft
        # confidence ramp so thin-OI snapshots taper toward zero rather
        # than snapping to it.
        magnitude_confidence = min(1.0, total / _MIN_TOTAL_GAMMA) if _MIN_TOTAL_GAMMA > 0 else 1.0

        return max(-1.0, min(1.0, score * wing_confidence * magnitude_confidence))

    def context_values(self, ctx: MarketContext) -> dict:
        buckets = self._buckets(ctx)
        if buckets is None:
            return {
                "source": "unavailable",
                "above_spot_gamma_abs": None,
                "below_spot_gamma_abs": None,
                "atm_gamma_abs": None,
                "wing_gamma_abs": None,
                "asymmetry": None,
            }
        above = buckets["above_abs_gamma"]
        below = buckets["below_abs_gamma"]
        total = above + below
        asymmetry = (above - below) / total if total > 0 else 0.0
        return {
            "source": "gex_by_strike",
            "above_spot_gamma_abs": round(above, 2),
            "below_spot_gamma_abs": round(below, 2),
            "atm_gamma_abs": round(buckets["atm_gamma_abs"], 2),
            "wing_gamma_abs": round(buckets["wing_gamma_abs"], 2),
            "above_spot_gamma_signed": round(buckets["above_signed_gamma"], 2),
            "below_spot_gamma_signed": round(buckets["below_signed_gamma"], 2),
            "wing_fraction": round(buckets["wing_fraction"], 4),
            "asymmetry": round(asymmetry, 4),
            "strike_count": buckets["strike_count"],
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _buckets(ctx: MarketContext) -> dict | None:
        rows = ctx.extra.get("gex_by_strike") if ctx.extra else None
        if not rows or ctx.close <= 0:
            return None

        above_abs = 0.0
        below_abs = 0.0
        atm_abs = 0.0
        wing_abs = 0.0
        above_signed = 0.0
        below_signed = 0.0
        strike_count = 0

        for row in rows:
            strike = row.get("strike") if isinstance(row, dict) else None
            strike_gex = row.get("net_gex") if isinstance(row, dict) else None
            if strike is None or strike_gex is None:
                continue
            try:
                strike_f = float(strike)
                gex_f = float(strike_gex)
            except (TypeError, ValueError):
                continue
            strike_count += 1
            distance = (strike_f - ctx.close) / ctx.close
            abs_distance = abs(distance)
            abs_gex = abs(gex_f)
            if distance > 0:
                above_abs += abs_gex
                above_signed += gex_f
            elif distance < 0:
                below_abs += abs_gex
                below_signed += gex_f
            if abs_distance <= _ATM_WINDOW_PCT:
                atm_abs += abs_gex
            elif abs_distance >= _WING_WINDOW_PCT:
                wing_abs += abs_gex

        total_abs = above_abs + below_abs
        wing_fraction = wing_abs / total_abs if total_abs > 0 else 0.0
        return {
            "above_abs_gamma": above_abs,
            "below_abs_gamma": below_abs,
            "above_signed_gamma": above_signed,
            "below_signed_gamma": below_signed,
            "atm_gamma_abs": atm_abs,
            "wing_gamma_abs": wing_abs,
            "wing_fraction": min(1.0, wing_fraction),
            "strike_count": strike_count,
        }
