"""Gamma Surface -- net dealer gamma across price AND time to settlement.

The spot-shift profile (``AnalyticsEngine._gamma_exposure_profile``) answers
"what is net dealer gamma if price moves to S right now?". This module adds the
axis that profile holds still -- time -- and answers "...and if price gets there
later, anywhere between now and the first selected settlement?".

Each cell re-prices every included contract's Black-Scholes gamma at the
hypothetical price with that much of its remaining life gone, then sums dealer
dollar gamma in the canonical GEX convention: calls +, puts - (modeled
dealer-long calls / short puts), ``gamma * OI * 100 * S^2 * 0.01`` dollars per
1% move. Implied vol is held at each contract's snapshot value (sticky strike),
as the profile does.

Two choices differ from the regime profile, and both are deliberate:

* No horizon-occupancy weight. That ramp exists so a same-day wall cannot pin
  the multi-day regime flip. Here the member picks which expirations to look
  at, and a same-day wall sharpening into its settlement is the thing the
  surface is for, not a distortion to damp. So the first row is the RAW
  (unweighted) profile of the selected contracts -- the ``gamma_flip_raw``
  convention -- not the weighted one behind the Net GEX headline.

* The time axis ends at the FIRST settlement among the included contracts.
  Every included contract is therefore alive on every row, and the surface has
  no cliff where one drops out. A later expiration's own run-up to settlement
  is one selection away: pick it on its own.

Effective time to expiry is floored at :data:`MIN_TTE_YEARS` (30 minutes) --
the same desk regularization the forced-flow spot-move views use against the
T->0 gamma singularity -- so the last rows into a same-day settlement hold
those contracts at their 30-minute shape instead of collapsing them into a
spike no price grid can sample.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Sequence

import numpy as np

from src.analytics.forced_flow import ContractLeg
from src.greeks_fd import CONTRACT_MULTIPLIER

# 30 minutes, in years -- see the module docstring.
MIN_TTE_YEARS = 30.0 / (365.0 * 24.0 * 60.0)

DEFAULT_PRICE_POINTS = 121
DEFAULT_TIME_STEPS = 41

# Auto span: the farthest included strike from spot plus this margin, so the
# outermost strike's gamma is drawn whole, clamped to [AUTO_SPAN_MIN,
# AUTO_SPAN_MAX]. Ingestion keeps strikes within a few percent of spot, so in
# practice this frames the strikes the book actually holds.
_AUTO_SPAN_MARGIN = 0.005
AUTO_SPAN_MIN = 0.0075
AUTO_SPAN_MAX = 0.06

_SQRT_2PI = float(np.sqrt(2.0 * np.pi))


def auto_span_pct(spot: float, strikes: Iterable[float]) -> float:
    """Half-width of the price grid, as a fraction of spot, for these strikes."""
    if spot <= 0:
        return AUTO_SPAN_MIN
    reach = max((abs(float(k) / spot - 1.0) for k in strikes if k > 0), default=0.0)
    return float(min(AUTO_SPAN_MAX, max(AUTO_SPAN_MIN, reach + _AUTO_SPAN_MARGIN)))


def price_grid(spot: float, span_pct: float, points: int) -> np.ndarray:
    """``points`` evenly spaced prices over ``spot * (1 +/- span_pct)``.

    An odd ``points`` puts spot itself on the middle grid point.
    """
    points = max(2, int(points))
    return np.linspace(spot * (1.0 - span_pct), spot * (1.0 + span_pct), points)


def compute_gamma_surface(
    legs: Sequence[ContractLeg],
    spot: float,
    r: float,
    q: float = 0.0,
    *,
    span_pct: Optional[float] = None,
    price_points: int = DEFAULT_PRICE_POINTS,
    time_steps: int = DEFAULT_TIME_STEPS,
    min_tte_years: float = MIN_TTE_YEARS,
) -> Optional[Dict[str, Any]]:
    """Net dealer dollar gamma on a (forward time x price) grid.

    Returns ``None`` when nothing can be priced (no spot, or no leg with a
    positive strike, open interest, implied vol and time to expiry).
    Otherwise a dict with:

    * ``grid``             -- ascending hypothetical prices.
    * ``times_years``      -- ascending forward times, ``0`` (the snapshot)
                              through ``settles_in_years``.
    * ``profiles``         -- ``len(times_years) x len(grid)``: dealer dollar
                              gamma per 1% move at ``(times_years[i], grid[j])``.
    * ``span_pct``         -- the grid half-width actually used.
    * ``settles_in_years`` -- time to the first settlement among the legs.
    """
    if spot is None or spot <= 0:
        return None
    usable = [
        leg
        for leg in legs
        if leg.strike > 0 and leg.open_interest > 0 and leg.iv > 0 and leg.tte_years > 0
    ]
    if not usable:
        return None

    strikes = np.array([leg.strike for leg in usable], dtype=float)
    sigma = np.array([leg.iv for leg in usable], dtype=float)
    tte = np.array([leg.tte_years for leg in usable], dtype=float)
    signed_oi = np.array(
        [(1.0 if leg.option_type == "C" else -1.0) * leg.open_interest for leg in usable],
        dtype=float,
    )

    span = auto_span_pct(spot, strikes) if span_pct is None else float(span_pct)
    grid = price_grid(spot, span, price_points)

    settles_in = float(tte.min())
    steps = max(1, int(time_steps))
    times = np.linspace(0.0, settles_in, steps) if steps > 1 else np.zeros(1)

    # Everything that does not depend on time, hoisted out of the row loop.
    log_moneyness = np.log(grid[None, :] / strikes[:, None])
    drift = r - q + 0.5 * sigma * sigma
    # gamma * OI * multiplier * S^2 * 0.01 summed with the dealer sign; the
    # 1/S in gamma cancels one S, leaving S * 0.01 per grid point.
    dollar_scale = CONTRACT_MULTIPLIER * grid * 0.01

    profiles = np.empty((times.size, grid.size), dtype=float)
    for i, t in enumerate(times):
        T = np.maximum(tte - t, min_tte_years)
        vol_sqrt_t = sigma * np.sqrt(T)
        d1 = (log_moneyness + (drift * T)[:, None]) / vol_sqrt_t[:, None]
        pdf = np.exp(-0.5 * d1 * d1) / _SQRT_2PI
        weight = signed_oi * np.exp(-q * T) / vol_sqrt_t
        profiles[i] = (weight[:, None] * pdf).sum(axis=0) * dollar_scale

    return {
        "grid": grid.tolist(),
        "times_years": times.tolist(),
        "profiles": profiles.tolist(),
        "span_pct": span,
        "settles_in_years": settles_in,
    }
