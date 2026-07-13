"""Shared finite-difference Greeks kernel: charm and vanna.

Single source of truth for the second-order dealer-hedging Greeks. Charm and
vanna are computed by FINITE DIFFERENCE against the canonical Black-Scholes-
Merton delta (:func:`bsm_delta`), never from closed forms. This is deliberate:

* It is correct by construction -- the literal definition of the derivative.
* It inherits the delta function's r / q / day-count conventions automatically,
  so there is no second implementation to drift out of sync.
* It is immune to the d(delta)/dt vs d(delta)/dT sign flip that is the classic
  charm bug.

Both the ingestion Greeks engine (``GreeksCalculator``) and the analytics
exposure engine (``AnalyticsEngine``) call these functions, so the per-contract
and aggregate charm/vanna can never diverge. Closed-form expressions live only
in the test suite, as validation targets (see tests/test_greeks_charm_vanna).

Conventions (match the existing GEX / delta code exactly):

* ``bsm_delta``  -- holder delta, dividend-discounted:
  call = e^{-qT}*N(d1); put = e^{-qT}*(N(d1) - 1). Returns 0.0 on degenerate
  inputs, the same guard the ingestion delta uses.
* ``fd_vanna``   -- d(delta)/d(sigma) per *unit* of sigma (sigma=1.00 == 100
  vol points). This is the same unit the analytics dollar step multiplies by
  0.01, so it is a drop-in for the previous closed form. Vanna is option-type
  independent (calls and puts share it) because e^{-qT} does not depend on
  sigma.
* ``fd_charm``   -- d(delta)/dt per *calendar day*. Calendar time t advances,
  so time-to-expiry T shrinks: charm is the change in delta as one full day
  passes. Call and put charm differ by exactly -q*e^{-qT}; at q=0 they are
  identical.

The dollar-conversion helpers put all three second-order flows on one unit --
dollars of stock a dealer must trade per unit of the driver -- reusing the
existing GEX dollar-gamma constant so nothing is reimplemented.
"""

from __future__ import annotations

import math

# 1/sqrt(2) -- precomputed for the erf-based normal CDF below.
_INV_SQRT2: float = 1.0 / math.sqrt(2.0)


def _norm_cdf(x: float) -> float:
    """Standard-normal CDF via ``math.erf`` -- N(x) = 0.5*(1 + erf(x/sqrt(2))).

    Accurate to ~1e-15 (identical to ``scipy.stats.norm.cdf`` to machine
    precision) but ~50-100x faster per scalar call: no SciPy frozen-distribution
    dispatch, no NumPy scalar boxing. This matters because :func:`bsm_delta` is
    called hundreds of thousands of times per analytics cycle by the forced-flow
    grid scans.
    """
    return 0.5 * (1.0 + math.erf(x * _INV_SQRT2))


# Finite-difference step sizes (spec 5.1). One calendar day of time; one
# volatility point of IV. Chosen so the *output units* are the ones we want
# ("delta per calendar day", "delta per vol point") without post-scaling.
CHARM_H_DAYS: float = 1.0 / 365.0  # one calendar day, expressed in years
VANNA_H_VOL: float = 0.01  # one volatility point (absolute)

# Shares per standard option contract -- the same multiplier the GEX
# dollar-gamma formula uses.
CONTRACT_MULTIPLIER: int = 100


def bsm_delta(
    S: float, K: float, T: float, r: float, sigma: float, option_type: str, q: float = 0.0
) -> float:
    """Black-Scholes-Merton delta -- the single canonical delta both layers use.

    Dividend-discounted: call = e^{-qT}*N(d1), put = e^{-qT}*(N(d1) - 1).
    Returns 0.0 for degenerate inputs (S/K/sigma/T <= 0) so an invalid or
    expired contract reads as non-directional rather than half-delta -- the
    same guard the ingestion Greeks apply.

    Args:
        S: Underlying price.
        K: Strike price.
        T: Time to expiration (years).
        r: Risk-free rate.
        sigma: Implied volatility.
        option_type: 'C' for call, anything else treated as put.
        q: Continuous dividend yield (0.0 = dividend-free model).
    """
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        return 0.0

    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    discount = math.exp(-q * T)

    if option_type == "C":
        return discount * _norm_cdf(d1)
    # Put
    return discount * (_norm_cdf(d1) - 1.0)


def fd_vanna(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str = "C",
    q: float = 0.0,
    h_vol: float = VANNA_H_VOL,
) -> float:
    """Vanna = d(delta)/d(sigma) per unit sigma, by central finite difference.

    Central difference on sigma (sigma +/- h) rather than the one-sided form --
    same quantity, but O(h^2) truncation error instead of O(h), so it matches
    the closed form more tightly. Vanna is option-type independent: e^{-qT}
    does not depend on sigma, so d(delta_put)/d(sigma) == d(delta_call)/d(sigma).
    ``option_type`` is accepted only for a uniform signature.

    Near sigma=0 the step is shrunk to keep sigma-h strictly positive (delta is
    0 for sigma<=0), preserving a valid two-sided estimate.
    """
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        return 0.0

    h = h_vol if sigma - h_vol > 0 else 0.5 * sigma
    up = bsm_delta(S, K, T, r, sigma + h, option_type, q)
    dn = bsm_delta(S, K, T, r, sigma - h, option_type, q)
    return (up - dn) / (2.0 * h)


def fd_charm(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str = "C",
    q: float = 0.0,
    h_days: float = CHARM_H_DAYS,
) -> float:
    """Charm = d(delta)/dt per calendar day, by one-sided finite difference.

    Calendar time t advances, so time-to-expiry T shrinks: charm is the change
    in delta as one full day passes,

        charm_per_day = delta(T - h) - delta(T),   h = one day in years.

    The one-sided (backward-in-T) step is deliberate -- it is the realized
    one-day decay a dealer actually faces, and it is what makes |charm| explode
    into the close (a near-the-money 0DTE option must resolve to 0 or 100).

    Boundary (final day): when T - h would be <= 0, the step is shrunk to keep
    T - h strictly inside (0, T] and the partial-day change is rescaled to a
    per-day rate. This preserves the unit and the near-expiry blow-up without
    ever evaluating delta at a non-positive T, and does not change the sign.

    Call and put charm differ by exactly -q*e^{-qT}; at q=0 they are equal.
    """
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        return 0.0

    h = h_days
    if T - h <= 0:
        h = 0.5 * T  # one-sided step that stays strictly inside (0, T]
    now = bsm_delta(S, K, T, r, sigma, option_type, q)
    nxt = bsm_delta(S, K, T - h, r, sigma, option_type, q)
    # Rescale the (possibly partial) step to a per-calendar-day rate.
    return (nxt - now) * (h_days / h)


def dollar_gamma_per_1pct(gamma: float, open_interest: float, spot: float) -> float:
    """Dollars of stock a dealer trades per 1% spot move: gamma*OI*100*S^2*0.01.

    Identical to the existing GEX dollar-gamma formula -- reused, not
    reimplemented (spec 5.3).
    """
    return gamma * open_interest * CONTRACT_MULTIPLIER * spot * spot * 0.01


def dollar_charm_per_day(charm_per_day: float, open_interest: float, spot: float) -> float:
    """Dollars of stock a dealer trades per calendar day: charm_day*OI*100*S.

    ``charm_per_day`` is the :func:`fd_charm` output (already per day), so no
    time scaling is applied here.
    """
    return charm_per_day * open_interest * CONTRACT_MULTIPLIER * spot


def dollar_vanna_per_volpt(vanna_per_unit_sigma: float, open_interest: float, spot: float) -> float:
    """Dollars of stock a dealer trades per 1 vol point: (d(delta)/d(sigma))*OI*100*S*0.01.

    ``vanna_per_unit_sigma`` is the :func:`fd_vanna` output (per unit sigma);
    the 0.01 converts it to a one-vol-point perturbation, matching the
    analytics exposure loop exactly.
    """
    return vanna_per_unit_sigma * open_interest * CONTRACT_MULTIPLIER * spot * 0.01
