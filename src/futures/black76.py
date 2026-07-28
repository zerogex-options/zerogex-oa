"""Black-76 pricing and Greeks for options on futures.

Options on ES/NQ futures are priced with the **Black-76** model (Black 1976),
NOT the Black-Scholes-Merton spot model used for the cash-index/ETF options
(``src/ingestion/greeks_calculator.py`` / ``src/greeks_fd.py``).  The
difference is not cosmetic:

* The underlying is the **futures price F**, which already embeds cost-of-carry
  — so there is **no separate spot, no dividend yield q**, and the drift term
  in ``d1`` is ``+½σ²T`` (no ``r − q``).
* The premium is **discounted at r** (options on futures are typically
  futures-style *or* equity-style margined; here we model the standard
  premium-paid equity-style option, so the premium and every Greek carry the
  ``e^{-rT}`` discount factor).
* **Futures delta ≠ equity delta.**  A Black-76 delta is the sensitivity to a
  1-point move in the *future*, hedged with the *future itself* — never
  interchangeable with an equity/index delta.  Downstream metadata always
  tags Greeks as futures Greeks so the two are never conflated (spec Phase 6).

Formulas (F futures price, K strike, T years, r rate, σ vol; N cdf, n pdf):

    d1 = [ln(F/K) + ½σ²T] / (σ√T),   d2 = d1 − σ√T
    call = e^{-rT}[F N(d1) − K N(d2)]
    put  = e^{-rT}[K N(−d2) − F N(−d1)]

    delta_call = e^{-rT} N(d1)          delta_put = −e^{-rT} N(−d1)
    gamma      = e^{-rT} n(d1) / (F σ√T)                (same both)
    vega       = F e^{-rT} n(d1) √T                     (per 1.00 vol)
    theta_yr   = r·price − F e^{-rT} n(d1) σ / (2√T)    (per year)
    rho        = −T · price                             (per 1.00 rate)

Vanna and charm are computed by **central/backward finite differences of the
Black-76 delta**, matching the deliberate house choice in ``greeks_fd.py``
(correct-by-construction, immune to closed-form sign bugs, and automatically
consistent with whatever ``r``/day-count is configured).

Reported Greek units (documented, and echoed in :class:`Black76Result`):

* ``delta``   — per 1.0 change in F (dimensionless)
* ``gamma``   — per 1.0 change in F (units 1/point)
* ``vega``    — per **1% (0.01)** change in σ    (annual vega × 0.01)
* ``theta``   — per **calendar day**             (annual theta ÷ 365)
* ``rho``     — per **1% (0.01)** change in r
* ``vanna``   — d(delta)/dσ per **1.00** vol      (σ=1.00 ≙ 100 vol points)
* ``charm``   — d(delta)/dt per **calendar day**
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

#: Day-count for converting an expiration instant to years.  Calendar 365,
#: matching ``src/market_calendar.calculate_time_to_expiration``.
DAY_COUNT = 365.0

#: Finite-difference steps for the second-order Greeks (mirror greeks_fd.py).
_VANNA_H_VOL = 0.01  # 1 vol point
_CHARM_H_DAYS = 1.0 / DAY_COUNT  # 1 calendar day in year units

#: IV solver bounds/behaviour (mirror src/config IV_* defaults so futures IV
#: is calibrated like the equity path).
DEFAULT_IV_MIN = 0.01
DEFAULT_IV_MAX = 5.0
DEFAULT_IV_TOLERANCE = 1e-5
DEFAULT_IV_MAX_ITERATIONS = 100


class ModelQuality(str, Enum):
    GOOD = "good"
    #: T <= 0 — priced at discounted intrinsic, Greeks degenerate.
    EXPIRED = "expired"
    #: An input was non-finite / non-positive; result is a safe zero.
    INVALID_INPUT = "invalid_input"
    #: IV solve saturated a bound / did not converge (price side only).
    IV_UNRESOLVED = "iv_unresolved"


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via erf (no scipy dependency in the hot loop)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


@dataclass(frozen=True)
class Black76Result:
    price: float
    delta: float
    gamma: float
    vega: float  # per 1% vol
    theta: float  # per calendar day
    rho: float  # per 1% rate
    vanna: float  # d(delta)/dvol per 1.00 vol
    charm: float  # d(delta)/dt per day
    intrinsic: float
    extrinsic: float
    d1: Optional[float]
    d2: Optional[float]
    quality: ModelQuality

    # Documented units, carried with the result so consumers never guess.
    vega_unit: str = "per_1pct_vol"
    theta_unit: str = "per_calendar_day"
    rho_unit: str = "per_1pct_rate"
    delta_convention: str = "futures"  # NOT equity/index delta

    @property
    def is_usable(self) -> bool:
        return self.quality in (ModelQuality.GOOD, ModelQuality.EXPIRED)


def _intrinsic(futures_price: float, strike: float, is_call: bool) -> float:
    return max(0.0, (futures_price - strike) if is_call else (strike - futures_price))


def d1_d2(F: float, K: float, T: float, sigma: float) -> tuple[float, float]:
    """Black-76 d1/d2.  Caller guarantees F,K,T,sigma > 0."""
    vol_sqrt_t = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / vol_sqrt_t
    return d1, d1 - vol_sqrt_t


def price(
    futures_price: float,
    strike: float,
    t_years: float,
    sigma: float,
    is_call: bool,
    rate: float = 0.0,
) -> float:
    """Black-76 option premium (index points).  Stable at/near expiry."""
    F, K, T = futures_price, strike, t_years
    disc = math.exp(-rate * max(T, 0.0))
    if T <= 0.0 or sigma <= 0.0 or F <= 0.0 or K <= 0.0:
        # Degenerate → discounted intrinsic (the correct limit).
        return disc * _intrinsic(F, K, is_call)
    d1, d2 = d1_d2(F, K, T, sigma)
    if is_call:
        return disc * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    return disc * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))


def delta(
    futures_price: float,
    strike: float,
    t_years: float,
    sigma: float,
    is_call: bool,
    rate: float = 0.0,
) -> float:
    """Black-76 futures delta, e^{-rT}·N(d1) (call) / −e^{-rT}·N(−d1) (put)."""
    F, K, T = futures_price, strike, t_years
    if T <= 0.0 or sigma <= 0.0 or F <= 0.0 or K <= 0.0:
        # At expiry delta is the step function (ignoring discount).
        itm = (F > K) if is_call else (F < K)
        if F == K:
            return 0.5 if is_call else -0.5
        base = 1.0 if itm else 0.0
        return base if is_call else -base
    disc = math.exp(-rate * T)
    d1, _ = d1_d2(F, K, T, sigma)
    return disc * _norm_cdf(d1) if is_call else -disc * _norm_cdf(-d1)


def evaluate(
    futures_price: float,
    strike: float,
    t_years: float,
    sigma: float,
    is_call: bool,
    rate: float = 0.0,
) -> Black76Result:
    """Full Black-76 price + Greeks for one option.

    All inputs are plain floats; ``t_years`` is the exact time-to-expiry in
    years (see :func:`years_to_expiration`).  Returns a :class:`Black76Result`
    whose ``quality`` flags degenerate regimes rather than raising.
    """
    F, K, T = futures_price, strike, t_years
    intrinsic = _intrinsic(F, K, is_call)

    if not all(math.isfinite(v) for v in (F, K, T, sigma, rate)) or F <= 0 or K <= 0:
        return Black76Result(
            price=0.0,
            delta=0.0,
            gamma=0.0,
            vega=0.0,
            theta=0.0,
            rho=0.0,
            vanna=0.0,
            charm=0.0,
            intrinsic=intrinsic,
            extrinsic=0.0,
            d1=None,
            d2=None,
            quality=ModelQuality.INVALID_INPUT,
        )

    if T <= 0.0 or sigma <= 0.0:
        disc = math.exp(-rate * max(T, 0.0))
        px = disc * intrinsic
        return Black76Result(
            price=px,
            delta=delta(F, K, T, sigma, is_call, rate),
            gamma=0.0,
            vega=0.0,
            theta=0.0,
            rho=-max(T, 0.0) * px * 0.01,
            vanna=0.0,
            charm=0.0,
            intrinsic=intrinsic,
            extrinsic=max(0.0, px - intrinsic),
            d1=None,
            d2=None,
            quality=ModelQuality.EXPIRED,
        )

    disc = math.exp(-rate * T)
    sqrt_t = math.sqrt(T)
    d1, d2 = d1_d2(F, K, T, sigma)
    pdf_d1 = _norm_pdf(d1)

    px = price(F, K, T, sigma, is_call, rate)
    dlt = delta(F, K, T, sigma, is_call, rate)
    gamma = disc * pdf_d1 / (F * sigma * sqrt_t)
    vega_annual = F * disc * pdf_d1 * sqrt_t
    theta_annual = rate * px - (F * disc * pdf_d1 * sigma) / (2.0 * sqrt_t)
    rho_absolute = -T * px

    # FD second-order Greeks against the Black-76 delta (house convention).
    vanna = (
        delta(F, K, T, sigma + _VANNA_H_VOL, is_call, rate)
        - delta(F, K, T, sigma - _VANNA_H_VOL, is_call, rate)
    ) / (2.0 * _VANNA_H_VOL)
    t_back = T - _CHARM_H_DAYS
    if t_back > 0.0:
        charm = (delta(F, K, t_back, sigma, is_call, rate) - dlt) / _CHARM_H_DAYS
    else:
        # Within a day of expiry: one-sided forward step keeps it finite.
        charm = (dlt - delta(F, K, T + _CHARM_H_DAYS, sigma, is_call, rate)) / _CHARM_H_DAYS

    return Black76Result(
        price=px,
        delta=dlt,
        gamma=gamma,
        vega=vega_annual * 0.01,  # per 1% vol
        theta=theta_annual / DAY_COUNT,  # per calendar day
        rho=rho_absolute * 0.01,  # per 1% rate
        vanna=vanna,
        charm=charm,
        intrinsic=intrinsic,
        extrinsic=max(0.0, px - intrinsic),
        d1=d1,
        d2=d2,
        quality=ModelQuality.GOOD,
    )


def implied_volatility(
    market_price: float,
    futures_price: float,
    strike: float,
    t_years: float,
    is_call: bool,
    rate: float = 0.0,
    *,
    iv_min: float = DEFAULT_IV_MIN,
    iv_max: float = DEFAULT_IV_MAX,
    tolerance: float = DEFAULT_IV_TOLERANCE,
    max_iterations: int = DEFAULT_IV_MAX_ITERATIONS,
    initial_guess: float = 0.25,
) -> Optional[float]:
    """Solve Black-76 implied volatility (Newton-Raphson + bisection fallback).

    Returns ``None`` when the price is below discounted intrinsic (no
    solution), the option is expired, or the solver saturates a bound — the
    same "leave it NULL rather than fabricate" contract as the equity IV
    solver (``src/ingestion/iv_calculator.py``).
    """
    F, K, T = futures_price, strike, t_years
    if T <= 0.0 or F <= 0.0 or K <= 0.0 or market_price <= 0.0:
        return None

    disc = math.exp(-rate * T)
    intrinsic = disc * _intrinsic(F, K, is_call)
    upper_bound = disc * (F if is_call else K)
    # Price must sit in (discounted intrinsic, discounted upper bound).
    if market_price < intrinsic - tolerance or market_price > upper_bound + tolerance:
        return None

    sigma = min(max(initial_guess, iv_min), iv_max)
    for _ in range(max_iterations):
        model = price(F, K, T, sigma, is_call, rate)
        diff = model - market_price
        if abs(diff) < tolerance:
            return sigma
        d1, _ = d1_d2(F, K, T, sigma)
        vega_annual = F * disc * _norm_pdf(d1) * math.sqrt(T)
        if vega_annual < 1e-12:
            break  # flat region — hand off to bisection
        sigma -= diff / vega_annual
        if sigma <= iv_min or sigma >= iv_max:
            sigma = min(max(sigma, iv_min), iv_max)

    # Bisection fallback across the full band — robust where Newton stalls.
    lo, hi = iv_min, iv_max
    p_lo = price(F, K, T, lo, is_call, rate) - market_price
    p_hi = price(F, K, T, hi, is_call, rate) - market_price
    if p_lo * p_hi > 0:
        return None  # target not bracketed → no reliable solution
    for _ in range(max_iterations):
        mid = 0.5 * (lo + hi)
        p_mid = price(F, K, T, mid, is_call, rate) - market_price
        if abs(p_mid) < tolerance:
            return mid
        if p_lo * p_mid < 0:
            hi, p_hi = mid, p_mid
        else:
            lo, p_lo = mid, p_mid
    return 0.5 * (lo + hi)


def years_to_expiration(now: datetime, expiration: datetime, *, min_minutes: float = 0.0) -> float:
    """Exact time-to-expiry in years between two tz-aware datetimes.

    Callers that need settlement-instant resolution should compute
    ``expiration`` via the CME calendar first.
    """
    seconds = (expiration - now).total_seconds()
    if seconds <= 0:
        return 0.0
    years = seconds / 86_400.0 / DAY_COUNT
    floor = (min_minutes / 60.0 / 24.0) / DAY_COUNT
    return max(years, floor)
