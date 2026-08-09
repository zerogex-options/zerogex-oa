"""Pin Strike — the reachable 0DTE strike with the strongest modeled
positive gamma stabilization into expiration.

This is a deliberately DISTINCT metric from everything else in the
dealer-positioning family, and the difference is the whole point:

* **Call / Put Wall** (:mod:`src.analytics.walls`) — the strike above/below
  spot with the largest *current* dollar call/put gamma. Concentration of
  one-sided gamma, measured at today's spot.
* **Gamma Flip** (:meth:`AnalyticsEngine._resolve_gamma_flip`) — the
  hypothetical spot at which the *aggregate* dealer gamma profile changes
  sign.
* **Max Pain** — the strike that minimizes aggregate option-holder intrinsic
  payout at settlement.
* **King Node / max-GEX** (``max_gamma_strike``) — the strike with the
  largest *current* ``|net_gex|``.

Pin Strike answers a different question: *if price drifted to strike K,
would dealer hedging there be locally stabilizing (net-positive, restoring
gamma), and is K actually reachable before the 0DTE close?* It therefore
simulates the book **as though spot were at each candidate strike**,
re-prices dealer gamma at that hypothetical spot with the canonical
Black-Scholes kernel, keeps only the locally-concentrated **positive**
(restoring) gamma, and multiplies by a reachability weight derived from
ATM implied vol and the actual remaining time to expiration.

Crucially, Pin Strike does **not** simply pick the largest-GEX strike: a
huge far-OTM gamma node that price cannot plausibly reach into the close
scores near zero because its reachability weight collapses. When no
neighborhood has net-positive restoring gamma, the metric returns **no
active pin** (``pin_strike = None`` with a ``reason``) rather than forcing
a value.

Design notes
------------
* **No second GEX convention.** ``GEX_i(K)`` reuses the exact dealer-sign
  and dollar-gamma convention the core engine uses in
  :meth:`AnalyticsEngine._calculate_gex_by_strike` and
  :meth:`_gamma_exposure_profile` — calls signed ``+``, puts signed ``-``
  (dealers modeled long the calls customers overwrite and short the puts
  customers buy), position basis = open interest, and
  ``γ · OI · 100 · S² · 0.01`` dollar-gamma-per-1%-move. The only twist is
  that the hypothetical spot ``S`` is the candidate strike ``K`` (so the
  ``S²`` factor is ``K²``), because the whole point is to price the book as
  if spot sat at K.
* **Gamma is injected, not re-derived.** The orchestrator takes a
  ``gamma_fn`` callable; production passes
  :meth:`AnalyticsEngine._calculate_bs_gamma` (the same vectorised BSM
  gamma the gamma-flip profile uses), so there is exactly one Black-Scholes
  gamma implementation in the codebase. Unit tests can inject any gamma
  function to construct precise scenarios.
* **Pure and independently testable.** Nothing here touches the DB, the
  clock, or ``market_calendar``. The caller resolves each contract's
  settlement-aware time-to-expiration ``T`` and the representative
  reachability horizon ``tau`` (both in years) and passes them in, so the
  functions are deterministic given their inputs.

This keeps V1 simple while leaving the door open for the V2
hedge-restoring-force refinement (simulate dealer delta just above/below
each candidate): that would swap the local-restoring-gamma term for a
finite-difference of aggregate dealer delta, but the candidate selection,
reachability weighting, confidence, and null-handling scaffolding here are
unchanged.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

# --------------------------------------------------------------------------- #
# Status / reason codes.
#
# The analytics engine has no reason-string enum (its nullable-analytics
# convention is "return None + persist NULL, never fabricate" — see the
# gamma-flip ``*_unresolved`` handling). Pin Strike keeps that spirit — an
# inactive pin is ``pin_strike = None`` — but additionally records WHY, because
# the spec calls for observability to validate the model before relying on it
# publicly. The reason travels as a plain string constant (mirrored by a
# nullable ``pin_strike_reason`` column) rather than a heavyweight status
# object, so it stays additive and does not introduce a parallel convention.
# --------------------------------------------------------------------------- #
STATUS_ACTIVE = "active"
STATUS_UNAVAILABLE = "unavailable"

REASON_NO_0DTE_EXPIRATION = "NO_0DTE_EXPIRATION"
REASON_INSUFFICIENT_OPTION_DATA = "INSUFFICIENT_OPTION_DATA"
REASON_INSUFFICIENT_IV_DATA = "INSUFFICIENT_IV_DATA"
REASON_EXPIRED = "EXPIRED"
REASON_NO_POSITIVE_RESTORING_GAMMA = "NO_POSITIVE_RESTORING_GAMMA"
REASON_PIN_SCORE_TOO_WEAK = "PIN_SCORE_TOO_WEAK"

# Shares per standard option contract — the same multiplier the core GEX
# dollar-gamma formula uses (see src.greeks_fd.CONTRACT_MULTIPLIER and
# AnalyticsEngine._calculate_gex_by_strike). Re-declared here as a named
# constant rather than importing to keep this module dependency-light and
# self-contained; the value is a market fact, not a tunable.
CONTRACT_MULTIPLIER: float = 100.0

# Default tunables. The engine passes the ``PIN_STRIKE_*`` config values at the
# call site; these defaults exist so the pure functions are usable (and
# testable) standalone with sensible behavior.
DEFAULT_BANDWIDTH_STRIKE_MULT: float = 1.0
DEFAULT_CANDIDATE_MAX_Z: float = 2.5
DEFAULT_MIN_PIN_SCORE: float = 0.0  # 0 == magnitude gate disabled (no fabricated threshold)
DEFAULT_ATM_IV_BAND_PCT: float = 0.01


# --------------------------------------------------------------------------- #
# Result containers.
# --------------------------------------------------------------------------- #
@dataclass
class PinCandidate:
    """One candidate strike's fully-decomposed Pin Strike calculation.

    Retained for every scored candidate so tests and diagnostic logging can
    inspect exactly why a strike won or lost — ``strike``, ``local_gex``
    (signed), ``restoring_gex`` (``max(local_gex, 0)``), ``reachability``,
    and the product ``pin_score``.
    """

    strike: float
    local_gex: float
    restoring_gex: float
    reachability: float
    pin_score: float


@dataclass
class PinStrikeResult:
    """Outcome of a Pin Strike computation.

    Mirrors the conceptual return object in the spec. ``status`` is
    ``"active"`` iff ``pin_strike`` is not ``None``; ``reason`` carries a
    ``REASON_*`` code when inactive (and stays ``None`` when active). The
    per-candidate breakdown lives in ``candidates`` for diagnostics and is
    never persisted.
    """

    pin_strike: Optional[float] = None
    pin_score: Optional[float] = None
    pin_confidence: Optional[float] = None
    local_gex: Optional[float] = None
    restoring_gex: Optional[float] = None
    reachability: Optional[float] = None
    expiration: Optional[date] = None
    status: str = STATUS_UNAVAILABLE
    reason: Optional[str] = None
    # Diagnostics (not persisted): the inputs and per-candidate curve.
    sigma: Optional[float] = None
    tau: Optional[float] = None
    bandwidth: Optional[float] = None
    candidates: List[PinCandidate] = field(default_factory=list)

    @property
    def is_active(self) -> bool:
        return self.pin_strike is not None

    def summary_fields(self) -> Dict[str, Any]:
        """The additive, persist-ready subset of the result.

        These are exactly the keys the analytics summary dict / ``gex_summary``
        row carry (see :meth:`AnalyticsEngine._calculate_gex_summary`). The
        diagnostic fields (candidates, sigma, tau, bandwidth) are intentionally
        excluded — they belong in logs, not columns.
        """
        return {
            "pin_strike": self.pin_strike,
            "pin_score": self.pin_score,
            "pin_confidence": self.pin_confidence,
            "pin_strike_reason": self.reason,
        }


def _unavailable(reason: str, *, expiration: Optional[date] = None, **diag: Any) -> PinStrikeResult:
    """Build an inactive result carrying ``reason`` (and optional diagnostics)."""
    return PinStrikeResult(
        pin_strike=None,
        pin_score=None,
        pin_confidence=None,
        local_gex=None,
        restoring_gex=None,
        reachability=None,
        expiration=expiration,
        status=STATUS_UNAVAILABLE,
        reason=reason,
        sigma=diag.get("sigma"),
        tau=diag.get("tau"),
        bandwidth=diag.get("bandwidth"),
        candidates=diag.get("candidates", []),
    )


# --------------------------------------------------------------------------- #
# Building blocks (each independently unit-testable).
# --------------------------------------------------------------------------- #
def representative_atm_iv(
    contracts: Sequence[Mapping[str, Any]],
    spot: float,
    *,
    band_pct: float = DEFAULT_ATM_IV_BAND_PCT,
) -> Optional[float]:
    """Representative ATM implied volatility for the reachability weight.

    Reuses the engine's established ATM-IV policy (see
    :meth:`AnalyticsEngine._store_daily_atm_iv`): the mean implied vol of
    contracts whose strike sits within ``±band_pct`` of spot, **calls
    preferred** (the ATM-call-IV anchor the ``daily_atm_iv`` table stores).
    Falls back to puts, then to any option type, only if no ATM call carries a
    usable IV, so a thin call side near the close still yields a value.

    Returns ``None`` when no ATM contract has a positive IV — the honest
    "insufficient IV data" signal. It never substitutes an arbitrary default
    vol; the engine's own IV pipeline explicitly abandoned the 0.20 sentinel to
    avoid fabricating exposure, and Pin Strike follows that policy.

    :param contracts: normalized contracts with ``strike``, ``option_type`` and
        ``iv`` keys (``iv`` may be ``None`` / non-positive → ignored).
    :param spot: current underlying price (``<= 0`` → ``None``).
    :param band_pct: half-width of the ATM band as a fraction of spot.
    """
    if spot is None or spot <= 0 or band_pct <= 0:
        return None
    low = spot * (1.0 - band_pct)
    high = spot * (1.0 + band_pct)

    calls: List[float] = []
    puts: List[float] = []
    for c in contracts:
        iv = c.get("iv")
        strike = c.get("strike")
        if iv is None or strike is None:
            continue
        try:
            iv_f = float(iv)
            strike_f = float(strike)
        except (TypeError, ValueError):
            continue
        if iv_f <= 0 or not (low <= strike_f <= high):
            continue
        if str(c.get("option_type")).upper() == "C":
            calls.append(iv_f)
        else:
            puts.append(iv_f)

    if calls:
        return sum(calls) / len(calls)
    if puts:
        return sum(puts) / len(puts)
    return None


def estimate_strike_bandwidth(
    strikes: Sequence[float],
    *,
    mult: float = DEFAULT_BANDWIDTH_STRIKE_MULT,
    fallback_increment: Optional[float] = None,
) -> Optional[float]:
    """Gaussian-kernel bandwidth derived from nearby strike spacing.

    The kernel that concentrates gamma around a candidate strike must scale
    with the product's strike grid — SPY lists ~$1 strikes, SPX ~5-point,
    NDX coarser still — so a globally-hardcoded bandwidth would be wrong for
    most underlyings. This returns ``mult × (median consecutive strike
    interval)`` over the supplied listed strikes, which adapts automatically.

    :param strikes: listed strikes (any order; deduplicated internally).
    :param mult: bandwidth as a multiple of the median interval
        (``PIN_STRIKE_BANDWIDTH_STRIKE_MULT``; ~1–2 strike intervals).
    :param fallback_increment: strike increment to use when fewer than two
        distinct strikes are available to measure spacing (e.g.
        ``default_strike_increment(symbol)``). ``None`` → returns ``None``.
    :returns: a positive bandwidth, or ``None`` when spacing cannot be
        established and no fallback is given.
    """
    if mult <= 0:
        return None
    uniq = sorted({float(s) for s in strikes if s is not None and float(s) > 0})
    if len(uniq) >= 2:
        intervals = [b - a for a, b in zip(uniq, uniq[1:]) if b > a]
        if intervals:
            median_interval = float(statistics.median(intervals))
            if median_interval > 0:
                return mult * median_interval
    if fallback_increment is not None and fallback_increment > 0:
        return mult * float(fallback_increment)
    return None


def calculate_pin_reachability(
    strike: float,
    *,
    spot: float,
    sigma: float,
    tau: float,
) -> float:
    """Probability-like weight that spot can reach/finish near ``strike``.

    ``z = ln(K / spot) / (sigma · √tau)`` is the log-moneyness of the
    candidate expressed in standard deviations of the terminal
    (lognormal) price distribution; ``reachability = exp(-½ z²)`` is the
    unnormalized Gaussian density at that distance. It peaks at 1.0 for a
    strike at spot and decays as the strike moves away faster than vol × √time
    can plausibly carry price. Because the distance is in vol·√time units,
    the same formula works across SPY / QQQ / SPX / NDX with no per-symbol
    dollar constants.

    Returns ``0.0`` for any degenerate input (non-positive spot / strike /
    sigma / tau), so an expired or IV-less book cannot manufacture a pin.

    :param tau: remaining time to expiration **in years**. For 0DTE this must
        be the actual intraday remainder (seconds-to-close / seconds-per-year),
        not ``1/365`` — the caller supplies it via the engine's
        ``calculate_time_to_expiration``.
    """
    if spot is None or spot <= 0 or strike is None or strike <= 0:
        return 0.0
    if sigma is None or sigma <= 0 or tau is None or tau <= 0:
        return 0.0
    denom = sigma * math.sqrt(tau)
    if denom <= 0:
        return 0.0
    z = math.log(strike / spot) / denom
    return math.exp(-0.5 * z * z)


def calculate_local_restoring_gex(
    candidate_strike: float,
    contracts: Sequence[Mapping[str, Any]],
    *,
    gamma_fn: Callable[..., float],
    bandwidth: float,
    risk_free_rate: float,
    dividend_yield: float = 0.0,
) -> Tuple[float, float]:
    """Local (Gaussian-weighted) dealer GEX if spot were at ``candidate_strike``.

    Simulates ``spot == K`` and, for every contract ``i``:

    * re-prices dealer gamma at ``S = K`` via the injected ``gamma_fn``
      (production: the canonical BSM gamma), using the contract's own IV and
      time-to-expiration ``T``;
    * signs and scales it to dollar GEX-per-1%-move with the **exact** core
      convention — ``dealer_sign · γ · OI · 100 · K² · 0.01`` (calls ``+``,
      puts ``-``; ``S = K`` so the ``S²`` factor is ``K²``);
    * weights it by a Gaussian kernel in strike space,
      ``exp(-(strike_i - K)² / (2 · bandwidth²))``, so only gamma
      *concentrated around K* contributes — a pin is a local feature, not the
      whole chain's gamma.

    :returns: ``(local_gex, restoring_gex)`` where ``restoring_gex =
        max(local_gex, 0)``. A neighborhood dominated by dealer-short (put)
        gamma has ``local_gex < 0`` → ``restoring_gex = 0`` → cannot pin,
        which is exactly the intended "only positive restoring gamma pins"
        behavior.
    """
    K = float(candidate_strike)
    if K <= 0 or bandwidth is None or bandwidth <= 0:
        return 0.0, 0.0

    two_bw_sq = 2.0 * bandwidth * bandwidth
    dollar_scale = CONTRACT_MULTIPLIER * K * K * 0.01

    local_gex = 0.0
    for c in contracts:
        try:
            oi = float(c.get("open_interest") or 0.0)
            iv = c.get("iv")
            iv_f = float(iv) if iv is not None else 0.0
            T = float(c.get("T") or 0.0)
            strike_i = float(c.get("strike") or 0.0)
        except (TypeError, ValueError):
            continue
        if oi <= 0 or iv_f <= 0 or T <= 0 or strike_i <= 0:
            continue

        gamma = gamma_fn(K, strike_i, T, risk_free_rate, iv_f, dividend_yield)
        try:
            gamma = float(gamma)
        except (TypeError, ValueError):
            continue
        if gamma <= 0 or not math.isfinite(gamma):
            continue

        dealer_sign = 1.0 if str(c.get("option_type")).upper() == "C" else -1.0
        gex_i = dealer_sign * gamma * oi * dollar_scale

        dist = strike_i - K
        kernel = math.exp(-(dist * dist) / two_bw_sq)
        local_gex += gex_i * kernel

    return local_gex, max(local_gex, 0.0)


def select_candidate_strikes(
    strikes: Sequence[float],
    *,
    spot: float,
    sigma: float,
    tau: float,
    max_z: float = DEFAULT_CANDIDATE_MAX_Z,
) -> List[float]:
    """Listed strikes realistically reachable before expiration.

    Restricts the (potentially huge) chain to strikes within ``±max_z``
    expected moves of spot — ``|ln(K/spot)| ≤ max_z · sigma · √tau`` — which is
    the log-moneyness form of "within ~N expected moves" and equivalently
    ``reachability(K) ≥ exp(-½ max_z²)``. This is scale-free (works for any
    underlying) and keeps the per-cycle simulation cheap.

    Only **actual listed strikes** (the distinct strikes present in the book)
    are returned, so the eventual Pin Strike is always a real, quotable
    contract. When the reachable band is degenerate (missing sigma/tau) or
    would be empty, the single nearest listed strike to spot is returned so a
    valid book always has at least one candidate and the null decision rests on
    gamma, not on an empty candidate set.
    """
    uniq = sorted({float(s) for s in strikes if s is not None and float(s) > 0})
    if not uniq or spot is None or spot <= 0:
        return []

    if sigma and sigma > 0 and tau and tau > 0 and max_z > 0:
        max_abs_z = max_z * sigma * math.sqrt(tau)
        in_band = [k for k in uniq if abs(math.log(k / spot)) <= max_abs_z]
        if in_band:
            return in_band

    # Degenerate band (or nothing in range): fall back to the nearest strike.
    nearest = min(uniq, key=lambda k: abs(k - spot))
    return [nearest]


# --------------------------------------------------------------------------- #
# Orchestrator.
# --------------------------------------------------------------------------- #
def calculate_pin_strike(
    contracts: Sequence[Mapping[str, Any]],
    *,
    spot: float,
    tau: float,
    sigma: Optional[float],
    gamma_fn: Callable[..., float],
    risk_free_rate: float,
    dividend_yield: float = 0.0,
    expiration: Optional[date] = None,
    has_0dte: bool = True,
    bandwidth: Optional[float] = None,
    bandwidth_strike_mult: float = DEFAULT_BANDWIDTH_STRIKE_MULT,
    fallback_increment: Optional[float] = None,
    candidate_max_z: float = DEFAULT_CANDIDATE_MAX_Z,
    min_pin_score: float = DEFAULT_MIN_PIN_SCORE,
) -> PinStrikeResult:
    """Compute the Pin Strike from a single-expiration (0DTE) contract set.

    Pipeline: derive the kernel bandwidth from strike spacing → restrict to
    reachable listed strikes → for each candidate simulate ``spot = K``,
    compute local restoring GEX and reachability, and score
    ``pin_score(K) = restoring_gex(K) · reachability(K)`` → pick the max, guard
    for meaningfulness, and compute a normalized confidence.

    The function is total: every early-out returns a :class:`PinStrikeResult`
    with ``pin_strike = None`` and a ``REASON_*`` code (never raises on bad
    data), so the caller can persist NULL + reason uniformly.

    :param contracts: normalized 0DTE contracts, each a mapping with keys
        ``strike``, ``option_type`` (``"C"``/``"P"``), ``open_interest``,
        ``iv``, and ``T`` (settlement-aware time-to-expiration in years — the
        caller resolves it so this stays clock-free).
    :param spot: current underlying price.
    :param tau: representative remaining time to the 0DTE expiration, in years
        (intraday-accurate for 0DTE). Drives reachability.
    :param sigma: representative ATM IV (see :func:`representative_atm_iv`);
        ``None``/non-positive → ``INSUFFICIENT_IV_DATA``.
    :param gamma_fn: ``(S, K, T, r, sigma, q) -> gamma`` — production passes
        ``AnalyticsEngine._calculate_bs_gamma`` so there is one BSM gamma.
    :param has_0dte: pass ``False`` when the caller found no same-day
        expiration at all → ``NO_0DTE_EXPIRATION`` (independent of whether a
        stray contract list was supplied).
    :param bandwidth: explicit kernel bandwidth; when ``None`` it is derived
        from the candidate strikes via :func:`estimate_strike_bandwidth`.
    :param min_pin_score: raw-magnitude floor below which the best pin is
        suppressed as ``PIN_SCORE_TOO_WEAK`` (``0`` disables the gate — the
        default, so no arbitrary user-facing threshold is invented).
    """
    if not has_0dte or expiration is None:
        return _unavailable(REASON_NO_0DTE_EXPIRATION, expiration=expiration)

    if spot is None or spot <= 0:
        return _unavailable(REASON_INSUFFICIENT_OPTION_DATA, expiration=expiration)

    # Keep only structurally-valid contracts (positive OI / IV / T / strike).
    valid: List[Dict[str, Any]] = []
    for c in contracts:
        try:
            oi = float(c.get("open_interest") or 0.0)
            iv = c.get("iv")
            iv_f = float(iv) if iv is not None else 0.0
            T = float(c.get("T") or 0.0)
            strike_i = float(c.get("strike") or 0.0)
        except (TypeError, ValueError):
            continue
        if oi <= 0 or iv_f <= 0 or T <= 0 or strike_i <= 0:
            continue
        valid.append(
            {
                "strike": strike_i,
                "option_type": "C" if str(c.get("option_type")).upper() == "C" else "P",
                "open_interest": oi,
                "iv": iv_f,
                "T": T,
            }
        )

    if not valid:
        return _unavailable(REASON_INSUFFICIENT_OPTION_DATA, expiration=expiration)

    # Past the settlement instant reachability is undefined — treat as expired
    # rather than silently scoring every candidate at zero.
    if tau is None or tau <= 0:
        return _unavailable(REASON_EXPIRED, expiration=expiration, tau=tau)

    if sigma is None or sigma <= 0:
        return _unavailable(REASON_INSUFFICIENT_IV_DATA, expiration=expiration, tau=tau)

    strikes = [c["strike"] for c in valid]
    candidate_strikes = select_candidate_strikes(
        strikes, spot=spot, sigma=sigma, tau=tau, max_z=candidate_max_z
    )
    if not candidate_strikes:
        return _unavailable(
            REASON_INSUFFICIENT_OPTION_DATA, expiration=expiration, sigma=sigma, tau=tau
        )

    if bandwidth is None:
        # Prefer the NEARBY (near-money candidate) strike spacing so a chain
        # with dense near-money strikes but sparse far wings (SPX: 5-wide near
        # money, 25-wide wings) doesn't inflate the kernel width. Degrade to the
        # full-book spacing, then to the product's listed increment.
        bandwidth = estimate_strike_bandwidth(candidate_strikes, mult=bandwidth_strike_mult)
        if bandwidth is None:
            bandwidth = estimate_strike_bandwidth(
                strikes, mult=bandwidth_strike_mult, fallback_increment=fallback_increment
            )
    if bandwidth is None or bandwidth <= 0:
        # No way to establish strike spacing (single strike, no fallback grid).
        return _unavailable(
            REASON_INSUFFICIENT_OPTION_DATA,
            expiration=expiration,
            sigma=sigma,
            tau=tau,
        )

    candidates: List[PinCandidate] = []
    for K in candidate_strikes:
        local_gex, restoring_gex = calculate_local_restoring_gex(
            K,
            valid,
            gamma_fn=gamma_fn,
            bandwidth=bandwidth,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
        )
        reach = calculate_pin_reachability(K, spot=spot, sigma=sigma, tau=tau)
        pin_score = restoring_gex * reach
        candidates.append(
            PinCandidate(
                strike=K,
                local_gex=local_gex,
                restoring_gex=restoring_gex,
                reachability=reach,
                pin_score=pin_score,
            )
        )

    positive = [c for c in candidates if c.pin_score > 0.0]
    if not positive:
        return _unavailable(
            REASON_NO_POSITIVE_RESTORING_GAMMA,
            expiration=expiration,
            sigma=sigma,
            tau=tau,
            bandwidth=bandwidth,
            candidates=candidates,
        )

    # Winner: max pin_score; ties broken toward the strike nearest spot (then
    # the lower strike), matching the walls' nearest-to-spot convention.
    best = max(positive, key=lambda c: (c.pin_score, -abs(c.strike - spot), -c.strike))

    if min_pin_score > 0.0 and best.pin_score < min_pin_score:
        return _unavailable(
            REASON_PIN_SCORE_TOO_WEAK,
            expiration=expiration,
            sigma=sigma,
            tau=tau,
            bandwidth=bandwidth,
            candidates=candidates,
        )

    # Confidence = dominance of the best pin over all viable pins. Guarded
    # against a zero denominator (can't actually be zero here since ``best`` is
    # positive, but the guard keeps the invariant explicit).
    total_positive = sum(c.pin_score for c in positive)
    pin_confidence = (best.pin_score / total_positive) if total_positive > 0 else None

    return PinStrikeResult(
        pin_strike=best.strike,
        pin_score=best.pin_score,
        pin_confidence=pin_confidence,
        local_gex=best.local_gex,
        restoring_gex=best.restoring_gex,
        reachability=best.reachability,
        expiration=expiration,
        status=STATUS_ACTIVE,
        reason=None,
        sigma=sigma,
        tau=tau,
        bandwidth=bandwidth,
        candidates=candidates,
    )
