"""Forced Flow engine -- the dollars of stock dealers are mechanically compelled
to buy or sell under a scenario of spot move, elapsed time, and IV shift.

This is the architectural core of the feature. Everything the product shows --
the Forced Flow Curve, Charm-into-Close, the Vanna Ladder, the surface, and the
charm / vanna / zero-flow levels -- is :func:`dealer_hedge_flow` evaluated over a
grid. Build the primitive once; the charts fall out of it.

How it is computed (spec 6.2): by FULL REPRICE, not a first-order Taylor
expansion. We compute the aggregate dealer delta of the whole book now, and
again under the scenario (S', T', sigma'), and the hedge is minus that change
times the scenario spot::

    hedge_flow = -(dealer_delta(S', T', sigma') - dealer_delta(S, T, sigma)) * S'

This is exact -- it captures gamma-of-gamma and every cross term for free -- and
needs only delta. Charm and vanna (from ``src.greeks_fd``) are used only for the
first-order ATTRIBUTION returned alongside the exact total, so the UI can say how
much of the flow is spot vs. time vs. vol.

Dealer sign convention (model A -- the GEX convention, long calls / short puts):
dealer delta = sum over contracts of ``sign * delta * OI * 100`` with
``sign = +1`` for calls and ``-1`` for puts. This is the ONLY convention
consistent with the shipped gamma flip (and the section-2 short-gamma squeeze):
below the flip dealers are short gamma, a spot rise forces buying into strength.
NOTE: the existing ``dealer_charm_exposure`` / ``dealer_vanna_exposure`` signal
columns use a different, inconsistent convention (dealer short the whole book);
they are intentionally left untouched here and flagged for a separate,
backtested migration.

Percent inputs are FRACTIONS, matching the codebase's ``span_pct`` convention
(0.05 == a 5% spot move), not whole percents.

Phase 6 seam (spec 10): the value this engine produces is the OI-PREDICTED
re-hedge. A second, OBSERVED source (signed delta flow from a tape we do not yet
have) slots in additively -- delta is linear, so the two sources sum. See
:func:`combine_flow_sources`; today the observed term is zero.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple

from src.greeks_fd import CONTRACT_MULTIPLIER, VANNA_H_VOL, bsm_delta, fd_charm, fd_vanna

# Smallest volatility we will price at, so a large downward vol shock cannot
# drive sigma to zero (delta is undefined there).
_MIN_SIGMA = 1e-4
# Spot bump (as a fraction of spot) for the attribution gamma finite difference.
_GAMMA_BUMP_FRAC = 1e-4


@dataclass(frozen=True)
class ContractLeg:
    """One option line in the book, normalized for the reprice.

    ``iv`` is sigma per unit (0.20 == 20 vol). ``tte_years`` is the CURRENT
    time to expiry in years (the caller computes it with the same
    settlement-anchored day-count the rest of the engine uses, so this module
    inherits those conventions). Legs with non-positive ``iv`` or ``tte_years``
    should be filtered out by the caller (they carry no reliable exposure).
    """

    strike: float
    option_type: str  # "C" | "P"
    open_interest: float
    iv: float
    tte_years: float


@dataclass
class ForcedFlow:
    """Result of one scenario.

    ``total_usd`` is exact (from the full reprice); positive means dealers must
    BUY stock, negative means SELL. The three components are the first-order
    attribution (Gamma*dS, Charm*dt, Vanna*dSigma); ``residual`` is
    ``total - sum(components)`` and grows when the scenario is far enough out
    that linear intuition fails -- a useful warning, worth surfacing on extreme
    scenarios.
    """

    total_usd: float
    gamma_component: float
    charm_component: float
    vanna_component: float
    residual: float


def build_legs(rows: Sequence[dict], tte_of: Callable[[dict], float]) -> List[ContractLeg]:
    """Normalize a snapshot's option rows into :class:`ContractLeg` list.

    ``tte_of(opt)`` returns the contract's current time-to-expiry in years (the
    caller supplies the engine's settlement-anchored day-count). Rows with a
    missing / non-positive IV or open interest, or a non-positive tte, are
    skipped -- they carry no reliable exposure, matching the null-IV skip the
    gamma/vanna/charm exposure loop already applies.
    """
    legs: List[ContractLeg] = []
    for opt in rows:
        iv = opt.get("implied_volatility")
        oi = opt.get("open_interest") or 0
        if iv is None or iv <= 0 or oi <= 0:
            continue
        tte = tte_of(opt)
        if tte <= 0:
            continue
        legs.append(
            ContractLeg(
                strike=float(opt["strike"]),
                option_type=opt["option_type"],
                open_interest=float(oi),
                iv=float(iv),
                tte_years=float(tte),
            )
        )
    return legs


def _scenario_delta(
    S: float, K: float, T: float, r: float, sigma: float, option_type: str, q: float
) -> float:
    """Delta under a scenario, resolving to intrinsic once the contract expires.

    Within a scenario horizon a 0DTE contract can reach T <= 0 (e.g. Charm-into-
    Close evaluated at the bell). ``bsm_delta`` returns 0.0 there; instead we use
    the intrinsic delta of the settled contract (a call is +1 in-the-money else
    0; a put is -1 in-the-money else 0), which is the hedge a resolved position
    actually requires.
    """
    if T <= 0:
        if option_type == "C":
            return 1.0 if S > K else 0.0
        return -1.0 if S < K else 0.0
    return bsm_delta(S, K, T, r, sigma, option_type, q)


def _aggregate_dealer_delta(
    legs: Sequence[ContractLeg],
    spot: float,
    days_elapsed: float,
    vol_change_pts: float,
    r: float,
    q: float,
) -> float:
    """Model-A aggregate dealer delta (shares) under (spot, +days, +vol points).

    ``sign = +1`` for calls, ``-1`` for puts -- the GEX convention, so the
    spot-derivative of this equals the shipped dealer gamma.
    """
    vol_shift = vol_change_pts * VANNA_H_VOL  # vol points -> absolute sigma
    total = 0.0
    for leg in legs:
        sigma = leg.iv + vol_shift
        if sigma < _MIN_SIGMA:
            sigma = _MIN_SIGMA
        tte = leg.tte_years - days_elapsed / 365.0
        sign = 1.0 if leg.option_type == "C" else -1.0
        delta = _scenario_delta(spot, leg.strike, tte, r, sigma, leg.option_type, q)
        total += sign * delta * leg.open_interest * CONTRACT_MULTIPLIER
    return total


def _dealer_greek_shares(
    legs: Sequence[ContractLeg],
    spot: float,
    greek: str,
    r: float,
    q: float,
) -> float:
    """First-order model-A dealer greek in *shares per unit driver*, at the
    current state -- used only for attribution.

    ``greek`` is one of: 'gamma' (per $1 of spot), 'charm' (per calendar day),
    'vanna' (per 1 vol point).
    """
    total = 0.0
    bump = spot * _GAMMA_BUMP_FRAC
    for leg in legs:
        sign = 1.0 if leg.option_type == "C" else -1.0
        if greek == "gamma":
            up = bsm_delta(spot + bump, leg.strike, leg.tte_years, r, leg.iv, leg.option_type, q)
            dn = bsm_delta(spot - bump, leg.strike, leg.tte_years, r, leg.iv, leg.option_type, q)
            g = (up - dn) / (2.0 * bump)
        elif greek == "charm":
            g = fd_charm(spot, leg.strike, leg.tte_years, r, leg.iv, leg.option_type, q)
        elif greek == "vanna":
            # fd_vanna is per unit sigma; scale to per one vol point.
            g = (
                fd_vanna(spot, leg.strike, leg.tte_years, r, leg.iv, leg.option_type, q)
                * VANNA_H_VOL
            )
        else:  # pragma: no cover - guarded by callers
            raise ValueError(f"unknown greek {greek!r}")
        total += sign * g * leg.open_interest * CONTRACT_MULTIPLIER
    return total


def flow_total(
    legs: Sequence[ContractLeg],
    spot: float,
    spot_move_pct: float,
    days_elapsed: float,
    vol_change_pts: float,
    r: float,
    q: float = 0.0,
) -> float:
    """Exact dealer hedge flow (USD) by full reprice, WITHOUT the attribution.

    The fast path for grid work (curves, flip levels) that needs only the total.
    :func:`dealer_hedge_flow` wraps this and adds the first-order breakdown.
    """
    spot_scn = spot * (1.0 + spot_move_pct)
    dd_now = _aggregate_dealer_delta(legs, spot, 0.0, 0.0, r, q)
    dd_scn = _aggregate_dealer_delta(legs, spot_scn, days_elapsed, vol_change_pts, r, q)
    return -(dd_scn - dd_now) * spot_scn


def dealer_hedge_flow(
    legs: Sequence[ContractLeg],
    spot: float,
    spot_move_pct: float,
    days_elapsed: float,
    vol_change_pts: float,
    r: float,
    q: float = 0.0,
) -> ForcedFlow:
    """Dollars of STOCK dealers must BUY (+) or SELL (-) to stay delta-flat.

    Args:
        legs: the option book (see :class:`ContractLeg`).
        spot: current underlying price.
        spot_move_pct: scenario spot move as a FRACTION (0.05 == +5%).
        days_elapsed: scenario calendar days that pass.
        vol_change_pts: scenario IV shift in vol points (+1 == +0.01 sigma).
        r: risk-free rate. q: dividend yield.

    Returns the exact total (full reprice) plus first-order attribution.
    """
    spot_scn = spot * (1.0 + spot_move_pct)
    total = flow_total(legs, spot, spot_move_pct, days_elapsed, vol_change_pts, r, q)

    # First-order attribution around the current state. Each term is the change
    # in dealer delta along one axis, dollarized with the scenario spot so the
    # three components and the exact total share one price basis.
    d_spot = spot * spot_move_pct
    gamma_shares = _dealer_greek_shares(legs, spot, "gamma", r, q)
    charm_shares = _dealer_greek_shares(legs, spot, "charm", r, q)
    vanna_shares = _dealer_greek_shares(legs, spot, "vanna", r, q)

    gamma_component = -gamma_shares * d_spot * spot_scn
    charm_component = -charm_shares * days_elapsed * spot_scn
    vanna_component = -vanna_shares * vol_change_pts * spot_scn
    residual = total - (gamma_component + charm_component + vanna_component)

    return ForcedFlow(
        total_usd=total,
        gamma_component=gamma_component,
        charm_component=charm_component,
        vanna_component=vanna_component,
        residual=residual,
    )


# --------------------------------------------------------------------------- #
# Grid helpers -- the charts. All are dealer_hedge_flow over a grid.
# --------------------------------------------------------------------------- #
def spot_grid(spot: float, span_pct: float, step_pct: float) -> List[float]:
    """Ascending spot grid over +/- ``span_pct`` in ``step_pct`` increments."""
    if spot <= 0 or step_pct <= 0 or span_pct <= 0:
        return [spot]
    n = int(round(span_pct / step_pct))
    grid = [spot * (1.0 + i * step_pct) for i in range(-n, n + 1)]
    return [s for s in grid if s > 0]


def forced_flow_curve(
    legs: Sequence[ContractLeg],
    spot: float,
    days_elapsed: float,
    vol_change_pts: float,
    r: float,
    q: float = 0.0,
    span_pct: float = 0.05,
    step_pct: float = 0.0025,
) -> List[Tuple[float, ForcedFlow]]:
    """The Forced Flow Curve: flow required to end the horizon at each spot level.

    Each point is the flow to move from ``spot`` to the grid level over
    ``days_elapsed`` with ``vol_change_pts``. The zero of ``total_usd`` is the
    zero-flow level.
    """
    out: List[Tuple[float, ForcedFlow]] = []
    for level in spot_grid(spot, span_pct, step_pct):
        move = level / spot - 1.0
        out.append((level, dealer_hedge_flow(legs, spot, move, days_elapsed, vol_change_pts, r, q)))
    return out


def charm_into_close(
    legs: Sequence[ContractLeg],
    spot: float,
    session_days: float,
    r: float,
    q: float = 0.0,
    steps: int = 24,
) -> List[Tuple[float, float]]:
    """Charm-into-Close: cumulative time-forced flow with spot held fixed.

    Returns ``[(days_elapsed, total_usd), ...]`` from 0 to ``session_days``. The
    curve steepens into the close -- that is the whole story.
    """
    out: List[Tuple[float, float]] = []
    for i in range(steps + 1):
        dt = session_days * i / steps
        out.append((dt, flow_total(legs, spot, 0.0, dt, 0.0, r, q)))
    return out


def vanna_ladder(
    legs: Sequence[ContractLeg],
    spot: float,
    r: float,
    q: float = 0.0,
    lo_pts: float = -3.0,
    hi_pts: float = 3.0,
    step_pts: float = 0.5,
) -> List[Tuple[float, float]]:
    """Vanna Ladder: flow vs. IV change (vol points), spot and time held fixed."""
    out: List[Tuple[float, float]] = []
    n = int(round((hi_pts - lo_pts) / step_pts))
    for i in range(n + 1):
        dv = lo_pts + i * step_pts
        out.append((dv, flow_total(legs, spot, 0.0, 0.0, dv, r, q)))
    return out


# --------------------------------------------------------------------------- #
# Derived levels (spec 6.4). Gamma flip already exists elsewhere; not recomputed.
# --------------------------------------------------------------------------- #
def _zero_crossings(points: Sequence[Tuple[float, float]]) -> List[float]:
    """X values where Y crosses zero, by linear interpolation between samples."""
    out: List[float] = []
    for (x1, y1), (x2, y2) in zip(points, points[1:]):
        if y1 == 0.0:
            out.append(x1)
        elif y1 * y2 < 0.0:
            out.append(x1 + (x2 - x1) * (-y1) / (y2 - y1))
    return out


def _nearest_crossing(points: Sequence[Tuple[float, float]], ref: float) -> Optional[float]:
    xs = _zero_crossings(points)
    if not xs:
        return None
    return min(xs, key=lambda x: abs(x - ref))


def _level_from_spot_axis(
    legs: Sequence[ContractLeg],
    spot: float,
    flow_at: Callable[[float], float],
    span_pct: float,
    step_pct: float,
) -> Optional[float]:
    points = [(level, flow_at(level)) for level in spot_grid(spot, span_pct, step_pct)]
    return _nearest_crossing(points, spot)


def charm_flip(
    legs: Sequence[ContractLeg],
    spot: float,
    session_days: float,
    r: float,
    q: float = 0.0,
    span_pct: float = 0.05,
    step_pct: float = 0.0025,
) -> Optional[float]:
    """Spot at which time-decay-driven hedging changes sign."""
    return _level_from_spot_axis(
        legs,
        spot,
        lambda level: flow_total(legs, level, 0.0, session_days, 0.0, r, q),
        span_pct,
        step_pct,
    )


def vanna_flip(
    legs: Sequence[ContractLeg],
    spot: float,
    r: float,
    q: float = 0.0,
    probe_pts: float = 1.0,
    span_pct: float = 0.05,
    step_pct: float = 0.0025,
) -> Optional[float]:
    """Spot at which vol-driven hedging changes sign (probed with +1 vol point)."""
    return _level_from_spot_axis(
        legs,
        spot,
        lambda level: flow_total(legs, level, 0.0, 0.0, probe_pts, r, q),
        span_pct,
        step_pct,
    )


def zero_flow_level(
    legs: Sequence[ContractLeg],
    spot: float,
    session_days: float,
    r: float,
    q: float = 0.0,
    vol_change_pts: float = 0.0,
    span_pct: float = 0.05,
    step_pct: float = 0.0025,
) -> Optional[float]:
    """Spot where TOTAL forced flow over the session is zero.

    The single most useful number in the product: the price at which dealers
    have nothing to do. It is the zero of the Forced Flow Curve.
    """
    points = [
        (level, flow_total(legs, spot, level / spot - 1.0, session_days, vol_change_pts, r, q))
        for level in spot_grid(spot, span_pct, step_pct)
    ]
    return _nearest_crossing(points, spot)


# --------------------------------------------------------------------------- #
# Phase 6 seam (spec 10) -- leave it, build nothing.
# --------------------------------------------------------------------------- #
def combine_flow_sources(predicted: ForcedFlow, observed_signed_flow_usd: float = 0.0) -> float:
    """Total dealer stock demand = OI-predicted re-hedge + observed new-trade hedge.

    Additive by construction: delta is linear, so the OI-predicted re-hedge (this
    engine, :attr:`ForcedFlow.total_usd`) and the observed signed-flow hedge
    (Phase 6, from a tape we do not yet ingest) sum with no reconciliation. Today
    the observed term is 0; the seam exists so Phase 6 is purely additive.
    """
    return predicted.total_usd + observed_signed_flow_usd


# --------------------------------------------------------------------------- #
# Track record -- does the morning charm-into-close sign predict the afternoon?
# --------------------------------------------------------------------------- #
# 95% two-sided normal quantile -- the multiplier for the Wilson interval and
# the threshold the significance verdict is read against.
_Z_95 = 1.959963984540054
# Below this many decisive sessions we refuse to call an edge "significant"
# regardless of p-value -- a normal approximation on a handful of days lies.
_MIN_SIGNIFICANT_N = 30


def _sign(x: float) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)


def _t_stat(values: Sequence[float]) -> Optional[float]:
    """One-sample t-statistic of ``values`` against a mean of zero.

    Applied to the per-session signal returns: how many standard errors the
    average P&L sits from break-even. |t| >~ 2 is the usual "distinguishable
    from zero" bar. Returns None below two samples or on zero dispersion.
    """
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / (n - 1)
    if var <= 0.0:
        return None
    return mean / math.sqrt(var / n)


def _normal_cdf(x: float) -> float:
    """Standard-normal CDF via the error function -- no scipy dependency."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _wilson_interval(hits: int, n: int, z: float = _Z_95) -> Tuple[float, float]:
    """Wilson score interval for a binomial proportion.

    Preferred over the normal (Wald) interval because it stays inside [0, 1]
    and behaves at small n and extreme rates -- exactly the regime a young
    track record lives in. Returns (low, high); (0.0, 1.0) when n == 0.
    """
    if n <= 0:
        return (0.0, 1.0)
    p = hits / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denom
    half = (z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))) / denom
    return (max(0.0, center - half), min(1.0, center + half))


def _accuracy_beats_baseline_p(hits: int, n: int, baseline: float) -> Optional[float]:
    """One-sided p-value that accuracy exceeds the no-information rate.

    The honest question for a directional classifier is not "beats 50%?" but
    "beats always guessing the more common outcome?" -- the no-information rate.
    This is the caret ``confusionMatrix`` "Accuracy > NIR" test, normal-approx:
    p = P(observed accuracy this high | true accuracy == baseline). Small p ->
    the edge over the naive rule is unlikely to be luck. Returns None if it
    cannot be formed (n == 0 or a degenerate baseline of 0/1).
    """
    if n <= 0 or baseline is None:
        return None
    if baseline >= 1.0:
        # Nothing can beat a perfect naive rule -- no edge is possible.
        return 1.0
    if baseline <= 0.0:
        return None
    se = math.sqrt(baseline * (1.0 - baseline) / n)
    if se == 0.0:
        return None
    z = (hits / n - baseline) / se
    return 1.0 - _normal_cdf(z)


def charm_backtest_summary(sessions: Sequence[dict], recent_limit: int = 90) -> dict:
    """Honest hit-rate of the Charm-into-Close forecast.

    The single testable claim behind the headline "time decay alone forces
    dealers to buy/sell $X by 4pm": if the morning ``close_charm_flow`` sign
    (dealers must BUY -> ``+``, must SELL -> ``-``) has any edge, it should lean
    the same way as the actual noon->close return. This aggregates one row per
    session -- each a dict with ``session_date``, ``charm_flow`` (the morning
    read), ``noon_px`` and ``close_px`` -- into a hit rate.

    It is deliberately unflattering by construction:

    * ``baseline_rate`` is the naive "always guess the more common direction"
      accuracy. A hit rate at or below it is worth nothing, and the frontend is
      handed both numbers side by side so a coin flip cannot read as signal.
    * ``signal_mean_return`` is the mean of ``predicted_dir * noon->close
      return`` -- the average P&L of taking the charm sign at noon and closing
      at the bell, before costs. It can be negative. That is the point.
    * ``hit_rate_ci_low`` / ``hit_rate_ci_high`` bound the hit rate with a 95%
      Wilson interval; ``edge_p_value`` is the one-sided probability the
      accuracy beats the naive baseline by luck; ``signal_t_stat`` tests the
      per-session P&L against zero; ``significant`` is True only when the edge
      clears 95% on a sample of at least ``_MIN_SIGNIFICANT_N`` sessions. A
      thin or lucky record cannot pass for a real one.

    Sessions with a flat charm read or an exactly-flat afternoon are counted in
    ``total_sessions`` but excluded from ``evaluated_sessions`` (no directional
    call to score). Pure function: no DB, no clock -- fully unit-testable.
    """
    decisive: List[dict] = []
    total = 0
    hits = 0
    up_real = 0
    signal_returns: List[float] = []

    for row in sessions:
        noon = float(row["noon_px"])
        close = float(row["close_px"])
        charm = row.get("charm_flow")
        if noon <= 0 or charm is None:
            continue
        total += 1
        ret = (close - noon) / noon
        pred = _sign(float(charm))
        real = _sign(ret)
        if pred == 0 or real == 0:
            continue
        hit = pred == real
        if hit:
            hits += 1
        if real > 0:
            up_real += 1
        signal_returns.append(pred * ret)
        decisive.append(
            {
                "date": str(row["session_date"]),
                "charm_flow": float(charm),
                "return_pct": ret,
                "predicted_dir": pred,
                "realized_dir": real,
                "hit": hit,
            }
        )

    evaluated = len(decisive)
    hit_rate = hits / evaluated if evaluated else None
    up_rate = up_real / evaluated if evaluated else None
    baseline = max(up_rate, 1.0 - up_rate) if up_rate is not None else None
    signal_mean_return = sum(signal_returns) / len(signal_returns) if signal_returns else None
    edge = hit_rate - baseline if (hit_rate is not None and baseline is not None) else None

    # Institutional read: a point estimate is not a result. Report a 95%
    # confidence band on the hit rate, the p-value that it beats the naive
    # baseline, and a t-stat on the per-session P&L -- so a thin or lucky
    # sample cannot pass for a real edge.
    ci_low, ci_high = _wilson_interval(hits, evaluated) if evaluated else (None, None)
    edge_p_value = _accuracy_beats_baseline_p(hits, evaluated, baseline) if evaluated else None
    signal_t_stat = _t_stat(signal_returns)
    # Significant only if the edge clears 95% AND the sample is not a handful.
    significant = bool(
        edge_p_value is not None and edge_p_value < 0.05 and evaluated >= _MIN_SIGNIFICANT_N
    )
    return {
        "total_sessions": total,
        "evaluated_sessions": evaluated,
        "hits": hits,
        "hit_rate": hit_rate,
        "hit_rate_ci_low": ci_low,
        "hit_rate_ci_high": ci_high,
        "baseline_rate": baseline,
        "edge": edge,
        "edge_p_value": edge_p_value,
        "significant": significant,
        "signal_mean_return": signal_mean_return,
        "signal_t_stat": signal_t_stat,
        # Most-recent first, capped -- the frontend shows a scannable ledger.
        "records": list(reversed(decisive))[:recent_limit],
    }
