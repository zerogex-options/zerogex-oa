"""Range / expected-volatility / level-odds model for the daily forecast — v1.4.

v1.4 reframes the card away from price prediction.  The projected RANGE is
kept as-is (a coverage-graded containment band), but the pin-strike and
chop/trend-regime tiles — which implicitly claimed the market must react to
gamma levels — are retired as headline claims and replaced by two things
gamma structure genuinely conditions and that grade objectively on the day's
OHLC magnitude, never on direction:

  * ``expected_vol_state`` / ``expected_vol_ratio`` — realized daily range as a
    multiple of a NORMAL day's range (√(8/π)·implied, per-symbol calibrated;
    long gamma damps, short gamma amplifies).
  * ``level_touch_probs`` / ``flip_cross_prob`` — reflection-principle odds that
    price reaches each structural wall / crosses the gamma flip today.

``regime`` is still computed (it conditions both of the above and tilts the
wall odds) but is no longer surfaced as a graded tile.

The range engine remains the feature-weighted heuristic that replaced
heuristic_v1's symmetric-around-spot band.  It folds in the signals ZeroGEX
already computes but wasn't using for the forecast:

  * Asymmetric structural bounds from call wall vs put wall distance
  * Wall-magnitude weighting (via top gamma nodes)
  * Options-implied 1-day move from VIX (SPY/SPX) / VXN (QQQ)
  * IV rank cross-sectional context
  * Realized volatility (ATR-style) blend
  * Directional MSI lean tied to signal intensity
  * "Screaming indicator" amplifier
  * Special-day handlers: monthly OPEX Friday, VIX-piration Wed,
    post-OPEX Monday, event days (FOMC/CPI/NFP)

Layer 2 correction scalars (per-symbol, updated nightly by
``forecast_calibrate``) are applied last so the writer can learn from
past misses without the heuristic itself being re-tuned by hand.  Both
the RAW (pre-correction) and CORRECTED bands are returned so the
receipt writer grades each separately and the calibration loop can tell
if its corrections actually helped.

TODO(2027-01): Replace heuristic_v1_3 with quantile-regression v2 once
we have ~120 receipt rows per symbol.  See
``docs/design/quantile-regression-range-model.md`` for the plan.  In the
meantime, heuristic_v1_3 + Layer 2 corrections is what powers the card.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

# Absolute floor and ceiling on the projected half-range as a fraction of
# spot.  0.15% is roughly a "dead-quiet SPY" and 3% caps the model on
# ultra-vol days so it can't project an absurd $30 SPY band.  Tighter than
# v1 (0.3% / 2.5%) because v1.2 is more information-dense — the model
# should be allowed to be sharper when signals agree.
MIN_RANGE_FRACTION = 0.0015
MAX_RANGE_FRACTION = 0.030

# Base wick allowance on wall distances.  1.15 = "walls plus a 15% margin
# so a wick tap doesn't count as a break."  Bumped from 1.10 in v1.3 —
# the first week of receipts showed v1.2 ranges systematically 20-35%
# too narrow; the calibration loop would eventually widen this, but a
# base bump lets us not wait 3 weeks for the correction layer to kick in.
BASE_WALL_EXPANSION = 1.15

# Event-day multiplier (FOMC / CPI / NFP).  Applied AFTER wall expansion.
EVENT_DAY_MULTIPLIER = 1.5

# OPEX Friday widen.  Positioning compresses THEN releases at 4 PM, so
# the full day is unusually wide relative to the morning walls.
OPEX_FRIDAY_MULTIPLIER = 1.15

# VIX-implied 1-day move: spot * (VIX/100) / sqrt(TRADING_DAYS_YEAR).
# 252 is the convention; forecast lands mid-day so it's close enough.
VIX_TRADING_DAYS_YEAR = 252
VIX_HIGH_Z_THRESHOLD = 2.0        # "screaming vol" trigger
VIX_LOW_Z_THRESHOLD = -1.5        # "compressed vol" — dampen the band

# MSI directional lean: cap the asymmetric shift at ±25% of the base band.
# ``msi_normalized`` is on a -100..+100 scale, so /400 caps at ±0.25.
MSI_LEAN_DIVISOR = 400.0
MSI_LEAN_CAP = 0.25

# Screaming-indicator thresholds.
MSI_SCREAMING_ABS_COMPOSITE = 0.6      # >0.6 or <-0.6 on -1..+1 composite
PCR_SCREAMING_HIGH = 1.5                # very bearish
PCR_SCREAMING_LOW = 0.5                 # very bullish

# Pin tolerance: dynamic per-symbol.  max(strike_step * 0.5, spot * 0.0015)
# gives ~15bps floor everywhere and half-a-strike ceiling.  Bumped in v1.3
# from 10bps → 15bps; "the close was within one strike of max_pain" is
# what a trader would call a pin day, and 10bps was too tight to catch
# real pinning behavior (SPY was pinning at 15-20bps of the pin strike).
PIN_TOLERANCE_MIN_STRIKE_FRACTION = 0.5
PIN_TOLERANCE_MIN_SPOT_FRACTION = 0.0015

# Wall-magnitude weight for "sticky node inside band" — top gamma node
# magnitudes measured in absolute net_gex.  1e8 is a full-chain "wall
# that matters"; smaller nodes are noise.
STICKY_NODE_MIN_MAGNITUDE = 1e8

# Regime chop/trend threshold — see _compute_regime_threshold.  0.6 ×
# implied-1-day-move is roughly "closed inside 60% of the implied
# envelope" — that's what a chop day looks like on a VIX-15 tape.  Below
# the floor a VIX-8 day still needs some tolerance; above the ceiling a
# VIX-40 tape can't be classified because everything moves.
REGIME_THRESHOLD_MULTIPLIER = 0.6
REGIME_THRESHOLD_MIN = 0.003        # 30bps absolute floor
REGIME_THRESHOLD_MAX = 0.020        # 2% absolute ceiling
REGIME_THRESHOLD_FALLBACK = 0.005   # matches legacy 0.5% behavior when no VIX

DEFAULT_STRIKE_STEP = 1.0

# ---------------------------------------------------------------------------
# v1.4 reframe — gradeable claims that replace the pin/regime tiles
# ---------------------------------------------------------------------------
# The card no longer asserts a pinned close or a chop/trend label graded on
# close-vs-open.  Instead it publishes two things gamma structure genuinely
# conditions and that can be scored objectively against the day's OHLC —
# without ever claiming price *direction*:
#
#   * Expected volatility: realized daily range as a multiple of a NORMAL day's
#     range (√(8/π)·implied — an intraday range is ~1.6× the 1-σ implied move,
#     not 1×, so this is the denominator that makes the buckets mean anything).
#     Long gamma damps (ratio < 1), short gamma amplifies (ratio > 1).  Graded
#     path-agnostically, so a round-trip trend day that closes flat still
#     scores as expansion because the *range* was large.
#   * Level touch odds: reflection-principle P(price reaches each structural
#     level today) + P(the gamma flip is crossed).  Graded by Brier score.
#
# Every weight below is a PRIOR (intuition), to be replaced by the nightly
# calibration loop once enough receipts accrue — nothing here claims measured
# edge.  See docs/design/quantile-regression-range-model.md for the v2 plan.

# Range-vs-sigma unit bridge.  ``implied_move`` is a 1-SIGMA close-to-close
# quantity (spot·VIX/100/√252), but the thing we grade — the day's realized
# high-low RANGE — is not a 1-sigma quantity: for a driftless diffusion the
# expected daily range is E[high-low] = √(8/π)·σ ≈ 1.596·σ (Parkinson).  So a
# perfectly ordinary day realizes a range ≈ 1.6× the implied 1-day move.  The
# original grader divided range by the bare 1-sigma implied move and bucketed
# on edges centered at 1.0, which stamped essentially every normal day
# "expansion" (see the demo in the design notes).  We divide by an EXPECTED
# RANGE (√(8/π)·implied) instead, so a normal day re-centers at ≈1.0 and the
# 0.85/1.15 edges below finally mean compression / normal / expansion.  The
# per-symbol ``vol_range_basis_mult`` calibration scalar (learned nightly from
# the trailing realized-range distribution) fine-tunes this center so the
# variance-risk premium — realized vol runs structurally below implied — is
# absorbed empirically rather than assumed.
RANGE_OVER_SIGMA = math.sqrt(8.0 / math.pi)  # ≈ 1.5958 — Parkinson E[range]/σ

VOL_BASE_RATIO = 1.0
VOL_GAMMA_WEIGHT = 0.35          # dealer-gamma sign is the dominant driver
VOL_GEX_SATURATION = 2.0e9      # |net_gex| that saturates the damping/amplifying pull
VOL_VIXZ_WEIGHT = 0.15          # vol-of-vol context (20d VIX z-score)
VOL_FLIP_PROX_WEIGHT = 0.10     # nearness to the flip raises expansion odds
VOL_FLIP_PROX_SPAN = 0.015      # |flip_distance| within this fraction counts as "near"
VOL_LOCAL_GAMMA_WEIGHT = 0.08   # dense local gamma adds pinning — long-gamma only
VOL_LOCAL_GEX_SATURATION = 5.0e8
VOL_RATIO_MIN = 0.45
VOL_RATIO_MAX = 1.90
# Grading band, shared with the receipt grader (kept in sync there).  The
# ratio is realized-range ÷ EXPECTED-range (√(8/π)·implied, per-symbol
# calibrated), so 1.0 == "a statistically normal day" and the edges read:
#   ratio <= LOW  -> compression | >= HIGH -> expansion | else normal
# The SAME classifier grades the model's prediction, which is likewise
# expressed as a multiple of a normal day's range (VOL_BASE_RATIO = 1.0), so
# prediction and outcome are finally measured on one scale.
VOL_NORMAL_LOW = 0.85
VOL_NORMAL_HIGH = 1.15

# Level touch-probability model.  Single-barrier hitting probability over a
# driftless 1-day horizon (reflection principle): P = 2·(1 − Φ(d/σ)), where
# d is the distance to the level and σ the implied 1-day move.  A dealer-regime
# tilt reflects that long-gamma walls are defended (harder to reach) while
# short-gamma walls give way.
LEVEL_TOUCH_MIN = 0.01
LEVEL_TOUCH_MAX = 0.99
LEVEL_LONG_GAMMA_DAMP = 0.90    # long gamma: walls defended -> lower touch odds
LEVEL_SHORT_GAMMA_AMP = 1.12    # short gamma: walls give way -> higher touch odds


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ForecastInputs:
    """Every input the v1.2 model consumes.  Every optional field has a
    graceful-degradation path in ``compute_forecast`` so the writer can
    still produce a forecast when a signal is missing."""

    # Anchoring inputs — spot is the only true hard requirement.
    symbol: str
    forecast_date: date
    spot: float

    # Structural GEX (full-chain, from gex_summary).
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    gamma_flip: Optional[float] = None
    max_pain: Optional[float] = None

    # Dealer-gamma regime inputs.  ``net_gex`` is the full-chain net dealer
    # gamma exposure (sign is the regime: >0 dealers long gamma, <0 short);
    # ``gamma_flip`` above is the price where it crosses zero.  These drive
    # the regime label directly — the label is dealer positioning, not the
    # MSI directional score (which stays a band-shaping input only).
    # ``gex_surface_fresh`` is False when the GEX snapshot the writer read
    # is stale/missing, in which case the regime degrades to "transition"
    # rather than asserting a positioning we can't substantiate.
    net_gex: Optional[float] = None
    gex_surface_fresh: bool = True

    # Vol-character inputs (from gex_summary).  ``local_gex`` is the summed
    # |net_gex| within ±1% of spot (pinning density); ``convexity_risk`` is
    # |total_net_gex| / distance-to-flip (acceleration risk); ``flip_distance``
    # is the signed (spot − flip)/spot.  All feed the expected-volatility call.
    local_gex: Optional[float] = None
    convexity_risk: Optional[float] = None
    flip_distance: Optional[float] = None

    # Structural GEX (0DTE only — nearest expiration walls derived from
    # gex_by_strike filtered to today's expiration).  When populated,
    # OPEX Friday and post-OPEX Monday use these preferentially.
    call_wall_0dte: Optional[float] = None
    put_wall_0dte: Optional[float] = None

    # Top-K gamma nodes by |net_gex|.  Each entry: {strike, net_gex}.
    # Used to detect "sticky" attractors and magnitude-weighted walls.
    top_gamma_nodes: list[dict[str, Any]] = field(default_factory=list)

    # Signals: MSI + component sub-scores + basic-signal readings.
    msi_composite: Optional[float] = None        # -1..+1
    msi_normalized: Optional[float] = None       # -100..+100
    put_call_ratio: Optional[float] = None
    skew_delta: Optional[float] = None           # otm_put_iv - otm_call_iv

    # Vol regime.
    vix_close: Optional[float] = None            # for SPY / SPX
    vxn_close: Optional[float] = None            # for QQQ
    vix_z_score_20d: Optional[float] = None      # for screaming-vol detection
    iv_rank_30d: Optional[float] = None          # 0-100
    atr_5d: Optional[float] = None               # dollar terms

    # Overnight action.
    futures_gap_pct: Optional[float] = None      # overnight ES/NQ gap

    # Spot-anchor provenance (writer metadata, not consumed by the model):
    # "cash" when ``spot`` is the live/last cash print; "futures_implied"
    # when a cash index was projected from its future outside the cash
    # session.  ``open_spot_projection`` carries the projection audit dict.
    open_spot_source: str = "cash"
    open_spot_projection: Optional[dict[str, Any]] = None

    # Playbook state.
    flagship_setup: Optional[dict[str, Any]] = None

    # Calendar flags — derived by the writer, not by the model.
    is_event_day: bool = False                   # FOMC / CPI / NFP
    is_opex_friday: bool = False                 # monthly opex 3rd Fri
    is_vix_expiration: bool = False              # 3rd Wed of month
    is_post_opex_monday: bool = False
    days_to_opex: Optional[int] = None

    # Ladder step for pin snapping.
    strike_step: float = DEFAULT_STRIKE_STEP

    # Layer 2 calibration scalars.  When None → cold start, no corrections.
    # When provided, the corrected fields on ForecastResult get shifted.
    calibration: Optional[dict[str, float]] = None


@dataclass
class ForecastResult:
    """Corrected (final) + raw (pre-correction) claims for the day.

    The ``projected_*`` fields are what the card publishes and what the
    receipt grades.  The ``raw_*`` fields are the pre-Layer-2 forecast
    kept side-by-side so the calibration loop can tell if its
    corrections improved on the base heuristic."""

    projected_low: float
    projected_high: float
    projected_close: float
    pin_strike: Optional[float]
    pin_tolerance: float

    regime: str
    regime_move_threshold: float = REGIME_THRESHOLD_FALLBACK
    range_model: str = "heuristic_v1_3"
    rationale: list[str] = field(default_factory=list)

    # Raw (pre-correction) snapshot — for A/B measurement.
    raw_projected_low: float = 0.0
    raw_projected_high: float = 0.0
    raw_pin_strike: Optional[float] = None

    # v1.4 reframe — the gradeable claims that replace the pin/regime tiles.
    # ``regime`` above is retained internally (it conditions these) but is no
    # longer a headline card claim.
    expected_vol_state: str = "normal"                     # compression | normal | expansion
    expected_vol_ratio: float = 1.0                        # predicted realized range ÷ implied move
    implied_move: Optional[float] = None                   # VIX-implied 1-day $ move (grader denominator)
    flip_cross_prob: Optional[float] = None                # P(spot crosses the gamma flip today)
    level_touch_probs: dict = field(default_factory=dict)  # {"call_wall": p, "put_wall": p}
    gravity_center: Optional[float] = None                 # max-gamma strike (long-gamma pull center)

    # Snapshot of the calibration scalars actually applied.  When no
    # calibration state existed, this is the neutral {1.0, 1.0, 0, 0}.
    calibration_applied: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _round_to_strike(value: float, step: float) -> float:
    if step <= 0:
        return round(value, 2)
    return round(round(value / step) * step, 4)


def _classify_regime(
    net_gex: Optional[float],
    spot: Optional[float],
    gamma_flip: Optional[float],
    *,
    surface_fresh: bool = True,
) -> str:
    """Dealer gamma regime from actual dealer positioning.

    The regime is the *sign of net dealer gamma* — the definition of a
    long-/short-gamma tape — not a directional-signal proxy::

        net_gex > 0  -> dealers net long gamma  -> vol-suppressing -> long_gamma
        net_gex < 0  -> dealers net short gamma -> vol-amplifying  -> short_gamma

    When ``net_gex`` is unavailable we fall back to spot-vs-gamma-flip
    (above the flip == long gamma, the same crossover ``net_gex``'s sign
    encodes).  We return ``transition`` when the GEX surface is stale/missing
    or no dealer-gamma signal exists at all — the forecast never asserts a
    regime off data it can't stand behind.  (This replaces the prior
    MSI-composite-sign proxy, which — fed the 0-100 Market State Index where
    it expected a signed -1..+1 value — silently pinned every symbol to
    ``long_gamma`` on every run.)
    """
    if not surface_fresh:
        return "transition"
    if net_gex is not None:
        if net_gex > 0:
            return "long_gamma"
        if net_gex < 0:
            return "short_gamma"
        return "transition"  # exactly at the flip
    # Fallback: spot relative to the gamma-flip level.
    if spot is not None and gamma_flip is not None and spot > 0 and gamma_flip > 0:
        if spot > gamma_flip:
            return "long_gamma"
        if spot < gamma_flip:
            return "short_gamma"
        return "transition"
    return "transition"


def _vix_implied_daily_move(spot: float, vix: Optional[float]) -> Optional[float]:
    """Options-implied 1-day move in dollars: spot * (VIX/100) / √252.
    Returns None when VIX is missing or non-positive."""
    if vix is None or vix <= 0 or spot <= 0:
        return None
    return spot * (vix / 100.0) / math.sqrt(VIX_TRADING_DAYS_YEAR)


def _compute_regime_threshold(
    spot: float, vix_close: Optional[float], vxn_close: Optional[float]
) -> float:
    """Chop/trend threshold as a fraction of spot.

    The receipt grader uses:
      * ``long_gamma``  correct when ``|close-open|/open <= threshold``
      * ``short_gamma`` correct when ``|close-open|/open >  threshold``

    v1 hardcoded 0.5% for every symbol and every vol regime.  That made
    the grade a VIX thermometer: on VIX-30 everything was a "trend day",
    on VIX-12 everything a "chop day".  v1.3 pins the threshold to
    ``0.6 × VIX-implied 1-day move / spot`` so a compressed-vol day
    grades chop against a tighter bar and a high-vol day grades trend
    against a looser bar.

    Floor 30bps (a VIX-8 tape still needs some tolerance) and cap 2%
    (a VIX-40 tape is unclassifiable — no threshold is meaningful).
    Fallback to 0.005 (legacy) when VIX/VXN is unavailable so pre-v1.3
    behavior is preserved for symbols we haven't wired vol for.
    """
    vol = vix_close if vix_close is not None else vxn_close
    implied_dollars = _vix_implied_daily_move(spot, vol)
    if implied_dollars is None or spot <= 0:
        return REGIME_THRESHOLD_FALLBACK
    implied_fraction = implied_dollars / spot
    threshold = REGIME_THRESHOLD_MULTIPLIER * implied_fraction
    return _clamp(threshold, REGIME_THRESHOLD_MIN, REGIME_THRESHOLD_MAX)


def _select_pin_strike(inp: ForecastInputs) -> Optional[float]:
    """Prefer ``max_pain``.  Fall back to nearest strike; return None if
    both are missing so the page renders 'no pin candidate' honestly."""
    if inp.max_pain is not None:
        return float(inp.max_pain)
    if inp.spot is None:
        return None
    return _round_to_strike(inp.spot, inp.strike_step)


def _pin_tolerance(spot: float, strike_step: float, calibration_mult: float) -> float:
    """Dynamic per-symbol pin tolerance.  Half a strike step for SPY/QQQ,
    10bps of spot as an absolute floor.  Scaled by the calibration
    multiplier so the correction loop can widen/tighten uniformly."""
    strike_based = strike_step * PIN_TOLERANCE_MIN_STRIKE_FRACTION
    spot_based = spot * PIN_TOLERANCE_MIN_SPOT_FRACTION
    return max(strike_based, spot_based) * calibration_mult


def _normal_cdf(x: float) -> float:
    """Standard-normal CDF via erf — no scipy dependency."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _barrier_touch_prob(distance: Optional[float], sigma: Optional[float]) -> Optional[float]:
    """P(a driftless 1-day path touches a level ``distance`` away), given a
    1-day stdev ``sigma``.  Reflection principle: P(max excursion ≥ d) =
    2·(1 − Φ(d/σ)).  Returns None when inputs are unusable; clamped to a sane
    [1%, 99%] so a level sitting on spot doesn't read as a certainty."""
    if sigma is None or sigma <= 0 or distance is None:
        return None
    p = 2.0 * (1.0 - _normal_cdf(abs(distance) / sigma))
    return _clamp(p, LEVEL_TOUCH_MIN, LEVEL_TOUCH_MAX)


def _classify_vol_state(ratio: float) -> str:
    """Map a realized-÷-implied ratio to compression / normal / expansion.
    Shared shape with the receipt grader so a committed prediction and its
    grade use identical band edges."""
    if ratio <= VOL_NORMAL_LOW:
        return "compression"
    if ratio >= VOL_NORMAL_HIGH:
        return "expansion"
    return "normal"


def _compute_expected_vol(inp: ForecastInputs, rationale: list[str]) -> tuple[str, float]:
    """Predict realized daily range as a multiple of a NORMAL day's range.

    A ratio of 1.0 means "expect a statistically ordinary day"; dealer-gamma
    sign is the dominant driver — long gamma suppresses realized volatility
    (ratio < 1, compression), short gamma amplifies it (ratio > 1, expansion).
    Dense local gamma adds pinning (long-gamma only), a stretched VIX z-score
    lifts the whole tape, and proximity to the flip raises expansion odds
    because the regime itself is unstable there.  The prediction is on the same
    "× a normal day's range" scale the receipt grades against (a normal day's
    range being √(8/π)·implied, per-symbol calibrated).  Makes NO claim about
    direction.
    """
    ratio = VOL_BASE_RATIO
    if inp.gex_surface_fresh and inp.net_gex is not None:
        g = math.tanh(inp.net_gex / VOL_GEX_SATURATION)  # +long / -short
        ratio *= 1.0 - VOL_GAMMA_WEIGHT * g
        if inp.net_gex > 0 and inp.local_gex is not None and inp.local_gex > 0:
            dens = math.tanh(inp.local_gex / VOL_LOCAL_GEX_SATURATION)
            ratio *= 1.0 - VOL_LOCAL_GAMMA_WEIGHT * dens
    if inp.vix_z_score_20d is not None:
        z = _clamp(inp.vix_z_score_20d / 3.0, -1.0, 1.0)
        ratio *= 1.0 + VOL_VIXZ_WEIGHT * z
    if inp.flip_distance is not None and VOL_FLIP_PROX_SPAN > 0:
        prox = max(0.0, 1.0 - abs(inp.flip_distance) / VOL_FLIP_PROX_SPAN)
        if prox > 0:
            ratio *= 1.0 + VOL_FLIP_PROX_WEIGHT * prox
    ratio = _clamp(ratio, VOL_RATIO_MIN, VOL_RATIO_MAX)
    state = _classify_vol_state(ratio)
    rationale.append(f"Expected vol={state} (realized≈{ratio:.2f}× a normal day's range)")
    return state, round(ratio, 4)


def _compute_level_touch_probs(
    inp: ForecastInputs,
    implied_move: Optional[float],
    regime: str,
    rationale: list[str],
) -> tuple[dict[str, float], Optional[float]]:
    """Reflection-principle P(touch today) for each wall + P(flip cross).

    Walls are tilted by the dealer regime (long gamma defends them, short
    gamma lets them break).  The flip-cross probability is deliberately NOT
    regime-tilted: crossing the flip *is* the regime-change event, so tilting
    by the current regime would double-count it.
    """
    spot = inp.spot
    if implied_move is None or implied_move <= 0 or spot <= 0:
        return {}, None
    tilt = (
        LEVEL_LONG_GAMMA_DAMP if regime == "long_gamma"
        else LEVEL_SHORT_GAMMA_AMP if regime == "short_gamma"
        else 1.0
    )
    probs: dict[str, float] = {}
    for name, level in (("call_wall", inp.call_wall), ("put_wall", inp.put_wall)):
        p = _barrier_touch_prob((level - spot) if level is not None else None, implied_move)
        if p is not None:
            probs[name] = round(_clamp(p * tilt, LEVEL_TOUCH_MIN, LEVEL_TOUCH_MAX), 4)
    flip_prob = _barrier_touch_prob(
        (inp.gamma_flip - spot) if inp.gamma_flip is not None else None, implied_move
    )
    if flip_prob is not None:
        flip_prob = round(flip_prob, 4)
    if probs or flip_prob is not None:
        parts = [f"{k} {v:.0%}" for k, v in probs.items()]
        if flip_prob is not None:
            parts.append(f"flip-cross {flip_prob:.0%}")
        rationale.append("Level touch odds: " + ", ".join(parts))
    return probs, flip_prob


def _select_gravity_center(inp: ForecastInputs) -> Optional[float]:
    """The max-gamma strike price acts as a pull center while dealers are long
    gamma.  Prefer the largest top gamma node; fall back to max_pain."""
    for node in inp.top_gamma_nodes[:1]:
        strike = node.get("strike")
        if strike is not None:
            try:
                return float(strike)
            except (TypeError, ValueError):
                pass
    return float(inp.max_pain) if inp.max_pain is not None else None


def grade_realized_claims(
    *,
    open_spot: float,
    actual_low: float,
    actual_high: float,
    implied_move: Optional[float],
    expected_vol_state: Optional[str],
    call_wall: Optional[float],
    put_wall: Optional[float],
    gamma_flip: Optional[float],
    level_touch_probs: Optional[dict],
    flip_cross_prob: Optional[float],
    range_over_sigma: float = RANGE_OVER_SIGMA,
) -> dict[str, Any]:
    """Grade the v1.4 claims against the day's cash-session OHLC.

    Pure + deterministic so the receipt grader in the DB layer stays a thin
    caller and the band edges live in exactly one place.  Everything here is a
    MAGNITUDE test — realized range, whether a level was reached, whether the
    flip was crossed — never a direction call.  A round-trip trend day that
    closes flat still reads as expansion because the *range* was large.

    ``realized_vol_ratio`` is the day's high-low range as a multiple of a
    NORMAL day's range, where a normal day's range is
    ``range_over_sigma × implied_move`` — ``range_over_sigma`` defaults to the
    Parkinson constant √(8/π) ≈ 1.6 and the receipt grader multiplies in the
    per-symbol ``vol_range_basis_mult`` calibration scalar committed that
    morning.  Dividing by the expected RANGE (not the bare 1-σ implied move) is
    what re-centers an ordinary day at ≈1.0 so the compression/normal/expansion
    buckets are meaningful; grading against the *committed* basis keeps the
    verdict deterministic and immutable per the morning commitment.

    Returns ``{realized_vol_ratio, vol_state_correct, flip_crossed,
    level_touch_outcomes, levels_brier}`` with ``None`` for any verdict whose
    inputs weren't committed (e.g. no implied move, no walls).
    """
    realized_range = actual_high - actual_low
    realized_vol_ratio: Optional[float] = None
    vol_state_correct: Optional[bool] = None
    if implied_move is not None and implied_move > 0 and range_over_sigma > 0:
        expected_range = range_over_sigma * implied_move
        realized_vol_ratio = round(realized_range / expected_range, 4)
        realized_bucket = _classify_vol_state(realized_vol_ratio)
        if expected_vol_state is not None:
            vol_state_correct = realized_bucket == expected_vol_state

    # Open sits on one side of the flip by definition; a cross is the intraday
    # extreme reaching the other side.
    flip_crossed: Optional[bool] = None
    if gamma_flip is not None:
        flip_crossed = (
            (open_spot > gamma_flip and actual_low <= gamma_flip)
            or (open_spot < gamma_flip and actual_high >= gamma_flip)
        )

    outcomes: dict[str, bool] = {}
    if call_wall is not None:
        outcomes["call_wall"] = actual_high >= call_wall
    if put_wall is not None:
        outcomes["put_wall"] = actual_low <= put_wall
    if flip_crossed is not None:
        outcomes["gamma_flip"] = flip_crossed

    probs = level_touch_probs if isinstance(level_touch_probs, dict) else {}
    brier_terms: list[float] = []
    for key in ("call_wall", "put_wall"):
        p = probs.get(key)
        outcome = outcomes.get(key)
        if p is not None and outcome is not None:
            brier_terms.append((float(p) - (1.0 if outcome else 0.0)) ** 2)
    if flip_cross_prob is not None and flip_crossed is not None:
        brier_terms.append((float(flip_cross_prob) - (1.0 if flip_crossed else 0.0)) ** 2)
    levels_brier = round(sum(brier_terms) / len(brier_terms), 4) if brier_terms else None

    return {
        "realized_vol_ratio": realized_vol_ratio,
        "vol_state_correct": vol_state_correct,
        "flip_crossed": flip_crossed,
        "level_touch_outcomes": outcomes,
        "levels_brier": levels_brier,
    }


def _structural_half_bands(inp: ForecastInputs) -> tuple[float, float, list[str]]:
    """Compute asymmetric (upside, downside) half-bands from the walls.

    Uses 0DTE walls on OPEX Friday / post-OPEX Monday when available,
    blended 70/30 with the full-chain values.  Everywhere else uses the
    full-chain call/put walls.  Returns (up_half, down_half, rationale)."""
    rationale: list[str] = []
    spot = inp.spot

    # Choose the wall inputs — 70/30 blend of 0DTE and full-chain on
    # special days when the 0DTE values are populated.  Elsewhere, plain
    # full-chain.  The blend keeps the full-chain as a sanity anchor so a
    # single wonky 0DTE strike doesn't blow up the band.
    use_dte_blend = (
        (inp.is_opex_friday or inp.is_post_opex_monday)
        and inp.call_wall_0dte is not None
        and inp.put_wall_0dte is not None
    )
    if use_dte_blend and inp.call_wall is not None and inp.put_wall is not None:
        call_wall = 0.7 * float(inp.call_wall_0dte) + 0.3 * float(inp.call_wall)
        put_wall = 0.7 * float(inp.put_wall_0dte) + 0.3 * float(inp.put_wall)
        rationale.append(
            f"Walls: 70/30 0DTE blend (call ${call_wall:.2f}, put ${put_wall:.2f})"
        )
    else:
        call_wall = float(inp.call_wall) if inp.call_wall is not None else None  # type: ignore[assignment]
        put_wall = float(inp.put_wall) if inp.put_wall is not None else None  # type: ignore[assignment]

    min_half = spot * MIN_RANGE_FRACTION

    if call_wall is None or put_wall is None:
        rationale.append(f"Walls missing — floor ±{MIN_RANGE_FRACTION:.2%}")
        return min_half, min_half, rationale

    # Guard against degenerate/inverted walls (call below spot or put
    # above spot).  Fall back to floor on that side only.
    up_raw = max(0.0, call_wall - spot)
    down_raw = max(0.0, spot - put_wall)
    up_half = max(min_half, up_raw * BASE_WALL_EXPANSION)
    down_half = max(min_half, down_raw * BASE_WALL_EXPANSION)

    if up_raw <= 0 or down_raw <= 0:
        rationale.append(
            f"One wall inverted vs spot — degraded (call {call_wall:.2f}, put {put_wall:.2f})"
        )
    else:
        rationale.append(
            f"Walls asym: up ${up_half:.2f} (×{BASE_WALL_EXPANSION}) / "
            f"down ${down_half:.2f}"
        )

    return up_half, down_half, rationale


def _apply_vix_blend(
    up_half: float,
    down_half: float,
    inp: ForecastInputs,
    rationale: list[str],
) -> tuple[float, float]:
    """Blend the structural band with the options-implied 1-day move.

    Take the max of the two so a compressed structural band doesn't
    under-predict on a high-VIX day.  VIX-piration Wednesday dampens the
    VIX weight in favor of realized ATR because the implied surface is
    rolling that afternoon."""
    vol_proxy = inp.vix_close if inp.vix_close is not None else inp.vxn_close
    if vol_proxy is None:
        return up_half, down_half

    implied = _vix_implied_daily_move(inp.spot, vol_proxy)
    if implied is None:
        return up_half, down_half

    if inp.is_vix_expiration:
        # Blend more heavily toward realized volatility on VIX-piration
        # Wed — the implied surface is stale mid-week during the roll.
        weight = 0.3
        rationale.append(f"VIX-piration Wed — implied blend @ 30% (implied ±${implied:.2f})")
    else:
        weight = 0.6
        rationale.append(f"Implied blend: ±${implied:.2f} @ {weight:.0%}")

    # Weight the max-of-band-and-implied by ``weight`` (rest goes to the
    # structural band).  Keeps asymmetry from the structural side.
    up_half = (1 - weight) * up_half + weight * max(up_half, implied)
    down_half = (1 - weight) * down_half + weight * max(down_half, implied)
    return up_half, down_half


def _apply_atr_floor(
    up_half: float,
    down_half: float,
    inp: ForecastInputs,
    rationale: list[str],
) -> tuple[float, float]:
    """Realized 5-day ATR as a floor.  A band that under-predicts the
    typical daily move for the last 5 sessions is misleading."""
    if inp.atr_5d is None or inp.atr_5d <= 0:
        return up_half, down_half
    # ATR is a full-day range proxy; half of it is the half-band floor.
    atr_half = inp.atr_5d * 0.5
    old_up, old_down = up_half, down_half
    up_half = max(up_half, atr_half)
    down_half = max(down_half, atr_half)
    if up_half > old_up or down_half > old_down:
        rationale.append(f"ATR floor lifted band to ±${atr_half:.2f}")
    return up_half, down_half


def _apply_msi_lean(
    up_half: float,
    down_half: float,
    inp: ForecastInputs,
    rationale: list[str],
) -> tuple[float, float]:
    """Shift the band asymmetrically based on the signed MSI.  Positive
    MSI (bullish structural context) widens upside + tightens downside;
    negative MSI does the reverse.  Cap at ±25% so a single strong
    signal can't collapse the band to zero on one side."""
    if inp.msi_normalized is None:
        return up_half, down_half
    lean = _clamp(inp.msi_normalized / MSI_LEAN_DIVISOR, -MSI_LEAN_CAP, MSI_LEAN_CAP)
    if abs(lean) < 0.02:
        return up_half, down_half
    up_half *= 1.0 + lean
    down_half *= 1.0 - lean
    rationale.append(
        f"MSI lean {lean:+.2f} (norm {inp.msi_normalized:+.1f}) → "
        f"up ×{1 + lean:.2f} / down ×{1 - lean:.2f}"
    )
    return up_half, down_half


def _apply_intensity_amplifier(
    up_half: float,
    down_half: float,
    inp: ForecastInputs,
    rationale: list[str],
) -> tuple[float, float]:
    """Screaming-indicator amplifier.

    When multiple signals agree on a direction with high intensity,
    tighten the band on the trend side (high conviction) and widen on
    the retracement side.  When vol is screaming but direction is
    unclear, widen symmetrically."""
    directional_screams = 0
    direction = 0
    if inp.msi_composite is not None and abs(inp.msi_composite) > MSI_SCREAMING_ABS_COMPOSITE:
        directional_screams += 1
        direction += 1 if inp.msi_composite > 0 else -1
    if inp.put_call_ratio is not None:
        if inp.put_call_ratio > PCR_SCREAMING_HIGH:
            directional_screams += 1
            direction -= 1
        elif inp.put_call_ratio < PCR_SCREAMING_LOW:
            directional_screams += 1
            direction += 1

    vol_screaming = inp.vix_z_score_20d is not None and inp.vix_z_score_20d > VIX_HIGH_Z_THRESHOLD
    vol_compressed = inp.vix_z_score_20d is not None and inp.vix_z_score_20d < VIX_LOW_Z_THRESHOLD

    if directional_screams >= 2:
        # High-conviction directional day — tighten trend side 10%, widen
        # retracement side 10% to leave room for a fade.
        if direction > 0:
            up_half *= 0.90
            down_half *= 1.10
            rationale.append(f"Screaming bullish ({directional_screams} signals) — tighten upside, widen downside")
        elif direction < 0:
            up_half *= 1.10
            down_half *= 0.90
            rationale.append(f"Screaming bearish ({directional_screams} signals) — widen upside, tighten downside")

    if vol_screaming:
        up_half *= 1.15
        down_half *= 1.15
        rationale.append(f"VIX z-score {inp.vix_z_score_20d:+.1f} — widen ±15%")
    elif vol_compressed:
        up_half *= 0.92
        down_half *= 0.92
        rationale.append(f"VIX z-score {inp.vix_z_score_20d:+.1f} — vol compressed, tighten ±8%")

    return up_half, down_half


def _apply_sticky_nodes(
    up_half: float,
    down_half: float,
    inp: ForecastInputs,
    rationale: list[str],
) -> tuple[float, float]:
    """A large gamma node inside the projected band acts as an attractor
    that constrains realized range.  Tighten the band 5% when any top
    node above the magnitude threshold sits inside."""
    if not inp.top_gamma_nodes:
        return up_half, down_half

    band_lo = inp.spot - down_half
    band_hi = inp.spot + up_half
    for node in inp.top_gamma_nodes[:3]:
        strike = node.get("strike")
        net_gex = node.get("net_gex")
        if strike is None or net_gex is None:
            continue
        if abs(float(net_gex)) < STICKY_NODE_MIN_MAGNITUDE:
            continue
        if band_lo <= float(strike) <= band_hi:
            up_half *= 0.95
            down_half *= 0.95
            rationale.append(
                f"Sticky node ${float(strike):.2f} (net_gex {float(net_gex):.2g}) — tighten ±5%"
            )
            break
    return up_half, down_half


def _apply_special_day_multipliers(
    up_half: float,
    down_half: float,
    inp: ForecastInputs,
    rationale: list[str],
) -> tuple[float, float]:
    """Apply the OPEX / event-day / gap adjustments last so their impact
    stacks predictably on top of the earlier signal-driven adjustments."""
    if inp.is_event_day:
        up_half *= EVENT_DAY_MULTIPLIER
        down_half *= EVENT_DAY_MULTIPLIER
        rationale.append(f"Event day ×{EVENT_DAY_MULTIPLIER}")
        # Overnight-gap tilt.
        if inp.futures_gap_pct is not None and abs(inp.futures_gap_pct) > 0.003:
            tilt = _clamp(inp.futures_gap_pct * 3.0, -0.20, 0.20)
            up_half *= 1.0 + max(0.0, tilt)
            down_half *= 1.0 + max(0.0, -tilt)
            rationale.append(f"Overnight gap {inp.futures_gap_pct:+.2%} → asymmetric event tilt {tilt:+.2f}")

    if inp.is_opex_friday:
        up_half *= OPEX_FRIDAY_MULTIPLIER
        down_half *= OPEX_FRIDAY_MULTIPLIER
        rationale.append(f"Monthly OPEX Fri — widen ×{OPEX_FRIDAY_MULTIPLIER}")

    return up_half, down_half


def _apply_calibration(
    up_half: float,
    down_half: float,
    calibration: dict[str, float],
    rationale: list[str],
) -> tuple[float, float]:
    """Layer 2 corrections.  These scalars come from the nightly
    calibration cron reading the trailing-20 receipts.  Applied
    multiplicatively so a neutral state (1.0 / 1.0 / 0 / 0) is a no-op."""
    band_mult = float(calibration.get("band_width_mult", 1.0))
    up_lean = float(calibration.get("upside_lean", 0.0))
    down_lean = float(calibration.get("downside_lean", 0.0))
    if band_mult != 1.0 or up_lean != 0.0 or down_lean != 0.0:
        up_half *= band_mult * (1.0 + up_lean)
        down_half *= band_mult * (1.0 + down_lean)
        rationale.append(
            f"Calibrated: band×{band_mult:.2f}, up_lean{up_lean:+.2f}, down_lean{down_lean:+.2f}"
        )
    return up_half, down_half


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


NEUTRAL_CALIBRATION: dict[str, float] = {
    "band_width_mult": 1.0,
    "pin_tolerance_mult": 1.0,
    "upside_lean": 0.0,
    "downside_lean": 0.0,
    # Per-symbol re-centering of the expected-range denominator used to grade
    # the vol call.  1.0 == "a normal day's range is exactly √(8/π)·implied";
    # the nightly calibrator lowers it toward the symbol's empirical median
    # range (variance-risk premium ⇒ typically < 1.0).  Captured into
    # forecast_inputs.calibration_applied so the receipt grades against the
    # basis committed that morning, never a later-drifted one.
    "vol_range_basis_mult": 1.0,
}


def compute_forecast(inp: ForecastInputs) -> ForecastResult:
    """Compute the committed daily forecast.

    Deterministic given inputs — running twice with the same inputs
    produces byte-identical output (used by the writer's content_hash).
    """
    spot = float(inp.spot)
    calibration = {**NEUTRAL_CALIBRATION, **(inp.calibration or {})}

    # Step 1 — structural asymmetric band from walls.
    up_half, down_half, rationale = _structural_half_bands(inp)

    # Step 2 — implied-vol blend + realized-vol floor.
    up_half, down_half = _apply_vix_blend(up_half, down_half, inp, rationale)
    up_half, down_half = _apply_atr_floor(up_half, down_half, inp, rationale)

    # Step 3 — signal-driven adjustments (directional lean + intensity).
    up_half, down_half = _apply_msi_lean(up_half, down_half, inp, rationale)
    up_half, down_half = _apply_intensity_amplifier(up_half, down_half, inp, rationale)

    # Step 4 — magnitude-weighted attractors.
    up_half, down_half = _apply_sticky_nodes(up_half, down_half, inp, rationale)

    # Step 5 — special-day multipliers (event / OPEX / gap tilt).
    up_half, down_half = _apply_special_day_multipliers(up_half, down_half, inp, rationale)

    # Step 6 — snapshot the RAW pre-calibration band (for A/B grading).
    max_half = spot * MAX_RANGE_FRACTION
    min_half = spot * MIN_RANGE_FRACTION
    raw_up = _clamp(up_half, min_half, max_half)
    raw_down = _clamp(down_half, min_half, max_half)
    raw_low = round(spot - raw_down, 4)
    raw_high = round(spot + raw_up, 4)

    # Step 7 — Layer 2 corrections.
    up_half, down_half = _apply_calibration(up_half, down_half, calibration, rationale)
    up_half = _clamp(up_half, min_half, max_half)
    down_half = _clamp(down_half, min_half, max_half)

    projected_low = round(spot - down_half, 4)
    projected_high = round(spot + up_half, 4)

    # Step 8 — pin strike + dynamic tolerance.
    pin_strike = _select_pin_strike(inp)
    raw_pin = pin_strike  # v1.2 does not correct pin selection, only tolerance
    if pin_strike is not None:
        if inp.max_pain is not None:
            rationale.append(f"Pin = max_pain ${pin_strike:.2f}")
        else:
            rationale.append(f"Pin = nearest strike ${pin_strike:.2f} (max_pain missing)")
        projected_close = _clamp(pin_strike, projected_low, projected_high)
    else:
        projected_close = spot
        rationale.append("No pin candidate — projected close = open spot")
    pin_tol = _pin_tolerance(spot, inp.strike_step, calibration["pin_tolerance_mult"])

    # Step 9 — regime (dealer gamma) + VIX-normalized chop/trend threshold.
    regime = _classify_regime(
        inp.net_gex, spot, inp.gamma_flip, surface_fresh=inp.gex_surface_fresh
    )
    regime_threshold = _compute_regime_threshold(spot, inp.vix_close, inp.vxn_close)
    if not inp.gex_surface_fresh:
        rationale.append("Regime=transition (GEX surface stale/missing)")
    elif inp.net_gex is not None:
        stance = "long" if inp.net_gex > 0 else "short" if inp.net_gex < 0 else "flat"
        rationale.append(
            f"Regime={regime} from net_gex={inp.net_gex:.3g} (dealers {stance} gamma)"
        )
    elif inp.gamma_flip is not None:
        rationale.append(
            f"Regime={regime} from spot ${spot:.2f} vs gamma-flip ${inp.gamma_flip:.2f}"
        )
    else:
        rationale.append("Regime=transition (no dealer-gamma signal)")
    rationale.append(
        f"Chop/trend threshold={regime_threshold:.4f} "
        f"({regime_threshold * 100:.2f}%)"
    )

    # Step 10 — v1.4 gradeable claims.  ``regime`` conditions both: it drives
    # the expected-vol sign and tilts the wall touch odds, but is no longer a
    # headline claim of its own.
    vol_proxy = inp.vix_close if inp.vix_close is not None else inp.vxn_close
    implied_move = _vix_implied_daily_move(spot, vol_proxy)
    expected_vol_state, expected_vol_ratio = _compute_expected_vol(inp, rationale)
    level_touch_probs, flip_cross_prob = _compute_level_touch_probs(
        inp, implied_move, regime, rationale
    )
    gravity_center = _select_gravity_center(inp)

    return ForecastResult(
        projected_low=projected_low,
        projected_high=projected_high,
        projected_close=round(projected_close, 4),
        pin_strike=pin_strike,
        pin_tolerance=round(pin_tol, 4),
        regime=regime,
        regime_move_threshold=round(regime_threshold, 6),
        range_model="heuristic_v1_4",
        rationale=rationale,
        raw_projected_low=raw_low,
        raw_projected_high=raw_high,
        raw_pin_strike=raw_pin,
        calibration_applied=dict(calibration),
        expected_vol_state=expected_vol_state,
        expected_vol_ratio=expected_vol_ratio,
        implied_move=round(implied_move, 4) if implied_move is not None else None,
        flip_cross_prob=flip_cross_prob,
        level_touch_probs=level_touch_probs,
        gravity_center=round(gravity_center, 4) if gravity_center is not None else None,
    )
