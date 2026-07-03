"""Range / pin / regime model for the daily forecast writer — v1.2.

This is the feature-weighted heuristic that replaces heuristic_v1's
symmetric-around-spot band.  It folds in the signals ZeroGEX already
computes but wasn't using for the forecast:

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

TODO(2027-01): Replace heuristic_v1_2 with quantile-regression v2 once
we have ~120 receipt rows per symbol.  See
``docs/design/quantile-regression-range-model.md`` for the plan.  In the
meantime, heuristic_v1_2 + Layer 2 corrections is what powers the card.
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

# Base wick allowance on wall distances.  1.10 = "walls plus a 10% margin
# so a wick tap doesn't count as a break."
BASE_WALL_EXPANSION = 1.10

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

# Pin tolerance: dynamic per-symbol.  max(strike_step * 0.5, spot * 0.001)
# gives ~10bps floor everywhere and half-a-strike ceiling.
PIN_TOLERANCE_MIN_STRIKE_FRACTION = 0.5
PIN_TOLERANCE_MIN_SPOT_FRACTION = 0.001

# Wall-magnitude weight for "sticky node inside band" — top gamma node
# magnitudes measured in absolute net_gex.  1e8 is a full-chain "wall
# that matters"; smaller nodes are noise.
STICKY_NODE_MIN_MAGNITUDE = 1e8

DEFAULT_STRIKE_STEP = 1.0


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
    range_model: str = "heuristic_v1_2"
    rationale: list[str] = field(default_factory=list)

    # Raw (pre-correction) snapshot — for A/B measurement.
    raw_projected_low: float = 0.0
    raw_projected_high: float = 0.0
    raw_pin_strike: Optional[float] = None

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


def _classify_regime(msi_composite: Optional[float]) -> str:
    """Composite-score sign is the cleanest gamma-regime proxy in 7 AM
    data.  Threshold is intentionally the same as v1 so history stays
    comparable; the informativeness improvement comes from what the
    regime label *does downstream*, not from a new label taxonomy."""
    if msi_composite is None:
        return "transition"
    if msi_composite > 0.15:
        return "long_gamma"
    if msi_composite < -0.15:
        return "short_gamma"
    return "transition"


def _vix_implied_daily_move(spot: float, vix: Optional[float]) -> Optional[float]:
    """Options-implied 1-day move in dollars: spot * (VIX/100) / √252.
    Returns None when VIX is missing or non-positive."""
    if vix is None or vix <= 0 or spot <= 0:
        return None
    return spot * (vix / 100.0) / math.sqrt(VIX_TRADING_DAYS_YEAR)


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

    # Step 9 — regime.
    regime = _classify_regime(inp.msi_composite)
    rationale.append(
        f"Regime={regime} from MSI composite={inp.msi_composite}"
        if inp.msi_composite is not None
        else "Regime=transition (no MSI)"
    )

    return ForecastResult(
        projected_low=projected_low,
        projected_high=projected_high,
        projected_close=round(projected_close, 4),
        pin_strike=pin_strike,
        pin_tolerance=round(pin_tol, 4),
        regime=regime,
        range_model="heuristic_v1_2",
        rationale=rationale,
        raw_projected_low=raw_low,
        raw_projected_high=raw_high,
        raw_pin_strike=raw_pin,
        calibration_applied=dict(calibration),
    )
