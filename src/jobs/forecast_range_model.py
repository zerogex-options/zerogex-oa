"""Range / pin / regime model for the daily forecast writer.

This is the v1 model — an honest heuristic that bounds the projected
intraday range by the call/put walls (which actually matter on most days),
expands by a safety margin so wicks count, and applies an event-day
multiplier for FOMC/CPI/NFP. The output is intentionally conservative:
better to under-promise tightness and be vindicated than to over-promise
and whiff publicly.

The interface (``compute_forecast``) returns a typed result whose contract
is what the quantile-regression v2 model will produce when it ships, so
swapping models later is a one-line change in the writer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

# Bounds on the projected range as a fraction of spot. The lower bound
# stops the model from issuing a sub-30bp band even in dead-quiet regimes
# (you'd never bet on it). The upper bound stops a degenerate "no walls"
# situation from producing an absurdly wide useless prediction.
MIN_RANGE_FRACTION = 0.003   # 0.3% of spot
MAX_RANGE_FRACTION = 0.025   # 2.5% of spot

# Wall-expansion factor: the band is centered on spot and stretched out
# to (max distance from spot to either wall) × this multiplier. 1.10
# means "the walls plus a 10% safety margin for wicks". When walls are
# missing or one-sided we fall back to a fixed ±MIN_RANGE_FRACTION/2.
WALL_EXPANSION = 1.10

# Multiplier applied on event days (FOMC / CPI / NFP). Realized ranges
# on macro days are ~1.5x typical session ranges; the band gets the same
# stretch so we don't whiff on the easy days to forecast.
EVENT_DAY_MULTIPLIER = 1.5

# Pin-strike search: when GEX summary doesn't carry a max_pain we fall
# back to the strike nearest to spot in the ladder. Default ladder step
# for SPY/QQQ/IWM is 1.0; SPX uses 5.0. The writer passes the actual
# step in when known.
DEFAULT_STRIKE_STEP = 1.0


@dataclass
class ForecastInputs:
    """Everything the model needs to make today's call. Every field maps
    to a real production endpoint so the writer can populate it via the
    existing API surface."""

    symbol: str
    forecast_date: date
    spot: float                          # /api/market/quote
    call_wall: Optional[float] = None    # /api/gex/summary
    put_wall: Optional[float] = None     # /api/gex/summary
    gamma_flip: Optional[float] = None   # /api/gex/summary
    max_pain: Optional[float] = None     # /api/gex/summary  or  /api/max-pain/current
    msi_composite: Optional[float] = None  # /api/signals/score (composite_score, -1..+1)
    msi_normalized: Optional[float] = None  # /api/signals/score (normalized_score, -100..+100)
    flagship_setup: Optional[dict[str, Any]] = None  # /api/signals/action (Trade Card; None on STAND_DOWN)
    is_event_day: bool = False           # FOMC / CPI / NFP — caller decides
    strike_step: float = DEFAULT_STRIKE_STEP


@dataclass
class ForecastResult:
    """The committed morning forecast. Mirrors daily_forecast row columns."""

    projected_low: float
    projected_high: float
    projected_close: float
    pin_strike: Optional[float]
    regime: str                          # 'long_gamma' | 'short_gamma' | 'transition'
    range_model: str
    rationale: list[str] = field(default_factory=list)


def _classify_regime(msi_composite: Optional[float]) -> str:
    """Composite-score sign is the cleanest gamma-regime proxy we have
    in 7AM data: > +0.15 = long-gamma stabilizing; < −0.15 = short-gamma
    destabilizing; the band in between is transition."""
    if msi_composite is None:
        return "transition"
    if msi_composite > 0.15:
        return "long_gamma"
    if msi_composite < -0.15:
        return "short_gamma"
    return "transition"


def _round_to_strike(value: float, step: float) -> float:
    """Round to the nearest strike on the symbol's strike ladder."""
    if step <= 0:
        return round(value, 2)
    return round(round(value / step) * step, 4)


def _select_pin_strike(inp: ForecastInputs) -> Optional[float]:
    """Prefer max_pain when published; fall back to the nearest strike to
    spot. Never invent a value out of nothing — return None if both inputs
    are missing so the page renders 'no pin candidate' honestly."""
    if inp.max_pain is not None:
        return float(inp.max_pain)
    if inp.spot is None:
        return None
    return _round_to_strike(inp.spot, inp.strike_step)


def _range_from_walls(spot: float, call_wall: Optional[float], put_wall: Optional[float]) -> Optional[float]:
    """Half-range in dollar terms inferred from the GEX walls.

    Returns None when either wall is missing — caller falls back to the
    spot-fraction minimum. The half-range is the larger of (call_wall −
    spot) and (spot − put_wall) so the band brackets the structurally
    more-distant wall; we then re-center the band on spot for symmetry."""
    if call_wall is None or put_wall is None:
        return None
    if call_wall <= spot <= put_wall or call_wall <= put_wall:
        # Degenerate: walls are inverted or sandwich spot the wrong way.
        # Falling back to the spot-fraction minimum keeps the model honest.
        return None
    upside = call_wall - spot
    downside = spot - put_wall
    return max(upside, downside)


def compute_forecast(inp: ForecastInputs) -> ForecastResult:
    """Compute today's committed morning forecast.

    Deterministic given inputs — running twice with the same inputs
    produces byte-identical output (used by the writer's content_hash).
    """
    rationale: list[str] = []
    spot = float(inp.spot)
    min_half = spot * MIN_RANGE_FRACTION / 2.0
    max_half = spot * MAX_RANGE_FRACTION / 2.0

    wall_half = _range_from_walls(spot, inp.call_wall, inp.put_wall)
    if wall_half is not None:
        half = wall_half * WALL_EXPANSION
        rationale.append(
            f"Wall-bounded: call ${inp.call_wall:.2f}, put ${inp.put_wall:.2f} → ±${half:.2f} (×{WALL_EXPANSION})"
        )
    else:
        half = min_half
        rationale.append(
            f"Walls missing or inverted — fell back to floor ±{MIN_RANGE_FRACTION:.1%}"
        )

    if inp.is_event_day:
        half *= EVENT_DAY_MULTIPLIER
        rationale.append(f"Event day multiplier ×{EVENT_DAY_MULTIPLIER}")

    half = max(min_half, min(max_half, half))
    projected_low = round(spot - half, 4)
    projected_high = round(spot + half, 4)

    pin_strike = _select_pin_strike(inp)
    if pin_strike is not None:
        if inp.max_pain is not None:
            rationale.append(f"Pin = max_pain ${pin_strike:.2f}")
        else:
            rationale.append(f"Pin = nearest strike to spot ${pin_strike:.2f}")
        # The committed projected_close is the pin strike (clamped into
        # the band) so the receipt has a single number to grade.
        projected_close = min(max(pin_strike, projected_low), projected_high)
    else:
        projected_close = spot
        rationale.append("No pin candidate — projected close = open spot")

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
        regime=regime,
        range_model="heuristic_v1",
        rationale=rationale,
    )
