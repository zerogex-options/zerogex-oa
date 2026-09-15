"""Flip cushion -- how much room price has before the gamma regime changes.

The gamma flip is the spot level where dealer gamma crosses zero: above it
dealers hedge against moves and the tape pins, below it they hedge with moves
and it accelerates. Everything else on the Hedging Flow page describes what is
happening inside a regime. This describes how close the regime itself is to
changing, which is a different and often more urgent question.

Four readings, per the Phase 1 spec:

* the current distance from spot to the flip, with which side we are on;
* the direction this bar -- widening or narrowing;
* a 15-minute rolling rate, which separates a sustained convergence from an
  ordinary wobble. Note that the 15-minute view is NOT a slower feed: it is a
  rolling window recomputed on every 5-minute bar;
* a state label: secure, thin, or crossing risk.

Points versus fraction
----------------------
Distance is carried BOTH ways and they are not interchangeable. Points are
what a trader reads ("18 points below the flip"); the fraction is what the
state label is computed from, because 18 points of SPX and 18 points of SPY
are completely different conditions and a threshold in points would mean
something different on every symbol.

The fraction is ``(spot - flip) / spot``, matching
``src.jobs.forecast_range_model``'s ``flip_distance``, and the "near" boundary
is that module's :data:`VOL_FLIP_PROX_SPAN` imported directly rather than
copied. There is already a calibrated house answer to "how close is close";
a second, independently drifting one would be worse than no answer.

Sign conventions
----------------
``distance_pts`` and ``distance_frac`` are SIGNED (positive = spot above the
flip). ``cushion_pts`` is the absolute room before crossing, which is what
"narrowing" and "widening" refer to. A cushion narrowing from above and one
narrowing from below are the same event for the purposes of this panel: price
is converging on the boundary.

No flip, no cushion
-------------------
``gamma_flip_point`` is NULL when the dealer-gamma profile is one-signed (no
crossing at all) or degraded. That is not a missing value to paper over: it
means there is no nearby regime boundary, which is its own state
(:data:`STATE_NO_FLIP`) and reads very differently from "the boundary is far
away". The two are kept distinct.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence

# The calibrated "near the flip" span, as a fraction of spot. Imported rather
# than redefined so this module and the volatility model cannot drift apart on
# what counts as close. That module is pure stdlib, so the import is cheap.
from src.jobs.forecast_range_model import VOL_FLIP_PROX_SPAN

#: Rolling window for the cushion rate, in 5-minute bars. Three bars = 15
#: minutes, which the spec defines as a rolling smoothing recomputed on every
#: 5-minute update rather than a slower feed.
DEFAULT_RATE_BARS = 3

#: |fraction| at or inside which the flip is close enough to be a live risk.
THIN_SPAN = VOL_FLIP_PROX_SPAN

#: |fraction| at or inside which a crossing is imminent rather than merely
#: possible. Expressed as a share of THIN_SPAN so there is ONE number to tune:
#: move the house span and both boundaries move together.
CROSSING_SHARE = 0.25
CROSSING_SPAN = THIN_SPAN * CROSSING_SHARE

STATE_SECURE = "SECURE"
STATE_THIN = "THIN"
STATE_CROSSING = "CROSSING"
STATE_NO_FLIP = "NO_FLIP"

SIDE_ABOVE = "above"
SIDE_BELOW = "below"


@dataclass(frozen=True)
class CushionBar:
    """One 5-minute reading of the spot-to-flip cushion.

    ``step_pts`` and ``rate_pts`` are changes in ``cushion_pts``: positive is
    widening, negative is narrowing. ``step_pts`` is this bar alone;
    ``rate_pts`` is the trailing window. Both are ``None`` where there is not
    enough history, or where either endpoint had no flip -- a cushion cannot
    be said to have narrowed from a bar that had no boundary to narrow toward.

    ``accelerating`` is only meaningful while narrowing, and is ``None``
    otherwise rather than ``False``: "not accelerating" and "not narrowing at
    all" are different statements and a chart should not conflate them.
    """

    bar_start: datetime
    spot: Optional[float]
    flip: Optional[float]
    distance_pts: Optional[float]
    distance_frac: Optional[float]
    cushion_pts: Optional[float]
    side: Optional[str]
    step_pts: Optional[float]
    rate_pts: Optional[float]
    accelerating: Optional[bool]
    state: str


def classify(distance_frac: Optional[float]) -> str:
    """State label from the signed distance fraction.

    Classified on the fraction, never on points -- see the module docstring.
    ``None`` means there was no flip to measure against, which is
    :data:`STATE_NO_FLIP` rather than a secure cushion.
    """
    if distance_frac is None:
        return STATE_NO_FLIP
    magnitude = abs(distance_frac)
    if magnitude <= CROSSING_SPAN:
        return STATE_CROSSING
    if magnitude <= THIN_SPAN:
        return STATE_THIN
    return STATE_SECURE


def measure(
    spot: Optional[float], flip: Optional[float]
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    """(distance_pts, distance_frac, cushion_pts, side) for one bar.

    Returns all-``None`` when either input is missing or spot is non-positive.
    A non-positive spot would make the fraction meaningless rather than merely
    large, so it is refused instead of divided by.
    """
    if spot is None or flip is None or spot <= 0:
        return None, None, None, None
    distance_pts = float(spot) - float(flip)
    distance_frac = distance_pts / float(spot)
    side = SIDE_ABOVE if distance_pts >= 0 else SIDE_BELOW
    return distance_pts, distance_frac, abs(distance_pts), side


def build_series(
    bars: Sequence[tuple[datetime, Optional[float], Optional[float]]],
    rate_bars: int = DEFAULT_RATE_BARS,
) -> List[CushionBar]:
    """Build the cushion series from chronological ``(bar_start, spot, flip)``.

    Every derived value looks only BACKWARD, so a bar's reading is identical
    whether computed live or from a completed session. The panel is meant to
    be read during the session; a rate that needed later bars could not be.
    """
    out: List[CushionBar] = []
    cushions: List[Optional[float]] = []

    for i, (bar_start, spot, flip) in enumerate(bars):
        distance_pts, distance_frac, cushion_pts, side = measure(spot, flip)

        prev = cushions[-1] if cushions else None
        step_pts = cushion_pts - prev if cushion_pts is not None and prev is not None else None

        rate_pts = None
        accelerating = None
        if cushion_pts is not None and rate_bars > 0 and i >= rate_bars:
            base = cushions[i - rate_bars]
            if base is not None:
                rate_pts = cushion_pts - base
                if rate_pts < 0 and step_pts is not None and step_pts < 0:
                    # Narrowing. It is accelerating when the newest bar gave up
                    # more room than the window's average bar did.
                    mean_step = abs(rate_pts) / rate_bars
                    accelerating = abs(step_pts) > mean_step

        cushions.append(cushion_pts)
        out.append(
            CushionBar(
                bar_start=bar_start,
                spot=spot,
                flip=flip,
                distance_pts=distance_pts,
                distance_frac=distance_frac,
                cushion_pts=cushion_pts,
                side=side,
                step_pts=step_pts,
                rate_pts=rate_pts,
                accelerating=accelerating,
                state=classify(distance_frac),
            )
        )

    return out


def describe(bar: CushionBar, rate_bars: int = DEFAULT_RATE_BARS) -> str:
    """The one-line read, in the shape the spec asks for.

    e.g. ``Flip cushion: 18 pts below, THIN | 5m: narrowing 4 pts |
    15m: narrowing 11 pts, accelerating``
    """
    if bar.state == STATE_NO_FLIP or bar.cushion_pts is None:
        return "Flip cushion: no gamma flip in the profile"

    parts = [f"Flip cushion: {bar.cushion_pts:.0f} pts {bar.side}, {bar.state}"]

    if bar.step_pts is not None and bar.step_pts != 0:
        word = "widening" if bar.step_pts > 0 else "narrowing"
        parts.append(f"5m: {word} {abs(bar.step_pts):.0f} pts")

    if bar.rate_pts is not None and bar.rate_pts != 0:
        word = "widening" if bar.rate_pts > 0 else "narrowing"
        window = f"{rate_bars * 5}m"
        tail = ", accelerating" if bar.accelerating else ""
        parts.append(f"{window}: {word} {abs(bar.rate_pts):.0f} pts{tail}")

    return " | ".join(parts)
