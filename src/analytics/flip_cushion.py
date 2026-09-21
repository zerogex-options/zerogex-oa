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

What the cushion is measured against
------------------------------------
Points are what a trader reads ("18 points below the flip"), but the STATE is
never classified from points: 18 points of SPX and 18 of SPY are different
conditions, and a threshold in points would silently mean something different
on every symbol.

The scale is a TYPICAL 30-MINUTE REALIZED MOVE. An earlier version used a
fraction of spot, and that was the wrong denominator: it adapts to price level
but not to volatility, so a fixed percentage is a thin cushion on a quiet
morning and a comfortable one on a wild afternoon, while reporting the same
label for both. Measured against how far price actually travels in half an
hour, "thin" means the same thing in both. Bands are in
:data:`THIN_BAND` and friends.

The spot-fraction path survives only as a FALLBACK for bars written before the
move scale existed, and :attr:`CushionBar.basis` always says which was used so
a reading is never silently comparing against a different yardstick.

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
crossing at all). That is not a missing value to paper over: it means there is
no nearby regime boundary, which is its own state (:data:`STATE_NO_FLIP`) and
reads very differently from "the boundary is far away". The two are kept
distinct.

Which is why a NULL arriving here has to be a MEASUREMENT and not a gap. A bar
whose five minutes received no ``gex_summary`` row at all measured nothing, and
the writer resolves it from the last level measured earlier in the same session
rather than passing a NULL down (:mod:`src.analytics.gamma_flip_carry`). This
module cannot make that distinction itself -- by the time a bar is read, the
two look identical -- so it is made once, where the bar is written.
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

# Cushion bands, as multiples of a typical 30-minute realized move. A cushion
# smaller than a quarter of the distance price usually covers in half an hour
# is a boundary price can reach without doing anything unusual.
CROSSING_BAND = 0.25
THIN_BAND = 0.60
NORMAL_BAND = 1.25

#: Fallback band, as a fraction of spot, for bars stored before the move scale
#: existed. Imported rather than copied so it cannot drift from the volatility
#: model's own notion of "near the flip".
FALLBACK_THIN_SPAN = VOL_FLIP_PROX_SPAN
FALLBACK_CROSSING_SPAN = FALLBACK_THIN_SPAN * 0.25

STATE_SECURE = "SECURE"
STATE_NORMAL = "NORMAL"
STATE_THIN = "THIN"
STATE_CROSSING = "CROSSING"
STATE_NO_FLIP = "NO_FLIP"

BASIS_MOVE = "move_30m"
BASIS_SPOT = "spot_fraction"
BASIS_NONE = "none"

#: Rate context over the trailing window. Separate from the cushion state
#: because thin-but-stable and thin-and-collapsing are very different
#: conditions and one label cannot carry both.
RATE_STABLE = "STABLE"
RATE_DRIFTING = "DRIFTING"
RATE_CONTRACTING = "CONTRACTING"
RATE_ACCELERATING = "ACCELERATING"

#: Rate bands, also as multiples of the typical move: how much of a normal
#: half-hour of travel the cushion gave up over the trailing window.
RATE_NOISE_BAND = 0.10
RATE_CONTRACTING_BAND = 0.35

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
    #: Typical 30-minute realized move the cushion was measured against, and
    #: the cushion expressed as a multiple of it. Stored per bar rather than
    #: recomputed, so a historical reading always shows the yardstick that
    #: was actually in force at the time.
    move_30m: Optional[float] = None
    move_ratio: Optional[float] = None
    #: Which yardstick produced ``state``: the move scale, the legacy spot
    #: fraction, or neither. Never leave a reader guessing which.
    basis: str = BASIS_NONE
    #: Trailing-window rate as a multiple of the typical move, and its label.
    #: Separate from ``state`` because thin-but-stable and thin-and-collapsing
    #: are different conditions.
    rate_ratio: Optional[float] = None
    rate_context: Optional[str] = None


def classify(
    distance_frac: Optional[float],
    cushion_pts: Optional[float] = None,
    move_30m: Optional[float] = None,
) -> tuple[str, Optional[float], str]:
    """``(state, move_ratio, basis)`` for one bar.

    Preferred yardstick is the typical 30-minute realized move: a cushion
    smaller than a quarter of the distance price usually covers in half an
    hour is a boundary price can reach without doing anything unusual, and
    that statement survives a change of instrument or a change of regime.

    Falls back to the spot fraction only when no move scale is available,
    which happens for bars stored before it existed. ``basis`` reports which
    was used, because the two are not comparable and a reader must never have
    to guess.

    No flip at all is :data:`STATE_NO_FLIP`, not a secure cushion: "there is
    no boundary" and "the boundary is far away" are different statements.
    """
    if distance_frac is None:
        return STATE_NO_FLIP, None, BASIS_NONE

    if cushion_pts is not None and move_30m is not None and move_30m > 0:
        ratio = cushion_pts / move_30m
        if ratio <= CROSSING_BAND:
            state = STATE_CROSSING
        elif ratio <= THIN_BAND:
            state = STATE_THIN
        elif ratio <= NORMAL_BAND:
            state = STATE_NORMAL
        else:
            state = STATE_SECURE
        return state, ratio, BASIS_MOVE

    magnitude = abs(distance_frac)
    if magnitude <= FALLBACK_CROSSING_SPAN:
        state = STATE_CROSSING
    elif magnitude <= FALLBACK_THIN_SPAN:
        state = STATE_THIN
    else:
        state = STATE_SECURE
    return state, None, BASIS_SPOT


def classify_rate(
    rate_pts: Optional[float], move_30m: Optional[float]
) -> tuple[Optional[float], Optional[str]]:
    """``(rate_ratio, context)``: is the cushion noise, drifting, contracting,
    or collapsing?

    Scaled by the same typical move as the cushion itself, so "contracting"
    means the same thing on every symbol. Only narrowing is graded past
    drifting: a cushion opening up quickly is not a risk condition, and giving
    it an urgent-sounding label would be noise dressed as a warning.
    """
    if rate_pts is None or move_30m is None or move_30m <= 0:
        return None, None

    ratio = rate_pts / move_30m
    magnitude = abs(ratio)
    if magnitude <= RATE_NOISE_BAND:
        return ratio, RATE_STABLE
    if ratio > 0:
        return ratio, RATE_DRIFTING
    if magnitude <= RATE_CONTRACTING_BAND:
        return ratio, RATE_CONTRACTING
    return ratio, RATE_ACCELERATING


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
    bars: Sequence[tuple],
    rate_bars: int = DEFAULT_RATE_BARS,
) -> List[CushionBar]:
    """Build the cushion series from chronological tuples.

    Each entry is ``(bar_start, spot, flip)`` or ``(bar_start, spot, flip,
    move_30m)``. The three-element form classifies on the legacy spot fraction
    and is what bars stored before the move scale existed look like.

    Every derived value looks only BACKWARD, so a bar's reading is identical
    whether computed live or from a completed session. The panel is meant to
    be read during the session; a rate that needed later bars could not be.
    """
    out: List[CushionBar] = []
    cushions: List[Optional[float]] = []

    for i, entry in enumerate(bars):
        bar_start, spot, flip = entry[0], entry[1], entry[2]
        move_30m = entry[3] if len(entry) > 3 else None
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

        state, move_ratio, basis = classify(distance_frac, cushion_pts, move_30m)
        rate_ratio, rate_context = classify_rate(rate_pts, move_30m)

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
                state=state,
                move_30m=move_30m,
                move_ratio=move_ratio,
                basis=basis,
                rate_ratio=rate_ratio,
                rate_context=rate_context,
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
        # Prefer the scaled context over the raw "accelerating" flag: it says
        # how much of a normal half-hour of travel was given up, which is the
        # difference between thin-but-stable and thin-and-collapsing.
        if bar.rate_context and bar.rate_context != RATE_STABLE:
            tail = f", {bar.rate_context.lower()}"
        elif bar.rate_context == RATE_STABLE:
            tail = ", stable"
        else:
            tail = ", accelerating" if bar.accelerating else ""
        parts.append(f"{window}: {word} {abs(bar.rate_pts):.0f} pts{tail}")

    return " | ".join(parts)
