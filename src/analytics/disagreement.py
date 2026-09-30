"""Does pressure win when structure says it should not?

The question Barrie posed, made precise enough to count.

Structure and pressure disagree all the time. The panel's whole claim is that
structure says whether a move can persist, so the interesting case is the one
where pressure pushes anyway: confirmed buying into a book that wants to
contain, or confirmed selling into one. If pressure wins there more often than
it wins in general, the disagreement is information. If it does not, the panel
should stop implying that it is.

Contain versus extend, not up versus down
-----------------------------------------
Only Lean has a side. Stability is pinning or accelerative and Gamma Trend is
building or thinning, and both of those say whether structure dampens a move or
permits it, not which way it leans. So all three vote on one axis, relative to
pressure's own direction:

    pinning contains, accelerative extends
    building contains, thinning extends
    lean contains when it opposes pressure, extends when it is with it

FLAT abstains. The majority of whoever is left decides, and it takes two: one
vote with two abstentions is not a structure read, it is a single component
wearing the word.

No look-ahead, which is the whole point
---------------------------------------
A bar's stance is known once that bar has printed, so the earliest price anyone
could act on is the NEXT bar's. Windows therefore start one bar after the
onset. Measuring from the onset bar's own spot would score the study on a price
that was already history when the signal appeared, and would flatter it exactly
where the move began inside the signal bar.

Onsets only. A disagreement that holds for an hour is one observation, not
twelve: overlapping windows would multiply the same event into a sample size
the data does not have.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from src.analytics import flip_cushion as fc
from src.analytics import gamma_weather as gw

#: What structure wants to do to a move, relative to pressure's direction.
CONTAIN = "CONTAIN"
EXTEND = "EXTEND"

#: A bar's standing in the study.
DISAGREE = "DISAGREE"
AGREE = "AGREE"
NEITHER = "NEITHER"

#: Votes needed before a structure read exists at all.
MIN_VOTES = 2

#: How far the move is measured, and how big it has to be. The unit is the
#: session's own typical 30-minute move, which is already what the flip cushion
#: is banded against, so the whole panel speaks one language.
HORIZON_MINUTES = 30.0
DEFAULT_EXTENSION = 1.0

#: Thresholds the report sweeps, so a reader can see whether a result is a
#: finding or an artifact of where the bar was set.
EXTENSION_SWEEP = (0.5, 1.0, 1.5)


@dataclass(frozen=True)
class DisagreementBar:
    """One bar, reduced to what the question needs."""

    #: BUYING or SELLING once confirmed, else None.
    pressure: Optional[str]
    #: CONTAIN, EXTEND, or None when no two components agree.
    structure: Optional[str]
    spot: Optional[float]
    #: The yardstick in force at this bar, never a later one.
    typical_move: Optional[float]
    cushion_state: str = fc.STATE_NO_FLIP
    #: Negative is narrowing. None where there is not enough history.
    cushion_rate: Optional[float] = None

    @property
    def stance(self) -> str:
        if self.pressure is None or self.structure is None:
            return NEITHER
        return DISAGREE if self.structure == CONTAIN else AGREE


def confirmed_pressure(pressure: str, persistence: str) -> Optional[str]:
    """The side, once it has earned the name.

    A Pulse is one bar of push and does not count as a disagreement; that was
    explicit in the spec and it is the difference between measuring a condition
    and measuring noise. MIXED has no side to disagree with.
    """
    if pressure not in (gw.PRESSURE_BUYING, gw.PRESSURE_SELLING):
        return None
    if persistence == gw.PERSISTENCE_PULSE:
        return None
    return pressure


def structure_stance(
    lean: Optional[str],
    stability: str,
    gamma_trend: str,
    pressure: Optional[str],
) -> Optional[str]:
    """CONTAIN or EXTEND, or None when fewer than two components agree."""
    if pressure is None:
        return None

    votes: List[str] = []

    # Stability and gamma trend are already on this axis: pinning and building
    # dampen a move, accelerative and thinning permit one. FLAT abstains.
    for value in (stability, gamma_trend):
        if value == gw.STRUCTURE_PINNING:
            votes.append(CONTAIN)
        elif value == gw.STRUCTURE_ACCELERATIVE:
            votes.append(EXTEND)

    # Lean is the only component with a side, so it is read against pressure's.
    # A supportive book resists selling and helps buying; a capping one is the
    # mirror. Nothing here is a view on direction, only on whether the book is
    # in pressure's way.
    if lean == gw.LEAN_SUPPORTIVE:
        votes.append(EXTEND if pressure == gw.PRESSURE_BUYING else CONTAIN)
    elif lean == gw.LEAN_CAPPING:
        votes.append(CONTAIN if pressure == gw.PRESSURE_BUYING else EXTEND)

    contain = votes.count(CONTAIN)
    extend = votes.count(EXTEND)
    if contain >= MIN_VOTES and contain > extend:
        return CONTAIN
    if extend >= MIN_VOTES and extend > contain:
        return EXTEND
    return None


def onsets(bars: Sequence[DisagreementBar], stance: str) -> List[int]:
    """Bars where `stance` begins, so one episode counts once.

    A run that holds for an hour is one observation. Anchoring every bar of it
    would turn twelve overlapping windows into twelve trials and hand back a
    sample size the session does not contain.
    """
    out: List[int] = []
    previous: Optional[str] = None
    for i, bar in enumerate(bars):
        current = bar.stance
        if current == stance and previous != stance:
            out.append(i)
        previous = current
    return out


def extension(
    bars: Sequence[DisagreementBar],
    anchor: int,
    horizon_bars: int,
    threshold: float = DEFAULT_EXTENSION,
) -> Optional[bool]:
    """Did price extend on pressure's side over the horizon?

    Measured from the bar AFTER the anchor, which is the first price anyone
    could have acted on, to that bar plus the horizon. Returns None when the
    window runs past the session, when either endpoint has no spot, or when the
    bar carries no yardstick to measure against: an unresolved observation is
    not a loss, and counting it as one would understate every rate here.
    """
    entry = anchor + 1
    exit_ = entry + horizon_bars
    if anchor < 0 or exit_ >= len(bars):
        return None

    start, end = bars[entry].spot, bars[exit_].spot
    side = bars[anchor].pressure
    unit = bars[anchor].typical_move
    if start is None or end is None or side is None:
        return None
    if unit is None or unit <= 0:
        return None

    move = end - start
    toward = move if side == gw.PRESSURE_BUYING else -move
    return toward >= threshold * unit


def cushion_band(bar: DisagreementBar) -> Optional[str]:
    """SECURE, THIN_NARROWING, or None for anything in between.

    The second cut, and deliberately not a partition: the question is whether
    pressure only wins when the cushion was already collapsing, which the
    middle of the range cannot answer either way. NORMAL, a thin cushion that
    is widening, and a session with no flip are all left out rather than
    forced into one side.
    """
    if bar.cushion_state == fc.STATE_SECURE:
        return "secure"
    if bar.cushion_state == fc.STATE_THIN and bar.cushion_rate is not None and bar.cushion_rate < 0:
        return "thin, narrowing"
    return None


def trials(
    sessions: Sequence[Sequence[DisagreementBar]],
    horizon_bars: int,
    threshold: float = DEFAULT_EXTENSION,
) -> Dict[str, List[Optional[bool]]]:
    """Outcomes per stance, pooled over sessions, onsets only.

    The AGREE arm is the control. Without it the lift has nothing to be a lift
    against, because a disagreement that extends 40% of the time says nothing
    until you know what the same pressure does when structure is with it.
    """
    out: Dict[str, List[Optional[bool]]] = {DISAGREE: [], AGREE: []}
    for bars in sessions:
        for stance in (DISAGREE, AGREE):
            for anchor in onsets(bars, stance):
                out[stance].append(extension(bars, anchor, horizon_bars, threshold))
    return out


def trials_by_cushion(
    sessions: Sequence[Sequence[DisagreementBar]],
    horizon_bars: int,
    threshold: float = DEFAULT_EXTENSION,
) -> Dict[str, List[Optional[bool]]]:
    """The same outcomes, split four ways by stance and cushion band."""
    out: Dict[str, List[Optional[bool]]] = {}
    for bars in sessions:
        for stance in (DISAGREE, AGREE):
            for anchor in onsets(bars, stance):
                band = cushion_band(bars[anchor])
                if band is None:
                    continue
                out.setdefault(f"{stance} · {band}", []).append(
                    extension(bars, anchor, horizon_bars, threshold)
                )
    return out
