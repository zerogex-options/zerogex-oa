"""Barrie's four-light scan strip: Agree, Heads-up, Fragile, Stand down.

Not a signal engine and not a trade recommendation. Four relationships that
Gamma Weather already reads, surfaced as a row of lights so the question
"does this moment match a relationship I trust?" can be answered at a glance
instead of by reading five chips and comparing them in your head.

Here rather than in the panel for one reason: the base-rate report grades the
rules the panel runs, never a lookalike. Heads-up is nearly the disagreement
condition :mod:`src.analytics.disagreement` already measures, and a second
implementation in TypeScript would drift from the one being graded. Lights are
derived on read from a :class:`gamma_weather.Weather`; nothing here changes,
overrides or recalculates the state.

THE TRAP, stated once because it would silently disable two of the four rules.
``gamma_trend`` is classified by ``classify_structure``, so its vocabulary is
PINNING / ACCELERATIVE / FLAT, NOT the "building" and "thinning" the panel
prints. Building IS pinning, thinning IS accelerative. A rule written against
the displayed words would never fire.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional

from src.analytics import gamma_weather as gw
from src.analytics.flip_cushion import STATE_CROSSING, STATE_SECURE, STATE_THIN

#: Lean, stability and gamma trend each point at one of two structural
#: stories, or abstain. The names are the sides Barrie's spec names.
SIDE_SUPPORTIVE = "SUPPORTIVE"
SIDE_CAPPING = "CAPPING"

#: Confirmed pressure, for Heads-up. A Pulse is the first evidence rather than
#: a condition, and Reversed is a moment rather than a direction, so neither
#: may light it on its own. Barrie's guardrail, as an allowlist so a new
#: persistence rung has to be considered rather than silently included.
CONFIRMED_PERSISTENCE = (gw.PERSISTENCE_BUILDING, gw.PERSISTENCE_PERSISTENT)

#: A cushion that is already close to the boundary. Agree will not light over
#: one, and it is half of what Fragile looks for.
CLOSE_BANDS = (STATE_THIN, STATE_CROSSING)


@dataclass(frozen=True)
class ScanLights:
    """Which lights are green. Any combination is legal, including none."""

    agree: bool
    heads_up: bool
    fragile: bool
    stand_down: bool

    def as_dict(self) -> dict:
        return asdict(self)


def structural_side(value: Optional[str], supportive: str, capping: str) -> Optional[str]:
    """One component's vote, or None when it abstains."""
    if value == supportive:
        return SIDE_SUPPORTIVE
    if value == capping:
        return SIDE_CAPPING
    return None


def structural_votes(weather: gw.Weather) -> tuple:
    """(supportive votes, capping votes) from lean, stability and gamma trend.

    FLAT abstains rather than counting for either side, and a missing lean
    abstains too. Three voters, so the counts run 0..3 and sum to at most 3.
    """
    votes = [
        structural_side(weather.lean_side, gw.LEAN_SUPPORTIVE, gw.LEAN_CAPPING),
        structural_side(weather.structure, gw.STRUCTURE_PINNING, gw.STRUCTURE_ACCELERATIVE),
        # Building is PINNING, thinning is ACCELERATIVE. See the module docstring.
        structural_side(weather.gamma_trend, gw.STRUCTURE_PINNING, gw.STRUCTURE_ACCELERATIVE),
    ]
    return votes.count(SIDE_SUPPORTIVE), votes.count(SIDE_CAPPING)


def agree(weather: gw.Weather) -> bool:
    """The book agrees with itself. Never "take the trade".

    Three of three, which is Barrie's call over the simple majority the spec
    first allowed: "I would rather it be rarer and clean than turn green on a
    simple majority." So lean, stability and gamma trend must all tell the same
    structural story, with none of them abstaining.

    A candidate state waiting on confirmation does not block this. The rule
    reads the components behind the CONFIRMED header, and a forming chip is
    information about what might come next rather than a reason to withhold a
    relationship that holds right now.
    """
    if weather.state == gw.STATE_MIXED:
        return False
    # Not over a cushion that is already thin, or thin and contracting hard.
    if weather.cushion_band in CLOSE_BANDS:
        return False
    if weather.cushion == gw.CUSHION_TRANSITION_RISK:
        return False
    supportive, capping = structural_votes(weather)
    return supportive == 3 or capping == 3


def heads_up(weather: gw.Weather) -> bool:
    """Confirmed pressure pressing against a book that still looks secure.

    Buying into a capping book or selling into a supportive one. The meaning is
    restraint, not prediction: do not casually fade a level because structure
    appears to contain price. Measured over 42 sessions this condition extended
    at 0.96x against everything else on a secure cushion, which is no edge in
    either direction -- and that is exactly why the light says "look again"
    rather than "pressure wins".
    """
    if weather.persistence not in CONFIRMED_PERSISTENCE:
        return False
    if weather.cushion_band != STATE_SECURE:
        return False
    if weather.pressure == gw.PRESSURE_BUYING:
        return weather.lean_side == gw.LEAN_CAPPING
    if weather.pressure == gw.PRESSURE_SELLING:
        return weather.lean_side == gw.LEAN_SUPPORTIVE
    return False


def fragile(weather: gw.Weather) -> bool:
    """The level may not behave reliably. Not a directional call.

    Either the cushion is thin and closing, or the book is accelerative and
    thinning at the same time.

    Note the overlap, which is a property of the rules rather than a bug: the
    second arm is exactly the structural pair Agree's capping side requires, so
    a capping Agree always carries Fragile with it. Raised with Barrie once it
    showed up, and kept deliberately. His reasoning, which is better than the
    one this docstring used to carry:

        "I don't see agree and fragile together on the capping side as a
        conflict. Agree is saying the book is coherent and Fragile is saying
        that coherent structure is not the stable/pinning kind, so the level
        may not behave reliably. That's useful to see together."

    So the two lights are never independent on that side, and that is the
    intended reading rather than something to decouple.
    """
    if weather.cushion_band in CLOSE_BANDS and weather.cushion in (
        gw.CUSHION_NARROWING,
        gw.CUSHION_TRANSITION_RISK,
    ):
        return True
    return (
        weather.structure == gw.STRUCTURE_ACCELERATIVE
        and weather.gamma_trend == gw.STRUCTURE_ACCELERATIVE
    )


def stand_down(weather: gw.Weather) -> bool:
    """The structural picture is not coherent enough for a clean read.

    Mixed, or no two of the three components agreeing at all. Deliberately NOT
    the complement of Agree: a two-of-three read with the third disagreeing
    lights neither, which is Barrie's middle state where the chips and the
    chart are worth a closer look.

    Structural conflict only. Nothing here reads price.
    """
    if weather.state == gw.STATE_MIXED:
        return True
    supportive, capping = structural_votes(weather)
    return max(supportive, capping) < 2


def scan_lights(weather: gw.Weather) -> ScanLights:
    """All four, for one bar."""
    return ScanLights(
        agree=agree(weather),
        heads_up=heads_up(weather),
        fragile=fragile(weather),
        stand_down=stand_down(weather),
    )
