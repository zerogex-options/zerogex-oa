"""The four-light scan strip, against the rules Barrie wrote.

Each rule is a way the strip could mislead, so each gets a test that fails if
the rule loosens: Agree turning green on a majority, Heads-up firing on a
pulse, Stand down and Agree between them leaving no middle state, or any light
reading the displayed wording instead of the stored vocabulary.
"""

from __future__ import annotations

from src.analytics import gamma_weather as gw
from src.analytics import scan_lights as sl
from src.analytics.flip_cushion import STATE_CROSSING, STATE_NORMAL, STATE_SECURE, STATE_THIN


def _weather(**over) -> gw.Weather:
    """A bar with nothing lit, so each test turns on only what it is about."""
    base = dict(
        state=gw.STATE_STABLE_BID,
        label="Stable bid",
        pressure=gw.PRESSURE_MIXED,
        structure=gw.STRUCTURE_FLAT,
        gamma_trend=gw.STRUCTURE_FLAT,
        lean_side=None,
        cushion=gw.CUSHION_STEADY,
        sentence="",
        persistence=gw.PERSISTENCE_PULSE,
        cushion_band=STATE_SECURE,
    )
    base.update(over)
    return gw.Weather(**base)


def _supportive(**over) -> gw.Weather:
    """All three components telling the supportive story, before overrides."""
    side = {
        "lean_side": gw.LEAN_SUPPORTIVE,
        "structure": gw.STRUCTURE_PINNING,
        "gamma_trend": gw.STRUCTURE_PINNING,
    }
    side.update(over)
    return _weather(**side)


def _capping(**over) -> gw.Weather:
    side = {
        "lean_side": gw.LEAN_CAPPING,
        "structure": gw.STRUCTURE_ACCELERATIVE,
        "gamma_trend": gw.STRUCTURE_ACCELERATIVE,
    }
    side.update(over)
    return _weather(**side)


# --------------------------------------------------------------------------- #
# Agree.
# --------------------------------------------------------------------------- #


def test_agree_needs_three_of_three_not_a_majority():
    """Barrie's call: rarer and clean beats green on a simple majority."""
    assert sl.agree(_supportive()) is True

    # Two of three, the third disagreeing.
    assert sl.agree(_supportive(gamma_trend=gw.STRUCTURE_ACCELERATIVE)) is False
    # Two of three, the third abstaining.
    assert sl.agree(_supportive(gamma_trend=gw.STRUCTURE_FLAT)) is False
    assert sl.agree(_supportive(lean_side=None)) is False


def test_agree_reads_the_stored_vocabulary_not_the_printed_words():
    """Gamma trend is classified by classify_structure, so "building" is
    PINNING. A rule written against the printed word would never fire, and
    this test is the one that would catch it."""
    bar = _supportive()

    assert bar.gamma_trend == gw.STRUCTURE_PINNING
    assert sl.agree(bar) is True
    # The displayed word is not a value the classifier can emit.
    assert sl.agree(_supportive(gamma_trend="BUILDING")) is False


def test_agree_holds_off_over_a_cushion_that_is_already_close():
    for band in (STATE_THIN, STATE_CROSSING):
        assert sl.agree(_supportive(cushion_band=band)) is False
    assert sl.agree(_supportive(cushion=gw.CUSHION_TRANSITION_RISK)) is False
    # A merely normal cushion is not a reason to withhold it.
    assert sl.agree(_supportive(cushion_band=STATE_NORMAL)) is True


def test_agree_never_lights_on_mixed():
    assert sl.agree(_supportive(state=gw.STATE_MIXED)) is False


def test_a_forming_candidate_does_not_block_agree():
    """Barrie's guardrail. The rule reads the components behind the confirmed
    header; a candidate waiting on confirmation is information about what may
    come next, not a reason to withhold a relationship that holds now."""
    forming = _supportive(pending_state=gw.STATE_UNSTABLE, pending_label="Unstable", pending_bars=1)

    assert sl.agree(forming) is True


# --------------------------------------------------------------------------- #
# Heads-up.
# --------------------------------------------------------------------------- #


def test_heads_up_is_confirmed_pressure_against_a_secure_book():
    buying_into_capping = _weather(
        pressure=gw.PRESSURE_BUYING,
        lean_side=gw.LEAN_CAPPING,
        persistence=gw.PERSISTENCE_PERSISTENT,
    )
    selling_into_supportive = _weather(
        pressure=gw.PRESSURE_SELLING,
        lean_side=gw.LEAN_SUPPORTIVE,
        persistence=gw.PERSISTENCE_BUILDING,
    )

    assert sl.heads_up(buying_into_capping) is True
    assert sl.heads_up(selling_into_supportive) is True

    # Pressure going WITH the book is not a heads-up.
    assert (
        sl.heads_up(
            _weather(
                pressure=gw.PRESSURE_BUYING,
                lean_side=gw.LEAN_SUPPORTIVE,
                persistence=gw.PERSISTENCE_PERSISTENT,
            )
        )
        is False
    )


def test_a_pulse_or_a_reversal_alone_never_lights_heads_up():
    """The guardrail Barrie wrote twice. A pulse is the first evidence rather
    than a condition, and a reversal is a moment rather than a direction."""
    for rung in (gw.PERSISTENCE_PULSE, gw.PERSISTENCE_REVERSED):
        bar = _weather(
            pressure=gw.PRESSURE_BUYING,
            lean_side=gw.LEAN_CAPPING,
            persistence=rung,
        )
        assert sl.heads_up(bar) is False, rung


def test_heads_up_wants_the_book_still_looking_secure():
    """The point is pressure running a wall that still looks like a wall. Once
    the cushion is thin, Fragile is the light that has something to say."""
    thin = _weather(
        pressure=gw.PRESSURE_BUYING,
        lean_side=gw.LEAN_CAPPING,
        persistence=gw.PERSISTENCE_PERSISTENT,
        cushion_band=STATE_THIN,
    )

    assert sl.heads_up(thin) is False


# --------------------------------------------------------------------------- #
# Fragile.
# --------------------------------------------------------------------------- #


def test_fragile_catches_a_closing_cushion_or_an_accelerative_thinning_book():
    assert sl.fragile(_weather(cushion_band=STATE_THIN, cushion=gw.CUSHION_NARROWING)) is True
    assert (
        sl.fragile(_weather(cushion_band=STATE_CROSSING, cushion=gw.CUSHION_TRANSITION_RISK))
        is True
    )
    assert (
        sl.fragile(
            _weather(
                structure=gw.STRUCTURE_ACCELERATIVE,
                gamma_trend=gw.STRUCTURE_ACCELERATIVE,
            )
        )
        is True
    )

    # Thin but widening is a fact about where price is, not a warning.
    assert sl.fragile(_weather(cushion_band=STATE_THIN, cushion=gw.CUSHION_WIDENING)) is False
    # Accelerative now while the session has built is only half of it.
    assert (
        sl.fragile(
            _weather(
                structure=gw.STRUCTURE_ACCELERATIVE,
                gamma_trend=gw.STRUCTURE_PINNING,
            )
        )
        is False
    )


def test_a_capping_agree_always_carries_fragile():
    """Not a bug, and pinned so nobody "fixes" it into one.

    Fragile's second arm is exactly the structural pair Agree's capping side
    requires, so the two are never independent on that side. This was put to
    Barrie as a question once it surfaced, and he chose to keep it:

        "I don't see agree and fragile together on the capping side as a
        conflict. Agree is saying the book is coherent and Fragile is saying
        that coherent structure is not the stable/pinning kind, so the level
        may not behave reliably. That's useful to see together."

    Decoupling them would need a new conversation with him, not a refactor.
    """
    bar = _capping()

    assert sl.agree(bar) is True
    assert sl.fragile(bar) is True
    # The supportive side has no such coupling.
    assert sl.agree(_supportive()) is True
    assert sl.fragile(_supportive()) is False


# --------------------------------------------------------------------------- #
# Stand down, and the middle state between it and Agree.
# --------------------------------------------------------------------------- #


def test_stand_down_is_mixed_or_no_two_of_three_agreement():
    assert sl.stand_down(_weather(state=gw.STATE_MIXED)) is True
    # One voter, two abstaining.
    assert sl.stand_down(_weather(lean_side=gw.LEAN_SUPPORTIVE)) is True
    # One each way, the third abstaining.
    assert (
        sl.stand_down(_weather(lean_side=gw.LEAN_SUPPORTIVE, structure=gw.STRUCTURE_ACCELERATIVE))
        is True
    )
    assert sl.stand_down(_supportive()) is False


def test_two_of_three_lights_neither_agree_nor_stand_down():
    """Barrie's middle state, and the reason Stand down is not Agree's
    complement: "that leaves a middle state where the chips/chart need a closer
    look". If either rule ever widens, this is the test that goes red."""
    split = _supportive(gamma_trend=gw.STRUCTURE_ACCELERATIVE)

    assert sl.agree(split) is False
    assert sl.stand_down(split) is False


def test_nothing_is_lit_on_an_empty_read():
    lights = sl.scan_lights(_weather())

    assert lights.agree is False
    assert lights.heads_up is False
    assert lights.fragile is False
    # Flat everywhere is no agreement at all, which is what Stand down is for.
    assert lights.stand_down is True


def test_more_than_one_light_can_be_on():
    """Explicitly allowed by the spec, and the strip must not pick a winner."""
    bar = _capping(
        pressure=gw.PRESSURE_BUYING,
        persistence=gw.PERSISTENCE_PERSISTENT,
    )
    lights = sl.scan_lights(bar)

    assert lights.agree is True
    assert lights.fragile is True
    assert lights.heads_up is True
    assert lights.stand_down is False
