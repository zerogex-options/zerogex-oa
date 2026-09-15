"""Gamma Weather -- the combined current-state read.

The spec left precedence open, so most of what is pinned here is precedence:
that every combination of inputs lands somewhere, that MIXED means what the
spec says it means rather than becoming a dumping ground, and that the cushion
qualifies a state instead of competing with it.
"""

from __future__ import annotations

import itertools

from src.analytics.gamma_weather import (
    CUSHION_NARROWING,
    CUSHION_NONE,
    CUSHION_STEADY,
    CUSHION_TRANSITION_RISK,
    CUSHION_WIDENING,
    LEAN_CAPPING,
    LEAN_SUPPORTIVE,
    PRESSURE_BUYING,
    PRESSURE_FLOOR_USD,
    PRESSURE_MIXED,
    PRESSURE_SELLING,
    STABILITY_FLAT_BAND_USD,
    STATE_FRAGILE_RALLY,
    STATE_LABELS,
    STATE_MIXED,
    STATE_STABLE_BID,
    STATE_SUPPORTED_DIP,
    STATE_UNSTABLE,
    STRUCTURE_ACCELERATIVE,
    STRUCTURE_FLAT,
    STRUCTURE_PINNING,
    WeatherInputs,
    classify,
    classify_cushion,
    classify_pressure,
    classify_structure,
)

BIG = PRESSURE_FLOOR_USD * 10
STRONG = STABILITY_FLAT_BAND_USD * 10


def _inputs(**kw) -> WeatherInputs:
    base = dict(pressure_bar=BIG, pressure_avg=BIG, lean=STRONG, stability=STRONG)
    base.update(kw)
    return WeatherInputs(**base)


# --------------------------------------------------------------------------- #
# The four states the spec names, from the spec's own definitions
# --------------------------------------------------------------------------- #
def test_buying_into_a_firming_book_is_a_stable_bid():
    assert classify(_inputs()).state == STATE_STABLE_BID


def test_selling_into_a_firming_book_is_a_supported_dip():
    assert classify(_inputs(pressure_bar=-BIG, pressure_avg=-BIG)).state == STATE_SUPPORTED_DIP


def test_buying_into_a_thinning_book_is_a_fragile_rally():
    assert classify(_inputs(stability=-STRONG, lean=-STRONG)).state == STATE_FRAGILE_RALLY


def test_selling_into_a_thinning_book_is_unstable():
    got = classify(_inputs(pressure_bar=-BIG, pressure_avg=-BIG, stability=-STRONG, lean=-STRONG))
    assert got.state == STATE_UNSTABLE


# --------------------------------------------------------------------------- #
# Coverage: nothing falls through, and MIXED is not a dumping ground
# --------------------------------------------------------------------------- #
def test_every_combination_of_inputs_produces_a_state():
    """The gap the spec left. Four dozen combinations onto a handful of states
    with no precedence rule is how a panel ends up blank most of the day."""
    pressures = [(BIG, BIG), (-BIG, -BIG), (BIG, -BIG), (None, None), (0.0, 0.0)]
    stabilities = [STRONG, -STRONG, 0.0, None]
    leans = [STRONG, -STRONG, None]
    cushions = [
        ("SECURE", 40.0, 1.0),
        ("THIN", 10.0, -6.0),
        ("NO_FLIP", None, None),
        (None, None, None),
    ]

    for (bar, avg), stab, lean, (cstate, cpts, crate) in itertools.product(
        pressures, stabilities, leans, cushions
    ):
        got = classify(
            WeatherInputs(
                pressure_bar=bar,
                pressure_avg=avg,
                lean=lean,
                stability=stab,
                cushion_state=cstate,
                cushion_pts=cpts,
                cushion_rate_pts=crate,
            )
        )
        assert got.state in STATE_LABELS
        assert got.sentence.startswith(STATE_LABELS[got.state])


def test_mixed_fires_only_when_pressure_is_mixed():
    """MIXED should mean the inputs disagree, per the spec, not 'a structure
    quadrant nobody wrote a rule for'."""
    for stab in (STRONG, -STRONG, 0.0, None):
        for lean in (STRONG, -STRONG, None):
            got = classify(_inputs(stability=stab, lean=lean))
            assert got.state != STATE_MIXED

    assert classify(_inputs(pressure_bar=BIG, pressure_avg=-BIG)).state == STATE_MIXED


# --------------------------------------------------------------------------- #
# Pressure
# --------------------------------------------------------------------------- #
def test_bar_and_average_must_agree():
    """A turn in progress is honestly mixed: something is happening but it has
    not established."""
    assert classify_pressure(BIG, -BIG) == PRESSURE_MIXED
    assert classify_pressure(BIG, BIG) == PRESSURE_BUYING
    assert classify_pressure(-BIG, -BIG) == PRESSURE_SELLING


def test_pressure_inside_the_floor_is_not_a_direction():
    small = PRESSURE_FLOOR_USD * 0.5
    assert classify_pressure(small, small) == PRESSURE_MIXED


def test_missing_pressure_is_mixed_not_a_guess():
    assert classify_pressure(None, BIG) == PRESSURE_MIXED


# --------------------------------------------------------------------------- #
# Structure
# --------------------------------------------------------------------------- #
def test_structure_bands():
    assert classify_structure(STRONG) == STRUCTURE_PINNING
    assert classify_structure(-STRONG) == STRUCTURE_ACCELERATIVE
    assert classify_structure(0.0) == STRUCTURE_FLAT


def test_flat_groups_with_pinning_for_the_state_but_says_flat():
    """A book that is not degrading is closer to stable than to fragile, but
    the sentence must not claim it is building."""
    got = classify(_inputs(stability=0.0))
    assert got.state == STATE_STABLE_BID
    assert "flat" in got.sentence
    assert "building" not in got.sentence


def test_stability_decides_the_state_and_lean_only_colors_it():
    """The spec's examples always have lean and stability agreeing, so it
    never says what happens when they do not. Stability wins."""
    supportive = classify(_inputs(stability=-STRONG, lean=STRONG))
    capping = classify(_inputs(stability=-STRONG, lean=-STRONG))

    assert supportive.state == capping.state == STATE_FRAGILE_RALLY
    assert supportive.lean_side == LEAN_SUPPORTIVE
    assert capping.lean_side == LEAN_CAPPING
    assert "supportive" in supportive.sentence
    assert "capping" in capping.sentence


# --------------------------------------------------------------------------- #
# Cushion as a modifier, not a peer state
# --------------------------------------------------------------------------- #
def test_cushion_never_changes_the_state():
    """The whole reason cushion was demoted from a state to a qualifier. As
    peers they compete and one has to be suppressed; composed, both survive."""
    base = _inputs(cushion_state="SECURE", cushion_pts=40.0, cushion_rate_pts=2.0)
    risky = _inputs(cushion_state="CROSSING", cushion_pts=4.0, cushion_rate_pts=-3.0)

    assert classify(base).state == classify(risky).state
    assert classify(risky).cushion == CUSHION_TRANSITION_RISK


def test_transition_risk_needs_both_proximity_and_speed():
    """A thin cushion sitting still is a fact about where price is. A fast
    cushion with room left is just movement. The spec says 'thin AND
    contracting quickly'."""
    thin_but_still = classify_cushion("THIN", 10.0, -0.1)
    fast_but_roomy = classify_cushion("SECURE", 100.0, -40.0)
    thin_and_fast = classify_cushion("THIN", 10.0, -4.0)

    assert thin_but_still == CUSHION_NARROWING
    assert fast_but_roomy == CUSHION_NARROWING
    assert thin_and_fast == CUSHION_TRANSITION_RISK


def test_transition_threshold_is_scale_free():
    """Share of remaining room, so it means the same on SPX and SPY."""
    assert classify_cushion("THIN", 100.0, -30.0) == CUSHION_TRANSITION_RISK
    assert classify_cushion("THIN", 10.0, -3.0) == CUSHION_TRANSITION_RISK


def test_widening_and_steady_and_absent_cushions():
    assert classify_cushion("SECURE", 40.0, 3.0) == CUSHION_WIDENING
    assert classify_cushion("SECURE", 40.0, 0.0) == CUSHION_STEADY
    assert classify_cushion("NO_FLIP", None, None) == CUSHION_NONE
    assert classify_cushion(None, None, None) == CUSHION_NONE


# --------------------------------------------------------------------------- #
# The sentence describes conditions, not outcomes
# --------------------------------------------------------------------------- #
def test_sentence_avoids_predictive_language():
    """The spec is explicit that this is health classification and not a
    directional call. The wording has to hold that line or the disclosure
    elsewhere is undermined."""
    forecasts = [
        "will ",
        "should ",
        "expect",
        "likely",
        "probably",
        "target",
        "breakout",
        "reversal ahead",
        "buy ",
        "sell signal",
    ]
    pressures = [(BIG, BIG), (-BIG, -BIG), (BIG, -BIG)]
    for bar, avg in pressures:
        for stab in (STRONG, -STRONG, 0.0):
            for cstate, cpts, crate in (
                ("THIN", 10.0, -4.0),
                ("SECURE", 40.0, 2.0),
                ("NO_FLIP", None, None),
            ):
                s = classify(
                    WeatherInputs(
                        pressure_bar=bar,
                        pressure_avg=avg,
                        lean=STRONG,
                        stability=stab,
                        cushion_state=cstate,
                        cushion_pts=cpts,
                        cushion_rate_pts=crate,
                    )
                ).sentence.lower()
                for word in forecasts:
                    assert word not in s, f"predictive wording {word!r} in: {s}"


def test_components_are_returned_for_auditing():
    """A panel that shows only a verdict cannot be checked against the charts
    sitting directly underneath it."""
    got = classify(_inputs(cushion_state="THIN", cushion_pts=10.0, cushion_rate_pts=-4.0))
    assert got.pressure == PRESSURE_BUYING
    assert got.structure == STRUCTURE_PINNING
    assert got.lean_side == LEAN_SUPPORTIVE
    assert got.cushion == CUSHION_TRANSITION_RISK


def test_gamma_trend_is_classified_separately_from_structure():
    """Structure is what the book is doing now; gamma trend is where the
    session has migrated to. They are the same measurement over two windows
    and can legitimately disagree."""
    got = classify(_inputs(stability=STRONG, gamma_trend=-STRONG))

    assert got.structure == STRUCTURE_PINNING
    assert got.gamma_trend == STRUCTURE_ACCELERATIVE


def test_gamma_trend_does_not_change_the_state():
    """Background health is context, not the verdict. The state comes from the
    rolling view, which is the one that speaks to persistence right now."""
    firming = classify(_inputs(stability=STRONG, gamma_trend=STRONG))
    migrated = classify(_inputs(stability=STRONG, gamma_trend=-STRONG))

    assert firming.state == migrated.state == STATE_STABLE_BID
