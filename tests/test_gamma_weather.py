"""Gamma Weather -- the combined current-state read.

The spec left precedence open, so most of what is pinned here is precedence:
that every combination of inputs lands somewhere, that MIXED means what the
spec says it means rather than becoming a dumping ground, and that the cushion
qualifies a state instead of competing with it.
"""

from __future__ import annotations

import itertools

from src.analytics.gamma_weather import (
    AGE_CONFIRMED,
    AGE_ESTABLISHED,
    AGE_LABELS,
    AGE_MATURE,
    AGE_NEW,
    PERSISTENCE_BUILDING,
    PERSISTENCE_LABELS,
    PERSISTENCE_PERSISTENT,
    PERSISTENCE_PULSE,
    classify_age,
    classify_persistence,
    classify_series,
    state_age_bars,
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


# --------------------------------------------------------------------------- #
# Pressure persistence: one bar is a pulse, three is a condition
# --------------------------------------------------------------------------- #
def test_the_persistence_ladder():
    assert classify_persistence([BIG, BIG, BIG], BIG, PRESSURE_BUYING) == PERSISTENCE_PERSISTENT
    assert classify_persistence([BIG, -BIG, BIG], BIG, PRESSURE_BUYING) == PERSISTENCE_BUILDING
    assert classify_persistence([-BIG, -BIG, BIG], BIG, PRESSURE_BUYING) == PERSISTENCE_PULSE


def test_developing_requires_the_average_to_agree():
    """Two of three bars without an aligned average is chop, not a direction
    taking hold."""
    assert classify_persistence([BIG, -BIG, BIG], -BIG, PRESSURE_BUYING) == PERSISTENCE_PULSE


def test_bars_inside_the_floor_are_not_evidence_either_way():
    """A quiet bar does not confirm a direction, but it does not argue against
    one either; it simply does not count."""
    tiny = PRESSURE_FLOOR_USD * 0.1
    assert classify_persistence([BIG, tiny, BIG], BIG, PRESSURE_BUYING) == PERSISTENCE_BUILDING


def test_mixed_pressure_is_always_a_pulse():
    """There is no direction to be persistent about."""
    assert classify_persistence([BIG, BIG, BIG], BIG, PRESSURE_MIXED) == PERSISTENCE_PULSE


def test_only_the_trailing_window_counts():
    """A long-dead run of aligned bars should not keep a stale direction
    looking established."""
    old = [BIG] * 10 + [-BIG, -BIG]
    assert classify_persistence(old, BIG, PRESSURE_BUYING) == PERSISTENCE_PULSE


# --------------------------------------------------------------------------- #
# State age
# --------------------------------------------------------------------------- #
def test_age_bands():
    assert classify_age(5) == AGE_NEW
    assert classify_age(18) == AGE_ESTABLISHED
    assert classify_age(41) == AGE_CONFIRMED
    assert classify_age(75) == AGE_MATURE


def test_the_gap_between_provisional_and_established_is_not_overstated():
    """Twelve minutes is past 'under ten' but short of the established line.
    It reads as developing, because calling it established would claim more
    than the clock supports."""
    assert classify_age(12) == AGE_NEW


def test_age_counts_backward_from_the_newest_state():
    assert state_age_bars(["A", "A", "B", "B", "B"]) == 3
    assert state_age_bars(["A", "B"]) == 1
    assert state_age_bars([]) == 0


def test_age_resets_when_the_state_changes():
    rows = [_inputs() for _ in range(4)]
    rows += [_inputs(pressure_bar=-BIG, pressure_avg=-BIG, stability=-STRONG)]
    series = classify_series(rows)

    assert series[3].age_minutes == 20
    assert series[4].age_minutes == 5
    assert series[4].state != series[3].state


def test_age_is_what_the_bar_would_have_read_at_the_time():
    """Counted backward at each point rather than assigned with hindsight, so
    a completed session and a live one agree bar for bar."""
    rows = [_inputs() for _ in range(6)]
    full = classify_series(rows)
    partial = classify_series(rows[:4])

    assert [w.age_minutes for w in partial] == [w.age_minutes for w in full[:4]]


def test_persistence_and_age_are_independent():
    """Pressure can be established while the state is young, and vice versa:
    one is about the push, the other about the condition."""
    rows = [_inputs() for _ in range(5)]
    rows.append(_inputs(stability=-STRONG))  # structure flips, pressure holds
    series = classify_series(rows)

    assert series[-1].persistence == PERSISTENCE_PERSISTENT
    assert series[-1].age_minutes == 5


# --------------------------------------------------------------------------- #
# Pairing the two stored series.
# --------------------------------------------------------------------------- #


def _bar(i):
    from datetime import datetime, timedelta

    return datetime(2026, 9, 17, 13, 30) + timedelta(minutes=5 * i)


def _regime(i, **over):
    row = {
        "bar_start": _bar(i),
        "spot": 600.0,
        "gamma_flip": 580.0,
        "rolling_lean": 1.0e6,
        "rolling_stability": STRONG,
        "anchored_stability": STRONG,
        "typical_move_30m": 3.0,
    }
    row.update(over)
    return row


def _flow(i, net=3.0e7):
    return {"bar_start": _bar(i), "net_flow_usd": net}


def test_pair_series_matches_on_bar_start_not_position():
    """The two series are written by different paths on the same grid. Pairing
    by position would put this bar's pressure beside last bar's structure in a
    sentence that claims to describe the same five minutes."""
    from src.analytics.gamma_weather import pair_series

    regime = [_regime(i) for i in (0, 1, 2, 3)]
    flow = [_flow(i) for i in (1, 3)]  # holes at 0 and 2

    paired = pair_series(regime, flow)

    assert [p.bar_start for p in paired] == [_bar(1), _bar(3)]


def test_pair_series_drops_structure_bars_with_no_flow():
    from src.analytics.gamma_weather import pair_series

    assert pair_series([_regime(0)], []) == []


def test_pair_series_drops_flow_bars_with_no_structure():
    from src.analytics.gamma_weather import pair_series

    assert pair_series([], [_flow(0)]) == []


def test_the_moving_average_spans_the_whole_flow_series():
    """Computed before pairing, so a bar's three-bar average is the number the
    flow chart draws even when the structure series starts later."""
    from src.analytics.gamma_weather import pair_series

    flow = [_flow(i, net=1.0e7 * (i + 1)) for i in range(4)]
    paired = pair_series([_regime(3)], flow)

    assert paired[0].pressure_avg == (2.0e7 + 3.0e7 + 4.0e7) / 3


def test_pair_series_carries_the_source_row_for_auditing():
    """The panel echoes the raw components it classified from. They come off
    the paired bar rather than a second lookup that could disagree."""
    from src.analytics.gamma_weather import pair_series

    paired = pair_series([_regime(0)], [_flow(0)])

    assert paired[0].regime["gamma_flip"] == 580.0
    assert paired[0].cushion.cushion_pts == 20.0


def test_a_missing_flip_still_pairs_and_reports_no_cushion():
    """NULL gamma_flip is meaningful -- the profile had no zero crossing -- and
    must not drop the bar out of the series."""
    from src.analytics.gamma_weather import pair_series
    from src.analytics.flip_cushion import STATE_NO_FLIP

    paired = pair_series([_regime(0, gamma_flip=None)], [_flow(0)])

    assert len(paired) == 1
    assert paired[0].cushion.state == STATE_NO_FLIP


# --------------------------------------------------------------------------- #
# The two ladders, kept apart.
# --------------------------------------------------------------------------- #


def test_the_two_ladders_share_no_words():
    """They answer different questions and the payload carries both at once,
    so "established" has to mean exactly one thing. They used to both run
    DEVELOPING -> ESTABLISHED, and a reader could not tell a settled pressure
    leg from a state old enough to trust."""
    assert not set(PERSISTENCE_LABELS) & set(AGE_LABELS)
    assert not {v.lower() for v in PERSISTENCE_LABELS.values()} & {
        v.lower() for v in AGE_LABELS.values()
    }


def test_every_rung_has_display_wording():
    """A missing entry would fall through to the raw code and put PERSISTENT
    in front of a user."""
    for code in (PERSISTENCE_PULSE, PERSISTENCE_BUILDING, PERSISTENCE_PERSISTENT):
        assert PERSISTENCE_LABELS[code]
    for code in (AGE_NEW, AGE_ESTABLISHED, AGE_CONFIRMED, AGE_MATURE):
        assert AGE_LABELS[code]


def test_the_ladders_are_barries_wording():
    """Pinned because these are his words, agreed in writing, and a later
    tidy-up that renamed them would be a change to a shared vocabulary rather
    than to an internal detail."""
    assert list(PERSISTENCE_LABELS.values()) == ["Pulse", "Building", "Persistent"]
    assert list(AGE_LABELS.values()) == ["New", "Established", "Confirmed", "Mature"]


def test_code_and_label_stay_in_lockstep():
    rows = [_inputs() for _ in range(9)]
    series = classify_series(rows)

    for w in series:
        assert w.persistence_label == PERSISTENCE_LABELS[w.persistence]
        assert w.age_label == AGE_LABELS[w.age]


def test_the_age_clock_climbs_barries_rungs():
    rows = [_inputs() for _ in range(13)]
    series = classify_series(rows)

    assert (series[1].age_minutes, series[1].age) == (10, AGE_NEW)
    assert (series[2].age_minutes, series[2].age) == (15, AGE_ESTABLISHED)
    assert (series[5].age_minutes, series[5].age) == (30, AGE_CONFIRMED)
    assert (series[11].age_minutes, series[11].age) == (60, AGE_MATURE)
