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
    CONFIRM_BARS,
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
    """Once the change confirms. A single bar of a new state leaves the header
    and its clock alone, which is the whole point of confirming."""
    rows = [_inputs() for _ in range(4)]
    rows += [_inputs(pressure_bar=-BIG, pressure_avg=-BIG, stability=-STRONG)] * 2
    series = classify_series(rows)

    assert series[3].age_minutes == 20
    assert series[4].age_minutes == 25  # candidate pending, clock still running
    assert series[4].state == series[3].state
    assert series[5].age_minutes == 5
    assert series[5].state != series[3].state


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
    # Structure flips and holds for two bars, so the new state confirms while
    # the pressure leg underneath it never wavered.
    rows += [_inputs(stability=-STRONG)] * 2
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
    assert list(PERSISTENCE_LABELS.values()) == [
        "Pulse",
        "Building",
        "Persistent",
        "Reversed",
    ]
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


# --------------------------------------------------------------------------- #
# Confirmation: the header holds until a new state repeats.
# --------------------------------------------------------------------------- #


def test_the_first_bar_of_a_session_takes_the_header_immediately():
    """Nothing to confirm it against, and withholding a headline for the first
    ten minutes of every session is a worse read than the one bar of noise it
    avoids."""
    series = classify_series([_inputs()])

    assert series[0].state == STATE_STABLE_BID
    assert series[0].pending_state is None


def test_a_one_bar_flicker_never_reaches_the_header():
    """The finding this rule exists for: unconfirmed, the classifier changed
    state roughly every eight minutes and no state survived half an hour."""
    rows = [_inputs()] * 3 + [_inputs(stability=-STRONG)] + [_inputs()] * 3
    series = classify_series(rows)

    assert {w.state for w in series} == {STATE_STABLE_BID}


def test_the_candidate_is_shown_while_it_waits():
    """Barrie's requirement, and the reason confirmation does not simply hide
    the early read: show it immediately as unconfirmed, upgrade when it holds."""
    rows = [_inputs()] * 3 + [_inputs(stability=-STRONG)]
    series = classify_series(rows)

    assert series[-1].state == STATE_STABLE_BID
    assert series[-1].pending_state == STATE_FRAGILE_RALLY
    assert series[-1].pending_label == "Fragile rally"
    assert series[-1].pending_bars == 1


def test_a_change_that_holds_takes_the_header_one_bar_late():
    rows = [_inputs()] * 3 + [_inputs(stability=-STRONG)] * 2
    series = classify_series(rows)

    assert series[3].state == STATE_STABLE_BID
    assert series[4].state == STATE_FRAGILE_RALLY
    assert series[4].pending_state is None


def test_the_sentence_explains_why_the_header_and_the_components_disagree():
    """A confirmed header over components that have already moved reads like a
    bug. Naming the candidate is what makes it read like a condition."""
    rows = [_inputs()] * 3 + [_inputs(stability=-STRONG)]
    sentence = classify_series(rows)[-1].sentence

    assert sentence.startswith("Stable bid.")
    assert "gamma near price is thinning" in sentence
    assert "Fragile rally is forming, not yet confirmed." in sentence


def test_two_candidates_in_a_row_confirm_neither():
    """The streak has to be the SAME state. Alternating candidates are exactly
    the churn the rule is there to absorb."""
    rows = [_inputs()] * 2
    rows += [_inputs(stability=-STRONG)]  # fragile rally
    rows += [_inputs(pressure_bar=-BIG, pressure_avg=-BIG)]  # supported dip
    rows += [_inputs(stability=-STRONG)]  # fragile rally again
    series = classify_series(rows)

    assert {w.state for w in series} == {STATE_STABLE_BID}
    assert series[-1].pending_bars == 1


def test_confirmation_is_causal():
    """Each bar is decided from bars at or before it, so a completed session
    replays to the headline the panel showed live. Without this the base-rate
    report's comparison would be flattered by hindsight and mean nothing."""
    rows = [_inputs()] * 3 + [_inputs(stability=-STRONG)] * 2 + [_inputs()] * 3
    full = classify_series(rows)

    for cut in range(1, len(rows) + 1):
        partial = classify_series(rows[:cut])
        assert [w.state for w in partial] == [w.state for w in full[:cut]]
        assert [w.pending_state for w in partial] == [w.pending_state for w in full[:cut]]


def test_confirmation_can_be_turned_off_for_measurement():
    """How the base-rate report shows what confirming is worth: the same
    classifier, not a debounced copy of its output."""
    rows = [_inputs()] * 2 + [_inputs(stability=-STRONG)] + [_inputs()] * 2
    raw = classify_series(rows, confirm_bars=1)

    assert raw[2].state == STATE_FRAGILE_RALLY
    assert raw[2].pending_state is None


def test_age_is_measured_on_the_confirmed_state():
    """Measuring it on the raw series resets the clock on every flicker, which
    is what made Barrie's Confirmed and Mature rungs unreachable."""
    rows = [_inputs()] * 6 + [_inputs(stability=-STRONG)] + [_inputs()] * 4
    series = classify_series(rows)

    assert series[-1].age_minutes == 55
    assert series[-1].age == AGE_CONFIRMED


# --------------------------------------------------------------------------- #
# The change trail.
# --------------------------------------------------------------------------- #


def _stamps(n):
    from datetime import datetime, timedelta

    base = datetime(2026, 9, 21, 13, 30)
    return [base + timedelta(minutes=5 * i) for i in range(n)]


def _trail(rows, field=None):
    from src.analytics.gamma_weather import changes

    series = classify_series(rows)
    out = changes(series, _stamps(len(rows)))
    return [c for c in out if field is None or c.field == field]


def test_a_quiet_session_goes_silent_once_it_settles():
    """The reason this exists. A state that holds all afternoon should produce
    a handful of lines, not one per bar, or the trail is as unreadable as the
    bars it was meant to replace.

    Pressure still climbs its ladder over the first bars, which is a real
    change and prints. After that, forty identical bars say nothing at all."""
    trail = _trail([_inputs() for _ in range(40)])
    settled = [c for c in trail if c.bar_start > _stamps(4)[-1]]

    assert settled == []
    assert len(trail) < 10


def test_the_open_is_reported_so_every_field_starts_somewhere():
    trail = _trail([_inputs() for _ in range(6)])
    fields = {c.field for c in trail if c.opening}

    assert "state" in fields
    assert "pressure" in fields
    assert "stability" in fields


def test_the_open_is_worded_as_a_reading_not_a_transition():
    """ "Pressure back to a pulse" at the open describes a return from nothing."""
    trail = _trail([_inputs() for _ in range(3)], field="pressure")

    assert [c.text for c in trail if c.opening] == ["Opened buying"]


def test_persistence_says_nothing_at_the_open():
    """The first bar of a session has no history behind it, so it is always a
    pulse. A line saying so on every session is the noise this leaves out."""
    trail = _trail([_inputs() for _ in range(1)], field="pressure")

    assert [c.kind for c in trail] == ["PRESSURE"]


def test_a_pressure_flip_is_reported_against_its_field():
    rows = [_inputs() for _ in range(4)]
    rows += [_inputs(pressure_bar=-BIG, pressure_avg=-BIG) for _ in range(3)]

    trail = _trail(rows, field="pressure")

    assert "Flipped to selling" in [c.text for c in trail]


def test_the_persistence_ladder_is_reported_step_by_step():
    trail = _trail([_inputs() for _ in range(6)], field="pressure")
    steps = [c.text for c in trail if c.kind == "PERSISTENCE"]

    assert steps == ["Pressure building", "Pressure persistent"]


def test_a_forming_candidate_and_its_confirmation_both_print():
    """Barrie's requirement: the early read is visible, and so is the moment it
    became the header."""
    rows = [_inputs() for _ in range(3)] + [_inputs(stability=-STRONG) for _ in range(2)]

    trail = _trail(rows, field="state")
    texts = [c.text for c in trail if not c.opening]

    assert texts == ["Fragile rally forming", "Fragile rally"]


def test_a_candidate_that_fades_does_not_announce_its_own_disappearance():
    """It never reached the header, so there is nothing to retract. Reporting
    it would be reporting the noise confirmation exists to absorb."""
    rows = [_inputs() for _ in range(3)] + [_inputs(stability=-STRONG)] + [_inputs()]

    trail = _trail(rows, field="state")

    assert [c.text for c in trail if not c.opening] == ["Fragile rally forming"]


def test_the_cushion_reports_where_it_is_and_where_it_is_going_separately():
    """A band change and a rate change are different facts: thin-and-stable is
    not thin-and-collapsing, which is the distinction Barrie asked for."""
    rows = [_inputs(cushion_state="SECURE", cushion_pts=40.0, cushion_rate_pts=2.0)] * 3
    rows += [_inputs(cushion_state="THIN", cushion_pts=6.0, cushion_rate_pts=-4.0)] * 2

    kinds = {c.kind for c in _trail(rows, field="cushion") if not c.opening}

    assert kinds == {"CUSHION_BAND", "CUSHION_RATE"}


def test_no_flip_reports_the_band_but_not_a_direction():
    """There is no boundary, so "steady" would describe a cushion that does
    not exist."""
    rows = [_inputs(cushion_state="NO_FLIP") for _ in range(3)]

    trail = _trail(rows, field="cushion")

    assert [c.kind for c in trail] == ["CUSHION_BAND"]
    assert trail[0].text == "No gamma flip in the profile"


def test_the_trail_is_ordered_by_time():
    rows = [_inputs() for _ in range(4)]
    rows += [_inputs(pressure_bar=-BIG, pressure_avg=-BIG, stability=-STRONG) for _ in range(3)]

    stamps = [c.bar_start for c in _trail(rows)]

    assert stamps == sorted(stamps)


def test_an_empty_session_has_no_trail():
    from src.analytics.gamma_weather import changes

    assert changes([], []) == []


def test_a_mismatched_trail_is_refused_rather_than_zipped_short():
    """Silently truncating would date every later comment wrongly, which is
    worse than failing, because nothing downstream could tell."""
    import pytest

    from src.analytics.gamma_weather import changes

    series = classify_series([_inputs() for _ in range(3)])
    with pytest.raises(ValueError):
        changes(series, _stamps(2))


def test_a_cause_is_reported_before_its_consequence():
    """Sorting the trail alphabetically put "Pressure persistent" above the
    "Flipped to buying" that produced it, which reads backwards to anyone
    scanning a session."""
    rows = [_inputs(pressure_bar=-BIG, pressure_avg=-BIG) for _ in range(4)]
    rows += [_inputs() for _ in range(3)]

    trail = [c for c in _trail(rows, field="pressure") if not c.opening]
    flip = next(c for c in trail if c.kind == "PRESSURE")
    # Only within the bar they share: a ladder step on an earlier bar is a
    # different event, not this one out of order.
    same_bar = [c.kind for c in trail if c.bar_start == flip.bar_start]

    assert same_bar.index("PRESSURE") < same_bar.index("PERSISTENCE")


# --------------------------------------------------------------------------- #
# Reversed: an established side giving way, not a new side appearing.
# --------------------------------------------------------------------------- #


def _pressure(*values):
    """A session of bars carrying only the pressure values that matter here."""
    return classify_series([_inputs(pressure_bar=v, pressure_avg=v) for v in values])


def test_reversed_needs_the_old_side_to_be_established():
    """A pulse that dies is a new pulse on the other side, not a reversal. The
    whole point is that Reversed stays rare enough to mean something."""
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED

    series = _pressure(BIG, -BIG, -BIG, -BIG)

    assert PERSISTENCE_REVERSED not in [w.persistence for w in series]


def test_an_established_side_giving_way_is_reversed():
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED

    series = _pressure(BIG, BIG, BIG, BIG, -BIG, -BIG)

    assert series[-1].persistence == PERSISTENCE_REVERSED


def test_the_reversal_takes_the_same_two_bars_the_header_takes():
    """One opposite print is not a reversal, it is one print."""
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED, REVERSAL_CONFIRM_BARS

    assert REVERSAL_CONFIRM_BARS == CONFIRM_BARS
    series = _pressure(BIG, BIG, BIG, BIG, -BIG)

    assert series[-1].persistence != PERSISTENCE_REVERSED
    assert series[-1].pressure_reversing_bars == 1


def test_two_quiet_bars_in_between_still_reverse():
    """Barrie's line: the old run has not died yet."""
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED

    series = _pressure(BIG, BIG, BIG, BIG, 0.0, 0.0, -BIG, -BIG)

    assert series[-1].persistence == PERSISTENCE_REVERSED


def test_three_quiet_bars_kill_the_old_side():
    """Nothing left to reverse, so the other side starts as a pulse."""
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED

    series = _pressure(BIG, BIG, BIG, BIG, 0.0, 0.0, 0.0, -BIG, -BIG)

    assert PERSISTENCE_REVERSED not in [w.persistence for w in series]
    assert all(w.pressure_reversing_bars == 0 for w in series)


def test_quiet_bars_do_not_count_toward_the_new_side():
    """They age out the old side and do nothing else. Two opposite prints are
    still needed, however many quiet bars sat between them."""
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED

    series = _pressure(BIG, BIG, BIG, BIG, -BIG, 0.0, -BIG)

    assert series[4].persistence != PERSISTENCE_REVERSED
    assert series[5].persistence != PERSISTENCE_REVERSED
    assert series[6].persistence == PERSISTENCE_REVERSED


def test_the_chip_appears_only_for_a_reversal():
    """Never for a fresh pulse on the other side, which is the case that would
    otherwise make the chip meaningless."""
    fresh = _pressure(BIG, -BIG, -BIG)
    real = _pressure(BIG, BIG, BIG, BIG, -BIG)

    assert all(w.pressure_reversing_bars == 0 for w in fresh)
    assert real[-1].pressure_reversing_bars == 1


def test_the_chip_clears_when_the_old_side_prints_again():
    """A flip that did not happen must not leave a chip on screen."""
    series = _pressure(BIG, BIG, BIG, BIG, -BIG, BIG)

    assert series[4].pressure_reversing_bars == 1
    assert series[5].pressure_reversing_bars == 0


def test_reversed_is_a_moment_and_the_new_side_carries_on():
    """Not a fourth rung that sticks. Barrie: once confirmed, pressure restarts
    on the new side and Weather re-evaluates."""
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED

    series = _pressure(BIG, BIG, BIG, BIG, -BIG, -BIG, -BIG, -BIG)

    assert series[5].persistence == PERSISTENCE_REVERSED
    assert series[6].persistence != PERSISTENCE_REVERSED
    assert series[7].persistence == PERSISTENCE_PERSISTENT


def test_reversed_has_display_wording_like_every_other_rung():
    from src.analytics.gamma_weather import PERSISTENCE_LABELS, PERSISTENCE_REVERSED

    assert PERSISTENCE_LABELS[PERSISTENCE_REVERSED] == "Reversed"


def test_a_session_that_never_establishes_never_reverses():
    from src.analytics.gamma_weather import PERSISTENCE_REVERSED

    series = _pressure(BIG, -BIG, BIG, -BIG, BIG, -BIG)

    assert PERSISTENCE_REVERSED not in [w.persistence for w in series]


def test_bar_side_treats_the_floor_as_quiet():
    from src.analytics.gamma_weather import bar_side

    assert bar_side(BIG) == 1
    assert bar_side(-BIG) == -1
    assert bar_side(PRESSURE_FLOOR_USD) == 0
    assert bar_side(None) == 0
