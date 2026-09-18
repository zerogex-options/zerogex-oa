"""Base rates -- the guards that stop a durability number from flattering itself.

Most of what is pinned here is the three ways the measurement could lie and
does not: censoring a run at the close, counting overlapping windows as
independent evidence, and judging a state against a reference that contains it.
Each has a test that fails loudly if the guard is removed, because each would
otherwise show up as a better-looking number rather than as an error.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

import pytz

from src.analytics.base_rates import (
    MIN_GRADED_TRIALS,
    VERDICT_DESCRIPTIVE,
    VERDICT_INSUFFICIENT,
    VERDICT_LESS_DURABLE,
    VERDICT_MORE_DURABLE,
    VERDICT_NO_EDGE,
    Proportion,
    RunLengths,
    Session,
    age_band_trials,
    change_attribution,
    component_churn,
    confirmation_lag,
    debounce,
    checkpoint_anchors,
    compare,
    every_bar_anchors,
    held,
    lift_table,
    onset_anchors,
    run_lengths,
    runs,
    state_share,
    survival_trials,
    tally,
    two_proportion_p,
    warning_trials,
)

ET = pytz.timezone("America/New_York")
OPEN = ET.localize(datetime(2026, 9, 17, 9, 30))


def _session(states, warmup=0, warnings=None, ages=None, label="d"):
    return Session(
        label=label,
        bar_starts=[OPEN + timedelta(minutes=5 * i) for i in range(len(states))],
        states=states,
        warnings=warnings or [],
        ages=ages or [],
        warmup=warmup,
    )


# --------------------------------------------------------------------------- #
# Runs.
# --------------------------------------------------------------------------- #


def test_runs_split_on_change_and_mark_the_last_one_truncated():
    result = runs(["A", "A", "B", "B", "B"])

    assert [(r.state, r.start, r.length) for r in result] == [("A", 0, 2), ("B", 2, 3)]
    assert result[0].truncated is False
    assert result[1].truncated is True


def test_runs_of_an_empty_session_is_empty():
    assert runs([]) == []


def test_a_state_that_returns_starts_a_new_run():
    """Not one long run with a hole in it: the spec's checkpoint question asks
    whether the state stayed INTACT, and a round trip is not intact."""
    result = runs(["A", "B", "A"])

    assert [r.state for r in result] == ["A", "B", "A"]


# --------------------------------------------------------------------------- #
# Survival, and the censoring asymmetry that is the whole point.
# --------------------------------------------------------------------------- #


def test_held_is_true_when_the_state_survives_the_whole_horizon():
    assert held(["A", "A", "A", "A"], 0, 3) is True


def test_held_is_false_when_a_break_is_observed():
    assert held(["A", "A", "B", "A"], 0, 3) is False


def test_survival_past_the_last_bar_is_unknown_not_a_success():
    """A state still running at the close lasted AT LEAST that long. Calling it
    a completed survival would inflate exactly the states that last longest."""
    assert held(["A", "A", "A"], 0, 5) is None


def test_a_break_past_the_last_bar_is_still_a_failure():
    """The asymmetry that makes the estimate unbiased rather than merely
    conservative. A break is visible the moment it happens, so a run that dies
    two bars into a six-bar horizon is a resolved failure even though the
    horizon runs off the end of the data. Dropping it as censored would throw
    away real failures and quietly flatter every state."""
    assert held(["A", "B", "A"], 0, 6) is False


def test_held_outside_the_series_is_unknown():
    assert held(["A"], 5, 1) is None
    assert held(["A"], -1, 1) is None


def test_a_horizon_of_zero_is_trivially_held():
    assert held(["A", "B"], 0, 0) is True


# --------------------------------------------------------------------------- #
# Anchors.
# --------------------------------------------------------------------------- #


def test_warmup_bars_cannot_anchor_a_measurement():
    """Before the rolling structure window fills, rolling_stability is NULL and
    the classifier is forced to read FLAT, which makes an accelerative state
    unreachable. Measuring from those bars measures the warmup."""
    session = _session(["A"] * 10, warmup=3)

    assert every_bar_anchors(session) == [3, 4, 5, 6, 7, 8, 9]


def test_onset_anchors_take_one_observation_per_run():
    session = _session(["A", "A", "B", "B", "B", "A"])

    assert onset_anchors(session) == [0, 2, 5]


def test_a_run_that_began_in_warmup_is_dropped_not_re_anchored():
    """Re-anchoring it at the first usable bar would measure a partly elapsed
    run as though it were fresh, which is survivorship bias with extra steps."""
    session = _session(["A", "A", "A", "A", "B", "B"], warmup=2)

    assert onset_anchors(session) == [4]


def test_checkpoint_anchors_pick_the_bar_that_was_on_screen():
    """The last bar starting at or before the checkpoint -- the reading a
    trader actually had at 10:00, not one that had yet to print."""
    session = _session(["A"] * 12)  # 09:30 .. 10:25

    assert checkpoint_anchors(session, [time(10, 0)], ET) == [6]
    assert checkpoint_anchors(session, [time(10, 2)], ET) == [6]


def test_checkpoints_outside_the_session_are_skipped():
    session = _session(["A"] * 4)  # 09:30 .. 09:45

    assert checkpoint_anchors(session, [time(9, 0), time(14, 30)], ET) == []


def test_a_checkpoint_falling_in_a_gap_is_skipped_not_answered_stale():
    """A session that stops at 09:45 must not answer the 14:30 checkpoint with
    its 09:45 reading. Reporting a stale bar as a checkpoint observation is
    worse than reporting none, because nothing downstream can tell."""
    session = Session(
        label="gappy",
        bar_starts=[
            ET.localize(datetime(2026, 9, 17, 9, 30)),
            ET.localize(datetime(2026, 9, 17, 12, 40)),
        ],
        states=["A", "A"],
    )

    assert checkpoint_anchors(session, [time(12, 0)], ET) == []
    assert checkpoint_anchors(session, [time(12, 40)], ET) == [1]


def test_checkpoint_anchors_respect_warmup():
    session = _session(["A"] * 12, warmup=8)

    assert checkpoint_anchors(session, [time(10, 0)], ET) == []


# --------------------------------------------------------------------------- #
# Trials.
# --------------------------------------------------------------------------- #


def test_a_horizon_never_reaches_across_a_session_boundary():
    """Two four-bar days are not one eight-bar day. A state cannot be said to
    have "held" through an overnight gap."""
    one = _session(["A"] * 4, label="mon")
    two = _session(["A"] * 4, label="tue")

    trials = survival_trials([one, two], horizon_bars=5, anchors=onset_anchors)

    assert trials["A"] == [None, None]


def test_survival_trials_group_by_the_state_being_anchored_on():
    session = _session(["A", "A", "A", "B", "B", "B", "B"])

    trials = survival_trials([session], horizon_bars=3, anchors=onset_anchors)

    assert trials["A"] == [False]  # 3 bars long, so it breaks inside the horizon
    assert trials["B"] == [True]


def test_age_bands_use_the_ages_the_panel_showed():
    session = _session(
        ["A"] * 6,
        ages=[1, 2, 3, 4, 5, 6],
    )
    bands = [("young", 1, 3), ("old", 3, None)]

    trials = age_band_trials([session], horizon_bars=1, bands=bands)

    assert len(trials["young"]) == 2
    assert len(trials["old"]) == 4


def test_age_bands_fall_back_to_derived_ages():
    session = _session(["A", "A", "B", "B"])
    bands = [("first", 1, 2), ("later", 2, None)]

    trials = age_band_trials([session], horizon_bars=1, bands=bands)

    assert len(trials["first"]) == 2  # the two run openings
    assert len(trials["later"]) == 2


def test_warning_trials_count_changes_not_survivals():
    """The outcome is inverted on purpose: a warning never followed by a
    transition is the failure worth catching."""
    session = _session(
        ["A", "A", "B", "B"],
        warnings=[False, True, False, False],
    )

    trials = warning_trials([session], horizon_bars=1)

    assert trials["WARNED"] == [True]  # bar 1 warned, bar 2 changed
    assert trials["QUIET"] == [False, False, None]


def test_run_lengths_keep_censored_runs_apart():
    session = _session(["A", "A", "B", "B", "B"])

    lengths = run_lengths([session])

    assert lengths["A"].complete == [2]
    assert lengths["A"].censored == []
    assert lengths["B"].complete == []
    assert lengths["B"].censored == [3]


def test_run_lengths_skip_runs_that_began_in_warmup():
    session = _session(["A", "A", "A", "B", "B"], warmup=2)

    lengths = run_lengths([session])

    assert "A" not in lengths
    assert lengths["B"].censored == [2]


def test_state_share_is_measured_over_usable_bars_only():
    session = _session(["A", "A", "B", "B"], warmup=2)

    share = state_share([session])

    assert share["B"].n == 2
    assert share["B"].rate == 1.0
    assert "A" not in share


# --------------------------------------------------------------------------- #
# Proportions and the comparison.
# --------------------------------------------------------------------------- #


def test_tally_treats_none_as_unresolved_rather_than_a_miss():
    p = tally([True, False, None, True])

    assert (p.n, p.hits, p.unresolved) == (3, 2, 1)
    assert p.rate == 2 / 3


def test_an_empty_proportion_has_no_rate():
    assert Proportion().rate is None


def test_survival_at_resolves_a_censored_run_that_already_outlived_the_horizon():
    """Cut off at 9 bars, it has certainly outlived a 6-bar question; cut off
    at 4, it has answered nothing and is excluded rather than scored a miss."""
    lengths = RunLengths(state="A", complete=[2, 8], censored=[9, 4])

    result = lengths.survival_at(6)

    assert (result.n, result.hits, result.unresolved) == (3, 2, 1)


def test_two_proportion_p_is_symmetric_and_refuses_degenerate_cells():
    a = Proportion(n=50, hits=40)
    b = Proportion(n=50, hits=20)

    assert two_proportion_p(a, b) == two_proportion_p(b, a)
    assert two_proportion_p(Proportion(n=10, hits=0), Proportion(n=10, hits=0)) is None
    assert two_proportion_p(Proportion(), b) is None


def test_overlapping_anchors_get_no_p_value_at_all():
    """Not a caveat in the text: the number is withheld. Overlapping windows
    make the significance test confidently wrong rather than imprecise, and a
    printed p-value will be read as one whatever the footnote says."""
    result = compare(
        "A",
        Proportion(n=500, hits=450),
        Proportion(n=500, hits=100),
        independent=False,
    )

    assert result.p_value is None
    assert result.verdict == VERDICT_DESCRIPTIVE
    assert result.lift is not None  # the rates are still reported


def test_a_thin_cell_is_ungraded_and_gets_no_p_value_either():
    result = compare(
        "A",
        Proportion(n=5, hits=5),
        Proportion(n=500, hits=100),
        independent=True,
    )

    assert result.verdict == VERDICT_INSUFFICIENT
    assert result.p_value is None


def test_a_thin_reference_is_as_disqualifying_as_a_thin_group():
    result = compare(
        "A",
        Proportion(n=500, hits=450),
        Proportion(n=5, hits=1),
        independent=True,
    )

    assert result.verdict == VERDICT_INSUFFICIENT


def test_a_clearly_stickier_state_grades_more_durable():
    result = compare(
        "A",
        Proportion(n=200, hits=170),
        Proportion(n=200, hits=80),
        independent=True,
    )

    assert result.verdict == VERDICT_MORE_DURABLE
    assert result.lift > 2.0


def test_a_clearly_more_fragile_state_grades_less_durable():
    """Two-sided on purpose. "Fragile rally is more fragile than the rest" is a
    finding the vocabulary actually claims, not a failed test."""
    result = compare(
        "A",
        Proportion(n=200, hits=40),
        Proportion(n=200, hits=140),
        independent=True,
    )

    assert result.verdict == VERDICT_LESS_DURABLE
    assert result.lift < 1.0


def test_a_significant_but_tiny_difference_is_not_an_edge():
    """A large sample can attach significance to a gap too small to change a
    decision. The material-lift floor is what keeps the verdict honest."""
    result = compare(
        "A",
        Proportion(n=20000, hits=10300),
        Proportion(n=20000, hits=10000),
        independent=True,
    )

    assert result.p_value < 0.05
    assert result.verdict == VERDICT_NO_EDGE


def test_the_reference_leaves_the_group_out():
    """Comparing a state against a pooled rate that includes it compares it
    partly against itself, which shrinks the lift of whichever state dominates
    the sample. The dominant state is usually the one being asked about."""
    trials = {
        "BIG": [True] * 90 + [False] * 10,
        "SMALL": [True] * 20 + [False] * 20,
    }

    table = {c.group: c for c in lift_table(trials, independent=True)}

    assert table["BIG"].other_p.n == 40  # SMALL only
    assert table["BIG"].other_p.rate == 0.5
    assert table["BIG"].lift > table["BIG"].pooled_lift


def test_lift_table_orders_by_sample_size_by_default():
    trials = {"SMALL": [True] * 5, "BIG": [True] * 50}

    assert [c.group for c in lift_table(trials, independent=True)] == ["BIG", "SMALL"]


def test_an_explicit_order_survives():
    """An age ladder read out of sample-size order hides the only thing it is
    there to show."""
    trials = {"young": [True] * 5, "old": [True] * 50}

    table = lift_table(trials, independent=True, order=["young", "old"])

    assert [c.group for c in table] == ["young", "old"]


def test_the_graded_floor_is_the_one_the_module_publishes():
    """Pinned so the threshold cannot drift below the point where a normal
    approximation stops meaning anything."""
    assert MIN_GRADED_TRIALS >= 30


# --------------------------------------------------------------------------- #
# Diagnosing a restless state.
# --------------------------------------------------------------------------- #


def _with_components(states, components, warmup=0):
    return Session(
        label="d",
        bar_starts=[OPEN + timedelta(minutes=5 * i) for i in range(len(states))],
        states=states,
        warmup=warmup,
        components=components,
    )


def test_component_churn_counts_changes_not_bars():
    session = _with_components(
        ["A"] * 5,
        [{"p": v} for v in ["X", "X", "Y", "Y", "X"]],
    )

    churn = component_churn([session], "p")

    assert churn.runs == 3
    assert churn.changes_per_session == 2.0
    assert churn.values["X"].hits == 3


def test_component_churn_ignores_warmup_bars():
    session = _with_components(
        ["A"] * 4,
        [{"p": v} for v in ["X", "Y", "Z", "Z"]],
        warmup=2,
    )

    churn = component_churn([session], "p")

    assert churn.bars == 2
    assert churn.runs == 1


def test_component_churn_survives_sessions_with_no_components():
    session = Session(label="d", bar_starts=[OPEN], states=["A"])

    churn = component_churn([session], "p")

    assert churn.runs == 0
    assert churn.changes_per_session is None


def test_change_attribution_names_the_inputs_that_moved():
    session = _with_components(
        ["A", "B", "C"],
        [
            {"p": "X", "s": "M"},
            {"p": "Y", "s": "M"},  # pressure alone
            {"p": "Y", "s": "N"},  # structure alone
        ],
    )

    result = change_attribution([session], ["p", "s"])

    assert result == {"p": 1, "s": 1}


def test_a_state_change_with_no_input_change_is_flagged_not_hidden():
    """If this bucket is ever non-empty the decomposition is missing one of
    the classifier's inputs, and the report has to say so rather than
    attributing the remainder to nothing."""
    session = _with_components(["A", "B"], [{"p": "X"}, {"p": "X"}])

    assert change_attribution([session], ["p"]) == {"(none)": 1}


# --------------------------------------------------------------------------- #
# The confirmation what-if.
# --------------------------------------------------------------------------- #


def test_confirming_one_bar_is_the_identity():
    assert debounce(list("ABAB"), 1) == list("ABAB")
    assert debounce(list("ABAB"), 0) == list("ABAB")


def test_confirmation_filters_a_single_bar_flip():
    assert debounce(list("AABAA"), 2) == list("AAAAA")


def test_a_change_that_holds_arrives_late_rather_than_never():
    assert debounce(list("AABB"), 2) == list("AAAB")


def test_confirmation_is_causal():
    """Each bar is decided from bars at or before it, so the debounced series
    is one a live panel could have shown. Without this the comparison flatters
    the confirmed track with hindsight and means nothing."""
    raw = list("AABBABBBAAA")

    full = debounce(raw, 2)
    for cut in range(1, len(raw) + 1):
        assert debounce(raw[:cut], 2) == full[:cut]


def test_confirmation_lag_measures_only_changes_that_arrive():
    """A flip filtered out never reaches the headline and has no lateness to
    report -- that is the point of filtering it, not a gap in the accounting."""
    raw = list("AABAABBB")
    confirmed = debounce(raw, 2)

    lags = confirmation_lag(raw, confirmed)

    assert lags == [1]  # the one-bar B is filtered; the real change is 1 late


def test_a_series_that_never_settles_confirms_nothing():
    raw = list("ABABABAB")

    assert set(debounce(raw, 2)) == {"A"}
    assert confirmation_lag(raw, debounce(raw, 2)) == []
