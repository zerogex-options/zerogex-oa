"""P(break | tested): the labelling contract and the anti-leakage guarantees.

The study's conclusions are only worth as much as these properties, so they
are pinned here rather than left to the self-test:

* a pierce is not a break, and a confirmed move is;
* a test that cannot resolve before the bell is censored, never counted as a
  hold — that bias would fall entirely on the late-session cases;
* one grind at a wall is one observation, not forty;
* no feature can see past its own test timestamp;
* cumulative flow is differenced, not summed;
* the strength percentile ranks against previous sessions only.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from research.wall_break_odds.dataset import TrailingStrength
from research.wall_break_odds.events import (
    ET,
    EventConfig,
    PriceBar,
    StepSeries,
    WallFrame,
    extract_wall_tests,
)
from research.wall_break_odds.features import FlowWindow, SummarySeries, build_features
from research.wall_break_odds.model import (
    MIN_EVENTS_FOR_MODEL,
    Row,
    base_rate,
    evaluate,
    session_walk_forward,
    wilson_interval,
)

SESSION = date(2026, 6, 1)
WALL = 500.0
OPEN = datetime.combine(SESSION, time(9, 30), tzinfo=ET)


def _path(closes, wall=WALL, highs=None, lows=None):
    """Frames + bars for a hand-specified close path."""
    bars = []
    for m, c in enumerate(closes):
        hi = highs[m] if highs else max(c, wall * 0.9999)
        lo = lows[m] if lows else min(c, wall * 0.9999)
        bars.append(PriceBar(ts=OPEN + timedelta(minutes=m), high=hi, low=lo, close=c))
    frames = [
        WallFrame(ts=OPEN + timedelta(minutes=m), call_wall=wall, put_wall=wall * 0.98)
        for m in range(len(closes))
    ]
    return frames, bars


def _calls(closes, cfg=None, **kw):
    frames, bars = _path(closes, **kw)
    events = extract_wall_tests("TEST", SESSION, frames, bars, cfg or EventConfig())
    return [e for e in events if e.side == "call"]


# ---------------------------------------------------------------------------
# Labelling
# ---------------------------------------------------------------------------


def test_short_pierce_is_not_a_break():
    """Three minutes above the wall then straight back is a failed breakout."""
    closes = [WALL * 1.002 if 30 <= m < 33 else WALL * 0.998 for m in range(150)]
    events = _calls(closes)
    assert events, "a path touching the wall must produce a test"
    assert all(e.outcome != "broke" for e in events)


def test_sustained_move_is_a_break():
    closes = [WALL * 1.002 if m >= 30 else WALL * 0.998 for m in range(150)]
    events = _calls(closes)
    assert events[0].outcome == "broke"
    # Confirmation completes on the 10th consecutive close beyond the buffer.
    assert events[0].minutes_to_resolve == pytest.approx(39.0)


def test_confirmation_run_resets_on_a_pullback():
    """Nine minutes through, one back inside, nine more: not a break."""
    closes = []
    for m in range(150):
        if 30 <= m < 39 or 40 <= m < 49:
            closes.append(WALL * 1.002)
        else:
            closes.append(WALL * 0.998)
    assert all(e.outcome != "broke" for e in _calls(closes))


def test_late_test_that_cannot_resolve_is_censored_not_held():
    """A test at 15:45 has 15 minutes of session left and a 60-minute horizon."""
    closes = [WALL * 0.990] * 375 + [WALL * 0.9999] * 15
    events = _calls(closes)
    late = [e for e in events if e.tested_at >= OPEN + timedelta(minutes=375)]
    assert late, "the late approach should register as a test"
    assert all(e.outcome == "censored" for e in late)


def test_a_grind_at_the_wall_is_not_forty_observations():
    """Forty minutes inside the touch band emits a handful of events, not 40."""
    closes = [WALL * 0.9999] * 200
    events = _calls(closes)
    assert 0 < len(events) <= 4, f"expected de-duplicated events, got {len(events)}"


def test_a_broken_wall_emits_no_further_tests():
    closes = [WALL * 1.002 if m >= 30 else WALL * 0.998 for m in range(300)]
    events = _calls(closes)
    assert sum(1 for e in events if e.outcome == "broke") == 1
    # Nothing after the break: that wall value is spent for the session.
    broke_at = next(e.resolved_at for e in events if e.outcome == "broke")
    assert all(e.tested_at <= broke_at for e in events)


def test_put_wall_is_mirrored():
    """A sustained move DOWN through the put wall breaks it."""
    put = WALL * 0.98
    closes = [put * 0.998 if m >= 30 else put * 1.002 for m in range(150)]
    frames, bars = _path(closes)
    events = extract_wall_tests("TEST", SESSION, frames, bars, EventConfig())
    puts = [e for e in events if e.side == "put"]
    assert puts and puts[0].outcome == "broke"


def test_thin_session_produces_nothing():
    """Too few frames to characterise the day is not a session with no tests."""
    closes = [WALL * 0.9999] * 20
    frames, bars = _path(closes)
    assert extract_wall_tests("TEST", SESSION, frames, bars, EventConfig()) == []


# ---------------------------------------------------------------------------
# Step series
# ---------------------------------------------------------------------------


def test_step_series_never_reads_the_next_publish():
    s = StepSeries([(OPEN, 1.0), (OPEN + timedelta(minutes=10), 2.0)])
    assert s.at(OPEN + timedelta(minutes=9)) == 1.0
    assert s.at(OPEN + timedelta(minutes=10)) == 2.0
    assert s.at(OPEN - timedelta(minutes=1)) is None


def test_step_series_age_walks_back_over_equal_values():
    pts = [(OPEN + timedelta(minutes=m), 7.0) for m in range(30)]
    s = StepSeries(pts)
    assert s.age_minutes(OPEN + timedelta(minutes=29)) == pytest.approx(29.0)


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------


def _rows(n=200, strength=1.0e9):
    return [
        {
            "timestamp": OPEN + timedelta(minutes=m),
            "call_wall": WALL,
            "put_wall": WALL * 0.98,
            "call_wall_strength": strength,
            "put_wall_strength": strength,
            "total_net_gex": 2.0e9,
            "gamma_flip_point": WALL * 0.995,
            "flip_distance": 0.001,
            "local_gex": 5.0e8,
            "convexity_risk": 1.0e11,
        }
        for m in range(n)
    ]


def test_features_are_blind_to_the_future():
    closes = [WALL * 0.9999] * 200
    frames, bars = _path(closes)
    event = extract_wall_tests("TEST", SESSION, frames, bars, EventConfig())[0]
    rows = _rows()
    clean = build_features(event, SummarySeries.from_rows(rows), bars)

    poisoned = [
        (
            r
            if r["timestamp"] <= event.tested_at
            else {**r, "call_wall_strength": 9e18, "call_wall": 1.0}
        )
        for r in rows
    ]
    poisoned_bars = [
        b if b.ts <= event.tested_at else PriceBar(ts=b.ts, high=1e9, low=0.0, close=1e9)
        for b in bars
    ]
    dirty = build_features(event, SummarySeries.from_rows(poisoned), poisoned_bars)
    assert clean == dirty


def test_distance_to_wall_is_not_a_feature():
    """It is ~0 by construction once a test has happened; including it would
    smuggle in a P(touch) term and make the model look informative."""
    from research.wall_break_odds.features import FEATURE_NAMES

    assert not any("distance" in n for n in FEATURE_NAMES if n != "flip_distance")


def test_flow_window_differences_cumulative_it_does_not_sum():
    """net_premium is day-to-date cumulative; a window is a difference."""
    rows = [
        {
            "timestamp": OPEN + timedelta(minutes=5),
            "option_type": "call",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 100.0,
        },
        {
            "timestamp": OPEN + timedelta(minutes=35),
            "option_type": "call",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 250.0,
        },
        {
            "timestamp": OPEN + timedelta(minutes=65),
            "option_type": "call",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 400.0,
        },
    ]
    flow = FlowWindow.from_rows(rows)
    # Cumulative rose 250 -> 400 across this window: the answer is 150, and it
    # would be 650 if the buckets were summed.
    got = flow.window_premium(
        "call", [WALL], OPEN + timedelta(minutes=35), OPEN + timedelta(minutes=65)
    )
    assert got == pytest.approx(150.0)


def test_flow_window_reports_none_when_nothing_traded():
    """An untraded strike must be distinguishable from a balanced one."""
    flow = FlowWindow.from_rows([])
    assert flow.window_premium("call", [WALL], OPEN, OPEN + timedelta(minutes=30)) is None


def test_contract_first_printing_mid_window_is_not_booked_whole():
    """A contract whose first print lands inside the window contributes only
    what it did inside it — differencing pre-summed totals would book its
    entire day-to-date figure as window activity."""
    rows = [
        {
            "timestamp": OPEN + timedelta(minutes=10),
            "option_type": "call",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 1000.0,
        },
        {
            "timestamp": OPEN + timedelta(minutes=50),
            "option_type": "call",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 1000.0,
        },
        # A second contract that only starts trading at minute 50.
        {
            "timestamp": OPEN + timedelta(minutes=50),
            "option_type": "call",
            "strike": WALL,
            "expiration": date(2026, 7, 17),
            "net_premium": 60.0,
        },
    ]
    flow = FlowWindow.from_rows(rows)
    got = flow.window_premium(
        "call", [WALL], OPEN + timedelta(minutes=40), OPEN + timedelta(minutes=50)
    )
    assert got == pytest.approx(60.0)


# ---------------------------------------------------------------------------
# Causality of the trailing percentile
# ---------------------------------------------------------------------------


def test_trailing_percentile_excludes_the_current_session():
    trailing = TrailingStrength()
    assert trailing.percentile("call", OPEN, 1.0e9) is None, "cold start must not invent 50"
    for _ in range(5):
        trailing.add_session(_rows(n=60, strength=1.0e9))
    # A far larger wall than anything in history ranks at the top.
    assert trailing.percentile("call", OPEN, 5.0e9) == pytest.approx(100.0)
    assert trailing.percentile("call", OPEN, 1.0e8) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Model plumbing
# ---------------------------------------------------------------------------


def test_wilson_interval_stays_inside_the_unit_range():
    lo, hi = wilson_interval(0, 5)
    assert lo >= 0.0 and hi <= 1.0
    lo, hi = wilson_interval(5, 5)
    assert lo >= 0.0 and hi <= 1.0


def test_base_rate_marks_small_buckets_unreportable():
    rows = [Row(session=SESSION, side="call", broke=1, features={}) for _ in range(5)]
    assert base_rate(rows)["overall"]["reportable"] is False


def test_walk_forward_never_splits_a_session():
    rows = []
    for d in range(40):
        day = SESSION + timedelta(days=d)
        for _ in range(3):
            rows.append(Row(session=day, side="call", broke=0, features={}))
    for train, test in session_walk_forward(rows, n_folds=4):
        train_sessions = {rows[i].session for i in train}
        test_sessions = {rows[i].session for i in test}
        assert not (train_sessions & test_sessions)


def test_evaluate_refuses_to_model_a_small_sample():
    rows = [Row(session=SESSION, side="call", broke=0, features={"x": 1.0}) for _ in range(10)]
    out = evaluate(rows)
    assert out["status"] == "insufficient_data"
    assert out["required"] == MIN_EVENTS_FOR_MODEL


# ---------------------------------------------------------------------------
# Option-type encoding — the defect that emptied the flow column on the first
# real run: flow_by_contract stores 'C'/'P', the feature layer asked for
# 'call'/'put', and every lookup returned None looking like a quiet tape.
# ---------------------------------------------------------------------------


def test_flow_matches_the_c_p_encoding_the_table_actually_uses():
    rows = [
        {
            "timestamp": OPEN + timedelta(minutes=5),
            "option_type": "C",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 100.0,
        },
        {
            "timestamp": OPEN + timedelta(minutes=35),
            "option_type": "C",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 400.0,
        },
    ]
    flow = FlowWindow.from_rows(rows)
    assert flow.coverage() == {"C": 1}
    got = flow.window_premium(
        "call", [WALL], OPEN + timedelta(minutes=5), OPEN + timedelta(minutes=35)
    )
    assert got == pytest.approx(300.0)


def test_flow_accepts_either_spelling_on_both_sides():
    from research.wall_break_odds.features import canonical_option_type

    assert canonical_option_type("C") == canonical_option_type("call") == "C"
    assert canonical_option_type("P") == canonical_option_type("PUT") == "P"
    assert canonical_option_type("banana") is None


def test_unrecognised_option_type_is_recorded_not_swallowed():
    rows = [
        {
            "timestamp": OPEN,
            "option_type": "CALL_OPTION",
            "strike": WALL,
            "expiration": SESSION,
            "net_premium": 1.0,
        },
    ]
    flow = FlowWindow.from_rows(rows)
    assert flow.coverage() == {}
    assert flow.unrecognised == {"CALL_OPTION": 1}


# ---------------------------------------------------------------------------
# Split rule
# ---------------------------------------------------------------------------


def test_binary_feature_is_screenable():
    """A median split leaves the 'above' group empty when the majority value
    IS the median; the balanced split lands on the 0/1 boundary instead."""
    from research.wall_break_odds.model import _best_split, univariate_screen

    assert _best_split([1.0] * 60 + [0.0] * 40) == 1.0

    rows = []
    for i in range(200):
        day = SESSION + timedelta(days=i % 50)
        above = 1.0 if i % 5 else 0.0  # 80% ones — median is 1
        rows.append(Row(session=day, side="call", broke=i % 2, features={"spot_above_flip": above}))
    screened = univariate_screen(rows, feature_names=("spot_above_flip",))[0]
    assert screened["n"] == 200
    # It may or may not clear the per-group floor, but it must not fail for the
    # structural reason the median split used to fail for.
    assert screened.get("reason") != "median split degenerate"


def test_continuous_split_still_lands_on_the_median():
    from research.wall_break_odds.model import _best_split

    assert _best_split([float(i) for i in range(100)]) == 50.0


# ---------------------------------------------------------------------------
# Survival — the horizon-free answer, and the recovery of censored events
# ---------------------------------------------------------------------------


def test_kaplan_meier_matches_a_hand_computed_curve():
    from research.wall_break_odds.survival import Observation, kaplan_meier

    obs = [
        Observation(10, True),
        Observation(20, False),  # censored before the second break
        Observation(30, True),
        Observation(40, False),
        Observation(50, False),
    ]
    curve = kaplan_meier(obs)
    assert [p.minutes for p in curve] == [10, 30]
    assert curve[0].survival == pytest.approx(0.8)  # 4/5
    # The censored subject leaves the risk set: 3 at risk at t=30, not 4.
    assert curve[1].at_risk == 3
    assert curve[1].survival == pytest.approx(0.8 * 2 / 3)


def test_censored_observations_still_inform_the_curve():
    """A test that held 15 minutes and hit the bell is data, not missing data.

    Dropping it (the old behaviour) shrinks the risk set and inflates the
    estimated break probability.
    """
    from research.wall_break_odds.survival import Observation, kaplan_meier

    # A censored observation counts toward the risk set for every moment it
    # WAS watched — so it informs the curve at times before its censoring,
    # which is precisely the information dropping it threw away.
    with_censored = [Observation(10, True)] + [Observation(15, False)] * 20
    dropped = [Observation(10, True)]
    p_with = kaplan_meier(with_censored)[0].break_prob
    p_without = kaplan_meier(dropped)[0].break_prob
    assert p_with == pytest.approx(1 / 21)
    assert p_without == pytest.approx(1.0)
    assert p_with < p_without

    # ...and it correctly stops counting once watching stopped: the same 20
    # tests, censored at 15, say nothing about a break at 30.
    later = kaplan_meier([Observation(30, True)] + [Observation(15, False)] * 20)
    assert later[0].at_risk == 1


def test_break_probability_is_monotone_in_time():
    from research.wall_break_odds.survival import (
        Observation,
        break_probability_at,
        kaplan_meier,
    )

    obs = [Observation(float(5 * i), i % 3 == 0) for i in range(1, 40)]
    curve = kaplan_meier(obs)
    probs = [
        p.break_prob
        for p in (break_probability_at(curve, t) for t in (5, 15, 30, 45, 60))
        if p is not None
    ]
    assert probs == sorted(probs)


def test_no_breaks_yields_no_curve_rather_than_a_flat_zero():
    from research.wall_break_odds.survival import Observation, kaplan_meier

    assert kaplan_meier([Observation(30, False)] * 50) == []


def test_observed_minutes_is_recorded_for_every_outcome():
    """held and censored must both carry a watch time, or they cannot be
    pooled into the survival estimate."""
    closes = [WALL * 0.990] * 375 + [WALL * 0.9999] * 15
    events = _calls(closes)
    assert events
    for e in events:
        assert e.observed_minutes is not None
        assert e.observed_minutes >= 0
    late = [e for e in events if e.outcome == "censored"]
    assert late and all(e.observed_minutes < 60 for e in late)


def test_survival_marks_below_the_confirmation_window_are_not_reported_as_zero():
    """No break can be OBSERVED before confirm_minutes have elapsed, so a
    5-minute row is a property of the label, not a measurement."""
    from research.wall_break_odds.report import _survival_block
    from research.wall_break_odds.survival import Observation, kaplan_meier

    curve = kaplan_meier([Observation(20, True)] + [Observation(60, False)] * 40)
    text = "\n".join(_survival_block(curve, 41, 1, confirm_minutes=10))
    assert "not observable" in text
    assert "no breaks yet" not in text.split("15 min")[0]


def test_session_half_split_uses_et_and_the_session_midpoint():
    from research.wall_break_odds.cli import _in_half

    early = {"tested_at": "2026-07-01T14:00:00+00:00"}  # 10:00 ET
    late = {"tested_at": "2026-07-01T19:30:00+00:00"}  # 15:30 ET
    assert _in_half(early, morning=True) and not _in_half(early, morning=False)
    assert _in_half(late, morning=False) and not _in_half(late, morning=True)
    assert not _in_half({"tested_at": "nonsense"}, morning=True)
