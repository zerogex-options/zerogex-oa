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


def test_logrank_separates_real_differences_from_noise():
    import random

    from research.wall_break_odds.survival import Observation, logrank

    rng = random.Random(5)

    def group(p_break, n):
        out = []
        for _ in range(n):
            if rng.random() < p_break:
                out.append(Observation(rng.uniform(10, 60), True))
            else:
                out.append(Observation(60.0, False))
        return out

    assert logrank(group(0.30, 200), group(0.30, 200)).p_value > 0.05
    assert logrank(group(0.55, 200), group(0.15, 200)).p_value < 0.001


def test_logrank_declines_rather_than_guessing_without_events():
    from research.wall_break_odds.survival import Observation, logrank

    assert logrank([Observation(60, False)] * 50, [Observation(60, False)] * 50) is None
    assert logrank([], [Observation(10, True)]) is None


def test_logrank_reproduces_the_freireich_benchmark():
    """The standard worked example (6-MP vs placebo leukemia trial).

    Pinned against published values — observed 9, expected 19.25, chi2 16.79 —
    because this statistic is the only thing standing between a raw count
    difference and a claim about hazards, and it has to be right under
    UNEQUAL censoring, which is exactly the situation in this study.
    """
    from research.wall_break_odds.survival import Observation, logrank

    mp = [
        (6, 0),
        (6, 1),
        (6, 1),
        (6, 1),
        (7, 1),
        (9, 0),
        (10, 0),
        (10, 1),
        (11, 0),
        (13, 1),
        (16, 1),
        (17, 0),
        (19, 0),
        (20, 0),
        (22, 1),
        (23, 1),
        (25, 0),
        (32, 0),
        (32, 0),
        (34, 0),
        (35, 0),
    ]
    placebo = [
        (1, 1),
        (1, 1),
        (2, 1),
        (2, 1),
        (3, 1),
        (4, 1),
        (4, 1),
        (5, 1),
        (5, 1),
        (8, 1),
        (8, 1),
        (8, 1),
        (8, 1),
        (11, 1),
        (11, 1),
        (12, 1),
        (12, 1),
        (15, 1),
        (17, 1),
        (22, 1),
        (23, 1),
    ]
    result = logrank(
        [Observation(float(t), bool(e)) for t, e in mp],
        [Observation(float(t), bool(e)) for t, e in placebo],
    )
    assert result.observed_a == 9
    assert result.expected_a == pytest.approx(19.25, abs=0.01)
    assert result.chi2 == pytest.approx(16.79, abs=0.01)
    assert result.p_value < 0.0001


def test_logrank_adjusts_for_unequal_exposure():
    """Fewer breaks in a group watched for less time is not a lower hazard.

    Afternoon wall tests are censored by the closing bell, so they carry far
    less exposure than morning ones. Comparing raw break counts across the two
    is the mistake this test exists to prevent: both groups here are drawn
    from the SAME exponential hazard and differ only in when watching stopped,
    so the second group shows far fewer breaks and the log-rank must still
    report no difference.
    """
    import random

    from research.wall_break_odds.survival import Observation, logrank

    rng = random.Random(17)

    def draw(n, censor_at):
        out = []
        for _ in range(n):
            t = rng.expovariate(1 / 80.0)  # identical hazard in both arms
            c = censor_at()
            out.append(Observation(min(t, c), t <= c))
        return out

    watched = draw(300, lambda: 60.0)
    cut_short = draw(300, lambda: rng.uniform(5, 30))
    assert sum(o.broke for o in cut_short) < sum(o.broke for o in watched) / 2
    result = logrank(watched, cut_short)
    assert result is not None
    assert result.p_value > 0.05, f"same hazard flagged as different (p={result.p_value})"


def test_strike_step_defaults_per_symbol():
    """A $5 ladder aimed at QQQ's $1 strikes silently finds no flow — it reads
    as 'no flow at the wall' rather than as a misconfiguration, which is the
    same class of silent failure as the option_type mismatch."""
    from research.wall_break_odds.cli import strike_step_for

    assert strike_step_for("SPX") == 5.0
    assert strike_step_for("ndx") == 5.0
    assert strike_step_for("QQQ") == 1.0
    assert strike_step_for("SPY") == 1.0


def test_pooled_meta_flags_conflicting_label_settings():
    """Datasets built with different thresholds must not be silently pooled —
    a 'break' means something different in each."""
    from research.wall_break_odds.cli import _merge_meta

    a = {
        "symbol": "SPX",
        "start": "2026-01-01",
        "end": "2026-02-01",
        "events_total": 10,
        "config": {"confirm_minutes": 10},
    }
    b = {
        "symbol": "QQQ",
        "start": "2026-01-15",
        "end": "2026-03-01",
        "events_total": 20,
        "config": {"confirm_minutes": 20},
    }
    merged = _merge_meta([a, b], [])
    assert merged["config_conflict"] is True
    assert merged["events_total"] == 30
    assert merged["start"] == "2026-01-01" and merged["end"] == "2026-03-01"
    same = _merge_meta([a, {**b, "config": {"confirm_minutes": 10}}], [])
    assert "config_conflict" not in same


def _pooling(p_value):
    from research.wall_break_odds.survival import LogRank

    return {
        "curves": {"SPX": ([], 100, 30), "QQQ": ([], 100, 50)},
        "logrank": LogRank(30, 40.0, 9.0, p_value, 100, 100),
        "pair": ("QQQ", "SPX"),
    }


def test_rejected_pooling_withholds_every_pooled_number():
    """A failed same-process test invalidates the curve, the base rate and —
    most dangerously — the screen, where a dollar-scale feature can stand in
    for 'which symbol is this' and read as a finding about walls."""
    from research.wall_break_odds.report import render_report

    text = render_report(
        {"symbol": "SPX + QQQ", "config": {"confirm_minutes": 10}},
        {"overall": {"n": 200, "breaks": 80, "rate": 0.4, "ci95": [0.3, 0.5], "reportable": True}},
        [
            {
                "feature": "wall_strength_log",
                "n": 200,
                "rate_above": 0.3,
                "rate_below": 0.5,
                "delta": -0.2,
                "reportable": True,
            }
        ],
        {"status": "ok", "n": 200, "oos": {}},
        pooling=_pooling(0.0007),
    )
    assert "POOLED ANALYSIS WITHHELD" in text
    assert "UNIVARIATE SCREEN" not in text
    assert "BASE RATES" not in text
    assert "P(BREAK WITHIN" not in text
    assert "LIMITS" in text


def test_accepted_pooling_reports_normally():
    from research.wall_break_odds.report import render_report

    text = render_report(
        {"symbol": "SPX + QQQ", "config": {"confirm_minutes": 10}},
        {"overall": {"n": 200, "breaks": 80, "rate": 0.4, "ci95": [0.3, 0.5], "reportable": True}},
        [],
        {"status": "ok", "n": 200, "oos": {}},
        pooling=_pooling(0.42),
    )
    assert "POOLED ANALYSIS WITHHELD" not in text
    assert "BASE RATES" in text


def test_model_names_the_feature_costing_the_most_complete_cases():
    """With plenty of events but one sparse column, the fix is dropping the
    column, not waiting months for more sessions."""
    from research.wall_break_odds.model import evaluate

    rows = []
    for i in range(260):
        feats = {"dense_a": float(i % 7), "dense_b": float(i % 5)}
        # Present on 70% of rows: passes the coverage filter, then destroys
        # the complete-case count.
        feats["sparse"] = float(i % 3) if i % 10 < 7 else None
        rows.append(
            Row(session=SESSION + timedelta(days=i % 60), side="call", broke=i % 2, features=feats)
        )
    out = evaluate(rows, feature_names=("dense_a", "dense_b", "sparse"))
    assert out["status"] == "insufficient_complete_cases"
    assert out["n_resolved"] == 260 and out["n"] < 200
    assert out["bottleneck"], "the limiting feature must be named"
    assert out["bottleneck"][0]["feature"] == "sparse"


def _screen_of(deltas):
    return [{"feature": k, "delta": v, "reportable": True, "n": 100} for k, v in deltas.items()]


def test_replication_detects_agreement_and_its_absence():
    from research.wall_break_odds.model import replication

    feats = [f"f{i}" for i in range(8)]
    same = {f: (i - 4) / 10.0 for i, f in enumerate(feats)}
    rep = replication({"SPX": _screen_of(same), "QQQ": _screen_of(same)})
    assert rep["spearman"] == pytest.approx(1.0)
    assert rep["sign_agreement"] == pytest.approx(1.0)

    flipped = {f: -v for f, v in same.items()}
    rep = replication({"SPX": _screen_of(same), "QQQ": _screen_of(flipped)})
    assert rep["spearman"] == pytest.approx(-1.0)
    assert rep["sign_agreement"] == pytest.approx(0.0)


def test_anticorrelated_screens_are_not_called_agreement():
    """Testing |r| would score two samples that systematically DISAGREE as
    replication — the exact opposite of the finding."""
    from research.wall_break_odds.report import _replication_block

    feats = [f"f{i}" for i in range(8)]
    rep = {
        "symbols": ["SPX", "QQQ"],
        "n_features": 8,
        "spearman": -0.9,
        "sign_agreement": 0.1,
        "n_signed": 8,
        "rows": [{"feature": f, "SPX": 0.1, "QQQ": -0.1} for f in feats],
    }
    text = "\n".join(_replication_block(rep))
    assert "NO replication" in text
    assert "some agreement" not in text


def test_pooling_matrix_rejects_when_any_pair_differs():
    """One rejecting pair invalidates the whole pool — pooling four symbols
    is only legitimate if every pair agrees, not the average of them."""
    from research.wall_break_odds.report import _pooling_block, _pooling_rejected
    from research.wall_break_odds.survival import LogRank

    ok = LogRank(30, 31.0, 0.1, 0.75, 100, 100)
    bad = LogRank(30, 45.0, 12.0, 0.0005, 100, 100)
    pooling = {
        "curves": {s: ([], 100, 30) for s in ("SPX", "SPY", "QQQ", "NDX")},
        "pairs": {("NDX", "SPX"): ok, ("QQQ", "SPY"): ok, ("SPX", "SPY"): bad},
        "symbols": ["NDX", "QQQ", "SPX", "SPY"],
        "logrank": bad,
        "any_pair_differs": True,
    }
    assert _pooling_rejected(pooling) is True
    text = "\n".join(_pooling_block(pooling))
    assert "DIFFER" in text
    assert "cannot all be pooled" in text

    clean = {**pooling, "pairs": {("NDX", "SPX"): ok}, "logrank": None, "any_pair_differs": False}
    assert _pooling_rejected(clean) is False


def test_replication_matrix_renders_every_pair():
    from research.wall_break_odds.report import _replication_block

    rep = {
        "symbols": ["NDX", "QQQ", "SPX", "SPY"],
        "matrix": {
            ("SPX", "SPY"): {
                "symbols": ["SPX", "SPY"],
                "n_features": 12,
                "spearman": 0.75,
                "sign_agreement": 0.83,
                "rows": [],
                "substantive": {
                    "n_features": 8,
                    "spearman": 0.71,
                    "sign_agreement": 0.75,
                },
            },
            # Agrees overall but ONLY on the clock: the substantive columns
            # are flat. The verdict must follow the substantive column.
            ("QQQ", "SPX"): {
                "symbols": ["QQQ", "SPX"],
                "n_features": 12,
                "spearman": 0.68,
                "sign_agreement": 0.80,
                "rows": [],
                "substantive": {
                    "n_features": 8,
                    "spearman": -0.05,
                    "sign_agreement": 0.50,
                },
            },
        },
    }
    text = "\n".join(_replication_block(rep))
    assert "SPX vs SPY" in text and "replicates" in text
    qqq_line = next(ln for ln in text.splitlines() if "QQQ vs SPX" in ln)
    assert "NO replication" in qqq_line
