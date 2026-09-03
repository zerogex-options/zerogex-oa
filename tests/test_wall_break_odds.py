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
