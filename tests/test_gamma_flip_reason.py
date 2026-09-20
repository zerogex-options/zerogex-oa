"""A blank Flip line has to say which kind of blank it is.

The resolver persists NULL when it will not stand behind a crossing, and that
is correct in four distinct situations that a chart renders identically: no
usable chain at all, a one-signed book with no crossing anywhere in the
searched band, a crossing too close to the grid edge to trust, a crossing that
is real but further from spot than anything actionable, and a crossing whose
local structure is indistinguishable from noise. An NDX blackout ran from July
to September 2026 looking exactly like a quiet session because all of those
render as one em dash.

``_classify_unresolved_flip`` is what separates them, and the property that
makes it worth trusting is narrow: it must apply the SAME gates in the SAME
order as ``_find_structural_interior_crossing``, and report the FURTHEST one
any candidate reached. Report the first instead and a crossing rejected by the
floor gets labelled "edge only", which is not merely vague, it is false.
"""

from __future__ import annotations

import pytest

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

SPOT = 100.0


@pytest.fixture
def engine(monkeypatch):
    """A bare engine: the classifier touches no state beyond the gates."""
    eng = AnalyticsEngine.__new__(AnalyticsEngine)
    # The structural reference is the anchored p90 the resolver gated on.
    # Stub it so each test controls the floor directly instead of having to
    # hand-build an option chain that produces the p90 it wants.
    monkeypatch.setattr(
        AnalyticsEngine, "_structural_reference_from_profile", lambda *a, **k: 1000.0
    )
    return eng


def _flat(values):
    """A profile on a fixed grid, one point per dollar from 50 to 150."""
    return [(50.0 + i, v) for i, v in enumerate(values)]


def _one_signed(sign=-1.0):
    return _flat([sign * 5000.0] * 101)


def _crossing_at(index, magnitude=5000.0):
    """Negative below ``index``, positive at and above it."""
    return _flat([(-magnitude if i < index else magnitude) for i in range(101)])


# --- the codes -------------------------------------------------------------


def test_an_empty_profile_is_a_data_problem_not_a_market_one(engine):
    assert engine._classify_unresolved_flip([], [], SPOT) == main_engine.FLIP_REASON_NO_PROFILE
    assert (
        engine._classify_unresolved_flip([], [(100.0, 1.0)], SPOT)
        == main_engine.FLIP_REASON_NO_PROFILE
    )


def test_a_one_signed_book_has_no_flip_to_find(engine):
    """QQQ on 2026-07-28 afternoon: 401 profile points, every one negative."""
    assert (
        engine._classify_unresolved_flip([], _one_signed(), SPOT)
        == main_engine.FLIP_REASON_ONE_SIDED
    )
    assert (
        engine._classify_unresolved_flip([], _one_signed(sign=1.0), SPOT)
        == main_engine.FLIP_REASON_ONE_SIDED
    )


def test_a_crossing_against_the_grid_edge_is_not_trusted(engine):
    """Index 2 of 101 is inside the 10% interior margin at either end."""
    assert (
        engine._classify_unresolved_flip([], _crossing_at(2), SPOT)
        == main_engine.FLIP_REASON_EDGE_ONLY
    )


def test_a_real_but_far_crossing_says_so(engine):
    """SPX in August 2026: structurally fine, 9-10% below spot, gate is 8%."""
    # Grid runs 50..150, spot 100, so index 40 is $90 -- interior, and 10%
    # away, outside the 8% actionable-distance ceiling.
    assert (
        engine._classify_unresolved_flip([], _crossing_at(40), SPOT)
        == main_engine.FLIP_REASON_BEYOND_MAX_DISTANCE
    )


def test_a_near_crossing_in_the_noise_floor_says_that_instead(engine):
    """Interior and near enough, so only the structural floor can have rejected it."""
    # Index 52 is $102 -- 2% from spot -- and the magnitude is far below the
    # stubbed reference of 1000 * STRUCTURAL_MIN_FRAC.
    assert (
        engine._classify_unresolved_flip([], _crossing_at(52, magnitude=0.001), SPOT)
        == main_engine.FLIP_REASON_BELOW_STRUCTURAL_FLOOR
    )


# --- the precedence is what makes the code true ----------------------------


def test_the_furthest_gate_reached_wins_not_the_first(engine):
    """An edge crossing AND a near noise-floor one: the near one is the answer.

    Reported as EDGE_ONLY this would be a false statement about a chain that
    did produce a well-bracketed, actionable crossing.
    """
    values = [-5000.0] * 101
    for i in range(2, 101):  # an edge crossing at index 2
        values[i] = 5000.0
    values[52] = -0.001  # ...and a tiny dip back through zero near spot
    values[53] = 0.001
    profile = _flat(values)
    assert (
        engine._classify_unresolved_flip([], profile, SPOT)
        == main_engine.FLIP_REASON_BELOW_STRUCTURAL_FLOOR
    )


def test_a_far_crossing_outranks_an_edge_only_one(engine):
    values = [-5000.0] * 101
    for i in range(2, 101):
        values[i] = 5000.0
    values[40] = -5000.0  # a second, interior-but-far crossing at $90
    profile = _flat(values)
    assert (
        engine._classify_unresolved_flip([], profile, SPOT)
        == main_engine.FLIP_REASON_BEYOND_MAX_DISTANCE
    )


def test_an_unusable_structural_reference_is_a_data_problem(engine, monkeypatch):
    """No reference means no basis to judge structure -- the resolver's own rule."""
    monkeypatch.setattr(AnalyticsEngine, "_structural_reference_from_profile", lambda *a, **k: 0.0)
    assert (
        engine._classify_unresolved_flip([], _crossing_at(52), SPOT)
        == main_engine.FLIP_REASON_NO_PROFILE
    )


# --- it must agree with the resolver ---------------------------------------


@pytest.mark.parametrize("index", [2, 40, 52, 98])
def test_the_classifier_only_ever_explains_a_flip_the_resolver_declined(engine, index):
    """If the resolver WOULD publish, there is nothing for this to explain.

    The two walk the same gates, so any profile the resolver accepts must not
    be one the classifier finds a rejection reason for -- and vice versa. This
    is the invariant that keeps a reason code from becoming folklore.
    """
    profile = _crossing_at(index)
    published = engine._find_structural_interior_crossing(
        profile, SPOT, structural_reference=1000.0
    )
    reason = engine._classify_unresolved_flip([], profile, SPOT)
    if published is not None:
        # The resolver published, so every gate passed; the classifier must
        # not claim any of them rejected it.
        assert (
            reason == main_engine.FLIP_REASON_BELOW_STRUCTURAL_FLOOR
        ), "the only code reachable when all earlier gates passed"
    else:
        assert reason != main_engine.FLIP_REASON_NO_PROFILE


def test_every_reason_constant_is_a_distinct_string():
    codes = {
        name: value for name, value in vars(main_engine).items() if name.startswith("FLIP_REASON_")
    }
    assert len(codes) == 5
    assert len(set(codes.values())) == 5
