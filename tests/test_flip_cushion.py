"""Flip cushion -- spot-to-flip distance, its direction, rate and state label.

The thing worth pinning hardest is that the state label is computed from the
FRACTION and never from points. A threshold in points silently means something
different on every symbol, and the failure is invisible: the panel keeps
rendering a confident label that is simply wrong for that instrument.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.analytics.flip_cushion import (
    BASIS_MOVE,
    BASIS_SPOT,
    CROSSING_BAND,
    DEFAULT_RATE_BARS,
    FALLBACK_CROSSING_SPAN,
    FALLBACK_THIN_SPAN,
    NORMAL_BAND,
    RATE_ACCELERATING,
    RATE_CONTRACTING,
    RATE_DRIFTING,
    RATE_STABLE,
    STATE_NORMAL,
    THIN_BAND,
    SIDE_ABOVE,
    SIDE_BELOW,
    STATE_CROSSING,
    STATE_NO_FLIP,
    STATE_SECURE,
    STATE_THIN,
    build_series,
    classify,
    classify_rate,
    describe,
    measure,
)

#: A typical 30-minute move, for tests that exercise the real yardstick.
MOVE = 20.0


def _state(frac, cushion=None, move=None):
    """classify() returns (state, ratio, basis); most tests want the state."""
    return classify(frac, cushion, move)[0]


UTC = timezone.utc
T0 = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)


def _bars(pairs):
    """(spot, flip) pairs on a 5-minute grid."""
    return [(T0 + timedelta(minutes=5 * i), s, f) for i, (s, f) in enumerate(pairs)]


# --------------------------------------------------------------------------- #
# The measurement
# --------------------------------------------------------------------------- #
def test_spot_above_flip_reads_above():
    pts, frac, cushion, side = measure(700.0, 690.0)
    assert pts == pytest.approx(10.0)
    assert frac == pytest.approx(10.0 / 700.0)
    assert cushion == pytest.approx(10.0)
    assert side == SIDE_ABOVE


def test_spot_below_flip_reads_below_with_positive_cushion():
    """Distance is signed, cushion is not: room before crossing is room
    whichever side you are converging from."""
    pts, _frac, cushion, side = measure(690.0, 700.0)
    assert pts == pytest.approx(-10.0)
    assert cushion == pytest.approx(10.0)
    assert side == SIDE_BELOW


def test_missing_flip_measures_nothing():
    assert measure(700.0, None) == (None, None, None, None)


def test_non_positive_spot_is_refused_rather_than_divided_by():
    assert measure(0.0, 690.0) == (None, None, None, None)


# --------------------------------------------------------------------------- #
# Classification is on the fraction, not the points
# --------------------------------------------------------------------------- #
def test_the_same_cushion_reads_differently_in_different_volatility():
    """The reason the yardstick changed from a fraction of spot to a typical
    30-minute move. Ten points is a live crossing risk on a quiet tape and an
    ordinary cushion on a fast one, and only the move scale can say so."""
    quiet = _state(10.0 / 700.0, cushion=10.0, move=8.0)
    fast = _state(10.0 / 700.0, cushion=10.0, move=40.0)

    assert quiet == STATE_NORMAL
    assert fast == STATE_CROSSING


def test_the_four_bands():
    assert _state(0.01, cushion=MOVE * 0.20, move=MOVE) == STATE_CROSSING
    assert _state(0.01, cushion=MOVE * 0.45, move=MOVE) == STATE_THIN
    assert _state(0.01, cushion=MOVE * 0.90, move=MOVE) == STATE_NORMAL
    assert _state(0.01, cushion=MOVE * 2.00, move=MOVE) == STATE_SECURE


def test_band_boundaries_are_inclusive_at_the_lower_edge():
    assert _state(0.01, cushion=MOVE * CROSSING_BAND, move=MOVE) == STATE_CROSSING
    assert _state(0.01, cushion=MOVE * THIN_BAND, move=MOVE) == STATE_THIN
    assert _state(0.01, cushion=MOVE * NORMAL_BAND, move=MOVE) == STATE_NORMAL


def test_classification_ignores_which_side_we_are_on():
    above = _state(0.005, cushion=10.0, move=MOVE)
    below = _state(-0.005, cushion=10.0, move=MOVE)
    assert above == below


def test_basis_says_which_yardstick_was_used():
    """The two scales are not comparable, so a reading must never leave a
    reader guessing which produced it."""
    assert classify(0.01, 10.0, MOVE)[2] == BASIS_MOVE
    assert classify(0.01, 10.0, None)[2] == BASIS_SPOT


def test_falls_back_to_the_spot_fraction_without_a_move_scale():
    """Bars stored before the move scale existed still classify."""
    assert _state(FALLBACK_THIN_SPAN * 2) == STATE_SECURE
    assert _state(FALLBACK_THIN_SPAN) == STATE_THIN
    assert _state(FALLBACK_CROSSING_SPAN) == STATE_CROSSING


def test_no_flip_is_its_own_state_not_a_secure_cushion():
    """A profile with no crossing at all is a different statement from one
    whose crossing is far away."""
    assert _state(None) == STATE_NO_FLIP
    assert _state(None) != STATE_SECURE


# --------------------------------------------------------------------------- #
# Direction and rate
# --------------------------------------------------------------------------- #
def test_first_bar_has_no_step_or_rate():
    series = build_series(_bars([(700.0, 690.0)]))
    assert series[0].step_pts is None
    assert series[0].rate_pts is None


def test_narrowing_cushion_gives_negative_step():
    series = build_series(_bars([(700.0, 690.0), (695.0, 690.0)]))
    assert series[1].step_pts == pytest.approx(-5.0)


def test_widening_cushion_gives_positive_step():
    series = build_series(_bars([(695.0, 690.0), (700.0, 690.0)]))
    assert series[1].step_pts == pytest.approx(5.0)


def test_rate_spans_the_rolling_window():
    # Cushion walks 20, 17, 14, 11: over 3 bars it gave up 9 points.
    series = build_series(_bars([(710.0, 690.0), (707.0, 690.0), (704.0, 690.0), (701.0, 690.0)]))
    assert series[3].rate_pts == pytest.approx(-9.0)


def test_rate_is_none_until_the_window_fills():
    series = build_series(_bars([(710.0, 690.0)] * DEFAULT_RATE_BARS))
    assert all(b.rate_pts is None for b in series)


def test_rate_window_is_configurable():
    series = build_series(_bars([(710.0, 690.0), (708.0, 690.0), (706.0, 690.0)]), rate_bars=1)
    assert series[1].rate_pts == pytest.approx(-2.0)


def test_steady_narrowing_is_not_accelerating():
    series = build_series(_bars([(710.0, 690.0), (707.0, 690.0), (704.0, 690.0), (701.0, 690.0)]))
    assert series[3].accelerating is False


def test_quickening_narrowing_is_accelerating():
    """Sustained convergence versus ordinary wobble is the whole point of the
    rolling window; acceleration is what separates them."""
    series = build_series(_bars([(720.0, 690.0), (719.0, 690.0), (717.0, 690.0), (708.0, 690.0)]))
    assert series[3].accelerating is True


def test_acceleration_is_none_when_widening():
    """Not accelerating and not narrowing at all are different statements."""
    series = build_series(_bars([(700.0, 690.0), (703.0, 690.0), (706.0, 690.0), (712.0, 690.0)]))
    assert series[3].rate_pts > 0
    assert series[3].accelerating is None


def test_a_gap_in_the_flip_breaks_the_chain_rather_than_inventing_a_step():
    """A cushion cannot have narrowed from a bar that had no boundary."""
    series = build_series(_bars([(700.0, 690.0), (698.0, None), (696.0, 690.0)]))
    assert series[1].state == STATE_NO_FLIP
    assert series[1].step_pts is None
    assert series[2].step_pts is None


# --------------------------------------------------------------------------- #
# Causality
# --------------------------------------------------------------------------- #
def test_readings_are_identical_live_and_after_the_fact():
    """Every value looks only backward, so a bar reads the same during the
    session as it does once the session is over."""
    pairs = [(720.0 - i, 690.0) for i in range(12)]
    full = build_series(_bars(pairs))
    truncated = build_series(_bars(pairs[:7]))

    assert [b.rate_pts for b in truncated] == [b.rate_pts for b in full[:7]]
    assert [b.state for b in truncated] == [b.state for b in full[:7]]


# --------------------------------------------------------------------------- #
# The one-line read
# --------------------------------------------------------------------------- #
def test_describe_matches_the_spec_shape():
    series = build_series(_bars([(720.0, 690.0), (719.0, 690.0), (717.0, 690.0), (708.0, 690.0)]))
    line = describe(series[3])

    assert "Flip cushion: 18 pts above" in line
    assert "5m: narrowing 9 pts" in line
    assert "15m: narrowing 12 pts, accelerating" in line


def test_describe_says_so_when_there_is_no_flip():
    series = build_series(_bars([(700.0, None)]))
    assert "no gamma flip" in describe(series[0])


# --------------------------------------------------------------------------- #
# Rate context: thin-but-stable versus thin-and-collapsing
# --------------------------------------------------------------------------- #
def test_rate_context_grades_the_trailing_window():
    assert classify_rate(-MOVE * 0.02, MOVE)[1] == RATE_STABLE
    assert classify_rate(MOVE * 0.40, MOVE)[1] == RATE_DRIFTING
    assert classify_rate(-MOVE * 0.25, MOVE)[1] == RATE_CONTRACTING
    assert classify_rate(-MOVE * 0.80, MOVE)[1] == RATE_ACCELERATING


def test_a_widening_cushion_is_never_graded_past_drifting():
    """A cushion opening up quickly is not a risk condition, and an urgent
    label for it would be noise dressed as a warning."""
    assert classify_rate(MOVE * 5.0, MOVE)[1] == RATE_DRIFTING


def test_rate_context_is_scale_free():
    """Same fraction of a typical move, same label, whatever the instrument."""
    small = classify_rate(-4.0, 10.0)[1]
    large = classify_rate(-400.0, 1000.0)[1]
    assert small == large == RATE_ACCELERATING


def test_rate_context_is_absent_without_a_move_scale():
    assert classify_rate(-10.0, None) == (None, None)
    assert classify_rate(None, MOVE) == (None, None)


def test_thin_and_stable_is_distinguishable_from_thin_and_collapsing():
    """The distinction Barrie asked for: the cushion state is the same in both
    and only the rate says which condition you are actually in."""
    stable = build_series(
        [(T0 + timedelta(minutes=5 * i), 700.0 - 0.05 * i, 692.0, MOVE) for i in range(5)]
    )[-1]
    # Ends at 9 points, the same THIN band as the stable case, having given up
    # 12 points getting there. Both must land in THIN or the test is comparing
    # states rather than rates.
    collapsing = build_series(
        [(T0 + timedelta(minutes=5 * i), 717.0 - 4.0 * i, 692.0, MOVE) for i in range(5)]
    )[-1]

    assert stable.state == collapsing.state == STATE_THIN
    assert stable.rate_context == RATE_STABLE
    assert collapsing.rate_context == RATE_ACCELERATING


def test_the_move_scale_is_carried_on_every_bar():
    """Stored per bar rather than recomputed, so a historical reading always
    shows the yardstick that was actually in force at the time."""
    bar = build_series([(T0, 700.0, 690.0, MOVE)])[0]

    assert bar.move_30m == MOVE
    assert bar.move_ratio == pytest.approx(10.0 / MOVE)
    assert bar.basis == BASIS_MOVE
