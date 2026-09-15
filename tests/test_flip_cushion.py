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
    CROSSING_SPAN,
    DEFAULT_RATE_BARS,
    SIDE_ABOVE,
    SIDE_BELOW,
    STATE_CROSSING,
    STATE_NO_FLIP,
    STATE_SECURE,
    STATE_THIN,
    THIN_SPAN,
    build_series,
    classify,
    describe,
    measure,
)

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
def test_same_points_classify_differently_across_symbols():
    """Ten points is a live crossing risk on SPX and a comfortable cushion on
    a low-priced underlying. A points threshold cannot express that."""
    spx = measure(5000.0, 4990.0)[1]
    spy = measure(700.0, 690.0)[1]

    assert classify(spx) == STATE_CROSSING
    assert classify(spy) == STATE_THIN


def test_secure_thin_and_crossing_boundaries():
    assert classify(THIN_SPAN * 2) == STATE_SECURE
    assert classify(THIN_SPAN) == STATE_THIN
    assert classify(CROSSING_SPAN) == STATE_CROSSING


def test_classification_ignores_which_side_we_are_on():
    assert classify(THIN_SPAN * 0.5) == classify(-THIN_SPAN * 0.5)


def test_no_flip_is_its_own_state_not_a_secure_cushion():
    """A profile with no crossing at all is a different statement from one
    whose crossing is far away."""
    assert classify(None) == STATE_NO_FLIP
    assert classify(None) != STATE_SECURE


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
