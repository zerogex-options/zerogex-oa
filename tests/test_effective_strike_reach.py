"""Tests for the effective strike reach of an ingested band.

``INGEST_STRIKE_PCT_RANGE`` is not what the selected strikes actually span:
selection filters to that band and then trims the furthest strikes inward to
fit ``INGEST_STRIKE_COUNT_MAX``, so on a dense chain the cap binds first and
the realized band is narrower than the configured percentage. Nothing
downstream can tell -- a truncated chain and a genuinely short one look
identical once the strikes are gone -- which is what these pin down.
"""

import pytest

from src.ingestion.stream_manager import effective_strike_reach_pct


def test_symmetric_band_reaches_its_half_width():
    spot = 500.0
    strikes = [spot * (1 - 0.04), spot, spot * (1 + 0.04)]
    assert effective_strike_reach_pct(strikes, spot) == pytest.approx(0.04, abs=1e-9)


def test_reach_is_the_narrower_wing_not_the_wider():
    """6% up and 1% down is a 1% reach: a ±X% consumer needs X% on BOTH sides."""
    spot = 500.0
    strikes = [spot * (1 - 0.01), spot, spot * (1 + 0.06)]
    assert effective_strike_reach_pct(strikes, spot) == pytest.approx(0.01, abs=1e-9)


def test_one_sided_selection_reaches_nothing():
    """All strikes above spot: the downside wing is unmeasured, so reach is 0."""
    spot = 500.0
    strikes = [spot * 1.02, spot * 1.05, spot * 1.08]
    assert effective_strike_reach_pct(strikes, spot) == 0.0


def test_count_cap_truncation_is_visible_in_the_reach():
    """The production failure, in miniature.

    A ±4% band on a $5-strike chain around 7765 holds ~250 strikes; a count cap
    of 80 keeps the 80 nearest, which reach only ~±2.6%. The configured
    percentage is untouched and the realized reach is what changed.
    """
    spot = 7765.0
    all_in_band = [s for s in range(7460, 8076, 5) if abs(s - spot) / spot <= 0.04]
    assert effective_strike_reach_pct(all_in_band, spot) > 0.039

    capped = sorted(all_in_band, key=lambda s: abs(s - spot))[:80]
    reach = effective_strike_reach_pct(capped, spot)
    assert reach == pytest.approx(0.0257, abs=0.002)
    assert reach < 0.04  # below the wing window, with the pct range still at 4%


def test_degenerate_inputs_are_zero_not_an_error():
    assert effective_strike_reach_pct([], 500.0) == 0.0
    assert effective_strike_reach_pct([495.0, 505.0], 0.0) == 0.0
    assert effective_strike_reach_pct([495.0, 505.0], -1.0) == 0.0
    assert effective_strike_reach_pct([500.0], 500.0) == 0.0
