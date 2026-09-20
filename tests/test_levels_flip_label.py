"""The one line a panel can draw when there is no flip to draw.

The NinjaTrader indicator is compiled by hand on a tester's machine from a
file sent by email, so anything baked into it ships once and then never
changes. The 8% is GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT and the wording is a
product decision; neither belongs in C#. The server therefore hands the client
a finished string and the client draws it without deciding anything.

Which makes the label's correctness entirely this function's problem: the
threshold has to track the config, the arrow has to point the way the flip
actually sits, and a reason with no direction available has to lose the arrow
rather than guess one -- an arrow pointing the wrong way is worse than no
arrow, because a trader would act on it.
"""

from __future__ import annotations

import pytest

from src.analytics.main_engine import (
    FLIP_REASON_BELOW_STRUCTURAL_FLOOR,
    FLIP_REASON_BEYOND_MAX_DISTANCE,
    FLIP_REASON_EDGE_ONLY,
    FLIP_REASON_NO_PROFILE,
    FLIP_REASON_ONE_SIDED,
)
from src.api.routers import levels as mod
from src.api.routers.levels import _flip_label

ALL_REASONS = [
    FLIP_REASON_NO_PROFILE,
    FLIP_REASON_ONE_SIDED,
    FLIP_REASON_EDGE_ONLY,
    FLIP_REASON_BEYOND_MAX_DISTANCE,
    FLIP_REASON_BELOW_STRUCTURAL_FLOOR,
]


def test_a_published_flip_has_no_label():
    """The client draws the number; a label alongside it would be noise."""
    assert _flip_label(None, 6936.0, 7726.0) is None
    assert _flip_label("", 6936.0, 7726.0) is None


def test_a_flip_below_spot_points_down():
    """SPX, August 2026: raw 6,936 against a spot of 7,726."""
    assert _flip_label(FLIP_REASON_BEYOND_MAX_DISTANCE, 6936.0, 7726.0) == "Flip >8%↓"


def test_a_flip_above_spot_points_up():
    """QQQ, 2026-07-28 morning: raw 791 against a spot of 676."""
    assert _flip_label(FLIP_REASON_BEYOND_MAX_DISTANCE, 791.03, 676.40) == "Flip >8%↑"


@pytest.mark.parametrize(
    "raw,spot",
    [(None, 676.0), (791.0, None), (791.0, 0.0), (None, None)],
)
def test_no_usable_direction_drops_the_arrow_rather_than_guessing(raw, spot):
    """An arrow pointing the wrong way is worse than none: a trader acts on it."""
    assert _flip_label(FLIP_REASON_BEYOND_MAX_DISTANCE, raw, spot) == "Flip >8% away"


def test_the_threshold_in_the_label_tracks_the_config(monkeypatch):
    """Hardcode it and the label lies the first time the gate is retuned."""
    monkeypatch.setattr(mod, "GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT", 0.05)
    assert _flip_label(FLIP_REASON_BEYOND_MAX_DISTANCE, 6936.0, 7726.0) == "Flip >5%↓"
    monkeypatch.setattr(mod, "GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT", 0.125)
    assert _flip_label(FLIP_REASON_BEYOND_MAX_DISTANCE, 6936.0, 7726.0) == "Flip >12.5%↓"


def test_a_one_sided_book_says_out_of_range_not_broken():
    """QQQ, 2026-07-28 afternoon: 401 profile points, every one negative."""
    assert _flip_label(FLIP_REASON_ONE_SIDED, None, 676.0) == "Flip out of range"


def test_only_the_data_fault_is_worded_like_a_data_fault():
    """Three of the five reasons are correct behavior and must not read as errors."""
    assert _flip_label(FLIP_REASON_NO_PROFILE, None, None) == "Flip no data"
    for reason in (
        FLIP_REASON_ONE_SIDED,
        FLIP_REASON_EDGE_ONLY,
        FLIP_REASON_BEYOND_MAX_DISTANCE,
        FLIP_REASON_BELOW_STRUCTURAL_FLOOR,
    ):
        assert "no data" not in _flip_label(reason, 6936.0, 7726.0)


def test_a_weak_crossing_is_unresolved_rather_than_absent():
    assert _flip_label(FLIP_REASON_EDGE_ONLY, 6936.0, 7726.0) == "Flip unresolved"
    assert _flip_label(FLIP_REASON_BELOW_STRUCTURAL_FLOOR, 6936.0, 7726.0) == "Flip unresolved"


@pytest.mark.parametrize("reason", ALL_REASONS)
def test_every_reason_produces_a_label_short_enough_for_the_panel(reason):
    """It shares a panel line with the Call Wall; a long string pushes it off."""
    label = _flip_label(reason, 6936.0, 7726.0)
    assert label is not None
    assert len(label) <= 20, label
    assert "\n" not in label


def test_an_unknown_reason_still_draws_something_honest():
    """A code added to the engine and not here must not blank the line."""
    assert _flip_label("SOME_FUTURE_CODE", None, None) == "Flip unresolved"
