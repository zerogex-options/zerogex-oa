"""The two opening-range guards have to survive a ONE-minute opening range.

Both were written when the brief said 5 / 15 / 30 minutes, and both quietly
assumed it. The tester whose idea this study exists to test runs the 1-minute
range as his primary setting, which is how the assumptions surfaced.

``min_or_bars`` was a fixed 2. A 1-minute window holds exactly one minute bar,
so every session was skipped and a 1-minute sweep returned an empty sample
rather than an answer. Loud, but only once somebody ran it.

``min_or_width_bp`` was a fixed 2.0, and the number was right for one reason
nobody had written down: a ladder step is ``extension_step * R``, so a step
narrower than the touch band makes adjacent rungs indistinguishable, and
1.0 / 0.5 is 2.0. Hard-coding it meant the touch sweep -- which runs
``touch_tolerance_bp`` out to 5 bp -- kept a 2 bp floor where it needed 10,
and measured rungs it could not tell apart. That one is silent.

These tests pin the fix in both directions: the defaults must not move (the
fingerprint is a run's identity), and the guards must now track the parameters
they depend on.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from research.or_gamma_confluence.config import MIN_OR_WIDTH_FLOOR_BP, ResearchConfig
from research.or_gamma_confluence.ranges import Bar, build_opening_range

SESSION = date(2026, 9, 4)


def _bars(n: int, high: float, low: float, tz) -> list[Bar]:
    start = datetime.combine(SESSION, time(9, 30), tzinfo=tz)
    return [
        Bar(start + timedelta(minutes=i), (high + low) / 2, high, low, (high + low) / 2)
        for i in range(n)
    ]


def _tz(cfg: ResearchConfig):
    from research.or_gamma_confluence.ranges import session_window

    return session_window(SESSION, cfg)[0].tzinfo


# --- the defaults must not move ------------------------------------------


def test_the_shipped_defaults_resolve_to_the_values_they_replaced():
    """These were hard-coded 2 and 2.0. A run's identity is its config
    fingerprint, so a derived default that landed anywhere else would orphan
    every cached dataset."""
    cfg = ResearchConfig()
    assert cfg.min_or_bars == 2
    assert cfg.min_or_width_bp == 2.0


def test_deriving_a_default_does_not_change_the_fingerprint():
    derived = ResearchConfig()
    explicit = ResearchConfig(min_or_bars=2, min_or_width_bp=2.0)
    assert derived.fingerprint() == explicit.fingerprint()


def test_a_zero_touch_band_does_not_dissolve_the_width_floor():
    """The suite's own helper builds configs with touch_tolerance_bp=0 to make
    touches exact. Deriving the floor purely from the touch band would have
    turned that into no floor at all and let degenerate ranges through."""
    assert ResearchConfig(touch_tolerance_bp=0.0).min_or_width_bp == MIN_OR_WIDTH_FLOOR_BP


# --- min_or_bars: the blocking fix ---------------------------------------


@pytest.mark.parametrize(
    "minutes,expected",
    [(1, 1), (2, 2), (5, 2), (15, 2), (30, 2)],
)
def test_min_or_bars_never_exceeds_the_window_it_measures(minutes, expected):
    assert ResearchConfig(opening_range_minutes=minutes).min_or_bars == expected


def test_a_one_minute_opening_range_actually_builds():
    """The end of the story: before this, every 1-minute session was skipped."""
    cfg = ResearchConfig(opening_range_minutes=1)
    orange, reason = build_opening_range(_bars(1, 29600.0, 29500.0, _tz(cfg)), SESSION, "NQ", cfg)
    assert reason is None
    assert orange is not None and orange.n_bars == 1
    assert (orange.high, orange.low) == (29600.0, 29500.0)


def test_asking_for_more_bars_than_the_window_holds_raises():
    """Rather than silently never matching. A config that cannot be satisfied
    is a config that lies about what the run measured."""
    with pytest.raises(ValueError, match="cannot hold"):
        ResearchConfig(opening_range_minutes=1, min_or_bars=2)


def test_min_or_bars_must_be_at_least_one():
    with pytest.raises(ValueError, match="at least 1"):
        ResearchConfig(min_or_bars=0)


# --- min_or_width_bp: the silent one -------------------------------------


@pytest.mark.parametrize(
    "touch_bp,step,expected",
    [
        (1.0, 0.5, 2.0),  # the shipped pair, and where the old constant came from
        (0.5, 0.5, 2.0),  # derived term below the absolute floor, floor wins
        (2.0, 0.5, 4.0),
        (5.0, 0.5, 10.0),  # the wide end of the touch sweep
        (1.0, 0.25, 4.0),  # a finer ladder needs a wider range, not a narrower one
    ],
)
def test_the_width_floor_tracks_the_touch_band_and_the_ladder_step(touch_bp, step, expected):
    cfg = ResearchConfig(touch_tolerance_bp=touch_bp, extension_step=step)
    assert cfg.min_or_width_bp == expected


def test_a_floor_below_the_touch_band_raises():
    """One ladder step would be narrower than the band that decides a touch,
    so 'reached +150%' and 'reached +200%' would be the same statement."""
    with pytest.raises(ValueError, match="below"):
        ResearchConfig(touch_tolerance_bp=5.0, extension_step=0.5, min_or_width_bp=2.0)


def test_demanding_a_wider_range_than_derived_is_allowed():
    assert ResearchConfig(min_or_width_bp=25.0).min_or_width_bp == 25.0


# --- the skip reasons have to aggregate ----------------------------------


def test_skip_reasons_are_constant_across_sessions_so_they_can_be_counted():
    """The run's .meta.json counts skip reasons by exact string. Embedding the
    MEASURED width gave every dropped session a unique key with a count of one,
    so "a third of the sample fell under the floor" -- the sample-selection
    effect a 1-minute range makes real -- appeared nowhere."""
    cfg = ResearchConfig(opening_range_minutes=1)
    tz = _tz(cfg)

    reasons = set()
    for width in (0.5, 1.0, 2.0, 3.0):  # several different too-narrow ranges
        _orange, reason = build_opening_range(
            _bars(1, 29500.0 + width, 29500.0, tz), SESSION, "NQ", cfg
        )
        assert reason is not None
        reasons.add(reason)

    assert len(reasons) == 1, f"reasons must aggregate, got {reasons}"
    assert reasons == {f"or_width_below_min_{cfg.min_or_width_bp}bp"}


def test_the_thin_window_reason_is_constant_too():
    cfg = ResearchConfig(opening_range_minutes=5, min_or_bars=4)
    tz = _tz(cfg)
    seen = set()
    for n in (1, 2, 3):
        _orange, reason = build_opening_range(_bars(n, 29600.0, 29500.0, tz), SESSION, "NQ", cfg)
        seen.add(reason)
    assert seen == {"or_bars_below_min_4"}


# --- variant() has to re-derive, or the sweep never sees the fix ---------


def test_variant_re_derives_a_width_floor_it_did_not_choose():
    """The sweep varies touch_tolerance_bp through variant(). replace() hands
    the resolved default back as though the caller had asked for it, which
    would pin a 2 bp floor at a 5 bp touch band -- the exact miscalibration
    deriving it exists to prevent."""
    base = ResearchConfig()
    assert base.min_or_width_bp == 2.0
    assert base.variant(touch_tolerance_bp=5.0).min_or_width_bp == 10.0


def test_variant_re_derives_min_or_bars_so_a_one_minute_cell_can_run():
    """Without this the OR sweep raises on its 1-minute cell instead of
    narrowing the bar requirement to fit the window."""
    assert ResearchConfig().variant(opening_range_minutes=1).min_or_bars == 1


def test_variant_carries_an_explicit_value_through_untouched():
    explicit = ResearchConfig(min_or_width_bp=4.0)
    assert explicit.variant(touch_tolerance_bp=0.5).min_or_width_bp == 4.0


def test_an_explicit_override_on_the_variant_itself_wins():
    assert ResearchConfig().variant(min_or_width_bp=25.0).min_or_width_bp == 25.0


def test_the_derived_marker_stays_out_of_the_fingerprint():
    """It is provenance about provenance. If it reached to_dict it would make
    a derived config and an identically-valued explicit one different runs."""
    assert "_derived" not in ResearchConfig().to_dict()
