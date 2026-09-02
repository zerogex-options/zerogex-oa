"""Pin stability — the session's pin path reduced to held-since + migration.

The behaviour under test is mostly about what the metric REFUSES to call a
migration.  A pin ticking to a neighbouring strike for a minute and back is a
scoring near-tie, and reporting it as movement would bury the one or two moves
that actually mattered — the exact failure the walls' flicker pruning exists to
prevent in src/jobs/level_history.py.
"""

from datetime import datetime, timedelta, timezone

from src.analytics.pin_stability import (
    MIN_HOLD_SAMPLES,
    build_pin_stability,
)

BASE = datetime(2026, 8, 31, 13, 30, tzinfo=timezone.utc)  # 09:30 ET


def _frames(values):
    """One frame per minute from BASE, carrying the given pin values."""
    return [
        {"timestamp": BASE + timedelta(minutes=i), "pin_strike": v}
        for i, v in enumerate(values)
    ]


def test_no_frames_and_no_pin_yield_nothing_rather_than_zeros():
    assert build_pin_stability([]) is None
    assert build_pin_stability(_frames([None] * 30)) is None


def test_a_pin_held_all_session_reports_the_open_and_no_migration():
    st = build_pin_stability(_frames([7730.0] * 60))
    assert st is not None
    assert st.current_pin == 7730.0
    assert st.session_open_pin == 7730.0
    assert st.net_migration == 0.0
    assert st.distinct_values == 1
    assert st.held_since == BASE
    assert st.held_samples == 60


def test_a_genuine_migration_is_reported_signed_and_from_the_open():
    # 7730 -> 7715 -> 7700, each held well past the noise floor: the session
    # Andres described, where the pin walked DOWN with the tape.
    st = build_pin_stability(_frames([7730.0] * 30 + [7715.0] * 30 + [7700.0] * 30))
    assert st is not None
    assert st.current_pin == 7700.0
    assert st.session_open_pin == 7730.0
    assert st.net_migration == -30.0
    assert st.distinct_values == 3
    # held_since is when the CURRENT value took hold, not the session open.
    assert st.held_since == BASE + timedelta(minutes=60)
    assert st.held_samples == 30


def test_a_single_minute_excursion_is_not_a_migration():
    # The 713 pin that printed 712 once and came back (Aug 24 QQQ). Pruning it
    # is the whole point: without it the level reads as having moved twice.
    values = [713.0] * 30 + [712.0] + [713.0] * 30
    st = build_pin_stability(_frames(values))
    assert st is not None
    assert st.distinct_values == 1
    assert st.current_pin == 713.0
    assert st.net_migration == 0.0
    # The flicker is absorbed into the surrounding run, so the pin has held
    # since the open rather than since the blip.
    assert st.held_since == BASE
    # held_samples counts frames the value was actually in force, so the pruned
    # excursion is neither a hold nor a quiet minute -- it is simply not 713.
    # 60 held + 0 quiet + 1 pruned = 61 total, and the accounting stays honest
    # rather than rounding the blip into the run it interrupted.
    assert st.held_samples == 60
    assert st.quiet_samples == 0
    assert st.total_samples == 61


def test_quiet_minutes_do_not_reset_the_hold():
    # pin_strike is NULL whenever no candidate clears the score floor, and that
    # happens mid-session. A pin that goes quiet and returns to the SAME strike
    # has held it throughout; claiming otherwise would invent a migration.
    values = [7730.0] * 20 + [None] * 5 + [7730.0] * 20
    st = build_pin_stability(_frames(values))
    assert st is not None
    assert st.distinct_values == 1
    assert st.held_since == BASE
    assert st.quiet_samples == 5
    assert st.total_samples == 45
    # Quiet minutes are reported, never folded into the held count.
    assert st.held_samples == 40


def test_the_current_run_survives_pruning_however_new_it_is():
    # A move that just happened is still where the pin IS. Dropping it for
    # being short would answer "where is the pin" with the previous one.
    values = [7730.0] * 40 + [7700.0] * (MIN_HOLD_SAMPLES - 1)
    st = build_pin_stability(_frames(values))
    assert st is not None
    assert st.current_pin == 7700.0
    assert st.distinct_values == 2
    assert st.net_migration == -30.0


def test_non_positive_and_unparseable_strikes_are_not_pins():
    st = build_pin_stability(_frames([0.0, -1.0, None, 7730.0] + [7730.0] * 10))
    assert st is not None
    assert st.current_pin == 7730.0
    assert st.session_open_pin == 7730.0
    # The three junk rows count as quiet, not as an occupied strike.
    assert st.distinct_values == 1
    assert st.quiet_samples == 3


def test_to_dict_renders_held_since_as_an_iso_string():
    st = build_pin_stability(_frames([7730.0] * 15))
    assert st is not None
    d = st.to_dict()
    assert d["held_since"] == BASE.isoformat()
    assert d["current_pin"] == 7730.0
    assert d["net_migration"] == 0.0
