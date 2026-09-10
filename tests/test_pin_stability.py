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
    assert st.held_pin == 7730.0
    assert st.current_established is True
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
    assert st.held_pin == 7700.0
    assert st.current_established is True
    assert st.session_open_pin == 7730.0
    assert st.net_migration == -30.0
    assert st.distinct_values == 3
    # held_since is when the CURRENT value took hold, not the session open.
    assert st.held_since == BASE + timedelta(minutes=60)
    assert st.held_samples == 30


def test_a_single_minute_excursion_is_not_a_migration():
    # The 713 pin that printed 712 once and came back (Aug 24 QQQ). Pruning it
    # is the whole point: without it the level reads as having moved twice.
    st = build_pin_stability(_frames([713.0] * 30 + [712.0] + [713.0] * 30))
    assert st is not None
    assert st.distinct_values == 1
    assert st.current_pin == 713.0
    assert st.held_pin == 713.0
    assert st.net_migration == 0.0
    assert st.held_since == BASE
    # held_samples counts frames the value was actually in force, so the pruned
    # excursion is neither a hold nor a quiet minute -- it is simply not 713.
    # 60 held + 0 quiet + 1 pruned = 61 total: the accounting stays honest
    # rather than rounding the blip into the run it interrupted.
    assert st.held_samples == 60
    assert st.quiet_samples == 0
    assert st.total_samples == 61


def test_a_late_tick_is_provisional_not_a_migration():
    """The regression this fix exists for, taken from live SPX data.

    The pin sat at 7675 for essentially the whole session and ticked to 7670 on
    the FINAL sample.  The first implementation exempted the last run from
    flicker pruning -- reasonable, since it is where the pin is -- and the
    exemption promoted a one-minute print at the bell into a reported "-5 pts
    today".  Mid-session that same blip was pruned, so the metric contradicted
    itself depending on where in the session the tick landed.

    Now the two questions are answered separately: the pin IS 7670, and no
    migration has been established.
    """
    st = build_pin_stability(_frames([7675.0] * 389 + [7670.0]))
    assert st is not None
    # Where the pin is, unqualified.
    assert st.current_pin == 7670.0
    assert st.current_samples == 1
    # ...but it has not settled, so nothing has migrated.
    assert st.current_established is False
    assert st.held_pin == 7675.0
    assert st.held_since == BASE
    assert st.net_migration == 0.0
    assert st.distinct_values == 1
    assert st.total_samples == 390


def test_a_late_tick_becomes_a_migration_once_it_settles():
    """The same tick, once it has actually held: now it is a real move."""
    st = build_pin_stability(_frames([7675.0] * 380 + [7670.0] * MIN_HOLD_SAMPLES))
    assert st is not None
    assert st.current_established is True
    assert st.held_pin == 7670.0
    assert st.net_migration == -5.0
    assert st.distinct_values == 2
    assert st.held_since == BASE + timedelta(minutes=380)


def test_an_unsettled_opening_reports_no_migration_yet():
    # Minutes after the open nothing has cleared the floor. The honest reading
    # is "no migration established", not a move measured between two levels
    # neither of which has held.
    st = build_pin_stability(_frames([7730.0, 7730.0, 7725.0]))
    assert st is not None
    assert st.current_pin == 7725.0
    assert st.current_established is False
    assert st.held_pin == 7725.0
    assert st.net_migration == 0.0
    assert st.distinct_values == 1


def test_quiet_minutes_do_not_reset_the_hold():
    # pin_strike is NULL whenever no candidate clears the score floor, and that
    # happens mid-session. A pin that goes quiet and returns to the SAME strike
    # has held it throughout; claiming otherwise would invent a migration.
    st = build_pin_stability(_frames([7730.0] * 20 + [None] * 5 + [7730.0] * 20))
    assert st is not None
    assert st.distinct_values == 1
    assert st.held_since == BASE
    assert st.quiet_samples == 5
    assert st.total_samples == 45
    # Quiet minutes are reported, never folded into the held count.
    assert st.held_samples == 40


def test_non_positive_and_unparseable_strikes_are_not_pins():
    st = build_pin_stability(_frames([0.0, -1.0, None, 7730.0] + [7730.0] * 10))
    assert st is not None
    assert st.current_pin == 7730.0
    assert st.held_pin == 7730.0
    assert st.session_open_pin == 7730.0
    # The three junk rows count as quiet, not as an occupied strike.
    assert st.distinct_values == 1
    assert st.quiet_samples == 3


def test_to_dict_renders_timestamps_as_iso_strings():
    st = build_pin_stability(_frames([7730.0] * 15))
    assert st is not None
    d = st.to_dict()
    assert d["held_since"] == BASE.isoformat()
    assert d["current_since"] == BASE.isoformat()
    assert d["current_established"] is True
    assert d["current_pin"] == 7730.0
    assert d["net_migration"] == 0.0
