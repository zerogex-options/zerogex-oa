"""The per-cycle timing line.

A probe measuring the levels API found each snapshot 26-59s old the moment it
was published, swinging 30s minute to minute on a fixed 60s clock. The API
cannot say how much of that is the clock's phase within the minute and how
much is the cycle's own duration; this line can, so its arithmetic is pinned.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.analytics.main_engine import format_cycle_timing, format_loop_timing

STAMP = datetime(2026, 9, 8, 14, 12, tzinfo=timezone.utc)  # as_of 14:12:00


def test_the_line_splits_phase_from_duration_and_lists_stages_slowest_first():
    started = STAMP.timestamp() + 20.0  # the clock fired 20s into the minute
    published = started + 39.6  # and the cycle took 39.6s
    line = format_cycle_timing(
        "NDX",
        STAMP,
        started,
        published,
        {"store_results": 0.7, "snapshot": 30.2, "gex_by_strike": 6.1},
    )
    assert line.startswith("Cycle timing [NDX] snapshot=2026-09-08T14:12:00+00:00 ")
    assert "phase=+20.0s" in line
    assert "duration=39.6s" in line
    assert "publish_lag=59.6s" in line
    assert line.endswith("stages: snapshot=30.2s, gex_by_strike=6.1s, store_results=0.7s")


def test_a_naive_stamp_is_read_as_utc_rather_than_local_time():
    naive = STAMP.replace(tzinfo=None)
    started, published = STAMP.timestamp() + 5, STAMP.timestamp() + 15
    aware = format_cycle_timing("SPX", STAMP, started, published, {})
    assert format_cycle_timing("SPX", naive, started, published, {}) == aware
    assert "phase=+5.0s duration=10.0s publish_lag=15.0s stages: n/a" in aware


def test_a_cycle_that_started_before_its_stamp_shows_a_negative_phase():
    # Spot-anchored cycles can stamp a snapshot with a timestamp newer than
    # the cycle start; the sign must survive rather than be clamped.
    line = format_cycle_timing("NDX", STAMP, STAMP.timestamp() - 3.0, STAMP.timestamp() + 7.0, {})
    assert "phase=-3.0s duration=10.0s publish_lag=7.0s" in line


def test_loop_timing_line_names_the_overrun_that_moves_the_phase():
    quiet = format_loop_timing("$NDXP.X", 0.3, 4.2, 55.5, 60)
    assert quiet == "Loop timing [$NDXP.X] calc=0.3s flow=4.2s sleep=55.5s interval=60s"

    late = format_loop_timing("$NDXP.X", 0.3, 89.9, 0.0, 60)
    assert late.startswith("Loop timing [$NDXP.X] calc=0.3s flow=89.9s sleep=0.0s interval=60s")
    assert "OVERRUN by 30.2s" in late
