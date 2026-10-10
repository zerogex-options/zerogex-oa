"""The wall stability report counts flips the way the production report did.

Its database reads are exercised end to end by hand against a scratch
Postgres; these pin the pure parts: the flip/change count, the three-rule
replay and the table.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.tools.wall_stability_report import (
    ScopeResult,
    count_changes_and_flips,
    format_report,
    replay_session,
    summarize,
)

T0 = datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc)


def _times(n):
    return [T0 + timedelta(minutes=i) for i in range(n)]


def test_a_jump_that_returns_within_15_minutes_is_a_flip():
    values = [787, 780, 780, 787, 787]
    stats = count_changes_and_flips(_times(5), values)
    assert (stats.changes, stats.flips) == (2, 1)


def test_a_jump_that_stays_is_a_move_not_a_flip():
    values = [787] * 3 + [780] * 20 + [787]
    stats = count_changes_and_flips(_times(24), values)
    assert (stats.changes, stats.flips) == (2, 0)


def test_a_jump_to_no_wall_and_back_counts():
    stats = count_changes_and_flips(_times(3), [787, None, 787])
    assert stats.flips == 1


def _replay(rows_at, spots, refresh_minutes=15):
    times = _times(len(spots))
    walls = replay_session(
        times,
        {t: rows_at(i) for i, t in enumerate(times)},
        dict(zip(times, spots)),
        buffer_pct_of_spot=0.001,
        buffer_points=None,
        tie_pct=0.10,
        refresh_minutes=refresh_minutes,
    )
    return summarize("SPX", "all", times, walls)


def test_chop_across_a_strike_flips_old_and_neither_new_rule():
    rows = [
        {"strike": 7650.0, "call_gamma": 0.0, "put_gamma": 60.0},
        {"strike": 7700.0, "call_gamma": 100.0, "put_gamma": 100.0},
        {"strike": 7800.0, "call_gamma": 80.0, "put_gamma": 0.0},
    ]
    result = _replay(lambda i: rows, [7698.0, 7702.0] * 4)
    old_call, old_put = result.stats["old"]
    assert (old_call.flips, old_put.flips) == (6, 6)
    for variant in ("live", "new"):
        call, put = result.stats[variant]
        assert (call.changes, put.changes) == (0, 0)
    assert result.differs == 0  # new and live agree on every minute


def test_sizes_drifting_across_the_tie_line_flip_live_but_not_new():
    """NDX 2026-10-09 in miniature: two close strikes trade the lead, and every
    re-pick (here every minute) lands on whichever leads.  The wall in place
    keeps its place unless clearly beaten."""

    def rows_at(i):
        a, b = (100.0, 80.0) if i % 2 == 0 else (80.0, 100.0)
        return [
            {"strike": 7710.0, "call_gamma": a, "put_gamma": 0.0},
            {"strike": 7725.0, "call_gamma": b, "put_gamma": 0.0},
            {"strike": 7690.0, "call_gamma": 0.0, "put_gamma": 50.0},
        ]

    result = _replay(rows_at, [7700.0] * 10, refresh_minutes=0)
    assert result.stats["live"][0].flips == 8
    assert result.stats["new"][0].changes == 0
    assert result.differs == 5  # the minutes live handed the wall to 7725
    lines = format_report([result])
    assert lines[-1].split()[:10] == [
        "SPX",
        "all",
        "1",
        "10",
        "8/0",
        "8/0",
        "0/0",
        "9/0",
        "9/0",
        "0/0",
    ]


def test_totals_add_up():
    a = ScopeResult(symbol="SPX", scope="all", sessions=1, minutes=10)
    a.stats["old"][0].flips = 3
    b = ScopeResult(symbol="SPY", scope="all", sessions=2, minutes=20)
    b.stats["old"][0].flips = 4
    b.stats["new"][1].flips = 1
    lines = format_report([a, b])
    assert lines[-1].split()[:6] == ["TOTAL", "3", "30", "7/0", "0/0", "0/1"]
