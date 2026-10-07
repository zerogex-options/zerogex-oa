"""The wall stability report counts flips the way the production report did.

Its database reads are exercised end to end by hand against a scratch
Postgres; these pin the pure parts: the flip/change count and the replay.
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


def test_replay_old_rule_flickers_and_new_rule_holds():
    rows = [
        {"strike": 7650.0, "call_gamma": 0.0, "put_gamma": 60.0},
        {"strike": 7700.0, "call_gamma": 100.0, "put_gamma": 100.0},
        {"strike": 7800.0, "call_gamma": 80.0, "put_gamma": 0.0},
    ]
    times = _times(8)
    spots = [7698.0, 7702.0] * 4
    old, new = replay_session(
        times,
        {t: rows for t in times},
        dict(zip(times, spots)),
        buffer_pct_of_spot=0.001,
        buffer_points=None,
        tie_pct=0.10,
    )
    result = summarize("SPX", "all", times, old, new)
    assert result.old_call.flips == 6 and result.old_put.flips == 6
    assert result.new_call.changes == 0 and result.new_put.changes == 0
    assert result.differs == 4  # the minutes spot sat above 7700
    lines = format_report([result])
    assert lines[-1].startswith("SPX    all")


def test_totals_add_up():
    a = ScopeResult(symbol="SPX", scope="all", sessions=1, minutes=10)
    a.old_call.flips = 3
    b = ScopeResult(symbol="SPY", scope="all", sessions=2, minutes=20)
    b.old_call.flips = 4
    lines = format_report([a, b])
    assert lines[-1].split()[:4] == ["TOTAL", "3", "30", "7/0"]
