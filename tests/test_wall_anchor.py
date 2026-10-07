"""Call/Put Walls ignore chop, follow a real break, and stop near-ties trading places.

A plain argmax re-run every minute made the published walls jump and snap back
639 times across SPX/SPY/NDX/QQQ in the eight sessions from 2026-09-28: about
half from spot chopping across the biggest strike (which flipped it between
Call Wall and Put Wall on every crossing), half from two near-tied strikes
trading places.  A paying member cancelled over it.

Two rules replace that argmax, both functions of price and the strikes only,
so every expiration selection behaves the same:

* the wall anchor -- strikes are split on a sticky copy of spot that moves
  only once price closes beyond a break buffer, so a strike keeps its role
  while price lingers around it and hands it over the minute price plows
  through;
* the tie zone -- strikes within 10% of the biggest are tied and the one price
  reaches first wins, so a clearly bigger strike still takes over at once.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.analytics.main_engine import AnalyticsEngine
from src.analytics.walls import (
    WallAnchor,
    compute_call_put_walls,
    compute_wall_ladder,
    step_wall_anchor,
    wall_break_buffer,
)
from src.api.database import _scope_replay_frame_levels
from tests.test_strike_profile_timeseries_grouping import _row, _run

# conftest stubs these for every test; captured at import, before it does.
_REAL_READ_STORED_ANCHOR = AnalyticsEngine._read_stored_wall_anchor
_REAL_TYPICAL_MOVE = AnalyticsEngine._wall_typical_move

T0 = datetime(2026, 9, 28, 16, 42, tzinfo=timezone.utc)  # 12:42 ET


def _minute(n: int) -> datetime:
    return T0 + timedelta(minutes=n)


def _changes(series) -> int:
    return sum(1 for a, b in zip(series, series[1:]) if a != b)


def _replay(path, rows_at, *, buffer_pct=0.001):
    """Walk a price path minute by minute -> (old argmax walls, new walls)."""
    anchor = WallAnchor()
    raw, new = [], []
    for i, spot in enumerate(path):
        rows = rows_at(i)
        raw.append(compute_call_put_walls(rows, spot, tie_pct=0.0))
        a = anchor.update(spot, spot * buffer_pct, _minute(i))
        new.append(compute_call_put_walls(rows, spot, anchor=a))
    return raw, new


# SPX on the afternoon of 2026-09-28: 7700 carried the biggest call AND put
# gamma, and spot sat on it for an hour.  The old rule handed 7700 back and
# forth between Call Wall and Put Wall on every crossing.
SPX_ROWS = [
    {"strike": 7600.0, "call_gamma": 0.0, "put_gamma": 30.0},
    {"strike": 7650.0, "call_gamma": 0.0, "put_gamma": 60.0},
    {"strike": 7700.0, "call_gamma": 100.0, "put_gamma": 100.0},
    {"strike": 7750.0, "call_gamma": 40.0, "put_gamma": 0.0},
    {"strike": 7800.0, "call_gamma": 80.0, "put_gamma": 0.0},
]
# Minute closes from the production flip report, 12:41-13:10 ET.
SPX_CHOP = [7699.7, 7702.4, 7698.5, 7700.9, 7697.7, 7700.9, 7698.8, 7701.6, 7697.2, 7701.0]


# ---------------------------------------------------------------------------
# Break buffer and anchor
# ---------------------------------------------------------------------------


def test_buffer_is_the_larger_of_the_floor_and_the_typical_move_fraction():
    assert wall_break_buffer(7700.0, None) == pytest.approx(7.7)  # 0.1% floor
    assert wall_break_buffer(7700.0, 10.0) == pytest.approx(7.7)  # 0.4 x 10 < floor
    assert wall_break_buffer(7700.0, 40.0) == pytest.approx(16.0)  # fast tape
    assert wall_break_buffer(0.0, 40.0) == 0.0


def test_anchor_stays_put_while_price_lingers_and_trails_a_break():
    assert step_wall_anchor(None, 7700.0, 7.7) == 7700.0
    assert step_wall_anchor(7700.0, 7705.0, 7.7) == 7700.0  # inside the buffer
    assert step_wall_anchor(7700.0, 7712.0, 7.7) == pytest.approx(7704.3)  # plowed up
    assert step_wall_anchor(7704.3, 7690.0, 7.7) == pytest.approx(7697.7)  # plowed down


def test_anchor_steps_once_per_bucket_however_often_the_bucket_is_recomputed():
    anchor = WallAnchor()
    anchor.update(7700.0, 7.7, _minute(0))
    # Three passes over minute 1; the last one's spot is back near 7700.
    anchor.update(7720.0, 7.7, _minute(1))
    anchor.update(7715.0, 7.7, _minute(1))
    assert anchor.update(7701.0, 7.7, _minute(1)) == 7700.0
    # Minute 2 steps from minute 1's final anchor, not an intermediate pass.
    assert anchor.update(7709.0, 7.7, _minute(2)) == pytest.approx(7701.3)


def test_anchor_starts_over_at_spot_each_new_york_day():
    anchor = WallAnchor()
    anchor.update(7700.0, 7.7, _minute(0))
    assert anchor.update(7705.0, 7.7, _minute(1)) == 7700.0
    assert anchor.update(7705.0, 7.7, _minute(1) + timedelta(days=1)) == 7705.0


def test_seeded_anchor_resumes_from_the_stored_bucket():
    anchor = WallAnchor()
    anchor.seed(7700.0, _minute(-1))
    assert anchor.update(7705.0, 7.7, _minute(0)) == 7700.0


# ---------------------------------------------------------------------------
# The two rules on the ladder itself
# ---------------------------------------------------------------------------


def test_a_strike_inside_the_buffer_keeps_its_side():
    # Spot 7703 is above 7700, but the anchor (7699) is not: 7700 is still
    # the strike price has to get through, so it stays the Call Wall.
    call_wall, put_wall = compute_call_put_walls(SPX_ROWS, 7703.0, anchor=7699.0)
    assert (call_wall, put_wall) == (7700.0, 7650.0)
    # Split on spot, as before, 7700 jumps sides.
    assert compute_call_put_walls(SPX_ROWS, 7703.0, tie_pct=0.0) == (7800.0, 7700.0)


def test_tie_zone_picks_the_nearer_of_two_near_equal_strikes():
    rows = [
        {"strike": 780.0, "call_gamma": 95.0, "put_gamma": 0.0},
        {"strike": 787.0, "call_gamma": 100.0, "put_gamma": 0.0},
    ]
    call_walls, _ = compute_wall_ladder(rows, 774.5)
    assert [w["strike"] for w in call_walls] == [780.0, 787.0]
    assert call_walls[0]["strength"] == pytest.approx(95.0 * 100 * 774.5 * 774.5 * 0.01)
    # tie_pct=0 is the plain argmax.
    assert compute_call_put_walls(rows, 774.5, tie_pct=0.0)[0] == 787.0


def test_a_clearly_bigger_strike_wins_immediately():
    rows = [
        {"strike": 780.0, "call_gamma": 85.0, "put_gamma": 0.0},
        {"strike": 787.0, "call_gamma": 100.0, "put_gamma": 0.0},
    ]
    assert compute_call_put_walls(rows, 774.5)[0] == 787.0


def test_put_side_tie_zone_picks_the_highest_tied_strike():
    rows = [
        {"strike": 730.0, "call_gamma": 0.0, "put_gamma": 100.0},
        {"strike": 736.0, "call_gamma": 0.0, "put_gamma": 93.0},
        {"strike": 740.0, "call_gamma": 0.0, "put_gamma": 50.0},
    ]
    _, put_walls = compute_wall_ladder(rows, 744.0)
    assert [w["strike"] for w in put_walls] == [736.0, 730.0, 740.0]


# ---------------------------------------------------------------------------
# Replays of production patterns
# ---------------------------------------------------------------------------


def test_spx_chop_around_7700_no_longer_flips_either_wall():
    raw, new = _replay(SPX_CHOP, lambda i: SPX_ROWS)
    assert _changes([c for c, _ in raw]) == 9  # the old rule flipped every minute
    assert _changes([p for _, p in raw]) == 9
    assert set(new) == {(7700.0, 7650.0)}


def test_plowing_through_a_wall_moves_it_on_that_minute_and_lingering_does_not_undo_it():
    path = SPX_CHOP + [7712.0, 7699.0, 7701.0, 7698.0, 7690.0]
    _, new = _replay(path, lambda i: SPX_ROWS)
    chop = len(SPX_CHOP)
    assert new[chop - 1] == (7700.0, 7650.0)
    # 7712 is more than the 7.7-point buffer past 7700: the walls move now.
    assert new[chop] == (7800.0, 7700.0)
    # Back to 7698-7701: lingering, so 7700 stays support.
    assert new[chop + 1 : chop + 4] == [(7800.0, 7700.0)] * 3
    # 7690 is a decisive break back down: 7700 is resistance again.
    assert new[chop + 4] == (7700.0, 7650.0)


def test_spy_near_tie_787_780_no_longer_trades_places():
    # SPY call wall, 2026-10-07 morning: 787 and 780 within ~5% of each other
    # while spot sat near 774.5.  The old rule swapped them 10 times in 28
    # minutes.
    def rows_at(i):
        big, small = (100.0, 95.0) if i % 2 else (95.0, 100.0)
        return [
            {"strike": 780.0, "call_gamma": small, "put_gamma": 0.0},
            {"strike": 787.0, "call_gamma": big, "put_gamma": 0.0},
            {"strike": 770.0, "call_gamma": 0.0, "put_gamma": 60.0},
        ]

    path = [774.5 + (0.2 if i % 3 else -0.2) for i in range(12)]
    raw, new = _replay(path, rows_at)
    assert _changes([c for c, _ in raw]) == 11
    assert {c for c, _ in new} == {780.0}


def test_every_expiration_selection_breaks_on_the_same_minute():
    """The anchor is price only: a 0DTE view and the All view split on the
    same number, so a wall both rank at 7700 breaks on the same move."""
    zero_dte = [
        {"strike": 7690.0, "call_gamma": 0.0, "put_gamma": 20.0},
        {"strike": 7700.0, "call_gamma": 50.0, "put_gamma": 40.0},
        {"strike": 7725.0, "call_gamma": 30.0, "put_gamma": 0.0},
    ]
    path = SPX_CHOP + [7712.0, 7699.0]
    _, all_view = _replay(path, lambda i: SPX_ROWS)
    _, dte_view = _replay(path, lambda i: zero_dte)
    first_break_all = next(i for i, (c, _) in enumerate(all_view) if c != 7700.0)
    first_break_dte = next(i for i, (c, _) in enumerate(dte_view) if c != 7700.0)
    assert first_break_all == first_break_dte == len(SPX_CHOP)
    assert dte_view[len(SPX_CHOP)] == (7725.0, 7700.0)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def _engine() -> AnalyticsEngine:
    return AnalyticsEngine(underlying="SPX")


def test_engine_advances_the_anchor_and_reports_the_buffer():
    engine = _engine()
    anchor, buffer = engine._advance_wall_anchor(7699.7, _minute(0))
    assert (anchor, buffer) == (7699.7, pytest.approx(7.6997))
    anchor, _ = engine._advance_wall_anchor(7702.4, _minute(1))
    assert anchor == 7699.7
    assert engine._advance_wall_anchor(0.0, _minute(2)) == (None, None)


def _engine_rows():
    """SPX_ROWS with the extra per-strike fields the summary also reads."""
    return [
        {**r, "net_gex": r["call_gamma"] - r["put_gamma"], "call_oi": 100, "put_oi": 100}
        for r in SPX_ROWS
    ]


def test_engine_summary_splits_the_walls_on_the_anchor():
    engine = _engine()
    summary = engine._calculate_gex_summary(
        gex_by_strike=_engine_rows(),
        options=[],
        underlying_price=7703.0,
        timestamp=_minute(0),
        wall_anchor=7699.0,
    )
    assert (summary["call_wall"], summary["put_wall"]) == (7700.0, 7650.0)
    assert summary["call_wall_strength"] == pytest.approx(100.0 * 100 * 7703.0 * 7703.0 * 0.01)

    unanchored = engine._calculate_gex_summary(
        gex_by_strike=_engine_rows(), options=[], underlying_price=7703.0, timestamp=_minute(0)
    )
    assert (unanchored["call_wall"], unanchored["put_wall"]) == (7800.0, 7700.0)


@contextmanager
def _fake_db(row=None, error=None):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    conn = MagicMock()
    conn.cursor.return_value = cursor

    @contextmanager
    def _conn():
        if error is not None:
            raise error
        yield conn

    with patch("src.analytics.main_engine.db_connection", _conn):
        yield cursor


def test_restart_resumes_the_anchor_stored_earlier_today(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(AnalyticsEngine, "_read_stored_wall_anchor", _REAL_READ_STORED_ANCHOR)
    with _fake_db(row=(_minute(-1), Decimal("7699.0"))) as cursor:
        anchor, _ = engine._advance_wall_anchor(7705.0, _minute(0))
    assert anchor == 7699.0
    sql, params = cursor.execute.call_args[0]
    assert "timestamp < %s" in sql  # strictly before the bucket being rewritten
    assert params[0] == "SPX" and params[1] == _minute(0)


def test_a_failed_anchor_read_starts_at_spot(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(AnalyticsEngine, "_read_stored_wall_anchor", _REAL_READ_STORED_ANCHOR)
    with _fake_db(error=RuntimeError("db down")):
        anchor, _ = engine._advance_wall_anchor(7705.0, _minute(0))
    assert anchor == 7705.0


def test_typical_move_is_read_once_per_half_hour_and_failures_are_cached(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(AnalyticsEngine, "_wall_typical_move", _REAL_TYPICAL_MOVE)
    engine._typical_move_30m = MagicMock(return_value=40.0)
    with _fake_db():
        assert engine._wall_typical_move(_minute(0)) == 40.0
        assert engine._wall_typical_move(_minute(29)) == 40.0
        assert engine._typical_move_30m.call_count == 1
        assert engine._wall_typical_move(_minute(30)) == 40.0
        assert engine._typical_move_30m.call_count == 2

    broken = _engine()
    with _fake_db(error=RuntimeError("db down")):
        assert broken._wall_typical_move(_minute(0)) is None
    with _fake_db() as cursor:
        assert broken._wall_typical_move(_minute(1)) is None
    cursor.execute.assert_not_called()


# ---------------------------------------------------------------------------
# API: the rewind chart and the filtered replay split on the stored anchor
# ---------------------------------------------------------------------------

_TS = datetime(2026, 8, 17, 14, 30, tzinfo=timezone.utc)


def _chart_rows(anchor):
    out = []
    for strike, call_raw, put_raw in (
        (495.0, 1.0, 300.0),
        (500.0, 10.0, 100.0),
        (505.0, 200.0, 150.0),
        (515.0, 50.0, 1.0),
    ):
        r = _row(_TS, 505.5, strike=strike, call_raw=call_raw, put_raw=put_raw)
        r["wall_anchor"] = anchor
        out.append(r)
    return out


def test_chart_bucket_splits_on_its_stored_anchor():
    # Close 505.5 is past 505, but the anchor (504.9) says price has not
    # broken it: 505 stays the Call Wall.
    result, _conn, _db = _run(_chart_rows(504.9))
    assert result[0]["call_wall"] == 505.0
    assert result[0]["put_wall"] == 495.0
    assert result[0]["call_walls"][0]["label"] == "C1"


def test_chart_bucket_without_an_anchor_splits_on_its_close():
    result, _conn, _db = _run(_chart_rows(None))
    assert result[0]["call_wall"] == 515.0
    assert result[0]["put_wall"] == 495.0


def test_filtered_chart_uses_the_same_anchor():
    result, _conn, _db = _run(_chart_rows(504.9), expirations=[date(2026, 8, 17)])
    assert result[0]["call_wall"] == 505.0


def test_filtered_replay_frame_splits_on_the_stored_anchor():
    def frame(anchor):
        return {
            "timestamp": _TS,
            "_spot": Decimal("7703.0"),
            "_wall_anchor": anchor,
            "_max_pain_by_expiration": None,
            "_gamma_inputs": [dict(r) for r in SPX_ROWS],
        }

    held = _scope_replay_frame_levels(frame(7699.0), [date(2026, 9, 28)])
    assert (held["call_wall"], held["put_wall"]) == (7700.0, 7650.0)
    assert "_wall_anchor" not in held
    legacy = _scope_replay_frame_levels(frame(None), [date(2026, 9, 28)])
    assert (legacy["call_wall"], legacy["put_wall"]) == (7800.0, 7700.0)
