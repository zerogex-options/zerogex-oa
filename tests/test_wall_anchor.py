"""Call/Put Walls ignore chop, follow a real break, and stop near-ties trading places.

A plain argmax re-run every minute made the published walls jump and snap back
639 times across SPX/SPY/NDX/QQQ in the eight sessions from 2026-09-28: about
half from spot chopping across the biggest strike (which flipped it between
Call Wall and Put Wall on every crossing), half from two near-tied strikes
trading places.  A paying member cancelled over it.

Three pieces replace that argmax, all functions of price, the clock and the
strikes, so every expiration selection behaves the same:

* the wall anchor -- strikes are split on a sticky copy of spot that moves
  only once price closes beyond a break buffer, so a strike keeps its role
  while price lingers around it and hands it over the minute price plows
  through;
* the tie zone -- strikes within 10% of the biggest are tied and the one price
  reaches first wins, so a clearly bigger strike still takes over at once;
* re-pick timing -- the anchor and tie zone alone only halved the flicker
  (930 -> 443 on 2026-10-01..07): strike sizes wobble as price jiggles and
  every minute's re-pick near a tie could flip.  So the walls are re-picked
  only on the minute price breaks out or when the 15-minute re-check clock
  runs out, and held in between.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.analytics.main_engine import AnalyticsEngine
from src.analytics.walls import (
    WallTracker,
    compute_call_put_walls,
    compute_wall_ladder,
    step_wall_anchor,
    wall_break_buffer,
)
from src.api.database import DatabaseManager, _scope_replay_frame_levels
from tests.test_strike_profile_timeseries_grouping import _row, _run

# conftest stubs these for every test; captured at import, before it does.
_REAL_READ_STORED_STATE = AnalyticsEngine._read_stored_wall_state
_REAL_TYPICAL_MOVE = AnalyticsEngine._wall_typical_move

T0 = datetime(2026, 9, 28, 16, 42, tzinfo=timezone.utc)  # 12:42 ET


def _minute(n: int) -> datetime:
    return T0 + timedelta(minutes=n)


def _changes(series) -> int:
    return sum(1 for a, b in zip(series, series[1:]) if a != b)


def _replay(path, rows_at, *, buffer_pct=0.001, refresh_minutes=15):
    """Walk a price path minute by minute -> (old argmax walls, published walls)."""
    tracker = WallTracker(refresh_minutes)
    raw, new = [], []
    for i, spot in enumerate(path):
        rows = rows_at(i)
        raw.append(compute_call_put_walls(rows, spot, tie_pct=0.0))
        step = tracker.update(spot, spot * buffer_pct, _minute(i))
        if step.refreshed:
            tracker.hold(compute_call_put_walls(rows, spot, anchor=step.anchor))
        new.append(tracker.held())
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


# ---------------------------------------------------------------------------
# WallTracker: when the walls are re-picked
# ---------------------------------------------------------------------------


def test_first_bucket_of_the_day_re_picks():
    step = WallTracker(15).update(7700.0, 7.7, _minute(0))
    assert step.refreshed and step.refresh_ts == _minute(0) and step.anchor == 7700.0


def test_lingering_holds_until_the_re_check_clock():
    tracker = WallTracker(15)
    tracker.update(7700.0, 7.7, _minute(0))
    tracker.hold("walls@0")
    for m in range(1, 15):
        step = tracker.update(7700.0 + (3 if m % 2 else -3), 7.7, _minute(m))
        assert not step.refreshed and step.refresh_ts == _minute(0)
        assert tracker.held() == "walls@0"
    step = tracker.update(7701.0, 7.7, _minute(15))
    assert step.refreshed and step.refresh_ts == _minute(15)


def test_a_breakout_re_picks_on_that_minute():
    tracker = WallTracker(15)
    tracker.update(7700.0, 7.7, _minute(0))
    tracker.hold("walls@0")
    assert not tracker.update(7705.0, 7.7, _minute(1)).refreshed
    step = tracker.update(7712.0, 7.7, _minute(2))
    assert step.refreshed and step.anchor == pytest.approx(7704.3)
    assert tracker.held() is None  # until the caller holds this minute's walls


def test_refresh_minutes_zero_re_picks_every_minute():
    tracker = WallTracker(0)
    for m in range(3):
        assert tracker.update(7700.0, 7.7, _minute(m)).refreshed
        tracker.hold(m)


def test_a_bucket_steps_from_the_prior_bucket_however_often_it_is_recomputed():
    tracker = WallTracker(15)
    tracker.update(7700.0, 7.7, _minute(0))
    tracker.hold("walls@0")
    # Minute 1, pass 1: a spike re-picks ...
    assert tracker.update(7720.0, 7.7, _minute(1)).refreshed
    tracker.hold("abandoned")
    # ... pass 2, the minute's final spot is back near 7700: it holds, and
    # holds the walls of the last REAL re-pick, not the abandoned pass.
    step = tracker.update(7701.0, 7.7, _minute(1))
    assert not step.refreshed and step.refresh_ts == _minute(0) and step.anchor == 7700.0
    assert tracker.held() == "walls@0"
    # Minute 2 steps from minute 1's final state.
    step = tracker.update(7709.0, 7.7, _minute(2))
    assert step.refreshed and step.anchor == pytest.approx(7701.3)


def test_a_re_pick_whose_walls_never_arrived_re_picks_again():
    tracker = WallTracker(15)
    tracker.update(7700.0, 7.7, _minute(0))  # no hold(): the summary failed
    assert tracker.update(7700.0, 7.7, _minute(1)).refreshed


def test_the_tracker_starts_over_each_new_york_day():
    tracker = WallTracker(15)
    tracker.update(7700.0, 7.7, _minute(0))
    tracker.hold("yesterday")
    step = tracker.update(7705.0, 7.7, _minute(1) + timedelta(days=1))
    assert step.refreshed and step.anchor == 7705.0


def test_seed_resumes_the_stored_state():
    tracker = WallTracker(15)
    tracker.seed(_minute(-1), 7700.0, _minute(-5), "stored walls")
    step = tracker.update(7705.0, 7.7, _minute(0))
    assert not step.refreshed and step.refresh_ts == _minute(-5) and step.anchor == 7700.0
    assert tracker.held() == "stored walls"
    # A row from before re-pick timing existed (no refresh_ts) re-picks.
    legacy = WallTracker(15)
    legacy.seed(_minute(-1), 7700.0, None, "stored walls")
    assert legacy.update(7705.0, 7.7, _minute(0)).refreshed


# ---------------------------------------------------------------------------
# The ladder rules
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


def test_sizes_that_wobble_past_the_tie_zone_do_not_flip_while_price_lingers():
    """What the anchor and tie zone alone left behind: two strikes whose sizes
    swing 30% minute to minute as price jiggles (0DTE gamma).  Re-picked every
    minute they flip every minute; with re-pick timing they hold until the
    re-check clock."""

    def rows_at(i):
        a, b = (100.0, 70.0) if i % 2 else (70.0, 100.0)
        return [
            {"strike": 7710.0, "call_gamma": a, "put_gamma": 0.0},
            {"strike": 7725.0, "call_gamma": b, "put_gamma": 0.0},
            {"strike": 7690.0, "call_gamma": 0.0, "put_gamma": 50.0},
        ]

    path = [7700.0 + (2 if i % 2 else -2) for i in range(14)]
    _, every_minute = _replay(path, rows_at, refresh_minutes=0)
    _, timed = _replay(path, rows_at)
    assert _changes([c for c, _ in every_minute]) == 13
    assert _changes([c for c, _ in timed]) == 0


def test_every_expiration_selection_breaks_on_the_same_minute():
    """The anchor and the re-pick minutes are price only: a 0DTE view and the
    All view split and re-pick on the same numbers, so a wall both rank at 7700
    breaks on the same move."""
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


def _engine_rows():
    """SPX_ROWS with the extra per-strike fields the summary also reads."""
    return [
        {**r, "net_gex": r["call_gamma"] - r["put_gamma"], "call_oi": 100, "put_oi": 100}
        for r in SPX_ROWS
    ]


def test_engine_holds_its_walls_between_re_picks_and_stamps_the_state():
    engine = _engine()

    def cycle(minute, spot, call_wall, put_wall):
        step, buffer = engine._advance_walls(spot, _minute(minute))
        summary = {
            "call_wall": call_wall,
            "put_wall": put_wall,
            "call_wall_strength": 1.0,
            "put_wall_strength": 2.0,
        }
        engine._publish_walls(summary, step, buffer)
        return summary

    first = cycle(0, 7699.7, 7700.0, 7650.0)
    assert (first["call_wall"], first["put_wall"]) == (7700.0, 7650.0)
    assert first["wall_refresh_ts"] == _minute(0) and first["wall_anchor"] == 7699.7
    assert first["wall_break_buffer"] == pytest.approx(7.6997)
    # Price lingers; this minute's raw pick differs, the published walls hold.
    held = cycle(1, 7702.4, 7800.0, 7700.0)
    assert (held["call_wall"], held["put_wall"]) == (7700.0, 7650.0)
    assert held["wall_refresh_ts"] == _minute(0)
    # Price breaks out: re-picked on that minute.
    moved = cycle(2, 7712.0, 7800.0, 7700.0)
    assert (moved["call_wall"], moved["put_wall"]) == (7800.0, 7700.0)
    assert moved["wall_refresh_ts"] == _minute(2)
    assert engine._advance_walls(0.0, _minute(3)) == (None, None)


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


def test_restart_resumes_the_walls_and_anchor_stored_earlier_today(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(AnalyticsEngine, "_read_stored_wall_state", _REAL_READ_STORED_STATE)
    stored = (
        _minute(-1),
        Decimal("7699.0"),
        _minute(-6),
        Decimal("7700"),
        Decimal("7650"),
        1.5e9,
        2.5e9,
    )
    with _fake_db(row=stored) as cursor:
        step, _ = engine._advance_walls(7705.0, _minute(0))
    sql, params = cursor.execute.call_args[0]
    assert "timestamp < %s" in sql  # strictly before the bucket being rewritten
    assert params[0] == "SPX" and params[1] == _minute(0)
    assert step.anchor == 7699.0 and not step.refreshed and step.refresh_ts == _minute(-6)
    summary = {"call_wall": 7800.0, "put_wall": 7700.0}
    engine._publish_walls(summary, step, 7.7)
    assert summary["call_wall"] == 7700.0 and summary["put_wall"] == 7650.0
    assert summary["call_wall_strength"] == 1.5e9


def test_a_failed_state_read_starts_at_spot(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(AnalyticsEngine, "_read_stored_wall_state", _REAL_READ_STORED_STATE)
    with _fake_db(error=RuntimeError("db down")):
        step, _ = engine._advance_walls(7705.0, _minute(0))
    assert step.anchor == 7705.0 and step.refreshed


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


def test_flip_surface_shows_the_published_walls():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    far = date(2026, 10, 16)

    def opt(strike, kind, oi):
        return {
            "option_symbol": f"SPY {strike}{kind}",
            "strike": strike,
            "expiration": far,
            "option_type": kind,
            "open_interest": oi,
            "implied_volatility": 0.30,
            "gamma": 0.01,
            "volume": 0,
            "delta": 0.5,
            "theta": 0.0,
            "vega": 0.0,
        }

    options = [opt(92.0, "P", 180000), opt(108.0, "C", 420000), opt(104.0, "C", 1000)]
    surface = engine.compute_flip_surface(
        options,
        100.0,
        ts,
        [5.0],
        span_pct=0.10,
        step_pct=0.005,
        published_walls=(104.0, 92.0),
    )
    by_type = {w["type"]: w for w in surface["walls"]}
    assert by_type["call"]["strike"] == 104.0  # the published wall, not the 108 re-pick
    assert by_type["put"]["strike"] == 92.0


# ---------------------------------------------------------------------------
# API: every view splits on the stored anchor and re-picks on the same minutes
# ---------------------------------------------------------------------------

_TS = datetime(2026, 8, 17, 14, 30, tzinfo=timezone.utc)
_EARLIER = _TS - timedelta(minutes=7)


def _chart_rows(anchor, refresh_ts=None):
    out = []
    for strike, call_raw, put_raw in (
        (495.0, 1.0, 300.0),
        (500.0, 10.0, 100.0),
        (505.0, 200.0, 150.0),
        (515.0, 50.0, 1.0),
    ):
        r = _row(_TS, 505.5, strike=strike, call_raw=call_raw, put_raw=put_raw)
        r["wall_anchor"] = anchor
        r["rep_ts"] = _TS
        r["wall_refresh_ts"] = refresh_ts if refresh_ts is not None else _TS
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


class _DispatchConn:
    """The bucket read gets the bucket rows; the re-pick read its own rows."""

    def __init__(self, rows, refresh_rows):
        self.rows = rows
        self.refresh_rows = refresh_rows
        self.refresh_calls = []

    async def fetch(self, query, *args, **_kwargs):
        if "ANY($2::timestamptz[])" in query:
            self.refresh_calls.append(args)
            return self.refresh_rows
        return self.rows


def _run_dispatch(rows, refresh_rows, expirations=None):
    db = DatabaseManager()
    conn = _DispatchConn(rows, refresh_rows)

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    result = asyncio.run(db.get_strike_profile_timeseries("SPY", "5min", 40, expirations))
    return result, conn


# At the earlier re-pick minute 500 was the biggest call strike above 499.
_REFRESH_ROWS = [
    {"timestamp": _EARLIER, "strike": 495.0, "call_gamma": 1.0, "put_gamma": 300.0},
    {"timestamp": _EARLIER, "strike": 500.0, "call_gamma": 400.0, "put_gamma": 1.0},
    {"timestamp": _EARLIER, "strike": 505.0, "call_gamma": 200.0, "put_gamma": 1.0},
]


def test_a_held_bucket_picks_its_walls_from_the_re_pick_minute():
    result, conn = _run_dispatch(_chart_rows(499.0, refresh_ts=_EARLIER), _REFRESH_ROWS)
    assert result[0]["call_wall"] == 500.0  # from 7 minutes earlier, not this minute's 505
    assert result[0]["put_wall"] == 495.0
    # The bars are still this minute's.
    assert [float(s["strike"]) for s in result[0]["strikes"]] == [495.0, 500.0, 505.0, 515.0]
    assert len(conn.refresh_calls) == 1
    symbol, wanted, exp = conn.refresh_calls[0]
    assert symbol == "SPY" and wanted == [_EARLIER] and exp is None


def test_a_filtered_view_re_picks_from_the_same_minute_under_its_filter():
    exps = [date(2026, 8, 17)]
    result, conn = _run_dispatch(
        _chart_rows(499.0, refresh_ts=_EARLIER), _REFRESH_ROWS, expirations=exps
    )
    assert result[0]["call_wall"] == 500.0
    assert conn.refresh_calls[0][2] == exps


def test_a_bucket_that_re_picked_itself_needs_no_second_read():
    result, conn = _run_dispatch(_chart_rows(504.9), _REFRESH_ROWS)
    assert result[0]["call_wall"] == 505.0
    assert conn.refresh_calls == []


def _frame(ts, inputs, anchor, refresh_ts):
    return {
        "timestamp": ts,
        "_spot": Decimal("7703.0"),
        "_wall_anchor": anchor,
        "_wall_refresh_ts": refresh_ts,
        "_max_pain_by_expiration": None,
        "_gamma_inputs": [dict(r) for r in inputs],
    }


def test_filtered_replay_frame_splits_on_the_stored_anchor():
    held = _scope_replay_frame_levels(_frame(_TS, SPX_ROWS, 7699.0, None), [date(2026, 9, 28)])
    assert (held["call_wall"], held["put_wall"]) == (7700.0, 7650.0)
    assert "_wall_anchor" not in held and "_wall_refresh_ts" not in held
    legacy = _scope_replay_frame_levels(_frame(_TS, SPX_ROWS, None, None), [date(2026, 9, 28)])
    assert (legacy["call_wall"], legacy["put_wall"]) == (7800.0, 7700.0)


def test_filtered_replay_frame_holds_the_re_pick_minutes_walls():
    earlier_rows = [
        {"strike": 7650.0, "call_gamma": 0.0, "put_gamma": 60.0},
        {"strike": 7750.0, "call_gamma": 90.0, "put_gamma": 0.0},
    ]
    inputs_by_ts = {_EARLIER: earlier_rows, _TS: SPX_ROWS}
    frame = _scope_replay_frame_levels(
        _frame(_TS, SPX_ROWS, 7699.0, _EARLIER), [date(2026, 9, 28)], inputs_by_ts
    )
    assert (frame["call_wall"], frame["put_wall"]) == (7750.0, 7650.0)
