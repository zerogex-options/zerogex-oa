"""The published Call/Put Wall holds until a new strike has won for 5 minutes.

The wall ranking is a plain argmax re-run every minute.  Two near-tied strikes,
or spot chopping across the biggest strike, made the published wall jump and
come straight back: 639 such jump-and-revert events across SPX/SPY/NDX/QQQ in
the eight sessions from 2026-09-28, and a paying member cancelled over it.
``WallHold`` debounces the series; the analytics engine publishes the held
value, and the rewind chart's unfiltered view reads it back.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.analytics.main_engine import AnalyticsEngine
from src.analytics.walls import WallHold, wall_strength_at
from tests.test_strike_profile_timeseries_grouping import (
    _BUCKET1,
    _TS1,
    _row,
    _run,
)

# conftest stubs the seed for every test; captured at import, before it does.
_REAL_INIT_WALL_HOLDS = AnalyticsEngine._init_wall_holds

T0 = datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc)  # 10:00 ET


def _minute(n: int) -> datetime:
    return T0 + timedelta(minutes=n)


def _feed(hold: WallHold, raws):
    return [hold.update(raw, _minute(i)) for i, raw in enumerate(raws)]


def _changes(series) -> int:
    return sum(1 for a, b in zip(series, series[1:]) if a != b)


# ---------------------------------------------------------------------------
# WallHold
# ---------------------------------------------------------------------------


def test_a_jump_that_reverts_within_five_minutes_is_never_published():
    hold = WallHold(5)
    shown = _feed(hold, [700, 705, 705, 705, 705, 705, 700, 700])
    assert shown == [700.0] * 8


def test_a_new_wall_is_published_once_it_has_won_for_five_minutes():
    hold = WallHold(5)
    shown = _feed(hold, [700, 705, 705, 705, 705, 705, 705, 705])
    # First seen at minute 1, published at minute 6 (5 minutes later).
    assert shown == [700.0, 700.0, 700.0, 700.0, 700.0, 700.0, 705.0, 705.0]


def test_a_different_challenger_restarts_the_clock():
    hold = WallHold(5)
    shown = _feed(hold, [700, 705, 705, 705, 710, 710, 710, 710, 710, 710])
    # 705 never lasts 5 minutes; 710 is first seen at minute 4.
    assert shown[:9] == [700.0] * 9
    assert shown[9] == 710.0


def test_one_cycle_without_a_wall_does_not_blank_it():
    hold = WallHold(5)
    assert _feed(hold, [700, None, 700]) == [700.0, 700.0, 700.0]


def test_a_wall_that_stays_gone_is_cleared_after_the_hold():
    hold = WallHold(5)
    shown = _feed(hold, [700, None, None, None, None, None, None])
    assert shown[-2] == 700.0
    assert shown[-1] is None


def test_first_observation_of_a_new_day_is_published_immediately():
    hold = WallHold(5)
    hold.update(700, T0)
    next_morning = T0 + timedelta(days=1)
    assert hold.update(750, next_morning) == 750.0


def test_the_day_boundary_is_new_york_not_utc():
    hold = WallHold(5)
    # 19:30 ET and 20:30 ET are the same New York day but straddle UTC midnight.
    evening = datetime(2026, 10, 7, 23, 30, tzinfo=timezone.utc)
    hold.update(700, evening)
    assert hold.update(705, evening + timedelta(hours=1)) == 700.0


def test_seed_resumes_the_published_wall_on_the_same_day_only():
    hold = WallHold(5)
    hold.seed(700, T0)
    assert hold.update(705, _minute(1)) == 700.0

    stale = WallHold(5)
    stale.seed(700, T0 - timedelta(days=1))
    assert stale.update(705, _minute(1)) == 705.0


def test_hold_zero_passes_every_raw_value_through():
    hold = WallHold(0)
    assert _feed(hold, [700, 705, 700, None]) == [700.0, 705.0, 700.0, None]


def test_decimal_and_float_strikes_compare_equal():
    from decimal import Decimal

    hold = WallHold(5)
    hold.seed(Decimal("7700.0000"), T0)
    assert hold.update(7700.0, _minute(1)) == 7700.0
    assert hold._has_pending is False


def test_production_flicker_spy_call_wall_2026_10_07():
    """SPY call wall, 10:00-10:28 ET on 2026-10-07, minute by minute, rebuilt
    from the jump-and-revert report: 787 and 780 near-tied (within ~5% on
    every swap) while spot sat near 774.  10 raw changes; none lasted 5
    minutes, so none is published."""
    raws = (
        [780] * 4  # 10:00-10:03
        + [787]  # 10:04
        + [780] * 2  # 10:05-10:06
        + [787]  # 10:07
        + [780] * 5  # 10:08-10:12
        + [787]  # 10:13
        + [780]  # 10:14
        + [787]  # 10:15
        + [780]  # 10:16
        + [787] * 12  # 10:17-10:28
    )
    hold = WallHold(5)
    hold.seed(787, T0 - timedelta(minutes=1))  # published at 09:59
    shown = _feed(hold, raws)
    assert _changes([787] + raws) == 10
    assert set(shown) == {787.0}


# ---------------------------------------------------------------------------
# wall_strength_at
# ---------------------------------------------------------------------------


def test_strength_at_sums_expirations_on_the_ladder_scale():
    rows = [
        {"strike": 700.0, "call_gamma": 2.0, "put_gamma": 1.0},
        {"strike": 700.0, "call_gamma": 3.0, "put_gamma": 0.0},
        {"strike": 705.0, "call_gamma": 9.0, "put_gamma": 0.0},
    ]
    scale = 100.0 * 700.0 * 700.0 * 0.01
    assert wall_strength_at(rows, 700.0, "call", 700.0) == 5.0 * scale
    assert wall_strength_at(rows, 700.0, "put", 700.0) == 1.0 * scale
    assert wall_strength_at(rows, 710.0, "call", 700.0) is None
    assert wall_strength_at(rows, None, "call", 700.0) is None


# ---------------------------------------------------------------------------
# Engine: the persisted walls are the held walls
# ---------------------------------------------------------------------------


def _engine() -> AnalyticsEngine:
    return AnalyticsEngine(underlying="SPY")


def _summary(ts, call_wall, put_wall, spot=700.0):
    return {
        "timestamp": ts,
        "underlying_price": spot,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "call_wall_strength": 111.0,
        "put_wall_strength": 222.0,
    }


_ROWS = [
    {"strike": 690.0, "call_gamma": 0.0, "put_gamma": 4.0},
    {"strike": 695.0, "call_gamma": 0.0, "put_gamma": 5.0},
    {"strike": 705.0, "call_gamma": 6.0, "put_gamma": 0.0},
    {"strike": 710.0, "call_gamma": 7.0, "put_gamma": 0.0},
]


def test_engine_publishes_the_held_wall_and_its_own_strength():
    engine = _engine()
    first = _summary(_minute(0), 705.0, 695.0)
    engine._apply_wall_hold(first, _ROWS)
    assert (first["call_wall"], first["put_wall"]) == (705.0, 695.0)
    assert first["call_wall_strength"] == 111.0  # untouched when not held

    blip = _summary(_minute(1), 710.0, 690.0)
    engine._apply_wall_hold(blip, _ROWS)
    assert (blip["call_wall"], blip["put_wall"]) == (705.0, 695.0)
    scale = 100.0 * 700.0 * 700.0 * 0.01
    assert blip["call_wall_strength"] == 6.0 * scale
    assert blip["put_wall_strength"] == 5.0 * scale


def test_engine_adopts_a_wall_that_holds_for_five_minutes():
    engine = _engine()
    engine._apply_wall_hold(_summary(_minute(0), 705.0, 695.0), _ROWS)
    published = []
    for m in range(1, 7):
        s = _summary(_minute(m), 710.0, 695.0)
        engine._apply_wall_hold(s, _ROWS)
        published.append(s["call_wall"])
    assert published == [705.0, 705.0, 705.0, 705.0, 705.0, 710.0]


def test_engine_without_a_bucket_timestamp_publishes_raw():
    engine = _engine()
    s = _summary(None, 710.0, 690.0)
    engine._apply_wall_hold(s, _ROWS)
    assert (s["call_wall"], s["put_wall"]) == (710.0, 690.0)
    assert engine._wall_holds is None


@contextmanager
def _fake_db(row):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    conn = MagicMock()
    conn.cursor.return_value = cursor

    @contextmanager
    def _conn():
        yield conn

    with patch("src.analytics.main_engine.db_connection", _conn):
        yield cursor


def test_restart_resumes_the_walls_published_earlier_today():
    engine = _engine()
    with _fake_db((_minute(-1), 705.0, 695.0)) as cursor:
        holds = _REAL_INIT_WALL_HOLDS(engine, _minute(0))
    assert cursor.execute.call_count == 1
    assert holds["call"].update(710.0, _minute(0)) == 705.0
    assert holds["put"].update(690.0, _minute(0)) == 695.0


def test_restart_does_not_hold_yesterdays_walls():
    engine = _engine()
    with _fake_db((_minute(-1) - timedelta(days=1), 705.0, 695.0)):
        holds = _REAL_INIT_WALL_HOLDS(engine, _minute(0))
    assert holds["call"].update(710.0, _minute(0)) == 710.0


def test_a_failed_seed_read_starts_unseeded():
    engine = _engine()

    @contextmanager
    def _boom():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    with patch("src.analytics.main_engine.db_connection", _boom):
        holds = _REAL_INIT_WALL_HOLDS(engine, _minute(0))
    assert holds["call"].update(710.0, _minute(0)) == 710.0


# ---------------------------------------------------------------------------
# Rewind chart: the unfiltered view draws the published (held) wall
# ---------------------------------------------------------------------------


def _with_stored(rows, call_wall, put_wall):
    out = []
    for r in rows:
        r = dict(r)
        r["stored_call_wall"] = call_wall
        r["stored_put_wall"] = put_wall
        out.append(r)
    return out


def test_unfiltered_chart_draws_the_stored_wall_as_c1():
    # Raw rank 1 for this bucket is call 510 / put 495 (see _BUCKET1); the
    # engine published 515 / 500 (held from earlier minutes).
    result, _conn, _db = _run(_with_stored(_BUCKET1, 515.0, 500.0))
    bucket = result[0]
    assert bucket["call_wall"] == 515.0
    assert bucket["put_wall"] == 500.0
    assert bucket["call_walls"][0]["strike"] == 515.0
    assert bucket["call_walls"][0]["label"] == "C1"
    assert bucket["call_walls"][1]["strike"] == 510.0
    assert bucket["put_walls"][0]["strike"] == 500.0


def test_a_stored_wall_on_the_far_side_of_the_close_keeps_a_strength():
    # 500 is below the 505 close, so the call ladder never ranks it; the
    # strength is read at that strike from the bucket's own rows.
    result, _conn, _db = _run(_with_stored(_BUCKET1, 500.0, 495.0))
    c1 = result[0]["call_walls"][0]
    assert c1["strike"] == 500.0
    assert c1["strength"] == 10.0 * 100 * 505.0 * 505.0 * 0.01


def test_null_stored_wall_falls_back_to_the_recomputed_rank_one():
    result, _conn, _db = _run(_with_stored(_BUCKET1, None, None))
    assert result[0]["call_wall"] == 510.0
    assert result[0]["put_wall"] == 495.0


def test_filtered_chart_ignores_the_whole_chain_stored_wall():
    from datetime import date

    rows = _with_stored(_BUCKET1, 515.0, 500.0)
    result, _conn, _db = _run(rows, expirations=[date(2026, 8, 17)])
    assert result[0]["call_wall"] == 510.0
    assert result[0]["put_wall"] == 495.0


def test_bucket_without_strikes_keeps_null_walls_even_with_a_stored_wall():
    rows = _with_stored([_row(_TS1, 505.0, strike=None)], 515.0, 500.0)
    result, _conn, _db = _run(rows)
    assert result[0]["call_wall"] is None
