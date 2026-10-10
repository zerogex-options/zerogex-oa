"""The wall in place keeps its place at a re-pick unless clearly beaten.

Re-pick timing (tests/test_wall_anchor.py) cut the flicker from about 80 a day
to 13 on 2026-10-09, but most of what was left was one pattern: two strikes
close in size trading places at each re-check.  On NDX between 10:49 and 11:49
the Put Wall went 30800 -> 30700 -> 30800 -> 30700 -> 30800 -> 30700 -> 30800
and the Call Wall swapped 30960 <-> 31000 four times, while price sat in the
low 30,800s.  Nothing was wrong with the data (all 8 expirations present every
minute): as price moved 70 points the 30800 put swung smoothly between 75% and
100% of the 30700 put, crossing the tie zone's 90% line, and each re-check
caught it on a different side.

So a re-pick now keeps the walls in place while they are still on their side
of the anchor and within ``WALL_KEEP_PCT`` (25%) of the biggest strike there.
The engine carries that for the whole book; the API replays it per expiration
selection from the stored re-pick minutes, so every selection keeps and moves
its walls by the same rule.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.analytics.main_engine import AnalyticsEngine
from src.analytics.walls import (
    WallStep,
    WallTracker,
    compute_call_put_walls,
    compute_wall_ladder,
)
from src.api.database import DatabaseManager, _scope_replay_frame_levels
from tests.test_strike_profile_timeseries_grouping import _row

ET_OFFSET = timedelta(hours=4)  # EDT


def _et(day: int, hhmm: str) -> datetime:
    hour, minute = (int(x) for x in hhmm.split(":"))
    return datetime(2026, 10, day, hour, minute, tzinfo=timezone.utc) + ET_OFFSET


def _ndx_rows(p30800: float, p30700: float, c30960: float, c31000: float):
    return [
        {"strike": 30700.0, "call_gamma": 0.0, "put_gamma": 100.0 * p30700},
        {"strike": 30800.0, "call_gamma": 0.0, "put_gamma": 100.0 * p30800},
        {"strike": 30960.0, "call_gamma": 100.0 * c30960, "put_gamma": 0.0},
        {"strike": 31000.0, "call_gamma": 100.0 * c31000, "put_gamma": 0.0},
    ]


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------

PUTS = [
    {"strike": 30700.0, "call_gamma": 0.0, "put_gamma": 100.0},
    {"strike": 30800.0, "call_gamma": 0.0, "put_gamma": 79.0},
    {"strike": 30960.0, "call_gamma": 90.0, "put_gamma": 0.0},
    {"strike": 31000.0, "call_gamma": 100.0, "put_gamma": 0.0},
]
ANCHOR = 30833.5


def test_without_a_wall_in_place_the_pick_is_the_tie_zone_pick():
    # 30800 at 79% of 30700 is outside the 10% tie zone; 30960 at 90% is in.
    assert compute_call_put_walls(PUTS, ANCHOR, anchor=ANCHOR) == (30960.0, 30700.0)


def test_the_wall_in_place_keeps_its_place_until_clearly_beaten():
    kept = compute_call_put_walls(PUTS, ANCHOR, anchor=ANCHOR, incumbent=(31000.0, 30800.0))
    assert kept == (31000.0, 30800.0)
    clearly_beaten = [dict(r) for r in PUTS]
    clearly_beaten[1]["put_gamma"] = 74.0  # 30700 is now more than a third bigger
    assert compute_call_put_walls(
        clearly_beaten, ANCHOR, anchor=ANCHOR, incumbent=(31000.0, 30800.0)
    ) == (31000.0, 30700.0)


def test_a_wall_price_broke_through_has_no_claim_to_keep():
    # Price broke down through 30800: the anchor is below it, so it is no
    # longer a put strike at all, however big.
    assert (
        compute_call_put_walls(PUTS, 30760.0, anchor=30760.0, incumbent=(30960.0, 30800.0))[1]
        == 30700.0
    )


def test_a_wall_with_no_gamma_this_minute_is_dropped():
    assert compute_call_put_walls(PUTS, ANCHOR, anchor=ANCHOR, incumbent=(30990.0, 30750.0)) == (
        30960.0,
        30700.0,
    )


def test_the_edge_is_never_narrower_than_the_tie_zone():
    # 30700 is tied with 30800 (within 10%); the tie zone alone would hand the
    # wall to 30800, the strike price reaches first.  The wall in place stays.
    tied = [
        {"strike": 30700.0, "call_gamma": 0.0, "put_gamma": 100.0},
        {"strike": 30800.0, "call_gamma": 0.0, "put_gamma": 95.0},
    ]
    assert compute_call_put_walls(tied, ANCHOR, anchor=ANCHOR)[1] == 30800.0
    assert (
        compute_call_put_walls(
            tied, ANCHOR, anchor=ANCHOR, incumbent=(None, 30700.0), keep_pct=0.0
        )[1]
        == 30700.0
    )


def test_keep_one_reproduces_a_stored_pick():
    # How a reader ranks a ladder around the walls the engine published.
    small = [dict(r) for r in PUTS]
    small[1]["put_gamma"] = 5.0
    assert compute_call_put_walls(
        small, ANCHOR, anchor=ANCHOR, incumbent=(30960.0, 30800.0), keep_pct=1.0
    ) == (30960.0, 30800.0)


def test_the_kept_wall_carries_this_minutes_strength_and_the_ladder_stays_by_size():
    calls, puts = compute_wall_ladder(PUTS, ANCHOR, 3, anchor=ANCHOR, incumbent=(30960.0, 30800.0))
    scale = 100.0 * ANCHOR * ANCHOR * 0.01
    assert [w["strike"] for w in puts] == [30800.0, 30700.0]
    assert puts[0]["strength"] == pytest.approx(79.0 * scale)
    assert [w["label"] for w in puts] == ["P1", "P2"]
    assert [w["strike"] for w in calls] == [30960.0, 31000.0]


def test_an_unreadable_wall_in_place_picks_fresh():
    assert compute_call_put_walls(PUTS, ANCHOR, anchor=ANCHOR, incumbent=("n/a", None)) == (
        30960.0,
        30700.0,
    )


# ---------------------------------------------------------------------------
# NDX, 2026-10-09 10:30-11:59 ET, as the engine stored it
# ---------------------------------------------------------------------------

# Columns in the diagnostic query's order: ET minute, wall anchor, the minute
# the walls were picked from, the published Put Wall, the 30800 and 30700 puts
# as a share of the biggest put, the published Call Wall, and the 30960 and
# 31000 calls as a share of the biggest call.  11:08's 30960 printed as 0.90;
# the engine picked 31000 there, so it was just under (0.899).
NDX_1009 = """
10:30 30812.8 10:30 30800 0.91 1.00 30960 1.00 0.92
10:31 30812.8 10:30 30800 0.90 1.00 30960 1.00 0.93
10:32 30812.8 10:30 30800 0.87 1.00 30960 1.00 0.95
10:33 30812.8 10:30 30800 0.89 1.00 30960 1.00 0.94
10:34 30818.4 10:34 30800 0.92 1.00 30960 1.00 0.91
10:35 30818.4 10:34 30800 0.89 1.00 30960 1.00 0.93
10:36 30818.4 10:34 30800 0.90 1.00 30960 1.00 0.93
10:37 30818.4 10:34 30800 0.84 1.00 30960 1.00 0.99
10:38 30818.4 10:34 30800 0.89 1.00 30960 1.00 0.95
10:39 30818.4 10:34 30800 0.90 1.00 30960 1.00 0.95
10:40 30818.4 10:34 30800 0.82 1.00 30960 0.95 1.00
10:41 30818.4 10:34 30800 0.80 1.00 30960 0.94 1.00
10:42 30818.4 10:34 30800 0.75 1.00 30960 0.87 1.00
10:43 30818.4 10:34 30800 0.78 1.00 30960 0.90 1.00
10:44 30818.4 10:34 30800 0.78 1.00 30960 0.90 1.00
10:45 30818.4 10:34 30800 0.87 1.00 30960 0.97 1.00
10:46 30818.4 10:34 30800 0.89 1.00 30960 1.00 0.98
10:47 30818.4 10:34 30800 0.81 1.00 30960 0.92 1.00
10:48 30818.4 10:34 30800 0.78 1.00 30960 0.91 1.00
10:49 30818.4 10:49 30700 0.81 1.00 30960 0.92 1.00
10:50 30818.4 10:49 30700 0.92 1.00 30960 1.00 0.94
10:51 30820.8 10:51 30800 0.97 1.00 30960 1.00 0.89
10:52 30820.8 10:51 30800 0.98 1.00 30960 1.00 0.89
10:53 30820.8 10:51 30800 0.96 1.00 30960 1.00 0.90
10:54 30822.2 10:54 30800 0.97 1.00 30960 1.00 0.88
10:55 30825.0 10:55 30800 1.00 1.00 30960 1.00 0.86
10:56 30825.0 10:55 30800 1.00 0.99 30960 1.00 0.86
10:57 30830.3 10:57 30800 1.00 0.98 30960 1.00 0.85
10:58 30835.2 10:58 30800 1.00 0.97 30960 1.00 0.83
10:59 30835.2 10:58 30800 1.00 1.00 30960 1.00 0.86
11:00 30835.2 10:58 30800 0.99 1.00 30960 1.00 0.87
11:01 30835.2 10:58 30800 0.99 1.00 30960 1.00 0.87
11:02 30835.2 10:58 30800 0.97 1.00 30960 1.00 0.89
11:03 30835.2 10:58 30800 0.96 1.00 30960 1.00 0.90
11:04 30835.2 10:58 30800 1.00 0.96 30960 1.00 0.84
11:05 30835.2 10:58 30800 1.00 1.00 30960 1.00 0.86
11:06 30835.2 10:58 30800 0.93 1.00 30960 1.00 0.92
11:07 30835.2 10:58 30800 0.88 1.00 30960 1.00 0.99
11:08 30833.5 11:08 30700 0.79 1.00 31000 0.899 1.00
11:09 30833.5 11:08 30700 0.80 1.00 31000 0.89 1.00
11:10 30833.5 11:08 30700 0.84 1.00 31000 0.92 1.00
11:11 30833.5 11:08 30700 0.84 1.00 31000 0.93 1.00
11:12 30833.5 11:08 30700 0.89 1.00 31000 0.99 1.00
11:13 30833.5 11:08 30700 0.88 1.00 31000 0.95 1.00
11:14 30833.5 11:08 30700 0.86 1.00 31000 0.93 1.00
11:15 30833.5 11:08 30700 0.88 1.00 31000 0.94 1.00
11:16 30833.5 11:08 30700 0.91 1.00 31000 0.98 1.00
11:17 30833.5 11:08 30700 0.94 1.00 31000 1.00 1.00
11:18 30833.5 11:08 30700 0.99 1.00 31000 1.00 0.92
11:19 30833.5 11:08 30700 1.00 0.99 31000 1.00 0.89
11:20 30833.5 11:08 30700 0.97 1.00 31000 1.00 0.95
11:21 30833.5 11:08 30700 0.97 1.00 31000 1.00 0.94
11:22 30833.5 11:08 30700 0.96 1.00 31000 1.00 0.96
11:23 30833.5 11:23 30800 0.92 1.00 30960 1.00 1.00
11:24 30833.5 11:23 30800 0.99 1.00 30960 1.00 0.93
11:25 30833.5 11:23 30800 0.97 1.00 30960 1.00 0.94
11:26 30833.5 11:23 30800 1.00 0.99 30960 1.00 0.91
11:27 30833.5 11:23 30800 1.00 0.96 30960 1.00 0.88
11:28 30833.5 11:23 30800 0.99 1.00 30960 1.00 0.96
11:29 30833.5 11:23 30800 0.92 1.00 30960 0.95 1.00
11:30 30833.5 11:23 30800 0.90 1.00 30960 0.92 1.00
11:31 30833.5 11:23 30800 0.91 1.00 30960 0.93 1.00
11:32 30833.5 11:23 30800 0.91 1.00 30960 0.94 1.00
11:33 30833.5 11:23 30800 0.85 1.00 30960 0.89 1.00
11:34 30833.1 11:34 30700 0.84 1.00 31000 0.83 1.00
11:35 30833.1 11:34 30700 0.88 1.00 31000 0.89 1.00
11:36 30833.1 11:34 30700 0.90 1.00 31000 0.89 1.00
11:37 30833.1 11:34 30700 0.94 1.00 31000 0.92 1.00
11:38 30833.1 11:34 30700 1.00 0.97 31000 1.00 0.94
11:39 30833.1 11:34 30700 1.00 0.95 31000 1.00 0.91
11:40 30833.1 11:34 30700 1.00 0.95 31000 1.00 0.91
11:41 30833.1 11:34 30700 1.00 0.97 31000 1.00 0.93
11:42 30833.1 11:34 30700 1.00 0.91 31000 1.00 0.86
11:43 30833.1 11:34 30700 1.00 0.96 31000 1.00 0.93
11:44 30833.1 11:34 30700 1.00 0.93 31000 1.00 0.93
11:45 30833.1 11:34 30700 1.00 0.99 31000 1.00 0.98
11:46 30833.1 11:34 30700 1.00 0.98 31000 1.00 0.99
11:47 30833.1 11:34 30700 1.00 0.96 31000 1.00 0.97
11:48 30833.1 11:34 30700 1.00 0.93 31000 1.00 0.93
11:49 30833.1 11:49 30800 1.00 0.93 30960 1.00 0.92
11:50 30833.1 11:49 30800 1.00 0.88 30960 1.00 0.87
11:51 30833.1 11:49 30800 1.00 0.84 30960 1.00 0.82
11:52 30833.1 11:49 30800 1.00 0.85 30960 1.00 0.85
11:53 30833.1 11:49 30800 1.00 0.84 30960 1.00 0.84
11:54 30833.1 11:49 30800 1.00 0.84 30960 1.00 0.85
11:55 30833.1 11:49 30800 1.00 0.84 30960 1.00 0.84
11:56 30833.1 11:49 30800 1.00 0.83 30960 1.00 0.84
11:57 30833.1 11:49 30800 1.00 0.81 30960 1.00 0.80
11:58 30833.1 11:49 30800 1.00 0.82 30960 1.00 0.85
11:59 30833.1 11:49 30800 1.00 0.85 30960 1.00 0.92
"""


def _ndx_minutes():
    out = []
    for line in NDX_1009.strip().splitlines():
        et, anchor, picked, put_wall, p8, p7, call_wall, c96, c31 = line.split()
        out.append(
            {
                "et": et,
                "anchor": float(anchor),
                "repick": picked == et,
                "published": (float(call_wall), float(put_wall)),
                "rows": _ndx_rows(float(p8), float(p7), float(c96), float(c31)),
            }
        )
    return out


def _ndx_replay(keep):
    shown, held = [], None
    for m in _ndx_minutes():
        if m["repick"]:
            held = compute_call_put_walls(
                m["rows"],
                m["anchor"],
                anchor=m["anchor"],
                incumbent=held if keep is not None else None,
                keep_pct=keep if keep is not None else 0.0,
            )
        shown.append(held)
    return shown


def _changes(series):
    return sum(1 for a, b in zip(series, series[1:]) for i in (0, 1) if a[i] != b[i])


def test_ndx_replay_of_the_live_rule_matches_every_published_minute():
    minutes = _ndx_minutes()
    live = _ndx_replay(None)
    assert live == [m["published"] for m in minutes]
    assert _changes(live) == 10  # put 6, call 4


def test_ndx_hour_holds_both_walls_with_the_wall_in_place_keeping_its_place():
    new = _ndx_replay(0.25)
    assert set(new) == {(30960.0, 30800.0)}
    assert _changes(_ndx_replay(0.20)) == 1  # a narrower edge hands the put over once


# ---------------------------------------------------------------------------
# Tracker and engine
# ---------------------------------------------------------------------------

T0 = _et(9, "10:30")


def test_a_re_pick_carries_the_walls_in_place_and_a_hold_does_not():
    tracker = WallTracker(15)
    first = tracker.update(30847.7, 30.8, T0)
    assert first.refreshed and first.incumbent is None  # the day's first pick is fresh
    tracker.hold({"call_wall": 30960.0, "put_wall": 30800.0})
    held = tracker.update(30842.3, 30.8, T0 + timedelta(minutes=1))
    assert not held.refreshed and held.incumbent is None
    timed = tracker.update(30840.0, 30.8, T0 + timedelta(minutes=15))
    assert timed.refreshed and timed.incumbent == {"call_wall": 30960.0, "put_wall": 30800.0}


def test_every_pass_over_a_bucket_is_handed_the_previous_buckets_walls():
    tracker = WallTracker(15)
    tracker.update(30847.7, 30.8, T0)
    tracker.hold({"call_wall": 30960.0, "put_wall": 30800.0})
    t1 = T0 + timedelta(minutes=1)
    first_pass = tracker.update(30900.0, 30.8, t1)  # breakout: re-pick
    tracker.hold({"call_wall": 31000.0, "put_wall": 30800.0})
    second_pass = tracker.update(30905.0, 30.8, t1)
    assert (
        first_pass.incumbent
        == second_pass.incumbent
        == {
            "call_wall": 30960.0,
            "put_wall": 30800.0,
        }
    )


def test_the_walls_in_place_reset_each_new_york_day_and_survive_a_restart():
    tracker = WallTracker(15)
    tracker.update(30847.7, 30.8, T0)
    tracker.hold({"call_wall": 30960.0, "put_wall": 30800.0})
    next_day = tracker.update(30847.7, 30.8, T0 + timedelta(days=1))
    assert next_day.refreshed and next_day.incumbent is None

    resumed = WallTracker(15)
    resumed.seed(T0, 30812.8, T0, {"call_wall": 30960.0, "put_wall": 30800.0})
    step = resumed.update(30900.0, 30.8, T0 + timedelta(minutes=1))
    assert step.refreshed and step.incumbent == {"call_wall": 30960.0, "put_wall": 30800.0}


def test_engine_hands_a_re_pick_the_published_walls_only():
    payload = {"call_wall": 30960.0, "put_wall": 30800.0, "call_wall_strength": 1.0}
    repick = WallStep(anchor=ANCHOR, refresh_ts=T0, refreshed=True, incumbent=payload)
    assert AnalyticsEngine._wall_incumbent(repick) == (30960.0, 30800.0)
    hold = WallStep(anchor=ANCHOR, refresh_ts=T0, refreshed=False, incumbent=payload)
    assert AnalyticsEngine._wall_incumbent(hold) is None
    assert AnalyticsEngine._wall_incumbent(None) is None
    fresh = WallStep(anchor=ANCHOR, refresh_ts=T0, refreshed=True)
    assert AnalyticsEngine._wall_incumbent(fresh) is None


def _engine_rows(rows):
    return [
        {**r, "net_gex": r["call_gamma"] - r["put_gamma"], "call_oi": 100, "put_oi": 100}
        for r in rows
    ]


def test_engine_keeps_its_published_wall_through_the_re_check():
    """The NDX 10:30 -> 10:49 case end to end: the 15-minute re-check finds the
    30800 put at 81% of 30700 and, before, handed the Put Wall to 30700."""
    engine = AnalyticsEngine(underlying="NDX")

    def cycle(ts, spot, rows):
        step, buffer = engine._advance_walls(spot, ts)
        summary = engine._calculate_gex_summary(
            _engine_rows(rows),
            [],
            spot,
            ts,
            wall_anchor=step.anchor,
            wall_incumbent=engine._wall_incumbent(step),
        )
        engine._publish_walls(summary, step, buffer)
        return summary

    first = cycle(T0, 30812.8, _ndx_rows(0.91, 1.00, 1.00, 0.92))
    assert (first["call_wall"], first["put_wall"]) == (30960.0, 30800.0)
    recheck = cycle(T0 + timedelta(minutes=19), 30811.4, _ndx_rows(0.81, 1.00, 0.92, 1.00))
    assert recheck["wall_refresh_ts"] == T0 + timedelta(minutes=19)
    assert (recheck["call_wall"], recheck["put_wall"]) == (30960.0, 30800.0)
    scale = 100.0 * 30811.4 * 30811.4 * 0.01
    assert recheck["put_wall_strength"] == pytest.approx(81.0 * scale)


# ---------------------------------------------------------------------------
# API: every expiration selection carries its own walls through the day
# ---------------------------------------------------------------------------

DAY = 9
T1 = _et(DAY, "10:30")
T2 = _et(DAY, "10:35")
EXPS = [date(2026, 10, 9)]


def _bucket_rows(ts, puts, stored=(None, None)):
    """One bucket that re-picked on its own minute, anchor 500."""
    out = []
    for strike, call_raw, put_raw in (
        (495.0, 0.0, puts[0]),
        (498.0, 0.0, puts[1]),
        (505.0, 100.0, 0.0),
    ):
        r = _row(ts, 500.0, strike=strike, call_raw=call_raw, put_raw=put_raw)
        r["wall_anchor"] = 500.0
        r["rep_ts"] = ts
        r["wall_refresh_ts"] = ts
        r["stored_call_wall"], r["stored_put_wall"] = stored
        out.append(r)
    return out


class _ChainConn:
    """Bucket read, the re-pick list (honouring its bounds) and per-strike rows."""

    def __init__(self, bucket_rows, repicks, newest, inputs=None, fail_chain=False):
        self.bucket_rows = bucket_rows
        self.repicks = repicks  # [(ts, anchor)]
        self.newest = newest
        self.inputs = inputs or []
        self.fail_chain = fail_chain
        self.chain_calls = []
        self.input_calls = []

    async def fetch(self, query, *args, **_kwargs):
        if "-- wall re-pick minutes" in query:
            self.chain_calls.append(args)
            if self.fail_chain:
                raise RuntimeError("statement timeout")
            _symbol, lower, upper = args
            return [
                {"timestamp": ts, "wall_anchor": anchor, "newest": self.newest}
                for ts, anchor in self.repicks
                if lower < ts <= upper
            ]
        if "ANY($2::timestamptz[])" in query:
            self.input_calls.append(args)
            wanted = set(args[1])
            return [r for r in self.inputs if r["timestamp"] in wanted]
        return self.bucket_rows


def _timeseries(db, conn, expirations):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    db._strike_profile_timeseries_cache_ttl_seconds = 0.0
    return asyncio.run(db.get_strike_profile_timeseries("SPY", "5min", 40, expirations))


# 498 is tied with 495 at 10:30 (95%) and wins as the nearer strike.  At 10:35
# it is down to 80%: a fresh pick hands the Put Wall to 495, the wall in place
# keeps it.
BUCKETS = _bucket_rows(T1, (100.0, 95.0)) + _bucket_rows(T2, (100.0, 80.0))


def test_a_filtered_view_keeps_the_wall_it_picked_at_its_previous_re_pick():
    conn = _ChainConn(BUCKETS, [(T1, 500.0), (T2, 500.0)], newest=T2)
    result = _timeseries(DatabaseManager(), conn, EXPS)
    assert [b["put_wall"] for b in result] == [498.0, 498.0]
    assert result[1]["put_walls"][0] == {
        "rank": 1,
        "label": "P1",
        "strike": 498.0,
        "strength": pytest.approx(80.0 * 100 * 500.0 * 500.0 * 0.01),
    }
    # Both re-pick minutes are in the window: no per-strike read needed.
    assert conn.input_calls == []
    symbol, lower, upper = conn.chain_calls[0]
    assert symbol == "SPY" and upper == T2
    assert lower < datetime(2026, 10, 9, 4, 0, tzinfo=timezone.utc)  # from ET midnight


def test_without_the_chain_a_filtered_view_picks_fresh_and_still_draws():
    conn = _ChainConn(BUCKETS, [], newest=T2, fail_chain=True)
    result = _timeseries(DatabaseManager(), conn, EXPS)
    assert [b["put_wall"] for b in result] == [498.0, 495.0]


def test_a_failed_chain_read_is_not_retried_on_every_poll(monkeypatch):
    """The pool is a few connections deep: a slow database must not make every
    chart poll wait out the chain read's timeout again."""
    clock = [1000.0]
    monkeypatch.setattr("src.api.database.time_module.monotonic", lambda: clock[0])
    db = DatabaseManager()
    failing = _ChainConn(BUCKETS, [], newest=T2, fail_chain=True)
    _timeseries(db, failing, EXPS)
    assert len(failing.chain_calls) == 1
    clock[0] += 30.0
    _timeseries(db, failing, EXPS)
    assert len(failing.chain_calls) == 1  # backed off: fell back without asking
    clock[0] += 31.0
    healthy = _ChainConn(BUCKETS, [(T1, 500.0), (T2, 500.0)], newest=T2)
    assert [b["put_wall"] for b in _timeseries(db, healthy, EXPS)] == [498.0, 498.0]
    assert len(healthy.chain_calls) == 1


def test_the_all_view_ranks_around_the_walls_the_engine_published():
    rows = _bucket_rows(T1, (100.0, 95.0), stored=(505.0, 498.0)) + _bucket_rows(
        T2, (100.0, 80.0), stored=(505.0, 498.0)
    )
    conn = _ChainConn(rows, [], newest=T2)
    result = _timeseries(DatabaseManager(), conn, None)
    assert [b["put_wall"] for b in result] == [498.0, 498.0]
    assert [w["strike"] for w in result[1]["put_walls"]] == [498.0, 495.0]
    assert conn.chain_calls == []  # the engine's walls need no replay
    legacy = _bucket_rows(T2, (100.0, 80.0))
    assert (
        _timeseries(DatabaseManager(), _ChainConn(legacy, [], newest=T2), None)[0]["put_wall"]
        == 495.0
    )


def test_finished_re_picks_are_replayed_once_per_worker():
    db = DatabaseManager()
    inputs = [
        {"timestamp": ts, "strike": strike, "call_gamma": call, "put_gamma": put}
        for ts, puts in ((T1, (100.0, 95.0)), (T2, (100.0, 80.0)))
        for strike, call, put in ((495.0, 0.0, puts[0]), (498.0, 0.0, puts[1]), (505.0, 100.0, 0.0))
    ]
    first = _ChainConn(BUCKETS[3:], [(T1, 500.0), (T2, 500.0)], newest=T2, inputs=inputs)
    assert _timeseries(db, first, EXPS)[0]["put_wall"] == 498.0
    # 10:30 is out of the window, so the chain read its rows once.
    assert first.input_calls and set(first.input_calls[0][1]) == {T1}

    later = _ChainConn(BUCKETS[3:], [(T1, 500.0), (T2, 500.0)], newest=T2, inputs=inputs)
    assert _timeseries(db, later, EXPS)[0]["put_wall"] == 498.0
    # 10:30 is final and cached; only the newest re-pick is replayed.
    assert later.chain_calls[0][1] == T1
    assert later.input_calls == []


def test_the_chain_starts_fresh_each_day():
    db = DatabaseManager()
    next_day = _et(DAY + 1, "10:30")
    rows = [
        {"timestamp": ts, "strike": 498.0, "call_gamma": 0.0, "put_gamma": 80.0}
        for ts in (T1, next_day)
    ] + [
        {"timestamp": ts, "strike": 495.0, "call_gamma": 0.0, "put_gamma": 100.0}
        for ts in (T1, next_day)
    ]
    conn = _ChainConn([], [(T1, 500.0), (next_day, 500.0)], newest=next_day, inputs=rows)

    async def _go():
        return await db._scope_wall_chain(conn, "SPY", EXPS, [T1, next_day])

    links = asyncio.run(_go())
    assert links[T1][0] is None and links[next_day][0] is None
    assert len(conn.chain_calls) == 2


def test_a_replay_frame_shows_the_chains_walls_for_its_re_pick_minute():
    frame = {
        "timestamp": T2,
        "_spot": Decimal("500.0"),
        "_wall_anchor": 500.0,
        "_wall_refresh_ts": T1,
        "_max_pain_by_expiration": None,
        "_gamma_inputs": [
            {"strike": 495.0, "call_gamma": 0.0, "put_gamma": 100.0},
            {"strike": 498.0, "call_gamma": 0.0, "put_gamma": 80.0},
        ],
    }
    out = _scope_replay_frame_levels(
        dict(frame), EXPS, {T2: frame["_gamma_inputs"]}, {T1: (505.0, 498.0)}
    )
    assert (out["call_wall"], out["put_wall"]) == (505.0, 498.0)
    # No chain for that minute: re-picked from the frame's own ladder.
    out = _scope_replay_frame_levels(dict(frame), EXPS, {T2: frame["_gamma_inputs"]}, {})
    assert out["put_wall"] == 495.0


def test_replay_reads_the_chain_for_the_frames_re_pick_minutes():
    db = DatabaseManager()
    conn = _ChainConn(
        [],
        [(T1, 500.0), (T2, 500.0)],
        newest=T2,
        inputs=[
            {"timestamp": ts, "strike": s, "call_gamma": 0.0, "put_gamma": g}
            for ts, (a, b) in ((T1, (100.0, 95.0)), (T2, (100.0, 80.0)))
            for s, g in ((495.0, a), (498.0, b))
        ],
    )

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    frames = [{"_wall_refresh_ts": T1}, {"_wall_refresh_ts": T2}, {"_wall_refresh_ts": None}]
    walls = asyncio.run(db._replay_chain_walls("SPY", EXPS, frames))
    assert walls == {T1: (None, 498.0), T2: (None, 498.0)}
    assert asyncio.run(db._replay_chain_walls("SPY", EXPS, [{"_wall_refresh_ts": None}])) is None
    conn.fail_chain = True
    db._wall_chain_cache.clear()
    assert asyncio.run(db._replay_chain_walls("SPY", EXPS, frames)) is None
