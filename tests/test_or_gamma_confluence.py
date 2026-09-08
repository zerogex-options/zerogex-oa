"""Tests for the opening-range × gamma-confluence research harness.

Organised around the requirements the brief numbers explicitly, plus three the
repository's own structure makes necessary (publish clock, wall re-centring,
basis anchoring).  Every test is pure — no database, no network.

The look-ahead tests are the ones that matter.  A harness with a look-ahead bug
does not crash; it reports a large, stable, wrong effect, and every summary
statistic looks healthy.  So each one is written to FAIL LOUDLY under the
specific bug it guards, not merely to exercise the happy path.
"""

from __future__ import annotations

import json
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pytest

from research.msi_regime_excursion.excursion import ET, Bar, BarSeries
from research.or_gamma_confluence.basis import BasisReader, BasisUnavailable, project_snapshot
from research.or_gamma_confluence.cohorts import (
    book_of,
    build_cohorts,
    compare_to_baseline,
    pooling_check,
    summarize,
)
from research.or_gamma_confluence.config import (
    CLOCK_DATA,
    CLOCK_PUBLISHED,
    CLOCK_VISIBLE,
    MODE_OPEN,
    ResearchConfig,
)
from research.or_gamma_confluence.events import (
    OUTCOME_AMBIGUOUS,
    OUTCOME_CENSORED,
    OUTCOME_CONTINUATION,
    OUTCOME_CONTINUATION_SAME_BAR,
    OUTCOME_REVERSAL,
    extract_touch_events,
)
from research.or_gamma_confluence.features import SessionContext, build_features
from research.or_gamma_confluence.instruments import spec
from research.or_gamma_confluence.levels import (
    KIND_GEX_CALL_RANK,
    GammaTimeline,
    SnapshotRejected,
    build_snapshot,
    build_timeline_tolerant,
    confluence_at,
)
from research.or_gamma_confluence.outcomes import measure_outcome, reversion_sign
from research.or_gamma_confluence.ranges import (
    build_ladder,
    build_opening_range,
    dephantom_open_bar,
    session_window,
)
from research.or_gamma_confluence.selftest import run_selftest

SESSION = date(2026, 9, 4)
ORL, ORH = 29500.0, 29600.0  # R = 100 -> +50 = 29650, +100 = 29700


def _ts(h: int, m: int, s: int = 0, day: date = SESSION) -> datetime:
    return datetime.combine(day, time(h, m, s), tzinfo=ET)


def _cfg(**kw) -> ResearchConfig:
    kw.setdefault("touch_tolerance_bp", 0.0)
    kw.setdefault("touch_tolerance_min", 0.0)
    return ResearchConfig(**kw)


def _bar(i: int, o: float, h: float, low: float, c: float, day: date = SESSION) -> Bar:
    return Bar(_ts(9, 30, day=day) + timedelta(minutes=i), o, h, low, c)


def _or_bars(day: date = SESSION) -> list[Bar]:
    return [_bar(i, 29550, ORH, ORL, 29550, day) for i in range(5)]


def _frame(hh: int, mm: int, *, call_wall: float, lag_s: int = 8, **over) -> dict:
    row = {
        "timestamp": _ts(hh, mm),
        "created_at": _ts(hh, mm) + timedelta(seconds=lag_s),
        "call_wall": call_wall,
        "put_wall": call_wall - 200.0,
        "gamma_flip_point": call_wall - 100.0,
        "max_pain": call_wall - 50.0,
        "total_net_gex": 1.0e9,
        "net_gex_at_spot": 5.0e8,
    }
    row.update(over)
    return row


# ── 1. The opening range freezes ──────────────────────────────────────


def test_or_does_not_change_after_its_window_closes():
    cfg = _cfg(opening_range_minutes=5)
    base = _or_bars()
    early, _ = build_opening_range(base, SESSION, "NQ", cfg)

    # A later bar that would have blown the range wide open.
    later = base + [_bar(9, 29550, 40000.0, 20000.0, 29550)]
    after, _ = build_opening_range(later, SESSION, "NQ", cfg)

    assert (early.high, early.low, early.width) == (after.high, after.low, after.width)
    assert after.high == ORH and after.low == ORL


def test_or_window_is_half_open_on_bar_start_stamps():
    """Bars are period-START stamped, so a 5-min OR is 09:30..09:34 inclusive."""
    cfg = _cfg(opening_range_minutes=5)
    bars = [_bar(i, 29550, 29550 + i, 29550 - i, 29550) for i in range(8)]
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg)
    assert orange.n_bars == 5
    # Bar 5 (09:35) had the widest range and must be excluded.
    assert orange.high == 29554 and orange.low == 29546


def test_ladder_is_frozen_and_neighbours_are_correct():
    cfg = _cfg()
    orange, _ = build_opening_range(_or_bars(), SESSION, "NQ", cfg)
    lad = build_ladder(orange, cfg)
    minus_300 = lad.at(-6)
    assert minus_300.label == "-300"
    assert minus_300.price == pytest.approx(ORL - 3.0 * 100.0)
    # The brief's own example: at -300, previous is -250 and next is -350.
    assert lad.previous_price(minus_300) == pytest.approx(ORL - 2.5 * 100.0)
    assert lad.next_price(minus_300) == pytest.approx(ORL - 3.5 * 100.0)
    # The first rung reverts to the range boundary, not to another rung.
    assert lad.previous_price(lad.at(1)) == pytest.approx(ORH)
    assert lad.previous_price(lad.at(-1)) == pytest.approx(ORL)
    # The outermost rung has no next: continuation is unobservable there.
    assert lad.next_price(lad.at(20)) is None


def test_mode_open_anchors_both_sides_on_the_session_open():
    cfg = _cfg(extension_mode=MODE_OPEN)
    orange, _ = build_opening_range(_or_bars(), SESSION, "NQ", cfg)
    lad = build_ladder(orange, cfg)
    assert lad.anchor_up == lad.anchor_down == orange.open_price
    assert lad.at(1).price == pytest.approx(orange.open_price + 0.5 * orange.width)
    assert lad.at(-1).price == pytest.approx(orange.open_price - 0.5 * orange.width)


# ── 2 / 3 / 13. Gamma availability, lead time, publish clock ──────────


def test_no_future_snapshot_can_be_selected():
    cfg = _cfg(availability_clock=CLOCK_VISIBLE)
    rows = [_frame(10, m, call_wall=29500 + m) for m in range(6)]
    # A frame whose DATA instant precedes the touch but which was PUBLISHED
    # after it. Selecting it is the canonical look-ahead.
    rows.append(_frame(10, 4, call_wall=99999.0, created_at=_ts(10, 6)))
    tl = GammaTimeline(build_snapshot(r, cfg, spot=29500.0) for r in rows)

    snap = tl.as_of(_ts(10, 5), 0)
    assert snap is not None
    assert snap.available_at <= _ts(10, 5)
    assert all(lv.price != 99999.0 for lv in snap.levels)


@pytest.mark.parametrize("lead", [0, 30, 60, 120, 180])
def test_gamma_min_lead_seconds_is_respected(lead):
    cfg = _cfg(availability_clock=CLOCK_VISIBLE)
    rows = [_frame(10, m, call_wall=29500 + m) for m in range(6)]
    tl = GammaTimeline(build_snapshot(r, cfg, spot=29500.0) for r in rows)
    touch = _ts(10, 5)
    snap = tl.as_of(touch, lead)
    if snap is not None:
        assert (touch - snap.available_at).total_seconds() >= lead


def test_lead_boundary_is_inclusive_at_the_cutoff_and_exclusive_past_it():
    cfg = _cfg(availability_clock=CLOCK_DATA)
    tl = GammaTimeline(
        build_snapshot(_frame(10, 3, call_wall=29500.0), cfg, spot=29500.0) for _ in (0,)
    )
    touch = _ts(10, 5)
    assert tl.as_of(touch, 120) is not None  # available_at == cutoff
    assert tl.as_of(touch, 121) is None  # one second too late


def test_visible_clock_adds_the_client_poll_lag():
    row = _frame(10, 0, call_wall=29500.0, lag_s=8)
    data = build_snapshot(row, _cfg(availability_clock=CLOCK_DATA), spot=29500.0)
    pub = build_snapshot(row, _cfg(availability_clock=CLOCK_PUBLISHED), spot=29500.0)
    vis = build_snapshot(
        row,
        _cfg(availability_clock=CLOCK_VISIBLE, client_poll_lag_seconds=30),
        spot=29500.0,
    )
    assert data.available_at == _ts(10, 0)
    assert pub.available_at == _ts(10, 0) + timedelta(seconds=8)
    assert vis.available_at == _ts(10, 0) + timedelta(seconds=38)
    # The clock choice must be recoverable from the row itself.
    assert vis.to_audit()["gamma_clock"] == CLOCK_VISIBLE


# ── 8. Missing / untrustworthy gamma fails closed ─────────────────────


@pytest.mark.parametrize(
    "override, clock",
    [
        ({"created_at": None}, CLOCK_PUBLISHED),
        ({"created_at": None}, CLOCK_VISIBLE),
        ({"created_at": _ts(9, 59)}, CLOCK_VISIBLE),  # before timestamp
        # Backfill scale: written days later, so created_at is a backfill time.
        ({"created_at": _ts(10, 0) + timedelta(days=3)}, CLOCK_VISIBLE),
    ],
)
def test_untrustworthy_publish_clock_fails_closed(override, clock):
    row = _frame(10, 0, call_wall=29500.0, **override)
    with pytest.raises(SnapshotRejected):
        build_snapshot(row, _cfg(availability_clock=clock), spot=29500.0)


@pytest.mark.parametrize("lag_s", [83, 96, 651, 1618])
def test_a_slow_publish_is_kept_and_reported_as_late(lag_s):
    """The measured production tail (p95 ~65 s, maxima 83-1618 s) is slow
    publishing, not backfilling. Discarding it would throw away correctly
    timed frames; the visible clock already handles it by reporting the level
    as late, which is exactly what a trader experienced."""
    cfg = _cfg(availability_clock=CLOCK_VISIBLE)
    snap = build_snapshot(_frame(10, 0, call_wall=29500.0, lag_s=lag_s), cfg, spot=29500.0)
    assert snap.publish_lag_seconds == pytest.approx(lag_s)
    assert snap.available_at == _ts(10, 0) + timedelta(seconds=lag_s + cfg.client_poll_lag_seconds)


def _minute_frames(n: int) -> list[dict]:
    """``n`` one-a-minute frames from 10:00, the production cadence."""
    rows = []
    for m in range(n):
        ts = _ts(10, 0) + timedelta(minutes=m)
        row = _frame(10, 0, call_wall=29500.0 + m)
        row["timestamp"] = ts
        row["created_at"] = ts + timedelta(seconds=8)
        rows.append(row)
    return rows


def test_one_bad_frame_is_dropped_not_the_whole_session():
    """A step function holds the previous value across a dropped frame, so an
    isolated bad clock costs one frame — not a session."""
    cfg = _cfg(availability_clock=CLOCK_VISIBLE)
    rows = _minute_frames(100)
    rows[7]["created_at"] = None  # one unusable frame

    built = build_timeline_tolerant(rows, cfg, spot_at=lambda ts: 29500.0)
    assert built.rejected == 1
    assert built.accepted == 99
    assert built.reasons == {"created_at_missing": 1}
    assert built.rejected_frac < cfg.max_rejected_frame_frac

    # The 10:07 frame is gone, so 10:06 stays in force across it.
    snap = built.timeline.as_of(_ts(10, 8), 0)
    assert snap is not None and snap.data_ts == _ts(10, 6)


def test_a_session_of_bad_frames_still_fails_closed():
    cfg = _cfg(availability_clock=CLOCK_VISIBLE)
    rows = _minute_frames(100)
    for row in rows[:50]:
        row["created_at"] = None
    built = build_timeline_tolerant(rows, cfg, spot_at=lambda ts: 29500.0)
    assert built.rejected_frac == pytest.approx(0.5)
    assert built.rejected_frac > cfg.max_rejected_frame_frac


def test_the_measured_production_outliers_do_not_cost_a_session():
    """SPY carries one ~1618 s publish and QQQ one negative-lag row across
    ~59 sessions each. Under a per-session rule those cost a session apiece;
    under the per-frame rule they cost a frame."""
    cfg = _cfg(availability_clock=CLOCK_VISIBLE)
    rows = _minute_frames(390)  # a full session
    rows[100]["created_at"] = rows[100]["timestamp"] - timedelta(seconds=1)  # negative
    built = build_timeline_tolerant(rows, cfg, spot_at=lambda ts: 29500.0)
    assert built.rejected == 1
    assert built.reasons == {"created_at_before_timestamp": 1}
    assert built.rejected_frac <= cfg.max_rejected_frame_frac
    assert built.accepted >= cfg.min_session_frames


def test_data_clock_never_consults_created_at():
    """The optimistic clock must not reject rows it does not use."""
    row = _frame(10, 0, call_wall=29500.0, created_at=None)
    snap = build_snapshot(row, _cfg(availability_clock=CLOCK_DATA), spot=29500.0)
    assert snap.available_at == _ts(10, 0)


def test_no_qualifying_frame_yields_none_not_the_next_one():
    cfg = _cfg(availability_clock=CLOCK_DATA)
    tl = GammaTimeline([build_snapshot(_frame(10, 30, call_wall=29500.0), cfg, spot=29500.0)])
    # Every frame is AFTER this touch: the honest answer is "no information".
    assert tl.as_of(_ts(10, 0), 0) is None


# ── 4 / 14. Re-centring: ranks are as of the lead time, not the touch ──


def _strike_rows(peak: float, others: list[float]) -> list[dict]:
    rows = [{"strike": peak, "call_gamma": 9.0e6, "put_gamma": 1.0e5}]
    rows += [{"strike": s, "call_gamma": 1.0e6, "put_gamma": 1.0e5} for s in others]
    return rows


def test_a_strike_that_becomes_gex_1_after_price_arrives_is_not_confluence():
    """The brief's requirement 4, stated as a path.

    Before the touch the dominant call strike is 29800.  Only AFTER price
    reaches 29650 does 29650 become the top-ranked strike (gamma piles up at
    the money on 0DTE).  Confluence at the touch must see the EARLIER ladder.
    """
    cfg = _cfg(availability_clock=CLOCK_DATA, gamma_min_lead_seconds=60)
    early = build_snapshot(
        _frame(10, 0, call_wall=29800.0),
        cfg,
        spot=29500.0,
        strike_rows=_strike_rows(29800.0, [29650.0, 29900.0]),
    )
    late = build_snapshot(
        _frame(10, 5, call_wall=29650.0),
        cfg,
        spot=29650.0,
        strike_rows=_strike_rows(29650.0, [29800.0, 29900.0]),
    )
    tl = GammaTimeline([early, late])

    touch_ts, level = _ts(10, 5), 29650.0
    snap = tl.as_of(touch_ts, cfg.gamma_min_lead_seconds)
    assert snap is early, "lead time must exclude the frame stamped at the touch"

    conf = confluence_at(snap, level, max_distance=50.0)
    top = [lv for lv in snap.levels if lv.kind == KIND_GEX_CALL_RANK and lv.rank == 1]
    assert top and top[0].price == 29800.0, "C1 must still be the pre-touch strike"
    # 29650 is present only as a LOWER-ranked strike, never as rank 1.
    at_level = [lv for lv, _ in conf.matches if lv.price == 29650.0]
    assert all(lv.rank != 1 for lv in at_level if lv.rank is not None)

    # Sanity: the bug this guards against would have found rank 1 here.
    late_conf = confluence_at(late, level, max_distance=50.0)
    assert any(lv.rank == 1 for lv, _ in late_conf.matches if lv.kind == KIND_GEX_CALL_RANK)


def test_gex_rank_availability_is_reported_on_the_snapshot():
    """Regression: the ladder was being built and then not flagged, so every
    row claimed ranked levels were unavailable and the ranked-confluence arm
    would have silently reported an empty cohort."""
    cfg = _cfg(availability_clock=CLOCK_DATA)
    rows = _strike_rows(29800.0, [29650.0, 29400.0])
    with_ranks = build_snapshot(
        _frame(10, 0, call_wall=29800.0), cfg, spot=29500.0, strike_rows=rows
    )
    assert with_ranks.gex_ranks_available is True
    assert with_ranks.to_audit()["gex_ranks_available"] is True
    assert any(lv.label == "C1" for lv in with_ranks.levels)

    # No strike rows (outside the gex_by_strike retention window) degrades to
    # "no ranked levels", never to "no session".
    without = build_snapshot(_frame(10, 0, call_wall=29800.0), cfg, spot=29500.0)
    assert without.gex_ranks_available is False
    assert without.levels, "wall / flip / max-pain levels must still be present"


def test_gex_ladder_is_ranked_against_the_snapshot_spot_not_the_touch_spot():
    """Spot decides which side of the book a strike is on, so it must be the
    snapshot's own spot — passing a later spot re-partitions the whole ladder."""
    cfg = _cfg(availability_clock=CLOCK_DATA)
    rows = _strike_rows(29800.0, [29650.0, 29550.0])
    low = build_snapshot(_frame(10, 0, call_wall=29800.0), cfg, spot=29500.0, strike_rows=rows)
    high = build_snapshot(_frame(10, 0, call_wall=29800.0), cfg, spot=29700.0, strike_rows=rows)

    def calls(s):
        return {lv.price for lv in s.levels if lv.kind == KIND_GEX_CALL_RANK}

    # 29650 is above spot at 29500 (eligible as a call wall) but below it at
    # 29700 (no longer eligible). The two ladders must therefore differ.
    assert 29650.0 in calls(low)
    assert 29650.0 not in calls(high)


# ── 5. First-touch semantics ──────────────────────────────────────────


def test_a_grind_at_one_level_produces_exactly_one_event():
    cfg = _cfg()
    bars = _or_bars() + [_bar(5, 29550, 29650, 29550, 29648)]
    bars += [_bar(i, 29648, 29651, 29646, 29648) for i in range(6, 60)]
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg)
    events = extract_touch_events("NQ", orange, build_ladder(orange, cfg), bars, cfg, tick=0.25)
    plus_50 = [e for e in events if e.rung_label == "+50"]
    assert len(plus_50) == 1
    assert plus_50[0].touch_ordinal == 1


def test_rearm_requires_both_a_cooldown_and_real_travel():
    """With re-arming on, a level still must be LEFT before it can fire again."""
    cfg_grind = _cfg(rearm_minutes=5, rearm_distance_r=0.25)
    bars = _or_bars() + [_bar(5, 29550, 29650, 29550, 29648)]
    bars += [_bar(i, 29648, 29651, 29646, 29648) for i in range(6, 60)]
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg_grind)
    events = extract_touch_events(
        "NQ", orange, build_ladder(orange, cfg_grind), bars, cfg_grind, tick=0.25
    )
    # Price never travelled 0.25R (25 pts) away, so no re-arm despite the cooldown.
    assert len([e for e in events if e.rung_label == "+50"]) == 1


def test_one_bar_through_several_rungs_fires_them_nearest_first():
    cfg = _cfg()
    bars = _or_bars() + [_bar(5, 29550, 29760, 29550, 29755)]
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg)
    events = extract_touch_events("NQ", orange, build_ladder(orange, cfg), bars, cfg, tick=0.25)
    labels = [e.rung_label for e in events]
    assert labels == ["+50", "+100", "+150"]
    assert all(e.same_bar_touches == 3 for e in events)


# ── 6. Outcome labelling ──────────────────────────────────────────────


def _outcome_for(tail: list[Bar], cfg: ResearchConfig | None = None) -> list:
    cfg = cfg or _cfg()
    bars = _or_bars() + tail
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg)
    return extract_touch_events("NQ", orange, build_ladder(orange, cfg), bars, cfg, tick=0.25)


def test_previous_before_next_is_a_reversal():
    events = _outcome_for(
        [_bar(5, 29550, 29650, 29550, 29640), _bar(6, 29640, 29645, 29595, 29600)]
    )
    assert events[0].rung_label == "+50"
    assert events[0].outcome == OUTCOME_REVERSAL
    assert events[0].minutes_to_prev == 1.0


def test_next_before_previous_is_a_continuation():
    events = _outcome_for(
        [_bar(5, 29550, 29650, 29550, 29645), _bar(6, 29645, 29705, 29645, 29700)]
    )
    assert events[0].outcome == OUTCOME_CONTINUATION
    assert events[0].minutes_to_next == 1.0


def test_a_bar_that_slices_past_the_next_rung_is_labelled_same_bar():
    events = _outcome_for([_bar(5, 29550, 29760, 29550, 29755)])
    assert events[0].outcome == OUTCOME_CONTINUATION_SAME_BAR


def test_both_rungs_inside_one_minute_is_ambiguous_not_a_coin_flip():
    events = _outcome_for(
        [_bar(5, 29550, 29650, 29550, 29640), _bar(6, 29640, 29705, 29595, 29650)]
    )
    assert events[0].outcome == OUTCOME_AMBIGUOUS


def test_unresolved_by_the_bell_is_censored_not_counted_as_a_hold():
    events = _outcome_for(
        [_bar(5, 29550, 29650, 29550, 29648)]
        + [_bar(i, 29648, 29651, 29646, 29648) for i in range(6, 60)]
    )
    assert events[0].outcome == OUTCOME_CENSORED
    stat = summarize([{"outcome": OUTCOME_CENSORED, "symbol": "NQ", "session": "x"}])
    assert stat["n_resolved"] == 0, "censored rows must not enter the rate denominator"


def test_down_side_outcomes_mirror_the_up_side():
    events = _outcome_for(
        [_bar(5, 29550, 29550, 29450, 29455), _bar(6, 29455, 29505, 29455, 29500)]
    )
    assert events[0].rung_label == "-50"
    assert events[0].outcome == OUTCOME_REVERSAL


def test_mfe_and_mae_are_oriented_to_the_reversion_trade():
    assert reversion_sign("up") == -1
    assert reversion_sign("down") == 1
    cfg = _cfg()
    bars = _or_bars() + [_bar(5, 29550, 29650, 29550, 29650)]
    bars += [_bar(i, 29650, 29650, 29600, 29600) for i in range(6, 40)]
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg)
    events = extract_touch_events("NQ", orange, build_ladder(orange, cfg), bars, cfg, tick=0.25)
    series = BarSeries(bars)
    up = measure_outcome(events[0], series, cfg)
    # Price fell 50 points after an UP-side touch: favourable for the reversion.
    assert up["mfe_pts_15"] == pytest.approx(50.0)
    assert up["mae_pts_15"] == pytest.approx(0.0)


# ── 7. Sessions, timezones, DST ───────────────────────────────────────


@pytest.mark.parametrize(
    "session, expected_offset_hours",
    [(date(2026, 1, 15), -5), (date(2026, 7, 15), -4), (date(2026, 11, 2), -5)],
)
def test_session_window_is_0930_et_across_dst(session, expected_offset_hours):
    start, end = session_window(session, _cfg())
    assert (start.hour, start.minute) == (9, 30)
    assert (end.hour, end.minute) == (16, 0)
    assert start.utcoffset() == timedelta(hours=expected_offset_hours)


def test_dst_transition_days_are_still_six_and_a_half_hours():
    for session in (date(2026, 3, 8), date(2026, 11, 1)):
        start, end = session_window(session, _cfg())
        assert (end - start) == timedelta(hours=6, minutes=30)


def test_events_carry_the_et_session_date_not_the_utc_one():
    """A 16:00 ET bar is the NEXT UTC day in winter; the session must not shift."""
    cfg = _cfg()
    winter = date(2026, 1, 15)
    bars = _or_bars(winter) + [_bar(5, 29550, 29650, 29550, 29640, winter)]
    orange, _ = build_opening_range(bars, winter, "NQ", cfg)
    events = extract_touch_events("NQ", orange, build_ladder(orange, cfg), bars, cfg, tick=0.25)
    assert events[0].session == winter
    assert events[0].touched_at.astimezone(ET).date() == winter


# ── 9 / 15. Futures mapping and basis anchoring use production logic ──


def test_instrument_mapping_matches_production_symbols_module():
    from src.symbols import resolve_futures_index, resolve_futures_tick

    for future in ("ES", "NQ"):
        s = spec(future)
        assert s.gamma_symbol == resolve_futures_index(future)
        assert s.tick == resolve_futures_tick(future)
        assert s.needs_basis is True
        # futures_quotes is keyed by the CASH index, not the future.
        assert s.bar_symbol == resolve_futures_index(future)
    for cash in ("SPY", "QQQ", "SPX", "NDX"):
        assert spec(cash).needs_basis is False


def test_projection_moves_prices_and_leaves_dollar_quantities_alone():
    from src.jobs.futures_projection import FuturesBasis

    cfg = _cfg(availability_clock=CLOCK_DATA)
    snap = build_snapshot(_frame(10, 0, call_wall=29530.0), cfg, spot=29500.0)
    basis = FuturesBasis("NDX", "NQ", 1.00713, "measured", None, 15, "@NQ")
    out = project_snapshot(snap, basis, spec("NQ").tick)

    before = {lv.kind: lv.price for lv in snap.levels}
    after = {lv.kind: lv.price for lv in out.levels}
    for kind, price in before.items():
        assert after[kind] == pytest.approx(basis.project(price, tick=0.25))
        assert after[kind] % 0.25 == pytest.approx(0.0)  # on the NQ tick grid
    assert out.total_net_gex == snap.total_net_gex
    assert out.spot == snap.spot  # spot stays on the index axis


def test_a_cash_symbol_projects_to_itself():
    cfg = _cfg(availability_clock=CLOCK_DATA)
    snap = build_snapshot(_frame(10, 0, call_wall=29530.0), cfg, spot=29500.0)
    assert project_snapshot(snap, None, None) is snap


def test_historical_basis_read_refuses_an_unanchored_call():
    """``at=None`` would apply today's carry ratio to a past frame."""
    import asyncio

    reader = BasisReader(conn=None)
    with pytest.raises(BasisUnavailable):
        asyncio.run(reader.get_futures_basis_samples("NDX", at=None))


# ── 10 / 12. Determinism and cache isolation ──────────────────────────


def test_the_same_seed_and_config_reproduce_the_same_events():
    from research.or_gamma_confluence.selftest import _rows_for

    cfg = ResearchConfig(availability_clock=CLOCK_VISIBLE)

    def run():
        rng = random.Random(99)
        return [
            json.dumps(r, sort_keys=True, default=str)
            for i in range(6)
            for r in _rows_for(SESSION + timedelta(days=i), rng, cfg, effect=0.4)
        ]

    assert run() == run()


def test_config_fingerprint_changes_with_every_parameter_that_changes_an_answer():
    base = ResearchConfig()
    for field, value in (
        ("opening_range_minutes", 15),
        ("extension_step", 0.25),
        ("max_extension", 5.0),
        ("touch_tolerance_bp", 5.0),
        ("gamma_min_lead_seconds", 60),
        ("availability_clock", CLOCK_DATA),
        ("client_poll_lag_seconds", 10),
        ("extension_mode", MODE_OPEN),
        ("use_gex_ranks", False),
        ("confluence_buckets_pts", (1.0, 2.0)),
    ):
        assert base.variant(**{field: value}).fingerprint() != base.fingerprint(), field


def test_relabelling_a_run_does_not_invalidate_its_cache():
    base = ResearchConfig()
    assert base.variant(label="anything at all").fingerprint() == base.fingerprint()


def test_analyze_refuses_a_dataset_that_mixes_config_fingerprints(tmp_path: Path):
    from research.or_gamma_confluence.cli import main

    a, b = ResearchConfig(), ResearchConfig(opening_range_minutes=30)
    path = tmp_path / "mixed.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "outcome": OUTCOME_REVERSAL,
                    "symbol": "NQ",
                    "session": "2026-09-04",
                    "extension_k": 1.0,
                    "config_fingerprint": c.fingerprint(),
                }
            )
            for c in (a, b)
        )
        + "\n"
    )
    (tmp_path / "mixed.jsonl.meta.json").write_text(
        json.dumps({"config_fingerprint": a.fingerprint()})
    )
    assert main(["analyze", str(path)]) == 2


# ── 11. Transaction costs — Phase 4, not yet built ───────────────────


@pytest.mark.skip(
    reason="Phase 4. TradeWorkz has no futures execution model (no point value, "
    "tick value or round-turn commission anywhere in src/tradeworkz, "
    "src/backtesting or src/signals/execution.py), so the fill model this test "
    "covers does not exist yet. Config already carries slippage_ticks, "
    "commission_round_turn and entry_delay_bars; the spec is in "
    "docs/design/or-extension-gamma-confluence.md section 6."
)
def test_simulated_pnl_includes_slippage_and_fees():  # pragma: no cover
    raise AssertionError("unimplemented")


# ── Cash-index open-bar phantom (a 5-minute OR is 20% this bar) ────────


def test_phantom_open_bar_is_repaired_for_a_cash_index():
    """SPX/NDX print a stale opening-rotation value at 09:30; on a gap day it
    becomes a bar extreme and would set ORH or ORL outright."""
    start = _ts(9, 30)
    phantom = Bar(start, 29600.0, 29600.0, 29400.0, 29420.0)  # open == high, gapped
    bars = [phantom, Bar(start + timedelta(minutes=1), 29420, 29450, 29410, 29440)]

    repaired, did = dephantom_open_bar(bars, "NDX", start)
    assert did is True
    assert repaired[0].open == 29420.0
    assert repaired[0].high == 29420.0  # the stale 29600 is gone
    assert repaired[0].low == 29400.0  # the real extreme is kept

    # Idempotent, and never applied to a symbol with real traded opens.
    assert dephantom_open_bar(repaired, "NDX", start)[1] is False
    assert dephantom_open_bar(bars, "SPY", start)[1] is False
    assert dephantom_open_bar(bars, "NQ", start)[1] is False


def test_phantom_repair_changes_the_opening_range():
    """The reason the repair matters: it moves the whole ladder."""
    cfg = _cfg()
    start = _ts(9, 30)
    bars = [Bar(start, 29600.0, 29600.0, 29400.0, 29420.0)] + [
        Bar(start + timedelta(minutes=i), 29420, 29440, 29410, 29430) for i in range(1, 5)
    ]
    ndx, _ = build_opening_range(bars, SESSION, "NDX", cfg)  # repaired
    spy, _ = build_opening_range(bars, SESSION, "SPY", cfg)  # not a cash index
    assert ndx.high == 29440.0 and spy.high == 29600.0
    assert ndx.width != spy.width


# ── Feature-level anti-look-ahead ─────────────────────────────────────


def test_features_do_not_move_when_the_future_is_poisoned():
    cfg = _cfg()
    bars = _or_bars() + [_bar(5, 29550, 29650, 29550, 29640)]
    bars += [_bar(i, 29640, 29660, 29620, 29640) for i in range(6, 60)]
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg)
    ladder = build_ladder(orange, cfg)
    events = extract_touch_events("NQ", orange, ladder, bars, cfg, tick=0.25)
    ev = events[0]

    poisoned = [b if b.ts <= ev.touched_at else Bar(b.ts, 1e6, 1e6, 1e6, 1e6) for b in bars]
    clean = build_features(ev, SessionContext.build("NQ", bars, cfg), orange, ladder, cfg)
    dirty = build_features(ev, SessionContext.build("NQ", poisoned, cfg), orange, ladder, cfg)
    assert clean == dirty


def test_consecutive_extensions_broken_reads_the_path_not_later_outcomes():
    """Price runs +50 -> +100 -> +150 without retracing: two inner rungs broken.

    Derived from bars before the touch, so a later reversal cannot change it.
    """
    cfg = _cfg()
    bars = _or_bars() + [
        _bar(5, 29550, 29650, 29550, 29648),
        _bar(6, 29648, 29700, 29645, 29698),
        _bar(7, 29698, 29750, 29695, 29748),
    ]
    bars += [_bar(i, 29748, 29750, 29450, 29460) for i in range(8, 40)]  # huge later reversal
    orange, _ = build_opening_range(bars, SESSION, "NQ", cfg)
    ladder = build_ladder(orange, cfg)
    events = extract_touch_events("NQ", orange, ladder, bars, cfg, tick=0.25)
    ctx = SessionContext.build("NQ", bars, cfg)

    at_150 = next(e for e in events if e.rung_label == "+150")
    idx = events.index(at_150)
    feats = build_features(at_150, ctx, orange, ladder, cfg, prior=events[:idx])
    assert feats["consecutive_extensions_broken"] == 2
    assert feats["prior_extension_respect_score"] == pytest.approx(0.0)


def test_vwap_is_unavailable_rather_than_fabricated_without_volume():
    cfg = _cfg()
    bars = _or_bars() + [_bar(5, 29550, 29650, 29550, 29640)]
    ctx = SessionContext.build("NDX", bars, cfg, volumes={})
    orange, _ = build_opening_range(bars, SESSION, "NDX", cfg)
    ladder = build_ladder(orange, cfg)
    events = extract_touch_events("NDX", orange, ladder, bars, cfg, tick=None)
    feats = build_features(events[0], ctx, orange, ladder, cfg)
    assert feats["vwap_available"] is False
    assert feats["vwap"] is None
    assert feats["distance_from_vwap_pts"] is None


# ── Cohorts and reporting ─────────────────────────────────────────────


def test_a_row_with_no_gamma_frame_joins_the_no_confluence_cohort():
    """It must not vanish: 'no structure was there' is the comparison group."""
    row = {
        "outcome": OUTCOME_REVERSAL,
        "symbol": "NQ",
        "session": "2026-09-04",
        "extension_k": 3.0,
        "gamma_available": False,
    }
    cs = {
        c.key: c for c in build_cohorts(confluence_distance=10.0, min_extension=2.0, min_broken=2)
    }
    assert cs["no_confluence"].predicate(row) is True
    assert cs["confluence"].predicate(row) is False


def test_summarize_always_exposes_n_and_session_count():
    rows = [
        {"outcome": OUTCOME_REVERSAL, "symbol": "NQ", "session": "2026-09-04"},
        {"outcome": OUTCOME_CONTINUATION, "symbol": "NQ", "session": "2026-09-04"},
        {"outcome": OUTCOME_CENSORED, "symbol": "NQ", "session": "2026-09-05"},
    ]
    stat = summarize(rows)
    assert stat["n"] == 3 and stat["n_resolved"] == 2 and stat["n_sessions"] == 2
    assert stat["reversal_rate"] == pytest.approx(0.5)
    assert stat["thin"] is True


def test_out_of_sample_split_is_chronological_and_session_aligned():
    from research.or_gamma_confluence.report import split_chronological

    rows = [
        {"session": f"2026-06-{d:02d}", "symbol": "NQ", "outcome": OUTCOME_REVERSAL}
        for d in range(1, 11)
        for _ in range(3)
    ]
    parts = split_chronological(rows, ResearchConfig())
    dates = {k: sorted({r["session"] for r in v}) for k, v in parts.items()}
    assert max(dates["discovery"]) < min(dates["validation"])
    assert max(dates["validation"]) < min(dates["test"])
    # No session may straddle a split boundary.
    assert not (set(dates["discovery"]) & set(dates["test"]))


# ── The synthetic end-to-end check ────────────────────────────────────


def test_selftest_passes():
    assert run_selftest(sessions=40, seed=7, verbose=False) == 0


# ── Clustering unit and pooling ───────────────────────────────────────


def test_inference_clusters_on_the_calendar_session_not_the_symbol_session():
    """SPY, SPX and ES are one option book on one set of days. Counting three
    symbol-sessions as three independent clusters would shrink every interval
    by about sqrt(3)."""
    rows = [
        {
            "symbol": sym,
            "gamma_symbol": "SPX" if sym in ("SPX", "ES") else sym,
            "session": "2026-07-01",
            "outcome": OUTCOME_REVERSAL,
        }
        for sym in ("SPY", "SPX", "ES")
    ]
    stat = summarize(rows)
    assert stat["n_sessions"] == 1, "one calendar day is one observation"
    assert stat["n_symbol_sessions"] == 3


def test_book_of_maps_price_axes_onto_their_option_chain():
    assert book_of({"symbol": "NQ", "gamma_symbol": "NDX"}) == "Nasdaq"
    assert book_of({"symbol": "ES", "gamma_symbol": "SPX"}) == "S&P"
    assert book_of({"symbol": "SPY"}) == "S&P"
    assert book_of({"symbol": "QQQ"}) == "Nasdaq"
    assert book_of({"symbol": "IWM"}) is None


def _book_rows(sp_delta: float, ndx_delta: float, *, days: int = 40, seed: int = 3):
    rng = random.Random(seed)
    rows = []
    for d in range(days):
        offset = rng.gauss(0.0, 0.08)
        for sym, gamma, delta in (
            ("SPY", "SPY", sp_delta),
            ("SPX", "SPX", sp_delta),
            ("QQQ", "QQQ", ndx_delta),
            ("NDX", "NDX", ndx_delta),
        ):
            for _ in range(10):
                conf = rng.random() < 0.35
                p = min(0.98, max(0.02, 0.55 + offset + (delta if conf else 0.0)))
                rows.append(
                    {
                        "symbol": sym,
                        "gamma_symbol": gamma,
                        "session": f"2026-07-{d % 28 + 1:02d}",
                        "outcome": (OUTCOME_REVERSAL if rng.random() < p else OUTCOME_CONTINUATION),
                        "conf": conf,
                    }
                )
    return rows


def test_pooling_check_refuses_to_pool_books_that_disagree():
    rows = _book_rows(sp_delta=0.20, ndx_delta=0.0)
    result = pooling_check(rows, lambda r: r["conf"], iterations=600)
    assert result["verdict"] == "books_disagree"
    assert set(result["per_book"]) == {"S&P", "Nasdaq"}
    assert (
        result["per_book"]["S&P"]["clustered_diff"] > result["per_book"]["Nasdaq"]["clustered_diff"]
    )


def test_pooling_check_allows_pooling_when_books_agree():
    rows = _book_rows(sp_delta=0.12, ndx_delta=0.12, seed=11)
    result = pooling_check(rows, lambda r: r["conf"], iterations=600)
    assert result["verdict"] == "consistent"


def test_pooling_check_says_so_when_only_one_book_is_present():
    rows = [r for r in _book_rows(sp_delta=0.10, ndx_delta=0.10) if r["symbol"] in ("SPY", "SPX")]
    assert pooling_check(rows, lambda r: r["conf"], iterations=400)["verdict"] == "single_book"


def test_compare_to_baseline_point_estimate_lies_inside_its_own_interval():
    """Regression: the bootstrap returns (ci_lo, ci_hi, p) and was once
    unpacked as (diff, lo, hi), which produced reversed bounds that did not
    contain their own point estimate."""
    rows = _book_rows(sp_delta=0.15, ndx_delta=0.15, seed=5)
    res = compare_to_baseline(rows, lambda r: r["conf"], iterations=600)
    assert res["clustered_ci_low"] < res["clustered_ci_high"]
    assert res["clustered_ci_low"] <= res["clustered_diff"] <= res["clustered_ci_high"]
    assert res["clustered_diff"] == pytest.approx(res["rate"] - res["baseline_rate"])
