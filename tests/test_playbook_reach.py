"""Exits sized to the move price can make before a Card's hold runs out."""

from __future__ import annotations

import math
import random
from datetime import date, datetime, timezone
from typing import Optional

import pytest

from src import market_calendar
from src.signals.components.base import MarketContext
from src.signals.playbook import reach
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.patterns.max_pain_gravitation import PATTERN as MAX_PAIN
from src.signals.playbook.patterns.vwap_reversion import PATTERN as VWAP_REV

# 18:30 UTC = 14:30 ET and 15:00 UTC = 11:00 ET on 2026-05-01 (EDT).
TS_1430 = datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)
TS_1100 = datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc)
TS_1530 = datetime(2026, 5, 1, 19, 30, tzinfo=timezone.utc)


def _walk(end: float, sigma: float, n: int = 120, seed: int = 7) -> list[float]:
    """``n`` 1-minute closes of a random walk with per-minute ``sigma``,
    shifted so the last close is ``end``."""
    rng = random.Random(seed)
    prices = [100.0]
    for _ in range(n - 1):
        prices.append(prices[-1] * (1.0 + rng.gauss(0.0, sigma)))
    scale = end / prices[-1]
    return [p * scale for p in prices]


# ----------------------------------------------------------------------
# Measuring the move
# ----------------------------------------------------------------------


def test_sigma_needs_enough_bars():
    assert reach.minute_sigma([100.0] * 10) is None
    assert reach.expected_move(100.0, [100.0] * 10, 60) is None


def test_sigma_recovers_the_walk_volatility():
    sigma = reach.minute_sigma(_walk(660.0, 0.0004))
    assert sigma == pytest.approx(0.0004, rel=0.25)


def test_one_gap_does_not_inflate_sigma():
    closes = _walk(660.0, 0.0004)
    gapped = closes[:90] + [c * 1.02 for c in closes[90:]]  # a 2% jump
    assert reach.minute_sigma(gapped) == pytest.approx(reach.minute_sigma(closes), rel=0.25)


def test_evenly_stepping_prices_do_not_collapse_sigma():
    """A median-based estimate reads ~0 here; the trimmed deviation doesn't."""
    closes = [743.0 + (i % 4) * 0.10 for i in range(60)]
    assert reach.minute_sigma(closes) > 1e-4


def test_expected_move_scales_with_the_square_root_of_time():
    closes = _walk(660.0, 0.0004)
    one = reach.expected_move(660.0, closes, 25)
    four = reach.expected_move(660.0, closes, 100)
    assert four == pytest.approx(2.0 * one)


def test_0dte_hold_stops_at_the_close():
    assert reach.usable_hold(TS_1530, 120, "0DTE") == 30
    assert reach.usable_hold(TS_1100, 120, "0DTE") == 120
    assert reach.usable_hold(TS_1530, 120, "swing") == 120


def test_0dte_hold_stops_at_an_early_close(monkeypatch):
    monkeypatch.setattr(market_calendar, "NYSE_HALF_DAYS", {date(2026, 5, 1)})
    assert reach.usable_hold(TS_1100, 120, "0DTE") == 120  # 11:00 -> 13:00
    noon = datetime(2026, 5, 1, 16, 30, tzinfo=timezone.utc)  # 12:30 ET
    assert reach.usable_hold(noon, 120, "0DTE") == 30


# ----------------------------------------------------------------------
# Sizing the exits
# ----------------------------------------------------------------------


def test_level_within_reach_is_the_target():
    exits = reach.size_exits(
        direction="bullish", entry=100.0, move=1.0, level=100.5, level_name="vwap"
    )
    assert (exits.target, exits.target_name) == (100.5, "vwap")
    assert exits.stop == pytest.approx(100.0 - reach.STOP_MULT)


def test_level_out_of_reach_is_aimed_at_partway():
    exits = reach.size_exits(
        direction="bearish", entry=100.0, move=1.0, level=97.0, level_name="max_pain"
    )
    assert exits.target == pytest.approx(100.0 - reach.TARGET_MULT)
    assert exits.target_name == "toward_max_pain"
    assert exits.stop == pytest.approx(100.0 + reach.STOP_MULT)


def test_no_level_aims_at_the_move():
    exits = reach.size_exits(
        direction="bullish", entry=100.0, move=1.0, level=None, level_name=None
    )
    assert exits.target_name == "expected_move"
    assert exits.target == pytest.approx(100.0 + reach.TARGET_MULT)


def test_a_closer_structural_stop_wins():
    exits = reach.size_exits(
        direction="bullish",
        entry=100.0,
        move=1.0,
        level=None,
        level_name=None,
        structural_stop=99.8,
    )
    assert exits.stop == pytest.approx(99.8)


def test_the_stop_stays_past_the_structure_it_protects():
    """Fading a call wall at 101 from 100.5: a volatility stop at 100.9 would
    sit below the wall, so the stop moves to just above it."""
    exits = reach.size_exits(
        direction="bearish",
        entry=100.5,
        move=0.53,
        level=None,
        level_name=None,
        structural_stop=101.3,
        beyond=101.0,
    )
    assert exits.stop == pytest.approx(101.0 * (1 + reach.STRUCTURE_BUFFER_PCT))


def test_too_small_a_target_means_no_card():
    assert (
        reach.size_exits(direction="bullish", entry=660.0, move=0.3, level=None, level_name=None)
        is None
    )


# ----------------------------------------------------------------------
# The patterns
# ----------------------------------------------------------------------


def _mr_ctx(
    *,
    close: float = 678.4,
    closes: Optional[list[float]] = None,
    max_pain: Optional[float] = 668.0,
    vwap: Optional[float] = None,
    timestamp: datetime = TS_1430,
) -> PlaybookContext:
    market = MarketContext(
        timestamp=timestamp,
        underlying="SPY",
        close=close,
        net_gex=7.1e9,
        gamma_flip=676.5,
        put_call_ratio=0.9,
        max_pain=max_pain,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=closes if closes is not None else _walk(close, 0.0004),
        iv_rank=None,
        vwap=vwap,
    )
    return PlaybookContext(
        market=market,
        msi_score=0.0,
        msi_regime="chop_range",
        msi_components={},
        advanced_signals={},
        basic_signals={},
        levels={"max_pain": max_pain},
    )


def test_max_pain_aims_partway_when_max_pain_is_out_of_reach():
    # Max pain 1.5% below; price moves ~0.3% in 90 minutes.
    ctx = _mr_ctx(timestamp=TS_1100)
    card = MAX_PAIN.match(ctx)
    move = reach.expected_move(678.4, ctx.market.recent_closes, 120)
    assert card.direction == "bearish"
    assert card.target.level_name == "toward_max_pain"
    assert 678.4 - card.target.ref_price == pytest.approx(reach.TARGET_MULT * move, abs=1e-3)
    assert card.stop.ref_price - 678.4 == pytest.approx(reach.STOP_MULT * move, abs=1e-3)
    assert card.max_hold_minutes == 120
    assert "sized to the" in card.rationale
    assert card.context["expected_move_pct"] > 0


def test_max_pain_within_reach_is_the_target():
    # 0.35% below; at 0.08%/min over the 90 minutes left, reach is ~0.57%.
    ctx = _mr_ctx(max_pain=676.0, closes=_walk(678.4, 0.0008))
    card = MAX_PAIN.match(ctx)
    assert card.target.level_name == "max_pain"
    assert card.target.ref_price == pytest.approx(676.0)


def test_max_pain_hold_shrinks_into_the_close():
    early = MAX_PAIN.match(_mr_ctx(timestamp=TS_1100))
    late = MAX_PAIN.match(_mr_ctx(timestamp=TS_1530))
    assert late.max_hold_minutes == 30
    assert (678.4 - late.target.ref_price) == pytest.approx(
        (678.4 - early.target.ref_price) * math.sqrt(30 / 120), abs=1e-3
    )


def test_max_pain_stands_down_in_a_dead_market():
    ctx = _mr_ctx(closes=_walk(678.4, 0.00001))
    assert MAX_PAIN.match(ctx) is None
    assert any(m.startswith("too quiet") for m in MAX_PAIN.explain_miss(ctx))


def test_max_pain_without_bar_history_keeps_the_catalog_exits():
    card = MAX_PAIN.match(_mr_ctx(closes=[678.4]))
    assert (card.target.ref_price, card.target.level_name) == (668.0, "max_pain")
    assert card.stop.ref_price == pytest.approx(678.4 * 1.004, abs=1e-3)


def test_vwap_reversion_is_sized_the_same_way():
    ctx = _mr_ctx(vwap=688.0, max_pain=None, timestamp=TS_1100)  # 1.4% above
    card = VWAP_REV.match(ctx)
    assert card.direction == "bullish"
    assert card.target.level_name == "toward_vwap"
    assert card.target.ref_price < 688.0
    assert card.stop.ref_price < 678.4


def test_wall_fade_stop_is_back_above_the_wall_and_within_reach():
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CALL_WALL_FADE
    from tests.test_playbook_call_wall_fade import _ctx as cwf_ctx

    closes = _walk(678.4, 0.0004)
    card = CALL_WALL_FADE.match(cwf_ctx(recent_closes=closes))
    move = reach.expected_move(678.4, closes, 90)
    assert card.target.level_name in ("max_pain", "toward_max_pain")
    assert card.target.ref_price >= 675.0
    assert card.stop.kind == "level"
    assert card.stop.ref_price > 678.0  # past the wall
    assert card.stop.ref_price <= 678.0 * 1.0030  # never past the catalog stop
    assert card.stop.ref_price - 678.4 <= max(reach.STOP_MULT * move, 678.0 * 1.0005 - 678.4)


def test_put_wall_bounce_mirrors_it():
    from src.signals.playbook.patterns.put_wall_bounce import PATTERN as PUT_WALL_BOUNCE
    from tests.test_playbook_put_wall_bounce import _ctx as pwb_ctx

    probe = pwb_ctx()
    closes = _walk(probe.close, 0.0004)
    card = PUT_WALL_BOUNCE.match(pwb_ctx(recent_closes=closes))
    wall = probe.level("put_wall")
    assert card.direction == "bullish"
    assert card.stop.kind == "level"
    assert card.stop.ref_price < wall
    assert card.target.ref_price > probe.close
