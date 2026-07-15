"""Volatility-scaled, wick-confirmed wall-break stop (commit 2).

The Bouncer fades a wall expecting it to hold. Its stop must ``allow it to
play out`` — walls get pierced by noise and revert — without letting a
genuine break run. These tests lock three things:

1. **Correct fade semantics.** The prior BaseBot._wall_stop_signal keyed the
   break off the option ``direction`` and inverted it: for a call-wall fade
   (bearish) it fired ``wall_break`` when spot fell TOWARD the target,
   capping winners at wall∓drift and mislabeling them, and the migration
   branch never fired. The rewrite keys off the wall ``side`` (the
   barrier): a call wall fails on a push ABOVE it, a put wall on a break
   BELOW it. A fade moving toward its target must never be cut as a break.

2. **Volatility scaling.** The buffer is ``clamp(vol_mult × sigma_1min,
   min, max)``, widened for a historically large wall — wider in a high-vol
   tape / big wall, and floored at the old 0.3% so calm-market behavior is
   unchanged (never tighter than before).

3. **Wick confirmation.** A single print beyond the buffer does not stop the
   trade; the break must persist across ``confirm_bars`` one-minute closes.
"""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import pytest

from src.tradeworkz.bots.base import BaseBot, _short_horizon_sigma_pct
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, OpenPosition

TS = datetime(2026, 7, 14, 15, 30, tzinfo=timezone.utc)  # 11:30 ET, mid-session


def _bot(**params) -> BaseBot:
    spec = BotSpec(
        id="wall_bot",
        display_name="Wall Bot",
        strategy_class="BaseBot",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params=dict(params),
    )
    return BaseBot(spec)


def _snap(
    spot: float,
    *,
    vix: Optional[float] = None,
    recent_closes: Optional[List[float]] = None,
    call_wall: Optional[float] = None,
    put_wall: Optional[float] = None,
) -> MarketSnapshot:
    return MarketSnapshot(
        underlying="SPY",
        timestamp=TS,
        spot=spot,
        vix=vix,
        call_wall=call_wall,
        put_wall=put_wall,
        recent_closes=list(recent_closes or []),
    )


def _pos(
    *,
    direction: str,
    wall_ref_price: Optional[float],
    wall_ref_side: Optional[str],
    wall_strength: Optional[float] = None,
) -> OpenPosition:
    now = datetime.now(timezone.utc)
    comp = {} if wall_strength is None else {"wall_strength": wall_strength}
    return OpenPosition(
        id=1,
        bot_id="wall_bot",
        underlying="SPY",
        opened_at=now - timedelta(minutes=5),
        direction=direction,
        strategy_type="BUY_PUT_DEBIT" if direction == "bearish" else "BUY_CALL_DEBIT",
        legs=[],
        entry_price=1.0,
        current_price=1.0,
        quantity_open=10,
        unrealized_pnl=0.0,
        stop_price=None,
        target_price=None,
        time_stop_at=now + timedelta(hours=1),
        min_hold_until=now - timedelta(seconds=120),  # min-hold already elapsed
        wall_ref_price=wall_ref_price,
        wall_ref_side=wall_ref_side,
        entry_conviction=0.7,
        components_at_entry=comp,
    )


# ----------------------------------------------------------------------
# _short_horizon_sigma_pct
# ----------------------------------------------------------------------


def test_sigma_prefers_realized_closes():
    closes = [100.0, 100.5, 100.2, 100.8, 100.3, 100.9, 100.4]
    rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
    expected = statistics.pstdev(rets)
    got = _short_horizon_sigma_pct(_snap(100.4, recent_closes=closes, vix=16.0))
    assert got == pytest.approx(expected)


def test_sigma_falls_back_to_vix_when_too_few_closes():
    got = _short_horizon_sigma_pct(_snap(100.0, recent_closes=[100.0, 101.0], vix=16.0))
    assert got == pytest.approx((16.0 / 100.0) / math.sqrt(252.0 * 390.0))


def test_sigma_none_without_vol_signal():
    assert _short_horizon_sigma_pct(_snap(100.0, recent_closes=[], vix=None)) is None
    # Flat closes -> zero realized sigma -> VIX fallback; no VIX -> None.
    assert _short_horizon_sigma_pct(_snap(100.0, recent_closes=[100.0] * 8)) is None


# ----------------------------------------------------------------------
# _wall_break_buffer_pct
# ----------------------------------------------------------------------


def test_buffer_floor_without_vol_data():
    buf = _bot()._wall_break_buffer_pct(_snap(755.0, vix=None), None)
    assert buf == pytest.approx(0.003)  # min_pct floor


def test_buffer_baseline_vix_is_near_old_flat_default():
    """~16 VIX must land near the old 0.3% flat stop — no calm-tape regression."""
    buf = _bot()._wall_break_buffer_pct(_snap(755.0, vix=16.0), None)
    assert buf == pytest.approx(0.00306, abs=2e-4)


def test_buffer_widens_with_volatility_up_to_cap():
    b16 = _bot()._wall_break_buffer_pct(_snap(755.0, vix=16.0), None)
    b32 = _bot()._wall_break_buffer_pct(_snap(755.0, vix=32.0), None)
    b_huge = _bot()._wall_break_buffer_pct(_snap(755.0, vix=200.0), None)
    assert b32 > b16
    assert b_huge == pytest.approx(0.012)  # max_pct cap


def test_buffer_widens_with_wall_strength():
    base = _bot()._wall_break_buffer_pct(_snap(755.0, vix=32.0), None)
    big = _bot()._wall_break_buffer_pct(_snap(755.0, vix=32.0), 1.0)
    assert big == pytest.approx(base * 1.5)  # strength_widen 0.5 * 1.0


def test_buffer_never_below_floor_even_in_dead_calm():
    buf = _bot()._wall_break_buffer_pct(_snap(755.0, recent_closes=[755.0] * 10), None)
    assert buf == pytest.approx(0.003)


# ----------------------------------------------------------------------
# _beyond_confirmed
# ----------------------------------------------------------------------


def test_confirm_single_tick_when_disabled():
    bot = _bot(wall_break_confirm_bars=1)
    assert bot._beyond_confirmed(_snap(101.0), 100.0, above=True) is True
    assert bot._beyond_confirmed(_snap(99.0), 100.0, above=True) is False


def test_confirm_requires_consecutive_closes_beyond():
    bot = _bot(wall_break_confirm_bars=2)
    # Latest print beyond but prior close inside -> a wick, not confirmed.
    wick = _snap(101.0, recent_closes=[99.5, 101.0])
    assert bot._beyond_confirmed(wick, 100.0, above=True) is False
    # Two consecutive closes beyond -> confirmed.
    real = _snap(101.0, recent_closes=[100.5, 101.0])
    assert bot._beyond_confirmed(real, 100.0, above=True) is True


def test_confirm_below_side():
    bot = _bot(wall_break_confirm_bars=2)
    assert (
        bot._beyond_confirmed(_snap(99.0, recent_closes=[99.5, 99.0]), 100.0, above=False) is True
    )
    assert (
        bot._beyond_confirmed(_snap(99.0, recent_closes=[100.5, 99.0]), 100.0, above=False) is False
    )


def test_confirm_degrades_to_single_tick_on_thin_history():
    bot = _bot(wall_break_confirm_bars=3)
    # Only one close available (< confirm_bars) -> single-tick decision.
    assert bot._beyond_confirmed(_snap(101.0, recent_closes=[101.0]), 100.0, above=True) is True


# ----------------------------------------------------------------------
# _wall_stop_signal — corrected fade semantics (the bug fix)
# ----------------------------------------------------------------------


def test_call_fade_does_not_break_when_spot_moves_toward_target():
    """Bearish call-wall fade with spot BELOW the wall (winning) must hold.

    Regression: the old direction-keyed logic fired wall_break here,
    capping winners at wall∓drift and booking them as 'wall_break'.
    """
    pos = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call")
    snap = _snap(752.0, call_wall=755.0, recent_closes=[753.0, 752.5, 752.0])
    assert _bot()._wall_stop_signal(snap, pos) is None


def test_call_fade_breaks_when_spot_pushes_above_wall_confirmed():
    pos = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call")
    # buffer at VIX None = 0.3% -> level 757.265; two closes beyond it.
    snap = _snap(758.0, call_wall=755.0, recent_closes=[757.6, 758.0])
    decision = _bot()._wall_stop_signal(snap, pos)
    assert decision is not None and decision.reason == "wall_break"


def test_call_fade_wick_above_wall_does_not_break():
    pos = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call")
    # Latest print above 757.265 but the prior close is back inside -> wick.
    snap = _snap(758.0, call_wall=755.0, recent_closes=[756.0, 758.0])
    assert _bot()._wall_stop_signal(snap, pos) is None


def test_put_bounce_does_not_break_when_spot_moves_toward_target():
    """Bullish put-wall bounce with spot ABOVE the wall (winning) must hold."""
    pos = _pos(direction="bullish", wall_ref_price=750.0, wall_ref_side="put")
    snap = _snap(753.0, put_wall=750.0, recent_closes=[751.0, 752.0, 753.0])
    assert _bot()._wall_stop_signal(snap, pos) is None


def test_put_bounce_breaks_when_spot_falls_below_wall_confirmed():
    pos = _pos(direction="bullish", wall_ref_price=750.0, wall_ref_side="put")
    # level = 750 * (1 - 0.003) = 747.75; two closes below it.
    snap = _snap(747.0, put_wall=750.0, recent_closes=[747.4, 747.0])
    decision = _bot()._wall_stop_signal(snap, pos)
    assert decision is not None and decision.reason == "wall_break"


# ----------------------------------------------------------------------
# _wall_stop_signal — wall migration (now actually fires for the Bouncer)
# ----------------------------------------------------------------------


def test_call_wall_shift_up_cuts_the_fade():
    pos = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call")
    # Call wall rolled up past 755 * 1.003 = 757.265; spot still below wall.
    snap = _snap(754.0, call_wall=760.0, recent_closes=[754.5, 754.0])
    decision = _bot()._wall_stop_signal(snap, pos)
    assert decision is not None and decision.reason == "wall_shift"


def test_put_wall_shift_down_cuts_the_bounce():
    pos = _pos(direction="bullish", wall_ref_price=750.0, wall_ref_side="put")
    snap = _snap(751.0, put_wall=745.0, recent_closes=[750.5, 751.0])
    decision = _bot()._wall_stop_signal(snap, pos)
    assert decision is not None and decision.reason == "wall_shift"


# ----------------------------------------------------------------------
# _wall_stop_signal — non-wall positions untouched (iron condor safety)
# ----------------------------------------------------------------------


def test_no_wall_ref_returns_none():
    pos = _pos(direction="bearish", wall_ref_price=None, wall_ref_side=None)
    assert _bot()._wall_stop_signal(_snap(999.0), pos) is None


def test_wall_ref_price_without_side_returns_none():
    """The iron condor sets wall_ref_price but no side -> must no-op here."""
    pos = _pos(direction="neutral", wall_ref_price=750.0, wall_ref_side=None)
    assert _bot()._wall_stop_signal(_snap(999.0, call_wall=800.0), pos) is None


# ----------------------------------------------------------------------
# Volatility responsiveness: a break in calm vol is absorbed in high vol
# ----------------------------------------------------------------------


def test_high_vol_absorbs_a_move_that_breaks_in_calm_vol():
    pos = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call")
    closes = [757.6, 758.0]  # ~0.4% above the wall
    calm = _snap(758.0, call_wall=755.0, vix=12.0, recent_closes=closes)
    stormy = _snap(758.0, call_wall=755.0, vix=90.0, recent_closes=closes)
    # Calm: 0.4% clears the ~0.3% buffer -> break. Stormy: buffer widens
    # past 0.4% -> the same move is treated as noise and held.
    assert _bot()._wall_stop_signal(calm, pos).reason == "wall_break"
    assert _bot()._wall_stop_signal(stormy, pos) is None


def test_large_wall_gets_more_room_than_small_wall():
    """Same move, same vol: a historically large wall holds where a small one breaks.

    At VIX 16 the small-wall break level is ~757.31 and the large-wall
    (strength 1.0, +50%) level is ~758.47; a confirmed move to 758.0 sits
    between them.
    """
    closes = [757.5, 758.0]
    snap = _snap(758.0, call_wall=755.0, vix=16.0, recent_closes=closes)
    small = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call", wall_strength=0.0)
    big = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call", wall_strength=1.0)
    assert _bot()._wall_stop_signal(snap, small).reason == "wall_break"
    assert _bot()._wall_stop_signal(snap, big) is None


# ----------------------------------------------------------------------
# End-to-end through exit_criteria: winner held, confirmed break cut
# ----------------------------------------------------------------------


def test_exit_criteria_holds_winning_call_fade():
    """Full exit path: a bearish fade moving toward target is NOT closed."""
    pos = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call")
    pos.target_price = 750.0  # spot 752 hasn't reached it yet
    snap = _snap(752.0, call_wall=755.0, recent_closes=[753.0, 752.5, 752.0])
    decision = _bot().exit_criteria(snap, pos)
    assert decision.should_close is False


def test_exit_criteria_cuts_confirmed_break():
    pos = _pos(direction="bearish", wall_ref_price=755.0, wall_ref_side="call")
    pos.target_price = 750.0
    snap = _snap(758.0, call_wall=755.0, recent_closes=[757.6, 758.0])
    decision = _bot().exit_criteria(snap, pos)
    assert decision.should_close is True
    assert decision.reason == "wall_break"
