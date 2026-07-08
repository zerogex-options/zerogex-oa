"""Regression: BaseBot.exit_criteria compares SPOT vs spot-level target/stop.

Every bot in the fleet sets ``position.target_price`` and
``position.stop_price`` in terms of the UNDERLYING SPOT price (e.g., "sell
the bearish put when SPY drops to 740"). The old code compared those
spot-level thresholds against ``position.current_price`` — which is the
mark-to-market OPTION PREMIUM (~$0.68) — so:

  * bearish long put with target=740: 0.68 <= 740 → True → close as "target"
    on the first tick after min_hold expired, regardless of where SPY
    actually was.
  * bullish long call with stop=743: 0.94 <= 743 → True → close as "stop"
    on the first tick after min_hold expired.

That's why the audit showed "target = loss" — the exit was firing on a
comparison that had nothing to do with the trade's thesis. Fix compares
against ``snap.spot``. If a future bot wants to add option-premium-level
exits, do it in an override / additional field, not by reusing
target_price for two different unit systems.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, ExitDecision, OpenPosition


def _snap(spot: float, minutes_since_open: float = 60.0) -> MarketSnapshot:
    ts = datetime(2026, 7, 8, 14, 30, tzinfo=timezone.utc)  # ~10:30 ET
    return MarketSnapshot(underlying="SPY", timestamp=ts, spot=spot)


def _bot() -> BaseBot:
    spec = BotSpec(
        id="unit_test_bot",
        display_name="Unit Test Bot",
        strategy_class="BaseBot",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params={},
    )
    return BaseBot(spec)


def _pos(
    direction: str,
    target_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    min_hold_seconds_ago: float = 120.0,
    time_stop_seconds_ahead: float = 3600.0,
) -> OpenPosition:
    """min_hold_seconds_ago > 0 means min_hold_until has already passed.
    Negative values put min_hold_until in the future (still in min-hold)."""
    now = datetime.now(timezone.utc)
    return OpenPosition(
        id=1,
        bot_id="unit_test_bot",
        underlying="SPY",
        opened_at=now - timedelta(minutes=5),
        direction=direction,
        strategy_type=(
            "BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT"
        ),
        legs=[],
        entry_price=1.00,
        current_price=1.00,   # option premium (NOT the spot the exit compares against)
        quantity_open=10,
        unrealized_pnl=0.0,
        stop_price=stop_price,
        target_price=target_price,
        time_stop_at=now + timedelta(seconds=time_stop_seconds_ahead),
        min_hold_until=now - timedelta(seconds=min_hold_seconds_ago),
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.7,
        components_at_entry={},
    )


# ── Target hits: SPOT reaches target → close as "target" ──────────────


def test_bullish_target_fires_when_spot_reaches_call_target():
    pos = _pos("bullish", target_price=750.0, stop_price=743.0)
    decision = _bot().exit_criteria(_snap(spot=750.10), pos)
    assert decision.should_close is True
    assert decision.reason == "target"


def test_bearish_target_fires_when_spot_drops_to_put_target():
    pos = _pos("bearish", target_price=740.0, stop_price=744.0)
    decision = _bot().exit_criteria(_snap(spot=739.90), pos)
    assert decision.should_close is True
    assert decision.reason == "target"


# ── Stop hits: SPOT reaches stop → close as "stop" ────────────────────


def test_bullish_stop_fires_when_spot_drops_to_stop():
    pos = _pos("bullish", target_price=750.0, stop_price=743.0)
    decision = _bot().exit_criteria(_snap(spot=742.90), pos)
    assert decision.should_close is True
    assert decision.reason == "stop"


def test_bearish_stop_fires_when_spot_rises_to_stop():
    pos = _pos("bearish", target_price=740.0, stop_price=744.0)
    decision = _bot().exit_criteria(_snap(spot=744.10), pos)
    assert decision.should_close is True
    assert decision.reason == "stop"


# ── The regression: spot BETWEEN target and stop must NOT close ───────
# This is what the old code got wrong — it closed immediately even
# though spot was nowhere near the bot's target or stop.


def test_bullish_spot_between_stop_and_target_holds():
    pos = _pos("bullish", target_price=750.0, stop_price=743.0)
    decision = _bot().exit_criteria(_snap(spot=746.16), pos)
    assert decision.should_close is False, (
        "Bullish trade at SPY=746, target=750, stop=743 must HOLD — "
        "old buggy code closed as 'stop' because 0.94 <= 743 was trivially true."
    )


def test_bearish_spot_between_target_and_stop_holds():
    pos = _pos("bearish", target_price=740.0, stop_price=744.0)
    decision = _bot().exit_criteria(_snap(spot=741.53), pos)
    assert decision.should_close is False, (
        "Bearish trade at SPY=741.53, target=740, stop=744 must HOLD — "
        "old buggy code closed as 'target' because 0.68 <= 740 was trivially true."
    )


# ── min_hold_until gates price-level exits ────────────────────────────


def test_min_hold_prevents_target_close():
    """Even if spot is past target, we hold until min_hold elapses."""
    pos = _pos(
        "bullish", target_price=750.0, stop_price=743.0,
        min_hold_seconds_ago=-30.0,  # min_hold is 30s in the future
    )
    decision = _bot().exit_criteria(_snap(spot=751.0), pos)
    assert decision.should_close is False


def test_min_hold_prevents_stop_close():
    pos = _pos(
        "bearish", target_price=740.0, stop_price=744.0,
        min_hold_seconds_ago=-30.0,
    )
    decision = _bot().exit_criteria(_snap(spot=745.0), pos)
    assert decision.should_close is False


# ── time_stop_at forces a close when neither price triggers ───────────


def test_time_stop_closes_after_expiry():
    pos = _pos(
        "bullish", target_price=750.0, stop_price=743.0,
        time_stop_seconds_ahead=-10.0,  # 10s ago
    )
    decision = _bot().exit_criteria(_snap(spot=746.0), pos)
    assert decision.should_close is True
    assert decision.reason == "time_stop"
