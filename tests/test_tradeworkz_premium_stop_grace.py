"""The premium stop must not fire on the entry-tick bid/ask + slippage gap.

A freshly opened position is first marked at the close side (bid - slippage),
so a wide-market 0DTE structure can show a big phantom loss on the very first
mark (a $0.62 spread marking ~$0.42, ~-32%) that is not a real adverse move.
PREMIUM_STOP_GRACE_SECONDS suppresses the premium stop for a short window after
open so it fires on moves, not the spread.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.tradeworkz import config as tw_config
from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, OpenPosition


@pytest.fixture(autouse=True)
def _fixed_stop(monkeypatch):
    monkeypatch.setattr(tw_config, "MAX_PREMIUM_LOSS_PCT", 0.40)
    monkeypatch.setattr(tw_config, "PREMIUM_STOP_GRACE_SECONDS", 45)


def _bot() -> BaseBot:
    spec = BotSpec(
        id="unit_test_bot", display_name="U", strategy_class="BaseBot", tier="0DTE",
        direction_mode="context", universe="SPY", tagline="", description="", params={},
    )
    return BaseBot(spec)


def _snap(spot: float = 746.0) -> MarketSnapshot:
    return MarketSnapshot(
        underlying="SPY", timestamp=datetime(2026, 7, 9, 14, 30, tzinfo=timezone.utc), spot=spot,
    )


def _pos(opened_seconds_ago: float, current_price: float = 0.50) -> OpenPosition:
    # entry 1.00, current 0.50 => -50% (past the 40% stop) unless graced.
    now = datetime.now(timezone.utc)
    return OpenPosition(
        id=1, bot_id="unit_test_bot", underlying="SPY",
        opened_at=now - timedelta(seconds=opened_seconds_ago),
        direction="bearish", strategy_type="BUY_PUT_DEBIT", legs=[],
        entry_price=1.00, current_price=current_price, quantity_open=10,
        unrealized_pnl=(current_price - 1.00) * 10 * 100,
        stop_price=754.0, target_price=740.0,
        time_stop_at=now + timedelta(minutes=30),
        min_hold_until=now - timedelta(seconds=120),  # min-hold already elapsed
        wall_ref_price=None, wall_ref_side=None, entry_conviction=0.7, components_at_entry={},
    )


def test_premium_stop_suppressed_within_grace():
    d = _bot().exit_criteria(_snap(), _pos(opened_seconds_ago=10))  # 10s old, -50%
    assert d.should_close is False  # graced — the entry-tick gap does not close it


def test_premium_stop_fires_after_grace():
    d = _bot().exit_criteria(_snap(), _pos(opened_seconds_ago=120))  # 2 min old, -50%
    assert d.should_close is True
    assert d.reason == "premium_stop"


def test_grace_zero_disables_the_window(monkeypatch):
    monkeypatch.setattr(tw_config, "PREMIUM_STOP_GRACE_SECONDS", 0)
    d = _bot().exit_criteria(_snap(), _pos(opened_seconds_ago=1))  # 1s old, -50%
    assert d.should_close is True
    assert d.reason == "premium_stop"


def test_min_entry_premium_defaults_to_disabled():
    assert tw_config.MIN_ENTRY_PREMIUM == pytest.approx(0.0)
