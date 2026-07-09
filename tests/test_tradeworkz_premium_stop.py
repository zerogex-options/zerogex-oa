"""Regression: exit_criteria closes on premium loss as damage control.

Spot-based structural stops (a break of the wall, a re-entry through
the flip, etc.) invalidate the trade thesis, but they leave a wide
premium runway on 0DTE ATM debits — a 0.3-1% adverse spot move eats
60-80% of premium before spot reaches the structural level. The
audit showed three separate trades exiting at -60% / -66% / -81%
premium loss with reason='stop', which is exactly the mode this
damage-control layer prevents.

Fired regardless of ``min_hold_until`` — damage-control trumps
patience — but only for long-debit positions (``entry_price > 0``);
credit structures have different loss geometry and are handled by
their own exit rules.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from src.tradeworkz import config as tw_config
from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, OpenPosition


def _snap(spot: float = 746.0) -> MarketSnapshot:
    return MarketSnapshot(
        underlying="SPY",
        timestamp=datetime(2026, 7, 9, 14, 30, tzinfo=timezone.utc),
        spot=spot,
    )


def _bot(params: Optional[dict] = None) -> BaseBot:
    spec = BotSpec(
        id="unit_test_bot",
        display_name="Unit",
        strategy_class="BaseBot",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params=params or {},
    )
    return BaseBot(spec)


def _pos(
    entry_price: float = 1.00,
    current_price: float = 1.00,
    direction: str = "bearish",
    min_hold_seconds_ago: float = 120.0,
    target_price: Optional[float] = 740.0,
    stop_price: Optional[float] = 754.0,
) -> OpenPosition:
    """min_hold_seconds_ago > 0 means min_hold_until is already elapsed;
    negative values leave us still inside the min-hold window."""
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
        entry_price=entry_price,
        current_price=current_price,
        quantity_open=10,
        unrealized_pnl=(current_price - entry_price) * 10 * 100,
        stop_price=stop_price,
        target_price=target_price,
        time_stop_at=now + timedelta(minutes=30),
        min_hold_until=now - timedelta(seconds=min_hold_seconds_ago),
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.7,
        components_at_entry={},
    )


@pytest.fixture(autouse=True)
def _pin_default_threshold(monkeypatch):
    """Every test starts with the code default so an operator's .env can't
    make the suite fail on the server while CI stays green."""
    monkeypatch.setattr(tw_config, "MAX_PREMIUM_LOSS_PCT", 0.40)


# ── Premium stop fires on ≥ threshold loss ────────────────────────────


def test_premium_stop_fires_at_exactly_threshold():
    """Entry 1.00, current 0.60 → exactly 40% loss → close."""
    decision = _bot().exit_criteria(
        _snap(), _pos(entry_price=1.00, current_price=0.60),
    )
    assert decision.should_close is True
    assert decision.reason == "premium_stop"


def test_premium_stop_fires_below_threshold_too():
    decision = _bot().exit_criteria(
        _snap(), _pos(entry_price=1.00, current_price=0.30),
    )
    assert decision.should_close is True
    assert decision.reason == "premium_stop"


def test_premium_stop_holds_above_threshold():
    """Entry 1.00, current 0.65 → only 35% loss → hold (unless spot hits
    structural stop/target, which it doesn't in this fixture)."""
    decision = _bot().exit_criteria(
        _snap(), _pos(entry_price=1.00, current_price=0.65),
    )
    assert decision.should_close is False


# ── Damage-control trumps patience ────────────────────────────────────


def test_premium_stop_bypasses_min_hold():
    """The premium loss check must fire even inside min_hold — a wick
    that eats 50% of premium in the first 30 seconds is EXACTLY when
    damage control matters most."""
    decision = _bot().exit_criteria(
        _snap(),
        _pos(
            entry_price=1.00, current_price=0.50,
            min_hold_seconds_ago=-30.0,  # 30s in the FUTURE, still in min-hold
        ),
    )
    assert decision.should_close is True
    assert decision.reason == "premium_stop"


# ── Per-bot override ──────────────────────────────────────────────────


def test_per_bot_param_tightens_default():
    """A bot's params['max_premium_loss_pct'] overrides the fleet default.
    Straddle bots can raise it; wall bouncers can tighten it."""
    decision = _bot(params={"max_premium_loss_pct": 0.20}).exit_criteria(
        _snap(), _pos(entry_price=1.00, current_price=0.79),
    )
    # 21% loss > 20% override → close
    assert decision.should_close is True
    assert decision.reason == "premium_stop"


def test_per_bot_param_loosens_default():
    decision = _bot(params={"max_premium_loss_pct": 0.60}).exit_criteria(
        _snap(), _pos(entry_price=1.00, current_price=0.55),
    )
    # 45% loss < 60% override → hold
    assert decision.should_close is False


def test_zero_override_disables_check():
    """A per-bot 0 turns damage-control off for that bot specifically."""
    decision = _bot(params={"max_premium_loss_pct": 0.0}).exit_criteria(
        _snap(), _pos(entry_price=1.00, current_price=0.10),
    )
    assert decision.should_close is False


def test_zero_env_disables_check(monkeypatch):
    """TRADEWORKZ_MAX_PREMIUM_LOSS_PCT=0 disables the check fleet-wide."""
    monkeypatch.setattr(tw_config, "MAX_PREMIUM_LOSS_PCT", 0.0)
    decision = _bot().exit_criteria(
        _snap(), _pos(entry_price=1.00, current_price=0.10),
    )
    assert decision.should_close is False


# ── Edge cases ────────────────────────────────────────────────────────


def test_credit_structure_never_hits_premium_stop():
    """entry_price <= 0 means a credit structure (short iron condor).
    Loss geometry differs — the check is intentionally skipped."""
    decision = _bot().exit_criteria(
        _snap(), _pos(entry_price=-0.50, current_price=-2.00),
    )
    # Would be a 300% "loss" if we applied the same formula naively;
    # the guard clause protects credit trades from a false trigger.
    assert decision.should_close is False


def test_bullish_trade_also_gets_damage_control():
    """The check is direction-agnostic — a bullish long call bleeds
    just as fast when spot ticks down."""
    decision = _bot().exit_criteria(
        _snap(),
        _pos(
            entry_price=1.00, current_price=0.50,
            direction="bullish",
            target_price=750.0, stop_price=743.0,
        ),
    )
    assert decision.should_close is True
    assert decision.reason == "premium_stop"


def test_current_price_none_skips_check():
    """A position never marked (extremely early state) shouldn't crash
    the check — skip and let time_stop / structural exits handle it."""
    pos = _pos()
    pos.current_price = None  # type: ignore[assignment]
    decision = _bot().exit_criteria(_snap(), pos)
    assert decision.should_close is False
