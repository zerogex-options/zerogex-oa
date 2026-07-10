"""Tests for SpecStrategyBot — the backtest→live bridge bot.

A saved custom-strategy rule, evaluated against the live snapshot with the same
condition logic the backtest uses. Verifies it fires when the rule holds, stays
flat when it doesn't, and refuses strategies it can't fully evaluate live.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.tradeworkz.bots.spec_strategy import (
    SpecStrategyBot,
    _snap_to_bar,
    is_live_deployable,
)
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec

TS = datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc)


def _snap(**over) -> MarketSnapshot:
    base = dict(underlying="SPY", timestamp=TS, spot=500.0)
    base.update(over)
    return MarketSnapshot(**base)


def _bot(strategy: dict, **params) -> SpecStrategyBot:
    params = {"strategy": strategy, **params}
    spec = BotSpec(
        id="user-bot-1",
        display_name="My Strategy",
        strategy_class="spec_strategy",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params=params,
    )
    return SpecStrategyBot(spec)


_BULLISH_NEG_GEX = {
    "direction": "bullish",
    "structure": "single",
    "conditions": [{"field": "net_gex", "op": "<", "value": 0}],
    "entry": {"dte": 0},
    "target_offset_pct": 0.003,
    "stop_offset_pct": 0.003,
}


# ----------------------------------------------------------------------
# is_live_deployable
# ----------------------------------------------------------------------


def test_deployable_directional_with_mappable_fields():
    assert is_live_deployable(_BULLISH_NEG_GEX) is True


def test_not_deployable_neutral_structure():
    strat = {**_BULLISH_NEG_GEX, "structure": "straddle", "direction": "neutral"}
    assert is_live_deployable(strat) is False


def test_not_deployable_unmappable_field():
    # convexity_risk is a valid backtest field but the live snapshot can't supply it.
    strat = {**_BULLISH_NEG_GEX, "conditions": [{"field": "convexity_risk", "op": ">", "value": 1}]}
    assert is_live_deployable(strat) is False


# ----------------------------------------------------------------------
# snapshot → bar mapping
# ----------------------------------------------------------------------


def test_snap_to_bar_derives_signs_and_distances():
    bar = _snap_to_bar(_snap(net_gex=-1e9, gamma_flip=498.0, call_wall=505.0, put_wall=495.0))
    assert bar["price"] == 500.0
    assert bar["net_gex_sign"] == "negative"
    assert round(bar["flip_distance_pct"], 4) == round(2.0 / 500.0 * 100, 4)
    assert round(bar["dist_to_call_wall_pct"], 4) == round(5.0 / 500.0 * 100, 4)
    assert round(bar["dist_to_put_wall_pct"], 4) == round(5.0 / 500.0 * 100, 4)


# ----------------------------------------------------------------------
# open_criteria
# ----------------------------------------------------------------------


def test_fires_when_conditions_hold():
    bot = _bot(_BULLISH_NEG_GEX)
    sig = bot.open_criteria(_snap(net_gex=-1e9))  # negative ⇒ net_gex < 0 holds
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.strategy_type == "BUY_CALL_DEBIT"
    assert len(sig.legs) == 1 and sig.legs[0].option_type == "call"
    assert round(sig.target_price, 2) == round(500.0 * 1.003, 2)
    assert round(sig.stop_price, 2) == round(500.0 * 0.997, 2)


def test_stays_flat_when_a_condition_fails():
    bot = _bot(_BULLISH_NEG_GEX)
    assert bot.open_criteria(_snap(net_gex=1e9)) is None  # positive ⇒ net_gex < 0 fails


def test_stays_flat_when_field_absent_live():
    # net_gex missing on the snapshot ⇒ condition fails closed ⇒ no trade.
    bot = _bot(_BULLISH_NEG_GEX)
    assert bot.open_criteria(_snap(net_gex=None)) is None


def test_vertical_structure_builds_two_legs():
    strat = {**_BULLISH_NEG_GEX, "structure": "vertical", "width": 5}
    sig = _bot(strat).open_criteria(_snap(net_gex=-1e9))
    assert sig is not None
    assert sig.strategy_type == "BUY_CALL_SPREAD"
    assert len(sig.legs) == 2
    assert sig.legs[0].side == "long" and sig.legs[1].side == "short"
    assert sig.legs[1].strike == round(500.0) + 5


def test_bearish_uses_puts():
    strat = {**_BULLISH_NEG_GEX, "direction": "bearish"}
    sig = _bot(strat).open_criteria(_snap(net_gex=-1e9))
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.legs[0].option_type == "put"
    # Bearish target is below spot, stop above.
    assert sig.target_price < 500.0 < sig.stop_price
