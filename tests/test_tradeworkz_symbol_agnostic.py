"""Regression: universe='*' means "trade every ticker in the fleet."

The bot spec's ``universe`` used to be a bare ticker ("SPY"), so a
QQQ-only variant needed its own BotSpec row. Two consequences:

* The fleet couldn't add a new symbol without shipping N new BotSpecs.
* $1M got sliced across 14 sleeves the moment the two QQQ variants
  landed, silently shrinking every other bot's per-trade risk.

The wildcard universe fixes both. The engine expands ``'*'`` to the
fleet universe on every tick and iterates ``(bot, underlying)``; the
roster is 12 symbol-agnostic bots getting an equal $1M / 12 slice.
"""

from __future__ import annotations

from src.tradeworkz import config as tw_config
from src.tradeworkz.engine import _bot_underlyings
from src.tradeworkz.registry import DEFAULT_ROSTER, RETIRED_BOT_IDS


def test_wildcard_expands_to_fleet_universe():
    assert _bot_underlyings("*", ("SPY", "QQQ", "IWM")) == ["SPY", "QQQ", "IWM"]


def test_pinned_ticker_stays_pinned():
    """A bot with a bare ticker in ``universe`` doesn't get the fleet."""
    assert _bot_underlyings("SPY", ("SPY", "QQQ", "IWM")) == ["SPY"]


def test_csv_universe_gets_that_subset():
    assert _bot_underlyings("SPY,QQQ", ("SPY", "QQQ", "IWM")) == ["SPY", "QQQ"]


def test_lowercase_ticker_upper_cased():
    assert _bot_underlyings("spy,qqq", ("SPY", "QQQ")) == ["SPY", "QQQ"]


def test_empty_universe_falls_back_to_fleet():
    """Legacy DB rows with empty universe are treated as symbol-agnostic
    rather than silently disabled — safer default when the roster
    schema evolves."""
    assert _bot_underlyings("", ("SPY", "QQQ")) == ["SPY", "QQQ"]


def test_fleet_universe_env_parses_csv(monkeypatch):
    monkeypatch.setattr(tw_config, "UNIVERSE", "SPY, QQQ , IWM")
    assert tw_config.fleet_universes() == ("SPY", "QQQ", "IWM")


def test_fleet_universe_env_single_ticker(monkeypatch):
    """Legacy TRADEWORKZ_UNIVERSE=SPY installations don't break."""
    monkeypatch.setattr(tw_config, "UNIVERSE", "SPY")
    assert tw_config.fleet_universes() == ("SPY",)


def test_every_default_roster_bot_is_symbol_agnostic():
    """Regression: someone edits registry.py and pins a bot back to SPY
    without noticing — this catches that on CI. The intent of the v2
    fleet is that every bot trades every ticker; pinning is only for
    the retired list."""
    for spec in DEFAULT_ROSTER:
        assert spec.universe == "*", (
            f"{spec.id} is pinned to {spec.universe!r} — either widen it "
            "to '*' or add a comment explaining the pin"
        )


def test_retired_bots_are_not_in_active_roster():
    """The retired ids must be absent from DEFAULT_ROSTER — otherwise
    provision_defaults would re-insert them the moment it retires them."""
    active_ids = {spec.id for spec in DEFAULT_ROSTER}
    for retired_id in RETIRED_BOT_IDS:
        assert retired_id not in active_ids


def test_fleet_capital_sliced_correctly_for_current_roster():
    """$1M split across the current roster produces a sensible sleeve
    size — the number changes with the roster count but the total pot
    must equal FLEET_CAPITAL to within rounding."""
    roster = list(DEFAULT_ROSTER)
    assert len(roster) >= 1
    slice_amount = round(tw_config.FLEET_CAPITAL / len(roster), 2)
    # Guard against a stale roster silently shrinking the sleeve to
    # single-trade sizes.
    assert slice_amount >= 50_000
    total = slice_amount * len(roster)
    assert abs(total - tw_config.FLEET_CAPITAL) <= 1.0
