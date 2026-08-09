"""Regression: universe='*' means "trade every ticker in the fleet."

The bot spec's ``universe`` used to be a bare ticker ("SPY"), so a
QQQ-only variant needed its own BotSpec row. Two consequences:

* The fleet couldn't add a new symbol without shipping N new BotSpecs.
* $1M got sliced across 14 sleeves the moment the two QQQ variants
  landed, silently shrinking every other bot's per-trade risk.

The wildcard universe fixes both. The engine expands ``'*'`` to the
fleet universe on every tick and iterates ``(bot, underlying)``; every
shipped bot is symbol-agnostic. (The active roster is currently empty —
the fleet was shelved on 2026-08-09 after the backtest found no edge — so
these invariants are checked against the preserved ``SHELVED_SPECS``
catalog.)
"""

from __future__ import annotations

from src.tradeworkz import config as tw_config
from src.tradeworkz.engine import _bot_underlyings
from src.tradeworkz.registry import DEFAULT_ROSTER, RETIRED_BOT_IDS, SHELVED_SPECS


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


def test_every_shipped_bot_is_symbol_agnostic():
    """Regression: someone edits registry.py and pins a bot back to SPY
    without noticing — this catches that on CI. The intent of the v2
    fleet is that every bot trades every ticker; pinning is only for the
    retired list. Checked against SHELVED_SPECS (the full shipped catalog)
    since the active roster is currently empty."""
    for spec in SHELVED_SPECS:
        assert spec.universe == "*", (
            f"{spec.id} is pinned to {spec.universe!r} — either widen it "
            "to '*' or add a comment explaining the pin"
        )


def test_retired_bots_are_not_in_active_roster():
    """The retired ids must be absent from DEFAULT_ROSTER — otherwise
    provision_defaults would re-insert them the moment it retires them.
    Holds trivially while the fleet is fully shelved (empty roster)."""
    active_ids = {spec.id for spec in DEFAULT_ROSTER}
    for retired_id in RETIRED_BOT_IDS:
        assert retired_id not in active_ids


def test_fleet_is_fully_shelved_pending_backtest_edge():
    """The whole fleet was retired on 2026-08-09 after the backtest screen
    found no bot has edge. Locks that state in: the active roster is empty,
    the shipped catalog is preserved, and every shipped bot is retired in
    the DB so none can open. Reviving one means editing BOTH tuples — this
    fails if someone re-activates a bot without also un-retiring it."""
    assert DEFAULT_ROSTER == ()
    assert len(SHELVED_SPECS) >= 12
    shelved_ids = {spec.id for spec in SHELVED_SPECS}
    for spec in SHELVED_SPECS:
        assert spec.id in RETIRED_BOT_IDS, (
            f"{spec.id} is shipped but not retired — while the fleet is "
            "shelved every shipped bot must be in RETIRED_BOT_IDS"
        )
    # Any active bot must NOT also be retired (the general invariant).
    for spec in DEFAULT_ROSTER:
        assert spec.id not in RETIRED_BOT_IDS
    # The legacy symbol-specific ids stay retired too.
    assert "qqq_gamma_flip_breaker" in RETIRED_BOT_IDS
    assert shelved_ids <= set(RETIRED_BOT_IDS)


def test_fleet_capital_slice_is_sane_on_revival():
    """When a spec is moved back into the active roster it should get a
    sensible sleeve, not a single-trade sliver. Simulate a one-bot revival
    off the shelved catalog so the slice math stays covered."""
    revived = [SHELVED_SPECS[0]]
    slice_amount = round(tw_config.FLEET_CAPITAL / len(revived), 2)
    assert slice_amount >= 50_000
