"""The /backtesting form publishes the strategy catalog, not scraped docstrings.

Replaces ``test_backtesting_meta_descriptions.py``, which tested the old
docstring-scraping path (``_extract_description`` / ``_docstring_for_pattern``).
Descriptions now come from the catalog's authored ``thesis``, so the concern
those tests covered — "is the customer-facing text the readable paragraph
rather than a dev header?" — is replaced by "does every catalog entry carry
real prose, and does the form publish it?".
"""

from __future__ import annotations

from src.backtesting.meta import _strategy_catalog
from src.strategies import all_strategies


def _by_id() -> dict:
    return {s["id"]: s for s in _strategy_catalog()}


def test_publishes_every_catalog_strategy():
    published = _by_id()
    assert set(published) == {e.id for e in all_strategies()}


def test_every_strategy_has_customer_readable_prose():
    """No dev headers, no ids, no backticks, no truncation artifacts."""
    for s in _strategy_catalog():
        thesis = s["thesis"]
        assert len(thesis) >= 40, f"{s['id']}: thesis too short to be useful"
        assert "`" not in thesis, f"{s['id']}: raw backticks would render literally"
        assert not thesis.startswith("Pattern "), f"{s['id']}: leaks a dev header"
        assert s["id"] not in thesis, f"{s['id']}: leaks its own slug into prose"
        assert s["name"], f"{s['id']}: missing display name"
        assert s["tagline"], f"{s['id']}: missing tagline"


def test_description_is_an_alias_of_thesis():
    """Older clients and saved configs read `description`."""
    for s in _strategy_catalog():
        assert s["description"] == s["thesis"]


def test_every_strategy_declares_how_it_is_backtested():
    for s in _strategy_catalog():
        if s["backtestable"]:
            assert s["backtest_via"] in ("pattern", "bot_replay")
            assert s["not_backtestable_reason"] is None
        else:
            assert s["backtest_via"] is None
            assert s["not_backtestable_reason"], f"{s['id']}: unexplained gap"


def test_pattern_binding_wins_over_bot_for_measurement():
    """Where both engines exist, the pattern's real emitted cards are used."""
    for s in _strategy_catalog():
        if s["pattern_id"] and s["bot_id"]:
            assert s["backtest_via"] == "pattern", s["id"]


def test_bot_only_strategies_are_backtestable_via_replay():
    """The gap this consolidation closed: bot-only strategies used to be
    invisible to Backtesting entirely."""
    bot_only = [s for s in _strategy_catalog() if s["bot_id"] and not s["pattern_id"]]
    assert bot_only, "expected bot-only strategies in the catalog"
    for s in bot_only:
        assert s["backtest_via"] == "bot_replay", s["id"]


def test_retirement_block_is_populated_for_every_strategy():
    for s in _strategy_catalog():
        r = s["retirement"]
        assert r["required_history_days"] == 5 * 365
        assert 0.0 <= r["history_progress"] <= 1.0
        if not r["eligible"]:
            assert r["blockers"], f"{s['id']}: ineligible with no reason given"
