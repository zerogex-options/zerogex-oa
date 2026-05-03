"""Tests for PR-15a Action Card consumption in portfolio_engine.

Covers ``_synthesize_score_from_action_card`` (the projection helper) plus
the public ``compute_target_with_action_card`` flow — verifying that:

* STAND_DOWN / missing / low-confidence Cards fall through to the
  existing advanced-signals path bit-for-bit (no production behavior
  change without the flag).
* Trade Cards above the confidence floor produce a synthesized
  ScoreSnapshot whose direction, normalized_score, and aggregation
  markers reflect the Card.
* The aggregation marker uses the new ``card_trigger`` key and does not
  collide with the legacy ``advanced_trigger`` / ``confluence_trigger``
  bypass keys.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

from src.signals.portfolio_engine import PortfolioEngine
from src.signals.scoring_engine import ScoreSnapshot

# ----------------------------------------------------------------------
# Builders
# ----------------------------------------------------------------------


def _base_score(
    *,
    composite: float = 50.0,
    direction: str = "chop_range",
    aggregation: dict | None = None,
) -> ScoreSnapshot:
    return ScoreSnapshot(
        timestamp=datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc),
        underlying="SPY",
        composite_score=composite,
        normalized_score=composite / 100.0,
        direction=direction,
        components={"net_gex_sign": {"max_points": 16, "score": -1.0}},
        aggregation=dict(aggregation or {}),
    )


def _trade_card_dict(
    *,
    pattern: str = "call_wall_fade",
    direction: str = "bearish",
    confidence: float = 0.65,
    action: str = "SELL_CALL_SPREAD",
    tier: str = "0DTE",
) -> dict[str, Any]:
    return {
        "underlying": "SPY",
        "timestamp": datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc).isoformat(),
        "action": action,
        "pattern": pattern,
        "tier": tier,
        "direction": direction,
        "confidence": confidence,
        "max_hold_minutes": 90,
        "legs": [],
        "entry": {"ref_price": 678.0, "trigger": "at_touch"},
        "target": {"ref_price": 675.0, "kind": "level"},
        "stop": {"ref_price": 680.0, "kind": "level"},
    }


# ----------------------------------------------------------------------
# _synthesize_score_from_action_card
# ----------------------------------------------------------------------


def test_synthesize_projects_direction_and_confidence():
    base = _base_score()
    card = _trade_card_dict(direction="bearish", confidence=0.72, pattern="call_wall_fade")
    out = PortfolioEngine._synthesize_score_from_action_card(base, card)

    assert out.direction == "bearish"
    assert out.normalized_score == 0.72
    # Composite is preserved — the card projects intent, not regime.
    assert out.composite_score == base.composite_score
    # Aggregation keys use the new ``card_*`` namespace.
    agg = out.aggregation or {}
    assert agg["card_trigger"] == "call_wall_fade"
    assert agg["card_action"] == "SELL_CALL_SPREAD"
    assert agg["card_tier"] == "0DTE"
    assert agg["card_confidence"] == 0.72
    assert agg["card_direction"] == "bearish"
    # Legacy bypass keys must NOT be set by this path.
    assert "advanced_trigger" not in agg
    assert "confluence_trigger" not in agg


def test_synthesize_clamps_confidence_to_unit_interval():
    base = _base_score()
    high = _trade_card_dict(confidence=1.5)
    low = _trade_card_dict(confidence=-0.4)
    assert PortfolioEngine._synthesize_score_from_action_card(base, high).normalized_score == 1.0
    assert PortfolioEngine._synthesize_score_from_action_card(base, low).normalized_score == 0.0


def test_synthesize_returns_base_for_stand_down():
    base = _base_score()
    out = PortfolioEngine._synthesize_score_from_action_card(
        base, {"action": "STAND_DOWN", "pattern": "stand_down"}
    )
    assert out is base  # untouched reference; no projection


def test_synthesize_returns_base_for_non_dict():
    base = _base_score()
    assert PortfolioEngine._synthesize_score_from_action_card(base, None) is base
    assert PortfolioEngine._synthesize_score_from_action_card(base, "not a dict") is base


def test_synthesize_normalizes_unexpected_direction_to_neutral():
    base = _base_score()
    card = _trade_card_dict(direction="context_dependent")
    out = PortfolioEngine._synthesize_score_from_action_card(base, card)
    assert out.direction == "neutral"


def test_synthesize_records_pattern_in_components():
    base = _base_score()
    card = _trade_card_dict(pattern="put_wall_bounce")
    out = PortfolioEngine._synthesize_score_from_action_card(base, card)
    assert "playbook:put_wall_bounce" in (out.components or {})


# ----------------------------------------------------------------------
# compute_target_with_action_card — flag-off / fall-through behavior
# ----------------------------------------------------------------------


def _engine_with_stub_pipeline():
    """PortfolioEngine instance with both compute paths replaced by stubs.

    Lets us assert which path was invoked without exercising the full
    sizing pipeline (which needs DB + market data).
    """
    engine = PortfolioEngine.__new__(PortfolioEngine)
    engine.compute_target_with_advanced_signals = MagicMock(  # type: ignore[method-assign]
        return_value="legacy-path-result"
    )
    return engine


def test_no_card_uses_legacy_path():
    engine = _engine_with_stub_pipeline()
    score = _base_score()
    result = engine.compute_target_with_action_card(
        score, market_ctx={}, action_card=None, advanced_results=[], basic_results=[]
    )
    assert result == "legacy-path-result"
    engine.compute_target_with_advanced_signals.assert_called_once()
    args, kwargs = engine.compute_target_with_advanced_signals.call_args
    assert args[0] is score  # base score, unmodified


def test_stand_down_card_uses_legacy_path():
    engine = _engine_with_stub_pipeline()
    score = _base_score()
    engine.compute_target_with_action_card(
        score,
        market_ctx={},
        action_card={"action": "STAND_DOWN", "pattern": "stand_down"},
        advanced_results=[],
        basic_results=[],
    )
    args, kwargs = engine.compute_target_with_advanced_signals.call_args
    assert args[0] is score  # untouched


def test_low_confidence_card_uses_legacy_path():
    engine = _engine_with_stub_pipeline()
    score = _base_score()
    engine.compute_target_with_action_card(
        score,
        market_ctx={},
        action_card=_trade_card_dict(confidence=0.30),
        advanced_results=[],
        basic_results=[],
        confidence_floor=0.50,
    )
    args, kwargs = engine.compute_target_with_advanced_signals.call_args
    assert args[0] is score  # untouched


# ----------------------------------------------------------------------
# compute_target_with_action_card — flag-on / projection path
# ----------------------------------------------------------------------


def test_high_confidence_card_projects_score_before_pipeline():
    engine = _engine_with_stub_pipeline()
    score = _base_score()
    card = _trade_card_dict(confidence=0.65, direction="bearish", pattern="call_wall_fade")
    engine.compute_target_with_action_card(
        score,
        market_ctx={"vix_level": 16.0},
        action_card=card,
        advanced_results=[],
        basic_results=[],
        confidence_floor=0.50,
    )
    args, kwargs = engine.compute_target_with_advanced_signals.call_args
    projected = args[0]
    assert projected is not score  # different object — projection happened
    assert projected.direction == "bearish"
    assert projected.normalized_score == 0.65
    assert (projected.aggregation or {}).get("card_trigger") == "call_wall_fade"


def test_card_projection_preserves_market_ctx_and_signal_lists():
    """Projection only touches the score; advanced/basic lists pass through."""
    engine = _engine_with_stub_pipeline()
    score = _base_score()
    advanced_marker = ["adv1"]
    basic_marker = ["basic1"]
    card = _trade_card_dict(confidence=0.80)
    engine.compute_target_with_action_card(
        score,
        market_ctx={"id": "marker"},
        action_card=card,
        advanced_results=advanced_marker,
        basic_results=basic_marker,
    )
    args, kwargs = engine.compute_target_with_advanced_signals.call_args
    assert args[1] == {"id": "marker"}
    assert kwargs["advanced_results"] is advanced_marker
    assert kwargs["basic_results"] is basic_marker
