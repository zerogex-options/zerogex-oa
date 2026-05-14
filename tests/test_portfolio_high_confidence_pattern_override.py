"""Tests for the high-confidence Playbook pattern size override.

When a card-driven entry (``aggregation['card_trigger']``) arrives with
``card_confidence >= SIGNALS_HIGH_CONFIDENCE_PATTERN_THRESHOLD``, the
regime size cap is lifted to
``SIGNALS_HIGH_CONFIDENCE_PATTERN_SIZE``.

This stops the chop_range 0.4x scalp multiplier from trimming
structural specialty setups like ``gamma_flip_bounce`` /
``put_wall_bounce`` in their preferred regimes.
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from src.signals.portfolio_engine import PortfolioEngine
from src.signals.position_optimizer_engine import SizingProfile, SpreadCandidate
from src.signals.scoring_engine import ScoreSnapshot


NOW = datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc)


def _make_engine() -> PortfolioEngine:
    with patch("src.signals.portfolio_engine.get_canonical_symbol", return_value="SPY"):
        return PortfolioEngine("SPY")


def _candidate(contracts: int = 10) -> SpreadCandidate:
    return SpreadCandidate(
        rank=1,
        strategy_type="bull_call_debit",
        expiry=date(2026, 4, 17),
        dte=1,
        strikes="Long 500C / Short 505C",
        option_type="C",
        entry_debit=250.0,
        entry_credit=0.0,
        width=5.0,
        max_profit=250.0,
        max_loss=250.0,
        risk_reward_ratio=1.0,
        probability_of_profit=0.55,
        expected_value=20.0,
        sharpe_like_ratio=0.08,
        liquidity_score=0.8,
        net_delta=20.0,
        net_gamma=1.0,
        net_theta=-3.0,
        premium_efficiency=1.0,
        market_structure_fit=0.8,
        greek_alignment_score=0.8,
        edge_score=0.7,
        kelly_fraction=0.10,
        sizing_profiles=[
            SizingProfile(
                profile="optimal",
                contracts=contracts,
                max_risk_dollars=2500.0,
                expected_value_dollars=200.0,
                constrained_by="kelly",
            )
        ],
    )


def _chop_score(*, aggregation: dict | None = None) -> ScoreSnapshot:
    """Chop-regime score with 0.70 normalized strength.

    With a card_trigger in aggregation, _directional_conviction returns the
    normalized_score directly, so conviction = 0.70 — clears the entry
    threshold (0.55) and the chop directional floor (0.30) and the chop
    high-conviction threshold (0.55).
    """
    return ScoreSnapshot(
        timestamp=NOW,
        underlying="SPY",
        composite_score=30.0,  # chop band
        normalized_score=0.70,
        direction="chop_range",
        components={"dealer_regime": {"score": 0.5, "weight": 0.12}},
        aggregation=dict(aggregation or {}),
    )


def _market_ctx() -> dict:
    # Bullish trend in recent_closes drives _resolve_trade_direction -> bullish.
    return {
        "close": 501.0,
        "net_gex": 1.0e9,
        "gamma_flip": 500.0,
        "put_call_ratio": 1.0,
        "max_pain": 500.0,
        "smart_call": 0.0,
        "smart_put": 0.0,
        "recent_closes": [499.0, 500.5, 501.0],
        "iv_rank": 0.3,
    }


def _compute_contracts(engine: PortfolioEngine, score: ScoreSnapshot) -> int:
    with (
        patch.object(
            engine,
            "_select_optimizer_candidate",
            return_value={
                "candidate": _candidate(),
                "signal_timeframe": "intraday",
                "signal_strength": "high",
            },
        ),
        patch.object(
            engine,
            "_resolve_option_symbol_for_leg",
            return_value="SPY 260417C500",
        ),
    ):
        target = engine.compute_target(score, _market_ctx(), conn=MagicMock())
    assert target.target_positions, target.rationale
    return target.total_target_contracts


# ----------------------------------------------------------------------
# Override behavior
# ----------------------------------------------------------------------


def test_chop_regime_without_card_trigger_uses_scalp_cap():
    """Baseline: pure-MSI chop score uses the chop_high_conviction cap (0.85)
    when conviction clears the threshold.  base=10, conviction=0.70,
    cap=0.85 -> contracts ~= int(10 * 0.70 * 0.85) = 5."""
    engine = _make_engine()
    score = _chop_score()  # no card_trigger
    # Pure-MSI bullish conviction = normalized_score = 0.70 (high enough
    # to clear chop_high_conviction_threshold 0.55), so cap lifts to 0.85.
    contracts = _compute_contracts(engine, score)
    assert contracts == 5


def test_chop_regime_with_high_confidence_card_lifts_to_full_size():
    """With a card_trigger and card_confidence >= 0.65, the cap lifts to 1.0:
    contracts = int(10 * 0.70 * 1.0) = 7."""
    engine = _make_engine()
    score = _chop_score(
        aggregation={
            "card_trigger": "gamma_flip_bounce",
            "card_confidence": 0.72,
        }
    )
    contracts = _compute_contracts(engine, score)
    assert contracts == 7


def test_chop_regime_with_low_confidence_card_does_not_override():
    """Card present but confidence below threshold -> override skipped, falls
    back to chop_high_conviction cap (0.85)."""
    engine = _make_engine()
    score = _chop_score(
        aggregation={
            "card_trigger": "gamma_flip_bounce",
            "card_confidence": 0.50,  # < default threshold 0.65
        }
    )
    contracts = _compute_contracts(engine, score)
    assert contracts == 5


def test_trend_expansion_with_card_unchanged_at_full_size():
    """Trend regimes already use 1.0x; override is a no-op."""
    engine = _make_engine()
    score = ScoreSnapshot(
        timestamp=NOW,
        underlying="SPY",
        composite_score=85.0,
        normalized_score=0.85,
        direction="trend_expansion",
        components={"dealer_regime": {"score": 0.8, "weight": 0.12}},
        aggregation={
            "card_trigger": "gamma_flip_bounce",
            "card_confidence": 0.72,
        },
    )
    contracts = _compute_contracts(engine, score)
    # base=10, conviction=0.85, multiplier=1.0 -> int(8.5) = 8.
    assert contracts == 8


def test_controlled_trend_with_card_lifts_to_full_size():
    """controlled_trend default multiplier is 0.75; override should lift to 1.0."""
    engine = _make_engine()
    score = ScoreSnapshot(
        timestamp=NOW,
        underlying="SPY",
        composite_score=55.0,
        normalized_score=0.70,
        direction="controlled_trend",
        components={"dealer_regime": {"score": 0.7, "weight": 0.12}},
        aggregation={
            "card_trigger": "put_wall_bounce",
            "card_confidence": 0.70,
        },
    )
    # With trend confirmation needed, also patch that gate.
    engine_obj = engine
    with patch.object(engine_obj, "_score_trend_confirmation", return_value=True):
        contracts = _compute_contracts(engine_obj, score)
    # base=10, conviction=0.70, multiplier lifts 0.75 -> 1.0 -> int(7.0) = 7.
    assert contracts == 7


def test_card_with_blank_pattern_id_does_not_override():
    """Aggregation with card_confidence but no pattern id -> override skipped."""
    engine = _make_engine()
    score = _chop_score(
        aggregation={
            "card_trigger": "",  # empty string is falsy
            "card_confidence": 0.80,
        }
    )
    contracts = _compute_contracts(engine, score)
    # Falls back to chop_high_conviction cap (0.85) -> 5.
    assert contracts == 5


def test_card_confidence_at_exact_threshold_triggers_override():
    """Boundary: card_confidence == threshold (0.65) should trigger override."""
    engine = _make_engine()
    score = _chop_score(
        aggregation={
            "card_trigger": "gamma_flip_bounce",
            "card_confidence": 0.65,
        }
    )
    contracts = _compute_contracts(engine, score)
    assert contracts == 7  # full 1.0x multiplier
