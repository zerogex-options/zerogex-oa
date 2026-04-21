from src.signals.strategy_builder import StrategyBuilder


def _rows_for_term_structure(near_iv: float, far_iv: float):
    return [
        {"expiration": "2026-04-24", "strike": 500.0, "option_type": "C", "iv": near_iv},
        {"expiration": "2026-05-29", "strike": 500.0, "option_type": "C", "iv": far_iv},
    ]


def test_long_volatility_regime_prefers_straddles_when_neutral_direction():
    sb = StrategyBuilder("SPY")
    decision = sb.decide(
        score_direction="neutral",
        score_normalized=0.12,
        market_ctx={
            "net_gex": -250_000_000.0,
            "iv_rank": 0.45,
            "recent_closes": [500.0] * 20,
        },
        option_rows=_rows_for_term_structure(0.20, 0.21),
    )
    assert decision.trade_type == "long_volatility"
    assert decision.optimizer_direction == "neutral"
    assert "long_straddle" in decision.preferred_strategies


def test_premium_sell_regime_prefers_short_structures():
    sb = StrategyBuilder("SPY")
    decision = sb.decide(
        score_direction="bullish",
        score_normalized=0.22,
        market_ctx={
            "net_gex": 200_000_000.0,
            "iv_rank": 0.88,
            "recent_closes": [500.0] * 20,
        },
        option_rows=_rows_for_term_structure(0.23, 0.24),
    )
    assert decision.trade_type == "premium_sell"
    assert decision.optimizer_direction == "neutral"
    assert "short_strangle" in decision.preferred_strategies


def test_calendar_regime_when_far_iv_exceeds_near_iv():
    sb = StrategyBuilder("SPY")
    decision = sb.decide(
        score_direction="neutral",
        score_normalized=0.28,
        market_ctx={
            "net_gex": 100_000_000.0,
            "iv_rank": 0.40,
            "recent_closes": [500.0] * 20,
        },
        option_rows=_rows_for_term_structure(0.14, 0.28),
    )
    assert decision.trade_type == "calendar"
    assert "calendar" in decision.preferred_strategies

