from datetime import datetime, timezone

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.scoring_engine import ScoringEngine


class _FakeComponent(ComponentBase):
    def __init__(self, name: str, score: float):
        self.name = name
        self.weight = 0.0
        self._score = score

    def compute(self, ctx: MarketContext) -> float:
        return self._score

    def context_values(self, ctx: MarketContext) -> dict:
        return {}


def _ctx() -> MarketContext:
    return MarketContext(
        timestamp=datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=-100_000_000.0,
        gamma_flip=498.0,
        put_call_ratio=1.0,
        max_pain=None,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[498.0, 499.0, 500.0],
        iv_rank=None,
    )


def _engine(scores: dict[str, float]) -> ScoringEngine:
    names = [
        "net_gex_sign",
        "flip_distance",
        "local_gamma",
        "put_call_ratio",
        "price_vs_max_gamma",
        "volatility_regime",
    ]
    return ScoringEngine(
        "SPY",
        [_FakeComponent(name, scores.get(name, 0.0)) for name in names],
    )


def test_market_state_score_is_centered_at_50_when_all_zero():
    eng = _engine({})
    snap, _ = eng.score(_ctx())
    assert snap.composite_score == 50.0
    assert snap.direction == "controlled_trend"


def test_market_state_score_caps_at_100():
    eng = _engine(
        {
            "net_gex_sign": 1.0,
            "flip_distance": 1.0,
            "local_gamma": 1.0,
            "put_call_ratio": 1.0,
            "price_vs_max_gamma": 1.0,
            "volatility_regime": 1.0,
        }
    )
    snap, _ = eng.score(_ctx())
    assert snap.composite_score == 100.0
    assert snap.direction == "trend_expansion"


def test_market_state_score_floor_at_zero():
    eng = _engine(
        {
            "net_gex_sign": -1.0,
            "flip_distance": -1.0,
            "local_gamma": -1.0,
            "put_call_ratio": -1.0,
            "price_vs_max_gamma": -1.0,
            "volatility_regime": -1.0,
        }
    )
    snap, _ = eng.score(_ctx())
    assert snap.composite_score == 0.0
    assert snap.direction == "high_risk_reversal"
