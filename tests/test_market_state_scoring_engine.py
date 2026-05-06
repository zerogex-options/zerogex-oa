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


# Active (non-zero-weight) component names in the post-Phase-2.1 layout.
# flip_distance / local_gamma / price_vs_max_gamma are intentionally
# omitted from this list — they remain registered in production with
# weight 0 (deprecated stubs for API back-compat), so injecting fake
# scores for them under the same names contributes nothing to composite.
_ACTIVE_COMPONENT_NAMES = [
    "net_gex_sign",
    "gamma_anchor",
    "put_call_ratio",
    "volatility_regime",
    "order_flow_imbalance",
    "dealer_delta_pressure",
]


def _engine(scores: dict[str, float], names=None) -> ScoringEngine:
    component_names = names if names is not None else _ACTIVE_COMPONENT_NAMES
    return ScoringEngine(
        "SPY",
        [_FakeComponent(name, scores.get(name, 0.0)) for name in component_names],
    )


def test_market_state_score_centered_near_50_when_all_components_abstain():
    """All-zero abstains get replaced with regime_tilt, so the composite
    sits close to 50 rather than landing exactly there.  The MarketContext
    used here has a slight bullish tilt (close > flip, PCR = 1.0,
    negative net GEX), so the composite drifts a hair above 50."""
    eng = _engine({})
    snap, _ = eng.score(_ctx())
    assert 49.0 <= snap.composite_score <= 60.0
    assert snap.direction in {"controlled_trend", "chop_range"}


def test_market_state_score_extreme_bullish_does_not_saturate_to_100():
    """Tanh saturation makes 100 an asymptotic extreme rather than a cap."""
    eng = _engine({name: 1.0 for name in _ACTIVE_COMPONENT_NAMES})
    snap, _ = eng.score(_ctx())
    assert 80.0 < snap.composite_score < 100.0
    assert snap.direction == "trend_expansion"


def test_market_state_score_extreme_bearish_does_not_saturate_to_zero():
    """Tanh saturation makes 0 an asymptotic extreme rather than a floor."""
    eng = _engine({name: -1.0 for name in _ACTIVE_COMPONENT_NAMES})
    snap, _ = eng.score(_ctx())
    assert 0.0 < snap.composite_score < 20.0
    assert snap.direction == "high_risk_reversal"


def test_unregistered_components_yield_default_max_points():
    """Components without a COMPONENT_POINTS entry fall back to ``weight * 100``.

    The fake components used here have ``weight = 0.0``, so an unregistered
    name contributes a regime-tilt fraction (rather than 0) once weighted
    by the fallback max_points.  We assert on max_points and direction
    rather than a precise composite value.
    """
    eng = _engine({"unknown_component": 1.0}, names=["unknown_component"])
    snap, _ = eng.score(_ctx())
    assert 49.0 <= snap.composite_score <= 51.0
    assert snap.components["unknown_component"]["max_points"] == 0.0
