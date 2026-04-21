"""Tests for the contrarian direction override in ScoringEngine.

The override flips the composite sign when the three contrarian
components (exhaustion, skew_delta, positioning_trap) agree strongly
against the trend-driven composite. Magnitude is preserved so Kelly
sizing still reflects total conviction.
"""

from datetime import datetime, timezone

import pytest

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.scoring_engine import ScoringEngine


class _FakeComponent(ComponentBase):
    def __init__(self, name: str, weight: float, score: float):
        self.name = name
        self.weight = weight
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
        net_gex=0.0,
        gamma_flip=None,
        put_call_ratio=1.0,
        max_pain=None,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[],
        iv_rank=None,
    )


def _engine(trend_score: float, contrarian_score: float) -> ScoringEngine:
    """Build an engine with one "trend" component (weight 0.7) and three
    contrarian components (exhaustion 0.05, skew_delta 0.04,
    positioning_trap 0.06, total 0.15) all voting the same direction,
    plus a filler component to make weights sum to 1.0."""
    components = [
        _FakeComponent("trend", 0.70, trend_score),
        _FakeComponent("exhaustion", 0.05, contrarian_score),
        _FakeComponent("skew_delta", 0.04, contrarian_score),
        _FakeComponent("positioning_trap", 0.06, contrarian_score),
        _FakeComponent("filler", 0.15, 0.0),
    ]
    return ScoringEngine("SPY", components)


class TestContrarianOverride:
    def test_bullish_trend_with_strong_bearish_contrarians_flips_direction(self):
        eng = _engine(trend_score=0.6, contrarian_score=-0.8)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bearish"
        assert snap.aggregation["contrarian_override"] is True
        assert snap.aggregation["contrarian_consensus"] < 0
        assert "pre_override_composite" in snap.aggregation
        assert snap.aggregation["pre_override_composite"] > 0

    def test_bearish_trend_with_strong_bullish_contrarians_flips_direction(self):
        eng = _engine(trend_score=-0.6, contrarian_score=0.8)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bullish"
        assert snap.aggregation["contrarian_override"] is True
        assert snap.aggregation["contrarian_consensus"] > 0

    def test_weak_contrarians_do_not_flip_direction(self):
        """Consensus below the 0.60 threshold shouldn't fire the override."""
        eng = _engine(trend_score=0.6, contrarian_score=-0.40)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bullish"
        assert snap.aggregation["contrarian_override"] is False

    def test_contrarians_aligned_with_trend_do_nothing(self):
        """When contrarian signals agree with the trend there is nothing
        to override -- the direction stays the same."""
        eng = _engine(trend_score=0.6, contrarian_score=0.8)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bullish"
        assert snap.aggregation["contrarian_override"] is False

    def test_near_zero_composite_is_not_flipped(self):
        """If the trend composite is below the min-composite floor the
        override stays out of the way -- no point flipping noise."""
        eng = _engine(trend_score=0.05, contrarian_score=-0.9)
        snap, _ = eng.score(_ctx())
        # Trend composite starts tiny; min-composite guard suppresses the flip.
        assert snap.aggregation["contrarian_override"] is False

    def test_override_preserves_magnitude(self):
        """Flipping only changes the sign -- Kelly sizing still sees the
        full trend-driven magnitude."""
        eng = _engine(trend_score=0.6, contrarian_score=-0.8)
        snap, _ = eng.score(_ctx())
        pre = snap.aggregation["pre_override_composite"]
        assert snap.composite_score == pytest.approx(-pre, rel=1e-6)

    def test_disabling_flag_restores_legacy_behavior(self, monkeypatch):
        monkeypatch.setattr(
            "src.signals.scoring_engine.SIGNALS_CONTRARIAN_OVERRIDE_ENABLED", False
        )
        eng = _engine(trend_score=0.6, contrarian_score=-0.8)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bullish"
        assert snap.aggregation["contrarian_override"] is False
        # Consensus is still recorded for observability.
        assert snap.aggregation["contrarian_consensus"] < 0
