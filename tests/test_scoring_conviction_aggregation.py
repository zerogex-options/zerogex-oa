"""Tests for ScoringEngine conviction aggregation.

Conviction aggregation fights abstention dilution by:
  1. Renormalizing against only the weights of active (non-abstaining) components
  2. Amplifying by an agreement multiplier (tie -> 0.5, unanimous -> 1.75)
  3. Amplifying by an extremity multiplier when the loudest active component
     is close to the rails
"""

from datetime import datetime, timezone

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


def _engine(scores: list[float]) -> ScoringEngine:
    w = 1.0 / len(scores)
    comps = [_FakeComponent(f"c{i}", w, s) for i, s in enumerate(scores)]
    return ScoringEngine("SPY", comps)


class TestConvictionAggregation:
    def test_strong_majority_clears_trigger_threshold(self):
        """8 components at +0.7 with 6 abstaining should max out the composite,
        whereas linear averaging would only yield 0.36."""
        scores = [0.7] * 8 + [-0.2] * 1 + [0.0] * 6
        eng = _engine(scores)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bullish"
        # Raw linear composite would be ~0.36; conviction should be substantially higher.
        assert snap.normalized_score >= 0.95

    def test_weak_mixed_stays_below_trigger(self):
        """Truly ambiguous signals (5 at +0.5, 4 at -0.3) should not fire."""
        scores = [0.5] * 5 + [-0.3] * 4 + [0.0] * 6
        eng = _engine(scores)
        snap, _ = eng.score(_ctx())
        # Amplified, but still below the default 0.52 trigger threshold.
        assert snap.normalized_score < 0.52

    def test_unanimous_signal_fully_amplified(self):
        """All active components agreeing should push the score to the max."""
        scores = [0.6] * 10 + [0.0] * 5
        eng = _engine(scores)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bullish"
        assert snap.normalized_score >= 0.95

    def test_legacy_tie_returns_neutral(self):
        """Perfect tie should still report a near-zero composite."""
        scores = [0.3] * 7 + [-0.3] * 7 + [0.0]
        eng = _engine(scores)
        snap, _ = eng.score(_ctx())
        assert abs(snap.composite_score) < 0.01

    def test_scalp_band_score_stays_actionable(self):
        """A moderate bullish consensus should land in the scalp band (~0.35)."""
        scores = [0.4] * 6 + [-0.2] * 2 + [0.0] * 7
        eng = _engine(scores)
        snap, _ = eng.score(_ctx())
        assert snap.direction == "bullish"
        # Linear composite would be 0.133 -- conviction lifts it materially.
        assert snap.normalized_score >= 0.30

    def test_aggregation_diagnostics_are_populated(self):
        scores = [0.7] * 8 + [0.0] * 7
        eng = _engine(scores)
        snap, _ = eng.score(_ctx())
        agg = snap.aggregation
        assert agg["mode"] == "conviction"
        assert agg["active_count"] == 8
        assert agg["agreement"] == 1.0
        assert agg["max_abs_component"] == 0.7
        assert agg["agreement_multiplier"] > 1.0

    def test_all_abstaining_returns_zero(self):
        eng = _engine([0.0] * 5)
        snap, _ = eng.score(_ctx())
        assert snap.composite_score == 0.0
        assert snap.direction == "neutral"
        assert snap.aggregation["active_count"] == 0
