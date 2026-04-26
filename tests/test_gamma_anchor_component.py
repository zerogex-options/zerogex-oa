"""Tests for the unified gamma_anchor MSI component (Phase 2.1).

The component blends the three pre-existing sub-signals (flip_distance,
local_gamma, price_vs_max_gamma) into a single weighted score so the
composite-score math stops triple-counting one underlying observation.
"""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.components.gamma_anchor import GammaAnchorComponent


def _ctx(
    *,
    close: float = 500.0,
    gamma_flip: float = None,
    max_gamma_strike: float = None,
    local_gex: float = None,
    net_gex: float = -100_000_000.0,
    recent_closes: list[float] = None,
) -> MarketContext:
    extra: dict = {}
    if max_gamma_strike is not None:
        extra["max_gamma_strike"] = max_gamma_strike
    if local_gex is not None:
        extra["local_gex"] = local_gex
        extra["normalizers"] = {"local_gex": abs(net_gex) or 1.0}
    return MarketContext(
        timestamp=datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=close,
        net_gex=net_gex,
        gamma_flip=gamma_flip,
        put_call_ratio=1.0,
        max_pain=None,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=recent_closes or [],
        iv_rank=None,
        extra=extra,
    )


class TestGammaAnchorBlend:
    def test_blend_weights_sum_to_one(self):
        cmp = GammaAnchorComponent()
        payload = cmp.context_values(_ctx())
        weights = payload["blend_weights"]
        total = weights["flip_distance"] + weights["local_gamma"] + weights["price_vs_max_gamma"]
        assert abs(total - 1.0) < 1e-6

    def test_score_in_unit_interval(self):
        cmp = GammaAnchorComponent()
        # All inputs configured to produce extreme bullish/anchored scores.
        ctx = _ctx(
            close=500.0,
            gamma_flip=500.0,         # at flip
            max_gamma_strike=500.0,   # at max-gamma
            local_gex=1.0e12,         # huge local gamma
            recent_closes=[500.0] * 30,
        )
        score = cmp.compute(ctx)
        assert -1.0 <= score <= 1.0

    def test_anchored_inputs_yield_negative_score(self):
        """At max-gamma strike + dense local gamma → 'anchored / chop'."""
        cmp = GammaAnchorComponent()
        ctx = _ctx(
            close=500.0,
            gamma_flip=480.0,          # far from flip → -1 sub-score
            max_gamma_strike=500.0,    # at strike → -1 sub-score
            local_gex=1.0e12,          # high local gamma → -1 sub-score
            recent_closes=[500.0] * 30,
        )
        score = cmp.compute(ctx)
        # All three sub-signals push toward -1; blend should be strongly negative.
        assert score < -0.5

    def test_free_inputs_yield_positive_score(self):
        """Near flip + thin local gamma + far from max-gamma → 'free / volatile'."""
        cmp = GammaAnchorComponent()
        ctx = _ctx(
            close=500.0,
            gamma_flip=500.0,          # at flip → +1 sub-score
            max_gamma_strike=480.0,    # 4% away → +1 sub-score
            local_gex=0.0,             # no local gamma → +1 sub-score
            recent_closes=[500.0] * 30,
        )
        score = cmp.compute(ctx)
        assert score > 0.5

    def test_disagreeing_inputs_blend_toward_zero(self):
        """When sub-signals disagree the blend cancels.  Replaces the old
        triple-counting that would have multiplied the same observation."""
        cmp = GammaAnchorComponent()
        ctx = _ctx(
            close=500.0,
            gamma_flip=500.0,          # at flip → +1 (free)
            max_gamma_strike=500.0,    # at strike → -1 (anchored)
            local_gex=1.0e12,          # high local gamma → -1 (anchored)
            recent_closes=[500.0] * 30,
        )
        score = cmp.compute(ctx)
        # 0.45 * (+1) + 0.35 * (-1) + 0.20 * (-1) = -0.10
        assert -0.20 < score < 0.0

    def test_context_values_emit_subscores(self):
        cmp = GammaAnchorComponent()
        ctx = _ctx(
            close=500.0,
            gamma_flip=499.0,
            max_gamma_strike=498.0,
            local_gex=1.0e8,
            recent_closes=[499.0, 500.0, 501.0] * 10,
        )
        payload = cmp.context_values(ctx)
        assert "flip_distance_subscore" in payload
        assert "local_gamma_subscore" in payload
        assert "price_vs_max_gamma_subscore" in payload
        assert "score" in payload
        # All sub-scores must be in [-1, +1].
        for k in (
            "flip_distance_subscore",
            "local_gamma_subscore",
            "price_vs_max_gamma_subscore",
            "score",
        ):
            assert -1.0 <= payload[k] <= 1.0


class TestGammaAnchorIntegrationWithScoringEngine:
    def test_gamma_anchor_drives_composite_when_strongly_anchored(self):
        """End-to-end: a strongly-anchored gamma_anchor reading shifts the
        composite below 50 while the deprecated stubs contribute nothing."""
        from src.signals.components.flip_distance import FlipDistanceComponent
        from src.signals.components.local_gamma import LocalGammaComponent
        from src.signals.components.price_vs_max_gamma import PriceVsMaxGammaComponent
        from src.signals.scoring_engine import ScoringEngine

        eng = ScoringEngine(
            "SPY",
            [
                GammaAnchorComponent(),
                FlipDistanceComponent(),       # zero-weight stub
                LocalGammaComponent(),         # zero-weight stub
                PriceVsMaxGammaComponent(),    # zero-weight stub
            ],
        )
        ctx = _ctx(
            close=500.0,
            gamma_flip=480.0,           # anchored
            max_gamma_strike=500.0,     # at strike
            local_gex=1.0e12,           # very dense
            recent_closes=[500.0] * 30,
        )
        snap, _ = eng.score(ctx)
        # gamma_anchor pulls strongly negative -> composite well below 50.
        assert snap.composite_score < 50.0
        # The three deprecated stubs appear in the dict with 0 max_points.
        for name in ("flip_distance", "local_gamma", "price_vs_max_gamma"):
            assert name in snap.components
            assert snap.components[name]["max_points"] == 0.0
