"""Tests for the continuous gamma_flip scoring component."""
from datetime import datetime, timezone

import pytest

from src.signals.components.base import MarketContext
from src.signals.basic.gamma_flip import GammaFlipComponent, _DIST_NORM


def _ctx(**overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=0.0,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    return MarketContext(**defaults)


comp = GammaFlipComponent()


def test_missing_flip_is_neutral():
    assert comp.compute(_ctx(gamma_flip=None)) == 0.0


def test_at_flip_is_neutral():
    assert comp.compute(_ctx(close=500.0, gamma_flip=500.0)) == pytest.approx(0.0)


def test_above_flip_is_bullish():
    # 1% above flip (2x the norm) -> strongly bullish but still < 1.
    score = comp.compute(_ctx(close=505.0, gamma_flip=500.0))
    assert score > 0.9
    assert score < 1.0


def test_below_flip_is_bearish():
    score = comp.compute(_ctx(close=495.0, gamma_flip=500.0))
    assert score < -0.9
    assert score > -1.0


def test_small_move_still_non_zero():
    """Legacy had a hard 0.3% dead zone; the continuous version should not."""
    score = comp.compute(_ctx(close=500.5, gamma_flip=500.0))
    assert score > 0.0
    assert score < 0.5


def test_monotonic_in_distance():
    vals = [
        comp.compute(_ctx(close=500.0 * (1 + d), gamma_flip=500.0))
        for d in [-0.02, -0.005, 0.0, 0.005, 0.02]
    ]
    for i in range(len(vals) - 1):
        assert vals[i] < vals[i + 1]


def test_score_bounded():
    for d in [-0.5, -0.1, 0.0, 0.1, 0.5]:
        score = comp.compute(_ctx(close=500.0 * (1 + d), gamma_flip=500.0))
        assert -1.0 <= score <= 1.0


def test_context_values_round_trip():
    ctx = _ctx(close=503.0, gamma_flip=500.0)
    cv = comp.context_values(ctx)
    assert cv["gamma_flip"] == 500.0
    assert cv["close"] == 503.0
    assert cv["distance_pct"] == pytest.approx(0.006)
    assert cv["dist_norm"] == _DIST_NORM
    assert cv["score"] == pytest.approx(comp.compute(ctx))
