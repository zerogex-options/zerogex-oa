"""Tests for the continuous gex_regime scoring component."""
from datetime import datetime, timezone

import pytest

from src.signals.components.base import MarketContext
from src.signals.components.gex_regime import GexRegimeComponent, _GEX_NORM


def _ctx(**overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=0.0,
        gamma_flip=498.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    return MarketContext(**defaults)


comp = GexRegimeComponent()


def test_zero_gex_is_neutral():
    assert comp.compute(_ctx(net_gex=0.0)) == pytest.approx(0.0, abs=1e-9)


def test_negative_gex_above_flip_is_bullish():
    assert comp.compute(_ctx(net_gex=-_GEX_NORM)) > 0.5


def test_negative_gex_below_flip_is_bearish():
    assert comp.compute(_ctx(net_gex=-_GEX_NORM, close=495.0, gamma_flip=498.0)) < -0.5


def test_positive_gex_above_flip_is_dampened_bullish():
    score = comp.compute(_ctx(net_gex=_GEX_NORM))
    assert score > 0
    assert score < 0.5


def test_positive_gex_below_flip_is_dampened_bearish():
    score = comp.compute(_ctx(net_gex=_GEX_NORM, close=495.0, gamma_flip=498.0))
    assert score < 0
    assert score > -0.5


def test_magnitude_scales_with_gex_size_in_same_direction_anchor():
    shallow = abs(comp.compute(_ctx(net_gex=-_GEX_NORM / 4)))
    deep = abs(comp.compute(_ctx(net_gex=-_GEX_NORM * 2)))
    assert deep > shallow


def test_score_bounded():
    # tanh is always in [-1, 1]; float precision can round to the endpoints.
    for g in [-1e12, -1e9, 0.0, 1e9, 1e12]:
        s = comp.compute(_ctx(net_gex=g))
        assert -1.0 <= s <= 1.0


def test_flip_neutral_band_returns_zero():
    # Within 0.1% of flip and flat momentum -> neutral anchor.
    score = comp.compute(
        _ctx(
            net_gex=-_GEX_NORM,
            close=498.04,
            gamma_flip=498.0,
            recent_closes=[498.0, 498.0, 498.0, 498.0, 498.01],
        )
    )
    assert score == pytest.approx(0.0, abs=1e-9)


def test_context_values_round_trip():
    ctx = _ctx(net_gex=-_GEX_NORM)
    cv = comp.context_values(ctx)
    assert cv["net_gex"] == -_GEX_NORM
    assert cv["gex_norm"] == _GEX_NORM
    assert cv["regime"] == "short_gamma"
    assert cv["score"] == pytest.approx(comp.compute(ctx))
