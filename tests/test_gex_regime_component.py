"""Tests for the flow+positioning anchored gex_regime scoring component."""
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
        smart_call=800_000.0,
        smart_put=200_000.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    return MarketContext(**defaults)


comp = GexRegimeComponent()


def test_zero_gex_is_neutral():
    # Regime strength is zero regardless of anchor.
    assert comp.compute(_ctx(net_gex=0.0)) == pytest.approx(0.0, abs=1e-9)


def test_negative_gex_with_bullish_flow_anchor_is_bullish():
    assert comp.compute(_ctx(net_gex=-_GEX_NORM)) > 0.1


def test_negative_gex_with_bearish_flow_anchor_is_bearish():
    score = comp.compute(_ctx(net_gex=-_GEX_NORM, smart_call=200_000.0, smart_put=800_000.0))
    assert score < -0.1


def test_positive_gex_dampens_same_anchor_vs_negative_gex():
    short_gamma = comp.compute(_ctx(net_gex=-_GEX_NORM))
    score = comp.compute(_ctx(net_gex=_GEX_NORM))
    assert abs(score) < abs(short_gamma)


def test_positive_gex_uses_mean_reversion_anchor_when_pin_available():
    # Price above pin in long-gamma should bias toward reversion (bearish pull).
    score = comp.compute(
        _ctx(
            net_gex=_GEX_NORM,
            close=502.0,
            gamma_flip=500.0,
            smart_call=900_000.0,
            smart_put=100_000.0,
            recent_closes=[500.0, 500.5, 501.0, 501.5, 502.0],
            extra={"max_gamma_strike": 499.0},
        )
    )
    assert score < 0.0


def test_magnitude_scales_with_gex_size_for_same_anchor():
    shallow = abs(comp.compute(_ctx(net_gex=-_GEX_NORM / 4)))
    deep = abs(comp.compute(_ctx(net_gex=-_GEX_NORM * 2)))
    assert deep > shallow


def test_score_bounded():
    # tanh is always in [-1, 1]; float precision can round to the endpoints.
    for g in [-1e12, -1e9, 0.0, 1e9, 1e12]:
        s = comp.compute(_ctx(net_gex=g))
        assert -1.0 <= s <= 1.0


def test_flip_neutral_band_returns_zero():
    # No flow/positioning edge + no tape edge -> neutral anchor.
    score = comp.compute(
        _ctx(
            net_gex=-_GEX_NORM,
            close=498.04,
            gamma_flip=498.0,
            smart_call=0.0,
            smart_put=0.0,
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
    assert cv["regime_state"] == "destabilizing_trend_amplifying"
    assert "direction_anchor_flow_positioning" in cv
    assert cv["score"] == pytest.approx(comp.compute(ctx))
