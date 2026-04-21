"""Tests for the gex_gradient scoring component."""
from datetime import datetime, timezone

import pytest

from src.signals.components.base import MarketContext
from src.signals.basic.gex_gradient import GexGradientComponent


def _ctx(rows=None, net_gex=-2.0e8, **overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=net_gex,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    ctx = MarketContext(**defaults)
    if rows is not None:
        ctx.extra["gex_by_strike"] = rows
    return ctx


comp = GexGradientComponent()


def test_no_rows_is_neutral():
    assert comp.compute(_ctx()) == 0.0


def test_empty_rows_is_neutral():
    assert comp.compute(_ctx(rows=[])) == 0.0


def test_thin_rows_below_min_total_are_neutral():
    """If total notional gamma is below the minimum, abstain."""
    rows = [
        {"strike": 502.0, "net_gex": 1.0e6},
        {"strike": 498.0, "net_gex": 1.0e6},
    ]
    assert comp.compute(_ctx(rows=rows)) == 0.0


def test_above_heavy_in_negative_gex_is_bullish():
    """Dealers short above-spot gamma (negative net_gex) must buy into a rally."""
    rows = [
        {"strike": 502.0, "net_gex": 2.0e8},
        {"strike": 504.0, "net_gex": 1.0e8},
        {"strike": 498.0, "net_gex": 1.0e7},
    ]
    score = comp.compute(_ctx(rows=rows, net_gex=-3.0e8))
    assert score > 0


def test_below_heavy_in_negative_gex_is_bearish():
    rows = [
        {"strike": 498.0, "net_gex": 2.0e8},
        {"strike": 496.0, "net_gex": 1.0e8},
        {"strike": 502.0, "net_gex": 1.0e7},
    ]
    score = comp.compute(_ctx(rows=rows, net_gex=-3.0e8))
    assert score < 0


def test_sign_flips_with_dealer_regime():
    """Same asymmetry should score opposite in positive vs negative net_gex."""
    rows = [
        {"strike": 502.0, "net_gex": 2.0e8},
        {"strike": 504.0, "net_gex": 1.0e8},
        {"strike": 498.0, "net_gex": 1.0e7},
    ]
    neg = comp.compute(_ctx(rows=rows, net_gex=-3.0e8))
    pos = comp.compute(_ctx(rows=rows, net_gex=3.0e8))
    assert neg * pos < 0  # opposite sign
    assert abs(pos) < abs(neg)  # long-gamma side is intentionally damped


def test_score_bounded():
    rows = [{"strike": 550.0, "net_gex": 1.0e12}]
    score = comp.compute(_ctx(rows=rows, net_gex=-1.0e12))
    assert -1.0 <= score <= 1.0


def test_context_values_populated_when_available():
    rows = [
        {"strike": 502.0, "net_gex": 2.0e8},
        {"strike": 498.0, "net_gex": 1.0e8},
    ]
    cv = comp.context_values(_ctx(rows=rows))
    assert cv["source"] == "gex_by_strike"
    assert cv["above_spot_gamma_abs"] == pytest.approx(2.0e8)
    assert cv["below_spot_gamma_abs"] == pytest.approx(1.0e8)
    assert cv["strike_count"] == 2


def test_context_values_unavailable_returns_nones():
    cv = comp.context_values(_ctx())
    assert cv["source"] == "unavailable"
    assert cv["above_spot_gamma_abs"] is None
