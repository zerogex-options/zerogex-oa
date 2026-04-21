"""Tests for dealer_delta_pressure component."""
from datetime import datetime, timezone

import pytest

from src.signals.components.base import MarketContext
from src.signals.basic.dealer_delta_pressure import (
    DealerDeltaPressureComponent,
    _DNI_NORM,
)


def _ctx(**overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=-2.0e8,
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


comp = DealerDeltaPressureComponent()


def test_no_data_is_neutral():
    assert comp.compute(_ctx()) == 0.0


def test_explicit_dealer_net_delta_shorts_bullish():
    ctx = _ctx(dealer_net_delta=-_DNI_NORM)
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_explicit_dealer_net_delta_longs_bearish():
    ctx = _ctx(dealer_net_delta=_DNI_NORM)
    assert comp.compute(ctx) == pytest.approx(-1.0)


def test_delta_oi_columns_used_when_available():
    """When rows include call_delta_oi/put_delta_oi, use them directly."""
    rows = [
        {"strike": 500.0, "call_delta_oi": 1.0e8, "put_delta_oi": -0.5e8},
    ]
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = rows
    # Total customer delta = 1.0e8 + (-0.5e8) = 0.5e8
    # Dealer delta = -customer = -0.5e8
    # Score sign = -dealer / norm = +0.5e8 / norm -> positive (bullish)
    assert comp.compute(ctx) > 0


def test_distance_proxy_fallback():
    """With only call_oi/put_oi, the linear-distance proxy kicks in."""
    rows = [
        {"strike": 500.0, "call_oi": 10000, "put_oi": 10000},
    ]
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = rows
    # Shouldn't raise; should yield a finite score in [-1, 1].
    score = comp.compute(ctx)
    assert -1.0 <= score <= 1.0


def test_context_values_reports_source_unavailable():
    cv = comp.context_values(_ctx())
    assert cv["source"] == "unavailable"
    assert cv["dealer_net_delta_estimated"] is None


def test_context_values_reports_source_delta_oi():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "call_delta_oi": 1e7, "put_delta_oi": -1e7}
    ]
    cv = comp.context_values(ctx)
    assert cv["source"] == "gex_by_strike.delta_oi"
