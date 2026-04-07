"""Unit tests for VolExpansionComponent."""
from datetime import datetime, timezone

import pytest

from src.signals.components.vol_expansion import VolExpansionComponent, _GEX_NORM, _MOMENTUM_NORM
from src.signals.components.base import MarketContext


def _ctx(**kwargs) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=550.0,
        net_gex=-2_500_000_000.0,
        gamma_flip=545.0,
        put_call_ratio=1.0,
        max_pain=548.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[548.0, 548.5, 549.0, 549.5, 550.0],  # 5 bars, rising
        iv_rank=None,
    )
    defaults.update(kwargs)
    return MarketContext(**defaults)


comp = VolExpansionComponent()


# ---------------------------------------------------------------------------
# Positive GEX → always 0
# ---------------------------------------------------------------------------

def test_positive_gex_returns_zero():
    ctx = _ctx(net_gex=1_000_000_000.0)
    assert comp.compute(ctx) == 0.0


def test_zero_gex_returns_zero():
    ctx = _ctx(net_gex=0.0)
    assert comp.compute(ctx) == 0.0


# ---------------------------------------------------------------------------
# Insufficient close history → 0
# ---------------------------------------------------------------------------

def test_fewer_than_5_closes_returns_zero():
    ctx = _ctx(recent_closes=[549.0, 550.0])
    assert comp.compute(ctx) == 0.0


# ---------------------------------------------------------------------------
# Negative GEX — score = momentum * vol_pressure
# ---------------------------------------------------------------------------

def test_full_bullish_score():
    # net_gex = -5B (vol_pressure = 1.0), price up 0.5% over 5 bars (momentum = 1.0)
    base = 550.0
    closes = [base] * 4 + [base * 1.005]
    ctx = _ctx(net_gex=-_GEX_NORM, recent_closes=closes)
    score = comp.compute(ctx)
    assert abs(score - 1.0) < 1e-6


def test_full_bearish_score():
    base = 550.0
    closes = [base] * 4 + [base * 0.995]
    ctx = _ctx(net_gex=-_GEX_NORM, recent_closes=closes)
    score = comp.compute(ctx)
    assert abs(score - (-1.0)) < 1e-6


def test_half_vol_pressure():
    # net_gex = -2.5B → vol_pressure = 0.5
    base = 550.0
    closes = [base] * 4 + [base * 1.005]  # full momentum
    ctx = _ctx(net_gex=-2_500_000_000.0, recent_closes=closes)
    score = comp.compute(ctx)
    assert abs(score - 0.5) < 1e-6


def test_partial_momentum():
    # 0.25% move → momentum = 0.5 (half of _MOMENTUM_NORM = 0.5%)
    base = 550.0
    closes = [base] * 4 + [base * (1 + _MOMENTUM_NORM / 2)]
    ctx = _ctx(net_gex=-_GEX_NORM, recent_closes=closes)
    score = comp.compute(ctx)
    assert abs(score - 0.5) < 1e-4


def test_momentum_clamped_at_1():
    # Huge move should not exceed ±1
    base = 550.0
    closes = [base] * 4 + [base * 1.10]  # 10% move
    ctx = _ctx(net_gex=-_GEX_NORM, recent_closes=closes)
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_flat_price_returns_zero():
    ctx = _ctx(recent_closes=[550.0] * 5)
    assert comp.compute(ctx) == 0.0


def test_score_in_bounds():
    ctx = _ctx()
    score = comp.compute(ctx)
    assert -1.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# context_values
# ---------------------------------------------------------------------------

def test_context_values_keys():
    ctx = _ctx()
    cv = comp.context_values(ctx)
    assert set(cv.keys()) == {"net_gex", "gex_regime", "vol_pressure", "pct_change_5bar"}


def test_context_values_positive_gex():
    ctx = _ctx(net_gex=1_000_000_000.0)
    cv = comp.context_values(ctx)
    assert cv["gex_regime"] == "positive"


def test_context_values_pct_change_none_when_insufficient_closes():
    ctx = _ctx(recent_closes=[550.0])
    cv = comp.context_values(ctx)
    assert cv["pct_change_5bar"] is None
