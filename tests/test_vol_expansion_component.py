"""Unit tests for VolExpansionComponent — primed-environment model."""
from datetime import datetime, timezone

import pytest

from src.signals.components.vol_expansion import (
    VolExpansionComponent,
    _GEX_NORM,
    _MOMENTUM_NORM,
)
from src.signals.components.base import MarketContext


def _ctx(**kwargs) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=550.0,
        net_gex=-_GEX_NORM,          # full vol_pressure = 1.0
        gamma_flip=545.0,
        put_call_ratio=1.0,
        max_pain=548.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[550.0] * 5,   # flat price by default
        iv_rank=None,
    )
    defaults.update(kwargs)
    return MarketContext(**defaults)


comp = VolExpansionComponent()


# ---------------------------------------------------------------------------
# Positive / zero GEX → always 0
# ---------------------------------------------------------------------------

def test_positive_gex_returns_zero():
    assert comp.compute(_ctx(net_gex=1_000_000_000.0)) == 0.0


def test_zero_gex_returns_zero():
    assert comp.compute(_ctx(net_gex=0.0)) == 0.0


# ---------------------------------------------------------------------------
# Flat / rising price → +vol_pressure (primed, uncontradicted)
# ---------------------------------------------------------------------------

def test_flat_price_returns_vol_pressure():
    """Core primed-environment property: flat price should NOT zero the score."""
    ctx = _ctx()  # vol_pressure = 1.0, momentum = 0
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_rising_price_returns_vol_pressure():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * 1.005])
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_any_positive_momentum_saturates_at_vol_pressure():
    """Any upward momentum gives the same score as flat — already at max readiness."""
    base = 550.0
    ctx_tiny = _ctx(recent_closes=[base] * 4 + [base * 1.0001])
    ctx_full = _ctx(recent_closes=[base] * 4 + [base * 1.005])
    assert comp.compute(ctx_tiny) == pytest.approx(comp.compute(ctx_full))


# ---------------------------------------------------------------------------
# Falling price — linear shift toward -vol_pressure
# ---------------------------------------------------------------------------

def test_full_bearish_momentum_returns_negative_vol_pressure():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM)])
    assert comp.compute(ctx) == pytest.approx(-1.0)


def test_half_bearish_momentum_returns_zero():
    """At half the bearish threshold, readiness and momentum exactly cancel."""
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM / 2)])
    assert comp.compute(ctx) == pytest.approx(0.0, abs=1e-6)


def test_quarter_bearish_momentum_returns_half_vol_pressure():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM / 4)])
    assert comp.compute(ctx) == pytest.approx(0.5, abs=1e-4)


def test_bearish_momentum_clamped():
    """A crash larger than _MOMENTUM_NORM should not exceed -vol_pressure."""
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * 0.90])  # -10% move
    assert comp.compute(ctx) == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# vol_pressure scaling
# ---------------------------------------------------------------------------

def test_half_gex_half_vol_pressure_flat():
    ctx = _ctx(net_gex=-_GEX_NORM / 2)
    assert comp.compute(ctx) == pytest.approx(0.5)


def test_score_with_realistic_gex():
    """$-210.9M with $300M norm → vol_pressure ≈ 0.703, flat price → score ≈ 0.703."""
    ctx = _ctx(net_gex=-210_900_000)
    score = comp.compute(ctx)
    assert 0.65 < score < 0.75


def test_gex_saturates_at_norm():
    ctx = _ctx(net_gex=-_GEX_NORM * 10)  # way beyond norm
    assert comp.compute(ctx) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Insufficient close history
# ---------------------------------------------------------------------------

def test_fewer_than_5_closes_returns_vol_pressure():
    """No momentum data → return pure readiness (positive in negative GEX)."""
    ctx = _ctx(recent_closes=[549.0, 550.0])
    assert comp.compute(ctx) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Output range
# ---------------------------------------------------------------------------

def test_score_always_in_bounds():
    for net_gex in [-_GEX_NORM * 2, -_GEX_NORM, -_GEX_NORM / 2, 0, _GEX_NORM]:
        for delta in [-0.02, -0.005, 0, 0.005, 0.02]:
            base = 550.0
            ctx = _ctx(
                net_gex=net_gex,
                recent_closes=[base] * 4 + [base * (1 + delta)],
            )
            score = comp.compute(ctx)
            assert -1.0 <= score <= 1.0, f"Out of bounds: {score} (gex={net_gex}, delta={delta})"


# ---------------------------------------------------------------------------
# context_values
# ---------------------------------------------------------------------------

def test_context_values_keys():
    ctx = _ctx()
    cv = comp.context_values(ctx)
    assert set(cv.keys()) == {"net_gex", "gex_regime", "vol_pressure", "pct_change_5bar", "momentum"}


def test_context_values_momentum_none_when_insufficient_closes():
    ctx = _ctx(recent_closes=[550.0])
    cv = comp.context_values(ctx)
    assert cv["pct_change_5bar"] is None
    assert cv["momentum"] is None


def test_context_values_positive_gex_regime():
    ctx = _ctx(net_gex=500_000_000.0)
    cv = comp.context_values(ctx)
    assert cv["gex_regime"] == "positive"
