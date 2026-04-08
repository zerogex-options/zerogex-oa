"""Unit tests for VolExpansionComponent — continuous spectrum model."""
from datetime import datetime, timezone

import pytest

from src.signals.components.vol_expansion import (
    VolExpansionComponent,
    _GEX_NORM,
    _GEX_FLOOR,
    _MOMENTUM_NORM,
)
from src.signals.components.base import MarketContext


def _ctx(**kwargs) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=550.0,
        net_gex=-_GEX_NORM,          # full readiness = 1.0
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
# Positive / zero GEX → suppressed but non-zero
# ---------------------------------------------------------------------------

def test_positive_gex_returns_floor():
    """Deeply positive GEX suppresses to floor, never zero."""
    score = comp.compute(_ctx(net_gex=1_000_000_000.0))
    assert score == pytest.approx(_GEX_FLOOR)


def test_zero_gex_returns_midpoint():
    """Zero GEX gives moderate readiness: midpoint of [FLOOR, 1.0]."""
    expected = (_GEX_FLOOR + 1.0) / 2.0
    assert comp.compute(_ctx(net_gex=0.0)) == pytest.approx(expected)


def test_positive_gex_never_zero():
    """No GEX value should ever produce exactly 0."""
    for gex in [0, 1_000_000, 100_000_000, 1_000_000_000, 10_000_000_000]:
        score = comp.compute(_ctx(net_gex=float(gex)))
        assert score > 0, f"Score should be > 0 for net_gex={gex}, got {score}"


# ---------------------------------------------------------------------------
# Negative GEX — readiness approaches 1.0
# ---------------------------------------------------------------------------

def test_full_negative_gex_flat_price():
    """net_gex = -GEX_NORM, flat price → readiness = 1.0."""
    ctx = _ctx()  # net_gex=-_GEX_NORM, flat closes
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_rising_price_returns_readiness():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * 1.005])
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_any_positive_momentum_saturates_at_readiness():
    """Any upward momentum gives the same score as flat — already at max."""
    base = 550.0
    ctx_tiny = _ctx(recent_closes=[base] * 4 + [base * 1.0001])
    ctx_full = _ctx(recent_closes=[base] * 4 + [base * 1.005])
    assert comp.compute(ctx_tiny) == pytest.approx(comp.compute(ctx_full))


# ---------------------------------------------------------------------------
# Falling price — linear shift toward -readiness
# ---------------------------------------------------------------------------

def test_full_bearish_momentum_returns_negative_readiness():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM)])
    assert comp.compute(ctx) == pytest.approx(-1.0)


def test_half_bearish_momentum_returns_zero():
    """At half the bearish threshold, readiness and momentum exactly cancel."""
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM / 2)])
    assert comp.compute(ctx) == pytest.approx(0.0, abs=1e-6)


def test_quarter_bearish_momentum():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM / 4)])
    # readiness=1.0, momentum=-0.25 → 1.0 * (1 + 2*(-0.25)) = 0.5
    assert comp.compute(ctx) == pytest.approx(0.5, abs=1e-4)


def test_bearish_momentum_clamped():
    """A crash larger than _MOMENTUM_NORM should not exceed -readiness."""
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * 0.90])  # -10% move
    assert comp.compute(ctx) == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# GEX readiness scaling (continuous spectrum)
# ---------------------------------------------------------------------------

def test_half_negative_gex_readiness():
    """Half GEX_NORM negative → readiness midpoint between mid and 1.0."""
    ctx = _ctx(net_gex=-_GEX_NORM / 2)
    # normalized = 0.5, readiness = FLOOR + (1-FLOOR) * 1.5/2 = FLOOR + 0.75*(1-FLOOR)
    expected = _GEX_FLOOR + (1.0 - _GEX_FLOOR) * 0.75
    assert comp.compute(ctx) == pytest.approx(expected)


def test_readiness_with_realistic_negative_gex():
    """$-210.9M with $300M norm → readiness well above midpoint."""
    ctx = _ctx(net_gex=-210_900_000)
    score = comp.compute(ctx)
    midpoint = (_GEX_FLOOR + 1.0) / 2.0
    assert score > midpoint


def test_gex_saturates_at_norm():
    ctx = _ctx(net_gex=-_GEX_NORM * 10)  # way beyond norm
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_positive_gex_saturates_at_floor():
    ctx = _ctx(net_gex=_GEX_NORM * 10)  # way beyond norm
    assert comp.compute(ctx) == pytest.approx(_GEX_FLOOR)


def test_readiness_monotonically_decreases_with_positive_gex():
    """As GEX goes more positive, readiness decreases."""
    scores = []
    for gex in [-_GEX_NORM, -_GEX_NORM / 2, 0, _GEX_NORM / 2, _GEX_NORM]:
        scores.append(comp.compute(_ctx(net_gex=gex)))
    for i in range(len(scores) - 1):
        assert scores[i] >= scores[i + 1], f"Not monotonic at index {i}: {scores}"


# ---------------------------------------------------------------------------
# Positive GEX with momentum
# ---------------------------------------------------------------------------

def test_positive_gex_with_bearish_momentum():
    """Positive GEX + falling price → small negative score (not zero)."""
    base = 550.0
    ctx = _ctx(
        net_gex=_GEX_NORM,  # deeply positive → readiness = FLOOR
        recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM)],
    )
    assert comp.compute(ctx) == pytest.approx(-_GEX_FLOOR)


def test_positive_gex_with_rising_momentum():
    """Positive GEX + rising price → small positive score (not zero)."""
    base = 550.0
    ctx = _ctx(
        net_gex=_GEX_NORM,
        recent_closes=[base] * 4 + [base * 1.005],
    )
    assert comp.compute(ctx) == pytest.approx(_GEX_FLOOR)


# ---------------------------------------------------------------------------
# Insufficient close history
# ---------------------------------------------------------------------------

def test_fewer_than_5_closes_returns_readiness():
    """No momentum data → return pure readiness score."""
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


def test_score_never_exactly_zero_with_flat_price():
    """With flat price (momentum=0), score should never be zero for any GEX."""
    for gex in [-_GEX_NORM, -_GEX_NORM / 2, 0, _GEX_NORM / 2, _GEX_NORM]:
        score = comp.compute(_ctx(net_gex=gex))
        assert score > 0, f"Score should be > 0 with flat price, got {score} for gex={gex}"


# ---------------------------------------------------------------------------
# context_values
# ---------------------------------------------------------------------------

def test_context_values_keys():
    ctx = _ctx()
    cv = comp.context_values(ctx)
    assert set(cv.keys()) == {"net_gex", "gex_regime", "gex_readiness", "pct_change_5bar", "momentum"}


def test_context_values_momentum_none_when_insufficient_closes():
    ctx = _ctx(recent_closes=[550.0])
    cv = comp.context_values(ctx)
    assert cv["pct_change_5bar"] is None
    assert cv["momentum"] is None


def test_context_values_positive_gex_regime():
    ctx = _ctx(net_gex=500_000_000.0)
    cv = comp.context_values(ctx)
    assert cv["gex_regime"] == "positive"


def test_context_values_gex_readiness_matches_compute():
    """gex_readiness in context_values should match what compute uses."""
    for gex in [-_GEX_NORM, 0, _GEX_NORM]:
        ctx = _ctx(net_gex=gex)
        cv = comp.context_values(ctx)
        assert cv["gex_readiness"] == pytest.approx(
            comp._gex_readiness(gex), abs=1e-4
        )
