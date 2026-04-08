"""Unit tests for VolExpansionComponent — continuous spectrum model with
expansion + direction split."""
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


# ===========================================================================
# expansion()  — 0 to 100, GEX-driven
# ===========================================================================

def test_expansion_deeply_negative_gex():
    """Deeply negative GEX → expansion approaches 100."""
    ctx = _ctx(net_gex=-_GEX_NORM)
    assert comp.expansion(ctx) == pytest.approx(100.0)


def test_expansion_zero_gex():
    """Zero GEX → moderate expansion."""
    ctx = _ctx(net_gex=0.0)
    expected = (_GEX_FLOOR + 1.0) / 2.0 * 100
    assert comp.expansion(ctx) == pytest.approx(expected)


def test_expansion_deeply_positive_gex():
    """Deeply positive GEX → expansion approaches floor."""
    ctx = _ctx(net_gex=_GEX_NORM * 10)
    assert comp.expansion(ctx) == pytest.approx(_GEX_FLOOR * 100)


def test_expansion_never_zero():
    """Expansion should never be zero for any GEX value."""
    for gex in [0, 1e8, 5e8, 1e9, 1e10]:
        assert comp.expansion(_ctx(net_gex=gex)) > 0


def test_expansion_monotonically_decreases_with_positive_gex():
    vals = [comp.expansion(_ctx(net_gex=g)) for g in [-_GEX_NORM, 0, _GEX_NORM]]
    for i in range(len(vals) - 1):
        assert vals[i] >= vals[i + 1]


def test_expansion_independent_of_momentum():
    """Expansion depends only on GEX, not on price movement."""
    base = 550.0
    ctx_flat = _ctx(recent_closes=[base] * 5)
    ctx_up = _ctx(recent_closes=[base] * 4 + [base * 1.01])
    ctx_down = _ctx(recent_closes=[base] * 4 + [base * 0.99])
    assert comp.expansion(ctx_flat) == comp.expansion(ctx_up) == comp.expansion(ctx_down)


# ===========================================================================
# direction_score()  — -100 to +100, momentum-driven
# ===========================================================================

def test_direction_flat_price():
    ctx = _ctx()
    assert comp.direction_score(ctx) == pytest.approx(0.0)


def test_direction_rising_price():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 + _MOMENTUM_NORM)])
    assert comp.direction_score(ctx) == pytest.approx(100.0)


def test_direction_falling_price():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM)])
    assert comp.direction_score(ctx) == pytest.approx(-100.0)


def test_direction_clamped_large_move():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * 0.90])  # -10%
    assert comp.direction_score(ctx) == pytest.approx(-100.0)


def test_direction_independent_of_gex():
    """Direction depends only on momentum, not on GEX."""
    base = 550.0
    closes = [base] * 4 + [base * 1.003]
    ctx_neg = _ctx(net_gex=-_GEX_NORM, recent_closes=closes)
    ctx_pos = _ctx(net_gex=_GEX_NORM, recent_closes=closes)
    assert comp.direction_score(ctx_neg) == comp.direction_score(ctx_pos)


def test_direction_insufficient_closes():
    ctx = _ctx(recent_closes=[550.0])
    assert comp.direction_score(ctx) == 0.0


# ===========================================================================
# compute()  — composite [-1, +1] for ScoringEngine
# ===========================================================================

def test_compute_full_negative_gex_flat_price():
    ctx = _ctx()
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_compute_positive_gex_flat_price():
    ctx = _ctx(net_gex=_GEX_NORM * 10)
    assert comp.compute(ctx) == pytest.approx(_GEX_FLOOR)


def test_compute_positive_gex_never_zero():
    for gex in [0, 1e8, 5e8, 1e9, 1e10]:
        score = comp.compute(_ctx(net_gex=gex))
        assert score > 0, f"compute() should be > 0 for gex={gex}, got {score}"


def test_compute_negative_gex_bearish_momentum():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM)])
    assert comp.compute(ctx) == pytest.approx(-1.0)


def test_compute_positive_gex_bearish_momentum():
    """Deeply positive GEX + falling hard → small negative score."""
    base = 550.0
    ctx = _ctx(
        net_gex=_GEX_NORM * 10,
        recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM)],
    )
    assert comp.compute(ctx) == pytest.approx(-_GEX_FLOOR)


def test_compute_half_bearish_momentum_returns_zero():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM / 2)])
    assert comp.compute(ctx) == pytest.approx(0.0, abs=1e-6)


def test_compute_quarter_bearish_momentum():
    base = 550.0
    ctx = _ctx(recent_closes=[base] * 4 + [base * (1 - _MOMENTUM_NORM / 4)])
    assert comp.compute(ctx) == pytest.approx(0.5, abs=1e-4)


def test_compute_insufficient_closes():
    ctx = _ctx(recent_closes=[549.0, 550.0])
    assert comp.compute(ctx) == pytest.approx(1.0)


def test_compute_always_in_bounds():
    for net_gex in [-_GEX_NORM * 2, -_GEX_NORM, 0, _GEX_NORM]:
        for delta in [-0.02, -0.005, 0, 0.005, 0.02]:
            base = 550.0
            ctx = _ctx(
                net_gex=net_gex,
                recent_closes=[base] * 4 + [base * (1 + delta)],
            )
            score = comp.compute(ctx)
            assert -1.0 <= score <= 1.0, f"Out of bounds: {score} (gex={net_gex}, delta={delta})"


# ===========================================================================
# context_values
# ===========================================================================

def test_context_values_keys():
    ctx = _ctx()
    cv = comp.context_values(ctx)
    expected = {"net_gex", "gex_regime", "expansion", "direction",
                "gex_readiness", "pct_change_5bar", "momentum"}
    assert set(cv.keys()) == expected


def test_context_values_expansion_matches_method():
    for gex in [-_GEX_NORM, 0, _GEX_NORM]:
        ctx = _ctx(net_gex=gex)
        cv = comp.context_values(ctx)
        assert cv["expansion"] == pytest.approx(comp.expansion(ctx))


def test_context_values_direction_matches_method():
    base = 550.0
    for delta in [-0.005, 0, 0.003]:
        ctx = _ctx(recent_closes=[base] * 4 + [base * (1 + delta)])
        cv = comp.context_values(ctx)
        assert cv["direction"] == pytest.approx(comp.direction_score(ctx))


def test_context_values_momentum_none_when_insufficient_closes():
    ctx = _ctx(recent_closes=[550.0])
    cv = comp.context_values(ctx)
    assert cv["pct_change_5bar"] is None
    assert cv["momentum"] is None


def test_context_values_positive_gex_regime():
    ctx = _ctx(net_gex=500_000_000.0)
    cv = comp.context_values(ctx)
    assert cv["gex_regime"] == "positive"
