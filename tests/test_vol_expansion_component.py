"""Unit tests for VolExpansionComponent — continuous spectrum model with
expansion + direction + magnitude + expected-move split.

Direction is driven by vol-normalized momentum (z-score of the 5-bar
return over per-bar realized sigma, scaled by sqrt(5)).  To exercise
deterministic clipping we build a price series whose last return
dominates realized sigma, so the z-score clips at ±_DIRECTION_Z_NORM.
"""

from datetime import datetime, timezone

import math

import pytest

from src.signals.advanced.vol_expansion import (
    VolExpansionComponent,
    _GEX_NORM,
    _GEX_FLOOR,
    _DIRECTION_Z_NORM,
)
from src.signals.components.base import MarketContext


def _ctx(**kwargs) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=550.0,
        net_gex=-_GEX_NORM,  # full readiness = 1.0
        gamma_flip=545.0,
        put_call_ratio=1.0,
        max_pain=548.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[550.0] * 5,  # flat price by default
        iv_rank=None,
    )
    defaults.update(kwargs)
    return MarketContext(**defaults)


def _closes_with_final_move(base: float, pct: float) -> list[float]:
    """Return 12 closes: 11 flat then one step.  Guarantees realized_sigma
    is the stdev of a single non-zero log-return, so z = sqrt(5)."""
    return [base] * 11 + [base * (1 + pct)]


comp = VolExpansionComponent()


# ===========================================================================
# expansion()  — 0 to 100, GEX-driven
# ===========================================================================


def test_expansion_deeply_negative_gex():
    ctx = _ctx(net_gex=-_GEX_NORM)
    assert comp.expansion(ctx) == pytest.approx(100.0)


def test_expansion_zero_gex():
    ctx = _ctx(net_gex=0.0)
    expected = (_GEX_FLOOR + 1.0) / 2.0 * 100
    assert comp.expansion(ctx) == pytest.approx(expected)


def test_expansion_deeply_positive_gex():
    ctx = _ctx(net_gex=_GEX_NORM * 10)
    assert comp.expansion(ctx) == pytest.approx(_GEX_FLOOR * 100)


def test_expansion_never_zero():
    for gex in [0, 1e8, 5e8, 1e9, 1e10]:
        assert comp.expansion(_ctx(net_gex=gex)) > 0


def test_expansion_monotonically_decreases_with_positive_gex():
    vals = [comp.expansion(_ctx(net_gex=g)) for g in [-_GEX_NORM, 0, _GEX_NORM]]
    for i in range(len(vals) - 1):
        assert vals[i] >= vals[i + 1]


def test_expansion_independent_of_momentum():
    base = 550.0
    ctx_flat = _ctx(recent_closes=[base] * 5)
    ctx_up = _ctx(recent_closes=_closes_with_final_move(base, 0.01))
    ctx_down = _ctx(recent_closes=_closes_with_final_move(base, -0.01))
    assert comp.expansion(ctx_flat) == comp.expansion(ctx_up) == comp.expansion(ctx_down)


# ===========================================================================
# direction_score()  — -100 to +100, vol-normalized momentum
# ===========================================================================


def test_direction_flat_price():
    ctx = _ctx()
    assert comp.direction_score(ctx) == pytest.approx(0.0)


def test_direction_rising_price_saturates():
    """Any rise dominates sigma → z-score ≥ _DIRECTION_Z_NORM → direction == +100."""
    ctx = _ctx(recent_closes=_closes_with_final_move(550.0, 0.005))
    assert comp.direction_score(ctx) == pytest.approx(100.0)


def test_direction_falling_price_saturates():
    ctx = _ctx(recent_closes=_closes_with_final_move(550.0, -0.005))
    assert comp.direction_score(ctx) == pytest.approx(-100.0)


def test_direction_clamped_large_move():
    ctx = _ctx(recent_closes=_closes_with_final_move(550.0, -0.10))
    assert comp.direction_score(ctx) == pytest.approx(-100.0)


def test_direction_independent_of_gex():
    closes = _closes_with_final_move(550.0, 0.003)
    ctx_neg = _ctx(net_gex=-_GEX_NORM, recent_closes=closes)
    ctx_pos = _ctx(net_gex=_GEX_NORM, recent_closes=closes)
    assert comp.direction_score(ctx_neg) == comp.direction_score(ctx_pos)


def test_direction_insufficient_closes():
    ctx = _ctx(recent_closes=[550.0])
    assert comp.direction_score(ctx) == 0.0


def test_direction_zero_when_sigma_is_zero():
    """Fully flat history → no realized vol → direction = 0 regardless of final bar."""
    ctx = _ctx(recent_closes=[550.0] * 5)
    assert comp.direction_score(ctx) == pytest.approx(0.0)


# ===========================================================================
# magnitude()  — unsigned impulse amplitude
# ===========================================================================


def test_magnitude_zero_on_flat_price():
    assert comp.magnitude(_ctx()) == pytest.approx(0.0)


def test_magnitude_saturates_with_big_move_and_neg_gex():
    ctx = _ctx(recent_closes=_closes_with_final_move(550.0, 0.01))
    # readiness = 1.0 at -_GEX_NORM, |momentum| clamped to 1.0
    assert comp.magnitude(ctx) == pytest.approx(100.0)


def test_magnitude_is_unsigned():
    up = _ctx(recent_closes=_closes_with_final_move(550.0, 0.005))
    dn = _ctx(recent_closes=_closes_with_final_move(550.0, -0.005))
    assert comp.magnitude(up) == comp.magnitude(dn)


# ===========================================================================
# expected_5min_move_bps()
# ===========================================================================


def test_expected_move_none_when_no_history():
    ctx = _ctx(recent_closes=[550.0])
    assert comp.expected_5min_move_bps(ctx) is None


def test_expected_move_sign_matches_direction():
    up = _ctx(recent_closes=_closes_with_final_move(550.0, 0.005))
    dn = _ctx(recent_closes=_closes_with_final_move(550.0, -0.005))
    up_move = comp.expected_5min_move_bps(up)
    dn_move = comp.expected_5min_move_bps(dn)
    assert up_move is not None and up_move > 0
    assert dn_move is not None and dn_move < 0


# ===========================================================================
# compute()  — composite [-1, +1] for ScoringEngine
# ===========================================================================


def test_compute_full_negative_gex_flat_price():
    assert comp.compute(_ctx()) == pytest.approx(0.0)


def test_compute_positive_gex_flat_price():
    assert comp.compute(_ctx(net_gex=_GEX_NORM * 10)) == pytest.approx(0.0)


def test_compute_with_momentum_matches_readiness_scaling():
    for gex in [0, 1e8, 5e8, 1e9, 1e10]:
        ctx = _ctx(net_gex=gex, recent_closes=_closes_with_final_move(550.0, 0.005))
        score = comp.compute(ctx)
        expected = comp._gex_readiness(gex)
        assert score == pytest.approx(
            expected, abs=1e-6
        ), f"Expected readiness scaling for gex={gex}"


def test_compute_negative_gex_bearish_momentum():
    ctx = _ctx(recent_closes=_closes_with_final_move(550.0, -0.005))
    assert comp.compute(ctx) == pytest.approx(-1.0)


def test_compute_positive_gex_bearish_momentum():
    ctx = _ctx(
        net_gex=_GEX_NORM * 10,
        recent_closes=_closes_with_final_move(550.0, -0.005),
    )
    assert comp.compute(ctx) == pytest.approx(-_GEX_FLOOR, abs=1e-6)


def test_compute_insufficient_closes():
    ctx = _ctx(recent_closes=[549.0, 550.0])
    assert comp.compute(ctx) == pytest.approx(0.0)


def test_compute_always_in_bounds():
    for net_gex in [-_GEX_NORM * 2, -_GEX_NORM, 0, _GEX_NORM]:
        for delta in [-0.02, -0.005, 0, 0.005, 0.02]:
            ctx = _ctx(
                net_gex=net_gex,
                recent_closes=_closes_with_final_move(550.0, delta),
            )
            score = comp.compute(ctx)
            assert -1.0 <= score <= 1.0, f"Out of bounds: {score} (gex={net_gex}, delta={delta})"


# ===========================================================================
# context_values
# ===========================================================================


def test_context_values_keys():
    ctx = _ctx()
    cv = comp.context_values(ctx)
    expected = {
        "net_gex",
        "gex_regime",
        "expansion",
        "direction",
        "magnitude",
        "expected_5min_move_bps",
        "gex_readiness",
        "pct_change_5bar",
        "momentum_z",
        "momentum",
        "realized_sigma_bar",
    }
    assert set(cv.keys()) == expected


def test_context_values_expansion_matches_method():
    for gex in [-_GEX_NORM, 0, _GEX_NORM]:
        ctx = _ctx(net_gex=gex)
        cv = comp.context_values(ctx)
        assert cv["expansion"] == pytest.approx(comp.expansion(ctx))


def test_context_values_direction_matches_method():
    for delta in [-0.005, 0, 0.003]:
        ctx = _ctx(recent_closes=_closes_with_final_move(550.0, delta))
        cv = comp.context_values(ctx)
        assert cv["direction"] == pytest.approx(comp.direction_score(ctx))


def test_context_values_momentum_none_when_insufficient_closes():
    ctx = _ctx(recent_closes=[550.0])
    cv = comp.context_values(ctx)
    assert cv["pct_change_5bar"] is None
    assert cv["momentum"] is None
    assert cv["momentum_z"] is None


def test_context_values_positive_gex_regime():
    ctx = _ctx(net_gex=500_000_000.0)
    cv = comp.context_values(ctx)
    assert cv["gex_regime"] == "positive"
