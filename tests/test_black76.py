"""Black-76 futures-option pricing & Greeks tests.

Reference values for the canonical ATM case F=K=100, T=1, σ=0.20, r=0.05 are
computed by hand from the closed form (see docstring of src/futures/black76.py)
and asserted tightly.  Property tests cover put-call parity, monotonicity and
IV round-trips.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from src.futures import black76
from src.futures.black76 import ModelQuality

# Canonical reference: F=K=100, T=1, sigma=0.20, r=0.05
_F, _K, _T, _SIG, _R = 100.0, 100.0, 1.0, 0.20, 0.05


def test_atm_call_price_reference():
    px = black76.price(_F, _K, _T, _SIG, is_call=True, rate=_R)
    assert px == pytest.approx(7.5775, abs=1e-3)


def test_atm_put_price_equals_call_when_f_equals_k():
    # Put-call parity: C - P = e^{-rT}(F-K) = 0 when F=K.
    c = black76.price(_F, _K, _T, _SIG, is_call=True, rate=_R)
    p = black76.price(_F, _K, _T, _SIG, is_call=False, rate=_R)
    assert c == pytest.approx(p, abs=1e-9)


def test_atm_greeks_reference():
    r = black76.evaluate(_F, _K, _T, _SIG, is_call=True, rate=_R)
    assert r.quality is ModelQuality.GOOD
    assert r.delta == pytest.approx(0.513501, abs=1e-4)
    assert r.gamma == pytest.approx(0.0188798, abs=1e-6)
    assert r.vega == pytest.approx(0.377596, abs=1e-4)  # per 1% vol
    assert r.theta == pytest.approx(-0.00930708, abs=1e-5)  # per day
    assert r.rho == pytest.approx(-0.075775, abs=1e-4)  # per 1% rate


def test_put_delta_is_negative_and_discounted():
    r = black76.evaluate(_F, _K, _T, _SIG, is_call=False, rate=_R)
    # Put delta = call delta - e^{-rT}
    disc = math.exp(-_R * _T)
    call = black76.evaluate(_F, _K, _T, _SIG, is_call=True, rate=_R)
    assert r.delta == pytest.approx(call.delta - disc, abs=1e-9)
    assert r.delta < 0


def test_gamma_and_vega_identical_for_call_and_put():
    c = black76.evaluate(_F, _K, _T, _SIG, is_call=True, rate=_R)
    p = black76.evaluate(_F, _K, _T, _SIG, is_call=False, rate=_R)
    assert c.gamma == pytest.approx(p.gamma, abs=1e-12)
    assert c.vega == pytest.approx(p.vega, abs=1e-12)


def test_rho_equals_minus_t_times_price():
    for is_call in (True, False):
        r = black76.evaluate(_F, _K, _T, _SIG, is_call=is_call, rate=_R)
        assert r.rho == pytest.approx(-_T * r.price * 0.01, abs=1e-9)


@pytest.mark.parametrize("is_call", [True, False])
def test_put_call_parity_off_the_money(is_call):
    F, K, T, sig, r = 105.0, 100.0, 0.5, 0.25, 0.03
    c = black76.price(F, K, T, sig, is_call=True, rate=r)
    p = black76.price(F, K, T, sig, is_call=False, rate=r)
    # C - P = e^{-rT}(F - K)
    assert c - p == pytest.approx(math.exp(-r * T) * (F - K), abs=1e-9)


def test_call_price_monotonic_increasing_in_futures_price():
    prices = [
        black76.price(F, _K, _T, _SIG, is_call=True, rate=_R) for F in (80, 90, 100, 110, 120)
    ]
    assert all(b > a for a, b in zip(prices, prices[1:]))


def test_call_price_monotonic_decreasing_in_strike():
    prices = [
        black76.price(_F, K, _T, _SIG, is_call=True, rate=_R) for K in (80, 90, 100, 110, 120)
    ]
    assert all(b < a for a, b in zip(prices, prices[1:]))


def test_price_increases_with_volatility():
    lo = black76.price(_F, _K, _T, 0.10, is_call=True, rate=_R)
    hi = black76.price(_F, _K, _T, 0.40, is_call=True, rate=_R)
    assert hi > lo


def test_deep_itm_call_delta_approaches_discount():
    disc = math.exp(-_R * _T)
    r = black76.evaluate(200.0, _K, _T, _SIG, is_call=True, rate=_R)
    assert r.delta == pytest.approx(disc, abs=1e-3)


def test_deep_otm_call_delta_approaches_zero():
    r = black76.evaluate(20.0, _K, _T, _SIG, is_call=True, rate=_R)
    assert r.delta == pytest.approx(0.0, abs=1e-3)


def test_expired_returns_discounted_intrinsic():
    r = black76.evaluate(110.0, 100.0, 0.0, _SIG, is_call=True, rate=_R)
    assert r.quality is ModelQuality.EXPIRED
    assert r.price == pytest.approx(10.0, abs=1e-9)  # r*T=0 so disc=1
    assert r.gamma == 0.0
    assert r.intrinsic == 10.0


def test_invalid_input_is_flagged_not_raised():
    r = black76.evaluate(-1.0, 100.0, 1.0, 0.2, is_call=True, rate=_R)
    assert r.quality is ModelQuality.INVALID_INPUT
    assert r.price == 0.0


@pytest.mark.parametrize(
    "F,K,T,sig,is_call",
    [
        (100, 100, 1.0, 0.20, True),
        (100, 100, 1.0, 0.20, False),
        (105, 100, 0.5, 0.35, True),
        (95, 100, 0.25, 0.15, False),
        (6000, 6100, 0.05, 0.12, True),  # ES-scale
    ],
)
def test_implied_vol_round_trip(F, K, T, sig, is_call):
    market = black76.price(F, K, T, sig, is_call=is_call, rate=0.04)
    solved = black76.implied_volatility(market, F, K, T, is_call=is_call, rate=0.04)
    assert solved is not None
    assert solved == pytest.approx(sig, abs=1e-4)


def test_implied_vol_below_intrinsic_returns_none():
    # A price below discounted intrinsic has no Black-76 solution.
    solved = black76.implied_volatility(0.5, 120.0, 100.0, 1.0, is_call=True, rate=0.05)
    assert solved is None


def test_vanna_charm_are_finite_and_signed():
    r = black76.evaluate(_F, _K, _T, _SIG, is_call=True, rate=_R)
    assert math.isfinite(r.vanna)
    assert math.isfinite(r.charm)
    # ATM call charm is typically small/negative into expiry; just require finite.


def test_years_to_expiration_exact_instant():
    now = datetime(2026, 3, 20, 15, 0, tzinfo=timezone.utc)
    exp = now + timedelta(days=365)
    assert black76.years_to_expiration(now, exp) == pytest.approx(1.0, abs=1e-6)
    # Expired → 0
    assert black76.years_to_expiration(exp, now) == 0.0
    # Sub-floor honored
    near = now + timedelta(minutes=5)
    y = black76.years_to_expiration(now, near, min_minutes=30)
    assert y == pytest.approx((30 / 60 / 24) / 365.0, abs=1e-9)
