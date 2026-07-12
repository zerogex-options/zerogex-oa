"""Phase 1 -- finite-difference charm & vanna in the Greeks engine.

Charm and vanna are computed by finite difference against the canonical BSM
delta (src.greeks_fd). This suite validates the FD values against the closed
forms (the analytic expressions are kept HERE as validation targets only, per
spec 5.2), asserts the behavioral facts that must hold regardless of formula
fiddling, exercises the near-expiry boundary, and proves the ingestion and
analytics layers are reconciled to the one shared kernel with no drift.
"""

import math
from datetime import datetime, timedelta

import pytz
from scipy.stats import norm

from src.greeks_fd import (
    CHARM_H_DAYS,
    VANNA_H_VOL,
    bsm_delta,
    dollar_charm_per_day,
    dollar_gamma_per_1pct,
    dollar_vanna_per_volpt,
    fd_charm,
    fd_vanna,
)
from src.ingestion.greeks_calculator import GreeksCalculator
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")

R = 0.05
SIGMA = 0.20


# --------------------------------------------------------------------------- #
# Closed-form reference expressions (spec 5.2) -- validation targets only.
# --------------------------------------------------------------------------- #
def ref_vanna(S, K, T, r, sigma, q=0.0):
    """Closed-form vanna: -e^{-qT} n(d1) d2 / sigma (per unit sigma)."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return -math.exp(-q * T) * norm.pdf(d1) * d2 / sigma


def ref_charm_per_day(S, K, T, r, sigma, q=0.0, option_type="C"):
    """Closed-form charm (d(delta)/dt), per day. Put = call - q e^{-qT}."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    disc_q = math.exp(-q * T)
    charm_call = q * disc_q * norm.cdf(d1) - disc_q * norm.pdf(d1) * (
        2 * (r - q) * T - d2 * sigma * sqrt_T
    ) / (2 * T * sigma * sqrt_T)
    charm = charm_call if option_type == "C" else charm_call - q * disc_q
    return charm / 365.0


# A grid of non-degenerate scenarios spanning moneyness and tenor.
_GRID = [
    (S, K, T)
    for S in (100.0,)
    for K in (85.0, 92.0, 100.0, 108.0, 116.0)
    for T in (7 / 365, 21 / 365, 45 / 365, 120 / 365)
]


# --------------------------------------------------------------------------- #
# Vanna: closed-form validation + behavioral facts
# --------------------------------------------------------------------------- #
def test_fd_vanna_small_step_matches_closed_form():
    # Method exactness: with a small step the central difference reproduces the
    # analytic vanna to tight relative tolerance -- this is the rigorous proof
    # that the finite difference computes the right derivative with the right
    # sign (spec 5.2). Skip points where vanna ~ 0 (relative error is
    # meaningless there and the absolute contribution is nil).
    for S, K, T in _GRID:
        for ot in ("C", "P"):
            ref = ref_vanna(S, K, T, R, SIGMA)
            if abs(ref) < 1e-6:
                continue
            fd = fd_vanna(S, K, T, R, SIGMA, ot, 0.0, h_vol=1e-4)
            assert math.isclose(fd, ref, rel_tol=2e-3, abs_tol=1e-8), (S, K, T, ot, fd, ref)


def test_fd_vanna_production_step_within_curvature_band():
    # The production one-vol-point central difference sits within the O(h^2)
    # curvature band of the instantaneous greek: <= 2.5e-3 per unit sigma
    # (under ~3% of ATM-scale vanna ~ 0.086). Sign and unit are unchanged; this
    # is the only difference the FD introduces vs the prior closed form.
    for S, K, T in _GRID:
        fd = fd_vanna(S, K, T, R, SIGMA, "C")
        ref = ref_vanna(S, K, T, R, SIGMA)
        assert abs(fd - ref) <= 2.5e-3, (S, K, T, fd, ref)


def test_vanna_identical_for_calls_and_puts():
    for S, K, T in _GRID:
        assert math.isclose(
            fd_vanna(S, K, T, R, SIGMA, "C"), fd_vanna(S, K, T, R, SIGMA, "P"), abs_tol=1e-12
        )


def test_otm_put_vanna_negative():
    # OTM put: strike below spot. Rising vol pushes put delta more negative,
    # so vanna < 0. This is the mechanical fuel behind the vol-crush grind.
    assert fd_vanna(100.0, 90.0, 30 / 365, R, SIGMA, "P") < 0.0
    assert fd_vanna(100.0, 85.0, 14 / 365, R, SIGMA, "P") < 0.0


# --------------------------------------------------------------------------- #
# Charm: closed-form validation + behavioral facts
# --------------------------------------------------------------------------- #
def test_fd_charm_matches_closed_form_bulk():
    # Away from expiry the one-calendar-day step tracks the instantaneous
    # closed form closely. Restrict to T >= 21d where the day-step truncation
    # is small; use a combined rel/abs tolerance so near-ATM (charm ~ 0) is not
    # judged on relative error alone.
    for S, K, T in _GRID:
        if T < 21 / 365:
            continue
        for ot in ("C", "P"):
            fd = fd_charm(S, K, T, R, SIGMA, ot)
            ref = ref_charm_per_day(S, K, T, R, SIGMA, 0.0, ot)
            assert abs(fd - ref) <= 0.06 * abs(ref) + 5e-5, (S, K, T, ot, fd, ref)


def test_fd_charm_is_exact_one_day_difference():
    # Exactness: away from the final day, fd_charm IS the spec formula
    # delta(T - 1 day) - delta(T) -- an exact equality, not an approximation.
    for S, K, T in _GRID:
        if T <= CHARM_H_DAYS:
            continue
        for ot in ("C", "P"):
            expected = bsm_delta(S, K, T - CHARM_H_DAYS, R, SIGMA, ot) - bsm_delta(
                S, K, T, R, SIGMA, ot
            )
            assert fd_charm(S, K, T, R, SIGMA, ot) == expected, (S, K, T, ot)


def test_otm_call_charm_negative():
    # OTM call delta must decay toward 0 as expiry approaches -> charm < 0.
    assert fd_charm(100.0, 110.0, 30 / 365, R, SIGMA, "C") < 0.0


def test_itm_call_charm_positive():
    # ITM call delta must climb toward 1 as expiry approaches -> charm > 0.
    assert fd_charm(100.0, 90.0, 30 / 365, R, SIGMA, "C") > 0.0


def test_atm_charm_near_zero():
    # ATM delta stays pinned near 0.5 -> charm ~ 0 (small relative to OTM/ITM).
    atm = abs(fd_charm(100.0, 100.0, 30 / 365, R, SIGMA, "C"))
    otm = abs(fd_charm(100.0, 110.0, 30 / 365, R, SIGMA, "C"))
    assert atm < otm
    assert atm < 5e-3


def test_charm_explodes_into_the_close():
    # The single most important economic fact: |charm| blows up as tau -> 0 for
    # near-but-not-at-the-money strikes. A slightly-OTM call must resolve to 0
    # by the close, and most of that journey is in the final hours.
    S, K = 100.0, 100.75
    far = abs(fd_charm(S, K, 30 / 365, R, SIGMA, "C"))
    near = abs(fd_charm(S, K, 2 / 24 / 365, R, SIGMA, "C"))  # 2 hours to expiry
    assert near > 10 * far


def test_charm_put_call_differ_by_dividend_carry():
    # charm_put = charm_call - q e^{-qT} (per year); at q=0 they coincide.
    S, K, T, q = 100.0, 100.0, 30 / 365, 0.03
    lhs = fd_charm(S, K, T, R, SIGMA, "P", q) - fd_charm(S, K, T, R, SIGMA, "C", q)
    rhs = -q * math.exp(-q * T) / 365.0
    assert math.isclose(lhs, rhs, rel_tol=1e-3, abs_tol=1e-8)

    # And at q=0, call and put charm are identical.
    assert math.isclose(
        fd_charm(S, K, T, R, SIGMA, "C", 0.0), fd_charm(S, K, T, R, SIGMA, "P", 0.0), abs_tol=1e-12
    )


# --------------------------------------------------------------------------- #
# Boundary + degenerate inputs
# --------------------------------------------------------------------------- #
def test_charm_final_day_stays_finite_and_signed():
    # Well inside the final day: T - 1 day would be negative. The boundary guard
    # must keep it finite, correctly signed, and NOT evaluate delta at T<=0.
    S, K = 100.0, 100.75  # slightly OTM call -> charm < 0
    for hours in (6.0, 3.0, 1.0, 0.25):
        T = hours / 24 / 365
        v = fd_charm(S, K, T, R, SIGMA, "C")
        assert math.isfinite(v)
        assert v < 0.0


def test_degenerate_inputs_return_zero():
    for fn in (fd_charm, fd_vanna):
        assert fn(0.0, 100.0, 0.1, R, SIGMA, "C") == 0.0  # S<=0
        assert fn(100.0, 0.0, 0.1, R, SIGMA, "C") == 0.0  # K<=0
        assert fn(100.0, 100.0, 0.0, R, SIGMA, "C") == 0.0  # T<=0
        assert fn(100.0, 100.0, 0.1, R, 0.0, "C") == 0.0  # sigma<=0
    assert bsm_delta(100.0, 100.0, 0.0, R, SIGMA, "C") == 0.0


def test_spec_step_constants():
    assert CHARM_H_DAYS == 1.0 / 365.0
    assert VANNA_H_VOL == 0.01


# --------------------------------------------------------------------------- #
# Dollar conversion (spec 5.3)
# --------------------------------------------------------------------------- #
def test_dollar_gamma_matches_existing_gex_formula():
    gamma, oi, S = 0.012, 1500, 100.0
    assert math.isclose(
        dollar_gamma_per_1pct(gamma, oi, S), gamma * oi * 100 * S * S * 0.01, rel_tol=0, abs_tol=0
    )


def test_dollar_charm_and_vanna_formulas():
    oi, S = 1500, 100.0
    assert math.isclose(dollar_charm_per_day(0.001, oi, S), 0.001 * oi * 100 * S)
    # vanna dollars take the per-unit-sigma value and apply the 0.01 vol-point.
    assert math.isclose(dollar_vanna_per_volpt(-0.08, oi, S), -0.08 * oi * 100 * S * 0.01)


# --------------------------------------------------------------------------- #
# Ingestion GreeksCalculator emits charm/vanna
# --------------------------------------------------------------------------- #
def _calc():
    return GreeksCalculator()


def test_all_greeks_emits_charm_and_vanna():
    calc = _calc()
    now = ET.localize(datetime(2026, 6, 1, 12, 0))
    exp = (now + timedelta(days=30)).date()
    g = calc.calculate_all_greeks(100.0, 100.0, exp, "C", now, implied_volatility=0.2)
    assert set(g) == {"delta", "gamma", "theta", "vega", "charm", "vanna"}
    assert all(math.isfinite(v) for v in g.values())


def test_calculator_methods_delegate_to_kernel():
    calc = _calc()
    S, K, T = 100.0, 105.0, 30 / 365
    assert calc.calculate_vanna(S, K, T, R, SIGMA, "C", 0.0) == fd_vanna(
        S, K, T, R, SIGMA, "C", 0.0
    )
    assert calc.calculate_charm(S, K, T, R, SIGMA, "P", 0.0) == fd_charm(
        S, K, T, R, SIGMA, "P", 0.0
    )
    # delta is also single-sourced through the shared kernel.
    assert calc.calculate_delta(S, K, T, R, SIGMA, "C", 0.0) == bsm_delta(
        S, K, T, R, SIGMA, "C", 0.0
    )


def test_degenerate_all_greeks_dict_has_charm_vanna_zero():
    calc = _calc()
    now = ET.localize(datetime(2026, 6, 1, 12, 0))
    exp = (now + timedelta(days=30)).date()
    g = calc.calculate_all_greeks(-1.0, 100.0, exp, "C", now, implied_volatility=0.2)  # bad spot
    assert g["charm"] == 0.0 and g["vanna"] == 0.0


def test_enrich_missing_field_sets_charm_vanna_none():
    calc = _calc()
    now = ET.localize(datetime(2026, 6, 1, 12, 0))
    option_data = {  # no 'strike' -> None-Greeks path
        "option_symbol": "SPY 260701C100",
        "timestamp": now,
        "underlying": "SPY",
        "expiration": (now + timedelta(days=30)).date(),
        "option_type": "C",
        "open_interest": 100,
    }
    out = calc.enrich_option_data(option_data, 100.0)
    assert out["charm"] is None and out["vanna"] is None


# --------------------------------------------------------------------------- #
# Reconciliation: analytics layer delegates to the same kernel, no drift.
# --------------------------------------------------------------------------- #
def test_analytics_methods_delegate_to_shared_kernel():
    # _calculate_vanna/_calculate_charm do not touch self, so pass None.
    for S, K, T in _GRID:
        assert AnalyticsEngine._calculate_vanna(None, S, K, T, R, SIGMA, 0.0) == fd_vanna(
            S, K, T, R, SIGMA, "C", 0.0
        )
        assert AnalyticsEngine._calculate_charm(
            None, S, K, T, R, SIGMA, 0.0, option_type="P"
        ) == fd_charm(S, K, T, R, SIGMA, "P", 0.0)


def test_analytics_no_drift_vs_prior_closed_form_at_q0():
    # At the configured default q=0, the reconciled analytics aggregate matches
    # the prior closed forms away from expiry -> the switch to FD does not move
    # production values in the bulk. (Near expiry the FD is intentionally the
    # realized one-day change; that is out of scope for this no-drift check.)
    for S, K, T in _GRID:
        if T < 21 / 365:
            continue
        # Vanna: within the O(h^2) one-vol-point curvature band (sign/unit kept).
        v_new = AnalyticsEngine._calculate_vanna(None, S, K, T, R, SIGMA, 0.0)
        assert abs(v_new - ref_vanna(S, K, T, R, SIGMA, 0.0)) <= 2.5e-3
        # Charm: one-calendar-day change tracks the instantaneous closed form
        # away from expiry.
        c_new = AnalyticsEngine._calculate_charm(None, S, K, T, R, SIGMA, 0.0, option_type="C")
        c_ref = ref_charm_per_day(S, K, T, R, SIGMA, 0.0, "C")
        assert abs(c_new - c_ref) <= 0.06 * abs(c_ref) + 5e-5
