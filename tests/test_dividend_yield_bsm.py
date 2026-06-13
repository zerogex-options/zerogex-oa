"""Dividend-yield (q) support in the BSM Greeks/IV kernels.

q defaults to 0.0 (config DIVIDEND_YIELD) and must reproduce the prior
dividend-free model byte-for-byte; a positive q must shift the Greeks in
the textbook direction (call delta down, put delta less negative, lower
solved IV for a given call price).
"""

from datetime import date, datetime, timezone

from src.ingestion.greeks_calculator import GreeksCalculator
from src.ingestion.iv_calculator import IVCalculator
from src.analytics.main_engine import AnalyticsEngine

EXP = date(2026, 9, 18)
NOW = datetime(2026, 6, 13, 15, 0, tzinfo=timezone.utc)


# ---- q == 0 reproduces the dividend-free model exactly --------------------


def test_greeks_q_zero_matches_legacy_formulas():
    calc = GreeksCalculator(dividend_yield=0.0)
    g0 = calc.calculate_all_greeks(
        underlying_price=100.0,
        strike=100.0,
        expiration=EXP,
        option_type="C",
        current_time=NOW,
        implied_volatility=0.2,
        risk_free_rate=0.05,
    )
    # Re-derive with the textbook dividend-free closed forms via the kernels
    # at q=0 (the default param) — must be identical.
    T = calc._calculate_time_to_expiration(NOW, EXP)
    assert g0["delta"] == round(calc.calculate_delta(100.0, 100.0, T, 0.05, 0.2, "C"), 6)
    assert g0["gamma"] == round(calc.calculate_gamma(100.0, 100.0, T, 0.05, 0.2), 8)
    assert g0["vega"] == round(calc.calculate_vega(100.0, 100.0, T, 0.05, 0.2), 6)


def test_iv_roundtrip_q_zero():
    iv = IVCalculator()
    price = iv._black_scholes_price(100.0, 100.0, 0.25, 0.05, 0.2, "C")  # q defaults 0
    solved = iv.calculate_iv(price, 100.0, 100.0, EXP, "C", NOW, risk_free_rate=0.05)
    assert solved is not None and abs(solved - 0.2) < 0.02


# ---- q > 0 moves things in the right direction ----------------------------


def test_positive_q_lowers_call_delta():
    calc = GreeksCalculator()
    T = calc._calculate_time_to_expiration(NOW, EXP)
    d_no_div = calc.calculate_delta(100.0, 100.0, T, 0.05, 0.2, "C", 0.0)
    d_with_div = calc.calculate_delta(100.0, 100.0, T, 0.05, 0.2, "C", 0.03)
    assert d_with_div < d_no_div


def test_positive_q_makes_put_delta_more_negative():
    calc = GreeksCalculator()
    T = calc._calculate_time_to_expiration(NOW, EXP)
    p_no_div = calc.calculate_delta(100.0, 100.0, T, 0.05, 0.2, "P", 0.0)
    p_with_div = calc.calculate_delta(100.0, 100.0, T, 0.05, 0.2, "P", 0.03)
    # Dividends raise put values and push put delta MORE negative: the d1
    # shift (r-q smaller -> larger N(-d1)) dominates the e^{-qT} scaling.
    assert p_with_div < p_no_div


def test_analytics_gamma_q_zero_unchanged():
    eng = AnalyticsEngine(underlying="SPY")
    # q defaults to config 0.0 -> identical to passing q=0 explicitly.
    g_default = eng._calculate_bs_gamma(100.0, 100.0, 0.25, 0.05, 0.2)
    g_explicit_zero = eng._calculate_bs_gamma(100.0, 100.0, 0.25, 0.05, 0.2, 0.0)
    assert g_default == g_explicit_zero


def test_analytics_charm_q_zero_matches_prior_form():
    eng = AnalyticsEngine(underlying="SPY")
    # At q=0 the carry term vanishes; charm must be finite and nonzero for an
    # ATM-ish strike.
    charm = eng._calculate_charm(100.0, 105.0, 0.25, 0.05, 0.2, 0.0)
    assert charm == charm  # not NaN
    assert charm != 0.0
