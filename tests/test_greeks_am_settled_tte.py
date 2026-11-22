"""Ingestion Greeks must use the per-contract settlement time.

SPX AM-settled monthly options settle at the 09:30 ET SOQ, not 16:00 ET.
On expiration morning the stored gamma/theta must reflect ~0 time
remaining, not the ~6.5h of phantom time value the 16:00 default carried.
SPXW (weekly, PM-settled) shares the $SPX.X underlying and must keep the
16:00 anchor.
"""

from datetime import date, datetime

import pytz

from src.ingestion.greeks_calculator import GreeksCalculator

ET = pytz.timezone("US/Eastern")


def _calc():
    return GreeksCalculator()


def test_settlement_close_time_spx_am_monthly():
    calc = _calc()
    third_friday = date(2026, 6, 19)  # 3rd Friday of June 2026
    assert calc._settlement_close_time("SPX", "SPX 260619C5000", third_friday) == "09:30:00"


def test_settlement_close_time_spxw_is_pm():
    calc = _calc()
    third_friday = date(2026, 6, 19)
    assert calc._settlement_close_time("SPX", "SPXW 260619C5000", third_friday) == "16:00:00"


def test_settlement_close_time_spy_is_pm():
    calc = _calc()
    assert calc._settlement_close_time("SPY", "SPY 260619C500", date(2026, 6, 19)) == "16:00:00"


def test_am_settled_gamma_decays_by_settlement():
    calc = _calc()
    exp = date(2026, 6, 19)
    # 09:15 ET on expiration morning, BEFORE the 09:30 SOQ: the AM-settled
    # contract has ~15 min of life (floored to the 30-min TTE floor) while
    # the PM (SPXW) view still sees ~6.75h. The ATM 1/sqrt(T) gamma is
    # therefore far larger for the (tiny-T) AM contract. (After 09:30 the
    # AM contract is expired and gamma is 0 per the expired-TTE fix; this
    # case pins the pre-settlement divergence directly.)
    now = ET.localize(datetime(2026, 6, 19, 9, 15))
    am = calc.calculate_all_greeks(
        underlying_price=5000.0,
        strike=5000.0,
        expiration=exp,
        option_type="C",
        current_time=now,
        implied_volatility=0.15,
        underlying_symbol="SPX",
        option_symbol="SPX 260619C5000",
    )
    pm = calc.calculate_all_greeks(
        underlying_price=5000.0,
        strike=5000.0,
        expiration=exp,
        option_type="C",
        current_time=now,
        implied_volatility=0.15,
        underlying_symbol="SPX",
        option_symbol="SPXW 260619C5000",
    )
    # AM-settled gamma uses the 30-min TTE floor; PM-settled still has ~6.75h.
    # The ATM 1/sqrt(T) gamma is therefore strictly larger for the
    # (tiny-T) AM contract, and both must be positive (alive).
    assert am["gamma"] > 0.0
    assert pm["gamma"] > 0.0
    assert am["gamma"] > pm["gamma"]


def test_am_settled_iv_is_solved_to_the_same_settlement_as_its_greeks():
    """The IV solver used to fit every contract against expiry at 16:00 while
    the Greeks used 09:30 for an AM-settled monthly, so the IV came out low
    (Thursday 10:00 before the Dec quarterly: 0.123 for a true 0.14) and the
    gamma computed from it high. Priced at 0.14 to the 09:30 settlement, the
    contract must solve back to 0.14."""
    from src.ingestion.iv_calculator import IVCalculator
    from src.market_calendar import calculate_time_to_expiration

    exp = date(2026, 12, 18)  # third Friday: the SPX monthly settles AM
    now = ET.localize(datetime(2026, 12, 17, 10, 0))
    spot, strike, sigma, r = 6000.0, 6000.0, 0.14, 0.05
    calc = IVCalculator()
    t_am = calculate_time_to_expiration(now, exp, market_close_time="09:30:00")
    price = calc._black_scholes_price(spot, strike, t_am, r, sigma, "C")
    row = {
        "underlying": "SPX",
        "option_symbol": "SPX 261218C6000",
        "strike": strike,
        "expiration": exp,
        "option_type": "C",
        "timestamp": now,
        "bid": price - 0.05,
        "ask": price + 0.05,
    }
    out = calc.enrich_option_data_with_iv(row, spot, risk_free_rate=r, dividend_yield=0.0)
    assert abs(out["implied_volatility"] - sigma) < 0.002
    # The PM-settled weekly on the same date still solves to 16:00.
    weekly = dict(row, option_symbol="SPXW 261218C6000")
    t_pm = calculate_time_to_expiration(now, exp)
    pm_price = calc._black_scholes_price(spot, strike, t_pm, r, sigma, "C")
    weekly.update(bid=pm_price - 0.05, ask=pm_price + 0.05, implied_volatility=None)
    out_pm = calc.enrich_option_data_with_iv(weekly, spot, risk_free_rate=r, dividend_yield=0.0)
    assert abs(out_pm["implied_volatility"] - sigma) < 0.002
