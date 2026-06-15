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
