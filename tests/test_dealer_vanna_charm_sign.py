"""Regression: the dealer vanna/charm columns use GEX's dealer convention.

GEX, the flip and forced_flow.dealer_hedge_flow all assume dealers are LONG
the calls customers sold them and SHORT the puts customers bought. A dealer
long an option trades -charm as time passes; short one, +charm. The engine
wrote dealer_charm = -(call + put): the flow of a dealer long EVERY contract,
which inverted every put's contribution relative to everything else. On a
put-heavy index book the signals reading it (vanna_charm_flow, eod_pressure,
market_pressure, TradeWorkz flow_context) pointed opposite to close_charm_flow.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
import pytz

from src.analytics.forced_flow import ContractLeg, dealer_hedge_flow
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")
TS = ET.localize(datetime(2026, 11, 12, 11, 0))  # Thursday 11:00 ET
EXP = date(2026, 11, 13)  # 1DTE
SPOT = 600.0


def _engine() -> AnalyticsEngine:
    eng = AnalyticsEngine.__new__(AnalyticsEngine)
    eng.db_symbol = "SPY"
    eng.risk_free_rate = 0.05
    eng.dividend_yield = 0.0
    return eng


def _row(eng: AnalyticsEngine, opt_type: str, strike: float, oi: int = 10_000, iv: float = 0.18):
    opt = dict(
        option_symbol=f"SPY 261113{opt_type}{int(strike * 1000):08d}",
        strike=strike,
        expiration=EXP,
        option_type=opt_type,
        open_interest=oi,
        volume=0,
        gamma=0.01,
        implied_volatility=iv,
    )
    row = eng._calculate_gex_by_strike([opt], SPOT, TS)[0]
    tte = eng._calculate_time_to_expiration(TS, EXP, opt["option_symbol"])
    reprice = dealer_hedge_flow(
        [ContractLeg(strike, opt_type, oi, iv, tte)], SPOT, 0.0, 1.0, 0.0, 0.05
    )
    return row, reprice.total_usd


@pytest.mark.parametrize(
    "opt_type, strike", [("P", 590.0), ("C", 610.0), ("P", 600.0), ("C", 595.0)]
)
def test_dealer_charm_matches_a_full_reprice_of_the_dealer_book(opt_type, strike):
    row, reprice_usd = _row(_engine(), opt_type, strike)
    assert row["dealer_charm_exposure"] == pytest.approx(reprice_usd, rel=0.02)


def test_dealer_flows_are_puts_minus_calls():
    eng = _engine()
    put_row, _ = _row(eng, "P", 590.0)
    call_row, _ = _row(eng, "C", 610.0)
    for row in (put_row, call_row):
        assert row["dealer_charm_exposure"] == pytest.approx(
            row["put_charm_exposure"] - row["call_charm_exposure"]
        )
        assert row["dealer_vanna_exposure"] == pytest.approx(
            row["put_vanna_exposure"] - row["call_vanna_exposure"]
        )
