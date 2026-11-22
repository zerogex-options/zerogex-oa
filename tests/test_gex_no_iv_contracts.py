"""Regression: a contract with no solved IV adds no gamma to GEX by strike.

Ingestion prices the gamma of a contract whose IV it can't solve at the flat
IMPLIED_VOLATILITY_DEFAULT (20%), so the row can carry approximate Greeks.
Near expiry gamma is 10-30x off at the wrong vol, and about half of all 0DTE
contracts have no solvable IV, so per-strike GEX near spot was inflated by a
third. The flip profile, vanna/charm, pin and forced flow already skipped those
contracts. Their open interest still counts.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
import pytz

from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")
TS = ET.localize(datetime(2026, 11, 13, 12, 0))
EXP = date(2026, 11, 13)


def _engine() -> AnalyticsEngine:
    eng = AnalyticsEngine.__new__(AnalyticsEngine)
    eng.db_symbol = "SPX"
    eng.risk_free_rate = 0.05
    eng.dividend_yield = 0.0
    return eng


def _opt(symbol: str, opt_type: str, strike: float, gamma: float, iv, oi: int = 1000) -> dict:
    return dict(
        option_symbol=symbol,
        strike=strike,
        expiration=EXP,
        option_type=opt_type,
        open_interest=oi,
        volume=10,
        gamma=gamma,
        implied_volatility=iv,
    )


def test_a_no_iv_contract_adds_its_open_interest_but_not_its_gamma():
    options = [
        # ITM 0DTE call quoted 59.80/60.30: IV unsolvable, gamma priced at 20%.
        _opt("SPXW 261113C05940000", "C", 5940.0, 9.6e-4, None),
        # Same strike, a solved put.
        _opt("SPXW 261113P05940000", "P", 5940.0, 2.0e-4, 0.13),
    ]
    row = _engine()._calculate_gex_by_strike(options, 6000.0, TS)[0]
    assert row["call_gamma"] == 0.0
    assert row["call_oi"] == 1000
    assert row["put_gamma"] == pytest.approx(2.0e-4 * 1000)


def test_a_zero_iv_is_treated_as_unsolved_too():
    options = [_opt("SPXW 261113C05940000", "C", 5940.0, 9.6e-4, 0.0)]
    row = _engine()._calculate_gex_by_strike(options, 6000.0, TS)[0]
    assert row["call_gamma"] == 0.0


def test_a_solved_contract_keeps_its_stored_gamma():
    options = [_opt("SPXW 261113C06000000", "C", 6000.0, 3.0e-3, 0.12)]
    row = _engine()._calculate_gex_by_strike(options, 6000.0, TS)[0]
    assert row["call_gamma"] == pytest.approx(3.0e-3 * 1000)
