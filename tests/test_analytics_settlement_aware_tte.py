"""Analytics-side time-to-expiration must be per-contract, not per-expiration.

Before SPX monthly chain expansion was wired in, the analytics engine
keyed its TTE cache on ``expiration`` alone. On a 3rd-Friday under
monthly expansion both an AM-settled SPX contract (settles 09:30 ET)
and a PM-settled SPXW contract (settles 16:00 ET) can share that date.
The bare-expiration cache collapsed both to the AM time and shorted
the SPXW row's T by ~6.5h on expiry morning — wrong gamma, wrong GEX
bucket, wrong gamma-flip diagnostic.

These tests pin the new (expiration, close_time)-keyed contract that
``settlement_close_time_for_contract`` is consulted per row.
"""

from __future__ import annotations

from datetime import date, datetime

import pytz

from src.market_calendar import settlement_close_time_for_contract

ET = pytz.timezone("US/Eastern")


# ----------------------------------------------------------------------
# Settlement-time resolver shared by both engines
# ----------------------------------------------------------------------


def test_settlement_helper_spx_third_friday_am():
    third_friday = date(2026, 6, 19)
    assert settlement_close_time_for_contract("SPX", "SPX 260619C5000", third_friday) == "09:30:00"


def test_settlement_helper_spxw_on_same_third_friday_pm():
    third_friday = date(2026, 6, 19)
    assert settlement_close_time_for_contract("SPX", "SPXW 260619C5000", third_friday) == "16:00:00"


def test_settlement_helper_no_underlying_defaults_pm():
    # Defensive: caller without a usable underlying must not silently
    # mis-classify a 3rd-Friday as AM (would invent settlement times for
    # equity ETF / non-index contracts).
    assert settlement_close_time_for_contract(None, "SPY 260619C500", date(2026, 6, 19)) == "16:00:00"


def test_settlement_helper_non_third_friday_pm():
    second_friday = date(2026, 6, 12)
    assert settlement_close_time_for_contract("SPX", "SPX 260612C5000", second_friday) == "16:00:00"


def test_settlement_helper_no_option_symbol_falls_back_to_underlying_default():
    # Legacy path: no option_symbol -> only underlying + 3rd-Friday
    # heuristic, so SPX 3rd-Friday is AM (matches the prior
    # expiration_close_time_et behavior).
    assert settlement_close_time_for_contract("SPX", None, date(2026, 6, 19)) == "09:30:00"


# ----------------------------------------------------------------------
# AnalyticsEngine._calculate_time_to_expiration per-contract override
# ----------------------------------------------------------------------


def _make_engine():
    from src.analytics.main_engine import AnalyticsEngine

    eng = object.__new__(AnalyticsEngine)
    eng.db_symbol = "SPX"
    return eng


def test_analytics_tte_uses_pm_close_for_spxw_on_third_friday():
    """SPXW 3rd-Friday must NOT be AM-settled (legacy path was wrong)."""
    eng = _make_engine()
    third_friday = date(2026, 6, 19)
    # Friday morning 09:00 ET — pre-settlement for both AM and PM
    # expiries on this date.
    now_et = ET.localize(datetime(2026, 6, 19, 9, 0, 0))

    spx_tte = eng._calculate_time_to_expiration(now_et, third_friday, "SPX 260619C5000")
    spxw_tte = eng._calculate_time_to_expiration(now_et, third_friday, "SPXW 260619C5000")

    # Both positive (we're pre-open). SPXW (PM-settled, 16:00) has ~6.5h
    # MORE time than SPX (AM-settled, 09:30). Difference: 6.5/24/365 yr.
    assert spx_tte > 0
    assert spxw_tte > spx_tte
    expected_gap_years = 6.5 / 24.0 / 365.0
    assert abs((spxw_tte - spx_tte) - expected_gap_years) < 1e-6, (spx_tte, spxw_tte)


def test_analytics_tte_legacy_call_unchanged_for_spx_3rd_friday():
    """Call without option_symbol must keep the prior (AM-settled) behavior
    so non-monthly callers stay byte-identical."""
    eng = _make_engine()
    third_friday = date(2026, 6, 19)
    now_et = ET.localize(datetime(2026, 6, 19, 9, 0, 0))

    legacy = eng._calculate_time_to_expiration(now_et, third_friday)
    with_root = eng._calculate_time_to_expiration(now_et, third_friday, "SPX 260619C5000")
    assert abs(legacy - with_root) < 1e-12
