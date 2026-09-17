"""AM settlement is a property of the PRODUCT, not of SPX.

SPX and NDX both list a monthly series that expires on the third Friday
and settles to a Special Opening Quotation at ~09:30 ET, alongside a
PM-settled series (SPXW, NDXP) that shares the same underlying in this
platform's data.  The rule was written for SPX and spelled out in four
places, each hard-coding the ``SPXW`` prefix, so NDX went unfiltered
through every third Friday: its settled monthlies stayed in the chain all
afternoon carrying whatever marks the feed last had for a dead instrument.

What that costs is specific.  The Spread Monitor reduces the same chain
the analytics snapshot hands it, so an unfiltered AM-settled monthly shows
up as a chain-wide liquidity event — on the one day of the month someone
is most likely to be checking whether the market has gone untradeable, and
in the rollup that every later session is then ranked against.

These tests pin the generalisation: the rule covers both products, the
PM-settled sibling of each survives it, and the products that never had an
AM series are untouched.
"""

from __future__ import annotations

from datetime import date

from src.market_calendar import (
    canonical_index_symbol,
    expiration_close_time_et,
    is_am_settled_contract,
    is_am_settled_index_expiration,
    pm_settled_root_for,
    settlement_close_time_for_contract,
)

#: Third Fridays. September 2026 is the expiry that prompted this.
SEP_THIRD_FRIDAY = date(2026, 9, 18)
JUN_THIRD_FRIDAY = date(2026, 6, 19)
#: A Friday in the same month that is NOT the third one.
SEP_WEEKLY_FRIDAY = date(2026, 9, 25)


# ---------------------------------------------------------------------------
# The date heuristic, for callers holding only (underlying, expiration)
# ---------------------------------------------------------------------------


def test_ndx_third_friday_is_am_settled():
    """The regression. NDX monthlies settle at the Nasdaq-100 SOQ."""
    assert is_am_settled_index_expiration("NDX", SEP_THIRD_FRIDAY) is True


def test_spx_third_friday_is_still_am_settled():
    assert is_am_settled_index_expiration("SPX", SEP_THIRD_FRIDAY) is True
    assert is_am_settled_index_expiration("$SPX.X", JUN_THIRD_FRIDAY) is True


def test_etfs_have_no_am_settled_series():
    """SPY / QQQ settle PM on every expiry they list."""
    for symbol in ("SPY", "QQQ", "IWM", "AAPL"):
        assert is_am_settled_index_expiration(symbol, SEP_THIRD_FRIDAY) is False


def test_a_non_third_friday_is_pm_on_both_indices():
    for symbol in ("SPX", "NDX"):
        assert is_am_settled_index_expiration(symbol, SEP_WEEKLY_FRIDAY) is False


def test_the_underlying_is_normalised_not_rstripped():
    """``rstrip(".X")`` treats its argument as a character set: SPX -> SP."""
    assert canonical_index_symbol("$SPX.X") == "SPX"
    assert canonical_index_symbol("SPX") == "SPX"
    assert canonical_index_symbol("$NDXP.X") == "NDXP"
    assert is_am_settled_index_expiration("$NDX.X", SEP_THIRD_FRIDAY) is True


# ---------------------------------------------------------------------------
# The per-contract rule, for callers holding an option_symbol
# ---------------------------------------------------------------------------


def test_ndxp_survives_the_third_friday():
    """The PM-settled sibling is a live contract, not a settled one.

    Dropping it would be the mirror-image bug: a weekly discarded from the
    chain on the day the monthly settles.
    """
    assert is_am_settled_contract("NDX", "NDXP 260918P24000", SEP_THIRD_FRIDAY) is False


def test_spxw_survives_the_third_friday():
    assert is_am_settled_contract("SPX", "SPXW 260918P6800", SEP_THIRD_FRIDAY) is False


def test_the_am_rooted_contract_is_caught_on_both():
    assert is_am_settled_contract("NDX", "NDX 260918P24000", SEP_THIRD_FRIDAY) is True
    assert is_am_settled_contract("SPX", "SPX 260918P6800", SEP_THIRD_FRIDAY) is True


def test_a_missing_option_symbol_degrades_to_the_date_rule():
    """Not every row carries one, and the answer must still be defined."""
    assert is_am_settled_contract("NDX", None, SEP_THIRD_FRIDAY) is True
    assert is_am_settled_contract("NDX", "", SEP_WEEKLY_FRIDAY) is False


def test_pm_root_lookup_is_the_single_place_a_product_is_registered():
    assert pm_settled_root_for("SPX") == "SPXW"
    assert pm_settled_root_for("NDX") == "NDXP"
    assert pm_settled_root_for("$NDX.X") == "NDXP"
    assert pm_settled_root_for("SPY") is None
    assert pm_settled_root_for(None) is None


# ---------------------------------------------------------------------------
# Time to expiration follows the same rule
# ---------------------------------------------------------------------------


def test_close_time_is_the_soq_for_an_ndx_monthly():
    """Otherwise an expiring NDX monthly carries ~6.5h of phantom time value."""
    assert expiration_close_time_et("NDX", SEP_THIRD_FRIDAY) == "09:30:00"
    assert expiration_close_time_et("NDX", SEP_WEEKLY_FRIDAY) == "16:00:00"


def test_per_contract_close_time_separates_the_two_ndx_roots():
    """They share an underlying AND a date; only the root tells them apart."""
    assert (
        settlement_close_time_for_contract("NDX", "NDX 260918C24000", SEP_THIRD_FRIDAY)
        == "09:30:00"
    )
    assert (
        settlement_close_time_for_contract("NDX", "NDXP 260918C24000", SEP_THIRD_FRIDAY)
        == "16:00:00"
    )


def test_per_contract_close_time_without_an_underlying_is_pm():
    assert settlement_close_time_for_contract(None, "SPX 260918C6800", SEP_THIRD_FRIDAY) == (
        "16:00:00"
    )
