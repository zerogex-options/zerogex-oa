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

from datetime import date, datetime

import pytest

from src.market_calendar import (
    ET,
    canonical_index_symbol,
    expiration_close_time_et,
    is_am_settled_contract,
    is_am_settled_index_expiration,
    is_settled_am_contract,
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


# ---------------------------------------------------------------------------
# The clock, which is the half of the rule three of the four callers lacked
# ---------------------------------------------------------------------------
#
# ``is_am_settled_contract`` answers a property of the contract: does this
# thing settle at the opening auction.  Every chain filter wants a different
# question — is it dead RIGHT NOW — and the difference between them is a
# tradable morning.  The analytics snapshot gated its drop on 09:30 ET; the
# Spread Monitor's reduction and both spread backfills compared the
# expiration against the session date and nothing else, so before the bell on
# a third Friday they discarded the expiring monthlies while those were still
# quoting.  Each of the three said in its docstring that it could not drift
# from the live path.

SPX_MONTHLY = "SPX  260918C05000000"
SPXW_WEEKLY = "SPXW 260918C05000000"
NDX_MONTHLY = "NDX  260918C24000000"


def _et(hour: int, minute: int, day: date = SEP_THIRD_FRIDAY) -> datetime:
    return ET.localize(datetime(day.year, day.month, day.day, hour, minute))


def test_the_monthly_is_live_until_the_bell():
    """The regression. At 08:00 on expiration Friday it is still trading."""
    assert is_settled_am_contract("SPX", SPX_MONTHLY, SEP_THIRD_FRIDAY, _et(8, 0)) is False
    assert is_settled_am_contract("SPX", SPX_MONTHLY, SEP_THIRD_FRIDAY, _et(9, 29)) is False
    assert is_settled_am_contract("NDX", NDX_MONTHLY, SEP_THIRD_FRIDAY, _et(9, 29)) is False


def test_the_soq_minute_itself_counts_as_settled():
    """09:30 is the boundary, and it belongs to the settled side."""
    assert is_settled_am_contract("SPX", SPX_MONTHLY, SEP_THIRD_FRIDAY, _et(9, 30)) is True
    assert is_settled_am_contract("SPX", SPX_MONTHLY, SEP_THIRD_FRIDAY, _et(15, 59)) is True
    assert is_settled_am_contract("NDX", NDX_MONTHLY, SEP_THIRD_FRIDAY, _et(15, 59)) is True


def test_the_pm_sibling_is_never_settled_by_the_soq():
    """Not at any hour: SPXW and NDXP settle at 16:00 like everything else."""
    for hour in (8, 9, 10, 15):
        assert (
            is_settled_am_contract("SPX", SPXW_WEEKLY, SEP_THIRD_FRIDAY, _et(hour, 30))
            is False
        )


def test_a_weekly_friday_has_no_soq_to_be_past():
    assert (
        is_settled_am_contract(
            "SPX", "SPXW 260925C05000000", SEP_WEEKLY_FRIDAY, _et(15, 0, SEP_WEEKLY_FRIDAY)
        )
        is False
    )


def test_the_timestamp_is_converted_to_eastern_not_read_as_wall_clock():
    """The backfills hand over a UTC timestamptz straight from Postgres.

    Read as wall clock, 13:00 UTC is past 09:30 and the contract looks dead
    four and a half hours early — on the one morning it is still trading.
    """
    naive_utc_0900_et = datetime(2026, 9, 18, 13, 0)
    naive_utc_1000_et = datetime(2026, 9, 18, 14, 0)
    assert is_settled_am_contract("SPX", SPX_MONTHLY, SEP_THIRD_FRIDAY, naive_utc_0900_et) is False
    assert is_settled_am_contract("SPX", SPX_MONTHLY, SEP_THIRD_FRIDAY, naive_utc_1000_et) is True


def test_other_days_do_not_depend_on_the_clock():
    """Before its expiration a monthly is live; after it, the row is dead."""
    october = date(2026, 10, 16)
    assert (
        is_settled_am_contract("SPX", "SPX  261016C05000000", october, _et(15, 0)) is False
    )
    june = date(2026, 6, 19)
    assert is_settled_am_contract("SPX", "SPX  260619C05000000", june, _et(8, 0)) is True


def test_a_bare_date_is_refused_rather_than_read_as_end_of_day():
    """Accepting one would silently restore the bug this function fixes."""
    with pytest.raises(TypeError, match="timestamp, not a"):
        is_settled_am_contract("SPX", SPX_MONTHLY, SEP_THIRD_FRIDAY, SEP_THIRD_FRIDAY)
