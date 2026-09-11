"""Quoted-spread statistics — the reductions behind the Spread Monitor.

Three properties matter more than any individual number here, and each has
its own group of tests below:

1. **A contract with no two-sided market never produces a width.**  A
   0.00 x 2.40 put is not "240% wide"; there is no market in it.  Inventing
   a width for it would put a fabricated number in the median.

2. **Those contracts are still counted.**  The opposite failure is subtler
   and worse: if no-bid contracts simply left the sample, a chain would
   appear to TIGHTEN as its wings went untradeable, because only the
   still-quoted contracts would remain to be measured.

3. **One stale wing cannot move the headline.**  The median is the headline
   precisely so a single 0.05 x 4.00 mark cannot report itself as the state
   of the chain; the p90 exists so that mark is not lost either.
"""

from __future__ import annotations

import pytest

from src.analytics.spread_stats import (
    QuoteState,
    aggregate,
    aggregate_by_expiration,
    aggregate_by_moneyness,
    aggregate_by_option_type,
    classify_quote,
    contract_spread,
    contract_spreads,
    percentile,
    percentile_rank,
)


def _row(strike, option_type="P", bid=None, ask=None, expiration="2026-09-10", **extra):
    return {
        "option_symbol": f"TEST{strike}{option_type}",
        "strike": strike,
        "option_type": option_type,
        "expiration": expiration,
        "bid": bid,
        "ask": ask,
        **extra,
    }


# ---------------------------------------------------------------------------
# Quote classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bid,ask,expected",
    [
        (1.00, 1.20, QuoteState.TWO_SIDED),
        (0.00, 2.40, QuoteState.ZERO_BID),   # offered, nothing to sell into
        (None, 2.40, QuoteState.ZERO_BID),
        (1.50, 1.50, QuoteState.LOCKED),
        (2.00, 1.50, QuoteState.CROSSED),    # stale marks, not free money
        (1.00, 0.00, QuoteState.NO_QUOTE),
        (1.00, None, QuoteState.NO_QUOTE),
        (None, None, QuoteState.NO_QUOTE),
    ],
)
def test_classify_quote(bid, ask, expected):
    assert classify_quote(bid, ask) is expected


def test_no_quote_takes_precedence_over_zero_bid():
    """An unquoted contract must not be reported as a no-bid one.

    "Nobody is showing a market" and "somebody is offering but nobody bids"
    are different market states, and only the second is the liquidity
    failure the page is about.
    """
    assert classify_quote(None, None) is QuoteState.NO_QUOTE
    assert classify_quote(0.0, 0.0) is QuoteState.NO_QUOTE


# ---------------------------------------------------------------------------
# Per-contract reduction
# ---------------------------------------------------------------------------


def test_two_sided_contract_carries_all_three_widths():
    spread = contract_spread(_row(100, bid=1.00, ask=1.20), spot=100.0)
    assert spread is not None
    assert spread.state is QuoteState.TWO_SIDED
    assert spread.spread == pytest.approx(0.20)
    assert spread.mid == pytest.approx(1.10)
    # 0.20 / 1.10 = 18.18% of the premium.
    assert spread.relative_spread_pct == pytest.approx(18.1818, abs=1e-3)
    # 0.20 / 100 = 20 bps of the underlying.
    assert spread.spread_bps_underlying == pytest.approx(20.0)


def test_untradeable_contract_has_no_width_at_all():
    """Property 1: no two-sided market means no width, not a huge width."""
    spread = contract_spread(_row(90, bid=0.0, ask=2.40), spot=100.0)
    assert spread is not None
    assert spread.state is QuoteState.ZERO_BID
    assert spread.spread is None
    assert spread.relative_spread_pct is None
    assert spread.spread_bps_underlying is None
    assert spread.is_tradable is False


def test_moneyness_is_signed_from_spot():
    """Below spot is negative, so the put wing sorts to the left."""
    below = contract_spread(_row(95), spot=100.0)
    above = contract_spread(_row(105), spot=100.0)
    assert below is not None and above is not None
    assert below.moneyness_pct == pytest.approx(-5.0)
    assert above.moneyness_pct == pytest.approx(5.0)


def test_decimal_and_string_quotes_are_coerced():
    """NUMERIC columns arrive as Decimal from asyncpg and str from psycopg2."""
    from decimal import Decimal

    spread = contract_spread(
        _row(100, bid=Decimal("1.00"), ask="1.20", strike_override=None), spot=100.0
    )
    assert spread is not None
    assert spread.spread == pytest.approx(0.20)


def test_structurally_unusable_rows_are_dropped():
    assert contract_spread(_row(None, bid=1.0, ask=1.2), spot=100.0) is None
    assert contract_spread(_row(100, bid=1.0, ask=1.2), spot=0.0) is None
    assert contract_spread(_row(-5, bid=1.0, ask=1.2), spot=100.0) is None


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def test_untradeable_contracts_are_counted_not_dropped():
    """Property 2: a deteriorating chain must not look like a tightening one.

    Two contracts, one tight and one with no bid.  The median width is the
    tight one's — correctly, since it is the only real market — but the
    aggregate must simultaneously report that only half the chain has a
    market at all.  Without the coverage number the reader would see an
    unchanged median and conclude nothing had happened.
    """
    spreads = contract_spreads(
        [
            _row(100, bid=1.00, ask=1.10),
            _row(90, bid=0.00, ask=2.40),
        ],
        spot=100.0,
    )
    agg = aggregate(spreads)
    assert agg.contract_count == 2
    assert agg.tradable_count == 1
    assert agg.two_sided_pct == pytest.approx(50.0)
    assert agg.zero_bid_pct == pytest.approx(50.0)
    assert agg.median_relative_spread_pct is not None


def test_median_resists_a_single_stale_wing_but_p90_reports_it():
    """Property 3: one garbage mark moves the tail, never the headline.

    Nine orderly contracts at 9.5% wide and one stale far wing quoted
    0.05 x 4.00 — 195% of its own mid.  A mean over this sample reports
    28%, roughly triple the width of any contract a trader would actually
    hit.  The median is unmoved at 9.5%; the p90 lifts well clear of it,
    which is exactly the division of labour the two statistics exist for.
    """
    rows = [_row(100 + i, bid=1.00, ask=1.10) for i in range(9)]
    rows.append(_row(60, bid=0.05, ask=4.00))
    agg = aggregate(contract_spreads(rows, spot=100.0))

    assert agg.median_relative_spread_pct == pytest.approx(9.5238, abs=1e-2)
    assert agg.p90_relative_spread_pct is not None
    assert agg.p90_relative_spread_pct > 2 * agg.median_relative_spread_pct


def test_empty_bucket_aggregates_to_nulls_not_zeros():
    """An expiration with nothing quoted renders "no data", never "0% wide"."""
    agg = aggregate([])
    assert agg.contract_count == 0
    assert agg.median_relative_spread_pct is None
    assert agg.p90_spread_bps_underlying is None
    assert agg.two_sided_pct == 0.0


def test_put_call_split_is_not_a_blended_median():
    """The split is the point: doubled put widths must not be halved away."""
    rows = [
        _row(95, "P", bid=1.00, ask=2.00),   # 66.7% wide
        _row(96, "P", bid=1.00, ask=2.00),
        _row(105, "C", bid=1.00, ask=1.05),  # 4.9% wide
        _row(106, "C", bid=1.00, ask=1.05),
    ]
    by_type = aggregate_by_option_type(contract_spreads(rows, spot=100.0))

    puts = by_type["puts"].median_relative_spread_pct
    calls = by_type["calls"].median_relative_spread_pct
    assert puts is not None and calls is not None
    assert puts > 10 * calls
    assert by_type["all"].contract_count == 4


def test_open_interest_totals_include_untradeable_contracts():
    """OI is context for the width; excluding no-bid strikes would misstate it."""
    rows = [
        _row(100, bid=1.00, ask=1.10, open_interest=500, volume=20),
        _row(90, bid=0.00, ask=2.40, open_interest=1500, volume=5),
    ]
    agg = aggregate(contract_spreads(rows, spot=100.0))
    assert agg.total_open_interest == 2000
    assert agg.total_volume == 25


# ---------------------------------------------------------------------------
# Bucketing
# ---------------------------------------------------------------------------


def test_moneyness_buckets_are_half_open_with_an_inclusive_last_edge():
    """A strike exactly on the outer edge must land somewhere, not vanish."""
    rows = [
        _row(90, bid=1.0, ask=1.2),    # -10.0%, the lower edge
        _row(110, bid=1.0, ask=1.2),   # +10.0%, the upper edge
        _row(100, bid=1.0, ask=1.2),   # 0%, the ATM bucket
    ]
    buckets = aggregate_by_moneyness(contract_spreads(rows, spot=100.0))
    assert sum(b["contract_count"] for b in buckets) == 3

    first = buckets[0]
    last = buckets[-1]
    assert first["moneyness_low_pct"] == -10.0 and first["contract_count"] == 1
    assert last["moneyness_high_pct"] == 10.0 and last["contract_count"] == 1


def test_moneyness_filter_isolates_one_option_type():
    rows = [_row(95, "P", bid=1.0, ask=1.2), _row(95, "C", bid=1.0, ask=1.2)]
    puts = aggregate_by_moneyness(contract_spreads(rows, spot=100.0), option_type="P")
    assert sum(b["contract_count"] for b in puts) == 1


def test_expirations_are_split_by_type_and_sorted_nearest_first():
    """The near-dated rows come first — that is where quotes go first."""
    rows = [
        _row(99, "P", bid=1.00, ask=2.00, expiration="friday"),
        _row(99, "P", bid=1.00, ask=1.05, expiration="today"),
        _row(101, "C", bid=1.00, ask=1.05, expiration="today"),
    ]
    out = aggregate_by_expiration(
        contract_spreads(rows, spot=100.0), {"today": 0, "friday": 4}
    )
    assert [slice_["dte"] for slice_ in out] == [0, 4]

    today = out[0]
    assert today["calls"]["contract_count"] == 1
    assert today["puts"]["contract_count"] == 1
    assert today["all"]["contract_count"] == 2

    friday = out[1]
    assert friday["calls"]["contract_count"] == 0
    assert friday["puts"]["median_relative_spread_pct"] > (
        today["puts"]["median_relative_spread_pct"]
    )


def test_expiration_the_caller_could_not_resolve_is_skipped():
    """An unresolvable expiration joins no row rather than widening one."""
    rows = [
        _row(100, bid=1.0, ask=1.2, expiration="known"),
        _row(101, bid=5.0, ask=9.0, expiration="mystery"),
    ]
    out = aggregate_by_expiration(contract_spreads(rows, spot=100.0), {"known": 0})
    assert len(out) == 1
    assert out[0]["all"]["contract_count"] == 1


# ---------------------------------------------------------------------------
# Percentile helpers
# ---------------------------------------------------------------------------


def test_percentile_matches_linear_interpolation():
    values = [1.0, 2.0, 3.0, 4.0]
    assert percentile(values, 0) == pytest.approx(1.0)
    assert percentile(values, 50) == pytest.approx(2.5)
    assert percentile(values, 90) == pytest.approx(3.7)
    assert percentile(values, 100) == pytest.approx(4.0)


def test_percentile_of_empty_is_none():
    assert percentile([], 50) is None


def test_percentile_rank_distinguishes_no_history_from_a_tight_day():
    """None means "no history"; 0-ish means "the tightest on record"."""
    assert percentile_rank(1.0, []) is None
    assert percentile_rank(0.5, [1.0, 2.0, 3.0]) == pytest.approx(0.0)
    assert percentile_rank(3.0, [1.0, 2.0, 3.0]) == pytest.approx(100.0)
    assert percentile_rank(2.0, [1.0, 2.0, 3.0, 4.0]) == pytest.approx(50.0)
