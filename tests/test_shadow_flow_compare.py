"""The bucket-level flow diff between a production DB and a rehearsal DB.

This is the measurement the cutover decision rests on, so the comparison
arithmetic is pure and tested here without a database. The SQL's own
derivation is NOT restated -- it splices in production's same-session
fragment from src.api.database, and a test pins that it did.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.tools.shadow_flow_compare import bucket_flow_sql, compare_buckets

T0 = datetime(2026, 10, 9, 13, 30, tzinfo=timezone.utc)


def _row(minute: int, net: int, vol: int = 10_000, underlying: str = "SPY"):
    return (underlying, T0 + timedelta(minutes=minute), net, vol)


# ---------------------------------------------------------------------------
# The SQL reuses production's derivation rather than copying it
# ---------------------------------------------------------------------------


def test_sql_splices_in_productions_own_same_session_clause():
    """A copied clause drifts. The 2026-06-01 phantom-midnight bug lived in
    exactly this logic, and a second hand-maintained copy would reintroduce
    it silently in the one place nobody re-reads."""
    from src.api.database import _flow_lag_same_session_clause

    sql = bucket_flow_sql()
    assert _flow_lag_same_session_clause(use_cash_keying=True) in sql
    assert "{same_session}" not in sql, "the placeholder must be substituted"


def test_sql_rescales_by_total_volume_not_raw_ask_minus_bid():
    """The published figure attributes the unclassified mid portion.

    Comparing raw ask-minus-bid would measure a number the product does not
    publish, and would understate every bucket with mid volume in it.
    """
    import re

    # Assert the WHOLE expression, not that the words appear somewhere. The
    # first version of this test checked only that "volume_delta" and the
    # guard were present, and a mutation that deleted the scaling itself
    # left both in place and passed.
    sql = re.sub(r"\s+", " ", bucket_flow_sql())
    assert (
        "(ask_vol_delta::numeric - bid_vol_delta) "
        "/ (ask_vol_delta + bid_vol_delta) * volume_delta"
    ) in sql, "the buy/sell split must be rescaled by the total volume delta"


# ---------------------------------------------------------------------------
# Comparison arithmetic
# ---------------------------------------------------------------------------


def test_identical_runs_agree_on_every_bucket():
    rows = [_row(0, 5_000), _row(1, -3_000), _row(2, 12_000)]
    out = compare_buckets(rows, rows)

    assert out["buckets_compared"] == 3
    assert out["sign_agrees"] == 3
    assert out["sign_agreement_pct"] == 100.0
    assert out["mean_abs_difference"] == 0


def test_a_sign_flip_is_counted():
    prod = [_row(0, 5_000), _row(1, -3_000)]
    shad = [_row(0, 5_000), _row(1, +2_000)]
    out = compare_buckets(prod, shad)

    assert out["sign_agrees"] == 1
    assert out["sign_disagrees"] == 1
    assert out["sign_agreement_pct"] == 50.0


def test_one_sided_difference_reads_as_bias_not_noise():
    """shadow_higher near n is the signature that separated the quote test's
    feed bias from harness noise on 2026-10-07; it belongs here too."""
    prod = [_row(i, 1_000) for i in range(6)]
    shad = [_row(i, 4_000) for i in range(6)]
    out = compare_buckets(prod, shad)

    assert out["shadow_higher"] == 6
    assert out["mean_signed_difference"] == 3_000
    assert out["mean_abs_difference"] == 3_000


def test_cancelling_differences_read_as_noise():
    prod = [_row(0, 1_000), _row(1, 1_000)]
    shad = [_row(0, 4_000), _row(1, -2_000)]
    out = compare_buckets(prod, shad)

    assert out["shadow_higher"] == 1, "one up, one down"
    assert out["mean_signed_difference"] == 0, "they cancel"
    assert out["mean_abs_difference"] == 3_000, "but the magnitude is real"


def test_buckets_present_in_only_one_run_are_reported_not_dropped():
    """A silent inner join is how 2026-09-28 reported a broken harness as a
    quiet market. A rehearsal that covered half the session must not read as
    perfect agreement on the half it managed."""
    prod = [_row(0, 5_000), _row(1, 5_000), _row(2, 5_000)]
    shad = [_row(1, 5_000)]
    out = compare_buckets(prod, shad)

    assert out["buckets_compared"] == 1
    assert out["production_only"] == 2
    assert out["shadow_only"] == 0
    assert out["sign_agreement_pct"] == 100.0, (
        "the one shared bucket does agree -- which is exactly why the "
        "production_only count has to be reported next to it"
    )


def test_underlyings_do_not_cross_match():
    """SPY's 13:31 bucket is not SPX's 13:31 bucket."""
    prod = [_row(0, 5_000, underlying="SPY"), _row(0, -5_000, underlying="SPX")]
    shad = [_row(0, 5_000, underlying="SPY")]
    out = compare_buckets(prod, shad)

    assert out["buckets_compared"] == 1
    assert out["production_only"] == 1
    assert out["rows"][0]["underlying"] == "SPY"


def test_two_zero_buckets_agree_rather_than_reading_as_a_flip():
    """0 is not positive, so a naive (a>0)==(b>0) would call 0 vs 0 agreement
    by accident and 0 vs +5 a flip. The first is right; pin it."""
    out = compare_buckets([_row(0, 0)], [_row(0, 0)])
    assert out["sign_agrees"] == 1


def test_no_overlap_at_all_is_not_agreement():
    out = compare_buckets([_row(0, 5_000)], [_row(99, 5_000)])

    assert out["buckets_compared"] == 0
    assert out["sign_agreement_pct"] is None, "must not be reported as 100%"
    assert out["production_only"] == 1 and out["shadow_only"] == 1
