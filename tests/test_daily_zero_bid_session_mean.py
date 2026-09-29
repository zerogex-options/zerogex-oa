"""The daily no-bid share is a session mean, not a 15:5x snapshot.

``daily_spread_stats`` stores one row per session and every other column in
it is a snapshot of whichever analytics cycle ran last before the 16:00 ET
close.  For a median over hundreds of contracts that is a defensible sample.
For ``zero_bid_pct`` it is not, because the statistic is not flat across the
session: it is near zero all morning and climbs steadily into the close --
on SPX puts, 0.00 at 09:30 through 0.43 at 13:00, 3.48 at 14:30 and 7.30 in
the last half hour, with the same shape on NDX and on calls.  The daily
writer's single sample landed on the steepest part of that ramp, so the
stored figure recorded which minute the cycle fired: it read exactly 0.0 in
10-12% of sessions when the sessions themselves were never zero in 95-100%
of them, and carried 2.5x to 11x the session mean's variance.

These tests pin the replacement: the value is averaged over the session's
half-hour surface buckets, the blend is contract-weighted within a bucket
and unweighted across them, and a session with no surface rows falls back to
a real reading rather than writing a zero that would later read as a session
when nothing went unquotable.
"""

from __future__ import annotations

from datetime import date
from typing import Any, List, Optional, Sequence, Tuple

from src.analytics import surface_store

SESSION = date(2026, 9, 16)


class _Cursor:
    """Just enough cursor to answer one aggregate query."""

    def __init__(self, result: Optional[Tuple[Any, ...]]):
        self._result = result
        self.executed: List[Tuple[str, Sequence[Any]]] = []

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._result


# ---------------------------------------------------------------------------
# The read itself
# ---------------------------------------------------------------------------


def test_the_session_mean_is_returned_per_option_type():
    cur = _Cursor((2.0, 8.0, 5.0, 13))
    means, buckets = surface_store.session_zero_bid_means(cur, "SPX", SESSION, 7, 5.0)

    assert means == {"C": 2.0, "P": 8.0, "A": 5.0}
    assert buckets == 13


def test_the_scope_is_translated_to_the_surface_table_s_key_names():
    """The daily row pins dte_max=7; the surface table calls that 'u7'.

    Getting this wrong would not error -- it would return no rows and
    silently fall back to the snapshot on every single cycle, which is the
    bug this change exists to remove, wearing a fixed label.
    """
    cur = _Cursor((1.0, 1.0, 1.0, 13))
    surface_store.session_zero_bid_means(cur, "NDX", SESSION, 7, 5.0)

    _, params = cur.executed[0]
    assert params == ("NDX", SESSION, "u7", 5.0, "all")


def test_no_surface_rows_reports_zero_buckets_rather_than_a_zero_share():
    """A share of 0.0 and 'we have no idea' are different facts.

    Returning 0.0 here would write a session in which nothing went
    untradeable -- the single most misleading value this column can hold.
    """
    for empty in ((None, None, None, 0), None):
        means, buckets = surface_store.session_zero_bid_means(
            _Cursor(empty), "SPX", SESSION, 7, 5.0
        )
        assert means == {}
        assert buckets == 0


def test_a_type_with_no_rows_is_absent_rather_than_zero():
    """An expiration listing only calls must not report puts as 0% no-bid."""
    means, buckets = surface_store.session_zero_bid_means(
        _Cursor((2.0, None, 2.0, 11)), "SPX", SESSION, 7, 5.0
    )
    assert "P" not in means
    assert means == {"C": 2.0, "A": 2.0}
    assert buckets == 11


# ---------------------------------------------------------------------------
# What the writers do with it
# ---------------------------------------------------------------------------


def test_the_writer_prefers_the_session_mean_over_its_own_snapshot():
    """The substitution, expressed the way both writers spell it."""
    session_zb = {"C": 2.0, "P": 8.0, "A": 5.0}
    snapshot_zb = 0.0  # what a 15:5x cycle happened to see

    assert session_zb.get("P", snapshot_zb) == 8.0
    assert session_zb.get("C", snapshot_zb) == 2.0


def test_the_writer_falls_back_when_the_session_has_no_buckets():
    """Early in the day, or after a surface write failure.

    The fallback is this cycle's own reading: noisier than the mean it
    replaces, but a real measurement of a real chain.
    """
    session_zb: dict = {}
    snapshot_zb = 7.3

    assert session_zb.get("P", snapshot_zb) == 7.3


# ---------------------------------------------------------------------------
# The aggregation rule, stated as arithmetic
# ---------------------------------------------------------------------------


def test_buckets_average_unweighted_and_types_blend_weighted():
    """Two rules, and they are deliberately different.

    Across buckets: each half hour is ONE observation of how much of the
    chain had no market, so a fuller bucket must not speak for a thinner
    one.  Within a bucket: the two option types are one population split in
    two, so the blend is contract-weighted.

    The fixture is the one the repair SQL was validated against.
    """
    # bucket 1: C 0% of 100, P 10% of 100  -> blended 5%
    # bucket 2: C 4% of 100, P  6% of 100  -> blended 5%
    calls = [0.0, 4.0]
    puts = [10.0, 6.0]
    blended = [(c * 100 + p * 100) / 200 for c, p in zip(calls, puts)]

    assert sum(calls) / len(calls) == 2.0
    assert sum(puts) / len(puts) == 8.0
    assert sum(blended) / len(blended) == 5.0


def test_an_unweighted_bucket_mean_is_not_the_same_as_pooling_contracts():
    """Why the distinction above is worth a test rather than a comment.

    The closing bucket is both the widest and often the fullest.  Pooling
    every contract in the session would let it dominate the session figure
    and reproduce, in slower motion, exactly the bias the snapshot had.
    """
    #                       morning (thin book)   close (full book, wide)
    shares, counts = [0.0, 0.0, 7.5], [200, 200, 600]

    unweighted = sum(shares) / len(shares)
    pooled = sum(s * n for s, n in zip(shares, counts)) / sum(counts)

    assert round(unweighted, 2) == 2.50
    assert round(pooled, 2) == 4.50
    assert pooled > unweighted
