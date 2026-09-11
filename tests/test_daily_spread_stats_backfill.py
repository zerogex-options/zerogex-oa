"""The daily_spread_stats backfill: how it walks history, and how fast.

The behaviours pinned here are the ones that do not show up in a unit test
of the arithmetic, because they are properties of the QUERIES rather than
the numbers:

* **One bounded probe per trading day, not one scan of the window.** The
  first version of this tool filtered on
  ``(timestamp AT TIME ZONE 'America/New_York')::time``, which is not
  sargable: the planner cannot use it as an index condition, so it scanned
  the whole retention window and discarded ~94% of it. Measured against a
  938k-row fixture that was 107ms and 261k rows-removed-by-filter per
  worker, versus 0.06ms and 4 buffers for the equivalent range probe — and
  the scan grows with the DENSITY of the chain, so production SPX is far
  worse than the fixture. The fix only holds while the SQL stays a range.

* **The day list comes from underlying_quotes**, the small table. A day
  with no underlying bar has no spot to centre the strike band on, so it
  could not be measured anyway.

* **A day with no late-session chain rows is skipped, not written.** A
  holiday or an ingestion gap must not enter the history as a session, or
  the trailing percentile ranks today against a day that never traded.
"""

from __future__ import annotations

import datetime as dt
import re
from contextlib import contextmanager
from typing import Any, List, Tuple
from unittest.mock import patch

from src.tools import daily_spread_stats_backfill as backfill


TRADING_DAYS = [dt.date(2026, 9, 10), dt.date(2026, 9, 9), dt.date(2026, 9, 8)]
ANCHOR = dt.datetime(2026, 9, 10, 20, 0, tzinfo=dt.timezone.utc)


class FakeCursor:
    """Records every statement and answers by matching on the SQL."""

    def __init__(self, *, anchor_for=None, chain_rows=None):
        self.statements: List[Tuple[str, Any]] = []
        self._last = ""
        self._anchor_for = anchor_for if anchor_for is not None else (lambda day: ANCHOR)
        self._chain_rows = chain_rows

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        self._last = sql

    def fetchone(self):
        if "FROM option_chains" in self._last and "MAX(timestamp)" in self._last:
            day = self.statements[-1][1][1]
            return (self._anchor_for(day),)
        if "FROM underlying_quotes" in self._last:
            return (6800.0,)
        return None

    def fetchall(self):
        if "FROM underlying_quotes" in self._last and "DISTINCT" in self._last:
            return [(d,) for d in TRADING_DAYS]
        if "FROM option_chains" in self._last:
            return self._chain_rows if self._chain_rows is not None else _rows()
        return []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _rows():
    """A tiny in-scope chain: tight calls, wide puts, one put with no bid."""
    exp = dt.date(2026, 9, 10)
    return [
        ("SPXW C1", 6800, "C", exp, 20.00, 20.40, 100, 10),
        ("SPXW P1", 6780, "P", exp, 18.00, 24.00, 300, 40),
        ("SPXW P2", 6700, "P", exp, 0.00, 4.00, 500, 2),
    ]


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True


@contextmanager
def _fake_db(cursor):
    yield FakeConn(cursor)


def _run(cursor) -> Tuple[int, int]:
    with patch.object(backfill, "db_connection", lambda: _fake_db(cursor)):
        return backfill._backfill_symbol("SPX", 90, 300000)


def _anchor_probes(cursor: FakeCursor):
    return [
        (sql, params)
        for sql, params in cursor.statements
        if "FROM option_chains" in sql and "MAX(timestamp)" in sql
    ]


# ---------------------------------------------------------------------------
# Query shape — the performance property
# ---------------------------------------------------------------------------


def test_anchor_probe_is_a_bounded_range_not_a_time_of_day_filter():
    """The regression guard. A ``::time`` predicate on the indexed column
    cannot be an index condition, and the tool degrades to a full scan of
    the retention window."""
    cursor = FakeCursor()
    _run(cursor)

    probes = _anchor_probes(cursor)
    assert probes, "no per-day anchor probe was issued"
    for sql, _ in probes:
        # ``::time\b`` and not a plain substring check: the sargable form
        # legitimately contains ``::timestamp``, which ``"::time" in sql``
        # would match.
        assert not re.search(r"::time\b", sql), (
            "the anchor probe filters on a time-of-day expression again — "
            "that is not sargable and re-introduces the full-window scan"
        )
        assert "AT TIME ZONE 'America/New_York')" in sql
        assert "timestamp >=" in sql and "timestamp <" in sql


def test_one_anchor_probe_per_trading_day():
    """Bounded work per day, so cost scales with DAYS rather than with how
    densely the chain is quoted."""
    cursor = FakeCursor()
    _run(cursor)
    probes = _anchor_probes(cursor)
    assert len(probes) == len(TRADING_DAYS)
    assert [p[1][1] for p in probes] == TRADING_DAYS


def test_day_list_comes_from_the_small_table():
    cursor = FakeCursor()
    _run(cursor)
    first_sql = cursor.statements[1][0]  # [0] is SET LOCAL statement_timeout
    assert "FROM underlying_quotes" in first_sql
    assert "DISTINCT" in first_sql


def test_statement_timeout_is_set_before_any_query():
    cursor = FakeCursor()
    _run(cursor)
    assert "SET LOCAL statement_timeout" in cursor.statements[0][0]


# ---------------------------------------------------------------------------
# What gets written
# ---------------------------------------------------------------------------


def test_writes_three_rows_per_day_and_commits():
    cursor = FakeCursor()
    written, skipped = _run(cursor)
    assert (written, skipped) == (3, 0)

    inserts = [s for s, _ in cursor.statements if "INSERT INTO daily_spread_stats" in s]
    assert len(inserts) == 3 * len(TRADING_DAYS)


def test_a_day_with_no_late_session_chain_is_skipped_not_written():
    """A holiday or an ingestion gap must not enter the history as a session."""
    missing = TRADING_DAYS[1]
    cursor = FakeCursor(anchor_for=lambda day: None if day == missing else ANCHOR)
    written, skipped = _run(cursor)

    assert written == len(TRADING_DAYS) - 1
    assert skipped == 1
    written_days = {
        params[1]
        for sql, params in cursor.statements
        if "INSERT INTO daily_spread_stats" in sql
    }
    assert missing not in written_days


def test_scope_travels_with_every_row():
    """A percentile only means something if each day measured the same
    population, so the filter is recorded on the row."""
    from src.config import SPREAD_STATS_DTE_MAX, SPREAD_STATS_MONEYNESS_BAND_PCT

    cursor = FakeCursor()
    _run(cursor)
    for sql, params in cursor.statements:
        if "INSERT INTO daily_spread_stats" in sql:
            assert params[4] == int(SPREAD_STATS_DTE_MAX)
            assert params[5] == float(SPREAD_STATS_MONEYNESS_BAND_PCT)


def test_no_trading_days_is_a_clean_no_op():
    class Empty(FakeCursor):
        def fetchall(self):
            return []

    cursor = Empty()
    assert _run(cursor) == (0, 0)
    assert not [s for s, _ in cursor.statements if "INSERT INTO" in s]


# ---------------------------------------------------------------------------
# SPX AM-settled filtering
# ---------------------------------------------------------------------------


def test_same_day_spx_monthly_is_dropped_but_spxw_is_kept():
    """A third-Friday SPX contract settled at the 09:30 SOQ is hours dead by
    the late-session anchor; counting it would report a chain-wide liquidity
    event every monthly expiry."""
    third_friday = dt.date(2026, 9, 18)
    assert not backfill._keep_contract("SPX", "SPX  260918C05000000", third_friday, third_friday)
    assert backfill._keep_contract("SPX", "SPXW 260918C05000000", third_friday, third_friday)
    assert backfill._keep_contract("SPY", "SPY   260918C00600000", third_friday, third_friday)
    # A future expiration is never filtered, whatever its series.
    assert backfill._keep_contract(
        "SPX", "SPX  260919C05000000", dt.date(2026, 9, 19), third_friday
    )
