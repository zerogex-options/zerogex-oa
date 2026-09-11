"""The daily_spread_stats writer: scope, gate, and what it refuses to write.

``AnalyticsEngine._store_daily_spread_stats`` UPSERTs three rows per trading
day (calls, puts, blended) from the snapshot already in memory.  Those rows
are the population the Spread Monitor's trailing percentile is scored
against, which makes two of its behaviours load-bearing rather than
cosmetic:

* **The cash-session gate, and where it closes.**  Once 16:00 ET passes,
  market makers stop quoting competitively and every chain goes wide.  The
  bound was 16:15 at first, to cover SPX's true session end; in production
  that let the closing rotation define the day whenever a cycle fell there
  (SPY 2026-09-02 recorded a 18.7% median with a 104% p90 — a tenth of the
  chain quoted wider than its own mid).  Because it only happened on the
  days a cycle landed late, it added noise to the trailing distribution
  rather than a bias that would at least cancel.  16:00 matches
  ``daily_atm_iv`` and the backfill's sampling window.

* **The pinned scope.**  A percentile only means something if each day
  measured the same contracts, so the DTE ceiling and moneyness band are
  applied on the way in and recorded on the way out.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytz

from src.analytics.main_engine import AnalyticsEngine


TRADE_DAY = date(2026, 5, 27)


def _options(spot: float = 600.0) -> list[dict]:
    """A small in-scope chain: tight calls, wide puts, one no-bid put."""
    return [
        {"strike": spot, "option_type": "C", "expiration": TRADE_DAY,
         "bid": 1.00, "ask": 1.05, "open_interest": 100, "volume": 10},
        {"strike": spot + 3, "option_type": "C", "expiration": TRADE_DAY,
         "bid": 0.50, "ask": 0.55, "open_interest": 80, "volume": 4},
        {"strike": spot - 3, "option_type": "P", "expiration": TRADE_DAY,
         "bid": 1.00, "ask": 2.00, "open_interest": 300, "volume": 40},
        {"strike": spot - 6, "option_type": "P", "expiration": TRADE_DAY,
         "bid": 0.00, "ask": 1.20, "open_interest": 500, "volume": 2},
    ]


def _summary(ts_utc: datetime, spot: float = 600.0) -> dict:
    return {"underlying": "SPY", "timestamp": ts_utc, "underlying_price": spot}


def _et(hour: int, minute: int) -> datetime:
    et = pytz.timezone("America/New_York")
    return et.localize(
        datetime(TRADE_DAY.year, TRADE_DAY.month, TRADE_DAY.day, hour, minute)
    ).astimezone(timezone.utc)


def _engine() -> AnalyticsEngine:
    return AnalyticsEngine(underlying="SPY")


def _params(cur: MagicMock) -> list[tuple]:
    """The parameter tuple of every UPSERT the writer issued."""
    return [call.args[1] for call in cur.execute.call_args_list]


def _option_types(cur: MagicMock) -> list[str]:
    # (underlying, timestamp, option_type, spot, ...) — see the INSERT.
    return [p[2] for p in _params(cur)]


# ---------------------------------------------------------------------------
# Cash-session gate
# ---------------------------------------------------------------------------


def test_writes_during_cash_session():
    engine = _engine()
    for hour, minute in [(9, 30), (12, 0), (15, 59), (16, 0)]:
        cur = MagicMock()
        engine._store_daily_spread_stats(_options(), _summary(_et(hour, minute)), cur)
        assert cur.execute.called, f"expected UPSERT at {hour:02d}:{minute:02d} ET"


def test_skips_outside_the_cash_session():
    """Pre-open and post-close both write nothing.

    The post-close half is the one that matters: a cycle after the bell
    would otherwise record the widest quotes of the day as that day's
    reading.  16:01 is in the list deliberately — the 16:00-16:15 closing
    rotation used to be inside the gate, and that is exactly the window
    that produced 104% p90 readings in production.
    """
    engine = _engine()
    for hour, minute in [(4, 0), (9, 29), (16, 1), (16, 15), (18, 5), (23, 59)]:
        cur = MagicMock()
        engine._store_daily_spread_stats(_options(), _summary(_et(hour, minute)), cur)
        assert (
            not cur.execute.called
        ), f"expected NO UPSERT at {hour:02d}:{minute:02d} ET"


# ---------------------------------------------------------------------------
# What gets written
# ---------------------------------------------------------------------------


def test_writes_calls_puts_and_blended_rows():
    """Three rows, because medians do not combine at read time."""
    cur = MagicMock()
    _engine()._store_daily_spread_stats(_options(), _summary(_et(12, 0)), cur)
    assert sorted(_option_types(cur)) == ["A", "C", "P"]


def test_scope_is_recorded_on_every_row():
    """dte_max and moneyness_band_pct travel with the reading.

    A trailing percentile compares like with like or it compares nothing, so
    the filter each row was computed under has to be readable from the row.
    """
    from src.config import SPREAD_STATS_DTE_MAX, SPREAD_STATS_MONEYNESS_BAND_PCT

    cur = MagicMock()
    _engine()._store_daily_spread_stats(_options(), _summary(_et(12, 0)), cur)
    for params in _params(cur):
        assert params[4] == int(SPREAD_STATS_DTE_MAX)
        assert params[5] == float(SPREAD_STATS_MONEYNESS_BAND_PCT)


def test_puts_row_carries_the_wider_median_than_the_calls_row():
    """The split has to survive the round trip, not be blended on the way in."""
    cur = MagicMock()
    _engine()._store_daily_spread_stats(_options(), _summary(_et(12, 0)), cur)
    by_type = {p[2]: p for p in _params(cur)}
    # index 12 == median_relative_spread_pct in the INSERT column order.
    assert by_type["P"][12] > by_type["C"][12]


def test_no_bid_contract_is_counted_but_not_measured():
    """The put wing with no bid lifts zero_bid_pct without moving the median."""
    cur = MagicMock()
    _engine()._store_daily_spread_stats(_options(), _summary(_et(12, 0)), cur)
    puts = {p[2]: p for p in _params(cur)}["P"]
    contract_count, tradable_count = puts[6], puts[7]
    zero_bid_pct = puts[9]
    assert contract_count == 2
    assert tradable_count == 1
    assert zero_bid_pct == 50.0


# ---------------------------------------------------------------------------
# Scope filtering
# ---------------------------------------------------------------------------


def test_contracts_outside_the_moneyness_band_are_excluded():
    """A far wing is structurally wide on the calmest day; including it would
    drown out the change the page exists to show."""
    options = _options() + [
        {"strike": 300.0, "option_type": "P", "expiration": TRADE_DAY,
         "bid": 0.05, "ask": 0.60, "open_interest": 10, "volume": 0},
    ]
    cur = MagicMock()
    _engine()._store_daily_spread_stats(options, _summary(_et(12, 0)), cur)
    blended = {p[2]: p for p in _params(cur)}["A"]
    assert blended[6] == 4  # the 300 strike is ~50% from spot, well outside


def test_contracts_beyond_the_dte_ceiling_are_excluded():
    options = _options() + [
        {"strike": 600.0, "option_type": "C", "expiration": date(2027, 1, 15),
         "bid": 20.0, "ask": 24.0, "open_interest": 10, "volume": 0},
    ]
    cur = MagicMock()
    _engine()._store_daily_spread_stats(options, _summary(_et(12, 0)), cur)
    blended = {p[2]: p for p in _params(cur)}["A"]
    assert blended[6] == 4


def test_nothing_in_scope_writes_nothing():
    """An empty scope is a legitimate state, not a zeroed row.

    A zeroed row would later read as "a session when the puts were fine",
    which is the opposite of what an empty chain means.
    """
    options = [
        {"strike": 300.0, "option_type": "P", "expiration": TRADE_DAY,
         "bid": 0.05, "ask": 0.60, "open_interest": 10, "volume": 0},
    ]
    cur = MagicMock()
    _engine()._store_daily_spread_stats(options, _summary(_et(12, 0)), cur)
    assert not cur.execute.called


def test_missing_spot_writes_nothing():
    cur = MagicMock()
    _engine()._store_daily_spread_stats(
        _options(), _summary(_et(12, 0), spot=0.0), cur
    )
    assert not cur.execute.called


# ---------------------------------------------------------------------------
# Robustness — this shares a transaction with the GEX persistence
# ---------------------------------------------------------------------------


def test_a_failing_upsert_is_swallowed_not_raised():
    """A liquidity rollup must never abort the GEX writes beside it."""
    cur = MagicMock()
    cur.execute.side_effect = RuntimeError("deadlock detected")
    _engine()._store_daily_spread_stats(_options(), _summary(_et(12, 0)), cur)


def test_naive_timestamp_is_treated_as_utc():
    """The schema stores TIMESTAMPTZ, but the writer must not crash if not."""
    cur = MagicMock()
    naive = _et(12, 0).replace(tzinfo=None)
    _engine()._store_daily_spread_stats(_options(), _summary(naive), cur)
    assert cur.execute.called
