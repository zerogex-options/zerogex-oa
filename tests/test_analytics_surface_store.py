"""The surface rollup writer: the time-of-day grid and the contract floors.

Everything the Spread Surface view compares against comes through here, so
a bug in this module is not a wrong pixel — it is a baseline that quietly
describes a different population than the label above the chart claims.

Pinned:

* the 30-minute grid, and the fact that the live writer and the backfill
  compute it from the SAME function (they have to interleave in one table);
* the two contract floors, and that a band-wide cell is held to the higher
  one;
* a cell whose whole population was unquotable is dropped rather than
  stored as a zero width;
* the write is an UPSERT keyed on the full scope, so a re-run of the
  backfill over a live-written bucket corrects it instead of duplicating it.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

import pytest

from src.analytics import spread_stats as ss
from src.analytics import surface_store
from src.config import (
    SPREAD_SURFACE_BUCKET_MINUTES,
    SPREAD_SURFACE_MIN_BUCKET_CONTRACTS,
    SPREAD_SURFACE_MIN_CONTRACTS,
)


def _et(hour: int, minute: int) -> dt.datetime:
    return dt.datetime(2026, 9, 10, hour, minute)


# ---------------------------------------------------------------------------
# The time-of-day grid
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hour,minute,expected,label",
    [
        (9, 30, 570, "09:30-10:00 ET"),
        (9, 31, 570, "09:30-10:00 ET"),
        (9, 59, 570, "09:30-10:00 ET"),
        (10, 0, 600, "10:00-10:30 ET"),
        (15, 42, 930, "15:30-16:00 ET"),
        (15, 59, 930, "15:30-16:00 ET"),
    ],
)
def test_the_clock_floors_to_the_half_hour(hour, minute, expected, label):
    assert surface_store.bucket_start_minutes(_et(hour, minute)) == expected
    assert surface_store.bucket_label(expected) == label


def test_the_session_covers_whole_buckets_only():
    """09:30 to 16:00, and the grid divides it exactly.

    A width that did not divide the session would leave a short terminal
    bucket whose population is a fraction of every other bucket's — and it
    would be the CLOSE, the busiest and widest part of the day, quietly
    ranked against a different amount of time.
    """
    span = surface_store.SESSION_END_MIN - surface_store.SESSION_START_MIN
    assert span % int(SPREAD_SURFACE_BUCKET_MINUTES) == 0
    assert surface_store.SESSION_START_MIN == 9 * 60 + 30
    assert surface_store.SESSION_END_MIN == 16 * 60


def test_the_backfill_seeds_the_buckets_the_live_writer_extends():
    """Two producers, one grid — otherwise history and today never align."""
    from src.tools.spread_surface_backfill import session_buckets

    buckets = session_buckets(int(SPREAD_SURFACE_BUCKET_MINUTES))
    assert buckets[0] == surface_store.SESSION_START_MIN
    assert buckets[-1] + int(SPREAD_SURFACE_BUCKET_MINUTES) == (
        surface_store.SESSION_END_MIN
    )
    for bucket in buckets:
        # Every seeded bucket is a fixed point of the live writer's floor.
        et = _et(bucket // 60, bucket % 60)
        assert surface_store.bucket_start_minutes(et) == bucket


# ---------------------------------------------------------------------------
# The floors
# ---------------------------------------------------------------------------


def _scope(money_bucket: str, count: int, median: float = 6.0):
    agg = ss.SpreadAggregate(
        contract_count=count,
        tradable_count=count,
        two_sided_pct=100.0,
        zero_bid_pct=0.0,
        crossed_or_locked_pct=0.0,
        no_quote_pct=0.0,
        median_spread=0.4,
        median_relative_spread_pct=median,
        p90_relative_spread_pct=median * 2,
        median_spread_bps_underlying=7.0,
        p90_spread_bps_underlying=14.0,
        total_open_interest=1000,
        total_volume=100,
    )
    return ss.SurfaceScope("u0", 5.0, money_bucket, agg)


def test_a_band_wide_cell_is_held_to_the_higher_floor():
    """The whole band and one slice of it are different sample sizes."""
    assert SPREAD_SURFACE_MIN_BUCKET_CONTRACTS < SPREAD_SURFACE_MIN_CONTRACTS

    floor = int(SPREAD_SURFACE_MIN_CONTRACTS)
    assert not surface_store.is_publishable(_scope(ss.BAND_WIDE, floor - 1))
    assert surface_store.is_publishable(_scope(ss.BAND_WIDE, floor))


def test_a_slice_clears_a_lower_floor_than_the_band():
    slice_key = ss.moneyness_bucket_key(-0.5, 0.5)
    floor = int(SPREAD_SURFACE_MIN_BUCKET_CONTRACTS)
    assert not surface_store.is_publishable(_scope(slice_key, floor - 1))
    assert surface_store.is_publishable(_scope(slice_key, floor))
    # And the slice floor does NOT let a thin band-wide cell through.
    assert not surface_store.is_publishable(_scope(ss.BAND_WIDE, floor))


def test_an_unquotable_population_is_dropped_however_large():
    """No median means no width — and a stored zero would read as "free"."""
    scope = _scope(ss.BAND_WIDE, 500, median=6.0)
    empty = ss.SurfaceScope(
        scope.dte_scope,
        scope.band_pct,
        scope.money_bucket,
        replace(
            scope.aggregate,
            median_relative_spread_pct=None,
            tradable_count=0,
            two_sided_pct=0.0,
            zero_bid_pct=100.0,
        ),
    )
    assert surface_store.is_publishable(scope)
    assert not surface_store.is_publishable(empty)


# ---------------------------------------------------------------------------
# The write
# ---------------------------------------------------------------------------


class _RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))


def _chain_spreads(width_pct: float, count: int = 12):
    spot = 6000.0
    rows = []
    for i in range(count):
        pct = -2.0 + (4.0 * i / max(1, count - 1))
        mid = 20.0
        half = mid * width_pct / 200.0
        rows.append(
            {
                "option_symbol": f"X{i}",
                "strike": round(spot * (1 + pct / 100.0), 2),
                "option_type": "P",
                "expiration": dt.date(2026, 9, 10),
                "bid": mid - half,
                "ask": mid + half,
            }
        )
    return list(ss.contract_spreads(rows, spot))


def test_the_write_is_an_upsert_on_the_whole_scope_key():
    """A backfill re-run over a live-written bucket corrects, not duplicates.

    The two producers overlap by design — the backfill seeds a day the live
    writer may already have touched — so the conflict target has to be the
    full scope identity, right down to the time bucket and the band.
    """
    sql = surface_store.SURFACE_UPSERT_SQL
    assert "ON CONFLICT" in sql
    for column in (
        "underlying",
        "trading_date",
        "bucket_start_min",
        "option_type",
        "dte_scope",
        "band_pct",
        "money_bucket",
    ):
        assert column in sql.split("ON CONFLICT", 1)[1].split(")", 1)[0], column
    assert "DO UPDATE" in sql
    assert "EXCLUDED.median_relative_spread_pct" in sql


def test_store_writes_one_statement_per_publishable_cell_per_side():
    spreads = _chain_spreads(6.0, count=40)
    cursor = _RecordingCursor()
    dte_of = {dt.date(2026, 9, 10): 0}

    written = surface_store.store_surface_scopes(
        cursor,
        "SPX",
        dt.date(2026, 9, 10),
        930,
        6000.0,
        dt.datetime(2026, 9, 10, 19, 42, tzinfo=dt.timezone.utc),
        {"P": spreads, "C": []},
        dte_of,
    )
    assert written == len(cursor.statements) > 0
    # Sides are never pooled: an empty call side writes nothing rather than
    # borrowing the put population.
    assert all(params[3] == "P" for _sql, params in cursor.statements)
    # Every row carries the bucket it was measured in.
    assert all(params[2] == 930 for _sql, params in cursor.statements)
