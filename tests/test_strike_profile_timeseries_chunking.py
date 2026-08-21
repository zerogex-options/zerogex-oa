"""Chunk-driver behaviour for the rewind-chart read (no database needed).

Value-level parity between a chunked window and a single-query one is pinned
against a real Postgres in
``test_strike_profile_timeseries_chunked_parity.py``.  What is left here is
the driver's own contract, which is where the operational risk lives:

  * chunking engages only when it was asked for AND the window needs it
  * chunks are cut from the bucket list, cover every bucket once, and the
    parts concatenate in order
  * the ASSEMBLED window is what gets cached — never a chunk on its own,
    which is a slice and would poison the window's key
  * a chunk that comes back empty aborts the whole window rather than
    assembling around the hole.  The bucket list already established those
    buckets exist, so an empty chunk means that read hit its guard; splicing
    the remainder together would silently drop history out of the MIDDLE of
    a chart, which is worse than returning nothing and retrying.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from src.api.database import DatabaseManager, _strike_profile_ts_cache_key

_T0 = datetime(2026, 8, 12, 17, 0, tzinfo=timezone.utc)
_BUCKETS = [_T0 + timedelta(minutes=5 * i) for i in range(10)]


def _db(chunk_units, buckets=None, per_chunk=None):
    """Manager with the DB replaced by canned bucket-list / chunk responses."""
    db = DatabaseManager()
    db._strike_profile_timeseries_chunk_units = chunk_units
    calls = []

    async def _bucket_list(symbol, timeframe, window_units):
        return list(_BUCKETS if buckets is None else buckets)

    async def _read(**kwargs):
        subset = kwargs.get("bucket_subset")
        calls.append(subset)
        if per_chunk is not None:
            return per_chunk(subset)
        if subset is None:
            return [{"timestamp": b} for b in _BUCKETS]
        return [{"timestamp": b} for b in subset]

    db._strike_profile_bucket_list = _bucket_list  # type: ignore[method-assign]
    db._get_strike_profile_timeseries_uncached = _read  # type: ignore[method-assign]
    return db, calls


def _run(db, window_units=10):
    return asyncio.run(
        db.get_strike_profile_timeseries(symbol="SPY", timeframe="5min", window_units=window_units)
    )


def test_single_query_path_when_chunking_disabled():
    db, calls = _db(chunk_units=0)
    _run(db)
    assert calls == [None]


def test_single_query_path_when_the_window_fits_in_one_chunk():
    """Splitting a window that already fits would only add round trips."""
    db, calls = _db(chunk_units=10)
    _run(db, window_units=10)
    assert calls == [None]


def test_chunks_cover_every_bucket_exactly_once_and_concatenate_in_order():
    db, calls = _db(chunk_units=4)
    result = _run(db)
    assert [len(c) for c in calls] == [4, 4, 2]
    flat = [b for c in calls for b in c]
    assert flat == _BUCKETS  # ordered, complete, no bucket in two chunks
    assert [r["timestamp"] for r in result] == _BUCKETS


def test_the_assembled_window_is_what_gets_cached():
    db, _ = _db(chunk_units=4)
    result = _run(db)
    key = _strike_profile_ts_cache_key("SPY", "5min", 10, None)
    assert db._cache_get(key) == result
    assert len(result) == len(_BUCKETS)


def test_an_empty_chunk_aborts_the_window_and_caches_nothing():
    def _second_chunk_times_out(subset):
        if subset and subset[0] == _BUCKETS[4]:
            return []
        return [{"timestamp": b} for b in subset]

    db, calls = _db(chunk_units=4, per_chunk=_second_chunk_times_out)
    result = _run(db)
    assert result == []
    # stopped at the bad chunk rather than assembling around the hole
    assert len(calls) == 2
    assert db._cache_get(_strike_profile_ts_cache_key("SPY", "5min", 10, None)) is None


def test_empty_bucket_list_short_circuits_without_reading():
    db, calls = _db(chunk_units=4, buckets=[])
    assert _run(db) == []
    assert calls == []
