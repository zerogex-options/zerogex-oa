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

from src.api.database import (
    DatabaseManager,
    _strike_profile_bucket_cache_key,
    _strike_profile_ts_cache_key,
)

_T0 = datetime(2026, 8, 12, 17, 0, tzinfo=timezone.utc)
_BUCKETS = [_T0 + timedelta(minutes=5 * i) for i in range(10)]


def _db(chunk_units, buckets=None, per_chunk=None, bucket_ttl=0.0):
    """Manager with the DB replaced by canned bucket-list / chunk responses."""
    db = DatabaseManager()
    db._strike_profile_timeseries_chunk_units = chunk_units
    db._strike_profile_bucket_cache_ttl_seconds = bucket_ttl
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


# ---------------------------------------------------------------------------
# Per-bucket caching
# ---------------------------------------------------------------------------


def test_only_the_live_bucket_is_re_read_once_the_window_is_warm():
    """The point of the whole thing: a poll costs one bucket, not the window.

    Every bucket but the newest is closed — fixed representative timestamp,
    final OHLC, settled per-strike aggregate — so re-aggregating them on each
    poll is pure waste.
    """
    db, calls = _db(chunk_units=0, bucket_ttl=900.0)
    first = _run(db)
    assert calls[0] == _BUCKETS  # cold: everything

    calls.clear()
    # Expire the WINDOW entry but leave the per-bucket ones: that is the real
    # steady state, since the window TTL is short and the bucket TTL is long.
    # Without this the second poll short-circuits on the window cache and
    # never exercises the path under test.
    db._cache_drop(_strike_profile_ts_cache_key("SPY", "5min", 10, None))
    second = _run(db)
    assert calls == [[_BUCKETS[-1]]]  # warm: the live bucket alone
    assert second == first


def test_the_live_bucket_is_never_written_to_the_bucket_cache():
    """It is still filling — caching it would pin a half-formed bar."""
    db, _ = _db(chunk_units=0, bucket_ttl=900.0)
    _run(db)
    closed_key = _strike_profile_bucket_cache_key("SPY", "5min", None, _BUCKETS[0])
    live_key = _strike_profile_bucket_cache_key("SPY", "5min", None, _BUCKETS[-1])
    assert db._cache_get(closed_key) is not None
    assert db._cache_get(live_key) is None


def test_a_cached_bucket_is_stored_as_a_list_so_its_size_is_counted():
    """``_cache_weight`` scores a bare dict as ONE unit however much it holds,
    so a bucket cached bare would sit in the cache's size bound while actually
    holding a few hundred strikes. Wrapping it restores real accounting."""
    db, _ = _db(chunk_units=0, bucket_ttl=900.0)
    _run(db)
    entry = db._cache_get(_strike_profile_bucket_cache_key("SPY", "5min", None, _BUCKETS[0]))
    assert isinstance(entry, list) and len(entry) == 1


def test_windows_of_different_widths_share_their_overlapping_buckets():
    """The bucket key carries no window size, so a narrower request reuses the
    buckets a wider one already paid for."""
    db, calls = _db(chunk_units=0, bucket_ttl=900.0)
    _run(db, window_units=10)
    calls.clear()

    # Narrower window over the same buckets: only the live one is unresolved.
    _run(db, window_units=6)
    assert calls == [[_BUCKETS[-1]]]


def test_bucket_cache_off_reads_the_whole_window_every_time():
    db, calls = _db(chunk_units=0, bucket_ttl=0.0)
    _run(db)
    calls.clear()
    _run(db)
    assert calls == [None]  # single-query path, nothing reused


def test_an_unresolved_bucket_fails_the_window_rather_than_shortening_it():
    """A short window would read as a legitimately shorter session rather
    than a fault, so it must not be served."""

    def _drops_a_bucket(subset):
        return [{"timestamp": b} for b in subset if b != _BUCKETS[3]]

    # chunk_units>0 so the assembled path runs at all; the single-query path
    # has no bucket list to check completeness against.
    db, _ = _db(chunk_units=4, per_chunk=_drops_a_bucket)
    assert _run(db) == []
