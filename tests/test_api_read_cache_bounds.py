"""Bounds on the process-local read caches.

Both caches here are pure latency optimisations in front of the DB, so
eviction can never change a response — but an unbounded one is a slow leak
in a worker whose whole cgroup budget is shared with its sibling worker.
These tests pin the bounds themselves:

  * ``DatabaseManager._read_cache`` is capped on entry count AND on
    approximate content size, so a handful of very wide payloads cannot sit
    inside a 2048-entry allowance and dwarf everything else.
  * ``forced_flow._cache`` is capped at all — it previously grew without
    limit, since a write only ever replaced its own key.
"""

from __future__ import annotations

from unittest.mock import patch

from src.api import database as database_module
from src.api.database import DatabaseManager, _cache_weight

# ---------------------------------------------------------------------------
# _cache_weight
# ---------------------------------------------------------------------------


def test_weight_of_scalar_and_empty_payloads_is_one():
    assert _cache_weight(None) == 1
    assert _cache_weight({"a": 1}) == 1
    assert _cache_weight(42) == 1
    assert _cache_weight([]) == 1


def test_weight_counts_flat_rows():
    assert _cache_weight([{"a": 1}, {"a": 2}, {"a": 3}]) == 3


def test_weight_extrapolates_the_widest_nested_list():
    """A bucket payload's cost is dominated by its per-strike list, not by the
    bucket count, so the nested width has to be part of the estimate."""
    payload = [{"timestamp": i, "strikes": [{}] * 10} for i in range(5)]
    assert _cache_weight(payload) == 5 * (1 + 10)


def test_weight_is_o1_and_samples_rather_than_walks():
    """Must not walk the payload — it runs on every cache write. A middle
    element wider than the ends is still picked up by the sampling."""
    payload = [{"strikes": []} for _ in range(1000)]
    payload[500]["strikes"] = [{}] * 7
    assert _cache_weight(payload) == 1000 * (1 + 7)


# ---------------------------------------------------------------------------
# DatabaseManager._read_cache
# ---------------------------------------------------------------------------


def _db_with_clock(fake_time):
    db = DatabaseManager()
    patcher = patch.object(database_module.time_module, "monotonic", lambda: fake_time[0])
    patcher.start()
    return db, patcher


def test_entry_count_bound_still_holds():
    fake_time = [0.0]
    db, patcher = _db_with_clock(fake_time)
    try:
        db._read_cache_maxsize = 64  # floor of the env-configurable bound
        for i in range(200):
            db._cache_set(f"k{i}", [{"a": i}], 60.0)
        assert len(db._read_cache) <= 64
        # The most recent writes survive; the oldest were evicted.
        assert db._cache_get("k199") is not None
        assert db._cache_get("k0") is None
    finally:
        patcher.stop()


def test_size_bound_evicts_even_when_entry_count_is_tiny():
    """Ten wide payloads sit far under the 2048-entry cap but blow the size
    budget — the entry cap alone would have let them all stay resident."""
    fake_time = [0.0]
    db, patcher = _db_with_clock(fake_time)
    try:
        db._read_cache_max_units = 1000
        for i in range(10):
            # 20 buckets x 30 strikes -> 20 * 31 = 620 units each.
            db._cache_set(f"wide{i}", [{"strikes": [{}] * 30} for _ in range(20)], 60.0)
        assert len(db._read_cache) < 10
        assert db._read_cache_units <= 1000
    finally:
        patcher.stop()


def test_running_unit_total_tracks_evictions_and_replacements():
    """The total must not drift: overwriting a key or expiring one has to
    decrement, or the cache slowly refuses to hold anything."""
    fake_time = [0.0]
    db, patcher = _db_with_clock(fake_time)
    try:
        db._cache_set("a", [{"x": 1}] * 10, 60.0)
        db._cache_set("b", [{"x": 1}] * 5, 60.0)
        assert db._read_cache_units == 15

        # Replacing a key swaps its contribution rather than adding to it.
        db._cache_set("a", [{"x": 1}] * 2, 60.0)
        assert db._read_cache_units == 7

        # Expiry via _cache_get decrements too.
        fake_time[0] = 1000.0
        assert db._cache_get("a") is None
        assert db._read_cache_units == 5

        # And the total equals the sum of what is actually resident.
        assert db._read_cache_units == sum(v[2] for v in db._read_cache.values())
    finally:
        patcher.stop()


def test_oversized_single_payload_is_still_cached():
    """The widest responses are exactly the ones whose TTL protects the DB.
    One payload bigger than the whole budget must still be served from cache
    rather than re-queried on every poll."""
    fake_time = [0.0]
    db, patcher = _db_with_clock(fake_time)
    try:
        db._read_cache_max_units = 100
        huge = [{"strikes": [{}] * 50} for _ in range(50)]  # 2550 units
        db._cache_set("huge", huge, 60.0)
        assert db._cache_get("huge") is huge
        # It is alone — everything else was evicted to make room.
        assert len(db._read_cache) == 1
    finally:
        patcher.stop()


def test_zero_ttl_disables_caching():
    db = DatabaseManager()
    db._cache_set("k", [{"a": 1}], 0.0)
    assert db._cache_get("k") is None
    assert db._read_cache_units == 0


# ---------------------------------------------------------------------------
# forced_flow._cache
# ---------------------------------------------------------------------------


def test_forced_flow_cache_is_bounded():
    from src.api.routers import forced_flow

    forced_flow._cache.clear()
    try:
        for i in range(forced_flow._RESPONSE_CACHE_MAX + 150):
            forced_flow._cache_put(("backtest", "SPY", i), {"lookback_days": i})
        assert len(forced_flow._cache) <= forced_flow._RESPONSE_CACHE_MAX
        # Most recent write survived.
        last = forced_flow._RESPONSE_CACHE_MAX + 149
        assert forced_flow._cache_get(("backtest", "SPY", last)) == {"lookback_days": last}
    finally:
        forced_flow._cache.clear()


def test_forced_flow_rewrite_refreshes_eviction_position():
    """The warmer rewrites the same key every 30s. That key must not be
    evicted as 'oldest' just because it was first inserted long ago."""
    from src.api.routers import forced_flow

    forced_flow._cache.clear()
    try:
        hot = ("session-surface", "SPX", 0.02, 30, 8, None)
        forced_flow._cache_put(hot, {"v": "first"})
        for i in range(forced_flow._RESPONSE_CACHE_MAX - 1):
            forced_flow._cache_put(("backtest", "SPY", i), {"v": i})
        # Warmer refreshes the hot key, then more distinct keys arrive.
        forced_flow._cache_put(hot, {"v": "refreshed"})
        for i in range(100):
            forced_flow._cache_put(("backtest", "QQQ", i), {"v": i})

        assert len(forced_flow._cache) <= forced_flow._RESPONSE_CACHE_MAX
        assert forced_flow._cache_get(hot) == {"v": "refreshed"}
    finally:
        forced_flow._cache.clear()


def test_forced_flow_cache_still_returns_values_within_ttl():
    """Bounding must not change the cache's actual job."""
    from src.api.routers import forced_flow

    forced_flow._cache.clear()
    try:
        forced_flow._cache_put(("levels", "SPY"), {"gamma_flip": 500.0})
        assert forced_flow._cache_get(("levels", "SPY")) == {"gamma_flip": 500.0}
        assert forced_flow._cache_get(("levels", "QQQ")) is None
    finally:
        forced_flow._cache.clear()
