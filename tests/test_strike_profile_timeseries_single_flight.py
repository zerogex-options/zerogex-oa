"""Stampede coverage for the rewind-chart read's single-flight guard.

``/api/gex/strike-profile-timeseries`` is polled at ~1 Hz per open chart and
fans out across symbols and expiration filters, so a cold cache key is asked
for many times before the first answer exists.  The TTL cache cannot absorb
that by itself — it only helps once a value has been WRITTEN, and nothing
writes one until a query finishes.  In a cash session that turned into a
self-sustaining collapse: every poll missed, every miss ran its own
multi-second aggregate, concurrent duplicates of one query saturated the
instance's CPU, each copy then blew the 15 s guard and returned ``[]``
*without* caching, so the next poll missed too.

``DatabaseManager._single_flight`` breaks that by letting one producer run per
key while the rest wait on it.  These tests pin the properties that make it
safe to sit in front of a live read:

  * concurrent callers on one key produce exactly one producer run
  * distinct keys stay independent
  * a producer failure reaches the leader AND every follower
  * the in-flight entry is always released, on success and on failure
  * a follower that is cancelled (client disconnect) cannot cancel the
    producer the other followers are still waiting on
  * it is a duplicate-work guard, not a cache — sequential callers re-run
  * the wrapper still short-circuits on a cache hit, and normalises before
    it picks the key it coalesces on
"""

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from src.api.database import DatabaseManager, _strike_profile_ts_cache_key

_D20 = date(2026, 8, 20)
_D21 = date(2026, 8, 21)


class _Producer:
    """Gated producer that records every call. Set ``boom`` to make it raise."""

    def __init__(self, result=None, boom: BaseException | None = None):
        self.calls: list = []
        self.started = asyncio.Event()
        self.gate = asyncio.Event()
        self._result = result if result is not None else ["payload"]
        self._boom = boom

    async def __call__(self, **kwargs):
        self.calls.append(kwargs)
        self.started.set()
        await self.gate.wait()
        if self._boom is not None:
            raise self._boom
        return self._result


# ---------------------------------------------------------------------------
# _single_flight
# ---------------------------------------------------------------------------


def test_concurrent_callers_on_one_key_run_the_producer_once():
    db = DatabaseManager()
    prod = _Producer()

    async def main():
        leader = asyncio.create_task(db._single_flight("k", prod))
        await prod.started.wait()
        followers = [asyncio.create_task(db._single_flight("k", prod)) for _ in range(4)]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        prod.gate.set()
        return await asyncio.gather(leader, *followers)

    results = asyncio.run(main())
    assert len(prod.calls) == 1
    # Every caller gets the leader's object, not a re-computed equal one.
    assert all(r is results[0] for r in results)


def test_distinct_keys_do_not_share_a_flight():
    db = DatabaseManager()
    prod = _Producer()

    async def main():
        a = asyncio.create_task(db._single_flight("a", prod))
        await prod.started.wait()
        b = asyncio.create_task(db._single_flight("b", prod))
        await asyncio.sleep(0)
        prod.gate.set()
        return await asyncio.gather(a, b)

    asyncio.run(main())
    assert len(prod.calls) == 2


def test_producer_failure_reaches_leader_and_followers():
    db = DatabaseManager()
    prod = _Producer(boom=RuntimeError("query exploded"))

    async def main():
        leader = asyncio.create_task(db._single_flight("k", prod))
        await prod.started.wait()
        follower = asyncio.create_task(db._single_flight("k", prod))
        await asyncio.sleep(0)
        prod.gate.set()
        return await asyncio.gather(leader, follower, return_exceptions=True)

    leader_exc, follower_exc = asyncio.run(main())
    assert isinstance(leader_exc, RuntimeError)
    assert isinstance(follower_exc, RuntimeError)
    # A failure must not leave the key wedged for every later caller.
    assert db._inflight == {}


def test_inflight_entry_is_released_after_success():
    db = DatabaseManager()
    prod = _Producer()
    prod.gate.set()
    asyncio.run(db._single_flight("k", prod))
    assert db._inflight == {}


def test_cancelled_follower_does_not_cancel_the_leader():
    """A client disconnecting cancels ITS task; the shared producer other
    followers still depend on has to survive that."""
    db = DatabaseManager()
    prod = _Producer()

    async def main():
        leader = asyncio.create_task(db._single_flight("k", prod))
        await prod.started.wait()
        doomed = asyncio.create_task(db._single_flight("k", prod))
        survivor = asyncio.create_task(db._single_flight("k", prod))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        doomed.cancel()
        await asyncio.sleep(0)
        prod.gate.set()
        return await asyncio.gather(leader, survivor)

    leader_result, survivor_result = asyncio.run(main())
    assert leader_result == ["payload"]
    assert survivor_result is leader_result
    assert len(prod.calls) == 1


def test_single_flight_is_not_a_cache():
    """Sequential (non-overlapping) callers must re-run: caching stays the
    producer's job, so the "a timed-out read is not cached" rule is intact."""
    db = DatabaseManager()
    prod = _Producer()
    prod.gate.set()
    asyncio.run(db._single_flight("k", prod))
    asyncio.run(db._single_flight("k", prod))
    assert len(prod.calls) == 2


# ---------------------------------------------------------------------------
# get_strike_profile_timeseries wrapper
# ---------------------------------------------------------------------------


def test_wrapper_serves_a_cache_hit_without_touching_the_read():
    db = DatabaseManager()
    payload = [{"timestamp": "t"}]
    db._cache_set(_strike_profile_ts_cache_key("SPX", "5min", 78, None), payload, 60.0)
    prod = _Producer()
    db._get_strike_profile_timeseries_uncached = prod

    got = asyncio.run(
        db.get_strike_profile_timeseries(symbol="SPX", timeframe="5min", window_units=78)
    )
    assert got is payload
    assert prod.calls == []


def test_wrapper_coalesces_concurrent_identical_requests():
    db = DatabaseManager()
    prod = _Producer(result=[{"timestamp": "t"}])
    db._get_strike_profile_timeseries_uncached = prod

    async def main():
        leader = asyncio.create_task(
            db.get_strike_profile_timeseries(symbol="SPX", timeframe="5min", window_units=78)
        )
        await prod.started.wait()
        followers = [
            asyncio.create_task(
                # lower-case symbol: normalisation has to happen BEFORE the
                # key is chosen, or these would each start their own read.
                db.get_strike_profile_timeseries(symbol="spx", timeframe="5min", window_units=78)
            )
            for _ in range(3)
        ]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        prod.gate.set()
        return await asyncio.gather(leader, *followers)

    results = asyncio.run(main())
    assert len(prod.calls) == 1
    assert all(r is results[0] for r in results)


def test_wrapper_coalesces_equivalent_expiration_scopes():
    """Same expiration SET, different order and a duplicate — one read."""
    db = DatabaseManager()
    prod = _Producer(result=[{"timestamp": "t"}])
    db._get_strike_profile_timeseries_uncached = prod

    async def main():
        leader = asyncio.create_task(
            db.get_strike_profile_timeseries(symbol="SPX", expirations=[_D21, _D20])
        )
        await prod.started.wait()
        follower = asyncio.create_task(
            db.get_strike_profile_timeseries(symbol="SPX", expirations=[_D20, _D21, _D20])
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        prod.gate.set()
        return await asyncio.gather(leader, follower)

    results = asyncio.run(main())
    assert len(prod.calls) == 1
    assert results[1] is results[0]
    # and the normalised scope is what reached the read
    assert prod.calls[0]["expirations"] == [_D20, _D21]


@pytest.mark.parametrize(
    "exp_filter, expected_scope",
    [(None, "all"), ([_D20], "2026-08-20"), ([_D20, _D21], "2026-08-20,2026-08-21")],
)
def test_cache_key_shape(exp_filter, expected_scope):
    assert (
        _strike_profile_ts_cache_key("SPX", "5min", 78, exp_filter)
        == f"strike_profile_ts:SPX:5min:78:{expected_scope}"
    )


def test_cancelled_leader_does_not_strand_its_followers():
    """The producer is owned by the flight, not by whoever arrived first.

    A leader disconnecting mid-read is routine here — the chart polls at ~1 Hz
    against a multi-second read — and if the work were tied to that caller its
    cancellation would take down every follower queued behind it, which is
    strictly worse than the un-coalesced behaviour this replaces.
    """
    db = DatabaseManager()
    prod = _Producer()

    async def main():
        leader = asyncio.create_task(db._single_flight("k", prod))
        await prod.started.wait()
        followers = [asyncio.create_task(db._single_flight("k", prod)) for _ in range(2)]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        leader.cancel()
        await asyncio.sleep(0)
        prod.gate.set()
        return await asyncio.gather(leader, *followers, return_exceptions=True)

    outcomes = asyncio.run(main())
    assert isinstance(outcomes[0], asyncio.CancelledError)
    assert all(r == ["payload"] for r in outcomes[1:])
    assert len(prod.calls) == 1
    assert db._inflight == {}
