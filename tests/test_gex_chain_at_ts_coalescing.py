"""``get_gex_chain_at_ts`` must not stampede the way the strike-profile read did.

Context: docs/runbooks/strike_profile_timeseries_stampede.md — the Aug-21 cash
session where duplicate concurrent reads of one expensive GEX query blanked the
rewind chart for every user while every health check reported green.

This read is smaller, but the Gamma Shift page calls it THREE times per view
(two snapshots for the regime-shift card, one for the roll-off panel) on a
poll, so uncoalesced it reproduces the same shape of duplicate work on a new
endpoint. These tests pin the two guards that stop it:

* concurrent callers for the same frame share ONE database read;
* a repeat caller inside the TTL does no database read at all.

Plus the two ways the guard must NOT misbehave: a live caller's
``datetime.now()`` still coalesces (the key floors to the minute, or nothing
would ever share), and a failed read is not cached (a transient timeout must
not pin an empty chain for every viewer — precisely the runbook's failure).
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from src.api.database import DatabaseManager

UTC = timezone.utc
ANCHOR = datetime(2026, 8, 21, 16, 0, 0, tzinfo=UTC)


class _FakeConn:
    """Counts fetches and can be made slow or failing."""

    def __init__(self, owner: "_Harness"):
        self.owner = owner

    async def fetch(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        self.owner.fetch_count += 1
        if self.owner.delay:
            await asyncio.sleep(self.owner.delay)
        if self.owner.raises:
            raise RuntimeError("statement timeout")
        return [
            {
                "timestamp": ANCHOR,
                "spot_price": 765.4,
                "strike": 765.0,
                "expiration": None,
                "call_oi": 100,
                "put_oi": 0,
                "call_gex": 1.0,
                "put_gex": 0.0,
                "net_gex": 1.0,
            }
        ]


class _Harness:
    """A DatabaseManager with its connection acquisition stubbed out."""

    def __init__(self, delay: float = 0.0, raises: bool = False):
        self.fetch_count = 0
        self.delay = delay
        self.raises = raises
        self.db = DatabaseManager()

        harness = self

        @asynccontextmanager
        async def _acquire():
            yield _FakeConn(harness)

        self.db._acquire_connection = _acquire  # type: ignore[assignment]


def test_concurrent_callers_for_one_frame_share_a_single_read():
    """The headline guard: N simultaneous callers, one database read."""

    async def run():
        h = _Harness(delay=0.05)
        results = await asyncio.gather(
            *[h.db.get_gex_chain_at_ts("SPY", ANCHOR) for _ in range(8)]
        )
        return h, results

    h, results = asyncio.run(run())

    assert h.fetch_count == 1, f"expected one read, got {h.fetch_count}"
    assert all(r["timestamp"] == ANCHOR for r in results)
    # Every follower gets the leader's payload, not a partial one.
    assert all(len(r["rows"]) == 1 for r in results)


def test_a_repeat_caller_inside_the_ttl_does_no_read_at_all():
    async def run():
        h = _Harness()
        first = await h.db.get_gex_chain_at_ts("SPY", ANCHOR)
        second = await h.db.get_gex_chain_at_ts("SPY", ANCHOR)
        return h, first, second

    h, first, second = asyncio.run(run())

    assert h.fetch_count == 1
    assert first is second, "the cached payload should be returned as-is"


def test_live_callers_coalesce_despite_unique_now_timestamps():
    """Without the minute floor the guard would never fire where it matters.

    Live callers pass ``datetime.now()``, so every request would otherwise key
    separately and share nothing — the guard would be present and useless.
    """

    async def run():
        h = _Harness(delay=0.05)
        base = ANCHOR.replace(second=0, microsecond=0)
        # Three callers within the same minute, microseconds apart.
        stamps = [
            base + timedelta(seconds=3),
            base + timedelta(seconds=17, microseconds=42),
            base + timedelta(seconds=59, microseconds=999999),
        ]
        await asyncio.gather(*[h.db.get_gex_chain_at_ts("SPY", ts) for ts in stamps])
        return h

    assert asyncio.run(run()).fetch_count == 1


def test_different_minutes_are_separate_frames():
    async def run():
        h = _Harness()
        await h.db.get_gex_chain_at_ts("SPY", ANCHOR)
        await h.db.get_gex_chain_at_ts("SPY", ANCHOR + timedelta(minutes=1))
        return h

    assert asyncio.run(run()).fetch_count == 2


def test_different_symbols_do_not_share_a_frame():
    async def run():
        h = _Harness()
        await h.db.get_gex_chain_at_ts("SPY", ANCHOR)
        await h.db.get_gex_chain_at_ts("QQQ", ANCHOR)
        return h

    assert asyncio.run(run()).fetch_count == 2


def test_different_row_caps_do_not_share_a_frame():
    """A truncated read must never be served to a caller that asked for more."""

    async def run():
        h = _Harness()
        await h.db.get_gex_chain_at_ts("SPY", ANCHOR, max_rows=100)
        await h.db.get_gex_chain_at_ts("SPY", ANCHOR, max_rows=6000)
        return h

    assert asyncio.run(run()).fetch_count == 2


def test_a_failed_read_is_not_cached():
    """A transient timeout must not pin an empty chain for the whole TTL.

    That is the runbook's failure mode exactly: one slow query becoming a
    blank surface for every viewer until the entry expires.
    """

    async def run():
        h = _Harness(raises=True)
        first = await h.db.get_gex_chain_at_ts("SPY", ANCHOR)
        h.raises = False
        second = await h.db.get_gex_chain_at_ts("SPY", ANCHOR)
        return h, first, second

    h, first, second = asyncio.run(run())

    assert first == {"timestamp": None, "spot": None, "rows": []}
    assert h.fetch_count == 2, "the retry must reach the database"
    assert second["timestamp"] == ANCHOR, "and must be allowed to succeed"


def test_an_empty_chain_is_not_cached_either():
    """Same reasoning: a symbol whose data has not landed yet must not be
    pinned as empty for every caller in the window."""

    async def run():
        h = _Harness()
        original = _FakeConn.fetch

        async def empty(self, query, *args):
            h.fetch_count += 1
            return []

        _FakeConn.fetch = empty  # type: ignore[assignment]
        try:
            first = await h.db.get_gex_chain_at_ts("NEW", ANCHOR)
            _FakeConn.fetch = original  # type: ignore[assignment]
            second = await h.db.get_gex_chain_at_ts("NEW", ANCHOR)
        finally:
            _FakeConn.fetch = original  # type: ignore[assignment]
        return h, first, second

    h, first, second = asyncio.run(run())

    assert first["rows"] == []
    assert h.fetch_count == 2
    assert second["timestamp"] == ANCHOR


def test_a_failing_leader_does_not_poison_its_followers():
    """Followers get the leader's (degraded) result rather than an exception
    escaping into three different endpoints' handlers."""

    async def run():
        h = _Harness(delay=0.05, raises=True)
        results = await asyncio.gather(
            *[h.db.get_gex_chain_at_ts("SPY", ANCHOR) for _ in range(4)],
            return_exceptions=True,
        )
        return h, results

    h, results = asyncio.run(run())

    assert not any(isinstance(r, BaseException) for r in results)
    assert all(r == {"timestamp": None, "spot": None, "rows": []} for r in results)
