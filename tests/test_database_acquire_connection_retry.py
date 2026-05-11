"""Tests for ``DatabaseManager._acquire_connection`` retry semantics.

The 2026-05-11 prod incident exposed two bugs in the original retry
plumbing:

* **Bug A** (fixed in a separate PR): statement-level ``TimeoutError`` was
  classified as a *transient pool* error and triggered pool-wide reconnects.
* **Bug B** (fixed here): the retry happened *inside* the ``async with``
  body, which meant a use-time exception followed by ``continue`` and a
  second ``yield`` produced ``RuntimeError: generator didn't stop after
  athrow()`` on top of the original exception.

After the fix, retry happens **before** the yield, in a helper coroutine
(``_acquire_with_retry``).  The yield happens exactly once per call.
Use-time exceptions propagate as-is.
"""

from __future__ import annotations

import asyncio
from typing import Any, List
from unittest.mock import AsyncMock

import pytest

from src.api.database import DatabaseManager


class _FakeConn:
    """Bare-minimum stand-in for an asyncpg.Connection."""


class _Pool:
    """Pool stub whose ``acquire()`` returns a connection or raises."""

    def __init__(self, behaviors: List[Any]) -> None:
        """``behaviors`` is consumed in order on each ``acquire()`` call.

        Each entry is either an ``Exception`` instance (raised) or a
        ``_FakeConn`` (returned).
        """
        self._behaviors = list(behaviors)
        self.acquire_calls = 0
        self.release_calls: List[_FakeConn] = []
        self._is_closing = False

    def is_closing(self) -> bool:
        return self._is_closing

    async def acquire(self) -> _FakeConn:
        self.acquire_calls += 1
        next_behavior = self._behaviors.pop(0)
        if isinstance(next_behavior, Exception):
            raise next_behavior
        return next_behavior

    async def release(self, conn: _FakeConn) -> None:
        self.release_calls.append(conn)

    async def close(self) -> None:
        self._is_closing = True


def _make_db_with_pools(initial_pool: _Pool, replacement_pool: _Pool) -> DatabaseManager:
    """Build a DatabaseManager whose ``self.pool`` starts at ``initial_pool``
    and is swapped to ``replacement_pool`` on every ``_reconnect_pool`` call.
    """
    db = DatabaseManager()
    db.pool = initial_pool  # type: ignore[assignment]

    async def fake_reconnect() -> None:
        # Mirror the real _reconnect_pool behavior: swap self.pool to a
        # fresh instance and close the old one.  No actual DB work.
        old = db.pool
        db.pool = replacement_pool  # type: ignore[assignment]
        if old is not None:
            await old.close()

    db._reconnect_pool = fake_reconnect  # type: ignore[assignment]
    return db


def test_acquire_time_transient_error_triggers_retry_on_new_pool():
    """A stale pooled connection (raises 'connection is closed' on acquire)
    must trigger one reconnect + retry against the fresh pool."""
    fresh_conn = _FakeConn()
    stale_pool = _Pool([ConnectionError("connection is closed")])
    fresh_pool = _Pool([fresh_conn])
    db = _make_db_with_pools(stale_pool, fresh_pool)

    async def run() -> _FakeConn:
        async with db._acquire_connection() as conn:
            return conn

    result = asyncio.run(run())

    assert result is fresh_conn, "second attempt must serve from the fresh pool"
    assert stale_pool.acquire_calls == 1
    assert fresh_pool.acquire_calls == 1
    assert fresh_pool.release_calls == [fresh_conn], "release must go to the fresh pool"
    # Old pool is never asked to release the conn we never got from it.
    assert stale_pool.release_calls == []


def test_non_transient_acquire_error_propagates_without_retry():
    """A non-transient acquire-time error (e.g. ValueError) must surface
    immediately — no reconnect, no second acquire."""
    stale_pool = _Pool([ValueError("not a transient DB error")])
    fresh_pool = _Pool([_FakeConn()])
    db = _make_db_with_pools(stale_pool, fresh_pool)

    async def run() -> None:
        async with db._acquire_connection() as _:
            pytest.fail("should never enter the body")

    with pytest.raises(ValueError, match="not a transient DB error"):
        asyncio.run(run())

    assert stale_pool.acquire_calls == 1
    assert fresh_pool.acquire_calls == 0, "must NOT retry on non-transient errors"


def test_use_time_exception_is_not_retried_and_propagates_cleanly():
    """The regression test for Bug B: an exception inside the `async with`
    body — including one that LOOKS transient — must NOT trigger a retry
    and must NOT produce 'generator didn't stop after athrow()'.
    The original exception must propagate as-is."""
    conn = _FakeConn()
    pool = _Pool([conn])
    fresh_pool = _Pool([_FakeConn()])
    db = _make_db_with_pools(pool, fresh_pool)

    async def run() -> None:
        async with db._acquire_connection() as _:
            # Simulate RDS dropping the connection mid-query: the error
            # message matches _is_transient_db_error's classifier.
            raise ConnectionError("connection is closed")

    with pytest.raises(ConnectionError, match="connection is closed"):
        asyncio.run(run())

    # No retry on use-time errors.
    assert pool.acquire_calls == 1
    assert fresh_pool.acquire_calls == 0
    # Connection still released back to the originating pool.
    assert pool.release_calls == [conn]


def test_release_failure_is_logged_but_does_not_overshadow_use_error():
    """If pool.release(conn) itself fails (e.g. pool already closed during
    use), the original use-time exception still wins — the release error
    is logged only."""
    conn = _FakeConn()

    class _PoolWithBrokenRelease(_Pool):
        async def release(self, c):  # type: ignore[override]
            raise RuntimeError("release failed — pool already closing")

    pool = _PoolWithBrokenRelease([conn])
    fresh_pool = _Pool([_FakeConn()])
    db = _make_db_with_pools(pool, fresh_pool)

    async def run() -> None:
        async with db._acquire_connection() as _:
            raise ValueError("real underlying error")

    # The body's ValueError must propagate — the release-time RuntimeError
    # must NOT shadow it.
    with pytest.raises(ValueError, match="real underlying error"):
        asyncio.run(run())


def test_normal_path_acquires_and_releases_exactly_once():
    """Smoke test: happy path acquires from the pool, yields the conn,
    releases it once."""
    conn = _FakeConn()
    pool = _Pool([conn])
    fresh_pool = _Pool([_FakeConn()])
    db = _make_db_with_pools(pool, fresh_pool)

    body_calls: List[_FakeConn] = []

    async def run() -> None:
        async with db._acquire_connection() as c:
            body_calls.append(c)

    asyncio.run(run())

    assert body_calls == [conn]
    assert pool.acquire_calls == 1
    assert pool.release_calls == [conn]
    assert fresh_pool.acquire_calls == 0
