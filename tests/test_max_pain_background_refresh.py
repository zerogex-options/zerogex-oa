"""Tests for the background max-pain snapshot refresh.

Covers:

1. ``DatabaseManager.refresh_max_pain_snapshots`` iterates symbols, wraps each
   recompute in a transaction, and applies ``SET LOCAL statement_timeout`` so
   the heavy CTE chain can run beyond the pool's default 30s timeout.
2. A per-symbol failure in the background refresh is logged but does not abort
   the loop — remaining symbols are still attempted.
3. ``get_max_pain_current`` skips the inline ``_refresh_max_pain_snapshot``
   call for symbols in ``MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS`` when the
   background refresh is enabled.
4. Unlisted symbols still fall through to the original on-demand recompute,
   so callers polling for symbols outside the background-refresh set retain
   the prior behavior.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, List

from src.api.database import DatabaseManager


class _FakeBackgroundConn:
    """Stand-in for an asyncpg.Connection used by the background refresh."""

    def __init__(self) -> None:
        self.execute_calls: List[tuple] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.execute_calls.append((query, args))
        return "SET"

    def transaction(self):
        outer = self

        class _Tx:
            async def __aenter__(self_inner) -> "_FakeBackgroundConn":
                return outer

            async def __aexit__(self_inner, *exc) -> bool:
                return False

        return _Tx()


def _install_fake_acquire(db: DatabaseManager, conn: _FakeBackgroundConn) -> None:
    """Replace ``db._acquire_connection`` with an asynccontextmanager
    that yields the given fake connection on every call."""

    @asynccontextmanager
    async def fake_acquire():
        yield conn

    db._acquire_connection = fake_acquire  # type: ignore[assignment]


def test_refresh_iterates_symbols_with_set_local_timeout():
    """Each symbol gets _refresh_max_pain_snapshot called inside a transaction
    with the configured statement_timeout applied via SET LOCAL, and that same
    budget is also forwarded as a per-call ``timeout=`` so asyncpg's
    client-side ``command_timeout`` (default 30s on this pool) doesn't fire
    before the server-side cancel."""
    db = DatabaseManager()
    conn = _FakeBackgroundConn()
    _install_fake_acquire(db, conn)

    refresh_calls: List[tuple] = []

    async def fake_refresh(c, symbol, strike_limit, timeout=None):
        refresh_calls.append((symbol, strike_limit, timeout))

    db._refresh_max_pain_snapshot = fake_refresh  # type: ignore[assignment]

    asyncio.run(db.refresh_max_pain_snapshots(["spy", "spx"], 500, 120_000))

    assert refresh_calls == [("SPY", 500, 120.0), ("SPX", 500, 120.0)]

    set_local = [q for (q, _) in conn.execute_calls if "SET LOCAL statement_timeout" in q]
    assert len(set_local) == 2, "expected one SET LOCAL per symbol"
    assert all("120000" in q for q in set_local)


def test_refresh_continues_after_per_symbol_failure():
    """If one symbol's refresh raises, the loop logs and proceeds with the
    remaining symbols — the background task must not crash on a single bad
    underlying."""
    db = DatabaseManager()
    conn = _FakeBackgroundConn()
    _install_fake_acquire(db, conn)

    attempts: List[str] = []

    async def fake_refresh(c, symbol, strike_limit, timeout=None):
        attempts.append(symbol)
        if symbol == "SPY":
            raise RuntimeError("simulated DB error")

    db._refresh_max_pain_snapshot = fake_refresh  # type: ignore[assignment]

    asyncio.run(db.refresh_max_pain_snapshots(["SPY", "SPX", "QQQ"], 500, 120_000))

    # SPY raised, but SPX and QQQ were still attempted.
    assert attempts == ["SPY", "SPX", "QQQ"]


class _FakeReadConn:
    """Stand-in for the connection inside get_max_pain_current."""

    async def fetchrow(self, query: str, *args: Any):
        return None

    async def fetch(self, query: str, *args: Any) -> List[Any]:
        return []


def test_get_max_pain_current_skips_inline_refresh_for_background_symbols():
    """When background refresh is enabled and the symbol is in the list,
    get_max_pain_current must NOT call _refresh_max_pain_snapshot inline."""
    db = DatabaseManager()
    db._max_pain_background_refresh_enabled = True
    db._max_pain_background_refresh_symbols = frozenset(["SPY"])

    @asynccontextmanager
    async def fake_acquire():
        yield _FakeReadConn()

    db._acquire_connection = fake_acquire  # type: ignore[assignment]

    inline_calls: List[str] = []

    async def fake_refresh(c, symbol, strike_limit):
        inline_calls.append(symbol)

    db._refresh_max_pain_snapshot = fake_refresh  # type: ignore[assignment]

    asyncio.run(db.get_max_pain_current(symbol="SPY", strike_limit=200))

    assert inline_calls == []


def test_get_max_pain_current_falls_back_to_inline_for_unlisted_symbols():
    """Symbols not in MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS keep on-demand
    refresh behavior."""
    db = DatabaseManager()
    db._max_pain_background_refresh_enabled = True
    db._max_pain_background_refresh_symbols = frozenset(["SPY"])

    @asynccontextmanager
    async def fake_acquire():
        yield _FakeReadConn()

    db._acquire_connection = fake_acquire  # type: ignore[assignment]

    inline_calls: List[str] = []

    async def fake_refresh(c, symbol, strike_limit):
        inline_calls.append(symbol)

    db._refresh_max_pain_snapshot = fake_refresh  # type: ignore[assignment]

    asyncio.run(db.get_max_pain_current(symbol="IWM", strike_limit=200))

    assert inline_calls == ["IWM"]
