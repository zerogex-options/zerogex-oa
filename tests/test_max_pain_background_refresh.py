"""Tests for the scheduled max-pain snapshot refresh + the pure-read endpoint.

The 5-min in-process loop and the inline on-request recompute were both
removed (they scanned option_chains during the cash session and starved
the Analytics engine).  The recompute now runs once/day off-process via
``src.tools.max_pain_refresh`` -> ``DatabaseManager.refresh_max_pain_snapshots``;
``get_max_pain_current`` is a pure cache read.

Covers:

1. ``refresh_max_pain_snapshots`` iterates symbols, wraps each recompute in
   a transaction, and applies ``SET LOCAL statement_timeout`` so the heavy
   CTE chain can run beyond the pool's default 30s timeout.
2. A per-symbol failure is logged but does not abort the run — remaining
   symbols are still attempted.
3. ``get_max_pain_current`` NEVER triggers a recompute (no
   ``_refresh_max_pain_snapshot`` call) and is a pure read: it returns the
   snapshot row, or None (-> endpoint 404) when none exists.
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

    def __init__(self, snapshot: Any, expirations: Any) -> None:
        self._snapshot = snapshot
        self._expirations = expirations

    async def fetchrow(self, query: str, *args: Any):
        return self._snapshot

    async def fetch(self, query: str, *args: Any) -> List[Any]:
        return self._expirations


def _explode_if_called(*_a: Any, **_k: Any):
    raise AssertionError(
        "get_max_pain_current must be a pure read — it must NOT recompute "
        "the snapshot on the request path"
    )


def test_get_max_pain_current_never_recomputes_and_returns_snapshot():
    """The endpoint reads whatever the scheduled job last wrote; it must
    not call _refresh_max_pain_snapshot for ANY symbol (the inline path
    that starved Analytics is gone)."""
    db = DatabaseManager()
    db._refresh_max_pain_snapshot = _explode_if_called  # type: ignore[assignment]

    snapshot = {
        "timestamp": "2026-05-18T19:15:00+00:00",
        "symbol": "SPY",
        "as_of_date": "2026-05-18",
        "underlying_price": 736.09,
        "max_pain": 740.0,
        "difference": 3.91,
    }

    @asynccontextmanager
    async def fake_acquire():
        yield _FakeReadConn(snapshot, [])

    db._acquire_connection = fake_acquire  # type: ignore[assignment]

    # An unlisted symbol (the old "fall back to inline" case) must also be
    # a pure read now.
    result = asyncio.run(db.get_max_pain_current(symbol="IWM", strike_limit=200))
    assert result is not None
    assert result["symbol"] == "SPY"
    assert result["expirations"] == []


def test_get_max_pain_current_returns_none_when_no_snapshot():
    """No row yet (job hasn't run) -> None, which the endpoint maps to a
    404 — never a recompute, never a 500."""
    db = DatabaseManager()
    db._refresh_max_pain_snapshot = _explode_if_called  # type: ignore[assignment]

    @asynccontextmanager
    async def fake_acquire():
        yield _FakeReadConn(None, [])

    db._acquire_connection = fake_acquire  # type: ignore[assignment]

    assert asyncio.run(db.get_max_pain_current(symbol="SPY", strike_limit=200)) is None
