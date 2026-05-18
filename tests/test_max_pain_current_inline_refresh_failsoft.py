"""Regression: a failing *inline* max-pain refresh must not 500 the endpoint.

Production incident (2026-05-18): ``GET /api/max-pain/current?symbol=SPY``
returned a continual stream of 500s.  The traceback was

    get_max_pain_current -> _refresh_max_pain_snapshot
    -> asyncpg conn.execute(...) -> TimeoutError    (duration_ms ~= 30000)

i.e. for a symbol on the on-demand path the heavy multi-CTE recompute ran
inline, blew the asyncpg ~30s command_timeout, and the raised TimeoutError
propagated out as a 500 — even though max_pain_oi_snapshot already held a
fresh, perfectly usable row for that symbol.

The inline recompute is a best-effort freshen, not a correctness
requirement (max pain is a daily OI snapshot).  get_max_pain_current must
therefore swallow a refresh timeout/DB error, fall through to the existing
snapshot, and only return None (-> 404) when there is genuinely no row.

Pure/hermetic: a fake connection stands in for asyncpg, no DB required.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, List

from src.api.database import DatabaseManager

_SNAPSHOT_ROW = {
    "timestamp": datetime(2026, 5, 18, 19, 15, tzinfo=timezone.utc),
    "symbol": "SPY",
    "as_of_date": date(2026, 5, 18),
    "underlying_price": Decimal("736.0900"),
    "max_pain": Decimal("740.0000"),
    "difference": Decimal("3.9100"),
}


class _FakeConn:
    """Yields the existing snapshot row; expiration fetch returns nothing."""

    def __init__(self, snapshot: Any) -> None:
        self._snapshot = snapshot
        self.fetchrow_calls = 0

    async def fetchrow(self, query: str, *args: Any):
        self.fetchrow_calls += 1
        return self._snapshot

    async def fetch(self, query: str, *args: Any) -> List[Any]:
        return []


def _make_db(snapshot: Any) -> tuple[DatabaseManager, dict]:
    db = DatabaseManager()
    # Force the inline (on-demand) path the prod symbol was hitting.
    db._max_pain_background_refresh_enabled = False
    db._max_pain_background_refresh_symbols = frozenset()

    captured: dict = {}

    @asynccontextmanager
    async def fake_acquire():
        yield _FakeConn(snapshot)

    db._acquire_connection = fake_acquire  # type: ignore[assignment]

    async def boom_refresh(conn, symbol, strike_limit, timeout=None):
        captured["timeout"] = timeout
        # Exactly the asyncpg command_timeout failure from the incident.
        raise TimeoutError

    db._refresh_max_pain_snapshot = boom_refresh  # type: ignore[assignment]
    return db, captured


def test_inline_refresh_timeout_serves_existing_snapshot():
    db, captured = _make_db(_SNAPSHOT_ROW)

    result = asyncio.run(db.get_max_pain_current(symbol="SPY", strike_limit=200))

    # Did NOT raise; degraded to the row already in the table.
    assert result is not None
    assert result["symbol"] == "SPY"
    assert result["max_pain"] == Decimal("740.0000")
    assert result["expirations"] == []
    # And the inline recompute was given the short fail-fast budget, not
    # the pool's ~30s default, so it can't pin a connection for 30s.
    assert captured["timeout"] == db._max_pain_inline_refresh_timeout_seconds
    assert db._max_pain_inline_refresh_timeout_seconds <= 30.0


def test_inline_refresh_failure_with_no_snapshot_returns_none():
    """No usable row + failed refresh -> None (endpoint maps to 404),
    still never an unhandled 500."""
    db, _ = _make_db(None)

    result = asyncio.run(db.get_max_pain_current(symbol="SPY", strike_limit=200))

    assert result is None
