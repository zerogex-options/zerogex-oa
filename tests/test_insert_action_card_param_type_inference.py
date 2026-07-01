"""Regression test for the asyncpg ``inconsistent types deduced for parameter $N``
warning in ``SignalsQueriesMixin.insert_action_card``.

The SQL inserts an Action Card and uses a ``WHERE NOT EXISTS`` idempotency
guard.  Three parameters (``$1`` underlying, ``$2`` timestamp, ``$3``
pattern) appear in BOTH the ``INSERT ... SELECT`` value position (where
asyncpg infers the type from the target column) AND a ``WHERE column =
$N`` equality (where asyncpg infers the type from the LHS column type).

asyncpg allows positional-parameter reuse, but only when each parameter
resolves to a single type unambiguously across all its occurrences.
Without explicit casts the two deductions conflict, the PostgreSQL
backend rejects the prepare with ``inconsistent types deduced for
parameter $N``, and every action-card write fails silently (the
``except`` block at the call site downgrades the error to a WARNING).

This test pins the explicit-cast pattern so a future "simplification"
that strips the casts back out can't silently reintroduce the
data-loss regression.  We check the canonical SQL string the method
sends to asyncpg by capturing it through a fake connection.
"""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, List, Tuple

from src.api.queries.signals import SignalsQueriesMixin


class _FakeConn:
    """Captures fetchval() calls for assertion.

    ``insert_action_card`` uses ``fetchval`` with ``RETURNING id`` so the
    handler can attach the persisted row id to the live ``/action`` response
    (enables /cards/{id} permalinks). The legacy ``execute`` shim returns
    None to keep any rare paths quiet during the suite.
    """

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Tuple[Any, ...]]] = []

    async def execute(self, sql: str, *args: Any) -> None:
        self.calls.append((sql, args))

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.calls.append((sql, args))
        # Simulate a fresh insert: return the synthetic primary key. Any
        # follow-up "existing id" lookup will be captured as a second call.
        return 4221


class _Stub(SignalsQueriesMixin):
    """Minimal SignalsQueriesMixin host that yields a captured fake conn."""

    def __init__(self) -> None:
        self.conn = _FakeConn()

    @asynccontextmanager
    async def _acquire_connection(self):
        yield self.conn


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


def _card() -> dict:
    return {
        "underlying": "SPY",
        "timestamp": datetime(2026, 5, 21, 22, 22, tzinfo=timezone.utc),
        "pattern": "pin_risk_premium_sell",
        "action": "SELL_IRON_CONDOR",
        "tier": "1DTE",
        "direction": "non_directional",
        "confidence": 0.42,
    }


def test_insert_action_card_casts_every_reused_parameter():
    """``$1``, ``$2``, ``$3`` each appear in two contexts (INSERT-SELECT
    value and WHERE equality).  Both occurrences must carry an explicit
    type cast so asyncpg's prepare-time type deduction is unambiguous."""
    stub = _Stub()
    inserted_id = _run(stub.insert_action_card(_card()))

    # The fresh-insert path runs exactly one fetchval (the INSERT ... RETURNING
    # id); the existing-id fallback only fires when the INSERT no-ops via the
    # idempotency guard, which our fake doesn't simulate.
    assert len(stub.conn.calls) == 1, "fetchval should have been called exactly once"
    assert inserted_id == 4221, "fresh insert path must return the new row id"
    sql, args = stub.conn.calls[0]

    # Reused params: each must appear with an explicit cast in BOTH
    # occurrences.  Two occurrences × three params = six cast sites.
    # We assert on count rather than positions so a reformat (whitespace,
    # newlines) doesn't flake the test.
    assert sql.count("$1::varchar") == 2, (
        "Underlying (param $1) must carry an explicit ::varchar cast at "
        "BOTH its INSERT-SELECT value site AND its WHERE comparison site; "
        f"found {sql.count('$1::varchar')} occurrences. SQL: {sql!r}"
    )
    assert sql.count("$2::timestamptz") == 2, (
        "Timestamp (param $2) must carry an explicit ::timestamptz cast "
        "at BOTH its INSERT-SELECT value site AND its WHERE comparison "
        f"site; found {sql.count('$2::timestamptz')} occurrences."
    )
    assert sql.count("$3::varchar") == 2, (
        "Pattern (param $3) must carry an explicit ::varchar cast at "
        "BOTH its INSERT-SELECT value site AND its WHERE comparison "
        f"site; found {sql.count('$3::varchar')} occurrences."
    )

    # Negative guard: every occurrence of $1/$2/$3 in the actual SQL
    # (comments stripped) must be the cast form.  Equivalent to "no bare
    # reused parameters" but immune to comment text mentioning ``$N``.
    sql_no_comments = re.sub(r"--[^\n]*", "", sql)
    for n in (1, 2, 3):
        total = len(re.findall(rf"\${n}\b", sql_no_comments))
        cast = sql_no_comments.count(f"${n}::")
        assert total == cast, (
            f"Param ${n} appears {total} time(s) but only {cast} carry a "
            f"cast — bare occurrences trigger asyncpg ``inconsistent "
            f"types deduced for parameter ${n}``. SQL (comments stripped):\n"
            f"{sql_no_comments!r}"
        )


def test_insert_action_card_short_circuits_stand_down():
    """STAND_DOWN cards never reach the DB.  Sanity-check the early
    return so the regression test above doesn't accidentally pass on
    a no-op path."""
    stub = _Stub()
    card = _card()
    card["action"] = "STAND_DOWN"
    result = _run(stub.insert_action_card(card))
    assert stub.conn.calls == [], "STAND_DOWN must short-circuit before any DB call"
    assert result is None, "STAND_DOWN must return None (no row to permalink)"
