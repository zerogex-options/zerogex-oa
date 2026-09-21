"""Every ``ls.`` column the GEX summary SELECT names must be projected by its CTE.

Production ran this for one deploy, on every ``/api/gex/summary`` and every
``/api/v1/levels`` request:

    column ls.gamma_flip_reason does not exist

The column existed. ``gex_summary.gamma_flip_reason`` was in schema.sql and in
the database; the engine was writing it. What did not exist was the column on
``ls`` -- the outer SELECT reads from the ``latest_summary`` CTE, and that CTE
projects an explicit list rather than ``gs.*``. Surfacing the new field added
``ls.gamma_flip_reason`` to the outer SELECT and stopped there, so the name
resolved against a relation that never carried it.

The error text is the trap. "column ... does not exist" reads as schema skew,
which this repo has a real history of -- code reaching production one deploy
ahead of its columns, the failure mode 2635cb3 hardened the regime writer
against. It sends you to ``make schema-apply``, which here would have changed
nothing: the query fails identically against a database that HAS the column,
because the CTE is what drops it. Both halves of the endpoint were down for
the length of that detour.

An explicit projection list is the right shape -- ``gs.*`` through a CTE joined
four ways invites ambiguity, and the aliases (``stored_call_wall``) need it --
so the list stays, and this test covers the gap it leaves. It asserts the
invariant for every ``ls.`` reference rather than for ``gamma_flip_reason``
alone: the next summary column surfaced on this endpoint is the one that will
need it, and it fails at import-time SQL text, with no database.
"""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager


class _CapturingConn:
    """Answers the newest-row probe, records the body query, returns no row."""

    def __init__(self) -> None:
        self.body_query: str | None = None

    async def fetchrow(self, query, *_args):
        # Same marker the cache tests key on: the probe announces itself in a
        # leading SQL comment so it is not mistaken for the body fetch.
        if "newest-row probe" in query:
            return None
        self.body_query = query
        return None


def _summary_query() -> str:
    """The query ``get_latest_gex_summary`` actually executes."""
    db = DatabaseManager()
    conn = _CapturingConn()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    asyncio.run(db.get_latest_gex_summary("SPY"))

    assert conn.body_query is not None, "body query was never executed"
    return conn.body_query


def _latest_summary_projection(query: str) -> set[str]:
    """Output column names of the ``latest_summary`` CTE, aliases applied."""
    start = query.index("WITH latest_summary AS (")
    body = query[start : query.index("latest_quote AS (", start)]
    return {
        (alias or col).lower() for col, alias in re.findall(r"gs\.(\w+)(?:\s+AS\s+(\w+))?", body)
    }


def _outer_ls_references(query: str) -> set[str]:
    """Every ``ls.<column>`` the final SELECT list reads."""
    start = query.index("            SELECT\n                ls.timestamp,")
    outer = query[start : query.index("FROM latest_summary ls", start)]
    return {m.group(1).lower() for m in re.finditer(r"\bls\.(\w+)", outer)}


def test_outer_select_reads_only_columns_the_cte_projects():
    query = _summary_query()
    projected = _latest_summary_projection(query)
    referenced = _outer_ls_references(query)

    # Guard the parse itself: a refactor that renames the CTE or reshapes the
    # SELECT must not leave this test quietly asserting nothing.
    assert len(projected) > 15, f"CTE projection parsed as {projected!r}"
    assert len(referenced) > 15, f"outer ls.* refs parsed as {referenced!r}"

    missing = sorted(referenced - projected)
    assert not missing, (
        f"the outer SELECT reads {missing} from ls, but the latest_summary CTE "
        f"does not project {'it' if len(missing) == 1 else 'them'}. Postgres "
        f"rejects the whole query with 'column ls.<name> does not exist' -- on "
        f"every request, and on a database that HAS the column. Add "
        f"{'it' if len(missing) == 1 else 'them'} to the CTE's gs.* list."
    )


def test_gamma_flip_reason_survives_the_cte():
    """The specific column the incident was about, named so a future
    reshuffle of the CTE cannot drop it silently."""
    query = _summary_query()
    assert "gamma_flip_reason" in _latest_summary_projection(query)
    assert "gamma_flip_reason" in _outer_ls_references(query)
