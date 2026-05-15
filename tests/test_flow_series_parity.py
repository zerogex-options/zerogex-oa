"""Value-level parity harness: live CTE vs flow_series_5min snapshot.

This is the gating check of the phase-1 -> phase-2 migration. Acceptance
criteria #3/#5 require diffing *both paths* against real data, which is
only meaningful against a live Postgres — CI has none and the rest of the
flow-series suite deliberately mocks the connection. So this test is
``integration``-marked and skips unless ``FLOW_SERIES_PARITY_DSN`` points
at a database with backfilled / engine-written snapshot rows.

Run it as the verification step before flipping ``FLOW_SERIES_USE_SNAPSHOT``:

    make flow-series-parity FLOW_SERIES_PARITY_DSN=postgres://... \\
        FLOW_SERIES_PARITY_SYMBOL=SPY FLOW_SERIES_PARITY_SESSION=prior

``session=prior`` is the strongest assertion: a fully-closed session's
bars are all window-invariant, so the snapshot must match the CTE
row-for-row with zero tolerance.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("FLOW_SERIES_PARITY_DSN")
_SYMBOL = os.getenv("FLOW_SERIES_PARITY_SYMBOL", "SPY")
_SESSION = os.getenv("FLOW_SERIES_PARITY_SESSION", "prior")

pytest_skip_reason = (
    "FLOW_SERIES_PARITY_DSN not set — integration parity harness skipped "
    "(run via `make flow-series-parity`)."
)


@pytest.mark.skipif(_DSN is None, reason=pytest_skip_reason)
def test_snapshot_matches_live_cte_row_for_row():
    import asyncio

    import asyncpg

    from src.api.database import DatabaseManager
    from src.flow_series_sql import (
        FLOW_SERIES_CTE_ASYNCPG,
        SNAPSHOT_SELECT_ASYNCPG,
    )

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            # Resolve the window through the production code path so the
            # comparison uses exactly the session the API would serve.
            db = DatabaseManager()

            @asynccontextmanager
            async def _acquire():
                yield conn

            db._acquire_connection = _acquire  # type: ignore[method-assign]
            resolved = await db._resolve_flow_series_session(conn, _SYMBOL, _SESSION)
            assert resolved is not None, f"{_SYMBOL} unknown in flow_by_contract"
            session_start, session_end, has_data = resolved
            if not has_data:
                pytest.skip(f"{_SYMBOL} has no {_SESSION} session data")

            cte_rows = await conn.fetch(
                FLOW_SERIES_CTE_ASYNCPG,
                _SYMBOL,
                session_start,
                session_end,
                None,
                None,
            )
            snap_rows = await conn.fetch(
                SNAPSHOT_SELECT_ASYNCPG,
                _SYMBOL,
                session_start,
                session_end,
            )
            return [dict(r) for r in cte_rows], [dict(r) for r in snap_rows]
        finally:
            await conn.close()

    cte, snap = asyncio.run(_run())

    if not snap:
        pytest.skip(
            f"flow_series_5min empty for {_SYMBOL}/{_SESSION} — run "
            "`python -m src.tools.flow_series_5min_backfill` first."
        )

    assert len(snap) == len(cte), (
        f"row count differs: snapshot={len(snap)} cte={len(cte)} " f"for {_SYMBOL}/{_SESSION}"
    )
    for i, (s_row, c_row) in enumerate(zip(snap, cte)):
        assert s_row == c_row, (
            f"row {i} differs at bar_start="
            f"{c_row.get('bar_start')}:\n  snapshot={s_row}\n  cte={c_row}"
        )
