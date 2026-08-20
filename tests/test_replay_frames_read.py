"""The replay frames read: its query fence, and how it reports failure.

Two guards, both regressions of the same visible bug — a blank
``/replay/SPY/<today>`` reading "the analytics engine didn't write that day"
on a day the engine wrote all 390 bars.

1. ``session_summary`` must stay MATERIALIZED. Inlined, PG substitutes the
   correlated ``spot`` sub-select once per REFERENCE and evaluates it below
   the gex_by_strike join: 4 x (minutes x strikes x expirations) executions
   instead of one per minute. Measured on a seeded 5.6M-row gex_by_strike,
   that is 8,274 ms / 3,867,893 buffers against 1,158 ms / 40,394 with the
   keyword — same 19,941 rows either way. The pool runs command_timeout=30,
   so on a chain with enough live expirations the inlined shape is the
   difference between a replay and an exception.

2. That exception must not come back as an empty session. It used to: the
   read answered every failure with ``[]``, the endpoint served 200 with
   ``count: 0``, and the page printed a sentence blaming our ingestion.
"""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.api.database import DatabaseManager, ReplayFramesUnavailable

from tests.test_replay_endpoint import _build_app

_DATABASE_PY = (
    Path(__file__).resolve().parents[1] / "src" / "api" / "database.py"
).read_text()

SESSION = date(2026, 8, 20)


def _frames_query() -> str:
    """The SQL text of ``get_gex_frames_for_session``, comments stripped.

    Stripped because the CTE's own comment spells out the inlined shape it
    exists to prevent — a structural assertion against the raw string would
    pass on the prose alone.
    """
    start = _DATABASE_PY.index("async def get_gex_frames_for_session(")
    m = re.search(r'query = """(.*?)"""', _DATABASE_PY[start:], re.DOTALL)
    assert m, "get_gex_frames_for_session: no query string found"
    return "\n".join(
        line.split("--", 1)[0] if "--" in line else line
        for line in m.group(1).splitlines()
    )


def _run_frames(fetch_result):
    """Run the real method with ``conn.fetch`` scripted to a value or error."""
    db = DatabaseManager()

    class _Conn:
        async def fetch(self, query, *args):
            if isinstance(fetch_result, Exception):
                raise fetch_result
            return fetch_result

    @asynccontextmanager
    async def _acquire():
        yield _Conn()

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return asyncio.run(db.get_gex_frames_for_session("SPY", SESSION))


# --------------------------------------------------------------------------
# 1. The query fence
# --------------------------------------------------------------------------

def test_session_summary_cte_is_materialized():
    sql = _frames_query()
    assert "session_summary AS MATERIALIZED (" in sql, (
        "session_summary must stay MATERIALIZED: inlined, the per-minute spot "
        "sub-select is re-evaluated once per (strike x expiration) row, four "
        "times over."
    )


def test_spot_subselect_lives_inside_the_materialized_cte():
    """Guard the guard: the fence only matters while the sub-select is behind it.

    Move the correlated ``spot`` lookup out into the outer SELECT and
    MATERIALIZED stops protecting anything, while the assertion above keeps
    passing on a keyword that no longer fences the expensive part.
    """
    sql = _frames_query()
    start = sql.index("session_summary AS MATERIALIZED (")
    end = sql.index("FROM gex_summary gs", start)
    cte = sql[start:end]
    assert "FROM underlying_quotes uq" in cte
    assert "ORDER BY uq.timestamp DESC" in cte
    assert "LIMIT 1) AS spot" in cte


def test_spot_is_referenced_more_than_once_downstream():
    """The multiplier this fence exists for: ``s.spot`` appears 4 times.

    Each reference is its own SubPlan when the CTE inlines. If a refactor
    ever narrows that to a single reference the cost drops on its own — and
    this test should be revisited rather than left asserting a number that no
    longer means anything.
    """
    sql = _frames_query()
    outer = sql[sql.index("FROM session_summary s"):]
    assert sql.count("s.spot") >= 2, "expected the multiplier this fence targets"
    assert "AVG(gbs.call_gamma * 100 * s.spot * s.spot * 0.01)" in sql
    assert "gex_by_strike gbs" in outer


# --------------------------------------------------------------------------
# 2. Failure is not emptiness
# --------------------------------------------------------------------------

def test_frames_read_raises_on_query_failure():
    with pytest.raises(ReplayFramesUnavailable):
        _run_frames(asyncio.TimeoutError("query timed out"))


def test_frames_read_returns_empty_for_a_genuinely_empty_session():
    """``[]`` keeps its literal meaning: the query ran, the session is dark."""
    assert _run_frames([]) == []


def test_range_answers_503_when_the_frames_read_fails(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(
        side_effect=dbmod.ReplayFramesUnavailable("boom")
    )
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[])
    with TestClient(app) as client:
        r = client.get("/api/replay/range?symbol=SPY&date=2026-08-20")
    assert r.status_code == 503
    detail = r.json()["detail"]
    assert detail["status"] == "frames_unavailable"
    assert "not an empty session" in detail["message"]


def test_range_still_answers_200_for_a_dark_session(monkeypatch):
    """The other side of the split — a real empty day must NOT become a 503.

    Sessions that predate GEX ingestion are a normal, permanent state of the
    archive; turning them into errors would be the same conflation pointed
    the other way.
    """
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(return_value=[])
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[])
    with TestClient(app) as client:
        r = client.get("/api/replay/range?symbol=SPY&date=2019-01-02")
    assert r.status_code == 200
    assert r.json()["count"] == 0
