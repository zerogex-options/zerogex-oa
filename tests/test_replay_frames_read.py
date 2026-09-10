"""The replay frames read: its query fences, and how it reports failure.

Three guards. All of them are regressions of one visible bug — a
``/replay/<symbol>/<date>`` that fails, or reads back "the analytics engine
didn't write that day", on a day the engine wrote all 390 bars.

1. ``session_summary`` must stay MATERIALIZED. Inlined, PG substitutes the
   correlated ``spot`` sub-select once per REFERENCE — four times here. When
   the ladder was a plain join those copies landed BELOW it, so the lookup ran
   4 x (minutes x strikes x expirations) times instead of once per minute:
   measured on a seeded 5.6M-row gex_by_strike, 8,274 ms / 3,867,893 buffers
   against 1,158 ms / 40,394 with the keyword, same 19,941 rows either way.
   Guard 3 now defends that same ground from the other side (the lateral's
   GROUP BY keeps the sub-select above the fan-out even when the CTE inlines,
   re-measured at 94 ms vs 96 ms), so the keyword is defence in depth rather
   than the load-bearing member it was. It stays: it pins "one lookup per
   minute" where that intent is written, and the pool has no timeout margin
   to spend on rediscovering this.

2. That exception must not come back as an empty session. It used to: the
   read answered every failure with ``[]``, the endpoint served 200 with
   ``count: 0``, and the page printed a sentence blaming our ingestion.

3. The gex_by_strike read must stay a correlated LATERAL. Because
   ``session_summary`` is a fence, a plain ``LEFT JOIN ... ON gbs.timestamp =
   s.timestamp`` leaves the session bound invisible to the planner, which
   periodically abandons the ~390 index probes for a hash/merge join over every
   row the underlying has in the table -- the whole DATA_RETENTION_DAYS window.
   That is ``get_gex_frames_for_session(NDX, 2026-07-24) failed after 31.9s``:
   a read that scales with retention rather than with the session, so it gets
   slower every day and blows command_timeout=30 on the biggest underlyings
   first. Measured on a seeded 8.4M-row gex_by_strike with the nested loop
   forced off (the production fallback), the rewrite takes rows read from
   gex_by_strike from 4,222,800 -- every NDX row in the table -- to 120 x 391,
   and 2,204 ms to 459 ms. Same gap, same fix, as get_strike_profile_timeseries
   (28a2c33) and get_gex_heatmap (15cb6c3).
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

_DATABASE_PY = (Path(__file__).resolve().parents[1] / "src" / "api" / "database.py").read_text()

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
        line.split("--", 1)[0] if "--" in line else line for line in m.group(1).splitlines()
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
        "session_summary must stay MATERIALIZED: it pins the per-minute spot "
        "sub-select to one lookup per minute at the place that intent is "
        "written. Inlined, PG substitutes it once per REFERENCE (four here); "
        "the lateral in test_strike_ladder_is_a_correlated_lateral_fenced_to_"
        "the_session currently keeps those copies above the strike fan-out "
        "anyway, so this is defence in depth -- but only while that shape "
        "holds, and dropping both is what cost 8,274 ms once already."
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
    outer = sql[sql.index("FROM session_summary s") :]
    assert sql.count("s.spot") >= 2, "expected the multiplier this fence targets"
    assert "AVG(gbs.call_gamma * 100 * s.spot * s.spot * 0.01)" in sql
    assert "gex_by_strike gbs" in outer


def test_strike_ladder_is_a_correlated_lateral_fenced_to_the_session():
    """The core anti-regression: the gex_by_strike read must be BOUNDED to the
    session's ~390 minutes by construction, not by the planner's goodwill.

    The read keys on ``gbs.timestamp = s.timestamp`` where ``s`` is
    ``session_summary`` -- a MATERIALIZED, i.e. optimisation-fence, CTE.  So
    the ``timestamp >= $2 AND timestamp < $3`` bound that makes this one
    session lives BEHIND the fence, where the planner cannot see it.  Written
    as a plain join, the ~390 index probes were only an intention: under
    production stats PG periodically picks a hash/merge join instead and reads
    every row the underlying has in gex_by_strike -- the entire
    DATA_RETENTION_DAYS window -- filtering afterwards.  A read whose cost
    scales with retention instead of with the session gets slower every day,
    which is what put ``get_gex_frames_for_session(NDX, ...) failed after
    31.9s`` in the warning log.

    The strike band does not save it either: the band caps how many rows
    SURVIVE, not how many the fallback reads before filtering -- and with the
    ingest universe at +/-3%, the default 4% band excludes nothing at all.

    So the fence is structural.  The ladder is a LATERAL subquery correlated on
    ``s.timestamp`` carrying its own GROUP BY: a lateral reference is evaluated
    per outer row, and the grouping blocks subquery pull-up, so PG cannot
    re-plan it as a hash/merge join over the table.

    Asserted against comment-stripped SQL -- the query's own comment names the
    join shape this test forbids, so matching the raw string would pass on the
    prose alone (the same trap found in the strike-profile and heatmap guards).
    """
    sql = _frames_query()
    ladder = sql[sql.index("FROM session_summary s") : sql.index(") ladder ON TRUE")]

    # Correlated LATERAL read: cannot be re-planned as a hash/merge join.
    assert "LEFT JOIN LATERAL (" in ladder
    assert "FROM gex_by_strike gbs" in ladder
    # Still keyed on the per-minute summary row (the point-lookup path).
    assert "gbs.timestamp = s.timestamp" in ladder
    # The lateral aggregates, which is what blocks PG from pulling it up into
    # the parent and losing the nested loop.
    assert "GROUP BY gbs.strike" in ladder[ladder.index("LEFT JOIN LATERAL (") :]
    # NOT re-planned back into a plain join on the table.
    assert "JOIN gex_by_strike" not in ladder.replace("LEFT JOIN LATERAL", "")

    # Session bound retained inside the lateral as defence in depth.
    assert "gbs.timestamp >= $2" in ladder
    assert "gbs.timestamp < $3" in ladder


def test_ladder_lateral_is_left_joined_so_a_bare_minute_keeps_its_frame():
    """``ON TRUE`` on a LEFT join, never CROSS JOIN LATERAL.

    A frame carries more than its strike ladder: the flip, both walls, max pain
    and the pin strike all come from gex_summary.  A minute whose ladder is
    empty -- no in-band strikes, or gex_by_strike missing for that analytics
    cycle -- must still emit its frame, or the scrubber's level path acquires a
    hole exactly where the old LEFT JOIN kept a row.  CROSS JOIN LATERAL would
    silently drop that minute, and no assertion on the ladder itself would
    notice.
    """
    sql = _frames_query()
    assert "LEFT JOIN LATERAL (" in sql
    assert ") ladder ON TRUE" in sql
    assert "CROSS JOIN LATERAL" not in sql


def test_a_minute_with_no_strikes_still_becomes_a_frame():
    """The grouping side of the guard above, at the Python layer.

    The lateral hands back a NULL strike for a bare minute, exactly as the LEFT
    JOIN did.  That row must still open a frame -- with its levels intact and an
    empty ladder -- rather than being skipped.
    """
    ts = "2026-08-20T14:00:00Z"
    frames = _run_frames(
        [
            {
                "timestamp": ts,
                "gamma_flip": 601.0,
                "call_wall": 605.0,
                "put_wall": 595.0,
                "max_pain": 600.0,
                "pin_strike": 600.0,
                "pin_confidence": 0.4,
                "max_gamma_strike": 511.0,
                "strike": None,
                "net_gex": None,
                "call_gex": None,
                "put_gex": None,
            }
        ]
    )
    assert len(frames) == 1
    assert frames[0]["timestamp"] == ts
    assert frames[0]["gamma_flip"] == 601.0
    assert frames[0]["strikes"] == []


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
