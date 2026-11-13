"""Regression: /api/max-pain/timeseries survives a gex_summary row with no max pain.

The analytics engine stores ``max_pain = NULL`` on purpose when no contract in
its snapshot carries open interest (``AnalyticsEngine._calculate_max_pain``).
``MaxPainTimeseriesPoint.max_pain`` is required, so one such row as the newest
in its bucket failed the whole response with a 500, every poll, for as long
as it stayed in the window. Members saw that on the Max Pain page after the
close on 2026-11-12.

The fix keeps the contract (every point carries a max pain) and filters the
NULL inside ``ranked``, before ROW_NUMBER: a bucket whose newest row has no
reading falls back to its previous one, and a bucket with no reading at all
has no point. Passing the NULL through instead would not do: the page turns
it into 0 and would plot max pain at $0.

The first test pins where the filter sits and runs everywhere. The rest run
the real SQL and the real route against a scratch Postgres built from
``setup/database/schema.sql``, so they are ``integration``-marked and skip
unless ``NULL_READING_TEST_DSN`` is set. A throwaway symbol is seeded and
torn down, so they never touch market data::

    NULL_READING_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_max_pain_timeseries_null_reading.py -q
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

_DSN = os.getenv("NULL_READING_TEST_DSN")

_SYMBOL = "ZZMPNULL"  # throwaway; cascade-deleted with its symbols row
_ET = timezone(timedelta(hours=-5))  # EST, as on 2026-11-12


def _et(hour: int, minute: int) -> datetime:
    return datetime(2026, 11, 12, hour, minute, tzinfo=_ET)


# ---------------------------------------------------------------------------
# Where the filter sits (no database needed)
# ---------------------------------------------------------------------------


class _RecordingConn:
    def __init__(self):
        self.queries: list = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        return []


def _fresh_api_modules() -> None:
    """Drop cached API modules: endpoint tests elsewhere replace
    DatabaseManager methods with mocks on the class, and those would answer
    here instead of the real query."""
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)


def _captured_query(symbol: str, timeframe: str = "5min") -> str:
    _fresh_api_modules()
    from src.api.database import DatabaseManager

    db = DatabaseManager()
    conn = _RecordingConn()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    asyncio.run(db.get_max_pain_timeseries(symbol, timeframe, 90))
    return conn.queries[0]


def _cte_body(sql: str, name: str) -> str:
    """The text between ``<name> AS (`` and its matching close paren."""
    start = sql.index(f"{name} AS (") + len(f"{name} AS (")
    depth = 1
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start:i]
    raise AssertionError(f"unbalanced CTE {name}")


@pytest.mark.parametrize("symbol", ["SPY", "SPX"])
def test_a_null_reading_is_dropped_before_each_bucket_picks_its_newest_row(symbol):
    sql = _captured_query(symbol)
    ranked = _cte_body(sql, "ranked")
    where = ranked[ranked.index("WHERE") :]

    # Inside ``ranked``'s WHERE, i.e. before ROW_NUMBER numbers the rows, so
    # rn = 1 is the newest row that HAS a reading. Filtering after rn = 1
    # would drop the bucket instead of falling back to its previous reading.
    assert re.search(r"\bmax_pain IS NOT NULL\b", where), ranked
    outer = sql[sql.rindex("FROM ranked") :]
    assert "max_pain IS NOT NULL" not in outer


# ---------------------------------------------------------------------------
# The real SQL and route against Postgres
# ---------------------------------------------------------------------------

# 1-minute rows from 15:50 ET. 15:59 is the newest row of the 15:55 bucket and
# has no reading; 16:00 is alone in its bucket with none; and the newest row of
# all, after the close at 22:05, has none either.
_READINGS = {_et(15, 50) + timedelta(minutes=i): Decimal(590 + i) for i in range(9)}
_NO_READING = [_et(15, 59), _et(16, 0), _et(22, 5)]


async def _seed(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) "
        "VALUES ($1, 'max pain null test', 'ETF', true)",
        _SYMBOL,
    )
    for ts, value in _READINGS.items():
        await conn.execute(
            "INSERT INTO gex_summary(underlying, timestamp, max_pain) VALUES ($1, $2, $3)",
            _SYMBOL,
            ts,
            float(value),
        )
    for ts in _NO_READING:
        await conn.execute(
            "INSERT INTO gex_summary(underlying, timestamp, max_pain) VALUES ($1, $2, NULL)",
            _SYMBOL,
            ts,
        )


def _run_route(timeframe: str):
    """Seed, call the real route handler on the scratch DB, always tear down."""
    import asyncpg

    _fresh_api_modules()
    from src.api import main as mainmod
    from src.api.database import DatabaseManager

    async def _go():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn)
            db = DatabaseManager()

            @asynccontextmanager
            async def _acquire():
                yield conn

            db._acquire_connection = _acquire  # type: ignore[method-assign]
            mainmod.db_manager = db
            # The route itself, error wrapper and response models included:
            # before the fix this raised HTTPException(500).
            return await mainmod.get_max_pain_timeseries(
                symbol=_SYMBOL, timeframe=timeframe, window_units=90
            )
        finally:
            await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
            await conn.close()

    return asyncio.run(_go())


skip_without_db = pytest.mark.skipif(
    _DSN is None,
    reason="NULL_READING_TEST_DSN not set: needs a scratch Postgres with the project schema.",
)


@pytest.mark.integration
@skip_without_db
def test_route_serves_after_the_close_with_null_readings_in_the_window():
    points = _run_route("5min")
    by_ts = {p.timestamp: p.max_pain for p in points}

    # The 15:55 bucket falls back to 15:58, its newest row with a reading.
    assert by_ts[_et(15, 55)] == Decimal(598)
    assert by_ts[_et(15, 50)] == Decimal(594)
    # A bucket with no reading has no point, rather than a NULL or a 500.
    assert _et(16, 0) not in by_ts
    assert _et(22, 5) not in by_ts
    assert len(points) == 2


@pytest.mark.integration
@skip_without_db
def test_route_at_one_minute_skips_exactly_the_minutes_without_a_reading():
    points = _run_route("1min")
    assert [p.timestamp for p in points] == sorted(_READINGS, reverse=True)
    assert all(p.max_pain == _READINGS[p.timestamp] for p in points)
