"""Value-level check of ``has_todays_close_landed`` against a real Postgres.

The quote endpoint and every WebSocket tick ask this once a second per symbol.
It used to read the symbol's whole history to answer "no" all day (40 ms on
three months of 1-minute bars). It now seeks today's bars only; these cases pin
that the answer did not move with it:

* bars after 16:00 on an EARLIER day never count, however recent;
* today's bars at or before 16:00 never count;
* a bar after 16:00 today counts once it is in the past, which the database
  clock decides, so the expectation is computed from the same clock.

Run against a scratch database with ``setup/database/schema.sql`` applied::

    CLOSE_LANDED_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_todays_close_landed_sql.py -q -m integration
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("CLOSE_LANDED_TEST_DSN")

_SKIP = (
    "CLOSE_LANDED_TEST_DSN not set — close-landed integration test skipped "
    "(needs a scratch Postgres with the project schema)."
)

_ET = ZoneInfo("America/New_York")

# Throwaway symbol: the teardown deletes it, so it must never be a real one.
_SYMBOL = "ZZCLOSE"


async def _landed(conn) -> bool:
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return await db.has_todays_close_landed(_SYMBOL, "ETF")


async def _insert(conn, stamps) -> None:
    await conn.executemany(
        "INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close) "
        "VALUES ($1, $2, 600, 600, 600, 600) ON CONFLICT DO NOTHING",
        [(_SYMBOL, ts) for ts in stamps],
    )


@pytest.mark.skipif(_DSN is None, reason=_SKIP)
def test_only_a_past_bar_after_16_00_today_counts():
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
            await conn.execute(
                "INSERT INTO symbols(symbol, name, asset_type, is_active) "
                "VALUES ($1, 'close landed test', 'ETF', true)",
                _SYMBOL,
            )
            now = (await conn.fetchval("SELECT NOW()")).astimezone(_ET)
            today = now.date()

            def at(day, hh, mm):
                return datetime.combine(day, time(hh, mm), tzinfo=_ET)

            # Earlier days' after-hours bars, the most recent ones first.
            earlier = [at(today - timedelta(days=d), hh, 30) for d in (1, 2, 3) for hh in (16, 19)]
            await _insert(conn, earlier)
            no_today = await _landed(conn)

            # Today's session up to the bell, never past the clock.
            session = [at(today, 9, 30) + timedelta(minutes=m) for m in range(391)]
            await _insert(conn, [ts for ts in session if ts <= now])
            bell_only = await _landed(conn)

            after_close = at(today, 16, 1)
            await _insert(conn, [after_close])
            with_after_close = await _landed(conn)
        finally:
            await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
            await conn.close()
        return now, after_close, no_today, bell_only, with_after_close

    now, after_close, no_today, bell_only, with_after_close = asyncio.run(_run())

    assert no_today is False
    assert bell_only is False
    weekday = now.isoweekday() <= 5
    assert with_after_close is (weekday and after_close <= now)
