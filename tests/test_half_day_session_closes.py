"""On an early-close day the session close is the 13:00 ET bar, not 16:00.

``get_session_closes`` took each day's last bar at or before 16:00 ET and
``get_previous_close`` the most recent 16:00 bar. On an early-close day an
ETF keeps printing after hours until 16:00, so its "close" became an
after-hours price (for the whole weekend after 2026-11-27, in the site header),
and an index, which has no 16:00 bar that day, had its half day skipped.

``integration``-marked: needs a scratch Postgres with the project schema::

    HALF_DAY_CLOSES_TEST_DSN=postgresql://user@localhost:5432/scratch \\
        pytest tests/test_half_day_session_closes.py -q -m integration
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("HALF_DAY_CLOSES_TEST_DSN")
_ETF = "ZZHDETF"
_INDEX = "ZZHDIDX"
_ET = ZoneInfo("America/New_York")

# Wed 11-25 (normal), Thu 11-26 holiday, Fri 11-27 closes at 13:00. The bar at
# 13:00 opens at 613.00 (the auction print); every after-hours bar is 650+.
_SEED = """
    INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close,
                                  up_volume, down_volume, volume)
    SELECT $1, ts,
           CASE
             WHEN (ts AT TIME ZONE 'America/New_York')::date = '2026-11-27'
              AND (ts AT TIME ZONE 'America/New_York')::time = '13:00' THEN 613.00
             WHEN (ts AT TIME ZONE 'America/New_York')::date = '2026-11-25'
              AND (ts AT TIME ZONE 'America/New_York')::time = '16:00' THEN 616.00
             WHEN (ts AT TIME ZONE 'America/New_York')::time > '13:00'
              AND (ts AT TIME ZONE 'America/New_York')::date = '2026-11-27' THEN 650.00
             ELSE 600.00
           END,
           700, 500,
           CASE
             WHEN (ts AT TIME ZONE 'America/New_York')::date = '2026-11-27'
              AND (ts AT TIME ZONE 'America/New_York')::time = '13:00' THEN 613.50
             WHEN (ts AT TIME ZONE 'America/New_York')::date = '2026-11-25'
              AND (ts AT TIME ZONE 'America/New_York')::time = '16:00' THEN 616.50
             WHEN (ts AT TIME ZONE 'America/New_York')::time > '13:00'
              AND (ts AT TIME ZONE 'America/New_York')::date = '2026-11-27' THEN 651.00
             ELSE 601.00
           END,
           1000, 900, NULL
    FROM generate_series(
        timestamptz '2026-11-25 04:00 America/New_York',
        timestamptz '2026-11-27 19:59 America/New_York',
        interval '1 minute'
    ) ts
    WHERE (ts AT TIME ZONE 'America/New_York')::date <> '2026-11-26'
      AND (ts AT TIME ZONE 'America/New_York')::time >= $2::time
      AND (ts AT TIME ZONE 'America/New_York')::time <= $3::time
"""


def _db(conn):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


@pytest.mark.skipif(_DSN is None, reason="HALF_DAY_CLOSES_TEST_DSN not set")
def test_half_day_closes_are_the_1300_bar(monkeypatch):
    import asyncpg
    from datetime import time

    monkeypatch.setenv("NYSE_HALF_DAYS", "2026-11-27,2026-12-24")
    monday_premarket = datetime(2026, 11, 30, 8, 0, tzinfo=_ET)
    friday_1430 = datetime(2026, 11, 27, 14, 30, tzinfo=_ET)

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            for sym in (_ETF, _INDEX):
                await conn.execute("DELETE FROM underlying_quotes WHERE symbol = $1", sym)
                await conn.execute("DELETE FROM symbols WHERE symbol = $1", sym)
            await conn.execute(
                "INSERT INTO symbols(symbol, name, asset_type, is_active) VALUES "
                "($1, 'half day test', 'ETF', true), ($2, 'half day test', 'INDEX', true)",
                _ETF,
                _INDEX,
            )
            # ETF: 04:00-20:00 every session. Index: cash session only, 13:00 on the half day.
            await conn.execute(_SEED, _ETF, time(4, 0), time(19, 59))
            await conn.execute(
                _SEED
                + " AND ((ts AT TIME ZONE 'America/New_York')::date <> '2026-11-27' OR (ts AT TIME ZONE 'America/New_York')::time <= '13:00')",
                _INDEX,
                time(9, 30),
                time(16, 0),
            )
            db = _db(conn)
            out = {}
            for sym in (_ETF, _INDEX):
                out[(sym, "closes_monday")] = await db.get_session_closes(
                    sym, as_of=monday_premarket
                )
                out[(sym, "closes_friday_1430")] = await db.get_session_closes(
                    sym, as_of=friday_1430
                )
                out[(sym, "previous")] = await db.get_previous_close(sym)
            for day in (date(2026, 11, 25), date(2026, 11, 27)):
                out[(_ETF, "receipt_bars", day)] = await db.get_underlying_bars_for_session(
                    _ETF, day
                )
            return out
        finally:
            for sym in (_ETF, _INDEX):
                await conn.execute("DELETE FROM underlying_quotes WHERE symbol = $1", sym)
                await conn.execute("DELETE FROM symbols WHERE symbol = $1", sym)
            await conn.close()

    out = asyncio.run(_run())
    # ETF: the 13:00 bar's open (the auction print), not an after-hours price.
    etf = out[(_ETF, "closes_monday")]
    assert float(etf["current_session_close"]) == 613.00, etf
    assert float(etf["prior_session_close"]) == 616.00, etf
    assert float(out[(_ETF, "previous")]["previous_close"]) == 613.00
    # Index: the 13:00 bar's close, and Friday is not skipped.
    idx = out[(_INDEX, "closes_monday")]
    assert float(idx["current_session_close"]) == 613.50, idx
    assert float(idx["prior_session_close"]) == 616.50, idx
    assert float(out[(_INDEX, "previous")]["previous_close"]) == 613.50
    # By 14:30 on the half day its session has closed, so it is the current close.
    assert float(out[(_ETF, "closes_friday_1430")]["current_session_close"]) == 613.00
    # The receipt's bars stop at the day's close: 13:00 on the half day (no
    # after-hours 650s in the graded range), 16:00 on a normal day as before.
    half = out[(_ETF, "receipt_bars", date(2026, 11, 27))]
    assert half[-1]["timestamp"].astimezone(_ET).time() == time(13, 0)
    assert max(float(b["open"]) for b in half) == 613.00
    normal = out[(_ETF, "receipt_bars", date(2026, 11, 25))]
    assert normal[0]["timestamp"].astimezone(_ET).time() == time(9, 30)
    assert normal[-1]["timestamp"].astimezone(_ET).time() == time(16, 0)
