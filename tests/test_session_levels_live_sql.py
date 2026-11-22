"""The live session-levels SQL reads with index bounds and returns the same levels.

``_SESSION_LEVELS_LIVE_SQL`` used to apply ``AT TIME ZONE ...::date / ::time``
to ``timestamp`` in every WHERE, so its three reads were full scans of the
symbol's 1-minute history (underlying_quotes has no retention). The rewrite
uses plain ranges on ``timestamp``. These tests run the previous SQL (kept
below verbatim) and the new one against the same seeded bars and compare the
results, on a Monday, after a holiday and after an early close.

``integration``-marked: needs a scratch Postgres with the project schema::

    SESSION_LEVELS_TEST_DSN=postgresql://user@localhost:5432/scratch \\
        pytest tests/test_session_levels_live_sql.py -q -m integration
"""

from __future__ import annotations

import asyncio
import os
from datetime import date

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("SESSION_LEVELS_TEST_DSN")
_SYMBOL = "ZZSLVL"  # throwaway; deleted on teardown

_PREVIOUS_SQL = """
    WITH premarket AS (
        SELECT MAX(high) AS pm_high, MIN(low) AS pm_low, COUNT(*) AS pm_bars
        FROM underlying_quotes
        WHERE symbol = $1
          AND (timestamp AT TIME ZONE 'America/New_York')::date = $2
          AND (timestamp AT TIME ZONE 'America/New_York')::time >= TIME '04:00'
          AND (timestamp AT TIME ZONE 'America/New_York')::time <  TIME '09:30'
    ),
    prev_date AS (
        SELECT MAX((timestamp AT TIME ZONE 'America/New_York')::date) AS d
        FROM underlying_quotes
        WHERE symbol = $1
          AND (timestamp AT TIME ZONE 'America/New_York')::date < $2
          AND EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 1 AND 5
          AND (timestamp AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
          AND (timestamp AT TIME ZONE 'America/New_York')::time <  TIME '16:00'
    ),
    prev_close AS (
        SELECT pd.d,
               CASE WHEN pd.d = ANY($3::date[]) THEN TIME '13:00' ELSE TIME '16:00' END AS close_t
        FROM prev_date pd
    ),
    prev_session AS (
        SELECT
            MAX(uq.high) FILTER (WHERE (uq.timestamp AT TIME ZONE 'America/New_York')::time < pc.close_t) AS rth_high,
            MIN(uq.low) FILTER (WHERE (uq.timestamp AT TIME ZONE 'America/New_York')::time < pc.close_t) AS rth_low,
            MAX(uq.open) FILTER (WHERE (uq.timestamp AT TIME ZONE 'America/New_York')::time = pc.close_t) AS auction_open
        FROM underlying_quotes uq CROSS JOIN prev_close pc
        WHERE uq.symbol = $1
          AND (uq.timestamp AT TIME ZONE 'America/New_York')::date = pc.d
          AND (uq.timestamp AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
          AND (uq.timestamp AT TIME ZONE 'America/New_York')::time <= pc.close_t
    )
    SELECT pm.pm_high, pm.pm_low, pm.pm_bars, pc.d AS prev_session_date,
           GREATEST(ps.rth_high, ps.auction_open) AS prev_high,
           LEAST(ps.rth_low, ps.auction_open) AS prev_low
    FROM premarket pm CROSS JOIN prev_close pc CROSS JOIN prev_session ps
"""

# 2026-11-02 .. 2026-11-30, 04:00-20:00 ET each weekday, except Thanksgiving
# (11-26); Friday 11-27 closes at 13:00 (bars still print to 20:00, as after-hours).
# The US clocks fell back on 11-01, so both EDT-adjacent and EST days are covered.
_HOLIDAY = date(2026, 11, 26)
_HALF_DAY = date(2026, 11, 27)
_SEED_SQL = """
    INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close,
                                  up_volume, down_volume, volume)
    SELECT $1, ts,
           600 + (extract(epoch FROM ts)::bigint % 997) / 100.0,
           610 + (extract(epoch FROM ts)::bigint % 991) / 100.0,
           590 - (extract(epoch FROM ts)::bigint % 983) / 100.0,
           600, 1000, 900, NULL
    FROM generate_series(
        timestamptz '2026-11-02 04:00 America/New_York',
        timestamptz '2026-11-30 19:59 America/New_York',
        interval '1 minute'
    ) ts
    WHERE extract(isodow FROM ts AT TIME ZONE 'America/New_York') < 6
      AND (ts AT TIME ZONE 'America/New_York')::time >= '04:00'
      AND (ts AT TIME ZONE 'America/New_York')::time <  '20:00'
      AND (ts AT TIME ZONE 'America/New_York')::date <> '2026-11-26'
"""


@pytest.mark.skipif(_DSN is None, reason="SESSION_LEVELS_TEST_DSN not set")
def test_the_bounded_sql_returns_the_previous_sqls_levels():
    import asyncpg

    from src.api.database import DatabaseManager

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await conn.execute("DELETE FROM underlying_quotes WHERE symbol = $1", _SYMBOL)
            await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
            await conn.execute(
                "INSERT INTO symbols(symbol, name, asset_type, is_active) "
                "VALUES ($1, 'session levels test', 'ETF', true)",
                _SYMBOL,
            )
            await conn.execute(_SEED_SQL, _SYMBOL)
            half_days = [_HALF_DAY]
            days = [
                date(2026, 11, 3),  # Tuesday
                date(2026, 11, 9),  # Monday: previous session is Friday
                date(2026, 11, 27),  # the day after the holiday: previous is Wednesday
                date(2026, 11, 30),  # Monday after the half day: 13:00 close
                date(2026, 11, 2),  # first seeded day: no previous session
                date(2026, 11, 28),  # Saturday: no premarket bars
            ]
            pairs = []
            for d in days:
                old = await conn.fetchrow(_PREVIOUS_SQL, _SYMBOL, d, half_days)
                new = await conn.fetchrow(
                    DatabaseManager._SESSION_LEVELS_LIVE_SQL, _SYMBOL, d, half_days
                )
                pairs.append((d, dict(old), dict(new)))
            return pairs
        finally:
            await conn.execute("DELETE FROM underlying_quotes WHERE symbol = $1", _SYMBOL)
            await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
            await conn.close()

    pairs = asyncio.run(_run())
    for d, old, new in pairs:
        assert new == old, f"{d}: {old} != {new}"
    by_day = {d: new for d, _old, new in pairs}
    assert by_day[date(2026, 11, 9)]["prev_session_date"] == date(2026, 11, 6)
    assert by_day[date(2026, 11, 27)]["prev_session_date"] == date(2026, 11, 25)
    assert by_day[date(2026, 11, 30)]["prev_session_date"] == date(2026, 11, 27)
    assert by_day[date(2026, 11, 3)]["pm_bars"] == 330
