"""Parity: the candle reads give the same bars with the tiered bucket floor.

``/api/market/historical`` finds where its window starts — the Nth most recent
bucket that has data — with a DISTINCT over the symbol's bars. Over the whole
history that cost 70-80 ms for an ETF with three months of 1-minute bars, on
every call, and the Gamma Terminal asks for its last 3 bars every second. With
``step_expr`` the floor is looked for near the end first (2 x N buckets of
time, then 7 days) and the whole history is read only when neither window
holds N buckets.

That is only safe if every answer is identical, gaps included: overnight, a
long weekend, and a hole longer than the 7-day window, ended both at the latest
bar and at earlier ``end_date`` values inside and just after those gaps. Each
case is read twice, once through the floor without ``step_expr`` (the full
scan this replaced), and the two must match bar for bar.

Value-level SQL behaviour is only meaningful against a real server, so this is
``integration``-marked and skips unless a DSN is exported::

    HISTORICAL_FLOOR_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_historical_bucket_floor_parity.py -q -m integration

The DSN's database must already have ``setup/database/schema.sql`` applied.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("HISTORICAL_FLOOR_TEST_DSN")

_SKIP = (
    "HISTORICAL_FLOOR_TEST_DSN not set — bucket-floor parity integration test "
    "skipped (needs a scratch Postgres with the project schema)."
)

_ET = ZoneInfo("America/New_York")

# Throwaway symbols: the teardown deletes them, so they must never be real ones.
_ETF = "ZZFLETF"
_INDEX = "ZZFLIDX"

_TIMEFRAMES = ("1min", "5min", "15min", "1hr", "1day")
_WINDOW_UNITS = (1, 3, 12, 576)


def _et(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=_ET)


# Mid-session; right after a long weekend; right after a 13-day hole, which no
# 7-day window can see past; and the latest bar (None).
_END_DATES = (
    None,
    _et("2026-10-06 12:34:30"),
    _et("2026-10-13 04:01:00"),
    _et("2026-10-13 09:31:00"),
    _et("2026-09-28 04:02:00"),
    _et("2026-09-28 09:33:00"),
)

# Weekdays from 2026-09-01 to 2026-10-14 13:57 ET, except a 13-day hole
# (2026-09-15 .. 2026-09-27) and Monday 2026-10-12, which leaves an 80-hour
# weekend in front of 2026-10-13.
_DAYS_SQL = """
    SELECT ts FROM generate_series(
        timestamptz '2026-09-01 00:00 America/New_York',
        timestamptz '2026-10-14 13:57 America/New_York',
        interval '1 minute'
    ) ts
    WHERE extract(isodow FROM ts AT TIME ZONE 'America/New_York') < 6
      AND (ts AT TIME ZONE 'America/New_York')::date NOT BETWEEN '2026-09-15' AND '2026-09-27'
      AND (ts AT TIME ZONE 'America/New_York')::date <> '2026-10-12'
"""


async def _seed(conn) -> None:
    await _teardown(conn)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) VALUES "
        "($1, 'bucket floor parity test', 'ETF', true), "
        "($2, 'bucket floor parity test', 'INDEX', true)",
        _ETF,
        _INDEX,
    )
    # ETF-shaped: 04:00-19:59 ET. Index-shaped: the cash session only.
    for symbol, start, end in ((_ETF, "04:00", "20:00"), (_INDEX, "09:30", "16:00")):
        await conn.execute(
            f"""
            INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close,
                                          up_volume, down_volume, volume)
            SELECT $1, ts,
                   600 + (extract(epoch FROM ts)::bigint % 997) / 100.0,
                   602, 598,
                   600 + (extract(epoch FROM ts)::bigint % 991) / 100.0,
                   1000 + (extract(epoch FROM ts)::bigint % 37), 900, NULL
            FROM ({_DAYS_SQL}) d
            WHERE (ts AT TIME ZONE 'America/New_York')::time >= '{start}'
              AND (ts AT TIME ZONE 'America/New_York')::time < '{end}'
            """,
            symbol,
        )
    # The index's overnight future: every hour but the 17:00 ET maintenance break.
    await conn.execute(
        f"""
        INSERT INTO futures_quotes(index_symbol, future_symbol, timestamp, open, high, low, close)
        SELECT $1, 'ZZF', ts, 6000, 6002, 5998,
               6000 + (extract(epoch FROM ts)::bigint % 983) / 10.0
        FROM ({_DAYS_SQL}) d
        WHERE extract(hour FROM ts AT TIME ZONE 'America/New_York') <> 17
        """,
        _INDEX,
    )


async def _teardown(conn) -> None:
    await conn.execute("DELETE FROM futures_quotes WHERE index_symbol = $1", _INDEX)
    await conn.execute("DELETE FROM symbols WHERE symbol IN ($1, $2)", _ETF, _INDEX)


def _db(conn):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


async def _read_all(conn):
    db = _db(conn)
    out = {}
    for end in _END_DATES:
        for timeframe in _TIMEFRAMES:
            for n in _WINDOW_UNITS:
                for symbol in (_ETF, _INDEX):
                    out[("quotes", symbol, end, timeframe, n)] = await db.get_historical_quotes(
                        symbol, None, end, n, timeframe
                    )
                out[("futures", _INDEX, end, timeframe, n)] = await db.get_historical_futures(
                    _INDEX, None, end, n, timeframe
                )
    return out


@pytest.mark.skipif(_DSN is None, reason=_SKIP)
def test_tiered_floor_reads_the_same_bars_as_the_full_scan(monkeypatch):
    import asyncpg

    import src.api.database as database

    full_scan = database._bucket_floor_subquery

    def without_tiers(**kwargs):
        kwargs.pop("step_expr", None)
        return full_scan(**kwargs)

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn)
            tiered = await _read_all(conn)
            monkeypatch.setattr(database, "_bucket_floor_subquery", without_tiers)
            reference = await _read_all(conn)
        finally:
            await _teardown(conn)
            await conn.close()
        return tiered, reference

    tiered, reference = asyncio.run(_run())

    assert tiered.keys() == reference.keys()
    # Every case must have read real bars, or a pass would prove nothing.
    empty = [case for case, rows in reference.items() if not rows]
    assert not empty, f"cases with no bars: {empty[:5]}"
    differing = [case for case in reference if tiered[case] != reference[case]]
    assert not differing, f"{len(differing)} of {len(reference)} cases differ, e.g. {differing[:5]}"
