"""The live quote's day volume reads one symbol-day, not the whole table.

``get_latest_quote`` decorates the newest bar with that ET day's cumulative
volume. It used to join the ``underlying_daily_volume`` view for it, and
Postgres cannot push a join condition into a GROUP BY view, so every cache miss
aggregated every symbol and every day in ``underlying_quotes`` to read one row
of the result -- a parallel seq scan that made ``/api/market/quote`` the
costliest endpoint on the API (2026-09-25: about 20 s of request time per
trading minute). The read now sums the symbol's own bars over the ET day, a
range the ``(symbol, timestamp)`` index can seek.

The first test pins that shape, so a refactor back onto the view fails here
rather than in production. The rest check the number itself against the view
on a real server -- across both clock changes and at ET midnight, where an
off-by-an-hour day boundary would move bars between days -- and are
``integration``-marked::

    LATEST_QUOTE_VOLUME_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_latest_quote_daily_volume.py -q -m integration

They build their own throwaway schema, so the database needs no project schema.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.api.database import DatabaseManager

_DSN = os.getenv("LATEST_QUOTE_VOLUME_TEST_DSN")
_SKIP = "LATEST_QUOTE_VOLUME_TEST_DSN not set — day-volume parity test skipped."

_ET = ZoneInfo("America/New_York")


def _et(y: int, mo: int, d: int, h: int, mi: int) -> datetime:
    return datetime(y, mo, d, h, mi, tzinfo=_ET)


class _RecordingConn:
    def __init__(self):
        self.queries = []

    async def fetchrow(self, query, *args):
        self.queries.append(query)
        return None


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def _new_db():
    db = DatabaseManager()
    # Every call must reach the SQL, not the cache.
    db._latest_quote_cache_ttl_seconds = 0.0
    return db


def test_day_volume_is_a_ranged_read_not_the_aggregate_view():
    db = _new_db()
    conn = _RecordingConn()
    _install_conn(db, conn)
    asyncio.run(db.get_latest_quote("SPY"))
    sql = conn.queries[0]

    assert "underlying_daily_volume" not in sql
    assert "LEFT JOIN LATERAL" in sql
    assert "v.symbol = lq.symbol" in sql
    # Both ends of the ET day bound the raw column, which is what the index seeks.
    assert "v.timestamp >=" in sql
    assert "v.timestamp <" in sql


# ── Against a real server ────────────────────────────────────────────────────

# Verbatim from setup/database/schema.sql: the definition the read replaced,
# kept here as the reference the new number must equal.
_VIEW = """
CREATE VIEW underlying_daily_volume AS
SELECT
    symbol,
    DATE(timestamp AT TIME ZONE 'America/New_York') AS trade_date_et,
    SUM(COALESCE(up_volume, 0) + COALESCE(down_volume, 0))::bigint AS cumulative_daily_volume
FROM underlying_quotes
GROUP BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York');
"""

# symbol -> (asset_type, [(ET bar start, up_volume, down_volume)], expected day volume
# for the newest bar). Throwaway symbols; the schema is dropped afterwards.
_CASES = {
    # A plain EST session; the prior day's bar is not today's volume.
    "ZZQA": (
        "ETF",
        [
            (_et(2026, 3, 5, 15, 59), 1000, 1000),
            (_et(2026, 3, 6, 9, 30), 10, 5),
            (_et(2026, 3, 6, 12, 0), 20, 5),
            (_et(2026, 3, 6, 15, 59), 1, 1),
        ],
        42,
    ),
    # First EDT weekday after spring-forward, pre-market included.
    "ZZQB": (
        "ETF",
        [
            (_et(2026, 3, 6, 15, 59), 500, 500),
            (_et(2026, 3, 9, 4, 0), 3, 4),
            (_et(2026, 3, 9, 9, 30), 7, 8),
        ],
        22,
    ),
    # The 23-hour spring-forward day itself, bars at both ET midnights.
    "ZZQC": (
        "INDEX",
        [
            (_et(2026, 3, 7, 23, 59), 100, 100),
            (_et(2026, 3, 8, 0, 0), 1, 2),
            (_et(2026, 3, 8, 1, 59), 3, 4),
            (_et(2026, 3, 8, 3, 0), 5, 6),
            (_et(2026, 3, 8, 23, 59), 7, 8),
        ],
        36,
    ),
    # The 25-hour fall-back day, then a bar exactly at the next ET midnight:
    # only that bar is the new day's.
    "ZZQD": (
        "ETF",
        [
            (_et(2026, 11, 1, 0, 0), 50, 50),
            (_et(2026, 11, 1, 23, 59), 60, 60),
            (_et(2026, 11, 2, 0, 0), 2, 3),
        ],
        5,
    ),
    # A NULL side counts as zero, as it does in the view.
    "ZZQE": (
        "ETF",
        [
            (_et(2026, 9, 25, 9, 30), None, 7),
            (_et(2026, 9, 25, 9, 31), 4, None),
        ],
        11,
    ),
}


async def _connect_scratch_schema():
    import asyncpg

    schema = f"zz_quote_volume_{os.getpid()}"
    conn = await asyncpg.connect(_DSN, server_settings={"search_path": schema})
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await conn.execute("""
        CREATE TABLE symbols (symbol VARCHAR(10) PRIMARY KEY, asset_type TEXT);
        CREATE TABLE underlying_quotes (
            symbol VARCHAR(10) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            open NUMERIC(12, 4) NOT NULL,
            high NUMERIC(12, 4) NOT NULL,
            low NUMERIC(12, 4) NOT NULL,
            close NUMERIC(12, 4) NOT NULL,
            up_volume BIGINT DEFAULT 0,
            down_volume BIGINT DEFAULT 0,
            PRIMARY KEY (symbol, timestamp)
        );
        """)
    await conn.execute(_VIEW)
    return conn, schema


@pytest.mark.integration
@pytest.mark.skipif(not _DSN, reason=_SKIP)
def test_day_volume_matches_the_view_across_clock_changes_and_midnight():
    async def _run():
        conn, schema = await _connect_scratch_schema()
        try:
            for symbol, (asset_type, bars, _) in _CASES.items():
                await conn.execute("INSERT INTO symbols VALUES ($1, $2)", symbol, asset_type)
                await conn.executemany(
                    "INSERT INTO underlying_quotes (symbol, timestamp, open, high, low, close,"
                    " up_volume, down_volume) VALUES ($1, $2, 100, 101, 99, 100.5, $3, $4)",
                    [(symbol, ts, up, down) for ts, up, down in bars],
                )

            db = _new_db()
            _install_conn(db, conn)
            for symbol, (asset_type, bars, expected) in _CASES.items():
                newest = max(ts for ts, _, _ in bars)
                row = await db.get_latest_quote(symbol)
                assert row is not None, symbol
                assert row["timestamp"] == newest, symbol
                assert row["asset_type"] == asset_type, symbol
                assert row["cumulative_daily_volume"] == expected, symbol

                reference = await conn.fetchval(
                    "SELECT cumulative_daily_volume FROM underlying_daily_volume"
                    " WHERE symbol = $1"
                    " AND trade_date_et = ($2::timestamptz AT TIME ZONE 'America/New_York')::date",
                    symbol,
                    newest,
                )
                assert row["cumulative_daily_volume"] == reference, symbol

            # A symbol with no bars still reads as no quote at all.
            assert await db.get_latest_quote("ZZQZ") is None
        finally:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
            await conn.close()

    asyncio.run(_run())
