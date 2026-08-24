"""The 16:00 ET mark ES / NQ measure their day change against.

``/api/market/session-closes?symbol=ES`` supplies ``current_session_close``,
which the header divides into the live ES print to produce the headline
"change since the previous close". Get the mark wrong and the first number a
futures trader reads is wrong.

Bars are start-of-minute stamped (``_parse_bar`` floors TradeStation's
close-stamp to the bar's own minute), so the bar timestamped 16:00:00 spans
16:00:00-16:00:59: it OPENS at the cash close and CLOSES a full minute into
post-close trading. ES and NQ trade 23 hours a day, so that minute is live
tape. Taking the bar's ``close`` measured the day change against 16:01 and let
the headline drift by whatever the future did right after the bell.

``get_session_closes`` has applied exactly this rule to every cash symbol that
trades after hours since the AH-contamination fix; futures are its most
extreme case and were the one query that never got it. These tests pin the
rule on both sides so they cannot drift apart again.
"""

import asyncio
import datetime as dt
import os
from contextlib import asynccontextmanager

import pytest

from src.api.database import DatabaseManager

# Optional self-seeding round-trip against a real PostgreSQL. Unlike the
# other *_parity tests this one creates and populates its own table, so it
# needs an empty scratch database rather than a production snapshot::
#
#     FUTURES_SESSION_CLOSES_DSN=postgresql:///scratch pytest \
#         tests/test_futures_session_closes.py
_DSN = os.getenv("FUTURES_SESSION_CLOSES_DSN")

ET = dt.timezone(dt.timedelta(hours=-4))  # America/New_York in August


class _RecordingConn:
    """Mock asyncpg connection that records queries and returns no rows."""

    def __init__(self):
        self.queries = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        return []


def _captured_sql() -> str:
    db = DatabaseManager()
    conn = _RecordingConn()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    asyncio.run(db.get_futures_session_closes("SPX"))
    return conn.queries[0]


# --- the rule --------------------------------------------------------------


def test_the_1600_bar_contributes_its_open_not_its_close():
    """The 16:00:00 print, not the 16:00:59 one."""
    sql = _captured_sql()
    assert "CASE" in sql
    assert "= TIME '16:00'" in sql
    assert "THEN open" in sql
    assert "ELSE close" in sql


def test_the_substitution_binds_on_the_1600_bar_alone():
    """A half-day's 13:14 last bar, and the 15:59 bar when the 16:00 one is
    missing, both already close AT the cash close — they must keep ``close``.
    A broader window would substitute an open for a plain last-tick."""
    sql = _captured_sql()
    assert "BETWEEN TIME '09:30' AND TIME '16:00'" in sql
    # Exactly one EQUALITY test against 16:00 — the CASE. The fence uses
    # BETWEEN and the in-progress guard uses >=, so count only the equality
    # (">= TIME '16:00'" contains "= TIME '16:00'" as a substring).
    assert sql.count("= TIME '16:00'") - sql.count(">= TIME '16:00'") == 1
    assert ">= TIME '16:00'" in sql


def test_the_scan_is_bounded_so_the_backfill_cannot_slow_it_down():
    """Without a lower bound this TZ-converts, partitions and sorts the
    index's ENTIRE futures history for a two-row result. That was tolerable
    while futures_quotes held a rolling overnight window; the backfill made it
    every minute of every session inside DATA_RETENTION_DAYS."""
    sql = _captured_sql()
    assert "NOW() - INTERVAL '30 days'" in sql


def test_the_in_progress_session_is_still_excluded():
    """Regression guard for the earlier fix: counting the half-finished day as
    'today's close' made ES/NQ read ~0.00% all session."""
    sql = _captured_sql()
    assert "(NOW() AT TIME ZONE 'America/New_York')::date" in sql


def test_futures_and_cash_agree_that_an_ah_traded_1600_close_is_contaminated():
    """The two queries express one rule. If someone reverts either to a plain
    ``close``, this fails rather than leaving the header quietly wrong on one
    side only."""
    db = DatabaseManager()
    conn = _RecordingConn()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    asyncio.run(db.get_futures_session_closes("SPX"))
    futures_sql = conn.queries[0]

    cash_src = DatabaseManager.get_session_closes.__doc__ or ""
    assert "16:00:00-16:00:59" in cash_src or "16:00:00–16:00:59" in cash_src
    assert "THEN uq.open" in _cash_sql()
    assert "THEN open" in futures_sql


def _cash_sql() -> str:
    db = DatabaseManager()
    db._latest_quote_cache_ttl_seconds = 0.0

    class _Rowless:
        def __init__(self):
            self.queries = []

        async def fetchrow(self, query, *args):
            self.queries.append(query)
            return None

    conn = _Rowless()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    asyncio.run(db.get_session_closes("SPY"))
    return conn.queries[0]


# --- round trip against a real PostgreSQL ----------------------------------

_CASH_1600 = 6600.00  # ES print at 16:00:00 Friday — the correct baseline
_POST_1601 = 6612.75  # ES print at 16:00:59 Friday — a minute of live tape
_THU_1600 = 6550.00


@pytest.mark.skipif(not _DSN, reason="set FUTURES_SESSION_CLOSES_DSN to run")
@pytest.mark.parametrize(
    "now_et,expected",
    [
        # Monday pre-market: CME has been open since Sunday 18:00, but the
        # last COMPLETED cash session is Friday's.
        (dt.datetime(2026, 8, 24, 7, 47, tzinfo=ET), _CASH_1600),
        # Friday mid-session: today has not closed, so Thursday's mark stands.
        (dt.datetime(2026, 8, 21, 11, 0, tzinfo=ET), _THU_1600),
        # Friday after the bell, and the weekend that follows.
        (dt.datetime(2026, 8, 21, 16, 30, tzinfo=ET), _CASH_1600),
        (dt.datetime(2026, 8, 22, 10, 0, tzinfo=ET), _CASH_1600),
    ],
)
def test_the_query_returns_the_1600_mark_against_real_postgres(now_et, expected):
    """Runs the production SQL with ``NOW()`` parametrised, over bars seeded
    with the same stamping the ingester and the backfill write."""
    import asyncpg

    sql = _captured_sql().replace("NOW()", "$2::timestamptz")

    def bar(rows, day, hour, minute, open_, close_):
        rows.append(
            (
                "SPX",
                "@ES",
                dt.datetime(2026, 8, day, hour, minute, tzinfo=ET),
                open_,
                max(open_, close_),
                min(open_, close_),
                close_,
                0,
                0,
            )
        )

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await conn.execute("DROP TABLE IF EXISTS futures_quotes")
            await conn.execute("""
                CREATE TABLE futures_quotes (
                    index_symbol TEXT, future_symbol TEXT, timestamp TIMESTAMPTZ,
                    open DOUBLE PRECISION, high DOUBLE PRECISION,
                    low DOUBLE PRECISION, close DOUBLE PRECISION,
                    up_volume BIGINT, down_volume BIGINT,
                    PRIMARY KEY (index_symbol, timestamp))
                """)
            rows: list = []
            for m in range(30, 60):
                bar(rows, 20, 9, m, _THU_1600, _THU_1600)
            bar(rows, 20, 15, 59, _THU_1600 - 1, _THU_1600)
            bar(rows, 20, 16, 0, _THU_1600, _THU_1600 + 9)
            for m in range(30, 60):
                bar(rows, 21, 9, m, 6580, 6580)
            bar(rows, 21, 15, 59, 6598, _CASH_1600)
            bar(rows, 21, 16, 0, _CASH_1600, _POST_1601)  # 16:00:00 -> 16:00:59
            for m in range(1, 60):
                bar(rows, 21, 16, m, _POST_1601, _POST_1601)
            for h in range(18, 24):  # Sunday reopen
                bar(rows, 23, h, 0, 6620, 6620)
            for h in range(0, 8):  # Monday overnight
                bar(rows, 24, h, 0, 6630, 6630)
            await conn.executemany(
                "INSERT INTO futures_quotes VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", rows
            )
            return [dict(r) for r in await conn.fetch(sql, "SPX", now_et)]
        finally:
            await conn.execute("DROP TABLE IF EXISTS futures_quotes")
            await conn.close()

    got = asyncio.run(_run())
    assert got, "query returned no marks"
    assert got[0]["close"] == pytest.approx(expected)
    # And never the post-close tape, which is what it used to return.
    assert got[0]["close"] != pytest.approx(_POST_1601)
