"""Regression tests for ``DatabaseManager.get_latest_in_session_quote``.

The endpoint ``/api/market/quote`` previously returned the most recent
``underlying_quotes`` row by timestamp, with no session filter. On weekends
and after the cash bell this surfaced post-market ETF bars (SPY/QQQ at
~19:59 ET Friday) as the displayed "spot price" — wrong, because every
downstream surface (header price, GEX flip distance, regime label,
% change against the prior close) anchors on the cash session close.

These tests pin the new behavior:

  * Query SQL filters candidate bars to ET-time in [09:30, 16:00] on
    weekdays (DOW 1..5). Universal rule, no time-of-day branching.
  * ``$1`` is the symbol parameter — same shape every caller already uses.
  * Cache hits short-circuit the DB. Misses populate the cache with the
    in-session payload (the cache key is distinct from ``get_latest_quote``
    so the two methods don't poison each other).
  * Cash-session bars are kept; pre-market / after-hours / weekend bars
    are excluded by the WHERE clause.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from src.api.database import DatabaseManager


class _RecordingConn:
    """Mock asyncpg connection that records queries and returns canned rows."""

    def __init__(self, fetchrow_result=None):
        self._fetchrow_result = fetchrow_result
        self.queries = []
        self.args = []

    async def fetchrow(self, query, *args):
        self.queries.append(query)
        self.args.append(args)
        return self._fetchrow_result


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def _new_db():
    db = DatabaseManager()
    # Force-disable the cache between cases so each test exercises the SQL.
    db._latest_quote_cache_ttl_seconds = 0.0
    return db


# ---------------------------------------------------------------------------
# SQL shape — the filter is what makes the method correct.
# ---------------------------------------------------------------------------


def _captured_sql(db, conn, symbol="SPY"):
    _install_conn(db, conn)
    asyncio.run(db.get_latest_in_session_quote(symbol))
    return conn.queries[0], conn.args[0]


def test_filters_to_cash_session_window():
    """ET-time must be bounded to [09:30, 16:00] — the bell-to-bell cash
    session. Any wider window re-admits the after-hours ETF bars that
    motivated the fix."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    # Time bounds explicit and inclusive (a 16:00 bar IS the canonical close).
    assert "TIME '09:30'" in sql
    assert "TIME '16:00'" in sql
    assert ">= TIME '09:30'" in sql
    assert "<= TIME '16:00'" in sql

    # ET timezone — never local server time, never UTC.
    assert "AT TIME ZONE 'America/New_York'" in sql


def test_excludes_weekends():
    """DOW must be clamped to 1..5. Without this, a stray weekend bar
    (rare data anomaly, but seen on holiday-eve ingestion replays) would
    win the ORDER BY DESC race and serve a non-trading-day price."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    assert "EXTRACT(" in sql and "DOW" in sql
    assert "BETWEEN 1 AND 5" in sql


def test_takes_latest_matching_bar():
    """ORDER BY timestamp DESC LIMIT 1 — universal rule across live /
    pre-market / AH / weekend / holiday. The WHERE clause does the
    session work; the ORDER BY just picks the most recent survivor."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    assert "ORDER BY uq.timestamp DESC" in sql
    assert "LIMIT 1" in sql


def test_symbol_param_position():
    """``$1`` is symbol. The endpoint, the cache key, and every test in
    this repo depend on the param shape being stable."""
    db = _new_db()
    sql, args = _captured_sql(db, _RecordingConn(fetchrow_result=None), symbol="QQQ")

    assert "WHERE uq.symbol = $1" in sql
    assert args == ("QQQ",)


def test_symbol_uppercased():
    """Symbol normalization must match ``get_latest_quote`` so a lowercase
    call hits the same cache bucket and the same row."""
    db = _new_db()
    conn = _RecordingConn(fetchrow_result=None)
    _install_conn(db, conn)
    asyncio.run(db.get_latest_in_session_quote("spy"))
    assert conn.args[0] == ("SPY",)


def test_joins_daily_volume_and_asset_type():
    """The payload shape must match ``get_latest_quote`` so the API
    endpoint can swap one for the other without reshaping the response."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    assert "underlying_daily_volume" in sql
    assert "cumulative_daily_volume" in sql
    assert "s.asset_type" in sql
    assert "LEFT JOIN symbols s" in sql


# ---------------------------------------------------------------------------
# Payload shape & caching
# ---------------------------------------------------------------------------


def test_returns_row_payload_as_dict():
    """When a row is found, return it as a plain dict (the endpoint
    spreads it into ``UnderlyingQuote`` and pops ``asset_type``)."""
    db = _new_db()
    row = {
        "timestamp": datetime(2026, 6, 26, 20, 0, tzinfo=timezone.utc),  # 16:00 ET
        "symbol": "SPY",
        "open": 728.50,
        "high": 729.10,
        "low": 728.40,
        "close": 728.99,
        "cumulative_daily_volume": 50_000_000,
        "asset_type": "ETF",
    }
    conn = _RecordingConn(fetchrow_result=row)
    _install_conn(db, conn)

    result = asyncio.run(db.get_latest_in_session_quote("SPY"))

    assert result == row
    assert isinstance(result, dict)


def test_returns_none_when_no_in_session_bar():
    """If only pre/after-hours or weekend bars exist for a symbol, the
    DB returns no row and we return None. The endpoint then 404s, which
    is the correct surface: there is no valid spot price to display."""
    db = _new_db()
    conn = _RecordingConn(fetchrow_result=None)
    _install_conn(db, conn)

    result = asyncio.run(db.get_latest_in_session_quote("SPY"))

    assert result is None


def test_cache_key_is_distinct_from_latest_quote():
    """The two methods must not share a cache bucket — otherwise the
    raw-latest payload (used by the soft-close tracker) could overwrite
    the in-session payload (used by the displayed price), or vice
    versa, depending on which method ran first inside the TTL."""
    db = DatabaseManager()
    db._latest_quote_cache_ttl_seconds = 60.0

    # Prime the in-session cache with one payload.
    in_session_row = {"symbol": "SPY", "close": 728.99}
    conn1 = _RecordingConn(fetchrow_result=in_session_row)
    _install_conn(db, conn1)
    asyncio.run(db.get_latest_in_session_quote("SPY"))

    # Now ask for the raw latest — must NOT come from the in-session cache.
    raw_row = {"symbol": "SPY", "close": 731.20}
    conn2 = _RecordingConn(fetchrow_result=raw_row)
    _install_conn(db, conn2)
    raw_result = asyncio.run(db.get_latest_quote("SPY"))

    assert raw_result == raw_row
    assert len(conn2.queries) == 1  # the raw call hit the DB, not the cache


def test_cache_hit_short_circuits_db():
    """A second call inside TTL must return the cached payload without
    issuing a query — same caching contract as ``get_latest_quote``."""
    db = DatabaseManager()
    db._latest_quote_cache_ttl_seconds = 60.0

    row = {"symbol": "SPY", "close": 728.99}
    conn = _RecordingConn(fetchrow_result=row)
    _install_conn(db, conn)

    first = asyncio.run(db.get_latest_in_session_quote("SPY"))
    second = asyncio.run(db.get_latest_in_session_quote("SPY"))

    assert first == row
    assert second == row
    assert len(conn.queries) == 1
