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
    assert "lq.asset_type" in sql
    assert "LEFT JOIN symbols s" in sql


# ---------------------------------------------------------------------------
# Asset-type-aware close selection for the 16:00 bar
# ---------------------------------------------------------------------------
#
# TradeStation start-of-minute-stamps its 1-minute bars, so the bar timestamped
# 16:00:00 spans 16:00:00–16:00:59 ET — a window that begins with the closing
# auction print and ends with the first ~60s of after-hours trading. The bar's
# ``close`` is therefore AH-contaminated for any symbol that trades extended
# hours. Verified against Friday 2026-06-26 data:
#
#                 15:59 close   16:00 open   16:00 close   official close
#   SPY (ETF)        731.66       729.01       731.54         728.99
#   QQQ (ETF)        706.63       705.88       707.75         706.52
#   SPX (INDEX)     7345.16      7335.70      7353.01        7354.03
#
# Non-INDEX symbols: 16:00 open (auction print) ≈ official close. INDEX symbols:
# 16:00 close (post-auction settled level) ≈ official close. These tests pin
# the asset-type-aware substitution that produces those values.


def test_sql_substitutes_open_for_non_index_at_1600():
    """The CASE in the projection must return the 16:00 bar's ``open``
    when the symbol is NOT an INDEX — for ETFs/stocks the bar's ``open``
    is the closing-auction print, while its ``close`` is the AH-
    contaminated last tick of the 16:00 minute."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    # The selector must key on bar timestamp == 16:00 ET AND non-INDEX.
    assert "CASE" in sql
    assert "TIME '16:00'" in sql
    assert "IS DISTINCT FROM 'INDEX'" in sql
    assert "THEN lq.open" in sql
    assert "ELSE lq.close" in sql


def test_sql_preserves_close_for_index_at_1600():
    """For INDEX symbols the 16:00 ``close`` is the post-auction settled
    level (its ``open`` is the pre-auction snapshot and is much further
    from the official close). The CASE must fall through to ELSE."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    # ``IS DISTINCT FROM 'INDEX'`` is the gate. For INDEX rows the gate
    # is False so the CASE returns the ELSE branch (lq.close), which is
    # what we want — the post-auction settled level.
    assert "lq.asset_type IS DISTINCT FROM 'INDEX'" in sql
    assert "ELSE lq.close" in sql


def test_sql_does_not_substitute_for_non_1600_bars():
    """Every live in-session bar (09:30 through 15:59, and the in-progress
    bar during cash hours) must use ``close`` unchanged so live ticking
    behaves exactly as ``get_latest_quote`` does. The CASE predicate must
    bind on the bar's timestamp equaling 16:00 ET specifically — not a
    broader window — otherwise a 15:59 bar would also have its ``open``
    substituted, freezing the displayed spot mid-session."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    # The equality predicate on the bar timestamp.
    assert "= TIME '16:00'" in sql


def test_asset_type_joined_in_inner_cte_not_just_outer():
    """``asset_type`` must be available to the CASE in the projection,
    which means the ``symbols`` join belongs inside the inner CTE (next
    to the candidate-bar selection), not only as an outer enrichment.
    Without this the CASE can't see ``lq.asset_type`` and the
    substitution silently no-ops."""
    db = _new_db()
    sql, _args = _captured_sql(db, _RecordingConn(fetchrow_result=None))

    # The inner CTE selects asset_type alongside OHLC, and the join is
    # inside that CTE.
    cte_start = sql.find("WITH latest_quote AS")
    cte_end = sql.find(")", cte_start)
    inner = sql[cte_start:cte_end]
    assert "s.asset_type" in inner
    assert "LEFT JOIN symbols s" in inner


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


# ---------------------------------------------------------------------------
# Other consumers of the 16:00 cash close must use the SAME asset-aware rule
# ---------------------------------------------------------------------------
#
# Header.tsx (and every page mirroring its price-change summary via
# ``getPrimaryPriceChangeSummary``) displays ``current_session_close`` from
# ``/api/market/session-closes`` as the headline price during after-hours,
# pre-market, weekends, and holidays — NOT the live ``/api/market/quote``
# value. If ``get_session_closes`` returns the AH-contaminated 16:00 ``close``
# while ``get_latest_in_session_quote`` returns the auction-print ``open``,
# the header drifts away from the cash close while the live-RTH read remains
# correct, and the fix appears not to work even after a hard refresh.
# Same applies to ``get_previous_close``, which feeds the prior-day anchor
# used by % change calculations.
#
# Pin both to the same rule below.


def _captured_conn(db, fn_name, fetchrow_result=None):
    """Run an async DB method against a fresh mock conn and return the
    captured SQL string for assertion."""
    conn = _RecordingConn(fetchrow_result=fetchrow_result)
    _install_conn(db, conn)
    asyncio.run(getattr(db, fn_name)("SPY"))
    return conn.queries[0]


def test_get_session_closes_uses_asset_aware_close_for_1600_bar():
    """``current_session_close`` and ``prior_session_close`` are returned by
    this query and drive the header price in the AH/closed/weekend states.
    Must use the same CASE the live-quote endpoint uses: 16:00 bar OPEN for
    non-INDEX, CLOSE for INDEX. Without this fix the header keeps showing
    AH-contaminated prices (the original bug) even after the live-quote
    endpoint is fixed."""
    db = _new_db()
    sql = _captured_conn(db, "get_session_closes")

    # Inside the session_closes CTE the SELECT now uses a CASE, not raw close.
    assert "CASE" in sql
    assert "TIME '16:00'" in sql
    assert "IS DISTINCT FROM 'INDEX'" in sql
    assert "THEN uq.open" in sql
    assert "ELSE uq.close" in sql

    # The symbols table must be joined so asset_type is in scope of the CASE.
    assert "LEFT JOIN symbols s" in sql


def test_get_previous_close_uses_asset_aware_close_for_1600_bar():
    """The prior-day anchor used by % change calculations must follow the
    same rule. Both the primary 16:00-exact CTE and the nearest-to-16:00
    fallback CTE need the CASE, since either can produce the returned
    ``previous_close`` depending on whether the exact bar exists."""
    db = _new_db()
    sql = _captured_conn(db, "get_previous_close")

    # CASE appears in both CTEs; the predicate is asset-type aware.
    assert sql.count("CASE") >= 2
    assert "IS DISTINCT FROM 'INDEX'" in sql

    # The primary CTE doesn't need the time predicate because the WHERE
    # already pins HOUR=16 AND MINUTE=0; the nearest-close CTE does need it
    # to scope the OPEN substitution to the 16:00 bar specifically.
    assert "= TIME '16:00'" in sql  # in the nearest_close CTE
    assert "LEFT JOIN symbols s" in sql
