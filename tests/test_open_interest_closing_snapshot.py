"""/api/market/open-interest reads the regular session's close after hours.

option_chains holds a row only for a contract that ticked in that minute, so
outside the option session the newest bucket is a handful of residual quotes.
On 2026-09-25 SPY's 19:59 ET bucket held 54 contracts from one expiration
against 854 across seven at 15:59, and the endpoint served the 54 as the whole
book all weekend. These tests pin the move to the closing bucket, the calendar
edges it walks, and the expiry roll-off that keeps Friday's expired contracts
out of a weekend read.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

ET = ZoneInfo("America/New_York")


def _et(*args):
    return datetime(*args, tzinfo=ET)


def _db_mod():
    # Resolved per test: other modules pop src.api.* from sys.modules.
    import src.api.database as db_mod

    return db_mod


class _ScriptedConn:
    """Replays fetchval results in order and records each call's arguments.

    fetchval #0 is the stable-snapshot resolution; any after it are the
    closing-bucket probes.
    """

    def __init__(self, fetchvals):
        self._fetchvals = list(fetchvals)
        self.fetchval_calls = []
        self.fetch_calls = []

    async def fetchrow(self, query, *args):
        return {"spot_price": 660.0}

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        return self._fetchvals.pop(0)

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        return [
            {
                "timestamp": args[1],
                "underlying": args[0],
                "strike": 660.0,
                "expiration": date(2026, 9, 28),
                "option_type": "C",
                "open_interest": 1200,
                "exposure": 0,
                "updated_at": args[1],
            }
        ]


def _read(conn, underlying, now):
    db = _db_mod().DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return asyncio.run(db.get_open_interest(underlying, now=now))


def _served_bucket(conn):
    ((_, args),) = conn.fetch_calls
    return args[1]


def _probe_bounds(conn):
    return [args[1] for _, args in conn.fetchval_calls[1:]]


def test_in_session_serves_the_newest_bucket_unchanged():
    newest = _et(2026, 9, 25, 14, 30)
    conn = _ScriptedConn([newest])
    _read(conn, "SPY", now=_et(2026, 9, 25, 14, 31))
    assert _probe_bounds(conn) == []
    assert _served_bucket(conn) == newest


def test_friday_evening_serves_the_closing_bucket_not_the_residual_ticks():
    conn = _ScriptedConn([_et(2026, 9, 25, 19, 59), _et(2026, 9, 25, 15, 59)])
    result = _read(conn, "spy", now=_et(2026, 9, 27, 8, 51))
    assert result is not None and result["underlying"] == "SPY"
    assert _probe_bounds(conn) == [_et(2026, 9, 25, 16, 0)]
    assert _served_bucket(conn) == _et(2026, 9, 25, 15, 59)


def test_closing_probe_is_one_backward_step_on_the_gamma_index():
    conn = _ScriptedConn([_et(2026, 9, 25, 19, 59), _et(2026, 9, 25, 15, 59)])
    _read(conn, "SPY", now=_et(2026, 9, 27, 8, 51))
    probe_sql, args = conn.fetchval_calls[1]
    assert "gamma IS NOT NULL" in probe_sql
    assert "timestamp < $2" in probe_sql
    assert "ORDER BY timestamp DESC" in probe_sql
    assert "LIMIT 1" in probe_sql
    assert args[0] == "SPY"


def test_weekend_read_drops_contracts_that_expired_friday():
    conn = _ScriptedConn([_et(2026, 9, 25, 19, 59), _et(2026, 9, 25, 15, 59)])
    _read(conn, "SPY", now=_et(2026, 9, 27, 8, 51))
    ((query, args),) = conn.fetch_calls
    assert "oc.expiration > $3" in query
    # Friday 09-25 is out; Monday 09-28 is in.
    assert args[2] == date(2026, 9, 26)


def test_etf_chain_stays_live_through_the_1615_late_session():
    newest = _et(2026, 9, 25, 16, 5)
    conn = _ScriptedConn([newest])
    _read(conn, "SPY", now=_et(2026, 9, 25, 16, 6))
    assert _probe_bounds(conn) == []
    assert _served_bucket(conn) == newest


def test_cash_index_chain_closes_at_1600():
    conn = _ScriptedConn([_et(2026, 9, 25, 16, 5), _et(2026, 9, 25, 15, 59)])
    _read(conn, "SPX", now=_et(2026, 9, 25, 16, 6))
    assert _probe_bounds(conn) == [_et(2026, 9, 25, 16, 0)]
    assert _served_bucket(conn) == _et(2026, 9, 25, 15, 59)


def test_premarket_reads_the_prior_session_close():
    conn = _ScriptedConn([_et(2026, 9, 28, 8, 0), _et(2026, 9, 25, 15, 59)])
    _read(conn, "SPY", now=_et(2026, 9, 28, 8, 1))
    assert _probe_bounds(conn) == [_et(2026, 9, 25, 16, 0)]
    assert _served_bucket(conn) == _et(2026, 9, 25, 15, 59)


def test_holiday_ticks_read_the_prior_session_close(monkeypatch):
    import src.market_calendar as mc

    monkeypatch.setattr(mc, "NYSE_HOLIDAYS", {date(2026, 9, 7)})
    conn = _ScriptedConn([_et(2026, 9, 7, 11, 0), _et(2026, 9, 4, 15, 59)])
    _read(conn, "SPY", now=_et(2026, 9, 7, 11, 1))
    assert _probe_bounds(conn) == [_et(2026, 9, 4, 16, 0)]
    assert _served_bucket(conn) == _et(2026, 9, 4, 15, 59)


def test_half_day_reads_the_1300_close(monkeypatch):
    import src.market_calendar as mc

    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {date(2026, 11, 27)})
    conn = _ScriptedConn([_et(2026, 11, 27, 13, 30), _et(2026, 11, 27, 12, 59)])
    _read(conn, "SPY", now=_et(2026, 11, 27, 13, 31))
    assert _probe_bounds(conn) == [_et(2026, 11, 27, 13, 0)]
    assert _served_bucket(conn) == _et(2026, 11, 27, 12, 59)


def test_a_day_with_no_session_data_walks_back_to_the_prior_close():
    # Friday has no gamma-bearing bucket in session, so the first probe lands
    # in Thursday evening's residual ticks and the bound steps back again.
    conn = _ScriptedConn(
        [_et(2026, 9, 25, 19, 59), _et(2026, 9, 24, 19, 58), _et(2026, 9, 24, 15, 59)]
    )
    _read(conn, "SPY", now=_et(2026, 9, 27, 8, 51))
    assert _probe_bounds(conn) == [_et(2026, 9, 25, 16, 0), _et(2026, 9, 24, 16, 0)]
    assert _served_bucket(conn) == _et(2026, 9, 24, 15, 59)


def test_no_session_bucket_keeps_the_newest_rather_than_nothing():
    newest = _et(2026, 9, 25, 19, 59)
    conn = _ScriptedConn([newest, None])
    assert _read(conn, "SPY", now=_et(2026, 9, 27, 8, 51)) is not None
    assert _served_bucket(conn) == newest


def test_no_snapshot_returns_none_without_reading_rows():
    conn = _ScriptedConn([None])
    assert _read(conn, "SPY", now=_et(2026, 9, 25, 14, 31)) is None
    assert conn.fetch_calls == []


@pytest.mark.parametrize(
    "now, expected",
    [
        (_et(2026, 9, 25, 15, 59), date(2026, 9, 24)),  # today's 0DTE still in the book
        (_et(2026, 9, 25, 16, 14), date(2026, 9, 24)),  # through the late session
        (_et(2026, 9, 25, 16, 15), date(2026, 9, 25)),  # rolled off at the options close
        (_et(2026, 9, 27, 12, 0), date(2026, 9, 26)),  # weekend: Friday's expiry is gone
    ],
)
def test_min_expiration_rolls_off_at_the_options_close(now, expected):
    assert _db_mod()._open_interest_min_expiration(now) == expected
