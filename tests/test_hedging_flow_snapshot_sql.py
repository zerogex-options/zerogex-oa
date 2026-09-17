"""Value-level parity harness: live hedging CTE vs the hedging_flow_5min snapshot.

The sibling of ``tests/test_flow_series_parity.py``, and it exists for the
same reason: the rest of the hedging-flow suite mocks the connection, so the
SQL that actually decides what a stored bar contains — the 5-minute bucketing,
the delta notional, the generated timeline, the price carry-forward, the
cumulative window, the scope filter and the ON CONFLICT guard — is never run.
Only a real Postgres can answer whether the snapshot and the live read agree.

Three claims are load-bearing for the dated permalinks, and each is asserted
against real rows rather than argued from the query text:

1. **Parity.** A stored bar equals what the live CTE computes for the same
   window, field for field. The snapshot is meant to be the same answer
   preserved, not a second implementation of it.
2. **Convergence.** Re-running the writer over a closed session writes ZERO
   rows. Closed bars are window-invariant, so the IS DISTINCT FROM guard
   should suppress every write — if it does not, every analytics cycle
   rewrites the whole session forever.
3. **Honest 0DTE.** A session that was not an expiry materialises no ``0dte``
   rows at all, rather than a day of synthetic zeros that a chart would draw
   as a flat line through the middle of the panel.

Run it against a scratch database:

    make hedging-flow-parity HEDGING_FLOW_PARITY_DSN=postgres://...

The DSN's database must already have ``setup/database/schema.sql`` applied.
The harness seeds its own synthetic session, so it needs no market data and
leaves nothing behind but rows for the sentinel symbol it creates.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from src.hedging_flow_sql import (
    HEDGING_FLOW_CTE_PSYCOPG2,
    HEDGING_FLOW_SCOPES,
    HEDGING_FLOW_SESSIONS_ASYNCPG,
    HEDGING_FLOW_SNAPSHOT_SELECT_ASYNCPG,
    HEDGING_FLOW_SNAPSHOT_UPSERT_PSYCOPG2,
    SCOPE_0DTE,
)

pytestmark = pytest.mark.integration

_DSN = os.getenv("HEDGING_FLOW_PARITY_DSN")
_ET = ZoneInfo("America/New_York")

#: A symbol no feed produces, so the harness can seed and re-seed without
#: touching a real underlying's rows.
_SYMBOL = os.getenv("HEDGING_FLOW_PARITY_SYMBOL", "ZZTEST")

_EXPIRY_SESSION = date(2026, 6, 12)
_PLAIN_SESSION = date(2026, 6, 11)

_SKIP_REASON = (
    "HEDGING_FLOW_PARITY_DSN not set — integration parity harness skipped "
    "(run via `make hedging-flow-parity`)."
)


def _window(session_date: date):
    start = datetime(
        session_date.year, session_date.month, session_date.day, 9, 30, tzinfo=_ET
    ).astimezone(timezone.utc)
    return start, start + timedelta(hours=6, minutes=45)


def _seed(cur, session_date: date, *, is_expiry: bool) -> None:
    start, end = _window(session_date)
    cur.execute("INSERT INTO symbols (symbol) VALUES (%s) ON CONFLICT DO NOTHING", (_SYMBOL,))
    bar = start
    i = 0
    while bar <= end:
        price = 600 + (i % 11) * 0.25
        cur.execute(
            """
            INSERT INTO underlying_quotes (symbol, timestamp, open, high, low, close)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """,
            (_SYMBOL, bar, price, price, price, price),
        )
        # Every 13th bar trades nothing, so the carry-forward and is_synthetic
        # paths are exercised rather than assumed.
        if i % 13 != 5:
            for opt, delta in (("C", 0.47), ("P", -0.39)):
                expirations = [session_date + timedelta(days=7)]
                if is_expiry:
                    expirations.append(session_date)
                for exp in expirations:
                    buys, sells = 900 + i * 3, 600 + i
                    cur.execute(
                        """
                        INSERT INTO flow_contract_facts (
                            symbol, option_symbol, timestamp, option_type, strike,
                            expiration, buy_volume, sell_volume, volume_delta,
                            premium_delta, signed_volume, signed_premium,
                            delta, underlying_price
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            _SYMBOL,
                            f"{_SYMBOL}{exp:%y%m%d}{opt}00600000",
                            bar + timedelta(minutes=1),
                            opt,
                            600.0,
                            exp,
                            buys,
                            sells,
                            buys + sells + 50,
                            (buys + sells + 50) * 1.25,
                            buys - sells,
                            (buys - sells) * 1.25,
                            delta,
                            price,
                        ),
                    )
        bar += timedelta(minutes=5)
        i += 1


def _write_snapshot(cur, session_date: date) -> dict:
    start, end = _window(session_date)
    written = {}
    for scope in HEDGING_FLOW_SCOPES:
        cur.execute(
            HEDGING_FLOW_SNAPSHOT_UPSERT_PSYCOPG2,
            {
                "symbol": _SYMBOL,
                "scope": scope,
                "session_start": start,
                "session_end": end,
                "strikes": None,
                "expirations": [session_date] if scope == SCOPE_0DTE else None,
            },
        )
        written[scope] = cur.rowcount or 0
    return written


def _positional(sql: str, count: int) -> str:
    """asyncpg $N -> psycopg2 %s, so one query text serves both drivers."""
    for n in range(count, 0, -1):
        sql = sql.replace(f"${n}", "%s")
    return sql


@pytest.fixture()
def cursor():
    import psycopg2

    conn = psycopg2.connect(_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DELETE FROM hedging_flow_5min WHERE symbol = %s", (_SYMBOL,))
    cur.execute("DELETE FROM flow_contract_facts WHERE symbol = %s", (_SYMBOL,))
    cur.execute("DELETE FROM underlying_quotes WHERE symbol = %s", (_SYMBOL,))
    try:
        yield cur
    finally:
        cur.execute("DELETE FROM hedging_flow_5min WHERE symbol = %s", (_SYMBOL,))
        cur.execute("DELETE FROM flow_contract_facts WHERE symbol = %s", (_SYMBOL,))
        cur.execute("DELETE FROM underlying_quotes WHERE symbol = %s", (_SYMBOL,))
        cur.close()
        conn.close()


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_snapshot_matches_live_cte_row_for_row(cursor):
    _seed(cursor, _EXPIRY_SESSION, is_expiry=True)
    _write_snapshot(cursor, _EXPIRY_SESSION)

    start, end = _window(_EXPIRY_SESSION)
    cursor.execute(
        _positional(HEDGING_FLOW_SNAPSHOT_SELECT_ASYNCPG, 4), (_SYMBOL, "all", start, end)
    )
    stored = cursor.fetchall()
    cursor.execute(
        HEDGING_FLOW_CTE_PSYCOPG2,
        {
            "symbol": _SYMBOL,
            "session_start": start,
            "session_end": end,
            "strikes": None,
            "expirations": None,
        },
    )
    live = cursor.fetchall()

    assert stored, "the writer produced no rows to compare"
    assert stored == live, "a stored bar must equal what the live read computes"


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_rewriting_a_closed_session_writes_nothing(cursor):
    _seed(cursor, _EXPIRY_SESSION, is_expiry=True)
    first = _write_snapshot(cursor, _EXPIRY_SESSION)
    second = _write_snapshot(cursor, _EXPIRY_SESSION)

    assert all(n > 0 for n in first.values()), first
    # Without this the engine rewrites the whole session on every cycle,
    # forever, for every symbol.
    assert all(n == 0 for n in second.values()), second


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_non_expiry_session_stores_no_0dte_scope(cursor):
    _seed(cursor, _PLAIN_SESSION, is_expiry=False)
    written = _write_snapshot(cursor, _PLAIN_SESSION)

    assert written["all"] > 0
    assert written[SCOPE_0DTE] == 0, "a non-expiry day must not fabricate a 0DTE series"


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_sessions_listing_groups_by_et_date(cursor):
    _seed(cursor, _PLAIN_SESSION, is_expiry=False)
    _seed(cursor, _EXPIRY_SESSION, is_expiry=True)
    _write_snapshot(cursor, _PLAIN_SESSION)
    _write_snapshot(cursor, _EXPIRY_SESSION)

    cursor.execute(_positional(HEDGING_FLOW_SESSIONS_ASYNCPG, 2), (_SYMBOL, 60))
    rows = cursor.fetchall()

    dates = [r[0] for r in rows]
    assert dates == [_EXPIRY_SESSION, _PLAIN_SESSION], "newest first"
    by_date = {r[0]: r for r in rows}
    # (session_date, bar_count, real_bar_count, had_0dte, cum_net_usd, first, last)
    assert by_date[_EXPIRY_SESSION][3] is True
    assert by_date[_PLAIN_SESSION][3] is False
    # bar_count counts the 'all' scope only — otherwise an expiry day would
    # report twice the bars of a plain one and every card would mis-grade.
    assert by_date[_EXPIRY_SESSION][1] == by_date[_PLAIN_SESSION][1]
    assert by_date[_EXPIRY_SESSION][2] < by_date[_EXPIRY_SESSION][1], "synthetic bars excluded"
