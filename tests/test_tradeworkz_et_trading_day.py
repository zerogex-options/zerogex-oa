"""'Today' P&L is bucketed by the ET trading day, not the UTC calendar day.

Symptom that surfaced this: the fleet summary card read
``Realized P&L Today -$12.78K / 9 trades`` at 09:50 ET while the audit
table (read in ET) showed no trades that day. The 9 trades closed
20:00-20:03 ET the PRIOR evening = 00:00-00:03 UTC — so a bucket keyed on
the UTC calendar day filed them under "today" even though, in ET, they
belonged to yesterday.

The three "today" surfaces — the summary card, the reconciler's daily
kill-switch, and the daily rollup — must bucket by
``(closed_at AT TIME ZONE 'America/New_York')::date`` against the ET
"now", so a close never lands in the wrong session. These tests capture
the emitted SQL and lock that in (the API card's asyncpg query is the
same one-line mirror, verified by inspection).
"""

from __future__ import annotations

from datetime import date

from src.tradeworkz.engine import rollup_daily
from src.tradeworkz.reconciler import daily_realized_pnl

_ET_EXPR = "AT TIME ZONE 'America/New_York'"


class _CapCursor:
    """Captures (normalized_sql, params) for every execute; fetch returns 0."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))

    def fetchone(self):
        return (0.0,)

    def fetchall(self):
        return []


class _CapConn:
    def __init__(self):
        self._cur = _CapCursor()

    def cursor(self):
        return self._cur


def test_daily_realized_pnl_buckets_by_et_trading_day():
    conn = _CapConn()
    daily_realized_pnl(conn, "some_bot")
    sql = conn.cursor().calls[0][0]
    assert _ET_EXPR in sql, "kill-switch daily P&L must bucket by ET, not UTC"
    # The old UTC-day predicate must be gone.
    assert "closed_at::date = CURRENT_DATE" not in sql


def test_rollup_daily_buckets_trades_by_et_trading_day():
    conn = _CapConn()
    rollup_daily(conn)

    trade_sqls = [s for (s, _p) in conn.cursor().calls if "FROM tw_trades" in s]
    assert len(trade_sqls) == 2, "rollup buckets both the equity curve and the metrics"
    for s in trade_sqls:
        assert _ET_EXPR in s, "rollup must bucket trades by the ET trading day"
        assert "closed_at::date = %s" not in s, "the bare UTC-cast predicate must be gone"

    # session_date is resolved as an actual date (the ET trading day), never a
    # string, and is what every execute is parameterized with.
    for _s, params in conn.cursor().calls:
        assert params, "every rollup statement is parameterized with the session date"
        assert all(isinstance(v, date) for v in params)
