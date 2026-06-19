"""Tests for the option_chains archive job (src/tools/backtest_archive.py)."""

from __future__ import annotations

from datetime import date, datetime

from src.tools import backtest_archive as arch


class _StubCursor:
    def __init__(self):
        self.executions: list[tuple[str, list]] = []
        self.rowcount = 7

    def execute(self, sql, params=None):
        self.executions.append((sql, params))


class _StubConn:
    def __init__(self):
        self._cur = _StubCursor()
        self.commits = 0

    def cursor(self):
        return self._cur

    def commit(self):
        self.commits += 1


def test_archive_day_window_and_conflict_clause():
    conn = _StubConn()
    inserted = arch.archive_day(conn, date(2026, 6, 15), underlyings=["SPY", "SPX"])
    assert inserted == 7
    assert conn.commits == 1
    sql, params = conn._cur.executions[0]
    # Half-open [day, day+1) window.
    assert params[0] == datetime(2026, 6, 15)
    assert params[1] == datetime(2026, 6, 16)
    # Underlying filter passed as an ANY(array) param, upper-cased.
    assert params[2] == ["SPY", "SPX"]
    assert "ON CONFLICT (option_symbol, timestamp) DO NOTHING" in " ".join(sql.split())
    assert "AND underlying = ANY(%s)" in sql


def test_archive_day_without_underlyings_has_no_filter():
    conn = _StubConn()
    arch.archive_day(conn, date(2026, 6, 15), underlyings=None)
    sql, params = conn._cur.executions[0]
    assert len(params) == 2  # only the timestamp window, no underlying array
    assert "ANY(%s)" not in sql


def test_archive_range_iterates_inclusive():
    conn = _StubConn()
    total = arch.archive_range(conn, date(2026, 6, 1), date(2026, 6, 3), underlyings=["SPY"])
    # 3 days inclusive × 7 rows each (stub rowcount).
    assert total == 21
    assert len(conn._cur.executions) == 3
    assert conn.commits == 3


def test_resolve_window_single_date():
    args = _Args(date="2026-06-10")
    assert arch._resolve_window(args) == (date(2026, 6, 10), date(2026, 6, 10))


def test_resolve_window_explicit_range():
    args = _Args(start="2026-03-01", end="2026-03-05")
    assert arch._resolve_window(args) == (date(2026, 3, 1), date(2026, 3, 5))


def test_resolve_window_days_ends_yesterday():
    args = _Args(days=3)
    start, end = arch._resolve_window(args)
    assert (end - start).days == 2  # 3 inclusive days
    assert end < date.today()  # ends yesterday, never includes today's partial


class _Args:
    """Minimal argparse.Namespace stand-in."""

    def __init__(self, *, date=None, days=None, start=None, end=None):
        self.date = date
        self.days = days
        self.start = start
        self.end = end
