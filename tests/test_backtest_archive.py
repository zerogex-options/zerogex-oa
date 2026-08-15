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


def _inserts(cur):
    return [(s, p) for (s, p) in cur.executions if "INSERT INTO option_chains_archive" in s]


def _timeout_sets(cur):
    return [(s, p) for (s, p) in cur.executions if "statement_timeout" in s]


def test_archive_day_chunks_window_and_bounds_timeout(monkeypatch):
    # 12h chunks → two windows for the day, each its own statement + commit.
    monkeypatch.setenv("BACKTEST_ARCHIVE_CHUNK_MINUTES", "720")
    monkeypatch.setenv("BACKTEST_ARCHIVE_STATEMENT_TIMEOUT_MS", "120000")
    conn = _StubConn()
    inserted = arch.archive_day(conn, date(2026, 6, 15), underlyings=["SPY", "SPX"])

    ins = _inserts(conn._cur)
    assert len(ins) == 2  # two 12h chunks tile the day
    assert conn.commits == 2  # committed per chunk so a mid-day failure resumes
    assert inserted == 14  # stub rowcount 7 × 2 inserts

    # Each chunk raises statement_timeout for its own transaction only.
    sets = _timeout_sets(conn._cur)
    assert len(sets) == 2
    assert all(p[0] == 120000 for (_s, p) in sets)

    # Windows tile [day, day+1) exactly, in order.
    assert ins[0][1][0] == datetime(2026, 6, 15, 0, 0)
    assert ins[0][1][1] == datetime(2026, 6, 15, 12, 0)
    assert ins[1][1][0] == datetime(2026, 6, 15, 12, 0)
    assert ins[1][1][1] == datetime(2026, 6, 16, 0, 0)

    # Underlying filter still bound as an upper-cased ANY(array) 3rd param.
    assert ins[0][1][2] == ["SPY", "SPX"]
    assert "ON CONFLICT (option_symbol, timestamp) DO NOTHING" in " ".join(ins[0][0].split())
    assert "AND underlying = ANY(%s)" in ins[0][0]


def test_archive_day_without_underlyings_has_no_filter(monkeypatch):
    monkeypatch.setenv("BACKTEST_ARCHIVE_CHUNK_MINUTES", "1440")  # single whole-day chunk
    conn = _StubConn()
    arch.archive_day(conn, date(2026, 6, 15), underlyings=None)
    ins = _inserts(conn._cur)
    assert len(ins) == 1
    sql, params = ins[0]
    assert len(params) == 2  # only the timestamp window, no underlying array
    assert "ANY(%s)" not in sql


def test_archive_day_timeout_disabled_skips_set_local(monkeypatch):
    # Setting the per-chunk timeout to 0 leaves the connection default in place.
    monkeypatch.setenv("BACKTEST_ARCHIVE_CHUNK_MINUTES", "1440")
    monkeypatch.setenv("BACKTEST_ARCHIVE_STATEMENT_TIMEOUT_MS", "0")
    conn = _StubConn()
    arch.archive_day(conn, date(2026, 6, 15), underlyings=None)
    assert _timeout_sets(conn._cur) == []


def test_archive_range_iterates_inclusive(monkeypatch):
    monkeypatch.setenv("BACKTEST_ARCHIVE_CHUNK_MINUTES", "1440")  # 1 chunk/day for a clean count
    conn = _StubConn()
    total = arch.archive_range(conn, date(2026, 6, 1), date(2026, 6, 3), underlyings=["SPY"])
    ins = _inserts(conn._cur)
    # 3 days inclusive × 1 chunk × 7 rows each (stub rowcount).
    assert total == 21
    assert len(ins) == 3
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
