"""The backtest data window must cover the archive, not just the hot table.

The engine prices option legs from ``option_chains`` and falls back to the
retention-exempt ``option_chains_archive``, but ``_data_window`` used to read
only the hot table. The Backtesting page clamps its date pickers to that
window, so every archived session was unreachable from the form.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.backtesting import meta


def _ts(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, 14, 30, tzinfo=timezone.utc)


class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self._conn = conn
        self._row = None

    def execute(self, sql: str, params=None) -> None:
        self._conn.queries.append((" ".join(sql.split()), params))
        if "to_regclass" in sql:
            self._row = (self._conn.has_archive,)
        elif "option_chains_archive" in sql:
            self._row = self._conn.archive_bounds.get(params[0], (None, None))
        else:
            self._row = self._conn.live_bounds

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, live_bounds, has_archive=False, archive_bounds=None) -> None:
        self.live_bounds = live_bounds
        self.has_archive = has_archive
        self.archive_bounds = archive_bounds or {}
        self.queries: list = []

    def cursor(self) -> _Cursor:
        return _Cursor(self)


@pytest.fixture(autouse=True)
def _two_underlyings(monkeypatch):
    monkeypatch.setattr(meta, "_underlyings", lambda: ["SPY", "SPX"])


def test_window_reaches_back_into_the_archive():
    conn = _Conn(
        live_bounds=(_ts(2026, 6, 29), _ts(2026, 9, 25)),
        has_archive=True,
        archive_bounds={
            "SPY": (_ts(2025, 1, 2), _ts(2026, 9, 24)),
            "SPX": (_ts(2025, 3, 3), _ts(2026, 9, 24)),
        },
    )
    window = meta._data_window(conn)
    assert window["earliest"] == "2025-01-02"
    assert window["latest"] == "2026-09-25"


def test_archive_bounds_are_read_per_underlying():
    # The archive is only indexed on (underlying, timestamp); an unfiltered
    # MIN(timestamp) would scan the whole table on every page load.
    conn = _Conn(live_bounds=(_ts(2026, 6, 29), _ts(2026, 9, 25)), has_archive=True)
    meta._data_window(conn)
    archive_queries = [(sql, p) for sql, p in conn.queries if "FROM option_chains_archive" in sql]
    assert [p for _, p in archive_queries] == [("SPY",), ("SPX",)]
    assert all("WHERE underlying = %s" in sql for sql, _ in archive_queries)


def test_without_an_archive_table_the_hot_table_is_the_window():
    conn = _Conn(live_bounds=(_ts(2026, 6, 29), _ts(2026, 9, 25)), has_archive=False)
    window = meta._data_window(conn)
    assert window == {
        "earliest": "2026-06-29",
        "latest": "2026-09-25",
        "retention_days": meta.DATA_RETENTION_DAYS,
    }
    assert not any("FROM option_chains_archive" in sql for sql, _ in conn.queries)


def test_empty_tables_give_no_window():
    conn = _Conn(live_bounds=(None, None), has_archive=True)
    window = meta._data_window(conn)
    assert window["earliest"] is None
    assert window["latest"] is None
