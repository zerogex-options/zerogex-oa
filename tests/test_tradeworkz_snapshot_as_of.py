"""build_snapshot(as_of=...) reconstructs a past instant for the backtest harness.

Phase 1 of the TradeWorkz backtest harness: the snapshot builder must be able to
answer "what did the fleet see at time T?" — every read bounded to
``timestamp <= COALESCE(as_of, NOW())``. Crucially, ``as_of=None`` (the live
engine path) must be byte-for-byte the old "latest row" behavior; this locks
both: the None path still builds, and a past as_of is threaded into every
timestamped query so no read leaks future data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, List, Tuple

from src.tradeworkz.context import build_snapshot

_BOUND = "COALESCE(%s::timestamptz, NOW())"


class _CapturingCursor:
    """Fake cursor that scripts rows by SQL-substring and records every call."""

    def __init__(self) -> None:
        self._script: List[Tuple[str, List[tuple]]] = []
        self._pending: List[tuple] = []
        self.calls: List[Tuple[str, Any]] = []

    def program(self, matcher: str, rows: Iterable[tuple]) -> None:
        self._script.append((matcher, list(rows)))

    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))
        self._pending = []
        for matcher, rows in self._script:
            if matcher in sql:
                self._pending = list(rows)
                return

    def fetchone(self):
        return self._pending[0] if self._pending else None

    def fetchall(self):
        return list(self._pending)


class _Conn:
    def __init__(self) -> None:
        self._cursor = _CapturingCursor()

    def cursor(self):
        return self._cursor


def _scripted() -> _Conn:
    conn = _Conn()
    cur = conn.cursor()
    ts = datetime(2026, 8, 1, 14, 30, tzinfo=timezone.utc)
    cur.program("SELECT timestamp, close\n        FROM underlying_quotes", [(ts, 746.0)])
    cur.program("SELECT MIN(low), MAX(high)", [(739.0, 750.0, 745.0)])
    cur.program(
        "FROM gex_summary",
        [(3.2e9, 745.5, 745.0, 1.0, 750.0, 740.0, 745.0, 3.2e9, 1.0e9, 8.0e8)],
    )
    cur.program("FROM vix_bars", [(17.0,)])
    cur.program("FROM underlying_vwap_deviation", [(746.5, -0.1)])
    cur.program("SELECT close FROM underlying_quotes\n        WHERE symbol", [(746.0,)] * 30)
    return conn


def _bounded_calls(conn: _Conn) -> List[Tuple[str, Any]]:
    return [c for c in conn.cursor().calls if _BOUND in c[0]]


def test_as_of_none_still_builds_and_passes_null():
    conn = _scripted()
    snap = build_snapshot(conn, "SPY")  # as_of defaults to None
    assert snap is not None
    assert snap.spot == 746.0
    bounded = _bounded_calls(conn)
    # Several reads are now time-bounded; with as_of=None each must pass None so
    # COALESCE falls back to NOW() (i.e. the old latest-row behavior).
    assert len(bounded) >= 5
    for sql, params in bounded:
        assert None in tuple(params or ()), f"None not threaded into: {sql[:70]}"


def test_as_of_threads_timestamp_into_every_read():
    as_of = datetime(2026, 8, 1, 15, 0, tzinfo=timezone.utc)
    conn = _scripted()
    snap = build_snapshot(conn, "SPY", as_of=as_of)
    assert snap is not None
    bounded = _bounded_calls(conn)
    assert len(bounded) >= 5
    for sql, params in bounded:
        assert as_of in tuple(params or ()), f"as_of not threaded into: {sql[:70]}"


def test_bias_read_is_bounded_by_as_of():
    as_of = datetime(2026, 8, 1, 15, 0, tzinfo=timezone.utc)
    conn = _scripted()
    build_snapshot(conn, "SPY", as_of=as_of)
    bias_calls = [
        c for c in conn.cursor().calls if "FROM trade_bias_scores" in c[0]
    ]
    assert bias_calls, "trade_bias read never issued"
    for sql, params in bias_calls:
        assert _BOUND in sql and as_of in tuple(params or ())
