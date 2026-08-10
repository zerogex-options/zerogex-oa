"""Unit tests for the edge-metric fetchers in src/tradeworkz/flow_context.py.

These are the best-effort, as-of-bounded reads that surface the flow /
second-order / forced-flow layers onto the snapshot. The tests use a scripted
fake cursor (no DB) and lock in: correct parsing / sign passthrough, the
smooth-over-raw preference for close-charm flow, None-safety on missing data,
and that every read threads ``as_of`` into a ``COALESCE(%s::timestamptz, NOW())``
bound so the backtest harness can never see a future row.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, List, Tuple

from src.tradeworkz.flow_context import (
    fetch_forced_flow,
    fetch_recent_flow_window,
    fetch_recent_option_flow,
    fetch_second_order_totals,
    fetch_vix_lookback,
)

_BOUND = "COALESCE(%s::timestamptz, NOW())"
AS_OF = datetime(2026, 8, 1, 15, 0, tzinfo=timezone.utc)


class _FakeCursor:
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
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur

    def cursor(self):
        return self._cur


def _select_calls(cur: _FakeCursor, table: str) -> List[Tuple[str, Any]]:
    return [c for c in cur.calls if table in c[0]]


# ---------------------------------------------------------------- forced flow


def test_forced_flow_prefers_smooth_and_passes_levels():
    cur = _FakeCursor()
    # (close_charm_flow, close_charm_flow_smooth, charm_flip, vanna_flip)
    cur.program("FROM forced_flow_profile", [(9.9e8, 6.0e8, 751.0, 748.0)])
    close, charm_flip, vanna_flip = fetch_forced_flow(_Conn(cur), "SPY", AS_OF)
    assert close == 6.0e8  # smooth preferred over raw 9.9e8
    assert charm_flip == 751.0
    assert vanna_flip == 748.0
    # as-of bounded
    call = _select_calls(cur, "FROM forced_flow_profile")[0]
    assert _BOUND in call[0] and AS_OF in tuple(call[1])


def test_forced_flow_falls_back_to_raw_when_smooth_null():
    cur = _FakeCursor()
    cur.program("FROM forced_flow_profile", [(9.9e8, None, None, None)])
    close, _, _ = fetch_forced_flow(_Conn(cur), "SPY", AS_OF)
    assert close == 9.9e8


def test_forced_flow_none_when_absent():
    cur = _FakeCursor()  # nothing programmed
    assert fetch_forced_flow(_Conn(cur), "SPY", AS_OF) == (None, None, None)


# ------------------------------------------------------------- recent flow


def test_recent_option_flow_returns_latest_prev_and_volume():
    cur = _FakeCursor()
    # rows are DESC by bar_start: latest first, then prior
    cur.program("FROM flow_series_5min", [(2.0e6, 5000), (1.0e6, 3000)])
    net_prem, net_prem_prev, net_vol = fetch_recent_option_flow(_Conn(cur), "SPY", AS_OF)
    assert net_prem == 2.0e6
    assert net_prem_prev == 1.0e6
    assert net_vol == 5000.0


def test_recent_option_flow_prev_none_with_one_bar():
    cur = _FakeCursor()
    cur.program("FROM flow_series_5min", [(2.0e6, 5000)])
    net_prem, net_prem_prev, net_vol = fetch_recent_option_flow(_Conn(cur), "SPY", AS_OF)
    assert net_prem == 2.0e6
    assert net_prem_prev is None
    assert net_vol == 5000.0


def test_recent_option_flow_none_when_absent():
    cur = _FakeCursor()
    assert fetch_recent_option_flow(_Conn(cur), "SPY", AS_OF) == (None, None, None)


# ---------------------------------------------------- recent flow window


def test_recent_flow_window_computes_recent_and_prior():
    cur = _FakeCursor()
    # DESC by bar_start: index 0 = latest. (net_premium_cum, net_volume_cum).
    # window=3: recent = cum[0]-cum[3]; prior = cum[3]-cum[6].
    cur.program(
        "FROM flow_series_5min",
        [(1000, 100), (950, 95), (900, 90), (800, 80), (700, 70), (600, 60), (500, 50)],
    )
    recent_p, recent_v, prior_p = fetch_recent_flow_window(_Conn(cur), "SPY", 3, AS_OF)
    assert recent_p == 200.0  # 1000 - 800
    assert recent_v == 20.0  # 100 - 80
    assert prior_p == 300.0  # 800 - 500


def test_recent_flow_window_prior_none_with_one_window():
    cur = _FakeCursor()
    # Only w+1 = 4 rows: recent computable, prior window absent.
    cur.program("FROM flow_series_5min", [(1000, 100), (950, 95), (900, 90), (800, 80)])
    recent_p, recent_v, prior_p = fetch_recent_flow_window(_Conn(cur), "SPY", 3, AS_OF)
    assert recent_p == 200.0
    assert prior_p is None


def test_recent_flow_window_none_when_too_little_history():
    cur = _FakeCursor()
    cur.program("FROM flow_series_5min", [(1000, 100), (950, 95)])  # < w+1
    assert fetch_recent_flow_window(_Conn(cur), "SPY", 3, AS_OF) == (None, None, None)


# ----------------------------------------------------- second-order totals


def test_second_order_none_without_timestamp():
    cur = _FakeCursor()  # MAX(timestamp) returns nothing
    assert fetch_second_order_totals(_Conn(cur), "SPY", AS_OF) == (None, None)


def test_second_order_sums_when_present():
    cur = _FakeCursor()
    ts0 = datetime(2026, 8, 1, 14, 30, tzinfo=timezone.utc)
    cur.program("SELECT MAX(timestamp)", [(ts0,)])
    # (sum dealer_vanna, sum dealer_charm, count)
    cur.program("COALESCE(SUM(", [(-1.2e8, 9.0e9, 40)])
    vanna, charm = fetch_second_order_totals(_Conn(cur), "SPY", AS_OF)
    assert vanna == -1.2e8
    assert charm == 9.0e9


def test_second_order_none_when_front_expiration_empty():
    cur = _FakeCursor()
    ts0 = datetime(2026, 8, 1, 14, 30, tzinfo=timezone.utc)
    cur.program("SELECT MAX(timestamp)", [(ts0,)])
    cur.program("COALESCE(SUM(", [(0.0, 0.0, 0)])  # count == 0 -> no data
    assert fetch_second_order_totals(_Conn(cur), "SPY", AS_OF) == (None, None)


# --------------------------------------------------------------- vix lookback


def test_vix_lookback_returns_close_and_is_bounded():
    cur = _FakeCursor()
    cur.program("FROM vix_bars", [(16.4,)])
    assert fetch_vix_lookback(_Conn(cur), 30, AS_OF) == 16.4
    call = _select_calls(cur, "FROM vix_bars")[0]
    assert _BOUND in call[0] and AS_OF in tuple(call[1])


def test_vix_lookback_none_when_absent():
    cur = _FakeCursor()
    assert fetch_vix_lookback(_Conn(cur), 30, AS_OF) is None
