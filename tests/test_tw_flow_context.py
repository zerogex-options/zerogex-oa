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
    fetch_hedge_impulse,
    fetch_profile_geometry,
    fetch_put_panic_window,
    fetch_recent_flow_window,
    fetch_recent_option_flow,
    fetch_second_order_by_bucket,
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
    close, raw, smooth, charm_flip, vanna_flip = fetch_forced_flow(_Conn(cur), "SPY", AS_OF)
    assert close == 6.0e8  # smooth preferred over raw 9.9e8
    assert raw == 9.9e8  # both sources surfaced separately (settlement residual)
    assert smooth == 6.0e8
    assert charm_flip == 751.0
    assert vanna_flip == 748.0
    # as-of bounded
    call = _select_calls(cur, "FROM forced_flow_profile")[0]
    assert _BOUND in call[0] and AS_OF in tuple(call[1])


def test_forced_flow_falls_back_to_raw_when_smooth_null():
    cur = _FakeCursor()
    cur.program("FROM forced_flow_profile", [(9.9e8, None, None, None)])
    close, raw, smooth, _, _ = fetch_forced_flow(_Conn(cur), "SPY", AS_OF)
    assert close == 9.9e8
    assert raw == 9.9e8
    assert smooth is None


def test_forced_flow_none_when_absent():
    cur = _FakeCursor()  # nothing programmed
    assert fetch_forced_flow(_Conn(cur), "SPY", AS_OF) == (None, None, None, None, None)


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


# --------------------------------------------------- v5: bucketed second order


def test_second_order_by_bucket_splits_the_ladder():
    cur = _FakeCursor()
    ts0 = datetime(2026, 8, 1, 14, 30, tzinfo=timezone.utc)
    cur.program("SELECT MAX(timestamp)", [(ts0,)])
    # (bucket, sum charm, sum vanna, count)
    cur.program(
        "GROUP BY expiration_bucket",
        [("0dte", 1.0e8, -3.0e7, 20), ("weekly", 6.0e8, -1.1e8, 55)],
    )
    charm_0, charm_w, vanna_0, vanna_w = fetch_second_order_by_bucket(_Conn(cur), "SPY", AS_OF)
    assert charm_0 == 1.0e8 and charm_w == 6.0e8
    assert vanna_0 == -3.0e7 and vanna_w == -1.1e8


def test_second_order_by_bucket_none_when_column_unpopulated():
    cur = _FakeCursor()
    ts0 = datetime(2026, 8, 1, 14, 30, tzinfo=timezone.utc)
    cur.program("SELECT MAX(timestamp)", [(ts0,)])
    # No rows grouped — expiration_bucket is null on early history.
    assert fetch_second_order_by_bucket(_Conn(cur), "SPY", AS_OF) == (None, None, None, None)


# --------------------------------------------------------- v5: hedge impulse


def test_hedge_impulse_returns_shares_and_tape_and_is_bounded():
    cur = _FakeCursor()
    cur.program("FROM flow_contract_facts", [(7.5e5, 340)])
    cur.program("FROM underlying_quotes", [(5.0e6,)])
    shares, tape = fetch_hedge_impulse(_Conn(cur), "SPY", 15, AS_OF)
    assert shares == 7.5e5
    assert tape == 5.0e6
    call = _select_calls(cur, "FROM flow_contract_facts")[0]
    assert _BOUND in call[0] and AS_OF in tuple(call[1])


def test_hedge_impulse_none_when_no_flow_rows():
    cur = _FakeCursor()
    cur.program("FROM flow_contract_facts", [(None, 0)])  # count == 0
    assert fetch_hedge_impulse(_Conn(cur), "SPY", 15, AS_OF) == (None, None)


# ------------------------------------------------------------ v5: put panic


def test_put_panic_window_metrics_and_median_baseline():
    cur = _FakeCursor()
    # Both queries hit flow_contract_facts; the cursor matches first-substring-
    # wins, so the more specific baseline matcher must be programmed first.
    cur.program("GROUP BY bucket_back", [(1, 1.0e5), (2, 3.0e5), (3, 0.5e5), (4, 2.0e5)])
    # (put net buy, put gross, all gross, count)
    cur.program("FROM flow_contract_facts", [(8.0e5, 1.2e6, 1.5e6, 90)])
    put_net, dominance, baseline = fetch_put_panic_window(_Conn(cur), "SPY", 15, AS_OF)
    assert put_net == 8.0e5
    assert dominance == 1.2e6 / 1.5e6
    assert baseline == 1.5e5  # median of [0.5e5, 1.0e5, 2.0e5, 3.0e5]


def test_put_panic_window_no_baseline_early_session():
    cur = _FakeCursor()
    cur.program("GROUP BY bucket_back", [(1, 1.0e5)])  # < 3 prior buckets
    cur.program("FROM flow_contract_facts", [(8.0e5, 1.2e6, 1.5e6, 90)])
    _, _, baseline = fetch_put_panic_window(_Conn(cur), "SPY", 15, AS_OF)
    assert baseline is None


# ------------------------------------------------------ v5: profile geometry


def _profile_points() -> list[dict]:
    # Simple V: deep trough 1% below spot 755, flat above.
    return [
        {"price": 743.0, "gex": -2.0e8},
        {"price": 747.5, "gex": -1.4e9},  # the trough
        {"price": 751.2, "gex": -9.0e8},
        {"price": 755.0, "gex": 1.0e8},
        {"price": 758.8, "gex": 2.0e8},
        {"price": 762.6, "gex": 3.0e8},
    ]


def test_profile_geometry_interpolates_and_finds_trough():
    cur = _FakeCursor()
    cur.program("FROM gex_profile", [(_profile_points(),)])
    g_dn, g_up, trough_price, trough_gex = fetch_profile_geometry(_Conn(cur), "SPY", 755.0, AS_OF)
    # spot*(1-0.005)=751.225 ~ the -9.0e8 grid point; spot*1.005 between +1e8/+2e8.
    assert g_dn is not None and -9.5e8 < g_dn < -8.5e8
    assert g_up is not None and 1.0e8 < g_up < 2.0e8
    assert trough_price == 747.5
    assert trough_gex == -1.4e9
    call = _select_calls(cur, "FROM gex_profile")[0]
    assert _BOUND in call[0] and AS_OF in tuple(call[1])


def test_profile_geometry_none_on_empty_profile():
    cur = _FakeCursor()
    cur.program("FROM gex_profile", [([],)])
    assert fetch_profile_geometry(_Conn(cur), "SPY", 755.0, AS_OF) == (
        None,
        None,
        None,
        None,
    )
