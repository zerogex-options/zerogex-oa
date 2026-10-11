"""The v6 volatility-pricing layer: fetchers and snapshot helpers.

Every earlier TradeWorkz tier traded direction off a magnitude signal. The v6
candidates trade the magnitude itself, so the snapshot gained the inputs that
makes possible: the ATM front straddle (what the market charges for the
remaining move), the morning forecast's committed implied move, today's ATM
IV ranked against its own history, and the CASH session's open / high / low.

The fetcher tests use the same scripted fake cursor as
``tests/test_tw_flow_context.py`` (no DB) and lock in parsing, the
two-sided-quote rule, the hot-then-archive fallback, None-safety, and that
every read is as-of bounded. The helper tests build a ``MarketSnapshot``
directly and pin the arithmetic — the magnitude rebuild in particular is
asserted against the scoring engine's own saturation formula.

All timestamps are ET -> UTC for August (EDT, UTC-4).
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Tuple

from src.tradeworkz.context import MarketSnapshot, _percentile_rank
from src.tradeworkz.flow_context import (
    fetch_atm_front_quote,
    fetch_atm_iv_history,
    fetch_cash_session_bars,
    fetch_daily_forecast,
)

_BOUND = "COALESCE(%s::timestamptz, NOW())"
# Thursday 2026-08-06, 12:00 ET.
MID_12ET = datetime(2026, 8, 6, 16, 0, tzinfo=timezone.utc)
# Same session, 09:45 ET — before the first half hour has completed.
EARLY_0945ET = datetime(2026, 8, 6, 13, 45, tzinfo=timezone.utc)
# Saturday 2026-08-08, 12:00 ET.
SATURDAY = datetime(2026, 8, 8, 16, 0, tzinfo=timezone.utc)


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


def _selects(cur: _FakeCursor, table: str) -> List[Tuple[str, Any]]:
    return [c for c in cur.calls if table in c[0]]


# ======================================================================
# fetch_atm_front_quote
# ======================================================================

_T = MID_12ET


def _chain_rows(strike: float, c: tuple, p: tuple, ts: datetime = _T) -> list:
    """(option_type, strike, bid, ask, iv, timestamp) rows for one strike."""
    return [("C", strike, *c, ts), ("P", strike, *p, ts)]


def test_atm_quote_picks_the_nearest_two_sided_strike():
    cur = _FakeCursor()
    rows = (
        _chain_rows(655.0, (3.00, 3.10, 0.20), (2.50, 2.60, 0.22))
        + _chain_rows(656.0, (2.40, 2.50, 0.19), (3.10, 3.20, 0.21))  # nearest to 655.9
        + _chain_rows(657.0, (1.90, 2.00, 0.18), (3.80, 3.90, 0.20))
    )
    cur.program("FROM option_chains\n", rows)
    strike, mid, bid, iv = fetch_atm_front_quote(_Conn(cur), "SPY", 655.9, "2026-08-06", _T)
    assert strike == 656.0
    assert abs(mid - ((2.40 + 2.50) / 2 + (3.10 + 3.20) / 2)) < 1e-9
    assert abs(bid - (2.40 + 3.10)) < 1e-9
    assert abs(iv - 0.20) < 1e-9
    call = _selects(cur, "FROM option_chains\n")[0]
    assert _BOUND in call[0]
    assert _T in tuple(call[1])
    assert "2026-08-06" in tuple(call[1])


def test_atm_quote_skips_a_one_sided_strike():
    cur = _FakeCursor()
    rows = _chain_rows(
        656.0, (2.40, 2.50, 0.19), (0.0, 3.20, 0.21)
    ) + _chain_rows(  # put has no bid
        657.0, (1.90, 2.00, 0.18), (3.80, 3.90, 0.20)
    )
    cur.program("FROM option_chains\n", rows)
    strike, mid, bid, iv = fetch_atm_front_quote(_Conn(cur), "SPY", 655.9, "2026-08-06", _T)
    assert strike == 657.0
    assert abs(bid - (1.90 + 3.80)) < 1e-9


def test_atm_quote_uses_the_latest_row_per_strike_and_type():
    cur = _FakeCursor()
    stale = _T - timedelta(minutes=8)
    rows = _chain_rows(656.0, (2.40, 2.50, 0.19), (3.10, 3.20, 0.21), ts=_T) + _chain_rows(
        656.0, (9.00, 9.10, 0.50), (9.00, 9.10, 0.50), ts=stale
    )
    cur.program("FROM option_chains\n", rows)
    _, mid, _, iv = fetch_atm_front_quote(_Conn(cur), "SPY", 656.0, "2026-08-06", _T)
    assert abs(mid - ((2.40 + 2.50) / 2 + (3.10 + 3.20) / 2)) < 1e-9
    assert abs(iv - 0.20) < 1e-9


def test_atm_quote_falls_back_to_the_archive_when_the_hot_table_is_empty():
    cur = _FakeCursor()
    cur.program(
        "FROM option_chains_archive",
        _chain_rows(6700.0, (20.0, 21.0, 0.15), (19.0, 20.0, 0.16)),
    )
    strike, mid, bid, iv = fetch_atm_front_quote(_Conn(cur), "SPX", 6701.0, "2026-08-06", _T)
    assert strike == 6700.0
    assert abs(bid - 39.0) < 1e-9
    assert len(_selects(cur, "FROM option_chains_archive")) == 1


def test_atm_quote_requires_both_legs_and_handles_absence():
    cur = _FakeCursor()
    cur.program("FROM option_chains\n", [("C", 656.0, 2.4, 2.5, 0.2, _T)])  # call only
    assert fetch_atm_front_quote(_Conn(cur), "SPY", 656.0, "2026-08-06", _T) == (
        None,
        None,
        None,
        None,
    )
    assert fetch_atm_front_quote(_Conn(_FakeCursor()), "SPY", 656.0, "2026-08-06", _T) == (
        None,
        None,
        None,
        None,
    )
    assert fetch_atm_front_quote(_Conn(_FakeCursor()), "SPY", 0.0, "2026-08-06", _T) == (
        None,
        None,
        None,
        None,
    )


# ======================================================================
# fetch_daily_forecast
# ======================================================================


def test_daily_forecast_reads_the_committed_row_as_of():
    cur = _FakeCursor()
    cur.program(
        "FROM daily_forecast",
        [(4.85, 0.92, "normal", {"vix_z_score_20d": -0.4, "is_event_day": False})],
    )
    im, ratio, state, vix_z, event = fetch_daily_forecast(_Conn(cur), "SPY", _T)
    assert im == 4.85 and ratio == 0.92 and state == "normal"
    assert vix_z == -0.4 and event is False
    call = _selects(cur, "FROM daily_forecast")[0]
    assert _BOUND in call[0]
    assert "open_ts <=" in call[0]  # never read before it was written
    assert _T in tuple(call[1])


def test_daily_forecast_parses_a_string_json_blob_and_tolerates_missing_keys():
    cur = _FakeCursor()
    cur.program("FROM daily_forecast", [(4.85, None, "expansion", '{"is_event_day": true}')])
    im, ratio, state, vix_z, event = fetch_daily_forecast(_Conn(cur), "SPY", _T)
    assert im == 4.85 and ratio is None and state == "expansion"
    assert vix_z is None and event is True


def test_daily_forecast_none_when_absent():
    assert fetch_daily_forecast(_Conn(_FakeCursor()), "QQQ", _T) == (None, None, None, None, None)


# ======================================================================
# fetch_atm_iv_history
# ======================================================================


def test_atm_iv_history_returns_prior_sessions_only_and_drops_junk():
    cur = _FakeCursor()
    cur.program("FROM daily_atm_iv", [(0.18,), (None,), (0.0,), (0.21,)])
    hist = fetch_atm_iv_history(_Conn(cur), "SPY", _T, 60)
    assert hist == [0.18, 0.21]
    call = _selects(cur, "FROM daily_atm_iv")[0]
    assert "trading_date <" in call[0]  # strictly before today's (still-moving) row
    assert _T in tuple(call[1])
    assert 60 in tuple(call[1])


def test_atm_iv_history_empty_when_absent():
    assert fetch_atm_iv_history(_Conn(_FakeCursor()), "SPY", _T) == []


def test_percentile_rank_needs_enough_history():
    assert _percentile_rank(0.2, [0.1] * 19) is None
    hist = [0.10 + 0.01 * i for i in range(20)]  # 0.10 .. 0.29
    assert _percentile_rank(0.195, hist) == 50.0
    assert _percentile_rank(0.50, hist) == 100.0
    assert _percentile_rank(0.05, hist) == 0.0
    assert _percentile_rank(None, hist) is None


# ======================================================================
# fetch_cash_session_bars
# ======================================================================


def test_cash_session_bars_bound_to_the_cash_open_and_as_of():
    cur = _FakeCursor()
    cur.program("FROM underlying_quotes", [(655.20, 657.90, 653.10, 656.40)])
    o, h, lo, fhh = fetch_cash_session_bars(_Conn(cur), "SPY", MID_12ET)
    assert (o, h, lo, fhh) == (655.20, 657.90, 653.10, 656.40)
    sql, params = _selects(cur, "FROM underlying_quotes")[0]
    # 09:30 ET on 2026-08-06 is 13:30 UTC; the open scan and the range scan
    # both start there, the first-half-hour scan ends at 10:00 ET, and
    # nothing reads past as_of.
    session_start = datetime(2026, 8, 6, 13, 30, tzinfo=timezone.utc)
    assert params[1] == session_start and params[2] == MID_12ET
    assert params[4] == session_start and params[5] == session_start + timedelta(minutes=30)
    assert params[7] == session_start and params[8] == MID_12ET


def test_cash_session_bars_index_open_skips_the_stale_0930_bar():
    cur = _FakeCursor()
    cur.program("FROM underlying_quotes", [(6700.0, 6710.0, 6690.0, 6702.0)])
    fetch_cash_session_bars(_Conn(cur), "SPX", MID_12ET, open_offset_min=1)
    _, params = _selects(cur, "FROM underlying_quotes")[0]
    assert params[1] == datetime(2026, 8, 6, 13, 31, tzinfo=timezone.utc)


def test_cash_session_bars_first_half_hour_is_none_before_ten_et():
    cur = _FakeCursor()
    cur.program("FROM underlying_quotes", [(655.20, 656.00, 654.80, 655.70)])
    o, h, lo, fhh = fetch_cash_session_bars(_Conn(cur), "SPY", EARLY_0945ET)
    assert o == 655.20 and h == 656.00 and lo == 654.80
    assert fhh is None  # the window has not completed


def test_cash_session_bars_none_outside_the_sessions_own_day():
    cur = _FakeCursor()
    cur.program("FROM underlying_quotes", [(1.0, 2.0, 0.5, 1.5)])
    assert fetch_cash_session_bars(_Conn(cur), "SPY", SATURDAY) == (None, None, None, None)
    assert not _selects(cur, "FROM underlying_quotes")  # never queried


# ======================================================================
# MarketSnapshot helpers
# ======================================================================


def _snap(**over) -> MarketSnapshot:
    base = dict(underlying="SPY", timestamp=MID_12ET, spot=656.0)
    base.update(over)
    return MarketSnapshot(**base)


def _msi_payload(**scores) -> dict:
    pts = {"gamma_anchor": 30, "net_gex_sign": 16, "volatility_regime": 6, "put_call_ratio": 12}
    return {k: {"score": v, "max_points": pts.get(k, 0)} for k, v in scores.items()}


def test_magnitude_score_prefers_the_published_shadow_score():
    snap = _snap(msi_components={"__aggregation__": {"magnitude_score": 63.2}})
    assert snap.magnitude_score() == 63.2
    assert snap.magnitude_band() == "controlled_trend"


def test_magnitude_score_rebuilds_with_the_engines_arithmetic():
    # Direction components present but ignored; the four magnitude components
    # renormalize onto the full scale and saturate exactly as ScoringEngine.
    comps = _msi_payload(
        gamma_anchor=0.5,
        net_gex_sign=-1.0,
        volatility_regime=0.2,
        put_call_ratio=0.1,
        order_flow_imbalance=1.0,
        dealer_delta_pressure=1.0,
    )
    comps["order_flow_imbalance"]["max_points"] = 19
    comps["dealer_delta_pressure"]["max_points"] = 17
    snap = _snap(msi_components=comps)
    offset = 30 * 0.5 + 16 * -1.0 + 6 * 0.2 + 12 * 0.1
    active = 30 + 16 + 6 + 12
    expected = 50.0 + 50.0 * math.tanh((offset * (100.0 / active)) / 50.0)
    assert abs(snap.magnitude_score() - expected) < 1e-9


def test_magnitude_score_renormalizes_a_partial_component_set():
    snap = _snap(msi_components=_msi_payload(gamma_anchor=1.0))
    # One component at +1 renormalizes to the full 100 points -> tanh(2).
    assert abs(snap.magnitude_score() - (50.0 + 50.0 * math.tanh(2.0))) < 1e-9
    assert snap.magnitude_band() == "trend_expansion"


def test_magnitude_score_none_without_magnitude_components():
    assert _snap().magnitude_score() is None
    assert _snap(msi_components={"order_flow_imbalance": {"score": 1.0}}).magnitude_score() is None
    assert _snap().magnitude_band() is None


def test_magnitude_band_edges_match_the_engine():
    for score, band in (
        (70.0, "trend_expansion"),
        (69.9, "controlled_trend"),
        (40.0, "controlled_trend"),
        (39.9, "chop_range"),
        (20.0, "chop_range"),
        (19.9, "high_risk_reversal"),
    ):
        snap = _snap(msi_components={"__aggregation__": {"magnitude_score": score}})
        assert snap.magnitude_band() == band, score


def test_realized_over_implied_annualizes_the_one_minute_sigma():
    # Alternating +/- 10 bps closes: population sigma of returns ~ 0.001.
    closes = [100.0 * (1.001 if i % 2 else 1.0) for i in range(31)]
    snap = _snap(recent_closes=closes, atm_iv_front=0.20)
    sigma = snap.realized_sigma_1min()
    assert sigma is not None and abs(sigma - 0.0009995) < 1e-5
    ratio = snap.realized_over_implied()
    assert abs(ratio - sigma * math.sqrt(252 * 390) / 0.20) < 1e-9
    assert _snap(recent_closes=closes).realized_over_implied() is None  # no IV
    assert _snap(recent_closes=[656.0] * 5, atm_iv_front=0.2).realized_over_implied() is None
    assert _snap(recent_closes=[656.0] * 30, atm_iv_front=0.2).realized_over_implied() is None


def test_straddle_implied_sigma_inverts_the_normal_straddle_value():
    snap = _snap(atm_straddle_mid_front=4.0)
    assert abs(snap.straddle_implied_sigma() - 4.0 / math.sqrt(2.0 / math.pi)) < 1e-9
    assert _snap().straddle_implied_sigma() is None


def test_expected_remaining_move_uses_the_diurnal_profile():
    from src.jobs.intraday_cone_model import variance_fraction

    snap = _snap(forecast_implied_move=5.0)  # 12:00 ET -> 150 minutes elapsed
    frac = variance_fraction(150.0, 390.0)
    assert abs(snap.expected_remaining_move() - 5.0 * math.sqrt(frac)) < 1e-9
    # Less than the naive sqrt-of-time scaling at lunch (midday trough)...
    assert snap.expected_remaining_move() < 5.0 * math.sqrt(240.0 / 390.0)
    # ...and None without a forecast or after the close.
    assert _snap().expected_remaining_move() is None
    after_close = _snap(
        timestamp=datetime(2026, 8, 6, 20, 30, tzinfo=timezone.utc), forecast_implied_move=5.0
    )
    assert after_close.expected_remaining_move() is None


def test_cash_range_and_returns():
    snap = _snap(
        spot=656.0,
        cash_open=650.0,
        cash_high=658.0,
        cash_low=649.0,
        first_half_hour_close=652.0,
        forecast_implied_move=6.0,
    )
    assert abs(snap.cash_range_over_implied() - 9.0 / 6.0) < 1e-9
    assert abs(snap.day_return() - (656.0 / 650.0 - 1.0)) < 1e-12
    assert abs(snap.first_half_hour_return() - (652.0 / 650.0 - 1.0)) < 1e-12
    bare = _snap()
    assert bare.cash_range_over_implied() is None
    assert bare.day_return() is None
    assert bare.first_half_hour_return() is None
