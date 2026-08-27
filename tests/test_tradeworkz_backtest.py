"""Tests for the TradeWorkz backtest harness (src/tradeworkz/backtest.py).

Covers the three pieces that carry the logic risk and can be unit-tested without
a live database:

* :func:`historical_spread_price` — no-look-ahead quote bounds, live→archive
  fallback, expired-leg intrinsic settlement, and the open/close sign model.
* :func:`rth_timesteps` and the market gates — RTH endpoints, weekend/holiday
  skip, window clamping, opens window, force-settle.
* The clock hook and the replay/reporting math — a full open→hold→exit
  round-trip through :class:`_BotRunner` with a stub bot and stubbed pricing,
  plus the expectancy / profit-factor / verdict summary.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from types import SimpleNamespace
from typing import Any, List, Optional, Tuple

import src.tradeworkz.backtest as bt
from src.tradeworkz.bots import base as bot_base
from src.tradeworkz.models import ExitDecision, Leg, OpenPosition, TradeSignal

# ---------------------------------------------------------------------------
# Fake DB plumbing
# ---------------------------------------------------------------------------


class _Cur:
    """Scriptable fake cursor. Routes by SQL substring; records every call."""

    def __init__(
        self,
        *,
        archive: bool = False,
        live_row: Optional[tuple] = None,
        archive_row: Optional[tuple] = None,
        settle_close: Optional[float] = None,
        record: Optional[List[Tuple[str, Any]]] = None,
    ) -> None:
        self.archive = archive
        self.live_row = live_row
        self.archive_row = archive_row
        self.settle_close = settle_close
        self.record: List[Tuple[str, Any]] = record if record is not None else []
        self._rows: List[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.record.append((sql, params))
        if "to_regclass" in sql:
            self._rows = [(self.archive,)]
        elif "option_chains_archive" in sql:
            self._rows = [self.archive_row] if self.archive_row else []
        elif "FROM option_chains" in sql:
            self._rows = [self.live_row] if self.live_row else []
        elif "FROM underlying_quotes" in sql:
            self._rows = [(self.settle_close,)] if self.settle_close is not None else []
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Conn:
    def __init__(self, cur: _Cur) -> None:
        self._cur = cur

    def cursor(self):
        return self._cur


def _leg(**over: Any) -> dict:
    leg = {
        "option_symbol": "SPY 260805C640",
        "side": "long",
        "option_type": "call",
        "strike": 640.0,
        "expiration": "2026-08-05",
    }
    leg.update(over)
    return leg


_AT = datetime(2026, 8, 5, 17, 0, tzinfo=timezone.utc)  # 13:00 ET, RTH


# ---------------------------------------------------------------------------
# Phase 2 — historical_spread_price
# ---------------------------------------------------------------------------


def test_open_prices_long_at_ask_short_at_bid():
    # long pays ask; a long-only debit is +net.
    conn = _Conn(_Cur(live_row=(1.00, 1.10, 1.05)))
    px = bt.historical_spread_price(conn, [_leg()], at=_AT, action="open", slippage_pct=0.0)
    assert abs(px - 1.10) < 1e-9
    # close inverts: long sells at bid.
    conn = _Conn(_Cur(live_row=(1.00, 1.10, 1.05)))
    pxc = bt.historical_spread_price(conn, [_leg()], at=_AT, action="close", slippage_pct=0.0)
    assert abs(pxc - 1.00) < 1e-9


def test_short_leg_collects_credit():
    # A single short call: open collects the bid → net is a credit (negative).
    conn = _Conn(_Cur(live_row=(2.00, 2.20, 2.10)))
    px = bt.historical_spread_price(
        conn, [_leg(side="short")], at=_AT, action="open", slippage_pct=0.0
    )
    assert abs(px - (-2.00)) < 1e-9


def test_quote_query_is_bounded_and_has_no_lookahead():
    rec: List[Tuple[str, Any]] = []
    conn = _Conn(_Cur(live_row=(1.0, 1.1, 1.0), record=rec))
    bt.historical_spread_price(
        conn, [_leg()], at=_AT, action="open", slippage_pct=0.0, tolerance_min=10
    )
    sym_calls = [c for c in rec if "option_symbol = %s" in c[0]]
    assert sym_calls, "exact-symbol query never issued"
    sql, params = sym_calls[0]
    assert "timestamp <= %s" in sql and "timestamp >= %s" in sql
    assert "NOW()" not in sql
    # params = (option_symbol, at, lo) with lo = at - tolerance (past-only).
    _, upper, lower = params
    assert upper == _AT
    assert lower == _AT - timedelta(minutes=10)
    assert lower < upper


def test_falls_back_to_archive_when_live_misses():
    # Live returns nothing for both symbol + structured; archive has the row.
    conn = _Conn(_Cur(archive=True, live_row=None, archive_row=(0.90, 1.00, 0.95)))
    px = bt.historical_spread_price(conn, [_leg()], at=_AT, action="open", slippage_pct=0.0)
    assert abs(px - 1.00) < 1e-9  # long open at archive ask


def test_archive_skipped_when_unavailable():
    conn = _Conn(_Cur(archive=False, live_row=None, archive_row=(9, 9, 9)))
    px = bt.historical_spread_price(conn, [_leg()], at=_AT, action="open")
    assert px is None  # archive not probed → no quote → no fill


def test_expired_leg_settles_at_intrinsic_on_close():
    # No option quote anywhere, but the leg expired on/before `at`; a long 640
    # call with the underlying closing at 645 settles at +5.00 intrinsic.
    conn = _Conn(_Cur(live_row=None, settle_close=645.0))
    px = bt.historical_spread_price(
        conn, [_leg(expiration="2026-08-05")], at=_AT, action="close", slippage_pct=0.0
    )
    assert abs(px - 5.0) < 1e-9


def test_no_quote_open_returns_none():
    conn = _Conn(_Cur(live_row=None))
    assert bt.historical_spread_price(conn, [_leg()], at=_AT, action="open") is None


# ---------------------------------------------------------------------------
# Phase 3 — timesteps + market gates + clock
# ---------------------------------------------------------------------------


def test_rth_timesteps_endpoints_and_interval():
    steps = bt.rth_timesteps(
        datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc),  # 08:00 ET
        datetime(2026, 8, 5, 21, 0, tzinfo=timezone.utc),  # 17:00 ET
        30,
    )
    et = [s.astimezone(bt._ET).strftime("%H:%M") for s in steps]
    assert et[0] == "09:30" and et[-1] == "16:00"
    assert "12:00" in et and len(et) == 14


def test_rth_timesteps_skips_weekend():
    steps = bt.rth_timesteps(
        datetime(2026, 8, 8, 13, 0, tzinfo=timezone.utc),  # Saturday
        datetime(2026, 8, 8, 20, 0, tzinfo=timezone.utc),
        30,
    )
    assert steps == []


def test_rth_timesteps_clamps_to_window():
    steps = bt.rth_timesteps(
        datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc),  # 10:00 ET
        datetime(2026, 8, 5, 18, 0, tzinfo=timezone.utc),  # 14:00 ET
        30,
    )
    et = [s.astimezone(bt._ET).strftime("%H:%M") for s in steps]
    assert et[0] == "10:00" and et[-1] == "14:00"


def test_rth_timesteps_skips_configured_holiday():
    from src.market_calendar import NYSE_HOLIDAYS

    weekday_holidays = [h for h in NYSE_HOLIDAYS if h.weekday() < 5]
    if not weekday_holidays:
        return  # no holiday data loaded in this env; nothing to assert
    h = sorted(weekday_holidays)[0]
    lo = bt._ET.localize(datetime.combine(h, time(8, 0))).astimezone(timezone.utc)
    hi = bt._ET.localize(datetime.combine(h, time(17, 0))).astimezone(timezone.utc)
    assert bt.rth_timesteps(lo, hi, 30) == []


def test_clock_hook_overrides_utcnow_and_clears():
    fixed = datetime(2020, 3, 4, 15, 30, tzinfo=timezone.utc)
    bot_base.set_backtest_clock(lambda: fixed)
    try:
        assert bot_base._utcnow() == fixed
    finally:
        bot_base.set_backtest_clock(None)
    assert bot_base._utcnow().year >= 2024


def test_opens_allowed_gates_session_and_cap():
    cap = time(15, 55)
    # 12:00 ET Wed — inside session, before cap.
    noon = bt._ET.localize(datetime(2026, 8, 5, 12, 0)).astimezone(timezone.utc)
    assert bt._opens_allowed(noon, cap) is True
    # 15:56 ET — past the no-new-opens cap.
    late = bt._ET.localize(datetime(2026, 8, 5, 15, 56)).astimezone(timezone.utc)
    assert bt._opens_allowed(late, cap) is False
    # Saturday — closed.
    sat = bt._ET.localize(datetime(2026, 8, 8, 12, 0)).astimezone(timezone.utc)
    assert bt._opens_allowed(sat, cap) is False


def _pos(**over: Any) -> OpenPosition:
    base = dict(
        id=-1,
        bot_id="b",
        underlying="SPY",
        opened_at=_AT,
        direction="bullish",
        strategy_type="single",
        legs=[_leg()],
        entry_price=1.0,
        current_price=1.0,
        quantity_open=1,
        unrealized_pnl=0.0,
        stop_price=None,
        target_price=None,
        time_stop_at=None,
        min_hold_until=None,
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.5,
        components_at_entry={},
    )
    base.update(over)
    return OpenPosition(**base)


def test_force_settle_due():
    past = _AT - timedelta(minutes=1)
    future = _AT + timedelta(hours=2)
    assert bt._force_settle_due(_pos(time_stop_at=past), _AT) is True
    assert bt._force_settle_due(_pos(time_stop_at=future), _AT) is False
    # Expired legs (yesterday) force-settle regardless of time_stop.
    expired = _pos(time_stop_at=None, legs=[_leg(expiration="2026-08-04")])
    assert bt._force_settle_due(expired, _AT) is True


# ---------------------------------------------------------------------------
# Replay round-trip through _BotRunner + reporting math
# ---------------------------------------------------------------------------


class _StubBot:
    def __init__(self, signal: Optional[TradeSignal], *, veto: bool = False):
        self._signal = signal
        self._veto = veto
        self.exit_calls = 0

    def open_criteria(self, snap):
        return self._signal

    def _bias_veto(self, snap, direction):
        return self._veto

    def exit_criteria(self, snap, pos):
        self.exit_calls += 1
        return ExitDecision(should_close=True, reason="test_exit")


def _runner_with(bot: _StubBot, enabled: bool = True) -> bt._BotRunner:
    r = object.__new__(bt._BotRunner)
    r.spec = SimpleNamespace(
        id="stub_bot", display_name="Stub", strategy_class="Stub", enabled=enabled
    )
    r.bot = bot
    r.slippage_pct = 0.0
    r.tolerance_min = 10
    r.open_by_underlying = {}
    r.trades = []
    r.vetoed = 0
    r.no_quote_opens = 0
    r.priced_marks = 0
    r.signals = 0
    r.entry_rejects = {}
    return r


def _signal() -> TradeSignal:
    return TradeSignal(
        bot_id="stub_bot",
        underlying="SPY",
        direction="bullish",
        strategy_type="single",
        legs=[
            Leg(
                option_symbol="SPY 260805C640",
                side="long",
                option_type="call",
                strike=640.0,
                expiration="2026-08-05",
            )
        ],
        entry_price=1.0,
        conviction=0.6,
        target_price=650.0,
        stop_price=630.0,
        time_stop_at=None,
        components_at_entry={"origin": "live"},
    )


def test_runner_open_then_exit_round_trip(monkeypatch):
    prices = {"open": 1.00, "close": 1.50}
    monkeypatch.setattr(
        bt,
        "historical_spread_price",
        lambda conn, legs, *, at, action, slippage_pct=None, tolerance_min=10: prices[action],
    )
    runner = _runner_with(_StubBot(_signal()))
    snap = SimpleNamespace(spot=640.0)
    t0 = _AT
    t1 = _AT + timedelta(minutes=5)
    runner.step(None, "SPY", snap, t0, opens_allowed=True)  # opens
    assert "SPY" in runner.open_by_underlying
    runner.step(None, "SPY", snap, t1, opens_allowed=True)  # marks + exits
    assert "SPY" not in runner.open_by_underlying
    assert len(runner.trades) == 1
    tr = runner.trades[0]
    assert tr.outcome == "win"
    assert abs(tr.pnl_dollars - 50.0) < 1e-6  # (1.50 − 1.00) × 100
    assert tr.reason == "test_exit"


def test_runner_respects_bias_veto(monkeypatch):
    monkeypatch.setattr(
        bt,
        "historical_spread_price",
        lambda *a, **k: 1.0,
    )
    runner = _runner_with(_StubBot(_signal(), veto=True))
    runner.step(None, "SPY", SimpleNamespace(spot=640.0), _AT, opens_allowed=True)
    assert runner.open_by_underlying == {}
    assert runner.vetoed == 1


def test_runner_skips_opens_when_not_allowed(monkeypatch):
    monkeypatch.setattr(bt, "historical_spread_price", lambda *a, **k: 1.0)
    runner = _runner_with(_StubBot(_signal()))
    runner.step(None, "SPY", SimpleNamespace(spot=640.0), _AT, opens_allowed=False)
    assert runner.open_by_underlying == {}


def test_runner_min_premium_floor_blocks_thin_entry(monkeypatch):
    monkeypatch.setattr(bt, "historical_spread_price", lambda *a, **k: 0.0)
    runner = _runner_with(_StubBot(_signal()))
    runner.step(None, "SPY", SimpleNamespace(spot=640.0), _AT, opens_allowed=True)
    assert runner.open_by_underlying == {}  # entry_net <= 0 → no fill


def _mk_trade(pnl: float) -> bt.SimTrade:
    return bt.SimTrade(
        bot_id="b",
        underlying="SPY",
        direction="bullish",
        strategy_type="single",
        opened_at=_AT,
        closed_at=_AT + timedelta(minutes=10),
        hold_minutes=10.0,
        entry_net=1.0,
        exit_net=1.0 + pnl / 100.0,
        max_loss_per_share=1.0,
        pnl_dollars=pnl,
        pnl_pct=pnl / 100.0,
        outcome="win" if pnl > 0 else "loss" if pnl < 0 else "scratch",
        reason="x",
        conviction=0.5,
    )


def test_summarize_bot_math():
    runner = _runner_with(_StubBot(None))
    runner.trades = [_mk_trade(100), _mk_trade(100), _mk_trade(-50), _mk_trade(-50)]
    rep = bt.summarize_bot(runner)
    assert rep["n_trades"] == 4
    assert rep["wins"] == 2 and rep["losses"] == 2
    assert abs(rep["win_rate"] - 0.5) < 1e-9
    assert abs(rep["profit_factor"] - 2.0) < 1e-9  # 200 / 100
    assert abs(rep["total_pnl"] - 100.0) < 1e-9
    assert abs(rep["expectancy"] - 25.0) < 1e-9
    assert rep["equity_curve"] == [100.0, 200.0, 150.0, 100.0]


def test_verdict_thresholds():
    assert bt._verdict(5, 3.0, 100.0) == "insufficient"  # too few trades
    assert bt._verdict(40, 1.5, 50.0) == "edge"  # PF>=1.1 & +expectancy
    assert bt._verdict(40, 0.95, -5.0) == "marginal"  # PF in [0.9,1.1)
    assert bt._verdict(40, 0.5, -100.0) == "no-edge"  # PF<0.9
    assert bt._verdict(40, None, 10.0) == "edge"  # no losses, +expectancy


def _mk_trade_at(pnl: float, at: datetime) -> bt.SimTrade:
    t = _mk_trade(pnl)
    t.opened_at = at
    t.closed_at = at + timedelta(minutes=10)
    return t


def test_split_report_robust_when_both_halves_positive():
    mid = _AT + timedelta(days=10)
    early, late = _AT, mid + timedelta(days=1)
    # 6+ trades per half (the soft floor), both halves net-positive.
    trades = (
        [_mk_trade_at(60, early) for _ in range(4)]
        + [_mk_trade_at(-20, early) for _ in range(3)]
        + [_mk_trade_at(50, late) for _ in range(4)]
        + [_mk_trade_at(-25, late) for _ in range(3)]
    )
    rep = bt._split_report(trades, mid, full_verdict="edge")
    assert rep is not None
    assert rep["train"]["n_trades"] == 7 and rep["test"]["n_trades"] == 7
    assert rep["train"]["expectancy"] > 0 and rep["test"]["expectancy"] > 0
    assert rep["robust"] is True


def test_split_report_not_robust_when_one_half_dies():
    """A config that shines in one half and dies in the other is noise, not
    edge — the exact pattern the guard exists to catch (e.g. a PF carried by
    two large wins clustered in one half of the window)."""
    mid = _AT + timedelta(days=10)
    early, late = _AT, mid + timedelta(days=1)
    trades = [_mk_trade_at(400, early) for _ in range(6)] + [  # all the profit early
        _mk_trade_at(-40, late) for _ in range(6)
    ]  # steady bleed late
    rep = bt._split_report(trades, mid, full_verdict="edge")
    assert rep["train"]["expectancy"] > 0
    assert rep["test"]["expectancy"] < 0
    assert rep["robust"] is False


def test_split_report_thin_half_cannot_certify():
    mid = _AT + timedelta(days=10)
    trades = [_mk_trade_at(50, _AT) for _ in range(2)] + [
        _mk_trade_at(50, mid + timedelta(days=1)) for _ in range(18)
    ]
    rep = bt._split_report(trades, mid, full_verdict="edge")
    assert rep["train"]["n_trades"] == 2  # below the per-half floor
    assert rep["robust"] is False


def test_split_report_requires_full_window_edge():
    mid = _AT + timedelta(days=10)
    trades = [_mk_trade_at(10, _AT) for _ in range(10)] + [
        _mk_trade_at(10, mid + timedelta(days=1)) for _ in range(10)
    ]
    assert bt._split_report(trades, mid, full_verdict="insufficient")["robust"] is False
    assert bt._split_report(trades, mid, full_verdict="edge")["robust"] is True
    assert bt._split_report(trades, None, full_verdict="edge") is None


def test_chain_window_spans_hot_and_archive():
    t0, t1 = _AT - timedelta(days=200), _AT - timedelta(days=80)
    t2, t3 = _AT - timedelta(days=90), _AT
    # Archive reaches further back than the pruned hot table: the clamp must
    # span both so screens are not capped at the hot table's rolling window.
    conn = _Conn(_Cur(archive=True, live_row=(t2, t3), archive_row=(t0, t1)))
    assert bt._chain_window(conn) == (t0, t3)


def test_chain_window_hot_only_when_no_archive():
    t2, t3 = _AT - timedelta(days=90), _AT
    conn = _Conn(_Cur(archive=False, live_row=(t2, t3), archive_row=(None, None)))
    assert bt._chain_window(conn) == (t2, t3)


def test_summarize_bot_carries_the_split():
    runner = _runner_with(_StubBot(None))
    runner.trades = [_mk_trade_at(100, _AT), _mk_trade_at(-50, _AT + timedelta(days=2))]
    rep = bt.summarize_bot(runner, split_at=_AT + timedelta(days=1))
    assert rep["split"]["train"]["n_trades"] == 1
    assert rep["split"]["test"]["n_trades"] == 1
    assert rep["split"]["robust"] is False  # insufficient full-window verdict
