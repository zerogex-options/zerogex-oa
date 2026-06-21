"""Tests for the backtesting platform engine (src/backtesting).

Covers:
  * BacktestSpec validation (SpecError branches + clamping).
  * Leg selection (persisted legs vs synthetic ATM fallback).
  * The deterministic capital/concurrency walk in ``_simulate`` (P&L math,
    sizing, equity curve, drawdown, summary stats).
  * The end-to-end ``run_backtest`` path against a SQL-routing fake conn that
    reuses the real ``fetch_action_cards`` / ``fetch_quotes`` / leg-quote SQL.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from src.backtesting.engine import (
    _apply_cooldown,
    _build_candidate,
    _select_leg,
    _simulate,
    run_backtest,
)
from src.signals.playbook.backtest import fetch_action_cards
from src.backtesting.models import BacktestSpec, SpecError
from src.signals.playbook.backtest import CardRow

ET = timezone.utc
T0 = datetime(2026, 5, 1, 14, 0, tzinfo=ET)


# ----------------------------------------------------------------------
# Spec validation
# ----------------------------------------------------------------------


def test_spec_minimal_ok():
    spec = BacktestSpec.from_dict(
        {"underlying": "spy", "start_date": "2026-05-01", "end_date": "2026-05-10"}
    )
    assert spec.underlying == "SPY"  # upper-cased
    assert spec.patterns == []
    assert spec.sizing.capital == 25_000.0  # default
    assert spec.fill_model.slippage_pct == 0.01


def test_spec_rejects_reversed_window():
    with pytest.raises(SpecError):
        BacktestSpec.from_dict(
            {"underlying": "SPY", "start_date": "2026-05-10", "end_date": "2026-05-01"}
        )


def test_spec_rejects_oversized_window():
    with pytest.raises(SpecError):
        BacktestSpec.from_dict(
            {"underlying": "SPY", "start_date": "2026-01-01", "end_date": "2026-12-31"}
        )


def test_spec_requires_underlying():
    with pytest.raises(SpecError):
        BacktestSpec.from_dict({"start_date": "2026-05-01", "end_date": "2026-05-02"})


def test_spec_clamps_sizing_and_fill():
    spec = BacktestSpec.from_dict(
        {
            "underlying": "SPY",
            "start_date": "2026-05-01",
            "end_date": "2026-05-02",
            "fill_model": {"slippage_pct": 5.0, "commission_per_contract": -3},
            "sizing": {"capital": 1, "risk_per_trade_pct": 999, "max_concurrent": 999},
        }
    )
    assert spec.fill_model.slippage_pct == 0.25  # clamped to hi
    assert spec.fill_model.commission_per_contract == 0.0  # clamped to lo
    assert spec.sizing.capital == 500.0  # min capital
    assert spec.sizing.risk_per_trade_pct == 100.0  # clamped
    assert spec.sizing.max_concurrent == 20  # clamped


def test_spec_roundtrips_through_dict():
    raw = {
        "underlying": "QQQ",
        "start_date": "2026-05-01",
        "end_date": "2026-05-05",
        "patterns": ["gamma_flip_break"],
        "exit": {"max_hold_minutes": 120},
    }
    spec = BacktestSpec.from_dict(raw)
    again = BacktestSpec.from_dict(spec.to_dict())
    assert again.underlying == "QQQ"
    assert again.patterns == ["gamma_flip_break"]
    assert again.exit.max_hold_minutes == 120


# ----------------------------------------------------------------------
# Leg selection
# ----------------------------------------------------------------------


def _card(**kw) -> CardRow:
    payload: dict[str, Any] = {
        "entry": {"ref_price": kw.get("entry", 500.0), "trigger": "at_market"},
        "target": {"ref_price": kw.get("target", 503.0), "kind": "level"},
        "stop": {"ref_price": kw.get("stop", 498.0), "kind": "level"},
        "max_hold_minutes": kw.get("max_hold", 120),
    }
    if "legs" in kw:
        payload["legs"] = kw["legs"]
    return CardRow(
        underlying="SPY",
        timestamp=kw.get("ts", T0),
        pattern=kw.get("pattern", "gamma_flip_break"),
        action="BUY_CALL",
        tier=kw.get("tier", "0DTE"),
        direction=kw.get("direction", "bullish"),
        confidence=0.7,
        payload=payload,
    )


def test_select_leg_prefers_persisted_buy_leg():
    leg = _select_leg(
        _card(legs=[{"expiry": "2026-05-01", "strike": 500, "right": "C", "side": "BUY"}])
    )
    assert leg == {"expiry": "2026-05-01", "strike": 500, "right": "C"}


def test_select_leg_synthesizes_atm_when_no_legs():
    leg = _select_leg(_card(direction="bearish", entry=501.4))
    assert leg["right"] == "P"  # bearish → put
    assert leg["strike"] == 501  # rounded ATM
    assert leg["expiry"] is None  # resolved at lookup time


def test_select_leg_none_without_entry_ref():
    card = _card()
    card.payload["entry"] = {}
    assert _select_leg(card) is None


# ----------------------------------------------------------------------
# Simulation math
# ----------------------------------------------------------------------


def _candidate(*, seq_pnl_per_contract: float, entry_premium: float, entered: datetime,
               exited: datetime, pattern: str = "gamma_flip_break", outcome: str = "target_hit"):
    return {
        "card": _card(pattern=pattern),
        "outcome": outcome,
        "entered_at": entered,
        "exited_at": exited,
        "option_symbol": "SPY 260501C500",
        "option_type": "C",
        "strike": 500.0,
        "expiration": date(2026, 5, 1),
        "entry_premium": entry_premium,
        "exit_premium": entry_premium + seq_pnl_per_contract / 100.0,
        "pnl_per_contract": seq_pnl_per_contract,
        "hold_minutes": int((exited - entered).total_seconds() // 60),
        "mfe_pct": 0.01,
        "mae_pct": -0.005,
    }


def test_simulate_single_winning_trade_pnl_and_commission():
    spec = BacktestSpec.from_dict(
        {
            "underlying": "SPY",
            "start_date": "2026-05-01",
            "end_date": "2026-05-01",
            "fill_model": {"slippage_pct": 0.0, "commission_per_contract": 1.0},
            "sizing": {"capital": 10_000, "risk_per_trade_pct": 10, "max_concurrent": 5},
        }
    )
    # entry premium $2.00 → $200/contract; 10% of 10k = $1000 risk → 5 contracts.
    cand = _candidate(
        seq_pnl_per_contract=50.0, entry_premium=2.0,
        entered=T0, exited=T0 + timedelta(minutes=30),
    )
    result = _simulate([cand], spec)
    assert len(result.trades) == 1
    tr = result.trades[0]
    assert tr.contracts == 5
    assert tr.gross_pnl == pytest.approx(250.0)  # 50 * 5
    assert tr.commission == pytest.approx(10.0)  # 1.0 * 5 * 2 (round trip)
    assert tr.net_pnl == pytest.approx(240.0)
    assert result.summary["n_trades"] == 1
    assert result.summary["win_rate"] == 1.0
    assert result.summary["net_pnl"] == pytest.approx(240.0)
    # equity curve ends above starting capital
    assert result.equity[-1].equity == pytest.approx(10_240.0)


def test_simulate_concurrency_cap_skips_overlap():
    spec = BacktestSpec.from_dict(
        {
            "underlying": "SPY",
            "start_date": "2026-05-01",
            "end_date": "2026-05-01",
            "fill_model": {"slippage_pct": 0.0, "commission_per_contract": 0.0},
            "sizing": {"capital": 10_000, "risk_per_trade_pct": 5, "max_concurrent": 1},
        }
    )
    # Two trades that overlap in time; max_concurrent=1 → second is skipped.
    a = _candidate(seq_pnl_per_contract=20.0, entry_premium=1.0,
                   entered=T0, exited=T0 + timedelta(minutes=60))
    b = _candidate(seq_pnl_per_contract=20.0, entry_premium=1.0,
                   entered=T0 + timedelta(minutes=10), exited=T0 + timedelta(minutes=70))
    result = _simulate([a, b], spec)
    assert len(result.trades) == 1  # second overlapping trade skipped


def test_simulate_drawdown_is_negative_after_loss():
    spec = BacktestSpec.from_dict(
        {
            "underlying": "SPY",
            "start_date": "2026-05-01",
            "end_date": "2026-05-01",
            "fill_model": {"slippage_pct": 0.0, "commission_per_contract": 0.0},
            "sizing": {"capital": 10_000, "risk_per_trade_pct": 5, "max_concurrent": 5},
        }
    )
    win = _candidate(seq_pnl_per_contract=30.0, entry_premium=1.0,
                     entered=T0, exited=T0 + timedelta(minutes=10))
    loss = _candidate(seq_pnl_per_contract=-40.0, entry_premium=1.0, outcome="stop_hit",
                      entered=T0 + timedelta(minutes=20), exited=T0 + timedelta(minutes=30))
    result = _simulate([win, loss], spec)
    assert result.summary["n_trades"] == 2
    assert result.summary["max_drawdown_pct"] < 0.0
    assert result.summary["profit_factor"] is not None


def test_simulate_empty_is_safe():
    spec = BacktestSpec.from_dict(
        {"underlying": "SPY", "start_date": "2026-05-01", "end_date": "2026-05-01"}
    )
    result = _simulate([], spec)
    assert result.summary["n_trades"] == 0
    assert result.trades == []
    assert result.equity == []


# ----------------------------------------------------------------------
# End-to-end run_backtest against a SQL-routing fake conn
# ----------------------------------------------------------------------


class _FakeCursor:
    """Routes by table name in the SQL to the right canned result set."""

    def __init__(self, store: dict):
        self._store = store
        self._result: list = []

    def execute(self, sql: str, params=None):
        s = " ".join(sql.split())
        if "FROM signal_action_cards" in s:
            self._result = self._store["cards"]
        elif "FROM underlying_quotes" in s:
            self._result = self._store["quotes"]
        elif "FROM option_chains" in s:
            # entry vs exit both hit option_chains; return the single canned
            # leg quote (closest-row LIMIT 1 semantics).
            self._result = [self._store["leg_quote"]]
        else:
            self._result = []

    def fetchall(self):
        return list(self._result)

    def fetchone(self):
        return self._result[0] if self._result else None


class _FakeConn:
    def __init__(self, store: dict):
        self._store = store

    def cursor(self):
        return _FakeCursor(self._store)


def test_run_backtest_end_to_end(monkeypatch):
    ts = datetime(2026, 5, 1, 14, 0, tzinfo=ET)
    payload = {
        "entry": {"ref_price": 500.0, "trigger": "at_market"},
        "target": {"ref_price": 503.0, "kind": "level"},
        "stop": {"ref_price": 498.0, "kind": "level"},
        "max_hold_minutes": 120,
        "legs": [{"expiry": "2026-05-01", "strike": 500, "right": "C", "side": "BUY"}],
    }
    cards = [
        ("SPY", ts, "gamma_flip_break", "BUY_CALL", "0DTE", "bullish", 0.7, json.dumps(payload)),
    ]
    # Underlying climbs through the 503 target so the card resolves target_hit.
    quotes = [
        (ts, 500.0, 500.5, 499.8, 500.2),
        (ts + timedelta(minutes=10), 500.2, 503.4, 500.0, 503.1),
    ]
    # option_chains row: (symbol, strike, expiration, type, bid, ask, last, mid, ts)
    leg_quote = ("SPY 260501C500", 500.0, date(2026, 5, 1), "C", 2.00, 2.10, 2.05, 2.05, ts)
    store = {"cards": cards, "quotes": quotes, "leg_quote": leg_quote}
    conn = _FakeConn(store)

    spec = BacktestSpec.from_dict(
        {
            "underlying": "SPY",
            "start_date": "2026-05-01",
            "end_date": "2026-05-01",
            "fill_model": {"slippage_pct": 0.0, "commission_per_contract": 0.5},
            "sizing": {"capital": 10_000, "risk_per_trade_pct": 10, "max_concurrent": 3},
        }
    )
    progress: list[float] = []
    result = run_backtest(conn, spec, progress_cb=progress.append)

    assert result.summary["n_trades"] == 1
    tr = result.trades[0]
    assert tr.outcome == "target_hit"
    assert tr.option_type == "C"
    # entry filled at ask (2.10), exit sold at bid (2.00) with zero slippage →
    # a small loss per contract before/after commission, but the pipeline ran.
    assert tr.entry_premium == pytest.approx(2.10)
    assert tr.exit_premium == pytest.approx(2.00)
    assert tr.contracts >= 1
    assert progress and progress[-1] == 1.0
    # Diagnostics funnel is populated so a 0-trade run would be explainable.
    diag = result.summary["diagnostics"]
    assert diag["cards_total"] == 1
    assert diag["cards_in_scope"] == 1
    assert diag["priced_candidates"] == 1
    assert diag["drops"] == {}


# ----------------------------------------------------------------------
# Cooldown / dedup
# ----------------------------------------------------------------------


def _bt_spec(**over):
    base = {"underlying": "SPY", "start_date": "2026-05-01", "end_date": "2026-05-01",
            "cooldown_minutes": 0,
            "fill_model": {"slippage_pct": 0.0, "commission_per_contract": 0.0}}
    base.update(over)
    return BacktestSpec.from_dict(base)


def _conn_with(cards, quotes, leg_quote):
    return _FakeConn({"cards": cards, "quotes": quotes, "leg_quote": leg_quote})


def run_backtest_single(conn, spec=None):
    """Load the first card via the real fetch + resolve it through _build_candidate."""
    spec = spec or _bt_spec()
    cards = fetch_action_cards(
        conn, "SPY",
        datetime(2026, 5, 1, tzinfo=ET), datetime(2026, 5, 2, tzinfo=ET),
    )
    return _build_candidate(conn, cards[0], spec)


def _legcard(*, direction="bullish", entry=500.0, target=503.0, stop=498.0,
             trigger="at_market", ts=None):
    ts = ts or datetime(2026, 5, 1, 14, 0, tzinfo=ET)
    payload = {
        "entry": {"ref_price": entry, "trigger": trigger},
        "target": {"ref_price": target, "kind": "level"},
        "stop": {"ref_price": stop, "kind": "level"},
        "max_hold_minutes": 120,
        "legs": [{"expiry": "2026-05-01", "strike": 500, "right": "C", "side": "BUY"}],
    }
    return ("SPY", ts, "gamma_flip_break", "BUY_CALL", "0DTE", direction, 0.7, json.dumps(payload))


def test_build_candidate_min_hold_no_same_bar_roundtrip():
    """A target that prints in the ENTRY bar must NOT book a zero-hold exit.

    Old model resolved target_hit at the entry bar → entered_at == exited_at →
    pure spread loss. The forward walk only looks at bars after the fill bar, so
    a fill-bar-only touch resolves to a held time_exit instead.
    """
    ts = datetime(2026, 5, 1, 14, 0, tzinfo=ET)
    # Entry bar's range spans BOTH entry (500) and target (503); the next bar
    # never reaches the target.
    quotes = [
        (ts, 500.0, 503.5, 499.8, 500.2),                       # fill bar (touches target)
        (ts + timedelta(minutes=5), 500.2, 501.0, 500.0, 500.5),  # later bar: no target
    ]
    leg_quote = ("SPY 260501C500", 500.0, date(2026, 5, 1), "C", 2.00, 2.10, 2.05, 2.05, ts)
    conn = _conn_with([_legcard()], quotes, leg_quote)
    cand, reason = run_backtest_single(conn)
    assert reason == "ok"
    assert cand["outcome"] == "time_exit"          # NOT target_hit on the entry bar
    assert cand["entered_at"] == ts
    assert cand["exited_at"] == ts + timedelta(minutes=5)
    assert cand["hold_minutes"] == 5               # real hold, not 0


def test_build_candidate_target_hit_on_later_bar():
    ts = datetime(2026, 5, 1, 14, 0, tzinfo=ET)
    quotes = [
        (ts, 500.0, 500.5, 499.8, 500.2),                          # fill bar
        (ts + timedelta(minutes=10), 500.2, 503.4, 500.0, 503.1),  # target hit here
    ]
    leg_quote = ("SPY 260501C500", 500.0, date(2026, 5, 1), "C", 2.00, 2.10, 2.05, 2.05, ts)
    cand, reason = run_backtest_single(_conn_with([_legcard()], quotes, leg_quote))
    assert reason == "ok"
    assert cand["outcome"] == "target_hit"
    assert cand["exited_at"] == ts + timedelta(minutes=10)


def test_build_candidate_touch_trigger_fills_at_touch_bar():
    ts = datetime(2026, 5, 1, 14, 0, tzinfo=ET)
    # on_break entry at 500: bar0 never reaches 500; bar1 touches it (fill); bar2
    # hits the target. Entry must be dated to bar1, not the Card timestamp.
    quotes = [
        (ts, 501.0, 501.5, 500.8, 501.2),                          # no fill (low 500.8 > 500)
        (ts + timedelta(minutes=3), 500.9, 501.5, 499.9, 500.4),   # touches 500 → fill here
        (ts + timedelta(minutes=8), 500.4, 503.6, 500.2, 503.3),   # target hit
    ]
    leg_quote = ("SPY 260501C500", 500.0, date(2026, 5, 1), "C", 2.00, 2.10, 2.05, 2.05, ts)
    cand, reason = run_backtest_single(
        _conn_with([_legcard(trigger="on_break")], quotes, leg_quote)
    )
    assert reason == "ok"
    assert cand["entered_at"] == ts + timedelta(minutes=3)   # filled at the touch bar
    assert cand["exited_at"] == ts + timedelta(minutes=8)    # target on a later bar
    assert cand["outcome"] == "target_hit"


def test_build_candidate_no_fill_when_trigger_never_touched():
    ts = datetime(2026, 5, 1, 14, 0, tzinfo=ET)
    quotes = [
        (ts, 501.0, 501.5, 500.8, 501.2),
        (ts + timedelta(minutes=5), 501.0, 502.0, 500.5, 501.5),  # never reaches 500
    ]
    leg_quote = ("SPY 260501C500", 500.0, date(2026, 5, 1), "C", 2.00, 2.10, 2.05, 2.05, ts)
    cand, reason = run_backtest_single(
        _conn_with([_legcard(trigger="on_break", entry=500.0)], quotes, leg_quote)
    )
    assert reason == "outcome:no_fill"
    assert cand is None


def test_apply_cooldown_collapses_rapid_same_pattern_cards():
    cards = [
        _card(pattern="p", ts=T0),
        _card(pattern="p", ts=T0 + timedelta(minutes=5)),    # within 30m → dropped
        _card(pattern="p", ts=T0 + timedelta(minutes=35)),   # >=30m → kept
        _card(pattern="q", ts=T0 + timedelta(minutes=1)),    # different pattern → kept
    ]
    kept = _apply_cooldown(cards, cooldown_minutes=30)
    times = sorted((c.pattern, c.timestamp) for c in kept)
    assert times == [
        ("p", T0),
        ("p", T0 + timedelta(minutes=35)),
        ("q", T0 + timedelta(minutes=1)),
    ]


def test_apply_cooldown_zero_is_passthrough():
    cards = [_card(pattern="p", ts=T0), _card(pattern="p", ts=T0 + timedelta(minutes=1))]
    assert len(_apply_cooldown(cards, cooldown_minutes=0)) == 2


def test_run_backtest_diagnostics_explains_missing_quote(monkeypatch):
    # Card resolves (target_hit) and synthesizes an ATM leg, but option_chains
    # has no matching row → dropped as "no_entry_quote", surfaced in diagnostics.
    ts = datetime(2026, 5, 1, 14, 0, tzinfo=ET)
    payload = {
        "entry": {"ref_price": 500.0, "trigger": "at_market"},
        "target": {"ref_price": 503.0, "kind": "level"},
        "stop": {"ref_price": 498.0, "kind": "level"},
        "max_hold_minutes": 120,
        # no legs
    }
    cards = [("SPY", ts, "p", "BUY_CALL", "0DTE", "bullish", 0.7, json.dumps(payload))]
    quotes = [
        (ts, 500.0, 500.5, 499.8, 500.2),
        (ts + timedelta(minutes=10), 500.2, 503.4, 500.0, 503.1),
    ]
    # No option_chains row → entry-quote lookup fails → "no_entry_quote".
    store = {"cards": cards, "quotes": quotes, "leg_quote": None}

    class _NoLegCursor(_FakeCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "FROM option_chains" in " ".join(sql.split()) and "to_regclass" not in sql:
                self._result = []  # no quote found

    class _NoLegConn(_FakeConn):
        def cursor(self):
            return _NoLegCursor(self._store)

    spec = BacktestSpec.from_dict(
        {"underlying": "SPY", "start_date": "2026-05-01", "end_date": "2026-05-01",
         "cooldown_minutes": 0}
    )
    result = run_backtest(_NoLegConn(store), spec)
    assert result.summary["n_trades"] == 0
    diag = result.summary["diagnostics"]
    assert diag["cards_in_scope"] == 1
    assert diag["priced_candidates"] == 0
    assert diag["drops"].get("no_entry_quote") == 1
