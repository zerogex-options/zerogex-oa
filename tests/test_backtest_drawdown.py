"""Tests for the mark-to-market equity curve / drawdown.

The realized-only curve stepped equity only at closes, so a position sitting in
a deep unrealized loss never showed up in the drawdown — it was understated in
the customer's favor. These tests pin the fix: open-position dips now count.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.backtesting.engine import (
    _downsample_equity,
    _mtm_equity_curve,
    _simulate,
)
from src.backtesting.models import BacktestSpec, EquityPoint
from src.signals.playbook.backtest import CardRow

UTC = timezone.utc
T0 = datetime(2026, 5, 1, 14, 0, tzinfo=UTC)


def _spec(**sizing):
    base = {"capital": 10_000, "risk_per_trade_pct": 10, "max_concurrent": 5}
    base.update(sizing)
    return BacktestSpec.from_dict(
        {
            "underlying": "SPY",
            "start_date": "2026-05-01",
            "end_date": "2026-05-01",
            "fill_model": {"slippage_pct": 0.0, "commission_per_contract": 0.0},
            "sizing": base,
        }
    )


def _cand(*, pnl_per_contract, entry_premium, entered, exited, marks=None, max_loss=None, n_legs=1):
    """A priced candidate as _build_candidate would emit it (marks optional)."""
    return {
        "card": CardRow(
            underlying="SPY",
            timestamp=entered,
            pattern="gamma_flip_break",
            action="BUY_CALL",
            tier="0DTE",
            direction="bullish",
            confidence=0.7,
            payload={},
        ),
        "outcome": "time_exit",
        "entered_at": entered,
        "exited_at": exited,
        "marks": marks,
        "structure": "single",
        "n_legs": n_legs,
        "legs": [],
        "option_symbol": "SPY_C",
        "option_type": "C",
        "strike": 500.0,
        "expiration": None,
        "entry_premium": entry_premium,
        "exit_premium": entry_premium + pnl_per_contract / 100.0,
        "max_loss_per_share": max_loss if max_loss is not None else entry_premium,
        "delta_per_contract": 0.0,
        "vega_per_contract": 0.0,
        "pnl_per_contract": pnl_per_contract,
        "hold_minutes": int((exited - entered).total_seconds() // 60),
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
    }


def test_mtm_drawdown_captures_open_position_dip():
    """A trade that WINS overall but dips deep mid-hold must show that drawdown.

    Realized-only stepping would report ~0 drawdown for a winner. The MtM walk
    prices the −$1.50/share mid-hold trough (× 100 × 5 contracts = −$750 on a
    $10k account ≈ −7.5%).
    """
    marks = [
        (T0, 0.0),
        (T0 + timedelta(minutes=10), -1.50),  # deep unrealized loss mid-hold
        (T0 + timedelta(minutes=20), 0.50),  # recovers to a winner
    ]
    cand = _cand(
        pnl_per_contract=50.0,
        entry_premium=2.0,
        entered=T0,
        exited=T0 + timedelta(minutes=20),
        marks=marks,
        max_loss=2.0,
    )
    result = _simulate([cand], _spec())
    tr = result.trades[0]
    assert tr.contracts == 5
    assert tr.net_pnl > 0  # a winner overall …
    assert result.summary["max_drawdown_pct"] < -5.0  # … that still drew down
    assert result.equity[-1].equity == 10_250.0  # ends at realized 10k + 5×$50


def test_mtm_final_equity_matches_realized():
    """Whatever the intra-hold path, the curve must END at realized equity."""
    marks = [(T0, 0.0), (T0 + timedelta(minutes=5), 3.0), (T0 + timedelta(minutes=10), 1.0)]
    cand = _cand(
        pnl_per_contract=100.0,
        entry_premium=1.0,
        entered=T0,
        exited=T0 + timedelta(minutes=10),
        marks=marks,
        max_loss=1.0,
    )
    result = _simulate([cand], _spec())
    # contracts = floor(1000/100) = 10; net = 100 × 10 = 1000.
    assert result.trades[0].net_pnl == 1000.0
    assert result.equity[-1].equity == 11_000.0


def test_mtm_curve_direct_two_overlapping_positions():
    a = {
        "entered_at": T0,
        "exited_at": T0 + timedelta(minutes=30),
        "net": 100.0,
        "marks": [
            (T0, 0.0),
            (T0 + timedelta(minutes=15), -200.0),
            (T0 + timedelta(minutes=30), 100.0),
        ],
    }
    b = {
        "entered_at": T0 + timedelta(minutes=10),
        "exited_at": T0 + timedelta(minutes=20),
        "net": -50.0,
        "marks": [(T0 + timedelta(minutes=10), 0.0), (T0 + timedelta(minutes=20), -50.0)],
    }
    curve = _mtm_equity_curve([a, b], 10_000.0)
    # Deepest point is t=20: b has just realized −50 AND a's step-mark is still
    # −200 (it holds until its t=30 mark) → 10_000 − 50 − 200 = 9_750.
    trough = min(p.equity for p in curve)
    assert trough == 9_750.0
    assert curve[-1].equity == 10_050.0  # realized 100 − 50
    assert min(p.drawdown_pct for p in curve) < 0.0


def test_downsample_keeps_endpoints_and_trough():
    pts = [
        EquityPoint(
            t=T0 + timedelta(minutes=i),
            equity=100.0 - (i == 500) * 90,
            drawdown_pct=-(i == 500) * 90.0,
        )
        for i in range(1000)
    ]
    out = _downsample_equity(pts, 100)
    assert len(out) <= 103  # ~cap + first/last/trough
    assert out[0] is pts[0] and out[-1] is pts[-1]
    assert min(p.drawdown_pct for p in out) == -90.0  # trough preserved


def test_empty_curve():
    assert _mtm_equity_curve([], 10_000.0) == []
