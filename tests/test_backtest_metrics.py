"""Tests for the risk-adjusted tearsheet metrics added to backtest summaries.

Covers the pure statistics helpers in ``src/backtesting/engine`` (Sharpe /
Sortino / Calmar / CAGR / exposure / streaks / expectancy) and their merge into
``_summarize``. All are computed from realized trades + equity curve + window
with no DB access, so they unit-test directly.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from src.backtesting.engine import (
    _daily_realized_equity,
    _exposure_pct,
    _monte_carlo,
    _percentile,
    _streaks,
    _summarize,
)
from src.backtesting.models import EquityPoint, TradeResult

UTC = timezone.utc


def mk_trade(
    seq: int,
    net_pnl: float,
    entered: datetime,
    exited: datetime,
    *,
    return_pct: float | None = None,
    commission: float = 1.30,
) -> TradeResult:
    """A minimal TradeResult sufficient for the summary/metrics math."""
    return TradeResult(
        seq=seq,
        pattern="gamma_flip_break",
        direction="bullish",
        tier="0DTE",
        option_symbol="SPY   260501C00600000",
        option_type="C",
        strike=600.0,
        expiration=exited.date(),
        entered_at=entered,
        exited_at=exited,
        entry_premium=1.0,
        exit_premium=1.0 + net_pnl / 100.0,
        contracts=1,
        gross_pnl=net_pnl + commission,
        commission=commission,
        net_pnl=net_pnl,
        return_pct=return_pct,
        outcome="target_hit" if net_pnl > 0 else "stop_hit",
        mfe_pct=0.0,
        mae_pct=0.0,
        hold_minutes=30,
    )


def _equity_from_trades(trades, capital):
    """A realized equity curve stepped at each exit (mirrors _simulate)."""
    eq = []
    running = capital
    peak = capital
    for t in sorted(trades, key=lambda x: x.exited_at):
        running += t.net_pnl
        peak = max(peak, running)
        dd = 0.0 if peak <= 0 else (running - peak) / peak * 100.0
        eq.append(EquityPoint(t=t.exited_at, equity=running, drawdown_pct=dd))
    return eq


# ----------------------------------------------------------------------
# Helper-level tests
# ----------------------------------------------------------------------


def test_streaks_counts_longest_runs():
    base = datetime(2026, 5, 1, 14, 0, tzinfo=UTC)
    outcomes = [1, 1, 1, -1, -1, 1, -1, -1, -1, -1]  # 3 wins then 4-loss tail
    trades = [
        mk_trade(i, float(v), base + timedelta(minutes=i), base + timedelta(minutes=i, seconds=30))
        for i, v in enumerate(outcomes)
    ]
    wins, losses = _streaks(trades)
    assert wins == 3
    assert losses == 4


def test_daily_realized_equity_steps_by_weekday():
    # Two trades exiting on Fri 5/1 and Mon 5/4 (skips the weekend).
    fri = datetime(2026, 5, 1, 15, 0, tzinfo=UTC)
    mon = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)
    trades = [
        mk_trade(1, 100.0, fri - timedelta(minutes=30), fri),
        mk_trade(2, -40.0, mon - timedelta(minutes=30), mon),
    ]
    series = _daily_realized_equity(trades, 10_000.0, date(2026, 5, 1), date(2026, 5, 4))
    # Fri, Mon only — Sat/Sun are not recorded.
    assert [d.isoformat() for d, _ in series] == ["2026-05-01", "2026-05-04"]
    assert series[0][1] == 10_100.0
    assert series[1][1] == 10_060.0


def test_exposure_pct_merges_overlaps():
    base = datetime(2026, 5, 1, 14, 0, tzinfo=UTC)
    # Two fully-overlapping 60-min holds on one weekday → 60 of 390 RTH min.
    trades = [
        mk_trade(1, 10.0, base, base + timedelta(minutes=60)),
        mk_trade(2, 10.0, base + timedelta(minutes=10), base + timedelta(minutes=50)),
    ]
    exp = _exposure_pct(trades, date(2026, 5, 1), date(2026, 5, 1))
    assert exp == round(60 / 390 * 100, 1)


# ----------------------------------------------------------------------
# Summary integration
# ----------------------------------------------------------------------


def test_summary_includes_risk_block_and_is_backward_compatible():
    base = datetime(2026, 5, 1, 14, 0, tzinfo=UTC)
    trades = [
        mk_trade(1, 200.0, base, base + timedelta(minutes=30), return_pct=20.0),
        mk_trade(
            2,
            -100.0,
            base + timedelta(days=1),
            base + timedelta(days=1, minutes=30),
            return_pct=-10.0,
        ),
        mk_trade(
            3,
            150.0,
            base + timedelta(days=2),
            base + timedelta(days=2, minutes=30),
            return_pct=15.0,
        ),
    ]
    equity = _equity_from_trades(trades, 10_000.0)
    summary = _summarize(trades, equity, 10_000.0, date(2026, 5, 1), date(2026, 5, 4))

    # Legacy keys still present and correct.
    assert summary["n_trades"] == 3
    assert summary["win_rate"] == 2 / 3
    assert summary["net_pnl"] == 250.0
    assert summary["profit_factor"] == round(350 / 100, 3)

    # New risk keys present.
    for k in (
        "sharpe",
        "sortino",
        "calmar",
        "cagr_pct",
        "annual_volatility_pct",
        "expectancy",
        "expectancy_pct",
        "avg_win",
        "avg_loss",
        "payoff_ratio",
        "largest_win",
        "largest_loss",
        "max_consecutive_wins",
        "max_consecutive_losses",
        "exposure_pct",
        "total_commission",
        "trading_days",
        "daily_equity",
    ):
        assert k in summary, f"missing metric {k}"

    assert summary["expectancy"] == round(250 / 3, 2)
    assert summary["avg_win"] == 175.0  # (200 + 150) / 2
    assert summary["avg_loss"] == -100.0
    assert summary["payoff_ratio"] == 1.75  # 175 / 100
    assert summary["largest_win"] == 200.0
    assert summary["largest_loss"] == -100.0
    assert summary["max_consecutive_wins"] == 1  # W, L, W
    assert summary["max_consecutive_losses"] == 1
    assert summary["total_commission"] == round(3 * 1.30, 2)
    assert summary["expectancy_pct"] == round((20 - 10 + 15) / 3, 2)


def test_empty_summary_has_risk_keys_as_none():
    summary = _summarize([], [], 25_000.0, date(2026, 5, 1), date(2026, 5, 10))
    assert summary["n_trades"] == 0
    assert summary["sharpe"] is None
    assert summary["cagr_pct"] is None
    assert summary["exposure_pct"] is None
    assert summary["max_consecutive_losses"] == 0
    assert summary["total_commission"] == 0.0
    assert summary["daily_equity"] == []


def test_sharpe_positive_for_steady_gains():
    # A steadily-rising account should post a positive, finite Sharpe/Sortino.
    base = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)  # a Monday
    trades = []
    for i in range(8):
        d = base + timedelta(days=i)
        trades.append(
            mk_trade(i + 1, 50.0 + (i % 2) * 20, d - timedelta(minutes=20), d, return_pct=5.0)
        )
    equity = _equity_from_trades(trades, 10_000.0)
    summary = _summarize(trades, equity, 10_000.0, date(2026, 5, 4), date(2026, 5, 15))
    assert summary["sharpe"] is not None and summary["sharpe"] > 0
    assert (
        summary["sortino"] is None or summary["sortino"] > 0
    )  # no losing days ⇒ Sortino undefined
    assert summary["cagr_pct"] is not None and summary["cagr_pct"] > 0
    assert summary["max_drawdown_pct"] == 0.0  # monotonic ⇒ calmar undefined
    assert summary["calmar"] is None


# ----------------------------------------------------------------------
# Monte Carlo
# ----------------------------------------------------------------------


def _mc_trades(pnls):
    base = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)
    return [
        mk_trade(
            i + 1, float(p), base + timedelta(minutes=i), base + timedelta(minutes=i, seconds=30)
        )
        for i, p in enumerate(pnls)
    ]


def test_percentile_interpolates():
    xs = [0.0, 10.0, 20.0, 30.0, 40.0]
    assert _percentile(xs, 0.0) == 0.0
    assert _percentile(xs, 1.0) == 40.0
    assert _percentile(xs, 0.5) == 20.0
    assert _percentile([5.0], 0.9) == 5.0


def test_monte_carlo_none_below_min_trades():
    assert _monte_carlo(_mc_trades([10.0] * 5), 10_000.0) is None


def test_monte_carlo_all_winners_never_ruins():
    mc = _monte_carlo(_mc_trades([100.0] * 20), 10_000.0)
    assert mc is not None
    assert mc["iterations"] == 1000
    assert mc["prob_profit"] == 1.0
    assert mc["risk_of_ruin_50pct"] == 0.0
    assert mc["terminal_return_pct"]["p5"] > 0
    # Cone ends above start and is ordered p5 ≤ p50 ≤ p95.
    last = mc["cone"][-1]
    assert last["p5"] <= last["p50"] <= last["p95"]


def test_monte_carlo_all_losers_always_profitless():
    mc = _monte_carlo(_mc_trades([-100.0] * 20), 10_000.0)
    assert mc["prob_profit"] == 0.0
    assert mc["terminal_return_pct"]["p95"] < 0


def test_monte_carlo_is_deterministic():
    trades = _mc_trades([50.0, -30.0, 80.0, -20.0, 40.0, -60.0, 20.0, 10.0, -15.0, 25.0])
    a = _monte_carlo(trades, 10_000.0)
    b = _monte_carlo(trades, 10_000.0)
    assert a == b  # fixed seed ⇒ identical cone + stats


def test_summary_includes_monte_carlo_block():
    summary = _summarize(
        _mc_trades([50.0, -30.0, 80.0, -20.0, 40.0, -60.0, 20.0, 10.0, -15.0, 25.0]),
        [],
        10_000.0,
        date(2026, 5, 4),
        date(2026, 5, 20),
    )
    assert summary["monte_carlo"] is not None
    assert "cone" in summary["monte_carlo"]
    assert summary["expectancy_tstat"] is not None
