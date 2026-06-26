"""Tests for the realized option-P&L calibration feed (the bridge).

Covers the beta-smoothed win-rate stat, trade aggregation, the option_pnl
upsert (SQL shape + source tag), and the end-to-end run() against a
monkeypatched engine + scripted connection — no database required.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.backtesting import calibration_feed as feed
from src.signals.playbook.backtest import _PRIOR_ALPHA, _PRIOR_BETA

_WS, _WE = date(2026, 4, 1), date(2026, 6, 1)


class _Trade:
    """Minimal stand-in for engine TradeResult (only fields the feed reads)."""

    def __init__(self, pattern, net_pnl):
        self.pattern = pattern
        self.net_pnl = net_pnl


def _stat(pattern, n_trades, n_wins):
    return feed.PnlPatternStat(
        pattern=pattern, underlying="SPY", window_start=_WS, window_end=_WE,
        n_trades=n_trades, n_wins=n_wins, n_losses=n_trades - n_wins,
    )


def test_proposed_base_beta_smoothing():
    s = _stat("p", n_trades=10, n_wins=6)
    expected = (6 + _PRIOR_ALPHA) / (10 + _PRIOR_ALPHA + _PRIOR_BETA)
    assert s.proposed_base == pytest.approx(expected)
    assert s.hit_rate == pytest.approx(0.6)


def test_proposed_base_none_when_no_trades():
    assert _stat("p", 0, 0).proposed_base is None
    assert _stat("p", 0, 0).hit_rate is None


def test_losing_pattern_recorded_below_floor_unclamped():
    # 2/20 winners ⇒ ~0.13 smoothed; must NOT be clamped up to the 0.40 band.
    s = _stat("loser", n_trades=20, n_wins=2)
    assert s.proposed_base < 0.40


def test_aggregate_trades_counts_wins_and_losses():
    trades = [
        _Trade("a", 100.0), _Trade("a", -50.0), _Trade("a", 25.0),
        _Trade("b", -10.0), _Trade("b", 0.0),  # 0 is not > 0 ⇒ a loss
    ]
    stats = {s.pattern: s for s in feed.aggregate_trades(
        trades, underlying="SPY", window_start=_WS, window_end=_WE)}
    assert (stats["a"].n_trades, stats["a"].n_wins, stats["a"].n_losses) == (3, 2, 1)
    assert (stats["b"].n_trades, stats["b"].n_wins, stats["b"].n_losses) == (2, 0, 2)


def test_calibration_spec_is_single_leg_permissive():
    spec = feed.calibration_spec("SPY", _WS, _WE)
    assert spec.underlying == "SPY"
    assert spec.strategy is None           # single-leg playbook cards
    assert spec.patterns == []             # all patterns
    assert spec.sizing.max_concurrent == 20
    assert spec.sizing.capital >= 1_000_000.0
    # Standardized premium stop applied; card targets kept (no premium target).
    assert spec.exit.stop_loss_pct == pytest.approx(0.50)
    assert spec.exit.profit_target_pct is None


def test_calibration_spec_stop_from_config(monkeypatch):
    from src import config

    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_PNL_STOP_PCT", 0.35)
    assert feed.calibration_spec("SPY", _WS, _WE).exit.stop_loss_pct == pytest.approx(0.35)
    # 0 disables the stop entirely.
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_PNL_STOP_PCT", 0.0)
    assert feed.calibration_spec("SPY", _WS, _WE).exit.stop_loss_pct is None


# ---- upsert + run() against a scripted connection ------------------------


class _Cur:
    def __init__(self):
        self.executed: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


class _Conn:
    def __init__(self):
        self._cur = _Cur()
        self.committed = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True


def test_upsert_tags_source_option_pnl():
    conn = _Conn()
    feed.upsert_pnl_stats(conn, [_stat("p", 10, 6)])
    sql, params = conn._cur.executed[0]
    assert "INSERT INTO playbook_pattern_stats" in sql
    conflict = "ON CONFLICT (pattern, underlying, window_start, window_end, source)"
    assert conflict in " ".join(sql.split())
    assert params[-1] == "option_pnl"          # source is the last bound value
    assert conn.committed is True


def test_run_aggregates_and_writes(monkeypatch):
    conn = _Conn()

    class _Result:
        trades = [_Trade("a", 100.0), _Trade("a", -20.0), _Trade("b", 5.0)]

    captured = {}

    def fake_run_backtest(c, spec, **kw):
        captured["spec"] = spec
        return _Result()

    monkeypatch.setattr(feed, "run_backtest", fake_run_backtest)

    stats = feed.run(underlying="SPY", days=30, conn=conn, write=True)
    by = {s.pattern: s for s in stats}
    assert by["a"].n_trades == 2 and by["a"].n_wins == 1
    assert by["b"].n_wins == 1
    # It ran the standardized single-leg spec and persisted with the source tag.
    assert captured["spec"].sizing.max_concurrent == 20
    assert conn.committed is True
    assert any(p[-1] == "option_pnl" for _, p in conn._cur.executed if p)


def test_run_no_write_skips_persist(monkeypatch):
    conn = _Conn()

    class _Result:
        trades = [_Trade("a", 100.0)]

    monkeypatch.setattr(feed, "run_backtest", lambda c, spec, **kw: _Result())
    feed.run(underlying="SPY", days=30, conn=conn, write=False)
    assert conn._cur.executed == []
    assert conn.committed is False
