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


def test_aggregate_trades_tracks_gross_pnl_economics():
    # gross_win is sum of positive net_pnl; gross_loss is the ABS sum of the
    # non-positive ones (PF/expectancy on the read side rely on both being ≥0).
    trades = [
        _Trade("p", 80.0),
        _Trade("p", 120.0),
        _Trade("p", -50.0),
        _Trade("p", -150.0),
        _Trade("p", 0.0),  # zero counts as a loss with 0 contribution
    ]
    stats = feed.aggregate_trades(
        trades, underlying="SPY", window_start=_WS, window_end=_WE,
    )
    p = next(s for s in stats if s.pattern == "p")
    assert p.gross_win_pnl == pytest.approx(200.0)
    assert p.gross_loss_pnl == pytest.approx(200.0)
    assert p.n_trades == 5 and p.n_wins == 2 and p.n_losses == 3


def test_aggregate_trades_winners_only_pattern():
    trades = [_Trade("hot", 10.0), _Trade("hot", 20.0)]
    p = feed.aggregate_trades(
        trades, underlying="SPY", window_start=_WS, window_end=_WE,
    )[0]
    assert p.gross_win_pnl == pytest.approx(30.0)
    assert p.gross_loss_pnl == pytest.approx(0.0)


def test_upsert_includes_gross_pnl_columns():
    conn = _Conn()
    s = feed.PnlPatternStat(
        pattern="p", underlying="SPY", window_start=_WS, window_end=_WE,
        n_trades=10, n_wins=4, n_losses=6,
        gross_win_pnl=400.0, gross_loss_pnl=180.0,
    )
    feed.upsert_pnl_stats(conn, [s])
    sql, params = conn._cur.executed[0]
    # Columns appear in the INSERT clause...
    flat = " ".join(sql.split())
    assert "gross_win_pnl" in flat and "gross_loss_pnl" in flat
    # ...and the ON CONFLICT branch updates them so a re-run of the same
    # window refreshes the economics rather than silently keeping stale ones.
    assert "gross_win_pnl  = EXCLUDED.gross_win_pnl" in flat or \
           "gross_win_pnl = EXCLUDED.gross_win_pnl" in flat
    assert "gross_loss_pnl = EXCLUDED.gross_loss_pnl" in flat
    # And the values are bound in the params tuple.
    assert 400.0 in params and 180.0 in params


def test_calibration_spec_is_single_leg_permissive():
    spec = feed.calibration_spec("SPY", _WS, _WE)
    assert spec.underlying == "SPY"
    assert spec.strategy is None           # single-leg playbook cards
    assert spec.patterns == []             # all patterns
    assert spec.sizing.max_concurrent == 20
    assert spec.sizing.capital >= 1_000_000.0
    # Standardized premium stop + take-profit overlay the card's own levels.
    assert spec.exit.stop_loss_pct == pytest.approx(0.50)
    assert spec.exit.profit_target_pct == pytest.approx(0.75)


def test_calibration_spec_exits_from_config(monkeypatch):
    from src import config

    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_PNL_STOP_PCT", 0.35)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_PNL_TARGET_PCT", 1.20)
    spec = feed.calibration_spec("SPY", _WS, _WE)
    assert spec.exit.stop_loss_pct == pytest.approx(0.35)
    assert spec.exit.profit_target_pct == pytest.approx(1.20)
    # 0 on either side disables it.
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_PNL_STOP_PCT", 0.0)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_PNL_TARGET_PCT", 0.0)
    spec = feed.calibration_spec("SPY", _WS, _WE)
    assert spec.exit.stop_loss_pct is None
    assert spec.exit.profit_target_pct is None


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


def test_to_vertical_scales_with_price():
    from src.backtesting.engine import _to_vertical

    # Bullish ATM call: short leg one width above, scaled off the entry price.
    legs = [{"expiry": "2026-03-02", "strike": 600, "right": "C", "side": "long", "qty": 1}]
    out = _to_vertical(legs, "bullish", 600.0, 0.01)
    assert [leg_["side"] for leg_ in out] == ["long", "short"]
    assert out[0]["strike"] == 600.0 and out[1]["strike"] == 606.0
    assert out[1]["right"] == "C"  # same right as the long

    # Bearish, synthetic (None strike) → anchored at round(entry); short below.
    spx = _to_vertical(
        [{"expiry": None, "strike": None, "right": "P", "side": "long", "qty": 1}],
        "bearish", 7000.0, 0.01,
    )
    assert spx[0]["strike"] == 7000.0 and spx[1]["strike"] == 6930.0


def test_spec_structure_validation():
    from src.backtesting.models import BacktestSpec, SpecError

    base = {"underlying": "SPY", "start_date": "2026-03-01", "end_date": "2026-06-01"}
    assert BacktestSpec.from_dict(base).structure == "single"        # default
    v = BacktestSpec.from_dict({**base, "structure": "vertical", "width_pct": 0.02})
    assert v.structure == "vertical" and v.width_pct == pytest.approx(0.02)
    assert v.to_dict()["structure"] == "vertical"                    # round-trips
    with pytest.raises(SpecError, match="structure must be"):
        BacktestSpec.from_dict({**base, "structure": "condor"})


def test_calibration_spec_structure_param():
    spec = feed.calibration_spec("SPY", _WS, _WE, structure="vertical", width_pct=0.015)
    assert spec.structure == "vertical"
    assert spec.width_pct == pytest.approx(0.015)


def test_aggregate_economics():
    trades = [
        _Trade("a", 100.0), _Trade("a", -50.0), _Trade("a", -25.0),  # 1 win, gw100 gl75
        _Trade("b", 200.0),
    ]
    econ = feed.aggregate_economics(trades)
    assert econ["a"]["n"] == 3 and econ["a"]["wins"] == 1
    assert econ["a"]["win_rate"] == pytest.approx(1 / 3)
    assert econ["a"]["pf"] == pytest.approx(100 / 75)
    assert econ["a"]["expectancy"] == pytest.approx((100 - 50 - 25) / 3)
    assert econ["b"]["pf"] == float("inf")  # no losses


def test_run_structures_runs_each_structure(monkeypatch):
    # Return different trades per structure so we can tell them apart.
    def fake_run_backtest(conn, spec, **kw):
        if spec.structure == "vertical":
            return type("R", (), {"trades": [_Trade("p", 50.0), _Trade("p", -10.0)]})()
        return type("R", (), {"trades": [_Trade("p", -100.0)]})()

    monkeypatch.setattr(feed, "run_backtest", fake_run_backtest)
    out = feed.run_structures(_Conn(), underlying="SPY", days=30)
    assert set(out) == {"single", "vertical"}
    assert out["single"]["p"]["win_rate"] == 0.0          # the lone trade lost
    assert out["vertical"]["p"]["win_rate"] == pytest.approx(0.5)


def test_run_no_write_skips_persist(monkeypatch):
    conn = _Conn()

    class _Result:
        trades = [_Trade("a", 100.0)]

    monkeypatch.setattr(feed, "run_backtest", lambda c, spec, **kw: _Result())
    feed.run(underlying="SPY", days=30, conn=conn, write=False)
    assert conn._cur.executed == []
    assert conn.committed is False
