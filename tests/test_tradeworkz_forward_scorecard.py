"""The tweak loop: every entry records its tuning; the scorecard reads it.

"Run them and keep tweaking until we find an edge" needs two things the
fleet lacked: each trade must say which tuning it ran under, so a tweak's
record never borrows from the last one, and something must score each bot —
by symbol, structure and tuning — against the bar that calls an edge.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.strategies import BOT_HARNESSES, PROMOTION_MIN_TRADES
from src.tradeworkz import engine
from src.tradeworkz.engine import params_version
from src.tradeworkz.forward_test import (
    TradeRow,
    build_report,
    format_report,
    load_trades,
    main,
    score,
    verdict,
)

T0 = datetime(2026, 11, 13, 15, 0, tzinfo=timezone.utc)


def _row(
    bot="charm_close_magnet", u="SPY", stype="BULL_CALL_DEBIT_SPREAD", pv="aaaa1111", pnl=10.0
):
    return TradeRow(bot, u, stype, pv, pnl, T0, T0 + timedelta(minutes=30))


# ----------------------------------------------------------------------
# params_version
# ----------------------------------------------------------------------


def test_params_version_ignores_key_order_and_changes_on_any_tweak():
    a = params_version({"dte_target": 0, "max_hold_minutes": 90, "nested": {"x": 1, "y": 2}})
    b = params_version({"nested": {"y": 2, "x": 1}, "max_hold_minutes": 90, "dte_target": 0})
    assert a == b and len(a) == 8
    assert (
        params_version({"dte_target": 0, "max_hold_minutes": 91, "nested": {"x": 1, "y": 2}}) != a
    )
    assert params_version(None) == params_version({})


@dataclass
class _NoML:
    pass


def test_the_engine_stamps_the_tuning_on_every_entry(monkeypatch):
    from src.tradeworkz.models import TradeSignal

    sig = TradeSignal(
        bot_id="gamma_flip_breaker",
        underlying="SPY",
        direction="bullish",
        strategy_type="LONG_CALL",
        legs=[],
        entry_price=0.0,
        conviction=0.7,
        components_at_entry={"net_gex": 1.0},
    )

    class _Bot:
        def __init__(self, spec, ml_state=None):
            self.spec = spec

        def size_multiplier(self):
            return 1.0

        def scale_config(self):
            return {}

        def evaluate_open(self, snap):
            return sig

        def _bias_veto(self, snap, direction):
            return False

    class _NullConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    seen = []
    monkeypatch.setattr(engine, "db_connection", lambda: _NullConn())
    monkeypatch.setattr(engine, "load_capital", lambda conn, bid: object())
    monkeypatch.setattr(engine.ml_mod, "load_state", lambda conn, bid: _NoML())
    monkeypatch.setattr(engine, "get_bot_class", lambda cls: _Bot)
    monkeypatch.setattr(engine, "load_open_positions", lambda conn, bid: [])
    monkeypatch.setattr(engine, "daily_realized_pnl", lambda conn, bid: 0.0)
    monkeypatch.setattr(
        engine, "open_position", lambda conn, signal, *a, **k: seen.append(signal) or 7
    )
    params = {"dte_target": 0, "structure_override": "credit_vertical"}
    row = ("gamma_flip_breaker", "G", "X", "0DTE", "context", "SPY", "t", "d", True, True, params)
    snap = SimpleNamespace(spot=600.0)

    stats = engine._run_bot(row, {"SPY": snap}, ("SPY",), opens_allowed=True)

    assert stats["opened"] == 1
    assert seen[0].components_at_entry == {
        "net_gex": 1.0,
        "params_version": params_version(params),
    }


def test_live_forward_trades_count_as_bot_evidence():
    assert "tradeworkz-live" in BOT_HARNESSES


# ----------------------------------------------------------------------
# Verdicts and scores
# ----------------------------------------------------------------------


def test_verdict_uses_the_catalog_promotion_bar():
    n = PROMOTION_MIN_TRADES
    assert verdict(0, None, 0.0) == "no trades yet"
    assert verdict(n, 1.2, 5.0) == "EDGE"
    assert verdict(n, None, 5.0) == "EDGE"  # never lost
    assert verdict(n - 1, 3.0, 5.0) == "promising"
    assert verdict(n, 1.05, 1.0) == "no edge yet"  # below PF 1.1
    assert verdict(n, 0.8, -4.0) == "losing"
    assert verdict(5, 0.5, -2.0) == "no edge yet"


def test_score_math():
    s = score([_row(pnl=30), _row(pnl=-10), _row(pnl=-10), _row(pnl=0)])
    assert s.trades == 4
    assert s.wins == 1
    assert s.win_rate == pytest.approx(0.25)
    assert s.profit_factor == pytest.approx(1.5)
    assert s.expectancy == pytest.approx(2.5)
    assert s.total_pnl == pytest.approx(10)
    assert s.avg_hold_minutes == pytest.approx(30)
    assert score([]).verdict == "no trades yet"


def test_report_splits_by_symbol_structure_and_tuning_and_flags_the_current_one():
    old = [_row(pv="old00000", pnl=-20) for _ in range(10)]
    new = [
        _row(pv="new11111", u="QQQ", pnl=15 if i % 3 else -10) for i in range(PROMOTION_MIN_TRADES)
    ]
    report = build_report(
        old + new, ["charm_close_magnet", "climax_flow_fade"], {"charm_close_magnet": "new11111"}
    )
    bot = next(b for b in report["bots"] if b["bot_id"] == "charm_close_magnet")
    assert set(bot["by_symbol"]) == {"SPY", "QQQ"}
    assert set(bot["by_structure"]) == {"BULL_CALL_DEBIT_SPREAD"}
    assert bot["by_tuning"]["new11111"]["current"] is True
    assert bot["by_tuning"]["old00000"]["verdict"] == "no edge yet"
    # The new tuning clears the bar on its own trades, whatever the old one did.
    assert bot["by_tuning"]["new11111"]["verdict"] == "EDGE"
    assert bot["current_tuning_clears_bar"] is True
    idle = next(b for b in report["bots"] if b["bot_id"] == "climax_flow_fade")
    assert idle["overall"]["verdict"] == "no trades yet"

    text = format_report(report)
    assert "tuning    new11111 (current)" in text
    assert "record ResearchRun(harness='tradeworkz-live'" in text
    assert "symbol    QQQ" in text


def test_load_trades_reads_live_rows_only(monkeypatch):
    class _Cur:
        def execute(self, sql, params=None):
            self.sql, self.params = " ".join(sql.split()), params

        def fetchall(self):
            return [("charm_close_magnet", "SPY", "LONG_CALL", 12.5, T0, T0, None)]

    class _Conn:
        cur = _Cur()

        def cursor(self):
            return self.cur

    conn = _Conn()
    rows = load_trades(conn, ["charm_close_magnet"], date(2026, 11, 13))
    assert "IS DISTINCT FROM 'simulate'" in conn.cur.sql
    assert conn.cur.params[0] == ["charm_close_magnet"]
    assert conn.cur.params[1] == datetime(2026, 11, 13, tzinfo=timezone.utc)
    assert rows[0].params_version == "untagged"
    assert rows[0].pnl == 12.5


def test_main_rejects_an_unknown_bot(capsys):
    assert main(["--bots", "not_a_bot"]) == 2
    assert "unknown bot id" in capsys.readouterr().err
