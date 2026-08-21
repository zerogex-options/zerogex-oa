"""The calibration path is now actually wired up (and self-governs).

Root cause of "the fleet bleeds at full size and never self-corrects":
``recalibrate_bot`` was dead code — never called — so profit_factor /
size_multiplier / the adaptive threshold were never computed in prod. These
tests lock the fix:

* ``recalibrate_bot`` computes the rolling aggregates AND applies the
  auto-disable circuit breaker;
* ``apply_auto_disable_guard`` disables only a bot that clears the
  min-trades bar and is below the hit-rate floor;
* ``engine.run_calibration_sweep`` recalibrates every bot, isolating
  failures;
* the scheduler's ``_sweep_due`` cadence (incl. run-once-on-start);
* ``provision_defaults`` no longer force-re-enables, so an auto-disable
  survives a redeploy.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.tradeworkz import config as tw_config
from src.tradeworkz import engine
from src.tradeworkz.ml import apply_auto_disable_guard, recalibrate_bot
from src.tradeworkz.scheduler import _sweep_due

# Anchored to the real clock, NOT a fixed date. ``recalibrate_bot`` windows
# trades against ``datetime.now() - 30 days``, so a hard-coded timestamp
# silently stops exercising the adaptive-threshold branch the moment it ages
# past that lookback: ``win_30d`` goes None, the clamp never runs, and
# ``confidence_threshold`` keeps its 0.55 dataclass default instead of the
# 0.60 ceiling. This test did exactly that on 2026-08-21, 36 days after the
# date below was written. Every use here is relative (trade ages, and
# _sweep_due comparisons against NOW +/- a delta), so a live anchor is safe.
NOW = datetime.now(timezone.utc).replace(microsecond=0)


# ----------------------------------------------------------------------
# apply_auto_disable_guard
# ----------------------------------------------------------------------


class _GuardConn:
    def __init__(self, rowcount: int = 1):
        self._rowcount = rowcount
        self.rowcount = 0
        self.executed = []

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        self.rowcount = self._rowcount


def test_guard_disables_a_persistent_bleeder():
    conn = _GuardConn(rowcount=1)
    assert apply_auto_disable_guard(conn, "eod_pin_drifter", 25, 0.20) is True
    assert conn.executed and "UPDATE tw_bots" in conn.executed[0][0]
    assert conn.executed[0][1] == ("eod_pin_drifter",)


def test_guard_noop_below_min_trades():
    conn = _GuardConn()
    assert apply_auto_disable_guard(conn, "b", 10, 0.10) is False
    assert conn.executed == []  # never even queried


def test_guard_noop_when_hit_rate_clears_bar():
    conn = _GuardConn()
    assert apply_auto_disable_guard(conn, "b", 50, 0.40) is False
    assert conn.executed == []


def test_guard_noop_when_hit_rate_missing():
    conn = _GuardConn()
    assert apply_auto_disable_guard(conn, "b", 50, None) is False
    assert conn.executed == []


def test_guard_respects_config_toggle(monkeypatch):
    monkeypatch.setattr(tw_config, "AUTO_DISABLE_ENABLED", False)
    conn = _GuardConn()
    assert apply_auto_disable_guard(conn, "b", 99, 0.01) is False
    assert conn.executed == []


def test_guard_returns_false_when_already_disabled():
    # rowcount 0 -> the WHERE enabled IS TRUE matched nothing.
    conn = _GuardConn(rowcount=0)
    assert apply_auto_disable_guard(conn, "b", 25, 0.10) is False


# ----------------------------------------------------------------------
# recalibrate_bot end-to-end (scripted fake conn)
# ----------------------------------------------------------------------


class _ScriptCursor:
    def __init__(self, trades):
        self._trades = trades
        self._pending = []
        self.rowcount = 0
        self.updates = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self._pending = []
        self.rowcount = 0
        if "FROM tw_ml_state" in s:
            self._pending = []  # no prior state -> load_state default
        elif "FROM tw_trades" in s:
            self._pending = list(self._trades)
        elif "UPDATE tw_bots" in s:
            self.rowcount = 1
            self.updates.append((s, params))
        # INSERT INTO tw_ml_state (save_state) -> nothing to fetch

    def fetchone(self):
        return self._pending[0] if self._pending else None

    def fetchall(self):
        return list(self._pending)


class _ScriptConn:
    def __init__(self, trades):
        self._cur = _ScriptCursor(trades)

    def cursor(self):
        return self._cur


def _trades(n_win: int, n_loss: int, win_pnl=50.0, loss_pnl=-100.0):
    rows = []
    for i in range(n_win):
        rows.append(("win", win_pnl, NOW - timedelta(hours=i + 1)))
    for i in range(n_loss):
        rows.append(("loss", loss_pnl, NOW - timedelta(hours=i + 1)))
    return rows


def test_recalibrate_throttles_and_disables_a_loser():
    # 5 wins / 20 losses -> hit 0.20, PF 0.125.
    conn = _ScriptConn(_trades(5, 20))
    state = recalibrate_bot(conn, "eod_pin_drifter")
    assert state.hit_rate == pytest.approx(0.20)
    assert state.last_profit_factor == pytest.approx(250.0 / 2000.0)
    assert state.size_multiplier == 0.5  # PF < 1 -> throttle
    assert state.confidence_base == pytest.approx(0.40)  # clamped to floor
    # Adaptive threshold: 1 - 0.20 + 0.30 = 1.10, clamped to the CEIL. The
    # ceiling is 0.60 (below the ~0.76 conviction ceiling), NOT the old 0.75
    # that locked the whole fleet out of trading.
    assert state.confidence_threshold == pytest.approx(0.60)  # adaptive, capped at CEIL
    # Circuit breaker fired (25 trades >= 20, hit 0.20 < 0.35).
    assert conn.cursor().updates, "auto-disable UPDATE should have run"


def test_recalibrate_leaves_a_winner_enabled_and_boosts_size():
    # 15 wins / 10 losses -> hit 0.60, PF 3.0.
    conn = _ScriptConn(_trades(15, 10, win_pnl=100.0, loss_pnl=-50.0))
    state = recalibrate_bot(conn, "vix_regime_breakout")
    assert state.hit_rate == pytest.approx(0.60)
    assert state.size_multiplier == 1.25  # PF >= 1.4 -> boost
    assert conn.cursor().updates == []  # guard did NOT fire


# ----------------------------------------------------------------------
# engine.run_calibration_sweep
# ----------------------------------------------------------------------


def test_sweep_recalibrates_every_bot(monkeypatch):
    called = []
    monkeypatch.setattr(engine.ml_mod, "recalibrate_bot", lambda conn, bid: called.append(bid))
    n = engine.run_calibration_sweep(object(), ["a", "b", "c"])
    assert n == 3 and called == ["a", "b", "c"]


def test_sweep_isolates_a_failing_bot(monkeypatch):
    def rc(conn, bid):
        if bid == "b":
            raise RuntimeError("boom")

    monkeypatch.setattr(engine.ml_mod, "recalibrate_bot", rc)

    class _Conn:
        def rollback(self):
            pass

    n = engine.run_calibration_sweep(_Conn(), ["a", "b", "c"])
    assert n == 2  # a and c still done


# ----------------------------------------------------------------------
# scheduler cadence
# ----------------------------------------------------------------------


def test_sweep_due_runs_once_on_start_then_on_interval():
    assert _sweep_due(None, NOW, 3600.0) is True  # first tick after start
    assert _sweep_due(NOW - timedelta(hours=2), NOW, 3600.0) is True  # elapsed
    assert _sweep_due(NOW - timedelta(minutes=30), NOW, 3600.0) is False  # too soon
    assert _sweep_due(None, NOW, 0.0) is False  # interval 0 -> disabled


# ----------------------------------------------------------------------
# provision no longer force-re-enables (so auto-disable sticks)
# ----------------------------------------------------------------------


class _ProvCursor:
    def __init__(self):
        self.rowcount = 1
        self.sqls = []

    def execute(self, sql, params=None):
        self.sqls.append(" ".join(sql.split()))

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _ProvConn:
    def __init__(self):
        self._cur = _ProvCursor()

    def cursor(self):
        return self._cur


def test_provision_does_not_resync_enabled(monkeypatch):
    # The live DEFAULT_ROSTER is currently empty (the fleet is shelved), so
    # inject a one-bot roster to exercise the upsert path independently of the
    # active roster's state — the invariant under test is about the SQL, not
    # which bots happen to be active.
    from src.tradeworkz.models import BotSpec

    fake = BotSpec(
        id="t_prov", display_name="T", strategy_class="PutCallWallBouncer",
        tier="0DTE", direction_mode="context", universe="*",
        tagline="", description="", params={},
    )
    monkeypatch.setattr(engine, "DEFAULT_ROSTER", (fake,))
    conn = _ProvConn()
    engine.provision_defaults(conn, fleet_capital=12000.0)
    upserts = [s for s in conn.cursor().sqls if "INSERT INTO tw_bots " in s]
    assert upserts, "provision should upsert tw_bots rows"
    for s in upserts:
        do_update = s.split("DO UPDATE SET", 1)[1]
        assert "enabled" not in do_update.lower(), (
            "provision_defaults must NOT re-sync enabled, or a runtime "
            "auto-disable would be undone on the next redeploy"
        )
