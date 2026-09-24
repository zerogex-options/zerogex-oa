"""Adaptive entry bar: each pattern earns its bar per symbol from graded ideas."""

from __future__ import annotations

import asyncio

import pytest

from src import config
from src.signals.playbook import adaptive_gate as gate


@pytest.fixture(autouse=True)
def _clean_store(monkeypatch):
    """No test leaks a loaded store (or a changed knob) into another."""
    gate.set_active_store(None)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_GATE_ENABLED", True)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_MIN_IDEAS", 8)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_PRIOR_WEIGHT", 10.0)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_COST_R", 0.10)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_NEUTRAL_BAR", 0.25)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_MIN_BAR", 0.20)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_MAX_BAR", 0.75)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_PROVEN_R", 0.25)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_PAUSE_R", -0.25)
    yield
    gate.set_active_store(None)


def _row(pattern, underlying, direction, n, mean_r, *, wins=None, losses=None, weight=None):
    """A records_sql row: (pattern, underlying, direction, n, weight, wins, losses, sum_wr)."""
    weight = float(n) if weight is None else weight
    wins = n // 2 if wins is None else wins
    losses = n - wins if losses is None else losses
    return (pattern, underlying, direction, n, weight, wins, losses, mean_r * weight)


# ----------------------------------------------------------------------
# The bar
# ----------------------------------------------------------------------


def test_bar_scales_with_expectancy():
    assert gate.bar_for(0.40) == (0.20, gate.PROVEN)
    bar, status = gate.bar_for(0.0)
    assert (round(bar, 4), status) == (0.25, gate.POSITIVE)
    bar, status = gate.bar_for(-0.125)  # halfway to the pause line
    assert status == gate.LAGGING and bar == pytest.approx(0.50)
    bar, status = gate.bar_for(-0.25)
    assert status == gate.PAUSED and bar > 1.0


def test_no_store_means_the_old_flat_floor():
    verdict = gate.assess("gamma_flip_break", "SPY", "bullish")
    assert verdict.status == gate.LEARNING
    assert verdict.bar == 0.25
    assert not verdict.is_adaptive


def test_gate_switched_off_means_the_old_flat_floor(monkeypatch):
    store = gate.build_store([_row("p", "SPY", "bullish", 40, -1.0, wins=0, losses=40)])
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_GATE_ENABLED", False)
    verdict = gate.assess("p", "SPY", "bullish", store)
    assert verdict.status == gate.LEARNING and verdict.bar == 0.25


def test_too_few_ideas_keeps_the_neutral_bar():
    store = gate.build_store([_row("p", "SPY", "bullish", 5, -1.0, wins=0, losses=5)])
    verdict = gate.assess("p", "SPY", "bullish", store)
    assert verdict.status == gate.LEARNING
    assert verdict.bar == 0.25
    assert verdict.record.n == 5


def test_a_losing_pattern_on_a_symbol_is_paused_there():
    store = gate.build_store([_row("p", "SPY", "bullish", 40, -0.5, wins=10, losses=30)])
    verdict = gate.assess("p", "SPY", "bullish", store)
    assert verdict.paused
    assert "paused" in verdict.miss_reason(0.9)
    assert "10 won, 30 lost of 40 graded ideas on SPY bullish" in verdict.miss_reason(0.9)


def test_a_mildly_losing_pattern_needs_much_more_confidence():
    store = gate.build_store([_row("p", "SPY", "bearish", 30, -0.10)])
    verdict = gate.assess("p", "SPY", "bearish", store)
    # (30 * -0.10) / 40 = -0.075 gross, -0.175 net of friction.
    assert verdict.expected_r == pytest.approx(-0.175)
    assert verdict.status == gate.LAGGING
    assert verdict.bar == pytest.approx(0.25 + 0.50 * 0.175 / 0.25)
    assert "needs confidence 0.60, has 0.45" in verdict.miss_reason(0.45)


def test_a_winning_pattern_gets_a_lower_bar():
    store = gate.build_store([_row("p", "SPY", "bullish", 60, 0.60, wins=40, losses=20)])
    verdict = gate.assess("p", "SPY", "bullish", store)
    # (60 * 0.60) / 70 = 0.514 gross, 0.414 net: proven.
    assert verdict.status == gate.PROVEN
    assert verdict.bar == 0.20


def test_record_is_per_symbol():
    """Works on SPY, fails on QQQ: SPY opens up while QQQ tightens."""
    store = gate.build_store(
        [
            _row("p", "SPY", "bullish", 60, 0.60, wins=40, losses=20),
            _row("p", "QQQ", "bullish", 60, -0.60, wins=15, losses=45),
        ]
    )
    spy = gate.assess("p", "SPY", "bullish", store)
    qqq = gate.assess("p", "QQQ", "bullish", store)
    assert spy.bar < 0.25 < qqq.bar
    assert qqq.paused


def test_thin_symbol_record_leans_on_the_pattern_elsewhere():
    """Three losses on a new symbol don't pause a pattern that works elsewhere."""
    store = gate.build_store(
        [
            _row("p", "SPY", "bullish", 60, 0.60, wins=40, losses=20),
            _row("p", "IWM", "bullish", 3, -1.0, wins=0, losses=3),
        ]
    )
    verdict = gate.assess("p", "IWM", "bullish", store)
    assert not verdict.paused
    assert verdict.status in (gate.POSITIVE, gate.PROVEN)


def test_a_symbol_with_no_record_borrows_the_pattern_record():
    store = gate.build_store([_row("p", "SPY", "bullish", 40, -0.5, wins=10, losses=30)])
    verdict = gate.assess("p", "QQQ", "bullish", store)
    assert verdict.status in (gate.LAGGING, gate.PAUSED)
    assert verdict.scope == "across all symbols"


def test_no_trade_counts_twice_in_the_blend():
    """With one record, the result is one shrink toward zero, not three."""
    store = gate.build_store([_row("p", "SPY", "bullish", 10, -0.30)])
    verdict = gate.assess("p", "SPY", "bullish", store)
    assert verdict.expected_r == pytest.approx((10 * -0.30) / 20 - 0.10)


def test_recency_weight_drives_the_mean_not_the_raw_count():
    # 20 ideas whose recency weight sums to 5: shrinks like 5 ideas.
    store = gate.build_store([_row("p", "SPY", "bullish", 20, -0.40, weight=5.0)])
    verdict = gate.assess("p", "SPY", "bullish", store)
    assert verdict.expected_r == pytest.approx((5 * -0.40) / 15 - 0.10)


def test_summary_is_json_safe():
    store = gate.build_store([_row("p", "SPY", "bullish", 40, -0.5, wins=10, losses=30)])
    summary = gate.assess("p", "SPY", "bullish", store).summary()
    assert summary["status"] == "paused"
    assert summary["bar"] is None
    assert summary["ideas"] == 40 and summary["wins"] == 10 and summary["losses"] == 30


def test_build_store_accepts_mappings_and_skips_empty_rows():
    rows = [
        {
            "pattern": "p",
            "underlying": "spy",
            "direction": "bullish",
            "n": 10,
            "weight": 10.0,
            "wins": 6,
            "losses": 4,
            "sum_wr": 2.0,
        },
        ("q", "SPY", "bullish", 0, 0.0, 0, 0, 0.0),
    ]
    store = gate.build_store(rows)
    assert list(store.by_leaf) == [("p", "SPY", "bullish")]
    assert store.by_leaf[("p", "SPY", "bullish")].mean_r == pytest.approx(0.2)
    assert store.by_pattern["p"].n == 10


def test_records_sql_leaves_out_repeats_and_ungraded_rows():
    sql = gate.records_sql()
    assert "NOT is_repeat" in sql
    assert "'target_hit', 'stop_hit', 'time_exit'" in sql
    assert "%s" not in sql and "$1" not in sql  # runs under both drivers


# ----------------------------------------------------------------------
# Refresh
# ----------------------------------------------------------------------


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


def test_load_store_reads_records():
    store = gate.load_store(_Conn([_row("p", "SPY", "bullish", 12, 0.1)]))
    assert store.by_pattern["p"].n == 12


def test_async_refresh_installs_records_once_per_ttl():
    calls = []

    class _Db:
        async def get_playbook_track_records(self):
            calls.append(1)
            return [_row("p", "SPY", "bullish", 40, -0.5, wins=10, losses=30)]

    asyncio.run(gate.refresh_async(_Db()))
    asyncio.run(gate.refresh_async(_Db()))
    assert len(calls) == 1
    assert gate.assess("p", "SPY", "bullish").paused


def test_async_refresh_failure_keeps_the_previous_records():
    gate.set_active_store(gate.build_store([_row("p", "SPY", "bullish", 40, -0.5)]))
    gate.active_store().loaded_at = 0.0  # due

    class _Db:
        async def get_playbook_track_records(self):
            return None

    asyncio.run(gate.refresh_async(_Db()))
    assert gate.assess("p", "SPY", "bullish").paused
    # And the failure waits a TTL before retrying.
    assert not gate._due(None)
