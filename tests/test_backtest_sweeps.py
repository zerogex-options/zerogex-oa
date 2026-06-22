"""Tests for parameter sweeps (Phase 6, src/backtesting.sweeps).

The grid math, override application, and axis validation are pure functions and
tested directly; persistence is exercised against a scripted fake connection.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.backtesting import sweeps as sweeps_mod
from src.backtesting.models import BacktestSpec
from src.backtesting.sweeps import (
    SweepError,
    apply_cell,
    expand_cells,
    validate_axes,
)


def _base_spec(strategy=False) -> BacktestSpec:
    raw = {
        "underlying": "SPY",
        "start_date": "2026-01-02",
        "end_date": "2026-01-10",
        "fill_model": {"slippage_pct": 0.01, "commission_per_contract": 0.65},
        "sizing": {"capital": 25000, "risk_per_trade_pct": 2.0, "max_concurrent": 3},
        "exit": {"max_hold_minutes": 120, "profit_target_pct": 0.5, "stop_loss_pct": 0.5},
    }
    if strategy:
        raw["strategy"] = {
            "direction": "bullish",
            "conditions": [{"field": "msi", "op": ">", "value": 50}],
            "entry": {"dte": 0},
            "structure": "vertical",
            "width": 5,
        }
    else:
        raw["patterns"] = ["gamma_squeeze"]
    return BacktestSpec.from_dict(raw)


def test_expand_cells_cartesian_product():
    axes = [
        {"param": "profit_target_pct", "values": [0.3, 0.5]},
        {"param": "stop_loss_pct", "values": [0.4, 0.6, 0.8]},
    ]
    cells = expand_cells(axes)
    assert len(cells) == 6
    assert {"profit_target_pct": 0.3, "stop_loss_pct": 0.4} in cells
    assert {"profit_target_pct": 0.5, "stop_loss_pct": 0.8} in cells


def test_apply_cell_sets_nested_path():
    base = _base_spec().to_dict()
    out = apply_cell(base, {"profit_target_pct": 0.9, "risk_per_trade_pct": 5.0})
    assert out["exit"]["profit_target_pct"] == 0.9
    assert out["sizing"]["risk_per_trade_pct"] == 5.0
    # Base dict is not mutated.
    assert base["exit"]["profit_target_pct"] == 0.5


def test_apply_cell_strategy_path():
    base = _base_spec(strategy=True).to_dict()
    out = apply_cell(base, {"dte": 2, "width": 10})
    assert out["strategy"]["entry"]["dte"] == 2
    assert out["strategy"]["width"] == 10


def test_validate_axes_rejects_unknown_param():
    with pytest.raises(SweepError, match="unknown sweep param"):
        validate_axes([{"param": "nope", "values": [1]}], _base_spec())


def test_validate_axes_rejects_strategy_param_on_pattern_spec():
    with pytest.raises(SweepError, match="custom-strategy"):
        validate_axes([{"param": "dte", "values": [0, 1]}], _base_spec())


def test_validate_axes_allows_strategy_param_on_strategy_spec():
    axes = validate_axes([{"param": "dte", "values": [0, 1, 2]}], _base_spec(strategy=True))
    assert axes[0]["param"] == "dte"
    assert axes[0]["values"] == [0, 1, 2]


def test_validate_axes_dedupes_values():
    axes = validate_axes(
        [{"param": "max_concurrent", "values": [1, 2, 2, 3, 1]}], _base_spec()
    )
    assert axes[0]["values"] == [1, 2, 3]


def test_validate_axes_rejects_too_many_cells():
    axes = [
        {"param": "profit_target_pct", "values": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]},
        {"param": "stop_loss_pct", "values": [0.1, 0.2, 0.3, 0.4, 0.5]},
    ]
    with pytest.raises(SweepError, match="limit is"):
        validate_axes(axes, _base_spec())


def test_validate_axes_rejects_more_than_two_axes():
    axes = [
        {"param": "profit_target_pct", "values": [0.1]},
        {"param": "stop_loss_pct", "values": [0.1]},
        {"param": "risk_per_trade_pct", "values": [1]},
    ]
    with pytest.raises(SweepError, match="at most"):
        validate_axes(axes, _base_spec())


def test_validate_axes_rejects_duplicate_param():
    axes = [
        {"param": "stop_loss_pct", "values": [0.1]},
        {"param": "stop_loss_pct", "values": [0.2]},
    ]
    with pytest.raises(SweepError, match="more than once"):
        validate_axes(axes, _base_spec())


def test_validate_axes_requires_nonempty_values():
    with pytest.raises(SweepError, match="non-empty"):
        validate_axes([{"param": "stop_loss_pct", "values": []}], _base_spec())


# ---- create_sweep persistence (scripted fake connection) -----------------


class _Cur:
    def __init__(self, fetch_results):
        self._results = list(fetch_results)
        self._i = 0
        self.executed: list[tuple] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        row = self._results[self._i]
        self._i += 1
        return row


class _Conn:
    def __init__(self, fetch_results):
        self.autocommit = False
        self._cur = _Cur(fetch_results)

    def cursor(self):
        return self._cur


def test_create_sweep_inserts_parent_and_children(monkeypatch):
    # Parent INSERT ... RETURNING id -> 100; then create_run is patched.
    sweep_conn = _Conn([(100,)])
    monkeypatch.setattr(sweeps_mod, "get_db_connection", lambda: sweep_conn)
    monkeypatch.setattr(sweeps_mod, "close_db_connection", lambda c: None)

    created: list[dict] = []

    def fake_create_run(spec, *, end_user, sweep_id=None, sweep_cell=None):
        created.append({"sweep_id": sweep_id, "cell": sweep_cell})
        return 200 + len(created)

    monkeypatch.setattr(sweeps_mod, "create_run", fake_create_run)

    axes = validate_axes(
        [{"param": "profit_target_pct", "values": [0.3, 0.5]}], _base_spec()
    )
    result = sweeps_mod.create_sweep(_base_spec(), axes, end_user="alice")

    assert result["sweep_id"] == 100
    assert result["n_cells"] == 2
    assert result["run_ids"] == [201, 202]
    # Every child run linked to the sweep and carries its cell.
    assert all(c["sweep_id"] == 100 for c in created)
    assert {"profit_target_pct": 0.3} in [c["cell"] for c in created]


def test_create_sweep_rejects_invalid_cell(monkeypatch):
    # An axis value that makes the spec invalid should abort before any insert.
    monkeypatch.setattr(
        sweeps_mod, "get_db_connection",
        lambda: pytest.fail("must not open a connection for an invalid sweep"),
    )
    # stop_loss_pct of 0 disables that exit; with profit target still set the
    # spec stays valid, so use an invalid underlying override path instead:
    base = _base_spec(strategy=True)
    # Force an invalid strategy by sweeping dte to a value, then monkeypatch
    # BacktestSpec.from_dict to raise for one cell.
    from src.backtesting import models

    real_from_dict = models.BacktestSpec.from_dict

    def flaky(raw):
        if raw.get("strategy", {}).get("entry", {}).get("dte") == 1:
            from src.backtesting.models import SpecError
            raise SpecError("bad dte")
        return real_from_dict(raw)

    monkeypatch.setattr(sweeps_mod.BacktestSpec, "from_dict", staticmethod(flaky))
    axes = [{"param": "dte", "values": [0, 1]}]
    with pytest.raises(SweepError, match="is invalid"):
        sweeps_mod.create_sweep(base, axes, end_user="alice")


def test_base_spec_helpers_have_expected_shape():
    assert _base_spec().strategy is None
    assert _base_spec(strategy=True).strategy is not None
    assert _base_spec().start_date == date(2026, 1, 2)
