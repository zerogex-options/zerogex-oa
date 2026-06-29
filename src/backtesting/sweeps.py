"""Parameter sweeps (Phase 6).

A *sweep* runs one base :class:`BacktestSpec` across the Cartesian product of one
or two parameter axes. Every grid cell is a normal ``backtest_runs`` row — so it
reuses the engine, worker, and persistence untouched — tied back to the parent
via ``sweep_id`` and tagged with its ``sweep_cell`` overrides.

Sweepable parameters are whitelisted (:data:`SWEEPABLE`): each maps a flat axis
key to a dotted path into the spec dict. Axis values are applied in the *same
units the spec stores* (e.g. ``profit_target_pct`` is a fraction, 0.5 = 50%),
then the whole spec is re-validated through ``BacktestSpec.from_dict`` so a cell
can never produce an invalid run.
"""

from __future__ import annotations

import copy
import itertools
import json
from typing import Optional

from src.backtesting.models import BacktestSpec, SpecError
from src.backtesting.runner import create_run
from src.database.connection import close_db_connection, db_connection, get_db_connection

# Flat axis key -> dotted path into the spec dict produced by ``to_dict``.
# ``strategy.*`` keys only apply when the base spec carries a ``strategy`` block.
SWEEPABLE: dict[str, tuple[str, ...]] = {
    "risk_per_trade_pct": ("sizing", "risk_per_trade_pct"),
    "max_concurrent": ("sizing", "max_concurrent"),
    "max_net_delta": ("sizing", "max_net_delta"),
    "max_net_vega": ("sizing", "max_net_vega"),
    "slippage_pct": ("fill_model", "slippage_pct"),
    "commission_per_contract": ("fill_model", "commission_per_contract"),
    "max_hold_minutes": ("exit", "max_hold_minutes"),
    "profit_target_pct": ("exit", "profit_target_pct"),
    "stop_loss_pct": ("exit", "stop_loss_pct"),
    "dte": ("strategy", "entry", "dte"),
    "width": ("strategy", "width"),
    "wing": ("strategy", "wing"),
    "target_offset_pct": ("strategy", "target_offset_pct"),
    "stop_offset_pct": ("strategy", "stop_offset_pct"),
}

# Bounds keep a single sweep from fanning out into an unbounded number of full
# backtests (each cell is a complete engine run over the window).
MAX_AXES = 2
MAX_VALUES_PER_AXIS = 8
MAX_CELLS = 24


class SweepError(ValueError):
    """Raised when a sweep request is structurally invalid."""


def validate_axes(axes: object, base_spec: BacktestSpec) -> list[dict]:
    """Normalize and validate the sweep axes against the base spec.

    Returns a list of ``{"param": str, "values": list}`` with de-duplicated,
    order-preserving values. Raises :class:`SweepError` on any problem.
    """
    if not isinstance(axes, list) or not axes:
        raise SweepError("axes must be a non-empty list")
    if len(axes) > MAX_AXES:
        raise SweepError(f"at most {MAX_AXES} axes may be swept at once")

    has_strategy = base_spec.strategy is not None
    out: list[dict] = []
    seen_params: set[str] = set()
    total = 1
    for axis in axes:
        if not isinstance(axis, dict):
            raise SweepError("each axis must be an object with `param` and `values`")
        param = str(axis.get("param") or "").strip()
        if param not in SWEEPABLE:
            raise SweepError(f"unknown sweep param {param!r}")
        if param in seen_params:
            raise SweepError(f"param {param!r} listed more than once")
        if SWEEPABLE[param][0] == "strategy" and not has_strategy:
            raise SweepError(f"param {param!r} requires a custom-strategy base spec")
        raw_values = axis.get("values")
        if not isinstance(raw_values, list) or not raw_values:
            raise SweepError(f"axis {param!r} needs a non-empty `values` list")
        # De-duplicate while preserving order.
        values: list = []
        for v in raw_values:
            if v not in values:
                values.append(v)
        if len(values) > MAX_VALUES_PER_AXIS:
            raise SweepError(
                f"axis {param!r} has more than {MAX_VALUES_PER_AXIS} values"
            )
        seen_params.add(param)
        total *= len(values)
        out.append({"param": param, "values": values})

    if total > MAX_CELLS:
        raise SweepError(
            f"sweep would produce {total} cells; the limit is {MAX_CELLS}"
        )
    return out


def _set_path(d: dict, path: tuple[str, ...], value) -> None:
    cur = d
    for key in path[:-1]:
        nxt = cur.get(key)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[key] = nxt
        cur = nxt
    cur[path[-1]] = value


def apply_cell(base_spec_dict: dict, cell: dict) -> dict:
    """Return a deep copy of the base spec dict with one cell's overrides set."""
    spec = copy.deepcopy(base_spec_dict)
    for param, value in cell.items():
        _set_path(spec, SWEEPABLE[param], value)
    return spec


def expand_cells(axes: list[dict]) -> list[dict]:
    """Cartesian product of the axes → list of ``{param: value}`` cells."""
    params = [a["param"] for a in axes]
    value_lists = [a["values"] for a in axes]
    return [dict(zip(params, combo)) for combo in itertools.product(*value_lists)]


def create_sweep(base_spec: BacktestSpec, axes: list[dict], *,
                 end_user: Optional[str]) -> dict:
    """Create the sweep row and one queued child run per grid cell.

    Validates every cell's spec up front (so a bad combination fails the whole
    request rather than silently producing a broken run). Returns
    ``{"sweep_id", "n_cells", "run_ids"}``.
    """
    base_dict = base_spec.to_dict()
    cells = expand_cells(axes)

    # Pre-validate each cell so we never persist a half-built sweep.
    child_specs: list[tuple[dict, BacktestSpec]] = []
    for cell in cells:
        spec_dict = apply_cell(base_dict, cell)
        try:
            child = BacktestSpec.from_dict(spec_dict)
        except SpecError as exc:
            raise SweepError(f"cell {cell} is invalid: {exc}")
        child_specs.append((cell, child))

    conn = get_db_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO backtest_sweeps (end_user, underlying, base_spec, axes, n_cells)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                end_user,
                base_spec.underlying,
                json.dumps(base_dict),
                json.dumps(axes),
                len(cells),
            ),
        )
        sweep_id = int(cur.fetchone()[0])
    finally:
        close_db_connection(conn)

    run_ids: list[int] = []
    for cell, child in child_specs:
        run_ids.append(
            create_run(child, end_user=end_user, sweep_id=sweep_id, sweep_cell=cell)
        )
    return {"sweep_id": sweep_id, "n_cells": len(cells), "run_ids": run_ids}


def _summary_metric(summary: Optional[dict]) -> Optional[dict]:
    """Slim a run summary down to the metrics the sweep grid displays."""
    if not summary:
        return None
    return {
        "n_trades": summary.get("n_trades"),
        "win_rate": summary.get("win_rate"),
        "net_pnl": summary.get("net_pnl"),
        "total_return_pct": summary.get("total_return_pct"),
        "max_drawdown_pct": summary.get("max_drawdown_pct"),
        "profit_factor": summary.get("profit_factor"),
    }


def get_sweep(sweep_id: int, *, end_user: Optional[str]) -> Optional[dict]:
    """Sweep header + per-cell run status/metrics, scoped to its owner."""
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, underlying, axes, n_cells, created_at, end_user "
            "FROM backtest_sweeps WHERE id = %s",
            (sweep_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        owner = row[5]
        if owner is not None and owner != end_user:
            return None

        cur.execute(
            "SELECT id, sweep_cell, status, summary, progress "
            "FROM backtest_runs WHERE sweep_id = %s ORDER BY id",
            (sweep_id,),
        )
        cells = [
            {
                "run_id": int(r[0]),
                "cell": r[1] or {},
                "status": r[2],
                "metrics": _summary_metric(r[3]),
                "progress": float(r[4]) if r[4] is not None else 0.0,
            }
            for r in cur.fetchall()
        ]
        done = sum(1 for c in cells if c["status"] in ("completed", "failed"))
        return {
            "sweep_id": int(row[0]),
            "underlying": row[1],
            "axes": row[2],
            "n_cells": int(row[3]),
            "created_at": row[4].isoformat() if row[4] else None,
            "cells": cells,
            "completed": done,
            "status": "completed" if done == len(cells) and cells else "running",
        }


def list_sweeps(*, end_user: Optional[str], limit: int = 25) -> list[dict]:
    """Recent sweeps for this end-user (or the anonymous pool)."""
    with db_connection() as conn:
        cur = conn.cursor()
        if end_user:
            cur.execute(
                "SELECT id, underlying, axes, n_cells, created_at FROM backtest_sweeps "
                "WHERE end_user = %s ORDER BY created_at DESC LIMIT %s",
                (end_user, limit),
            )
        else:
            cur.execute(
                "SELECT id, underlying, axes, n_cells, created_at FROM backtest_sweeps "
                "WHERE end_user IS NULL ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
        return [
            {
                "sweep_id": int(r[0]),
                "underlying": r[1],
                "axes": r[2],
                "n_cells": int(r[3]),
                "created_at": r[4].isoformat() if r[4] else None,
            }
            for r in cur.fetchall()
        ]
