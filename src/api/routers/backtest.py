"""Backtesting platform REST API.

Thin async surface over the synchronous engine in ``src/backtesting``. Writes
and reads are sync psycopg2 (so they reuse the engine's DB layer) and are
dispatched via ``asyncio.to_thread`` to avoid blocking the event loop. Run
execution is scheduled on ``BackgroundTasks`` — Starlette runs the sync task
in its threadpool.

See ``docs/design/backtesting-platform.md`` for the contract.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, Response

from src.api.identity import resolve_end_user
from src.backtesting import configs, queries, sweeps
from src.backtesting.meta import build_meta
from src.backtesting.models import BacktestSpec, SpecError
from src.backtesting.runner import create_run, execute_run
from src.backtesting.sweeps import SweepError
from src.database.connection import db_connection

logger = logging.getLogger(__name__)

# Tagged "Beta" in addition to "Backtesting" so the whole platform surfaces
# under the Beta group in the OpenAPI/Swagger docs while it stabilises.
router = APIRouter(prefix="/api/backtest", tags=["Backtesting", "Beta"])


def _build_meta_sync() -> dict:
    with db_connection() as conn:
        return build_meta(conn)


@router.get("/meta")
async def get_meta() -> dict:
    """Catalog for the configuration form: underlyings, patterns, data window, defaults."""
    return await asyncio.to_thread(_build_meta_sync)


@router.post("/runs", status_code=202)
async def create_backtest_run(request: Request, background_tasks: BackgroundTasks) -> dict:
    """Validate a BacktestSpec, persist a queued run, and schedule execution."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="request body must be valid JSON")
    try:
        spec = BacktestSpec.from_dict(body)
    except SpecError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    end_user, _ = resolve_end_user(request)
    try:
        run_id = await asyncio.to_thread(create_run, spec, end_user=end_user)
    except Exception:
        logger.exception("failed to create backtest run")
        raise HTTPException(status_code=500, detail="could not create backtest run")

    # With a dedicated worker the API only enqueues; otherwise Starlette runs
    # the sync execution in its threadpool after the response.
    from src.config import BACKTEST_WORKER_ENABLED

    if not BACKTEST_WORKER_ENABLED:
        background_tasks.add_task(execute_run, run_id)
    return {"run_id": run_id, "status": "queued"}


@router.get("/runs")
async def list_backtest_runs(
    request: Request,
    limit: int = Query(25, ge=1, le=100),
) -> list:
    """Recent runs for the calling end-user (anonymous runs when unauthenticated)."""
    end_user, _ = resolve_end_user(request)
    return await asyncio.to_thread(queries.list_runs, end_user=end_user, limit=limit)


@router.get("/runs/{run_id}")
async def get_backtest_run(run_id: int, request: Request) -> dict:
    """Status + summary for one run."""
    end_user, _ = resolve_end_user(request)
    run = await asyncio.to_thread(queries.get_run, run_id, end_user=end_user)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@router.get("/runs/{run_id}/trades")
async def get_backtest_trades(
    run_id: int,
    request: Request,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    """Paginated trade blotter for a run."""
    end_user, _ = resolve_end_user(request)
    run = await asyncio.to_thread(queries.get_run, run_id, end_user=end_user)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return await asyncio.to_thread(queries.get_trades, run_id, limit=limit, offset=offset)


# CSV columns for the trade-blotter export (flat fields only; ``legs`` omitted).
_CSV_COLUMNS = [
    "seq", "structure", "pattern", "direction", "tier", "option_symbol",
    "option_type", "strike", "expiration", "entered_at", "exited_at",
    "entry_premium", "exit_premium", "contracts", "net_pnl", "return_pct",
    "outcome", "hold_minutes", "net_delta", "net_vega",
]


@router.get("/runs/{run_id}/trades.csv")
async def download_backtest_trades_csv(run_id: int, request: Request) -> Response:
    """Download the full trade blotter for a run as CSV."""
    end_user, _ = resolve_end_user(request)
    run = await asyncio.to_thread(queries.get_run, run_id, end_user=end_user)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    page = await asyncio.to_thread(queries.get_trades, run_id, limit=1_000_000, offset=0)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for trade in page["trades"]:
        writer.writerow(trade)
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="backtest_run_{run_id}_trades.csv"',
        },
    )


@router.get("/runs/{run_id}/equity")
async def get_backtest_equity(run_id: int, request: Request) -> list:
    """Equity curve points for a run."""
    end_user, _ = resolve_end_user(request)
    run = await asyncio.to_thread(queries.get_run, run_id, end_user=end_user)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return await asyncio.to_thread(queries.get_equity, run_id)


# ---------------------------------------------------------------------------
# Saved & shareable configurations (Phase 6)
# ---------------------------------------------------------------------------


def _validated_spec(body: dict) -> BacktestSpec:
    """Validate the ``spec`` block of a config request body, or 422."""
    spec_in = body.get("spec")
    if not isinstance(spec_in, dict):
        raise HTTPException(status_code=422, detail="`spec` object is required")
    try:
        return BacktestSpec.from_dict(spec_in)
    except SpecError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/configs", status_code=201)
async def create_backtest_config(request: Request) -> dict:
    """Save a named, validated BacktestSpec; returns its summary + share token."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="request body must be valid JSON")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="`name` is required")
    if len(name) > 120:
        name = name[:120]
    spec = _validated_spec(body)
    end_user, _ = resolve_end_user(request)
    return await asyncio.to_thread(
        configs.save_config,
        spec.to_dict(),
        name=name,
        underlying=spec.underlying,
        end_user=end_user,
    )


@router.get("/configs")
async def list_backtest_configs(request: Request) -> list:
    """Saved configs for the calling end-user (anonymous pool when unauthenticated)."""
    end_user, _ = resolve_end_user(request)
    return await asyncio.to_thread(configs.list_configs, end_user=end_user)


@router.get("/configs/shared/{share_token}")
async def get_shared_backtest_config(share_token: str) -> dict:
    """Public read-only fetch of a shared config by token (clone into the form)."""
    cfg = await asyncio.to_thread(configs.get_shared_config, share_token)
    if cfg is None:
        raise HTTPException(status_code=404, detail="shared config not found")
    return cfg


@router.get("/configs/{config_id}")
async def get_backtest_config(config_id: int, request: Request) -> dict:
    """Fetch one saved config (incl. spec), scoped to its owner."""
    end_user, _ = resolve_end_user(request)
    cfg = await asyncio.to_thread(configs.get_config, config_id, end_user=end_user)
    if cfg is None:
        raise HTTPException(status_code=404, detail="config not found")
    return cfg


@router.put("/configs/{config_id}")
async def update_backtest_config(config_id: int, request: Request) -> dict:
    """Rename and/or replace a saved config's spec (owner only)."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="request body must be valid JSON")

    name = body.get("name")
    if name is not None:
        name = str(name).strip()[:120] or None

    spec_dict: dict | None = None
    underlying: str | None = None
    if body.get("spec") is not None:
        spec = _validated_spec(body)
        spec_dict = spec.to_dict()
        underlying = spec.underlying

    if name is None and spec_dict is None:
        raise HTTPException(status_code=422, detail="nothing to update")

    end_user, _ = resolve_end_user(request)
    cfg = await asyncio.to_thread(
        configs.update_config,
        config_id,
        end_user=end_user,
        name=name,
        spec_dict=spec_dict,
        underlying=underlying,
    )
    if cfg is None:
        raise HTTPException(status_code=404, detail="config not found")
    return cfg


@router.delete("/configs/{config_id}")
async def delete_backtest_config(config_id: int, request: Request) -> dict:
    """Delete a saved config (owner only)."""
    end_user, _ = resolve_end_user(request)
    ok = await asyncio.to_thread(configs.delete_config, config_id, end_user=end_user)
    if not ok:
        raise HTTPException(status_code=404, detail="config not found")
    return {"deleted": True}


# ---------------------------------------------------------------------------
# Parameter sweeps (Phase 6)
# ---------------------------------------------------------------------------


def _create_sweep_sync(base_spec: BacktestSpec, axes: list,
                       end_user: Optional[str]) -> dict:
    return sweeps.create_sweep(base_spec, axes, end_user=end_user)


@router.post("/sweeps", status_code=202)
async def create_backtest_sweep(request: Request,
                                background_tasks: BackgroundTasks) -> dict:
    """Run a base spec across a parameter grid; one queued child run per cell."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="request body must be valid JSON")
    try:
        base_spec = BacktestSpec.from_dict(body.get("spec") or {})
    except SpecError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    try:
        axes = sweeps.validate_axes(body.get("axes"), base_spec)
    except SweepError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    end_user, _ = resolve_end_user(request)
    try:
        result = await asyncio.to_thread(
            _create_sweep_sync, base_spec, axes, end_user
        )
    except SweepError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        logger.exception("failed to create backtest sweep")
        raise HTTPException(status_code=500, detail="could not create backtest sweep")

    # Same dispatch model as single runs: a dedicated worker drains the queue,
    # otherwise schedule each child run on Starlette's threadpool.
    from src.config import BACKTEST_WORKER_ENABLED

    if not BACKTEST_WORKER_ENABLED:
        for run_id in result["run_ids"]:
            background_tasks.add_task(execute_run, run_id)
    return result


@router.get("/sweeps")
async def list_backtest_sweeps(
    request: Request,
    limit: int = Query(25, ge=1, le=100),
) -> list:
    """Recent sweeps for the calling end-user."""
    end_user, _ = resolve_end_user(request)
    return await asyncio.to_thread(sweeps.list_sweeps, end_user=end_user, limit=limit)


@router.get("/sweeps/{sweep_id}")
async def get_backtest_sweep(sweep_id: int, request: Request) -> dict:
    """Sweep header + per-cell run status/metrics (used while polling)."""
    end_user, _ = resolve_end_user(request)
    sweep = await asyncio.to_thread(sweeps.get_sweep, sweep_id, end_user=end_user)
    if sweep is None:
        raise HTTPException(status_code=404, detail="sweep not found")
    return sweep
