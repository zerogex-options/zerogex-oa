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
import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request

from src.api.identity import resolve_end_user
from src.backtesting import queries
from src.backtesting.meta import build_meta
from src.backtesting.models import BacktestSpec, SpecError
from src.backtesting.runner import create_run, execute_run
from src.database.connection import close_db_connection, get_db_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/backtest", tags=["Backtesting"])


def _build_meta_sync() -> dict:
    conn = get_db_connection()
    try:
        return build_meta(conn)
    finally:
        close_db_connection(conn)


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

    # Starlette runs this sync task in its threadpool after the response.
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


@router.get("/runs/{run_id}/equity")
async def get_backtest_equity(run_id: int, request: Request) -> list:
    """Equity curve points for a run."""
    end_user, _ = resolve_end_user(request)
    run = await asyncio.to_thread(queries.get_run, run_id, end_user=end_user)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return await asyncio.to_thread(queries.get_equity, run_id)
