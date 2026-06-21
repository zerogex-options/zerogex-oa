"""Backtest run lifecycle: create, execute, persist.

The API creates a run row synchronously (so it can return a ``run_id``) and
schedules :func:`execute_run` on FastAPI ``BackgroundTasks``. Because the
engine and persistence are synchronous psycopg2 code, Starlette runs
``execute_run`` in its threadpool — no event-loop blocking. A dedicated
worker/queue replaces ``BackgroundTasks`` in Phase 3 (see the design doc).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from src.backtesting.engine import run_backtest
from src.backtesting.models import BacktestSpec, RunResult
from src.database.connection import close_db_connection, get_db_connection

logger = logging.getLogger(__name__)


def claim_next_queued_run(conn) -> Optional[int]:
    """Atomically transition the oldest queued run to running; return its id.

    Uses ``FOR UPDATE SKIP LOCKED`` so multiple worker processes never grab the
    same run. Returns None when the queue is empty.
    """
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE backtest_runs SET status = 'running', started_at = NOW()
        WHERE id = (
            SELECT id FROM backtest_runs
            WHERE status = 'queued'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id
        """
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def requeue_stale_runs(conn, *, older_than_minutes: int = 30) -> int:
    """Return runs orphaned in 'running' (e.g. a crashed worker) to the queue.

    A run that has been 'running' longer than ``older_than_minutes`` without
    completing was almost certainly abandoned by a dead process; reset it to
    'queued' so a worker picks it up again. Returns the number requeued.
    """
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE backtest_runs
        SET status = 'queued', started_at = NULL, progress = 0.0
        WHERE status = 'running'
          AND started_at IS NOT NULL
          AND started_at < NOW() - (%s || ' minutes')::interval
        """,
        (str(older_than_minutes),),
    )
    return cur.rowcount if cur.rowcount is not None else 0


def create_run(spec: BacktestSpec, *, end_user: Optional[str]) -> int:
    """Insert a queued ``backtest_runs`` row and return its id."""
    conn = get_db_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO backtest_runs
                (end_user, underlying, start_date, end_date, patterns, spec, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'queued')
            RETURNING id
            """,
            (
                end_user,
                spec.underlying,
                spec.start_date,
                spec.end_date,
                spec.patterns,
                json.dumps(spec.to_dict()),
            ),
        )
        run_id = int(cur.fetchone()[0])
        return run_id
    finally:
        close_db_connection(conn)


def _mark(conn, run_id: int, **fields) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k} = %s" for k in fields)
    cur = conn.cursor()
    cur.execute(
        f"UPDATE backtest_runs SET {cols} WHERE id = %s",
        (*fields.values(), run_id),
    )


def _persist_results(conn, run_id: int, result: RunResult) -> None:
    cur = conn.cursor()
    for t in result.trades:
        cur.execute(
            """
            INSERT INTO backtest_trades
                (run_id, seq, pattern, direction, tier, option_symbol, option_type,
                 strike, expiration, entered_at, exited_at, entry_premium, exit_premium,
                 contracts, gross_pnl, commission, net_pnl, return_pct, outcome,
                 mfe_pct, mae_pct, hold_minutes, structure, legs)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (
                run_id, t.seq, t.pattern, t.direction, t.tier, t.option_symbol,
                t.option_type, t.strike, t.expiration, t.entered_at, t.exited_at,
                t.entry_premium, t.exit_premium, t.contracts, t.gross_pnl, t.commission,
                t.net_pnl, t.return_pct, t.outcome, t.mfe_pct, t.mae_pct, t.hold_minutes,
                t.structure, json.dumps(t.legs),
            ),
        )
    for i, p in enumerate(result.equity, start=1):
        cur.execute(
            """
            INSERT INTO backtest_equity (run_id, seq, t, equity, drawdown_pct)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (run_id, i, p.t, p.equity, p.drawdown_pct),
        )


def execute_run(run_id: int) -> None:
    """Run the engine for ``run_id`` and persist results / status.

    Safe to invoke from a background thread. Any exception is captured onto the
    run row as ``status='failed'`` with the error message, never re-raised.
    """
    conn = get_db_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT spec FROM backtest_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if row is None:
            logger.error("execute_run: run %s not found", run_id)
            return
        spec = BacktestSpec.from_dict(row[0])

        _mark(conn, run_id, status="running", started_at=datetime.now(timezone.utc), progress=0.0)

        def _progress(frac: float) -> None:
            _mark(conn, run_id, progress=round(min(max(frac, 0.0), 1.0), 4))

        result = run_backtest(conn, spec, progress_cb=_progress)
        _persist_results(conn, run_id, result)
        _mark(
            conn, run_id,
            status="completed",
            progress=1.0,
            summary=json.dumps(result.summary),
            completed_at=datetime.now(timezone.utc),
        )
        logger.info("backtest run %s completed: %s trades", run_id, result.summary.get("n_trades"))
    except Exception as exc:  # noqa: BLE001 - surface to the run row, never crash the worker
        logger.exception("backtest run %s failed", run_id)
        try:
            _mark(
                conn, run_id,
                status="failed",
                error=str(exc)[:2000],
                completed_at=datetime.now(timezone.utc),
            )
        except Exception:  # pragma: no cover
            logger.exception("backtest run %s: failed to record failure", run_id)
    finally:
        close_db_connection(conn)
