"""Synchronous read helpers for the backtest API.

Kept separate from ``runner.py`` (which owns the write lifecycle). All return
plain JSON-serializable dicts so the async router can hand them straight to
FastAPI after an ``asyncio.to_thread`` hop.
"""

from __future__ import annotations

from typing import Optional

from src.database.connection import close_db_connection, get_db_connection


def _row_to_run(row, *, include_spec: bool) -> dict:
    out = {
        "run_id": int(row[0]),
        "underlying": row[1],
        "start_date": row[2].isoformat() if row[2] else None,
        "end_date": row[3].isoformat() if row[3] else None,
        "status": row[4],
        "progress": float(row[5]) if row[5] is not None else 0.0,
        "summary": row[6],
        "error": row[7],
        "created_at": row[8].isoformat() if row[8] else None,
        "completed_at": row[9].isoformat() if row[9] else None,
    }
    if include_spec:
        out["spec"] = row[10]
    return out


_RUN_COLS = (
    "id, underlying, start_date, end_date, status, progress, summary, error, "
    "created_at, completed_at"
)


def get_run(run_id: int, *, end_user: Optional[str]) -> Optional[dict]:
    """Fetch one run's status + summary, scoped to its owner.

    A run with a NULL ``end_user`` (anonymous) is readable by anyone; a run
    owned by an end-user is only readable by that same end-user.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {_RUN_COLS}, spec, end_user FROM backtest_runs WHERE id = %s",
            (run_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        owner = row[11]  # end_user is the 12th selected column
        if owner is not None and owner != end_user:
            return None
        return _row_to_run(row, include_spec=True)
    finally:
        close_db_connection(conn)


def list_runs(*, end_user: Optional[str], limit: int = 25) -> list[dict]:
    """Recent runs for this end-user (plus anonymous runs when unauthenticated)."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        if end_user:
            cur.execute(
                f"SELECT {_RUN_COLS} FROM backtest_runs "
                "WHERE end_user = %s ORDER BY created_at DESC LIMIT %s",
                (end_user, limit),
            )
        else:
            cur.execute(
                f"SELECT {_RUN_COLS} FROM backtest_runs "
                "WHERE end_user IS NULL ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
        return [_row_to_run(r, include_spec=False) for r in cur.fetchall()]
    finally:
        close_db_connection(conn)


def get_trades(run_id: int, *, limit: int = 100, offset: int = 0) -> dict:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM backtest_trades WHERE run_id = %s", (run_id,))
        total = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT seq, pattern, direction, tier, option_symbol, option_type,
                   strike, expiration, entered_at, exited_at, entry_premium,
                   exit_premium, contracts, net_pnl, return_pct, outcome, hold_minutes,
                   structure, legs, net_delta, net_vega
            FROM backtest_trades
            WHERE run_id = %s
            ORDER BY seq
            LIMIT %s OFFSET %s
            """,
            (run_id, limit, offset),
        )
        trades = [
            {
                "seq": r[0],
                "pattern": r[1],
                "direction": r[2],
                "tier": r[3],
                "option_symbol": r[4],
                "option_type": r[5],
                "strike": float(r[6]) if r[6] is not None else None,
                "expiration": r[7].isoformat() if r[7] else None,
                "entered_at": r[8].isoformat() if r[8] else None,
                "exited_at": r[9].isoformat() if r[9] else None,
                "entry_premium": float(r[10]) if r[10] is not None else None,
                "exit_premium": float(r[11]) if r[11] is not None else None,
                "contracts": r[12],
                "net_pnl": float(r[13]) if r[13] is not None else None,
                "return_pct": float(r[14]) if r[14] is not None else None,
                "outcome": r[15],
                "hold_minutes": r[16],
                "structure": r[17],
                "legs": r[18] if r[18] is not None else [],
                "net_delta": float(r[19]) if r[19] is not None else 0.0,
                "net_vega": float(r[20]) if r[20] is not None else 0.0,
            }
            for r in cur.fetchall()
        ]
        return {"trades": trades, "total": total}
    finally:
        close_db_connection(conn)


def get_equity(run_id: int) -> list[dict]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT t, equity, drawdown_pct FROM backtest_equity "
            "WHERE run_id = %s ORDER BY seq",
            (run_id,),
        )
        return [
            {
                "t": r[0].isoformat() if r[0] else None,
                "equity": float(r[1]) if r[1] is not None else None,
                "drawdown_pct": float(r[2]) if r[2] is not None else None,
            }
            for r in cur.fetchall()
        ]
    finally:
        close_db_connection(conn)
