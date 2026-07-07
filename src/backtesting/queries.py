"""Synchronous read helpers for the backtest API.

Kept separate from ``runner.py`` (which owns the write lifecycle). All return
plain JSON-serializable dicts so the async router can hand them straight to
FastAPI after an ``asyncio.to_thread`` hop.
"""

from __future__ import annotations

from typing import Optional

from src.database.connection import db_connection


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
    with db_connection() as conn:
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


def list_runs(*, end_user: Optional[str], limit: int = 25) -> list[dict]:
    """Recent runs for this end-user (plus anonymous runs when unauthenticated)."""
    with db_connection() as conn:
        cur = conn.cursor()
        # Sweep child runs (sweep_id NOT NULL) are surfaced via the sweep grid,
        # not the standalone Recent Runs list — filter them out here.
        if end_user:
            cur.execute(
                f"SELECT {_RUN_COLS} FROM backtest_runs "
                "WHERE end_user = %s AND sweep_id IS NULL "
                "ORDER BY created_at DESC LIMIT %s",
                (end_user, limit),
            )
        else:
            cur.execute(
                f"SELECT {_RUN_COLS} FROM backtest_runs "
                "WHERE end_user IS NULL AND sweep_id IS NULL "
                "ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
        return [_row_to_run(r, include_spec=False) for r in cur.fetchall()]


def get_trades(run_id: int, *, limit: int = 100, offset: int = 0) -> dict:
    with db_connection() as conn:
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


def get_equity(run_id: int) -> list[dict]:
    with db_connection() as conn:
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


# ---------------------------------------------------------------------------
# Pattern insights (the leaderboard read side)
# ---------------------------------------------------------------------------


def _derive_pattern_economics(
    n_resolved: int,
    n_wins: int,
    n_losses: int,
    gross_win_pnl: float | None,
    gross_loss_pnl: float | None,
) -> dict:
    """Compute PF / expectancy / net / avg win/loss from the persisted counts
    and dollar economics. Returns None for every field when the gross-pnl
    inputs are missing (touch-source rows persist NULL for these), and None
    for ``profit_factor`` when there are no losses (PF is undefined, not ∞).
    """
    if gross_win_pnl is None or gross_loss_pnl is None:
        return {
            "net_pnl": None, "profit_factor": None, "expectancy": None,
            "avg_win_pnl": None, "avg_loss_pnl": None,
        }
    gw = float(gross_win_pnl)
    gl = float(gross_loss_pnl)
    return {
        "net_pnl": gw - gl,
        "profit_factor": (gw / gl) if gl > 0 else None,
        "expectancy": ((gw - gl) / n_resolved) if n_resolved > 0 else None,
        "avg_win_pnl": (gw / n_wins) if n_wins > 0 else None,
        "avg_loss_pnl": (gl / n_losses) if n_losses > 0 else None,
    }


_VALID_INSIGHT_SOURCES = ("option_pnl", "underlying_touch")


def get_pattern_insights(
    *, source: str = "option_pnl", underlying: Optional[str] = None,
) -> list[dict]:
    """Latest stats row per (pattern, underlying) for the leaderboard.

    Returns one row per pair, ordered by net_pnl DESC and then by sample size
    DESC — the engine-server's opinion of "most profitable, then most
    trustworthy." The frontend can re-sort however it wants. Rows include the
    raw counts + dollar economics from the table, plus derived PF / expectancy
    / avg win / avg loss so the page doesn't need to recompute them.
    """
    if source not in _VALID_INSIGHT_SOURCES:
        source = "option_pnl"
    where = "WHERE source = %s"
    params: list = [source]
    if underlying:
        where += " AND underlying = %s"
        params.append(underlying.upper())
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT ON (pattern, underlying)
                   pattern, underlying, window_start, window_end,
                   n_emitted, n_resolved, n_target_hit, n_stop_hit,
                   hit_rate, proposed_base,
                   gross_win_pnl, gross_loss_pnl,
                   source, computed_at
            FROM playbook_pattern_stats
            {where}
            ORDER BY pattern, underlying, window_end DESC, computed_at DESC
            """,
            params,
        )
        rows = cur.fetchall()
    out: list[dict] = []
    for r in rows:
        (
            pattern, underlying_, window_start, window_end,
            n_emitted, n_resolved, n_wins, n_losses,
            hit_rate, proposed_base, gross_win_pnl, gross_loss_pnl,
            row_source, computed_at,
        ) = r
        econ = _derive_pattern_economics(
            int(n_resolved or 0),
            int(n_wins or 0),
            int(n_losses or 0),
            gross_win_pnl,
            gross_loss_pnl,
        )
        out.append({
            "pattern": pattern,
            "underlying": underlying_,
            "window_start": window_start.isoformat() if window_start else None,
            "window_end": window_end.isoformat() if window_end else None,
            "n_emitted": int(n_emitted or 0),
            "n_resolved": int(n_resolved or 0),
            "n_wins": int(n_wins or 0),
            "n_losses": int(n_losses or 0),
            "hit_rate": float(hit_rate) if hit_rate is not None else None,
            "proposed_base": float(proposed_base) if proposed_base is not None else None,
            "gross_win_pnl": (
                float(gross_win_pnl) if gross_win_pnl is not None else None
            ),
            "gross_loss_pnl": (
                float(gross_loss_pnl) if gross_loss_pnl is not None else None
            ),
            "source": row_source,
            "computed_at": computed_at.isoformat() if computed_at else None,
            **econ,
        })
    # Server-side default ordering: net_pnl desc (NULLs last), then n_resolved
    # desc. Stable enough that the client can show a usable view before it
    # re-sorts.

    def _sort_key(d: dict) -> tuple:
        net = d.get("net_pnl")
        return (
            0 if net is not None else 1,
            -(net if net is not None else 0.0),
            -(d.get("n_resolved") or 0),
        )

    out.sort(key=_sort_key)
    return out
