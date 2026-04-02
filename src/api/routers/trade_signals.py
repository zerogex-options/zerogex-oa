"""Signal endpoints backed by decoupled signal_scores/signal_trades tables."""

from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/signals", tags=["Trade Signals"])

STALE_THRESHOLD_SECONDS = 600


def get_db() -> DatabaseManager:
    from ..main import db_manager
    return db_manager


@router.get("/live")
async def get_live_signals(db: DatabaseManager = Depends(get_db)):
    """Return all live/open signal-trade rows across all symbols."""
    rows = await db.get_live_signal_trades()
    return {
        "count": len(rows),
        "signals": rows,
    }


@router.get("/history")
async def get_signal_history(
    limit: int = Query(default=500, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    """Return all closed signals/trades across all symbols."""
    rows = await db.get_closed_signal_trades(limit=limit)
    total_pnl = round(sum(float(r.get("total_pnl") or 0) for r in rows), 2)
    wins = sum(1 for r in rows if r.get("outcome") == "win")
    losses = sum(1 for r in rows if r.get("outcome") == "loss")
    return {
        "count": len(rows),
        "signals": rows,
        "summary": {
            "total_trades": len(rows),
            "wins": wins,
            "losses": losses,
            "win_rate": round((wins / len(rows)), 4) if rows else None,
            "total_pnl": total_pnl,
        },
    }


@router.get("/score")
async def get_signal_score(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    db: DatabaseManager = Depends(get_db),
):
    """Return most recent score snapshot + full component breakdown."""
    row = await db.get_latest_signal_score(symbol)
    if not row:
        raise HTTPException(status_code=404, detail=f"No score found for {symbol}")

    ts: datetime = row["timestamp"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - ts).total_seconds()
    if age_seconds > STALE_THRESHOLD_SECONDS:
        logger.warning(
            "Score for %s is %.0fs old (threshold: %ss)",
            symbol,
            age_seconds,
            STALE_THRESHOLD_SECONDS,
        )
    return row


@router.get("/score-history")
async def get_signal_score_history(
    limit: int = Query(default=100, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    """Return latest score history rows across all symbols."""
    rows = await db.get_signal_scores_history(limit=limit)
    return {
        "count": len(rows),
        "scores": rows,
    }


@router.get("/vol-expansion")
async def get_vol_expansion_signal(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    db: DatabaseManager = Depends(get_db),
):
    """Return integrated vol-expansion view derived from the new score system."""
    row = await db.get_vol_expansion_signal(symbol)
    if not row:
        raise HTTPException(status_code=404, detail=f"No vol-expansion view found for {symbol}")
    return row
