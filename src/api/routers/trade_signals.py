"""Signal/trade APIs backed by unified signal tables."""

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import DatabaseManager

router = APIRouter(prefix="/api/signals", tags=["Trade Signals"])


def get_db() -> DatabaseManager:
    from ..main import db_manager
    return db_manager


@router.get("/history")
async def get_signal_history(
    limit: int = Query(default=500, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    rows = await db.get_closed_signal_trades(limit=limit)
    total_pnl = round(sum(float(r.get("total_pnl") or 0) for r in rows), 4)
    wins = sum(1 for r in rows if r.get("outcome") == "win")
    return {
        "trades": rows,
        "summary": {
            "total_trades": len(rows),
            "wins": wins,
            "losses": sum(1 for r in rows if r.get("outcome") == "loss"),
            "win_rate": round(wins / len(rows), 4) if rows else None,
            "total_pnl": total_pnl,
        },
    }


@router.get("/live")
async def get_live_signals(db: DatabaseManager = Depends(get_db)):
    rows = await db.get_live_signal_trades()
    return {
        "trades": rows,
        "count": len(rows),
    }


@router.get("/score")
async def get_latest_score(
    underlying: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    row = await db.get_latest_signal_score(underlying.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No score rows found for {underlying.upper()}")
    return row


@router.get("/score-history")
async def get_score_history(
    underlying: str = Query(default="SPY"),
    limit: int = Query(default=100, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    rows = await db.get_signal_score_history(underlying.upper(), limit)
    return {
        "underlying": underlying.upper(),
        "rows": rows,
        "count": len(rows),
    }


@router.get("/vol-expansion")
async def get_vol_expansion_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    row = await db.get_vol_expansion_from_scores(symbol.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No score rows found for {symbol.upper()}")
    return row
