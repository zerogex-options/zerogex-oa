"""Signal/trade APIs backed by unified signal tables."""

from __future__ import annotations

import math
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import DatabaseManager

router = APIRouter(prefix="/api/signals", tags=["Trade Signals"])


def _scale_signed_100(value: Any) -> Any:
    """Scale a signed metric into [-100, 100].

    Leaves non-numeric values unchanged. Supports legacy [-1, 1], [-10, 10],
    and already-scaled [-100, 100] inputs.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value

    raw = float(value)
    if math.isnan(raw) or math.isinf(raw):
        return 0.0

    if -1.0 <= raw <= 1.0:
        scaled = raw * 100.0
    elif -10.0 <= raw <= 10.0:
        scaled = raw * 10.0
    else:
        scaled = raw

    scaled = max(-100.0, min(100.0, scaled))
    if scaled == 0.0 and raw != 0.0:
        scaled = 0.0001 if raw > 0 else -0.0001
    return round(scaled, 4)


def _normalize_vol_expansion_components(value: Any) -> Any:
    """Recursively normalize vol-expansion component score fields to [-100, 100]."""
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key in {"score", "raw_score", "weighted_score", "normalized_score", "composite_score"}:
                out[key] = _scale_signed_100(item)
            else:
                out[key] = _normalize_vol_expansion_components(item)
        return out
    if isinstance(value, list):
        return [_normalize_vol_expansion_components(v) for v in value]
    return value


def _normalize_signal_components(value: Any) -> Any:
    """Scale unified-signal component score fields to [-100, 100]."""
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key == "score":
                out[key] = _scale_signed_100(item)
            else:
                out[key] = _normalize_signal_components(item)
        return out
    if isinstance(value, list):
        return [_normalize_signal_components(v) for v in value]
    return value


def _normalize_signal_score_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize consolidated signal-score payload to [-100, 100]."""
    out = dict(row)
    out["composite_score"] = _scale_signed_100(out.get("composite_score"))
    out["normalized_score"] = _scale_signed_100(out.get("normalized_score"))
    if "components" in out:
        out["components"] = _normalize_signal_components(out.get("components"))
    return out


def _normalize_vol_expansion_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)

    max_score = out.get("max_possible_score")
    composite = out.get("composite_score")
    try:
        max_score_f = float(max_score)
        composite_f = float(composite)
        if max_score_f > 0:
            out["composite_score"] = _scale_signed_100(composite_f / max_score_f)
            out["max_possible_score"] = 100.0
        else:
            out["composite_score"] = _scale_signed_100(composite_f)
    except (TypeError, ValueError):
        pass

    try:
        norm = float(out.get("normalized_score"))
        direction = str(out.get("expected_direction") or "").lower()
        sign = -1.0 if direction in {"down", "bearish", "short"} else 1.0 if direction in {"up", "bullish", "long"} else 0.0
        out["normalized_score"] = _scale_signed_100(abs(norm) * sign if sign else norm)
    except (TypeError, ValueError):
        pass

    if "components" in out:
        out["components"] = _normalize_vol_expansion_components(out.get("components"))

    return out


def get_db() -> DatabaseManager:
    from ..main import db_manager
    return db_manager


@router.get("/trades-history")
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


@router.get("/trades-live")
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
    row = await db.get_latest_signal_score_enriched(underlying.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No score rows found for {underlying.upper()}")
    return _normalize_signal_score_row(row)


@router.get("/score-history")
async def get_score_history(
    underlying: str = Query(default="SPY"),
    limit: int = Query(default=100, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    rows = await db.get_signal_score_history(underlying.upper(), limit)
    normalized_rows = [_normalize_signal_score_row(row) for row in rows]
    return {
        "underlying": underlying.upper(),
        "rows": normalized_rows,
        "count": len(normalized_rows),
    }


@router.get("/vol-expansion")
async def get_vol_expansion_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    row = await db.get_vol_expansion_signal(symbol.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No volatility expansion rows found for {symbol.upper()}")
    return _normalize_vol_expansion_row(row)
