"""Trade Bias API — backend-computed directional regime + playbook.

Companion to ``/api/signals/score`` (the 0-100 Market State Index): where MSI is
a directionless state magnitude, Trade Bias is a SIGNED directional call — which
way, how convinced, the regime it started from, and (from Phase 3) whether live
price-action/flow/tape/momentum overrode the gamma-based posture. It is computed
by the Signals Engine each cycle (``src/signals/trade_bias/``) and persisted to
``trade_bias_scores``; these endpoints read the latest / recent rows.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import DatabaseManager

router = APIRouter(prefix="/api/signals", tags=["Trade Bias"])

# 'swing' = multi-day / structural read; 'intraday' = same-day / 0DTE read
# (populated from Phase 4). '0dte' / 'zero_dte' are accepted aliases.
_VALID_TENORS = {"swing", "intraday"}
_TENOR_ALIASES = {"0dte": "intraday", "zero_dte": "intraday", "same_day": "intraday"}


def get_db() -> DatabaseManager:
    from ..main import db_manager

    assert db_manager is not None, "db_manager not initialized"
    return db_manager


def _normalize_tenor(tenor: str) -> str:
    t = (tenor or "swing").strip().lower()
    t = _TENOR_ALIASES.get(t, t)
    if t not in _VALID_TENORS:
        valid = sorted(_VALID_TENORS)
        aliases = sorted(_TENOR_ALIASES)
        raise HTTPException(
            status_code=422,
            detail=f"tenor must be one of {valid} (or an alias: {aliases})",
        )
    return t


def _payload_from_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return the stored contract, with the row's scalar columns authoritative.

    The full contract is persisted in ``payload``; the top-level columns exist
    for indexing/filtering. We overlay the columns so a sparse or older payload
    can never disagree with the indexed values.
    """
    payload = row.get("payload")
    out: dict[str, Any] = dict(payload) if isinstance(payload, dict) and payload else {}
    out["tenor"] = row.get("tenor", out.get("tenor"))
    out["bias_score"] = row.get("bias_score")
    out["direction"] = row.get("direction")
    out["state"] = row.get("state")
    out["confidence"] = row.get("confidence")
    ts = row.get("timestamp")
    if ts is not None:
        out["timestamp"] = ts.isoformat() if hasattr(ts, "isoformat") else ts
    return out


@router.get("/trade-bias")
async def get_trade_bias(
    underlying: str = Query(default="SPY"),
    tenor: str = Query(default="swing"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest directional Trade Bias for a symbol.

    **Params:** `underlying` (default SPY), `tenor` (`swing` | `intraday`).

    **Returns:** the bias contract — `bias_score` (signed −100..+100),
    `direction`, `state`, `confidence`, `regime`, `bias`, `setup`, `playbook`,
    `checklist`, `inputs`, `timestamp`. 404 when no bias has been computed yet.
    """
    t = _normalize_tenor(tenor)
    row = await db.get_latest_trade_bias(underlying.upper(), t)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No trade bias available for {underlying.upper()} (tenor={t})",
        )
    return _payload_from_row(row)


@router.get("/trade-bias-history")
async def get_trade_bias_history(
    underlying: str = Query(default="SPY"),
    tenor: str = Query(default="swing"),
    limit: int = Query(default=600, ge=1, le=5000),
    lookback_days: int = Query(default=4, ge=1, le=30),
    db: DatabaseManager = Depends(get_db),
):
    """Recent Trade Bias history (newest first), for the intraday chart.

    **Params:** `underlying`, `tenor`, `limit` (1–5000, default 600),
    `lookback_days` (1–30, default 4). **Returns:** an array of the same
    contract objects as `/trade-bias`, ordered `timestamp DESC`.
    """
    t = _normalize_tenor(tenor)
    rows = await db.get_trade_bias_history(
        underlying.upper(), t, limit=limit, lookback_days=lookback_days
    )
    return [_payload_from_row(r) for r in rows]
