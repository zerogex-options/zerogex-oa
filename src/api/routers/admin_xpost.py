"""Admin HTTP endpoints backing the website's ``/admin/x-post`` review page.

The website BFF (never a browser directly) calls these server-to-server with
its API key *and* the ``X-Admin-Token`` shared secret; the whole router sits
behind :func:`security.require_admin`, so every route needs the admin token in
addition to the global API-key auth.

Endpoints (prefix ``/api/admin/x-post``):

* ``GET  /symbols``     — the configured ticker list + default for the
  regenerate dropdown (``$BULLETIN_TWEET_SYMBOLS`` / SPY).
* ``GET  /latest``      — the last auto-generated post + reply for a
  (timing, symbol).  Timing defaults to the current wall-clock ET slot
  (Morning / Midday / Post-Market).  ``record`` is null when nothing has
  been generated yet for that slot.
* ``POST /regenerate``  — generate a fresh post+reply for (symbol, mode)
  on demand, persist it as the new latest, and return it.  Never posts to X
  and skips media rendering (the scheduled job owns that).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.api.database import DatabaseManager
from src.api.security import require_admin
from src.jobs import bulletin_tweet as bt

logger = logging.getLogger(__name__)

# The whole router is admin-gated. require_admin fails closed (503) when
# KEY_ADMIN_TOKEN is unset and 403s on a missing/incorrect X-Admin-Token.
router = APIRouter(
    prefix="/api/admin/x-post",
    tags=["Admin"],
    dependencies=[Depends(require_admin)],
)


def get_db() -> DatabaseManager:
    # Import lazily (mirrors the other routers) so the module imports cleanly
    # before the lifespan has constructed db_manager.
    from src.api.main import db_manager

    if db_manager is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return db_manager


def _envelope(symbol: str, mode: str, record: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "mode": mode,
        "timing_label": bt.MODE_READ_LABEL.get(mode, "Market Read"),
        "record": record,
    }


@router.get("/symbols")
async def symbols() -> Dict[str, Any]:
    """Configured tickers + default for the regenerate dropdown."""
    syms, default = bt.configured_symbols()
    return {"symbols": syms, "default": default}


@router.get("/latest")
async def latest(
    symbol: Optional[str] = Query(default=None, max_length=16),
    mode: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """The last auto-generated record for (symbol, timing).

    ``symbol`` defaults to the configured default; ``mode`` defaults to the
    current wall-clock ET timing.  On the initial load (no explicit
    ``symbol``), if the default symbol has no record we fall back to the most
    recent scheduled run for this timing regardless of which symbol it
    featured — so the page always pre-fills from the last background run.
    ``record`` is null only when nothing at all has been generated for that
    slot (the page then offers Regenerate)."""
    _, default_symbol = bt.configured_symbols()
    explicit_symbol = bool(symbol)
    sym = (symbol or default_symbol).upper()
    resolved_mode = mode or bt.resolve_current_mode()
    if resolved_mode not in bt.MODES:
        raise HTTPException(status_code=400, detail=f"unknown mode {resolved_mode!r}")
    record = bt.read_latest_record(sym, resolved_mode)
    if record is None and not explicit_symbol:
        fallback = bt.read_latest_record_any(resolved_mode)
        if fallback is not None:
            fb_symbol = str(fallback.get("symbol") or sym).upper()
            return _envelope(fb_symbol, resolved_mode, fallback)
    return _envelope(sym, resolved_mode, record)


class RegenerateRequest(BaseModel):
    symbol: Optional[str] = Field(default=None, max_length=16)
    mode: Optional[str] = None


@router.post("/regenerate")
async def regenerate(
    body: RegenerateRequest, db: DatabaseManager = Depends(get_db)
) -> Dict[str, Any]:
    """Generate a fresh post+reply for (symbol, mode) and store it as latest."""
    syms, default_symbol = bt.configured_symbols()
    sym = (body.symbol or default_symbol).upper()
    if sym not in syms:
        raise HTTPException(
            status_code=400,
            detail=f"symbol {sym!r} is not configured; choose from {syms}",
        )
    resolved_mode = body.mode or bt.resolve_current_mode()
    if resolved_mode not in bt.MODES:
        raise HTTPException(status_code=400, detail=f"unknown mode {resolved_mode!r}")
    try:
        record = await bt.generate_and_store(db, resolved_mode, sym)
    except Exception as exc:  # noqa: BLE001 — surface as a clean 502
        logger.warning("admin_xpost: regenerate failed (%s)", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="post generation failed") from exc
    return _envelope(sym, resolved_mode, record)
