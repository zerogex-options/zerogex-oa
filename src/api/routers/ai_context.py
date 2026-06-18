"""AI market-context endpoint.

A single, compact, LLM-ready snapshot of the current market structure for
one underlying. It is the canonical grounding payload for the ZeroGEX AI
copilot (web) and the public MCP server: both call this so every answer is
backed by the same live, derived analytics rather than a model's training
priors.

Everything returned here is **derived analytics** (computed GEX, max pain,
the Market State Index, and the playbook Action Card) plus a calibration
narrative — never raw, license-restricted market data. The endpoint is
therefore gated on the :data:`~src.api.scopes.SIGNALS` scope (the
derived-analytics tier that already bundles GEX/flow/max-pain), and an
external customer key never needs ``market_raw`` to use it.

The payload is assembled by reusing the same ``DatabaseManager`` accessors
and ``PlaybookEngine`` that power ``/api/gex/summary``, ``/api/signals/score``
and ``/api/signals/action`` — it computes nothing new, it only composes.
Each section degrades to ``None`` independently so a partial snapshot (e.g.
MSI present but no Action Card yet) still returns 200 with whatever is
available, which is what an LLM grounding call wants.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import DatabaseManager
from ..scopes import SIGNALS
from ..security import require_scopes
from .trade_signals import (
    _get_playbook_engine,
    _normalize_signal_score_row,
    get_db,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/ai",
    tags=["AI"],
    dependencies=[Depends(require_scopes(SIGNALS))],
)

# Headline GEX-summary fields worth handing an LLM. Kept to the decision-
# relevant subset (spot + the structural levels) so the grounding payload
# stays compact; the full summary remains available at /api/gex/summary.
_MARKET_STATE_FIELDS = (
    "timestamp",
    "spot_price",
    "net_gex",
    "net_gex_at_spot",
    "gamma_flip",
    "flip_distance",
    "max_pain",
    "call_wall",
    "put_wall",
    "put_call_ratio",
)


def _to_jsonable(value: Any) -> Any:
    """Coerce DB-native types (Decimal/datetime) to JSON-friendly values."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _regime_hint(composite_score: Optional[float]) -> Optional[str]:
    """Map the MSI composite to its documented regime band.

    Same thresholds as the ``/api/signals/score`` docstring, surfaced as a
    short tag so the LLM doesn't have to re-derive the interpretation.
    """
    if composite_score is None:
        return None
    if composite_score >= 70:
        return "trend_expansion"
    if composite_score >= 40:
        return "controlled_trend"
    if composite_score >= 20:
        return "chop_range"
    return "high_risk_reversal"


@router.get("/context")
async def get_market_context(
    underlying: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Compact, grounded market-structure snapshot for one underlying.

    Composes three live, derived readings into one payload:

    * ``market_state`` — headline GEX structure (spot, net GEX, gamma flip,
      call/put walls, max pain, put/call ratio) from the latest analytics
      cycle (same source as ``/api/gex/summary``).
    * ``msi`` — the Market State Index composite + component breakdown, plus
      a ``regime_hint`` band (same source as ``/api/signals/score``).
    * ``action_card`` — the latest Playbook Action Card, the single decisive
      trade instruction (or an earned ``STAND_DOWN``); same engine as
      ``/api/signals/action``.

    Each section is independent: a missing piece becomes ``null`` rather than
    failing the whole call. A 404 is returned only when *nothing* is
    available for the symbol (no GEX summary and no MSI), which means the
    symbol is unknown or analytics haven't produced a first reading yet.

    **Params:** ``underlying`` (default ``SPY``).

    The ``disclaimer`` field is intentionally part of the payload so any
    downstream LLM has the not-financial-advice framing in-context.
    """
    sym = underlying.upper()

    market_state: Optional[Dict[str, Any]] = None
    try:
        summary = await db.get_latest_gex_summary(sym)
        if summary:
            market_state = {key: _to_jsonable(summary.get(key)) for key in _MARKET_STATE_FIELDS}
    except Exception:
        logger.warning("ai/context: GEX summary fetch failed for %s", sym, exc_info=True)

    msi: Optional[Dict[str, Any]] = None
    try:
        score_row = await db.get_latest_signal_score_enriched(sym)
        if score_row:
            msi = _normalize_signal_score_row(score_row)
            msi["regime_hint"] = _regime_hint(msi.get("composite_score"))
    except Exception:
        logger.warning("ai/context: MSI fetch failed for %s", sym, exc_info=True)

    if market_state is None and msi is None:
        raise HTTPException(
            status_code=404,
            detail=f"No market context available for {sym}",
        )

    action_card: Optional[Dict[str, Any]] = None
    try:
        from src.signals.playbook.context_builder import build_playbook_context

        ctx = await build_playbook_context(db=db, underlying=sym)
        if ctx is not None:
            card = _get_playbook_engine().evaluate(ctx)
            action_card = card.to_dict()
    except Exception:
        # The Action Card is a best-effort enrichment; never let it 500 the
        # grounding call. The copilot/MCP still get market_state + msi.
        logger.warning("ai/context: action card build failed for %s", sym, exc_info=True)

    return {
        "symbol": sym,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_state": market_state,
        "msi": msi,
        "action_card": action_card,
        "disclaimer": (
            "ZeroGEX provides options-market analytics for informational "
            "purposes only. This is not financial advice or a recommendation "
            "to buy or sell any security. Trading options involves substantial "
            "risk of loss."
        ),
    }
