"""Consolidated dealer-positioning levels — the versioned public contract.

``GET /api/v1/levels/{symbol}``

A single, stable snapshot of the four headline dealer-positioning levels
(gamma flip, call wall, put wall, max pain) plus the per-strike gamma
profile — the exact primitives a third-party charting integration overlays
on a price chart (the TradingView Charting Library widget, a NinjaScript
indicator, an embeddable partner widget).

Why this exists alongside the internal ``/api/gex/*`` surface:

* **Versioned** (``/api/v1/``).  The field names here are a committed
  contract external consumers code against; the internal ``/api/gex/*``
  models can churn without breaking integrations.
* **Consolidated.**  One round-trip returns what previously required
  stitching ``/api/gex/summary`` (flip + walls + max pain) and
  ``/api/gex/by-strike`` (the profile) — and it aggregates the profile
  across expirations so it shares the walls' cross-expiration basis.
* **Derived-only, ``gex``-scoped.**  No raw per-contract quotes, so it
  belongs to the broadly-redistributable ``analytics`` tier (see
  ``src/api/scopes.py``); ``market_raw`` is never touched here.
* **Freshness-aware.**  ``as_of`` + ``age_seconds`` let a consumer — or a
  future delayed-vs-live tier gate — reason about staleness explicitly.
  The metrics refresh on the ~60s analytics cycle (see the caches on
  ``DatabaseManager``); this is snapshot-poll, not a stream.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel

from ..database import DatabaseManager
from ..errors import handle_api_errors

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/levels", tags=["Levels (v1)"])


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


def get_db() -> DatabaseManager:
    from ..main import db_manager

    assert db_manager is not None, "db_manager not initialized"
    return db_manager


# ---------------------------------------------------------------------------
# Response models — the committed v1 contract
# ---------------------------------------------------------------------------


class StrikeGamma(BaseModel):
    """One bar of the per-strike gamma profile (dollar GEX per 1% move).

    Aggregated across expirations; ``net_gex == call_gex + put_gex`` by
    construction. Call exposure is signed positive and put exposure
    negative under the traditional modeled dealer-positioning convention
    (dealers modeled net long calls / net short puts). This is a modeled
    sign convention, not a direct observation of dealer inventory.
    """

    strike: float
    net_gex: float
    call_gex: float
    put_gex: float


class DealerLevels(BaseModel):
    """The four headline horizontal levels.

    Any field may be ``null`` when the analytics engine could not resolve
    it for the latest snapshot (e.g. an unresolved gamma flip on a thin
    chain) — consumers should hide, not zero, a null level.
    """

    gamma_flip: Optional[float] = None
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    max_pain: Optional[float] = None


class LevelsResponse(BaseModel):
    symbol: str
    spot: Optional[float] = None
    as_of: datetime
    age_seconds: Optional[int] = None
    net_gex_at_spot: Optional[float] = None
    levels: DealerLevels
    profile: List[StrikeGamma]


def _maybe_float(value: object) -> Optional[float]:
    """Coerce a DB ``Decimal``/number to ``float``; pass ``None`` through."""
    return float(value) if value is not None else None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get("/{symbol}", response_model=LevelsResponse)
@handle_api_errors("GET /api/v1/levels/{symbol}")
async def get_levels(
    symbol: str = Path(
        ...,
        min_length=1,
        max_length=16,
        pattern=r"^[A-Za-z0-9.^-]+$",
        description="Underlying symbol, e.g. SPY, SPX, QQQ.",
    ),
    strikes: int = Query(
        default=40,
        ge=1,
        le=200,
        description=(
            "Number of strikes nearest to spot to include in the gamma "
            "profile (aggregated across expirations)."
        ),
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Consolidated dealer-positioning levels for one underlying.

    Returns the latest gamma flip, call/put walls and max pain, plus the
    per-strike gamma profile nearest to spot — the derived, redistributable
    primitives a charting integration draws as horizontal lines and a
    histogram.  ``404`` when no snapshot exists for ``symbol`` yet.
    """
    sym = symbol.upper()

    summary = await db.get_latest_gex_summary(sym)
    if not summary:
        raise HTTPException(
            status_code=404,
            detail=f"No dealer-positioning levels available for {sym}",
        )

    strike_rows = await db.get_latest_strike_gamma_profile(sym, strikes)
    # DB returns nearest-to-spot first; render ascending by strike so the
    # histogram reads left-to-right in price order.
    profile = sorted(
        (
            StrikeGamma(
                strike=float(row["strike"]),
                net_gex=float(row["net_gex"]),
                call_gex=float(row["call_gex"]),
                put_gex=float(row["put_gex"]),
            )
            for row in strike_rows
        ),
        key=lambda bar: bar.strike,
    )

    as_of = summary["timestamp"]
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    age_seconds = max(0, int((datetime.now(timezone.utc) - as_of).total_seconds()))

    return LevelsResponse(
        symbol=sym,
        spot=_maybe_float(summary.get("spot_price")),
        as_of=as_of,
        age_seconds=age_seconds,
        net_gex_at_spot=_maybe_float(summary.get("net_gex_at_spot")),
        levels=DealerLevels(
            gamma_flip=_maybe_float(summary.get("gamma_flip")),
            call_wall=_maybe_float(summary.get("call_wall")),
            put_wall=_maybe_float(summary.get("put_wall")),
            max_pain=_maybe_float(summary.get("max_pain")),
        ),
        profile=profile,
    )
