"""
Multi-horizon Gamma Flip Router (MOCK)

GET /api/gex/flip-term-structure?symbol=SPX&horizons=1,3,5,10,20,60

Returns today's gamma flip resolved at each requested multi-day horizon
(by substituting that horizon for ``GAMMA_PROFILE_DTE_REF_DAYS`` in the
horizon-occupancy weight), alongside the persisted flip from the same
number of days ago for a quick "was this horizon's flip predictive?"
calibration overlay.

Status: PROTOTYPE.  This endpoint computes on-demand and runs the
analytics-engine snapshot fetch in a worker thread; the per-cycle cost
is N * the cost of the steady-state flip resolution (one re-greeked
profile per horizon).  Production deployment should pre-compute and
persist per-horizon flips in ``gex_summary`` (or a sibling table) so
this endpoint becomes a cheap read.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.analytics.main_engine import AnalyticsEngine

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gex", tags=["GEX"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HorizonFlipPoint(BaseModel):
    """One point on the term-structure curve.

    Units: ``flip`` in the underlying's price unit (USD for SPX/SPY).
    ``span_used`` is a fraction of spot (0.20 = the resolver's default
    ±20% scan).  ``net_gex_at_spot`` is dollar gamma per 1% move in the
    same convention the persisted ``gex_summary.net_gex_at_spot`` uses
    (calls +, puts −).
    """

    horizon_days: float
    flip: Optional[float] = None
    resolved: bool
    span_used: float
    net_gex_at_spot: Optional[float] = None


class HistoricalRealization(BaseModel):
    """Persisted flip from (now - offset_days), for the calibration overlay.

    NOTE: persisted rows were written with the production
    ``GAMMA_PROFILE_DTE_REF_DAYS`` (single scalar per cycle), so this
    is "what the production flip looked like h days ago" rather than
    "what the h-day-horizon flip looked like h days ago".  The two
    coincide today when ``horizon_days`` == production constant (5d by
    default); diverge otherwise.  The endpoint returns the row anyway
    so the frontend can label and surface this caveat.
    """

    horizon_days: float
    realized_at: Optional[datetime] = None  # actual gex_summary.timestamp
    target_at: datetime  # the (now - h) instant we anchored to
    flip: Optional[float] = None
    span_used: Optional[float] = None
    skew_seconds: Optional[float] = None  # realized_at - target_at, seconds


class FlipTermStructureResponse(BaseModel):
    symbol: str
    spot: float
    timestamp: datetime  # snapshot timestamp the curve was computed on
    horizons_days: List[float]
    curve: List[HorizonFlipPoint]
    historical: List[HistoricalRealization]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() if v is not None else None}


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def get_db() -> DatabaseManager:
    from ..main import db_manager

    return db_manager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_horizons(raw: str) -> List[float]:
    """Parse a comma-separated horizon list (in days).

    Bounds [0.25, 365] — a quarter-day is the floor (below that the
    grid step starts dominating the weight ramp); a year is the ceiling
    (anything longer is effectively unweighted).  Sorted ascending,
    de-duped, capped at 12 horizons to bound the per-request compute.
    """
    horizons: List[float] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            v = float(item)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"horizons must be numeric, got {item!r}")
        if not (0.25 <= v <= 365.0):
            raise HTTPException(
                status_code=400,
                detail=f"horizon {v} outside [0.25, 365] days",
            )
        horizons.append(v)
    horizons = sorted(set(horizons))
    if not horizons:
        raise HTTPException(status_code=400, detail="horizons cannot be empty")
    if len(horizons) > 12:
        raise HTTPException(status_code=400, detail="at most 12 horizons per request")
    return horizons


def _compute_sync(symbol: str, horizons: List[float]) -> Optional[dict]:
    """Run the analytics-engine snapshot fetch + multi-horizon flip
    resolution.  Synchronous (psycopg2 + numpy); call via
    ``asyncio.to_thread`` from the async endpoint."""
    engine = AnalyticsEngine(underlying=symbol)
    # _get_snapshot is the same path the production analytics loop uses
    # for the persisted cycle — same options filter, same AM-settled drop,
    # same OI-coverage guard.  Underscore is a soft convention here.
    snapshot = engine._get_snapshot()
    if not snapshot or not snapshot.get("options"):
        return None
    curve = engine.compute_flip_term_structure(
        snapshot["options"],
        snapshot["underlying_price"],
        snapshot["timestamp"],
        horizons,
    )
    return {
        "spot": snapshot["underlying_price"],
        "timestamp": snapshot["timestamp"],
        "curve": curve,
    }


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get("/flip-term-structure", response_model=FlipTermStructureResponse)
async def get_flip_term_structure(
    symbol: str = Query(default="SPX", description="Underlying symbol"),
    horizons: str = Query(
        default="1,3,5,10,20,60",
        description=(
            "Comma-separated list of multi-day reference horizons in days. "
            "Each value substitutes for GAMMA_PROFILE_DTE_REF_DAYS for one "
            "resolve. Bounds: [0.25, 365], at most 12 entries."
        ),
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Multi-horizon gamma flip + historical-realization overlay (PROTOTYPE)."""
    horizons_list = _parse_horizons(horizons)
    symbol_upper = symbol.upper()

    # Heavy work (DB snapshot fetch + N profile builds) is sync and CPU/IO
    # bound — run it off the event loop so concurrent requests don't queue.
    try:
        sync_result = await asyncio.to_thread(_compute_sync, symbol_upper, horizons_list)
    except Exception as e:
        logger.error(
            "flip-term-structure compute failed for %s: %s", symbol_upper, e, exc_info=True
        )
        raise HTTPException(status_code=500, detail="flip-term-structure compute failed")

    if sync_result is None:
        raise HTTPException(status_code=404, detail=f"No usable option snapshot for {symbol_upper}")

    spot = float(sync_result["spot"])
    snapshot_ts = sync_result["timestamp"]
    if snapshot_ts.tzinfo is None:
        snapshot_ts = snapshot_ts.replace(tzinfo=timezone.utc)

    curve_points = [HorizonFlipPoint(**row) for row in sync_result["curve"]]

    # Historical realization overlay — independent async query.
    try:
        hist_rows = await db.get_historical_flips_at_offsets(symbol_upper, horizons_list)
    except Exception as e:
        # The curve is the headline product; degrade gracefully if the
        # overlay query fails (typical cause: gex_summary empty for the
        # window, e.g. a freshly-seeded DB).
        logger.warning("historical-flips overlay failed for %s: %s", symbol_upper, e)
        hist_rows = []

    historical: List[HistoricalRealization] = []
    for row in hist_rows:
        target_at = row["target_ts"]
        if target_at is not None and target_at.tzinfo is None:
            target_at = target_at.replace(tzinfo=timezone.utc)
        realized_at = row["realized_ts"]
        if realized_at is not None and realized_at.tzinfo is None:
            realized_at = realized_at.replace(tzinfo=timezone.utc)
        historical.append(
            HistoricalRealization(
                horizon_days=float(row["offset_days"]),
                realized_at=realized_at,
                target_at=target_at,
                flip=float(row["flip"]) if row["flip"] is not None else None,
                span_used=(
                    float(row["gamma_flip_span_used"])
                    if row["gamma_flip_span_used"] is not None
                    else None
                ),
                skew_seconds=(
                    float(row["skew_seconds"]) if row["skew_seconds"] is not None else None
                ),
            )
        )

    return FlipTermStructureResponse(
        symbol=symbol_upper,
        spot=spot,
        timestamp=snapshot_ts,
        horizons_days=horizons_list,
        curve=curve_points,
        historical=historical,
    )
