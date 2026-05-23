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
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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


# On-demand endpoints get a wider snapshot lookback than the steady-state
# analytics tick.  Analytics runs every 60s and is designed for very fresh
# data (ANALYTICS_SNAPSHOT_LOOKBACK_HOURS commonly 0.25h = 15min), so its
# default narrow window is appropriate for that cadence.  The API path
# serves one-shot client requests that should tolerate a Greeks pipeline
# that's lagged by a few minutes / a partial extended-hours session.  4h
# covers a normal IV-pipeline lag without scanning 96h of option_chains.
_ENDPOINT_MIN_LOOKBACK_HOURS = 4

# In-memory response cache.  Heavy work (snapshot fetch + N profile builds)
# can take >30s per request on a stale-data path; with a polling dashboard
# this would multiply the cost N-fold for no signal.  TTL matches the 30s
# cadence at which a multi-horizon flip view becomes stale anyway (the
# underlying analytics tick is 60s, and a multi-day regime level doesn't
# move minute-by-minute).
_RESPONSE_CACHE: Dict[tuple, Dict[str, Any]] = {}
_RESPONSE_CACHE_LOCK = threading.Lock()
_RESPONSE_CACHE_TTL_SECONDS = 30.0
_RESPONSE_CACHE_MAX_ENTRIES = 64


def _cache_get(key: tuple) -> Optional[dict]:
    now = time.monotonic()
    with _RESPONSE_CACHE_LOCK:
        entry = _RESPONSE_CACHE.get(key)
        if entry and now - entry["ts"] < _RESPONSE_CACHE_TTL_SECONDS:
            return entry["data"]
        if entry is not None:
            _RESPONSE_CACHE.pop(key, None)
    return None


def _cache_put(key: tuple, data: dict) -> None:
    with _RESPONSE_CACHE_LOCK:
        _RESPONSE_CACHE[key] = {"data": data, "ts": time.monotonic()}
        # FIFO eviction — cheap; expected cardinality is tiny (a handful
        # of symbols × default horizon set).
        while len(_RESPONSE_CACHE) > _RESPONSE_CACHE_MAX_ENTRIES:
            oldest = next(iter(_RESPONSE_CACHE))
            _RESPONSE_CACHE.pop(oldest, None)


def _prepare_endpoint_engine(symbol: str) -> AnalyticsEngine:
    """Build an AnalyticsEngine tuned for the on-demand API path.

    Two adjustments vs. the persisted analytics tick:

    1. ``_snapshot_cold_start_consumed = True`` — skip the wide cold-start
       lookback scan (default 96h, ~2M+ rows for SPY).  That branch
       exists to absorb a process restart on a cold buffer pool, and it
       flips to True after the analytics tick's first cycle.  The API
       endpoint creates a fresh engine per request, so without this flip
       every request would do a 96h scan whenever data is even slightly
       older than the steady-state window — observed at 57s per call
       on the prod box with stale extended-hours data.

    2. ``snapshot_lookback_hours = max(configured, 4h)`` — the persisted
       tick runs every minute and its narrow default (often 0.25h) is
       right for that cadence.  On-demand calls should tolerate a few
       minutes of IV-pipeline lag without bouncing back empty, so we
       floor the lookback at 4h here.  This widens the scan modestly
       relative to the persisted tick but is still orders of magnitude
       cheaper than the cold-start path.
    """
    engine = AnalyticsEngine(underlying=symbol)
    engine._snapshot_cold_start_consumed = True
    engine.snapshot_lookback_hours = max(
        engine.snapshot_lookback_hours, _ENDPOINT_MIN_LOOKBACK_HOURS
    )
    return engine


def _compute_sync(symbol: str, horizons: List[float]) -> Optional[dict]:
    """Run the analytics-engine snapshot fetch + multi-horizon flip
    resolution.  Synchronous (psycopg2 + numpy); call via
    ``asyncio.to_thread`` from the async endpoint."""
    engine = _prepare_endpoint_engine(symbol)
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
    cache_key = ("term-structure", symbol_upper, tuple(horizons_list))

    sync_result = _cache_get(cache_key)
    if sync_result is None:
        # Heavy work (DB snapshot fetch + N profile builds) is sync and CPU/IO
        # bound — run it off the event loop so concurrent requests don't queue.
        try:
            sync_result = await asyncio.to_thread(_compute_sync, symbol_upper, horizons_list)
        except Exception as e:
            logger.error(
                "flip-term-structure compute failed for %s: %s", symbol_upper, e, exc_info=True
            )
            raise HTTPException(status_code=500, detail="flip-term-structure compute failed")
        if sync_result is not None:
            _cache_put(cache_key, sync_result)

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


# ---------------------------------------------------------------------------
# Surface endpoint — full per-horizon dealer-gamma profile on a shared grid.
# Drives the horizon × price contour and the 3D surface visualizations.
# Contract: docs/gex_flip_surface_contract.md.
# ---------------------------------------------------------------------------


class FlipSurfaceWall(BaseModel):
    strike: float
    type: str  # "call" | "put"
    abs_dollar_gex: float


class FlipSurfacePoint(BaseModel):
    horizon_days: float
    flip: Optional[float] = None
    resolved: bool
    span_used: float
    net_gex_at_spot: Optional[float] = None


class FlipSurfaceResponse(BaseModel):
    symbol: str
    spot: float
    timestamp: datetime
    grid: List[float]
    horizons_days: List[float]
    profiles: List[List[float]]
    flips: List[FlipSurfacePoint]
    walls: List[FlipSurfaceWall]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() if v is not None else None}


# Bounds match GAMMA_PROFILE_SPAN_PCT / GAMMA_PROFILE_STEP_PCT validators in
# src/config.py; matching them here keeps query-param validation in sync with
# the analytics-engine acceptance band.
_SURFACE_SPAN_MIN = 0.02
_SURFACE_SPAN_MAX = 1.0
_SURFACE_STEP_MIN = 0.0005
_SURFACE_STEP_MAX = 0.05
# Guard against combinatorial blow-up; matches the cap noted in the contract
# (len(grid) × len(horizons) ≤ 4000).  At default 0.20/0.0025 the grid is
# ~160 entries, leaving room for 12 horizons (the parse cap).
_SURFACE_MAX_GRID_HORIZON_PRODUCT = 4000


def _compute_surface_sync(
    symbol: str,
    horizons: List[float],
    span_pct: float,
    step_pct: float,
    include_walls: bool,
) -> Optional[dict]:
    """Sync compute: snapshot fetch + per-horizon profile + walls.  Called
    via asyncio.to_thread from the async endpoint."""
    engine = _prepare_endpoint_engine(symbol)
    snapshot = engine._get_snapshot()
    if not snapshot or not snapshot.get("options"):
        return None
    surface = engine.compute_flip_surface(
        snapshot["options"],
        snapshot["underlying_price"],
        snapshot["timestamp"],
        horizons,
        span_pct=span_pct,
        step_pct=step_pct,
        include_walls=include_walls,
    )
    return {
        "spot": snapshot["underlying_price"],
        "timestamp": snapshot["timestamp"],
        **surface,
    }


@router.get("/flip-surface", response_model=FlipSurfaceResponse)
async def get_flip_surface(
    symbol: str = Query(default="SPX", description="Underlying symbol"),
    horizons: str = Query(
        default="1,3,5,10,20,60",
        description=(
            "Comma-separated list of multi-day reference horizons in days. "
            "Bounds: [0.25, 365], at most 12 entries."
        ),
    ),
    span_pct: float = Query(
        default=0.20,
        ge=_SURFACE_SPAN_MIN,
        le=_SURFACE_SPAN_MAX,
        description="Half-width of the rendered price grid, as a fraction of spot.",
    ),
    step_pct: float = Query(
        default=0.0025,
        ge=_SURFACE_STEP_MIN,
        le=_SURFACE_STEP_MAX,
        description="Grid step, as a fraction of spot.",
    ),
    include_walls: bool = Query(
        default=True,
        description="Include the canonical Call/Put walls overlay.",
    ),
):
    """Multi-horizon spot-shift dealer-gamma surface (PROTOTYPE).

    Returns the dealer-gamma profile per horizon on a shared price grid,
    plus the resolved flip per horizon and a single wall overlay.  See
    docs/gex_flip_surface_contract.md for the full contract.
    """
    horizons_list = _parse_horizons(horizons)
    symbol_upper = symbol.upper()

    # Combinatorial cap: prevents a malicious caller from pinning a huge
    # grid (e.g. step_pct=0.0005, span_pct=1.0 → 4000+ entries) against
    # the max-12 horizons (12 × 4000 = 48k floats = ~400 KB JSON per
    # request).  The cap matches the contract.
    approx_grid_size = int(2 * span_pct / step_pct) + 1
    if approx_grid_size * len(horizons_list) > _SURFACE_MAX_GRID_HORIZON_PRODUCT:
        raise HTTPException(
            status_code=400,
            detail=(
                f"grid × horizons product {approx_grid_size * len(horizons_list)} "
                f"exceeds cap {_SURFACE_MAX_GRID_HORIZON_PRODUCT}; "
                "widen step_pct or drop horizons"
            ),
        )

    # Cache key includes the grid parameters so different rendering
    # resolutions don't share a cached entry.
    surface_cache_key = (
        "surface",
        symbol_upper,
        (
            tuple(horizons_list),
            float(span_pct),
            float(step_pct),
            bool(include_walls),
        ),
    )

    sync_result = _cache_get(surface_cache_key)
    if sync_result is None:
        try:
            sync_result = await asyncio.to_thread(
                _compute_surface_sync,
                symbol_upper,
                horizons_list,
                span_pct,
                step_pct,
                include_walls,
            )
        except Exception as e:
            logger.error("flip-surface compute failed for %s: %s", symbol_upper, e, exc_info=True)
            raise HTTPException(status_code=500, detail="flip-surface compute failed")
        if sync_result is not None:
            _cache_put(surface_cache_key, sync_result)

    if sync_result is None:
        raise HTTPException(status_code=404, detail=f"No usable option snapshot for {symbol_upper}")

    snapshot_ts = sync_result["timestamp"]
    if snapshot_ts.tzinfo is None:
        snapshot_ts = snapshot_ts.replace(tzinfo=timezone.utc)

    return FlipSurfaceResponse(
        symbol=symbol_upper,
        spot=float(sync_result["spot"]),
        timestamp=snapshot_ts,
        grid=sync_result["grid"],
        horizons_days=sync_result["horizons_days"],
        profiles=sync_result["profiles"],
        flips=[FlipSurfacePoint(**row) for row in sync_result["flips"]],
        walls=[FlipSurfaceWall(**w) for w in sync_result["walls"]],
    )
