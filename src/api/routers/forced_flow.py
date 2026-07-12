"""Forced Flow API (Phase 3).

Six read endpoints over the forced-flow engine (``src.analytics.forced_flow``),
all derived from the one ``dealer_hedge_flow`` primitive:

    GET /api/forced-flow/curve         flow vs. spot (grid) + per-source attribution
    GET /api/forced-flow/charm-decay   cumulative time-forced flow to the close
    GET /api/forced-flow/vanna-ladder  flow vs. IV change (vol points)
    GET /api/forced-flow/surface       spot x time-of-day -> flow (heatmap)
    GET /api/forced-flow/levels        gamma / charm / vanna flip + zero-flow level
    GET /api/forced-flow/scenario      one arbitrary (spot, time, vol) what-if
    GET /api/forced-flow/backtest      charm-into-close track record (hit rate)

Each response carries the snapshot ``timestamp`` and the ``spot`` it was computed
against. Results are recomputed on demand from the latest chain (fresh, always
current) using the same on-demand engine tuning the gamma-flip endpoints use,
and cached for 30s. The gamma flip on /levels is the EXISTING persisted value
(``gex_summary``) -- it is not recomputed here (spec 6.4). Additive: no existing
GEX endpoint is touched.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.analytics.forced_flow import (
    charm_backtest_summary,
    charm_flip,
    charm_into_close,
    dealer_hedge_flow,
    flow_total,
    forced_flow_curve,
    spot_grid,
    vanna_flip,
    vanna_ladder,
    zero_flow_level,
)
from src.analytics.main_engine import AnalyticsEngine

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/forced-flow", tags=["Forced Flow"])

# On-demand engine tuning (mirrors the gamma-flip endpoints): skip the wide
# cold-start scan and floor the lookback so a fresh per-request engine tolerates
# a few minutes of IV-pipeline lag without bouncing back empty.
_ENDPOINT_MIN_LOOKBACK_HOURS = 4
# Grid resolution for the curve / level scans, and the (coarser) surface.
_CURVE_STEP_PCT = 0.0025
_SURFACE_STEP_PCT = 0.01
_RESPONSE_CACHE_TTL_SECONDS = 30.0

_cache: Dict[tuple, Dict[str, Any]] = {}
_cache_lock = threading.Lock()


# --------------------------------------------------------------------------- #
# Dependencies + cache
# --------------------------------------------------------------------------- #
def get_db() -> DatabaseManager:
    from ..main import db_manager

    if db_manager is None:
        from fastapi import status

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database not initialized"
        )
    return db_manager


def _cache_get(key: tuple) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    with _cache_lock:
        entry = _cache.get(key)
        if entry and now - entry["ts"] < _RESPONSE_CACHE_TTL_SECONDS:
            return entry["data"]
    return None


def _cache_put(key: tuple, data: Dict[str, Any]) -> None:
    with _cache_lock:
        _cache[key] = {"ts": time.monotonic(), "data": data}


# --------------------------------------------------------------------------- #
# Shared load: fresh chain -> legs + pricing context
# --------------------------------------------------------------------------- #
def _prepare_engine(symbol: str) -> AnalyticsEngine:
    engine = AnalyticsEngine(underlying=symbol)
    engine._snapshot_cold_start_consumed = True
    engine.snapshot_lookback_hours = max(
        engine.snapshot_lookback_hours, _ENDPOINT_MIN_LOOKBACK_HOURS
    )
    return engine


def _load(symbol: str, expiry: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Fetch the latest snapshot and normalize it into forced-flow inputs.

    Returns legs + spot + timestamp + r/q + the remaining-session horizon, or
    None on a degraded snapshot (no spot / no priceable legs / empty expiry).
    """
    engine = _prepare_engine(symbol)
    snapshot = engine._get_snapshot()
    if not snapshot or not snapshot.get("options"):
        return None
    spot = snapshot.get("underlying_price")
    if not spot or spot <= 0:
        return None
    options = snapshot["options"]
    if expiry:
        options = [o for o in options if str(o.get("expiration")) == expiry]
        if not options:
            return None
    legs = engine._build_forced_flow_legs(options, snapshot["timestamp"])
    if not legs:
        return None
    return {
        "legs": legs,
        "spot": float(spot),
        "timestamp": snapshot["timestamp"],
        "r": engine.risk_free_rate,
        "q": engine.dividend_yield,
        "session_days": engine._session_days_remaining(snapshot["timestamp"]),
    }


async def _run(key: tuple, fn: Callable[..., Optional[Dict[str, Any]]], *args) -> Dict[str, Any]:
    """Cache-or-compute: returns cached data, else runs ``fn`` in a worker thread
    (snapshot fetch + reprice are sync psycopg2/numpy), caches, and returns it.
    404 on a degraded snapshot, 500 on unexpected failure."""
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        result = await asyncio.to_thread(fn, *args)
    except Exception as e:  # pragma: no cover - defensive
        logger.error("forced-flow compute failed for %s: %s", key, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    if result is None:
        raise HTTPException(status_code=404, detail="No forced-flow data available")
    _cache_put(key, result)
    return result


# --------------------------------------------------------------------------- #
# Response models
# --------------------------------------------------------------------------- #
class _TsModel(BaseModel):
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() if v is not None else None}


class CurvePoint(BaseModel):
    price: float
    total: float
    gamma: float
    charm: float
    vanna: float


class CurveResponse(_TsModel):
    symbol: str
    spot: float
    timestamp: datetime
    horizon_days: float
    zero_flow_level: Optional[float] = None
    curve: List[CurvePoint]


class CharmDecayPoint(BaseModel):
    days_elapsed: float
    flow: float


class CharmDecayResponse(_TsModel):
    symbol: str
    spot: float
    timestamp: datetime
    session_days: float
    close_flow_usd: float
    curve: List[CharmDecayPoint]


class VannaLadderPoint(BaseModel):
    vol_change_pts: float
    flow: float


class VannaLadderResponse(_TsModel):
    symbol: str
    spot: float
    timestamp: datetime
    curve: List[VannaLadderPoint]


class SurfaceResponse(_TsModel):
    symbol: str
    spot: float
    timestamp: datetime
    spots: List[float]
    times_days: List[float]
    z: List[List[float]]  # z[i][j] = flow at spots[i], times_days[j]


class LevelsResponse(_TsModel):
    symbol: str
    spot: float
    timestamp: datetime
    gamma_flip: Optional[float] = None  # existing, from gex_summary (not recomputed)
    charm_flip: Optional[float] = None
    vanna_flip: Optional[float] = None
    zero_flow_level: Optional[float] = None


class ScenarioResponse(_TsModel):
    symbol: str
    spot: float
    timestamp: datetime
    spot_move_pct: float
    days: float
    vol_change_pts: float
    total_usd: float
    gamma_component: float
    charm_component: float
    vanna_component: float
    residual: float


class BacktestRecord(BaseModel):
    date: str
    charm_flow: float
    return_pct: float
    predicted_dir: int
    realized_dir: int
    hit: bool


class BacktestResponse(BaseModel):
    symbol: str
    lookback_days: int
    total_sessions: int
    evaluated_sessions: int
    hits: int
    hit_rate: Optional[float] = None
    hit_rate_ci_low: Optional[float] = None
    hit_rate_ci_high: Optional[float] = None
    baseline_rate: Optional[float] = None
    edge: Optional[float] = None
    edge_p_value: Optional[float] = None
    significant: bool = False
    signal_mean_return: Optional[float] = None
    signal_t_stat: Optional[float] = None
    records: List[BacktestRecord]


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.get("/curve", response_model=CurveResponse)
async def get_curve(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    expiry: Optional[str] = Query(default=None, description="ISO expiry to isolate, else all"),
    spot_range_pct: float = Query(default=0.05, gt=0.0, le=0.5, description="+/- span, fraction"),
    vol_change_pts: float = Query(default=0.0, ge=-20.0, le=20.0),
    horizon_days: Optional[float] = Query(
        default=None, ge=0.0, le=30.0, description="Horizon in days; default = to today's close"
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Forced Flow Curve: flow vs. spot over a grid, with gamma/charm/vanna bands."""
    sym = symbol.upper()
    key = ("curve", sym, expiry, spot_range_pct, vol_change_pts, horizon_days)
    data = await _run(key, _curve_sync, sym, expiry, spot_range_pct, vol_change_pts, horizon_days)
    return CurveResponse(**data)


def _curve_sync(sym, expiry, spot_range_pct, vol_change_pts, horizon_days):
    ctx = _load(sym, expiry)
    if ctx is None:
        return None
    session = ctx["session_days"] if horizon_days is None else horizon_days
    curve = forced_flow_curve(
        ctx["legs"],
        ctx["spot"],
        session,
        vol_change_pts,
        ctx["r"],
        ctx["q"],
        span_pct=spot_range_pct,
        step_pct=_CURVE_STEP_PCT,
    )
    zfl = zero_flow_level(
        ctx["legs"],
        ctx["spot"],
        session,
        ctx["r"],
        ctx["q"],
        vol_change_pts,
        spot_range_pct,
        _CURVE_STEP_PCT,
    )
    return {
        "symbol": sym,
        "spot": ctx["spot"],
        "timestamp": ctx["timestamp"],
        "horizon_days": session,
        "zero_flow_level": zfl,
        "curve": [
            {
                "price": p,
                "total": ff.total_usd,
                "gamma": ff.gamma_component,
                "charm": ff.charm_component,
                "vanna": ff.vanna_component,
            }
            for p, ff in curve
        ],
    }


@router.get("/charm-decay", response_model=CharmDecayResponse)
async def get_charm_decay(
    symbol: str = Query(default="SPY"),
    expiry: Optional[str] = Query(default=None),
    steps: int = Query(default=26, ge=2, le=100),
    db: DatabaseManager = Depends(get_db),
):
    """Charm-into-Close: cumulative time-forced flow to the bell, spot held fixed."""
    sym = symbol.upper()
    key = ("charm-decay", sym, expiry, steps)
    data = await _run(key, _charm_decay_sync, sym, expiry, steps)
    return CharmDecayResponse(**data)


def _charm_decay_sync(sym, expiry, steps):
    ctx = _load(sym, expiry)
    if ctx is None:
        return None
    pts = charm_into_close(
        ctx["legs"], ctx["spot"], ctx["session_days"], ctx["r"], ctx["q"], steps=steps
    )
    return {
        "symbol": sym,
        "spot": ctx["spot"],
        "timestamp": ctx["timestamp"],
        "session_days": ctx["session_days"],
        "close_flow_usd": pts[-1][1] if pts else 0.0,
        "curve": [{"days_elapsed": dt, "flow": f} for dt, f in pts],
    }


@router.get("/vanna-ladder", response_model=VannaLadderResponse)
async def get_vanna_ladder(
    symbol: str = Query(default="SPY"),
    expiry: Optional[str] = Query(default=None),
    lo_pts: float = Query(default=-3.0, ge=-20.0, le=0.0),
    hi_pts: float = Query(default=3.0, ge=0.0, le=20.0),
    step_pts: float = Query(default=0.5, gt=0.0, le=5.0),
    db: DatabaseManager = Depends(get_db),
):
    """Vanna Ladder: forced flow vs. IV change in vol points (spot, time fixed)."""
    sym = symbol.upper()
    key = ("vanna-ladder", sym, expiry, lo_pts, hi_pts, step_pts)
    data = await _run(key, _vanna_ladder_sync, sym, expiry, lo_pts, hi_pts, step_pts)
    return VannaLadderResponse(**data)


def _vanna_ladder_sync(sym, expiry, lo_pts, hi_pts, step_pts):
    ctx = _load(sym, expiry)
    if ctx is None:
        return None
    ladder = vanna_ladder(ctx["legs"], ctx["spot"], ctx["r"], ctx["q"], lo_pts, hi_pts, step_pts)
    return {
        "symbol": sym,
        "spot": ctx["spot"],
        "timestamp": ctx["timestamp"],
        "curve": [{"vol_change_pts": dv, "flow": f} for dv, f in ladder],
    }


@router.get("/surface", response_model=SurfaceResponse)
async def get_surface(
    symbol: str = Query(default="SPY"),
    expiry: Optional[str] = Query(default=None),
    spot_range_pct: float = Query(default=0.05, gt=0.0, le=0.5),
    time_steps: int = Query(default=8, ge=1, le=48),
    db: DatabaseManager = Depends(get_db),
):
    """Forced Flow Surface: spot x time-of-day -> net dealer stock demand."""
    sym = symbol.upper()
    key = ("surface", sym, expiry, spot_range_pct, time_steps)
    data = await _run(key, _surface_sync, sym, expiry, spot_range_pct, time_steps)
    return SurfaceResponse(**data)


def _surface_sync(sym, expiry, spot_range_pct, time_steps):
    ctx = _load(sym, expiry)
    if ctx is None:
        return None
    spots = spot_grid(ctx["spot"], spot_range_pct, _SURFACE_STEP_PCT)
    times = [ctx["session_days"] * i / time_steps for i in range(time_steps + 1)]
    z = [
        [
            flow_total(ctx["legs"], ctx["spot"], s / ctx["spot"] - 1.0, t, 0.0, ctx["r"], ctx["q"])
            for t in times
        ]
        for s in spots
    ]
    return {
        "symbol": sym,
        "spot": ctx["spot"],
        "timestamp": ctx["timestamp"],
        "spots": spots,
        "times_days": times,
        "z": z,
    }


@router.get("/scenario", response_model=ScenarioResponse)
async def get_scenario(
    symbol: str = Query(default="SPY"),
    expiry: Optional[str] = Query(default=None),
    spot_move_pct: float = Query(default=0.0, ge=-0.5, le=0.5, description="Spot move, fraction"),
    days: float = Query(default=0.0, ge=0.0, le=30.0, description="Calendar days elapsed"),
    vol_change_pts: float = Query(default=0.0, ge=-20.0, le=20.0),
    db: DatabaseManager = Depends(get_db),
):
    """One arbitrary what-if: (spot move, time, vol shift) -> flow + attribution."""
    sym = symbol.upper()
    key = ("scenario", sym, expiry, spot_move_pct, days, vol_change_pts)
    data = await _run(key, _scenario_sync, sym, expiry, spot_move_pct, days, vol_change_pts)
    return ScenarioResponse(**data)


def _scenario_sync(sym, expiry, spot_move_pct, days, vol_change_pts):
    ctx = _load(sym, expiry)
    if ctx is None:
        return None
    ff = dealer_hedge_flow(
        ctx["legs"], ctx["spot"], spot_move_pct, days, vol_change_pts, ctx["r"], ctx["q"]
    )
    return {
        "symbol": sym,
        "spot": ctx["spot"],
        "timestamp": ctx["timestamp"],
        "spot_move_pct": spot_move_pct,
        "days": days,
        "vol_change_pts": vol_change_pts,
        "total_usd": ff.total_usd,
        "gamma_component": ff.gamma_component,
        "charm_component": ff.charm_component,
        "vanna_component": ff.vanna_component,
        "residual": ff.residual,
    }


@router.get("/levels", response_model=LevelsResponse)
async def get_levels(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Regime map: existing gamma flip + forced-flow charm/vanna/zero-flow levels."""
    sym = symbol.upper()
    key = ("levels", sym)
    cached = _cache_get(key)
    if cached is not None:
        return LevelsResponse(**cached)

    try:
        data = await asyncio.to_thread(_levels_sync, sym)
    except Exception as e:  # pragma: no cover - defensive
        logger.error("forced-flow levels failed for %s: %s", sym, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    if data is None:
        raise HTTPException(status_code=404, detail="No forced-flow data available")

    # Gamma flip is the EXISTING persisted value -- read it, do not recompute.
    gamma_flip: Optional[float] = None
    try:
        summary = await db.get_latest_gex_summary(sym)
        if summary:
            raw = summary.get("gamma_flip_point", summary.get("gamma_flip"))
            gamma_flip = float(raw) if raw is not None else None
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("gamma flip lookup failed for %s: %s", sym, e)
    data["gamma_flip"] = gamma_flip

    _cache_put(key, data)
    return LevelsResponse(**data)


def _levels_sync(sym):
    ctx = _load(sym)
    if ctx is None:
        return None
    return {
        "symbol": sym,
        "spot": ctx["spot"],
        "timestamp": ctx["timestamp"],
        "charm_flip": charm_flip(ctx["legs"], ctx["spot"], ctx["session_days"], ctx["r"], ctx["q"]),
        "vanna_flip": vanna_flip(ctx["legs"], ctx["spot"], ctx["r"], ctx["q"]),
        "zero_flow_level": zero_flow_level(
            ctx["legs"], ctx["spot"], ctx["session_days"], ctx["r"], ctx["q"]
        ),
    }


@router.get("/backtest", response_model=BacktestResponse)
async def get_backtest(
    symbol: str = Query(default="SPY"),
    lookback_days: int = Query(
        default=180, ge=5, le=1000, description="Calendar days of history to score"
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Charm-into-Close track record: does the morning charm-flow sign lean the
    same way as the actual noon->close return?

    Reads the persisted ``forced_flow_profile`` + ``underlying_quotes`` history
    and reports the hit rate against a naive directional baseline -- honestly,
    including when the sample is thin or the edge is nil. Returns 200 with zeroed
    counts (not 404) before enough sessions have accrued, so the page can show
    an honest "collecting" state rather than an error.
    """
    sym = symbol.upper()
    key = ("backtest", sym, lookback_days)
    cached = _cache_get(key)
    if cached is not None:
        return BacktestResponse(**cached)
    try:
        sessions = await db.get_charm_backtest_sessions(sym, lookback_days)
    except Exception as e:  # pragma: no cover - defensive
        logger.error("forced-flow backtest failed for %s: %s", sym, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    data = {"symbol": sym, "lookback_days": lookback_days, **charm_backtest_summary(sessions)}
    _cache_put(key, data)
    return BacktestResponse(**data)
