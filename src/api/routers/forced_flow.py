"""Forced Flow API (Phase 3).

Eight read endpoints over the forced-flow engine (``src.analytics.forced_flow``),
all derived from the one ``dealer_hedge_flow`` primitive:

    GET /api/forced-flow/curve            flow vs. spot (grid) + per-source attribution
    GET /api/forced-flow/charm-decay      cumulative time-forced flow to the close
    GET /api/forced-flow/vanna-ladder     flow vs. IV change (vol points)
    GET /api/forced-flow/surface          spot x time-of-day -> flow (heatmap)
    GET /api/forced-flow/session-surface  full-session field: actual history + projection
    GET /api/forced-flow/levels           gamma / charm / vanna flip + zero-flow level
    GET /api/forced-flow/scenario         one arbitrary (spot, time, vol) what-if
    GET /api/forced-flow/backtest         charm-into-close track record (hit rate)

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
import math
import threading
import time
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.analytics.forced_flow import (
    charm_backtest_summary,
    charm_flip,
    charm_into_close,
    dealer_hedge_flow,
    flow_total,
    forced_flow_curve,
    session_column_flow,
    spot_grid,
    vanna_flip,
    vanna_ladder,
    zero_flow_level,
)
from src.analytics.main_engine import ET, AnalyticsEngine
from src.config import _getenv_float, _getenv_str
from src.database import db_connection
from src.symbols import get_canonical_symbol

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
# Effective time-to-expiry floor (30 minutes, in years) for the SPOT-MOVE views
# (curve / surface / levels / vanna-ladder / scenario). Standard desk
# regularization against the T->0 gamma singularity: without it, a wide reprice
# in the final minutes -- or off a near-close snapshot -- settles the whole 0DTE
# book as a step function and the numbers blow up. Charm-into-Close is
# deliberately NOT floored (it is *about* the into-the-bell resolution).
_SCENARIO_MIN_TTE_YEARS = 30.0 / (365.0 * 24.0 * 60.0)
# Default +/- view span for the curve / surface. Kept tight because a wide span
# on an index walks spot far enough to settle the entire same-day book, which
# dominates the wings; the actionable gamma structure lives near spot. Callers
# can widen via the query param.
_DEFAULT_VIEW_SPAN_PCT = 0.02

# Full-session field (/session-surface) tuning.
# Fixed price-grid resolution, held constant across every column so the field is
# a clean rectangle sharing one axis (the surface heatmap the frontend draws).
_SESSION_PRICE_POINTS = 41
# Lookback for the as-of latest-per-contract reconstruction (query #3). During RTH
# every active contract requotes within a few minutes, so a SHORT window still
# captures the book as it stood at that minute -- and a narrow window is far
# cheaper per bucket (the DISTINCT-ON walks a fraction of the rows), which matters
# because a cold session rebuild fires ~30 of these back-to-back against a
# live-ingestion table (a 2h window there cost ~100s/build). Deliberately tighter
# than the engine's 2h GEX snapshot window -- this is a viz, not the persisted
# book. Widen via env if a thin far wing ever matters.
_ASOF_LOOKBACK_HOURS = _getenv_float(
    "SESSION_SURFACE_ASOF_LOOKBACK_HOURS", 20.0 / 60.0, min=1.0 / 60.0
)
# Hard row cap on the as-of chain query (mirrors the engine's default
# ANALYTICS_SNAPSHOT_MAX_ROWS); well above any realistic chain size.
_ASOF_SNAPSHOT_ROW_CAP = 50000
# Below this many surviving columns the field is too sparse to plot; return 404.
_SESSION_MIN_COLUMNS = 3
# Default column counts for /session-surface. Shared by the endpoint's Query
# defaults AND the background warmer so their response-cache keys can never drift.
_SESSION_PAST_STEPS_DEFAULT = 30
_SESSION_FUTURE_STEPS_DEFAULT = 8

_cache: Dict[tuple, Dict[str, Any]] = {}
_cache_lock = threading.Lock()

# Per-(trading-day, symbol, time-bucket, price-grid) cache of ONE computed PAST
# column of the session field. A past bucket's as-of book is immutable, so its
# z-row / magnet never change once computed -- memoize them so each 30s
# response-cache expiry reprices only the live now + projection columns (the
# whole recorded history is then DB-free and reprice-free on a warm cache). The
# trading date is the leading key element so a daily prune drops yesterday.
_past_col_cache: Dict[tuple, Dict[str, Any]] = {}
_past_col_cache_lock = threading.Lock()
# Insertion-order eviction ceiling (dicts preserve insertion order). ~30 buckets
# x a few symbols x a couple of grids per day sits far under this.
_PAST_COL_CACHE_MAX = 6000
# Snap the price band to this fraction of spot so a fresh intraday extreme only
# re-grids -- and cold-misses the past cache -- when it moves the band a real
# amount, not on every tick.
_SESSION_GRID_QUANTUM_PCT = 0.001

# Single-flight registry: coalesce concurrent identical session-surface rebuilds
# so the warmer, the frontend, and any ad-hoc caller SHARE one rebuild instead of
# each launching their own and stampeding the DB (the cold-start thundering herd
# that turned a ~30s build into minutes). Async, on the worker's event loop: the
# owner runs the sync build in ONE worker thread while every waiter just awaits the
# shared Task -- so a cold start can't tie up the (small) threadpool with blocked
# waiters and starve other endpoints. Keyed by the response-cache key.
_inflight_tasks: Dict[tuple, "asyncio.Task"] = {}


def _quantize_band(p_lo: float, p_hi: float, spot: float) -> Tuple[float, float]:
    """Snap ``[p_lo, p_hi]`` out to a coarse grid (~0.1% of spot) so tiny intraday
    range changes keep the same price axis -- and the same past-column cache."""
    step = max(1e-6, spot * _SESSION_GRID_QUANTUM_PCT)
    return math.floor(p_lo / step) * step, math.ceil(p_hi / step) * step


def _past_col_get(key: tuple) -> Optional[Dict[str, Any]]:
    with _past_col_cache_lock:
        return _past_col_cache.get(key)


def _past_col_put(key: tuple, value: Dict[str, Any]) -> None:
    with _past_col_cache_lock:
        _past_col_cache[key] = value
        while len(_past_col_cache) > _PAST_COL_CACHE_MAX:
            _past_col_cache.pop(next(iter(_past_col_cache)))  # evict oldest


def _past_col_prune_to_day(trading_date: str) -> None:
    """Drop cached columns from any other trading date (``key[0]`` is the date)."""
    with _past_col_cache_lock:
        for k in [k for k in _past_col_cache if k[0] != trading_date]:
            _past_col_cache.pop(k, None)


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


def _load_asof(sym: str, at_ts: datetime) -> Optional[Dict[str, Any]]:
    """As-of snapshot for the session field's PAST columns: the book actually
    recorded at or before ``at_ts``, rebuilt straight from ``option_chains``.

    Mirrors the three queries of ``AnalyticsEngine._get_snapshot`` -- (1) latest
    chain timestamp for the underlying, (2) underlying close as of that ts, (3)
    the DISTINCT-ON latest-per-contract chain over a lookback window with the
    expiration roll-off and ``gamma IS NOT NULL`` filter -- but pinned to
    ``timestamp <= at_ts`` and WITHOUT the live-cycle gates: no in-session
    max-staleness refusal, no close-stamp forward re-anchor, no extended-hours
    spot re-anchor. Those gates defend the LIVE recompute against a stuck feed;
    here we deliberately want the frozen historical book exactly as it was
    recorded at that minute, so recomputing the past field is faithful. The
    persisted ``forced_flow_profile.profile`` JSONB is written empty, so this
    reconstruction is the ONLY source of the actual past field.

    Does NOT modify ``_get_snapshot``. Query #3 reuses the engine's own
    ``_SNAPSHOT_QUERY`` class attribute, so the column list and predicates are
    byte-identical and cannot drift. Returns ``{'timestamp', 'spot', 'options'}``
    or None when no chain / no spot / no priceable rows exist at or before
    ``at_ts``.
    """
    db_symbol = get_canonical_symbol(sym)
    with db_connection() as conn:
        cursor = conn.cursor()

        # 1. Latest option-chain timestamp for this underlying at or before at_ts.
        cursor.execute(
            """
            SELECT timestamp
            FROM option_chains
            WHERE underlying = %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (db_symbol, at_ts),
        )
        ts_row = cursor.fetchone()
        if not ts_row or ts_row[0] is None:
            return None
        ts = ts_row[0]

        # 2. Underlying close as of that chain timestamp. No staleness gate: an
        # hours-old close is exactly what we want for a frozen historical minute.
        cursor.execute(
            """
            SELECT close
            FROM underlying_quotes
            WHERE symbol = %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (db_symbol, ts),
        )
        uq_row = cursor.fetchone()
        spot = float(uq_row[0]) if uq_row and uq_row[0] is not None else None
        if spot is None or spot <= 0:
            return None

        # 3. Latest-per-contract chain over the lookback window, with the same
        # 16:15 ET expiration roll-off as _get_snapshot and the gamma-populated
        # filter. Reuses _SNAPSHOT_QUERY verbatim so the SELECT column list and
        # predicates match query #3 exactly.
        ts_et = ts.astimezone(ET)
        if ts_et.time() < dt_time(16, 15):
            min_expiration = ts_et.date() - timedelta(days=1)
        else:
            min_expiration = ts_et.date()
        lookback_start = ts - timedelta(hours=_ASOF_LOOKBACK_HOURS)
        cursor.execute(
            AnalyticsEngine._SNAPSHOT_QUERY,
            (db_symbol, ts, lookback_start, min_expiration, _ASOF_SNAPSHOT_ROW_CAP),
        )
        rows = cursor.fetchall()

    # Same row->dict mapping as _get_snapshot (only the fields build_legs reads
    # are load-bearing; the rest are carried for parity).
    options = [
        {
            "option_symbol": row[0],
            "strike": float(row[1]),
            "expiration": row[2],
            "option_type": row[3],
            "last": float(row[4]) if row[4] else 0.0,
            "bid": float(row[5]) if row[5] else 0.0,
            "ask": float(row[6]) if row[6] else 0.0,
            "volume": int(row[7]) if row[7] else 0,
            "open_interest": int(row[8]) if row[8] else 0,
            "delta": float(row[9]) if row[9] else 0.0,
            "gamma": float(row[10]) if row[10] else 0.0,
            "theta": float(row[11]) if row[11] else 0.0,
            "vega": float(row[12]) if row[12] else 0.0,
            "implied_volatility": float(row[13]) if row[13] else None,
        }
        for row in rows
    ]
    if not options:
        return None
    return {"timestamp": ts, "spot": spot, "options": options}


def _session_day_range(
    db_symbol: str, day_start: datetime
) -> Tuple[Optional[float], Optional[float]]:
    """Today's realized ``(low, high)`` for ``db_symbol`` from ``underlying_quotes``,
    from ``day_start`` (the ET session open) to now.

    Used only to widen the field's price grid so it spans where spot ACTUALLY
    travelled today, not just a symmetric band around the last print. Returns
    ``(None, None)`` on any error or an empty session so the caller falls back to
    the span window alone -- a missing day-range must never fail the endpoint.
    """
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT min(low), max(high)
                FROM underlying_quotes
                WHERE symbol = %s
                  AND timestamp >= %s
                """,
                (db_symbol, day_start),
            )
            row = cursor.fetchone()
        if row and row[0] is not None and row[1] is not None:
            return float(row[0]), float(row[1])
    except Exception as e:  # pragma: no cover - defensive; grid falls back to span
        logger.debug("session-surface day-range query failed for %s: %s", db_symbol, e)
    return None, None


def _linspace(lo: float, hi: float, n: int) -> List[float]:
    """``n``-point inclusive linear grid from ``lo`` to ``hi`` (numpy-free)."""
    if n <= 1:
        return [lo]
    step = (hi - lo) / (n - 1)
    return [lo + step * i for i in range(n)]


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


class SessionColumnOut(BaseModel):
    min_to_close: float  # minutes remaining to the cash close at this time-slice
    spot: float  # reference spot for this column
    magnet: Optional[float] = None  # zero-flow level for this column (nearest to spot)
    is_past: bool  # True = actual recorded book; False = current-book projection


class SessionSurfaceResponse(_TsModel):
    symbol: str
    spot: float
    timestamp: datetime
    session_open_min_to_close: float  # minutes open->close (~390)
    now_min_to_close: float  # minutes now->close
    prices: List[float]  # shared ascending price axis
    z: List[List[float]]  # z[col][price] = flow to move spot to prices[price] by close
    columns: List[SessionColumnOut]  # one per time-slice, open (left) -> close (right)
    now_index: int  # index of the last actual (is_past) column -- the NOW column


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
    # The trailing-median "recent normal" this session was scored against, and
    # today's flow minus it. predicted_dir is the sign of charm_deviation.
    charm_baseline: float
    charm_deviation: float
    return_pct: float
    predicted_dir: int
    realized_dir: int
    hit: bool


class BacktestVariant(BaseModel):
    total_sessions: int
    evaluated_sessions: int
    # Early sessions excluded only because no trailing baseline exists yet.
    warmup_sessions: int = 0
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


class BacktestResponse(BaseModel):
    symbol: str
    lookback_days: int
    # Two forecast definitions scored over identical sessions, so the honest
    # A/B is visible: ``full`` is the 0DTE-inclusive close flow (dominated by
    # same-day expiry resolution), ``smooth`` is the first-order charm only.
    full: BacktestVariant
    smooth: BacktestVariant


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.get("/curve", response_model=CurveResponse)
async def get_curve(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    expiry: Optional[str] = Query(default=None, description="ISO expiry to isolate, else all"),
    spot_range_pct: float = Query(
        default=_DEFAULT_VIEW_SPAN_PCT, gt=0.0, le=0.5, description="+/- span, fraction"
    ),
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
        min_tte_years=_SCENARIO_MIN_TTE_YEARS,
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
        _SCENARIO_MIN_TTE_YEARS,
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
    ladder = vanna_ladder(
        ctx["legs"],
        ctx["spot"],
        ctx["r"],
        ctx["q"],
        lo_pts,
        hi_pts,
        step_pts,
        _SCENARIO_MIN_TTE_YEARS,
    )
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
    spot_range_pct: float = Query(default=_DEFAULT_VIEW_SPAN_PCT, gt=0.0, le=0.5),
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
            flow_total(
                ctx["legs"],
                ctx["spot"],
                s / ctx["spot"] - 1.0,
                t,
                0.0,
                ctx["r"],
                ctx["q"],
                _SCENARIO_MIN_TTE_YEARS,
            )
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


@router.get("/session-surface", response_model=SessionSurfaceResponse)
async def get_session_surface(
    symbol: str = Query(default="SPY"),
    spot_range_pct: float = Query(default=_DEFAULT_VIEW_SPAN_PCT, gt=0.0, le=0.5),
    past_steps: int = Query(
        default=_SESSION_PAST_STEPS_DEFAULT, ge=1, le=120, description="Actual-history columns"
    ),
    future_steps: int = Query(
        default=_SESSION_FUTURE_STEPS_DEFAULT,
        ge=1,
        le=48,
        description="Projection columns to close",
    ),
    expiry: Optional[str] = Query(default=None),
    db: DatabaseManager = Depends(get_db),
):
    """Full-session forced-flow field: the ACTUAL historical field recomputed from
    stored ``option_chains`` (RTH open -> now) plus a projection of the current
    book (now -> close).

    Columns march left (open) to right (close); ``now_index`` marks the boundary
    between recorded history and projection. History is rebuilt from
    ``option_chains`` because the persisted ``forced_flow_profile.profile`` JSONB
    is written empty -- so the past field cannot be read back, only recomputed.
    """
    sym = symbol.upper()
    data = await _session_surface_async(sym, spot_range_pct, past_steps, future_steps, expiry)
    if data is None:
        raise HTTPException(status_code=404, detail="No forced-flow data available")
    return SessionSurfaceResponse(**data)


def _session_surface_sync(sym, span, past_steps, future_steps, expiry):
    ctx = _load(sym, expiry)
    if ctx is None:
        return None
    now_ts = ctx["timestamp"]
    spot = ctx["spot"]
    r, q = ctx["r"], ctx["q"]
    t0 = time.monotonic()  # build-time telemetry (logged at the end)

    # Anchor the time axis to today's 16:00 ET cash close, in minutes-to-close.
    # Mirrors _session_days_remaining's .replace() convention (safe within a
    # single trading day -- no DST transition between the open and the close).
    now_et = now_ts.astimezone(ET)
    close_dt = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    open_dt = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    now_min_to_close = max(0.0, (close_dt - now_et).total_seconds() / 60.0)
    # Full RTH span (~390 min), floored at now's remaining minutes so a pre-open
    # call (now farther from the close than the open is) still yields open >= now.
    session_open_min_to_close = max(now_min_to_close, (close_dt - open_dt).total_seconds() / 60.0)

    # Price grid: union today's realized [low, high] with the +/- span window, so
    # the field spans both where spot actually travelled and a symmetric view
    # band. Any failure falls back to the span window alone. Quantized so a fresh
    # extreme keeps the same axis (and past-column cache) unless it moves the band
    # meaningfully.
    day_lo, day_hi = _session_day_range(get_canonical_symbol(sym), open_dt)
    p_lo = spot * (1.0 - span) if day_lo is None else min(day_lo, spot * (1.0 - span))
    p_hi = spot * (1.0 + span) if day_hi is None else max(day_hi, spot * (1.0 + span))
    if not (p_hi > p_lo > 0.0):
        p_lo, p_hi = spot * (1.0 - span), spot * (1.0 + span)
    p_lo, p_hi = _quantize_band(p_lo, p_hi, spot)
    prices = _linspace(p_lo, p_hi, _SESSION_PRICE_POINTS)

    # Cache identity for the immutable PAST columns: (trading-day, symbol,
    # time-bucket, price-grid). Prune any prior day, then reuse/compute per bucket.
    db_symbol = get_canonical_symbol(sym)
    trading_date = now_et.date().isoformat()
    grid_key = (round(p_lo, 4), round(p_hi, 4), _SESSION_PRICE_POINTS)
    _past_col_prune_to_day(trading_date)

    # One engine, reused for as-of leg-building across every (cold) past column.
    engine = AnalyticsEngine(sym)

    # PAST columns on a FIXED minutes-to-close grid marching from the open toward
    # now (exclusive of now). Fixed buckets map to the same wall-clock instant
    # every request, so each computed column stays cache-hot -- a warm session
    # reprices only the live now + projection columns below. Each cold bucket is
    # the actual book recorded at or before that instant, rebuilt via _load_asof.
    past_cols: List[Dict[str, Any]] = []
    cold_builds = 0  # past buckets rebuilt from the DB this call (cache misses)
    step = session_open_min_to_close / past_steps if past_steps > 0 else session_open_min_to_close
    for k in range(past_steps + 1):
        mtc = session_open_min_to_close - k * step
        if mtc <= now_min_to_close + 1e-6:
            break  # the live NOW column (below) takes over from here to the close
        ckey = (trading_date, db_symbol, round(mtc, 3), grid_key)
        cached = _past_col_get(ckey)
        if cached is not None:
            past_cols.append(cached)
            continue
        tau = close_dt - timedelta(minutes=mtc)
        try:
            asof = _load_asof(sym, tau)
            if asof is None:
                continue
            legs = engine._build_forced_flow_legs(asof["options"], asof["timestamp"])
        except Exception as e:  # pragma: no cover - defensive per-bucket skip
            logger.debug("session-surface as-of bucket at %s failed: %s", tau, e)
            continue
        if not legs:
            continue
        row, mag = session_column_flow(
            float(asof["spot"]), legs, mtc, prices, r, q, _SCENARIO_MIN_TTE_YEARS
        )
        col = {
            "min_to_close": mtc,
            "spot": float(asof["spot"]),
            "magnet": mag,
            "is_past": True,
            "z": row,
        }
        _past_col_put(ckey, col)
        past_cols.append(col)
        cold_builds += 1

    # NOW column: the live book/spot -- an ACTUAL reading and the boundary. Never
    # cached (it moves every cycle).
    now_row, now_mag = session_column_flow(
        spot, ctx["legs"], now_min_to_close, prices, r, q, _SCENARIO_MIN_TTE_YEARS
    )
    columns: List[Dict[str, Any]] = past_cols + [
        {
            "min_to_close": now_min_to_close,
            "spot": spot,
            "magnet": now_mag,
            "is_past": True,
            "z": now_row,
        }
    ]
    now_index = len(columns) - 1  # index of the NOW column (last is_past)

    # FUTURE columns: project the CURRENT book/spot forward, now -> ~close. Always
    # fresh (they advance every cycle) and cheap -- only a handful.
    for k in range(1, future_steps + 1):
        mtc = max(0.0, now_min_to_close * (1.0 - k / future_steps))
        row, mag = session_column_flow(
            spot, ctx["legs"], mtc, prices, r, q, _SCENARIO_MIN_TTE_YEARS
        )
        columns.append(
            {"min_to_close": mtc, "spot": spot, "magnet": mag, "is_past": False, "z": row}
        )

    # Too few surviving columns -> the field is not meaningfully plottable.
    if len(columns) < _SESSION_MIN_COLUMNS:
        return None

    logger.info(
        "session-surface %s: %d cols (%d cold buckets) built in %.1fs",
        sym,
        len(columns),
        cold_builds,
        time.monotonic() - t0,
    )
    return {
        "symbol": sym,
        "spot": spot,
        "timestamp": now_ts,
        "session_open_min_to_close": session_open_min_to_close,
        "now_min_to_close": now_min_to_close,
        "prices": prices,
        "z": [c["z"] for c in columns],
        "columns": [
            {
                "min_to_close": c["min_to_close"],
                "spot": c["spot"],
                "magnet": c["magnet"],
                "is_past": c["is_past"],
            }
            for c in columns
        ],
        "now_index": now_index,
    }


async def _session_surface_async(sym, span, past_steps, future_steps, expiry):
    """Single-flight, cached session-surface build (async, per worker event loop).

    Returns the cached response if warm. Otherwise exactly ONE caller (the owner)
    launches the rebuild as a Task -- running the sync build in a worker thread --
    while every concurrent caller for the same key awaits that same Task and shares
    its result. Waiters await on the loop and hold no thread, so a cold start can't
    starve the threadpool. This is what stops the stampede where both workers'
    warmers plus the frontend poll each launched their own full-day rebuild.

    Runs entirely on the single worker event loop (no await between the registry
    lookup and create_task), so the get/create is race-free without a lock.
    """
    key = ("session-surface", sym, span, past_steps, future_steps, expiry)
    cached = _cache_get(key)
    if cached is not None:
        return cached
    task = _inflight_tasks.get(key)
    if task is None:

        async def _build():
            try:
                data = await asyncio.to_thread(
                    _session_surface_sync, sym, span, past_steps, future_steps, expiry
                )
                if data is not None:
                    _cache_put(key, data)
                return data
            finally:
                _inflight_tasks.pop(key, None)

        task = asyncio.create_task(_build())
        _inflight_tasks[key] = task
    return await task


# --------------------------------------------------------------------------- #
# Background cache warmer
# --------------------------------------------------------------------------- #
# Keep the full-session field hot so no user ever pays the cold full-day rebuild.
# Critically, this runs PER uvicorn worker -- each worker holds its own in-process
# cache, so without per-worker warming the requests that land on a cold worker
# still pay the rebuild. Tunable / disable-able via env.
_WARM_ENABLED = _getenv_str("SESSION_SURFACE_WARM", "1").strip().lower() not in {"0", "false", "no"}
_WARM_INTERVAL_SECONDS = _getenv_float("SESSION_SURFACE_WARM_INTERVAL_SECONDS", 30.0, min=5.0)
_WARM_SYMBOLS = [
    s.strip().upper()
    for s in _getenv_str("SESSION_SURFACE_WARM_SYMBOLS", "SPY,SPX,QQQ,NDX").split(",")
    if s.strip()
]


async def _warm_one(sym: str) -> None:
    """Warm the DEFAULT session-surface for one symbol via the single-flight path.

    Awaits :func:`_session_surface_async`, so the warm coalesces with any concurrent
    frontend / ad-hoc request for the same (default-grid) field instead of racing
    it -- and it caches under the exact key :func:`get_session_surface` reads, so
    the next on-demand GET is an instant hit.
    """
    try:
        await _session_surface_async(
            sym,
            _DEFAULT_VIEW_SPAN_PCT,
            _SESSION_PAST_STEPS_DEFAULT,
            _SESSION_FUTURE_STEPS_DEFAULT,
            None,
        )
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("session-surface warm failed for %s: %s", sym, e)


async def session_surface_warm_loop() -> None:
    """Keep the session-field cache hot in the background (started per worker).

    The costly part is the cold full-day rebuild; running it here on startup and
    every ``_WARM_INTERVAL_SECONDS`` moves it entirely off the user's first view.
    The per-column cache makes every warm after the first cheap -- only the live
    now + projection columns and any newly-past bucket are repriced. The
    synchronous reprice is dispatched through ``asyncio.to_thread`` so it never
    blocks the event loop; the task is cancelled on shutdown.
    """
    if not _WARM_ENABLED or not _WARM_SYMBOLS:
        logger.info("session-surface warmer disabled")
        return
    logger.info(
        "session-surface warmer: %s every %.0fs",
        ",".join(_WARM_SYMBOLS),
        _WARM_INTERVAL_SECONDS,
    )
    while True:
        for sym in _WARM_SYMBOLS:
            try:
                await _warm_one(sym)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # pragma: no cover - defensive
                logger.debug("session-surface warm cycle error for %s: %s", sym, e)
        await asyncio.sleep(_WARM_INTERVAL_SECONDS)


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
        ctx["legs"],
        ctx["spot"],
        spot_move_pct,
        days,
        vol_change_pts,
        ctx["r"],
        ctx["q"],
        _SCENARIO_MIN_TTE_YEARS,
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
        "charm_flip": charm_flip(
            ctx["legs"],
            ctx["spot"],
            ctx["session_days"],
            ctx["r"],
            ctx["q"],
            min_tte_years=_SCENARIO_MIN_TTE_YEARS,
        ),
        "vanna_flip": vanna_flip(
            ctx["legs"], ctx["spot"], ctx["r"], ctx["q"], min_tte_years=_SCENARIO_MIN_TTE_YEARS
        ),
        "zero_flow_level": zero_flow_level(
            ctx["legs"],
            ctx["spot"],
            ctx["session_days"],
            ctx["r"],
            ctx["q"],
            min_tte_years=_SCENARIO_MIN_TTE_YEARS,
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
    """Charm-into-Close track record: when the morning charm flow is stronger
    (or weaker) than its own recent normal, does the noon->close return lean the
    same way?

    Scores the charm read RELATIVE to its trailing baseline rather than by raw
    sign -- for an index book the raw sign is structurally almost constant ("buy"
    nearly every day), so it degenerates to the naive baseline and tests nothing;
    the deviation from recent normal is the part that varies. Reads the persisted
    ``forced_flow_profile`` + ``underlying_quotes`` history and reports the hit
    rate against a naive directional baseline -- honestly, including when the
    sample is thin or the edge is nil. Scores TWO forecast definitions over
    identical sessions (``full`` = 0DTE-inclusive close flow; ``smooth`` =
    first-order charm only) so the A/B is visible. Returns 200 with zeroed counts
    (not 404) before enough sessions have accrued, so the page can show an honest
    "collecting" state rather than an error.
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
    data = {
        "symbol": sym,
        "lookback_days": lookback_days,
        "full": charm_backtest_summary(sessions, charm_key="charm_flow"),
        "smooth": charm_backtest_summary(sessions, charm_key="charm_flow_smooth"),
    }
    _cache_put(key, data)
    return BacktestResponse(**data)
