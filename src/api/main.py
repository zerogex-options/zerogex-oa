#!/usr/bin/env python3
"""
ZeroGEX API Server
FastAPI backend for serving analytics data to the frontend
"""

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from collections import deque
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, date as date_type
import asyncio
import os
import re
from typing import List, Optional, Literal
import pytz

from src import config

from .database import DatabaseManager
from .errors import handle_api_errors
from .middleware import RequestIdMiddleware
from .security import api_key_auth, key_store
from .models import (
    GEXSummary,
    GEXByStrike,
    FlowPoint,
    FlowSeriesPoint,
    FlowContractsResponse,
    SmartMoneyFlowPoint,
    MomentumDivergencePoint,
    FlowBuyingPressurePoint,
    UnderlyingQuote,
    SessionCloses,
    HealthStatus,
    MaxPainCurrent,
    MaxPainTimeseriesPoint,
    OptionQuote,
    OpenInterestRecord,
    OpenInterestResponse,
)
from .routers.trade_signals import router as trade_signals_router
from .routers.volatility_gauge import router as volatility_gauge_router
from .routers.option_contract import router as option_contract_router
from .routers.option_calculator import router as option_calculator_router
from .routers.vol_surface import router as vol_surface_router

# Logging is configured centrally in src.utils.logging; importing
# get_logger triggers _configure_logging which honors LOG_LEVEL and
# LOG_FORMAT and installs the request-id filter. We must NOT call
# logging.basicConfig() here — it would wipe the centralized handler
# and the structured/request-id format would silently revert to plain.
from src.utils import get_logger  # noqa: E402

logger = get_logger(__name__)

# Database manager
db_manager: Optional[DatabaseManager] = None


def _parse_cors_origins(raw_origins: Optional[str]) -> List[str]:
    """Parse comma-separated origins from env var into a normalized list.

    When ``ENVIRONMENT=production`` the wildcard ``"*"`` is refused — any
    production deployment must explicitly list its allowed origins so an
    accidentally-empty env var can't open the API to every cross-origin
    caller on the internet.
    """
    origins: List[str] = []
    if raw_origins:
        origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]

    environment = os.getenv("ENVIRONMENT", "development").strip().lower()
    if not origins:
        if environment == "production":
            raise RuntimeError(
                "CORS_ALLOW_ORIGINS is unset and ENVIRONMENT=production; "
                "refusing to start with wildcard CORS.  Set CORS_ALLOW_ORIGINS "
                "to an explicit comma-separated list of allowed origins."
            )
        return ["*"]

    if "*" in origins and environment == "production":
        raise RuntimeError(
            "CORS_ALLOW_ORIGINS contains '*' and ENVIRONMENT=production; "
            "wildcard CORS is not permitted in production."
        )
    return origins


async def _max_pain_refresh_loop() -> None:
    """Periodically refresh max_pain_oi_snapshot rows off the request path.

    The per-symbol recompute is heavy (>30s for SPY/SPX/QQQ) and previously
    ran inline on every /api/max-pain/current request, which triggered
    pool-reconnect storms.  This task moves the work into a fixed-cadence
    background loop so the endpoint becomes a pure cache read.

    Errors per-cycle are caught and logged; the loop keeps running.
    """
    interval = config.MAX_PAIN_BACKGROUND_REFRESH_INTERVAL_SECONDS
    symbols = config.MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS
    strike_limit = config.MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT
    statement_timeout_ms = config.MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS
    logger.info(
        "max-pain background refresh loop starting: symbols=%s interval=%ds "
        "strike_limit=%d statement_timeout=%dms",
        symbols,
        interval,
        strike_limit,
        statement_timeout_ms,
    )
    while True:
        # Sleep first so we don't block startup with an immediate heavy refresh.
        # The endpoint serves whatever is already in max_pain_oi_snapshot until
        # the first tick completes.
        await asyncio.sleep(interval)
        try:
            await db_manager.refresh_max_pain_snapshots(symbols, strike_limit, statement_timeout_ms)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("max-pain background refresh cycle failed; will retry")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global db_manager

    # Startup
    logger.info("Starting ZeroGEX API Server...")
    db_manager = DatabaseManager()
    await db_manager.connect()
    logger.info("Database connected successfully")

    # Wire the per-user API-key store to the live DB pool so api_key_auth()
    # can validate keys against the api_keys table.  Static API_KEY env-var
    # auth (if set) keeps working alongside this.
    #
    # Pass a *getter* rather than the pool itself: if DatabaseManager later
    # reconnects (replacing self.pool), the key store picks up the new pool
    # on the next lookup instead of holding a stale, closed reference.
    key_store.configure(lambda: db_manager.pool)

    max_pain_task: Optional[asyncio.Task] = None
    if config.MAX_PAIN_BACKGROUND_REFRESH_ENABLED and config.MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS:
        max_pain_task = asyncio.create_task(_max_pain_refresh_loop(), name="max_pain_refresh_loop")

    yield

    # Shutdown
    logger.info("Shutting down ZeroGEX API Server...")
    if max_pain_task is not None:
        max_pain_task.cancel()
        with suppress(asyncio.CancelledError):
            await max_pain_task
    key_store.configure(None)
    if db_manager:
        await db_manager.disconnect()
    logger.info("Shutdown complete")


# Create FastAPI app.
#
# The global dependency enforces API-key auth when API_KEY is set in the
# environment; when unset, the dependency is a no-op so local development
# and CI continue to work without credentials.
app = FastAPI(
    title="ZeroGEX API",
    description="Real-time options analytics API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
    dependencies=[Depends(api_key_auth)],
    # Alphabetize endpoints within each tag in the Swagger UI. Operations
    # (HTTP methods) are also sorted so per-path groups render in a stable
    # order. `tagsSorter` keeps the tag list itself alphabetical.
    swagger_ui_parameters={
        "operationsSorter": "alpha",
        "tagsSorter": "alpha",
    },
    openapi_tags=[
        {"name": "Health", "description": "API and database health checks"},
        {"name": "GEX", "description": "Gamma Exposure (GEX) analytics"},
        {"name": "Options Flow", "description": "Options flow and buying pressure data"},
        {"name": "Market Data", "description": "Underlying and option quote data"},
        {"name": "Max Pain", "description": "Max pain analysis"},
        {
            "name": "Technicals",
            "description": "Intraday technical signals: VWAP, ORB, dealer hedging, volume, momentum",
        },
        {
            "name": "Tools",
            "description": (
                "Trader-facing calculators and what-if utilities. "
                "Includes the option-calculator (intrinsic-value P&L fan across "
                "underlying-price moves)."
            ),
        },
        {
            "name": "Trade Signals",
            "description": (
                "Options-structure signal engine: composite Market State Index (MSI) gauge, "
                "advanced signal components (vol-expansion, eod-pressure, squeeze-setup, "
                "trap-detection, 0dte-position-imbalance, gamma-vwap-confluence, "
                "range-break-imminence), per-component event history with realized returns, "
                "a pairwise confluence matrix, and live/historical trade records. "
                "Default symbol is SPY; pass ?symbol= or ?underlying= to override."
            ),
        },
    ],
)

# CORS middleware
cors_origins = _parse_cors_origins(os.getenv("CORS_ALLOW_ORIGINS"))
allow_credentials = "*" not in cors_origins
if not allow_credentials:
    logger.info(
        "CORS_ALLOW_ORIGINS contains '*'; disabling allow_credentials for standards compliance."
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compress responses so that large JSON payloads from endpoints like
# /api/flow/by-contract (which can return hundreds of thousands of rows for a
# full session) don't get bottlenecked on transfer.
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Request-ID propagation: every log line emitted while handling a request
# carries the id, and the same id is echoed back via X-Request-Id so
# clients (and server logs) can correlate.
app.add_middleware(RequestIdMiddleware)

app.include_router(trade_signals_router)
app.include_router(volatility_gauge_router)
app.include_router(option_contract_router)
app.include_router(option_calculator_router)
app.include_router(vol_surface_router)

# ============================================================================
# Health Check
# ============================================================================


@app.get("/api/health", response_model=HealthStatus, tags=["Health"])
async def health_check():
    """Check API and database health"""
    try:
        # Test database connection
        is_healthy = await db_manager.check_health()

        # Get data freshness
        last_quote = await db_manager.get_latest_quote()
        last_update = last_quote["timestamp"] if last_quote else None

        # Calculate data age
        data_age_seconds = None
        if last_update:
            et_tz = pytz.timezone("US/Eastern")
            now = datetime.now(et_tz)
            age = (now - last_update).total_seconds()
            data_age_seconds = int(age)

        return HealthStatus(
            status="healthy" if is_healthy else "degraded",
            database_connected=is_healthy,
            last_data_update=last_update,
            data_age_seconds=data_age_seconds,
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")


# ============================================================================
# GEX Endpoints
# ============================================================================


@app.get("/api/gex/summary", response_model=GEXSummary, tags=["GEX"])
@handle_api_errors("GET /api/gex/summary")
async def get_gex_summary(symbol: str = Query(default="SPY")):
    """Get latest GEX summary"""
    data = await db_manager.get_latest_gex_summary(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No GEX data available")
    return GEXSummary(**data)


@app.get("/api/gex/by-strike", response_model=List[GEXByStrike], tags=["GEX"])
@handle_api_errors("GET /api/gex/by-strike")
async def get_gex_by_strike(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=50, le=200),
    sort_by: str = Query(
        default="distance",
        pattern="^(distance|impact)$",
        description="Sort by 'distance' (closest to spot) or 'impact' (highest absolute net GEX)",
    ),
):
    """
    Get GEX breakdown by strike

    Returns detailed gamma exposure data including vanna/charm for each strike.

    - sort_by=distance: Returns strikes closest to current spot price (default)
    - sort_by=impact: Returns strikes with highest absolute net GEX (like 'make gex-strikes')
    """
    data = await db_manager.get_gex_by_strike(symbol, limit, sort_by)
    return [GEXByStrike(**row) for row in data]


@app.get("/api/gex/historical", response_model=List[GEXSummary], tags=["GEX"])
async def get_historical_gex(
    symbol: str = Query(default="SPY"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window_units: int = Query(default=90, ge=1, le=90),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
):
    """Get historical GEX data"""
    try:
        # Parse dates if provided
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        data = await db_manager.get_historical_gex(
            symbol, start_dt, end_dt, window_units, timeframe
        )
        return [GEXSummary(**row) for row in data]
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error fetching historical GEX: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/gex/heatmap", tags=["GEX"])
@handle_api_errors("GET /api/gex/heatmap")
async def get_gex_heatmap(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="5min"),
    window_units: int = Query(default=60, ge=1, le=300),
):
    """Get GEX heatmap data (strike x time)"""
    data = await db_manager.get_gex_heatmap(symbol, timeframe, window_units)
    return data or []


# ============================================================================
# Options Flow Endpoints
# ============================================================================


@app.get("/api/flow/by-contract", response_model=List[FlowPoint], tags=["Options Flow"])
@handle_api_errors("GET /api/flow/by-contract")
async def get_flow_by_contract(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$"),
    intervals: Optional[int] = Query(
        default=None,
        ge=1,
        le=390,
        description=(
            "Number of trailing 5-minute buckets to return. Defaults to the "
            "entire session (09:30–16:15 ET, ~81 buckets). Capped at 390 "
            "(one trading day at 1-minute resolution) to bound DB load."
        ),
    ),
):
    """Per-contract option flow in 5-min buckets with session-cumulative values.

    Returns one row per (option_type, strike, expiration) per 5-min bucket.
    raw_volume, raw_premium, net_volume and net_premium are day-to-date
    cumulative for each contract as of the end of its bucket; counters reset
    at 09:30 ET (TradeStation RTH open).

    session=current returns today's open session (or most recent if closed);
    session=prior returns the previous full session. Pass intervals=N to
    limit the response to the most recent N 5-minute buckets.
    """
    data = await db_manager.get_flow(symbol, session, intervals=intervals)
    return [FlowPoint(**row) for row in data]


_FLOW_SYMBOL_PATTERN = re.compile(r"^[A-Z.]{1,10}$")


def _parse_flow_strikes(raw: Optional[str]) -> Optional[List[float]]:
    """Parse the ?strikes= CSV into a list of floats.

    Silently drops unparseable entries. Returns ``None`` for missing or
    empty input (meaning "no strike filter"). Raises ``HTTPException(400)``
    only when every supplied entry is unparseable — an all-bad filter is a
    client error, not an accidental no-op.
    """
    if raw is None:
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None
    parts = [p.strip() for p in trimmed.split(",") if p.strip()]
    if not parts:
        return None
    parsed: List[float] = []
    for part in parts:
        try:
            value = float(part)
        except ValueError:
            continue
        if value != value or value in (float("inf"), float("-inf")):  # NaN / inf
            continue
        parsed.append(value)
    if not parsed:
        raise HTTPException(
            status_code=400, detail="strikes must contain at least one finite number"
        )
    return parsed


_FLOW_EXPIRATION_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_flow_expirations(raw: Optional[str]) -> Optional[List[date_type]]:
    """Parse the ?expirations= CSV into a list of dates.

    Silently drops entries that don't match ``YYYY-MM-DD`` or aren't real
    calendar dates. Returns ``None`` for missing/empty input. Raises 400
    only when every entry is unparseable.
    """
    if raw is None:
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None
    parts = [p.strip() for p in trimmed.split(",") if p.strip()]
    if not parts:
        return None
    parsed: List[date_type] = []
    for part in parts:
        if not _FLOW_EXPIRATION_PATTERN.match(part):
            continue
        try:
            parsed.append(date_type.fromisoformat(part))
        except ValueError:
            continue
    if not parsed:
        raise HTTPException(
            status_code=400,
            detail="expirations must contain at least one valid YYYY-MM-DD date",
        )
    return parsed


def _format_flow_series_row(row: dict) -> dict:
    """Coerce a raw DB row into the JSON shape documented in the spec.

    The timestamp fields are emitted as ``...Z`` (trailing-Z UTC) — spec
    requirement. Decimal/Numeric columns are cast to float so JSON callers
    don't have to care about asyncpg's native Decimal output.
    """
    bar_start: datetime = row["bar_start"]
    if bar_start.tzinfo is None:
        bar_start = bar_start.replace(tzinfo=pytz.UTC)
    else:
        bar_start = bar_start.astimezone(pytz.UTC)
    bar_end = bar_start + timedelta(minutes=5)
    fmt = "%Y-%m-%dT%H:%M:%SZ"

    def _to_float(v):
        return float(v) if v is not None else None

    def _to_int(v):
        return int(v) if v is not None else 0

    return {
        "timestamp": bar_start.strftime(fmt),
        "bar_start": bar_start.strftime(fmt),
        "bar_end": bar_end.strftime(fmt),
        "call_premium_cum": _to_float(row.get("call_premium_cum")) or 0.0,
        "put_premium_cum": _to_float(row.get("put_premium_cum")) or 0.0,
        "call_volume_cum": _to_int(row.get("call_volume_cum")),
        "put_volume_cum": _to_int(row.get("put_volume_cum")),
        "net_volume_cum": _to_int(row.get("net_volume_cum")),
        "raw_volume_cum": _to_int(row.get("raw_volume_cum")),
        "call_position_cum": _to_int(row.get("call_position_cum")),
        "put_position_cum": _to_int(row.get("put_position_cum")),
        "net_premium_cum": _to_float(row.get("net_premium_cum")) or 0.0,
        "put_call_ratio": _to_float(row.get("put_call_ratio")),
        "underlying_price": _to_float(row.get("underlying_price")),
        "contract_count": _to_int(row.get("contract_count")),
        "is_synthetic": bool(row.get("is_synthetic")),
    }


@app.get("/api/flow/series", response_model=List[FlowSeriesPoint], tags=["Options Flow"])
@handle_api_errors("GET /api/flow/series")
async def get_flow_series(
    symbol: str = Query(..., min_length=1, max_length=10),
    session: Literal["current", "prior"] = Query(default="current"),
    strikes: Optional[str] = Query(
        default=None,
        description="Comma-separated strikes to include. Empty/missing = all strikes.",
    ),
    expirations: Optional[str] = Query(
        default=None,
        description="Comma-separated YYYY-MM-DD expirations to include. Empty/missing = all.",
    ),
    intervals: Optional[int] = Query(
        default=None,
        ge=1,
        le=390,
        description=(
            "If provided, return only the last N 5-minute bars (tail window) "
            "for cheap incremental polling. A full regular session is 81 bars."
        ),
    ),
):
    """Server-accumulated flow series — one row per 5-minute bar.

    Returns cumulative call/put premium, volume, position, net volume, and
    put/call ratio per bar across all contracts matching the optional
    ``strikes``/``expirations`` filters. Rows are contiguous (quiet bars
    carry forward as synthetic rows flagged by ``is_synthetic``). Frontend
    renders this series directly — no client-side accumulators.

    Rows are ordered newest → oldest so ``rows[0]`` is the most recent bar.
    With ``intervals=N`` you get the leading N rows (the trailing-N most
    recent bars).

    ``session=current`` is the most recent ET trading day that has any data
    for the symbol; ``session=prior`` is the ET day immediately before that.
    Unknown symbols return 404; symbols that exist but have no data for the
    requested session return 200 with ``[]``.
    """
    normalized = symbol.strip().upper()
    if not _FLOW_SYMBOL_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must match [A-Z.]{1,10} (letters and dots only, up to 10 chars)",
        )

    strikes_list = _parse_flow_strikes(strikes)
    expirations_list = _parse_flow_expirations(expirations)

    rows = await db_manager.get_flow_series(
        symbol=normalized,
        session=session,
        strikes=strikes_list,
        expirations=expirations_list,
        intervals=intervals,
    )
    if rows is None:
        raise HTTPException(status_code=404, detail="symbol not found")
    return JSONResponse(content=[_format_flow_series_row(r) for r in rows])


@app.get("/api/flow/contracts", response_model=FlowContractsResponse, tags=["Options Flow"])
@handle_api_errors("GET /api/flow/contracts")
async def get_flow_contracts(
    symbol: str = Query(..., min_length=1, max_length=10),
    session: Literal["current", "prior"] = Query(default="current"),
):
    """Distinct strikes and expirations that traded in the resolved session.

    Powers the Strike / Expiration filter chips on the Flow Analysis page.
    Companion to ``/api/flow/series``: same session resolution, same 404
    semantics for unknown symbols.
    """
    normalized = symbol.strip().upper()
    if not _FLOW_SYMBOL_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must match [A-Z.]{1,10} (letters and dots only, up to 10 chars)",
        )

    result = await db_manager.get_flow_contracts(symbol=normalized, session=session)
    if result is None:
        raise HTTPException(status_code=404, detail="symbol not found")
    return FlowContractsResponse(**result)


@app.get("/api/flow/smart-money", response_model=List[SmartMoneyFlowPoint], tags=["Options Flow"])
@handle_api_errors("GET /api/flow/smart-money")
async def get_smart_money_flow(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$"),
    limit: int = Query(default=50, ge=1, le=50),
):
    """Get unusual activity / smart money flow — 1-min intervals.
    Session runs 07:15–16:15 ET. session=current returns today's open session (or most recent if closed); session=prior returns the previous full session.
    """
    data = await db_manager.get_smart_money_flow(symbol, session, min(limit, 50))
    return [SmartMoneyFlowPoint(**row) for row in data]


@app.get(
    "/api/flow/buying-pressure", response_model=List[FlowBuyingPressurePoint], tags=["Options Flow"]
)
@handle_api_errors("GET /api/flow/buying-pressure")
async def get_flow_buying_pressure(
    symbol: str = Query(default="SPY"), limit: int = Query(default=20, ge=1, le=500)
):
    """Get underlying buying/selling pressure"""
    data = await db_manager.get_flow_buying_pressure(symbol, limit)
    return [FlowBuyingPressurePoint(**row) for row in data] if data else []


# ============================================================================
# Market Session Helper
# ============================================================================

from src.market_calendar import ET as _ET, NYSE_HOLIDAYS as _NYSE_HOLIDAYS

_SOFT_CLOSE_WINDOW = timedelta(seconds=30)

if not _NYSE_HOLIDAYS:
    logger.warning("NYSE_HOLIDAYS env var is empty — no holiday filtering will occur")


class _SoftCloseTracker:
    """Rolling window of the last 3 close prices for a symbol.

    Used to evaluate soft-close stability: if the last 3 consecutive
    price observations are all identical the price is considered stable
    and the session can transition to 'closed'.
    """

    __slots__ = ("_prices",)

    def __init__(self) -> None:
        self._prices: deque = deque(maxlen=3)

    def record(self, price) -> None:
        if price is not None:
            self._prices.append(price)

    def is_stable(self) -> bool:
        """True when 3 consecutive identical prices have been observed."""
        return len(self._prices) >= 3 and len(set(self._prices)) == 1


# Per-symbol soft-close trackers (populated lazily on first quote request)
_soft_close_trackers: dict[str, _SoftCloseTracker] = {}
_SOFT_CLOSE_TRACKER_MAX = 100  # prevent unbounded growth


def get_market_session(asset_type: Optional[str], price_is_stable: bool = False) -> str:
    """Return the current US equity market session label.

    Session boundaries (all times US/Eastern, exact to the second):

      Both types
        < 04:00:00            closed
        >= 20:00:30           closed
        weekends / holidays   closed

      non-INDEX only
        04:00:00 – 09:29:59   pre-market
        16:00:00 – 19:59:59   after-hours
        20:00:00 – 20:00:29   after-hours (soft close: closed once price_is_stable)

      INDEX only
        16:00:00 – 16:00:29   open (soft close: closed once price_is_stable)
        16:00:30 – 19:59:59   closed

      Both types
        09:30:00 – 15:59:59   open
    """
    now_et = datetime.now(_ET)
    today = now_et.date()

    if today.weekday() >= 5 or today in _NYSE_HOLIDAYS:
        return "closed"

    def _boundary(h: int, m: int, s: int = 0) -> datetime:
        return _ET.localize(datetime(today.year, today.month, today.day, h, m, s))

    pre_open_dt = _boundary(4, 0)
    market_open_dt = _boundary(9, 30)
    market_close_dt = _boundary(16, 0)
    ah_close_dt = _boundary(20, 0)

    is_index = asset_type == "INDEX"

    # Before pre-market
    if now_et < pre_open_dt:
        return "closed"

    # Pre-market (non-INDEX only)
    if pre_open_dt <= now_et < market_open_dt:
        return "pre-market" if not is_index else "closed"

    # Cash session — open for both types
    if market_open_dt <= now_et < market_close_dt:
        return "open"

    # Soft-close window at market close
    if market_close_dt <= now_et < market_close_dt + _SOFT_CLOSE_WINDOW:
        if is_index:
            # INDEX: soft close from 16:00:00 — closed once price is stable
            return "closed" if price_is_stable else "open"
        else:
            # non-INDEX: hard transition to after-hours at exactly 16:00:00
            return "after-hours"

    # [16:00:30, 20:00:00) window
    if market_close_dt + _SOFT_CLOSE_WINDOW <= now_et < ah_close_dt:
        return "closed" if is_index else "after-hours"

    # Soft-close window at after-hours close (non-INDEX only)
    if ah_close_dt <= now_et < ah_close_dt + _SOFT_CLOSE_WINDOW:
        if is_index:
            return "closed"
        return "closed" if price_is_stable else "after-hours"

    return "closed"


# ============================================================================
# Market Data Endpoints
# ============================================================================


@app.get(
    "/api/market/quote",
    response_model=UnderlyingQuote,
    response_model_exclude_none=True,
    tags=["Market Data"],
)
async def get_current_quote(symbol: str = Query(default="SPY")):
    """Get current underlying quote"""
    try:
        data = await db_manager.get_latest_quote(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="No quote data available")

        data = dict(data)
        asset_type = data.pop("asset_type", None)
        if "cumulative_daily_volume" in data:
            data["volume"] = data.pop("cumulative_daily_volume")

        # Update per-symbol soft-close tracker and evaluate stability
        # Evict oldest entries if tracker dict grows too large
        if (
            symbol not in _soft_close_trackers
            and len(_soft_close_trackers) >= _SOFT_CLOSE_TRACKER_MAX
        ):
            oldest_key = next(iter(_soft_close_trackers))
            del _soft_close_trackers[oldest_key]
        tracker = _soft_close_trackers.setdefault(symbol, _SoftCloseTracker())
        tracker.record(data.get("close"))

        data["session"] = get_market_session(asset_type, tracker.is_stable())
        return UnderlyingQuote(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching quote: {e!r}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/market/session-closes", response_model=SessionCloses, tags=["Market Data"])
@handle_api_errors("GET /api/market/session-closes")
async def get_session_closes(symbol: str = Query(default="SPY")):
    """
    Get the two most recently completed regular session closes.

    - current_session_close: the most recent cash session close (last bar <= 16:00 ET
      on the most recent completed trading day).
    - prior_session_close: the session close immediately before current.
    """
    data = await db_manager.get_session_closes(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No session close data available")
    return SessionCloses(**data)


@app.get("/api/market/historical", response_model=List[UnderlyingQuote], tags=["Market Data"])
async def get_historical_quotes(
    symbol: str = Query(default="SPY"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window_units: int = Query(default=192, ge=1, le=576),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
):
    """Get historical quotes"""
    try:
        # Parse dates if provided
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        data = await db_manager.get_historical_quotes(
            symbol, start_dt, end_dt, window_units, timeframe
        )
        return [UnderlyingQuote(**row) for row in data]
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error fetching historical quotes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/option/quote", response_model=OptionQuote, tags=["Market Data"])
async def get_option_quote(
    underlying: str = Query(default="SPY", description="Underlying symbol, e.g. SPY"),
    strike: Optional[float] = Query(default=None, description="Strike price"),
    expiration: Optional[str] = Query(default=None, description="Expiration date (YYYY-MM-DD)"),
    type: Optional[Literal["C", "P"]] = Query(
        default=None, description="Option type: C for Call, P for Put"
    ),
):
    """Get the most recent quote for a specific option contract"""
    try:
        data = await db_manager.get_option_quote(underlying, strike, expiration, type)
        if not data:
            raise HTTPException(status_code=404, detail="No option quote data available")
        return OptionQuote(**data)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid parameter: {e}")
    except Exception as e:
        logger.error(f"Error fetching option quote: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/market/open-interest", response_model=OpenInterestResponse, tags=["Market Data"])
@handle_api_errors("GET /api/market/open-interest")
async def get_open_interest(
    underlying: str = Query(default="SPY", description="Underlying symbol, e.g. SPY"),
):
    """Get current open interest for each option contract for the underlying.

    Returns one record per (strike, expiration, option_type) from the most recent
    option chain snapshot, ordered by expiration, strike, and option type.
    """
    data = await db_manager.get_open_interest(underlying)
    if not data or not data.get("contracts"):
        raise HTTPException(status_code=404, detail="No open interest data available")
    return OpenInterestResponse(
        underlying=data["underlying"],
        spot_price=data["spot_price"],
        contracts=[OpenInterestRecord(**row) for row in data["contracts"]],
    )


@app.get("/api/max-pain/timeseries", response_model=List[MaxPainTimeseriesPoint], tags=["Max Pain"])
@handle_api_errors("GET /api/max-pain/timeseries")
async def get_max_pain_timeseries(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="5min"),
    window_units: int = Query(default=90, ge=1, le=300),
):
    """Get max pain over time aggregated by timeframe."""
    data = await db_manager.get_max_pain_timeseries(symbol, timeframe, window_units)
    return [MaxPainTimeseriesPoint(**row) for row in data]


@app.get("/api/max-pain/current", response_model=MaxPainCurrent, tags=["Max Pain"])
@handle_api_errors("GET /api/max-pain/current")
async def get_max_pain_current(
    symbol: str = Query(default="SPY"), strike_limit: int = Query(default=200, ge=10, le=1000)
):
    """Get current max pain and strike-by-strike call/put payout notional."""
    data = await db_manager.get_max_pain_current(symbol, strike_limit)
    if not data:
        raise HTTPException(status_code=404, detail="No max pain data available")
    return MaxPainCurrent(**data)


# ============================================================================
# Technicals Endpoints
# ============================================================================


_TECHNICALS_SYMBOL_PATTERN = re.compile(r"^[A-Z.]{1,10}$")


@app.get("/api/technicals", tags=["Technicals"])
@handle_api_errors("GET /api/technicals")
async def get_technicals(
    symbol: str = Query(default="SPY", min_length=1, max_length=10),
    intervals: Optional[int] = Query(
        default=None,
        ge=1,
        le=192,
        description=(
            "If provided, return only the trailing N 5-minute bars (max "
            "192 = 16h, the full extended ETF session). Use this for "
            "cheap incremental polling."
        ),
    ),
):
    """Combined per 5-minute bar timeseries of VWAP deviation,
    opening-range breakout, unusual volume spikes (all classes), and
    momentum divergence — plus the underlying close — for the most
    recent session.

    Session window depends on ``symbols.asset_type``:
      - ``INDEX`` → 09:30–16:00 ET (cash session only)
      - otherwise (ETF, EQUITY) → 04:00–20:00 ET (extended hours)

    Each bar represents a 5-minute bucket; ``timestamp`` is the START
    of the bucket (e.g. 10:30 → 10:30:00–10:34:59). The bar aggregates
    whichever 1-minute underlying bars have landed in the bucket:
    ``close`` is the latest 1-minute close, volumes are summed,
    ``high`` / ``low`` use max / min. While the 5-minute window is
    still active the bar updates as new 1-minute bars arrive; once the
    window closes the bar becomes immutable.

    ``bars`` is returned newest-first (``bars[0]`` is the most recent
    5-minute bucket), matching the convention used by the other
    timeseries endpoints.

    Cash indices have no native volume; VWAP and volume-spike rolling
    stats are computed against a proxy ETF's per-bar volume when one
    is configured (SPX→SPY, NDX→QQQ, RUT→IWM, DJX→DIA). The active
    proxy is reported in the top-level ``volume_proxy`` field;
    ``null`` for equities/ETFs.

    The "most recent session" is the trading day of the latest bar in
    ``underlying_quotes`` for the symbol — i.e. the live session if
    it's in progress, otherwise the most recent completed session.
    Bars before 09:30 ET have ``opening_range`` fields nulled out (the
    ORB window is 09:30–09:59 ET, so it doesn't exist yet).

    ORB anchor: ``orb_high`` / ``orb_low`` come from the most recent
    ET date that has cash-session data (>= 09:30 ET), which can differ
    from ``session_date`` for ETFs in pre-market. While in pre-market
    of a new trading day, the response shows the previous session's
    ORB through every pre-market bar; once today's 09:30 ET data
    arrives, ORB switches to today's values. INDEX symbols never carry
    pre-market data, so ORB and session always agree for them.

    Pass ``intervals=N`` to get only the last N 5-minute buckets
    (trailing from the most recent existing bar). The response
    metadata (``session_start_et`` / ``session_end_et``) still reports
    the canonical session boundaries; ``bars`` is the trimmed tail.

    404 when ``symbol`` isn't in the ``symbols`` table; 200 with an
    empty ``bars`` list when the symbol exists but has no quote data.

    Dealer hedging is intentionally excluded — its underlying view is
    a point-in-time snapshot, not a timeseries. Use
    ``/api/technicals/dealer-hedging`` for the current-state read.
    """
    normalized = symbol.strip().upper()
    if not _TECHNICALS_SYMBOL_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must match [A-Z.]{1,10} (letters and dots only, up to 10 chars)",
        )

    result = await db_manager.get_technicals_timeseries(normalized, intervals=intervals)
    if result is None:
        raise HTTPException(status_code=404, detail="symbol not found")
    return result


@app.get("/api/technicals/vwap-deviation", tags=["Technicals"])
@handle_api_errors("GET /api/technicals/vwap-deviation")
async def get_vwap_deviation(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
):
    """Get VWAP deviation for mean reversion signals"""
    return await db_manager.get_vwap_deviation(symbol, timeframe, window_units)


@app.get("/api/technicals/opening-range", tags=["Technicals"])
@handle_api_errors("GET /api/technicals/opening-range")
async def get_opening_range(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
):
    """Get opening range breakout status"""
    return await db_manager.get_opening_range_breakout(symbol, timeframe, window_units)


@app.get("/api/technicals/dealer-hedging", tags=["Technicals"])
@handle_api_errors("GET /api/technicals/dealer-hedging")
async def get_dealer_hedging(symbol: str = Query(default="SPY")):
    """Get current dealer hedging pressure (point-in-time snapshot).

    The underlying view aggregates the latest snapshot of every option
    contract (delta × open interest × 100) on the symbol to produce a
    single ``expected_hedge_shares`` figure — the net share position
    market makers would have to be long to be delta-neutral against
    current option open interest. ``hedge_pressure`` classifies that
    figure as 🟢 Heavy Buy-Hedging Risk (< -1M), 🔴 Heavy Sell-Hedging
    Risk (> +1M), or ⚪ Balanced Hedging.

    Returns at most one row per symbol — this is not a timeseries.
    """
    return await db_manager.get_dealer_hedging_pressure(symbol)


@app.get("/api/technicals/volume-spikes", tags=["Technicals"])
@handle_api_errors("GET /api/technicals/volume-spikes")
async def get_volume_spikes(
    symbol: str = Query(default="SPY"), limit: int = Query(default=20, le=100)
):
    """Get unusual volume spikes"""
    return await db_manager.get_unusual_volume_spikes(symbol, limit)


@app.get(
    "/api/technicals/momentum-divergence",
    response_model=List[MomentumDivergencePoint],
    tags=["Technicals"],
)
@handle_api_errors("GET /api/technicals/momentum-divergence")
async def get_momentum_divergence(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
):
    """Get momentum divergence signals"""
    data = await db_manager.get_momentum_divergence(symbol, timeframe, window_units)
    return [MomentumDivergencePoint(**row) for row in data]


# ============================================================================
# Error Handlers
# ============================================================================


@app.exception_handler(404)
async def not_found_handler(request, exc):
    detail = getattr(exc, "detail", None)
    if not detail or detail == "Not Found":
        detail = "Endpoint not found"
    return JSONResponse(status_code=404, content={"detail": detail})


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
