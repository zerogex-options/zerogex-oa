#!/usr/bin/env python3
"""
ZeroGEX API Server
FastAPI backend for serving analytics data to the frontend
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, date as date_type
import logging
import os
from typing import List, Optional, Literal
import pytz

from .database import DatabaseManager
from .models import (
    GEXSummary,
    GEXByStrike,
    FlowByTypePoint,
    FlowByStrikePoint,
    FlowByExpirationPoint,
    SmartMoneyFlowPoint,
    MomentumDivergencePoint,
    FlowBuyingPressurePoint,
    UnderlyingQuote,
    SessionCloses,
    HealthStatus,
    MaxPainCurrent,
    MaxPainTimeseriesPoint,
    OptionQuote,
)
from .routers.trade_signals import router as trade_signals_router
from .routers.volatility_gauge import router as volatility_gauge_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database manager
db_manager: Optional[DatabaseManager] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global db_manager

    # Startup
    logger.info("Starting ZeroGEX API Server...")
    db_manager = DatabaseManager()
    await db_manager.connect()
    logger.info("Database connected successfully")

    yield

    # Shutdown
    logger.info("Shutting down ZeroGEX API Server...")
    if db_manager:
        await db_manager.disconnect()
    logger.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="ZeroGEX API",
    description="Real-time options analytics API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Health", "description": "API and database health checks"},
        {"name": "GEX", "description": "Gamma Exposure (GEX) analytics"},
        {"name": "Options Flow", "description": "Options flow and buying pressure data"},
        {"name": "Market Data", "description": "Underlying and option quote data"},
        {"name": "Max Pain", "description": "Max pain analysis"},
        {"name": "Day Trading", "description": "Intraday trading signals: VWAP, ORB, dealer hedging, volume, momentum"},
        {"name": "Trade Signals", "description": "Composite trade signal generation"},
        {"name": "Volatility", "description": "Volatility gauge and regime analysis"},
    ]
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trade_signals_router)
app.include_router(volatility_gauge_router)

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
        last_update = last_quote['timestamp'] if last_quote else None

        # Calculate data age
        data_age_seconds = None
        if last_update:
            et_tz = pytz.timezone('US/Eastern')
            now = datetime.now(et_tz)
            age = (now - last_update).total_seconds()
            data_age_seconds = int(age)

        return HealthStatus(
            status="healthy" if is_healthy else "degraded",
            database_connected=is_healthy,
            last_data_update=last_update,
            data_age_seconds=data_age_seconds
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

# ============================================================================
# GEX Endpoints
# ============================================================================

@app.get("/api/gex/summary", response_model=GEXSummary, tags=["GEX"])
async def get_gex_summary(symbol: str = Query(default="SPY")):
    """Get latest GEX summary"""
    try:
        data = await db_manager.get_latest_gex_summary(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="No GEX data available")

        return GEXSummary(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching GEX summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/gex/by-strike", response_model=List[GEXByStrike], tags=["GEX"])
async def get_gex_by_strike(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=50, le=200),
    sort_by: str = Query(
        default="distance",
        pattern="^(distance|impact)$",
        description="Sort by 'distance' (closest to spot) or 'impact' (highest absolute net GEX)"
    )
):
    """
    Get GEX breakdown by strike

    Returns detailed gamma exposure data including vanna/charm for each strike.

    - sort_by=distance: Returns strikes closest to current spot price (default)
    - sort_by=impact: Returns strikes with highest absolute net GEX (like 'make gex-strikes')
    """
    try:
        data = await db_manager.get_gex_by_strike(symbol, limit, sort_by)
        if not data:
            raise HTTPException(status_code=404, detail="No GEX data available")

        return [GEXByStrike(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching GEX by strike: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/gex/historical", response_model=List[GEXSummary], tags=["GEX"])
async def get_historical_gex(
    symbol: str = Query(default="SPY"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window_units: int = Query(default=90, ge=1, le=90),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min")
):
    """Get historical GEX data"""
    try:
        # Parse dates if provided
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        data = await db_manager.get_historical_gex(symbol, start_dt, end_dt, window_units, timeframe)
        if not data:
            raise HTTPException(status_code=404, detail="No historical data available")

        return [GEXSummary(**row) for row in data]
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error fetching historical GEX: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/gex/heatmap", tags=["GEX"])
async def get_gex_heatmap(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="5min"),
    window_units: int = Query(default=60, ge=1, le=90)
):
    """Get GEX heatmap data (strike x time)"""
    try:
        data = await db_manager.get_gex_heatmap(symbol, timeframe, window_units)
        if not data:
            raise HTTPException(status_code=404, detail="No GEX heatmap data available")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching GEX heatmap: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Options Flow Endpoints
# ============================================================================

@app.get("/api/flow/by-type", response_model=List[FlowByTypePoint], tags=["Options Flow"])
async def get_flow_by_type(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$")
):
    """Get option flow by type (calls vs puts) — 1-min intervals.
    session=current returns today's open session (or most recent if closed); session=prior returns the previous full session."""
    try:
        data = await db_manager.get_flow_by_type(symbol, session)
        return [FlowByTypePoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching flow by type: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/flow/by-strike", response_model=List[FlowByStrikePoint], tags=["Options Flow"])
async def get_flow_by_strike(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$"),
    limit: int = Query(default=20, ge=1, le=50000)
):
    """Get option flow by strike level — 1-min intervals.
    session=current returns today's open session (or most recent if closed); session=prior returns the previous full session."""
    try:
        data = await db_manager.get_flow_by_strike(symbol, session, limit)
        return [FlowByStrikePoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching flow by strike: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/flow/by-expiration", response_model=List[FlowByExpirationPoint], tags=["Options Flow"])
async def get_flow_by_expiration(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$"),
    limit: int = Query(default=20, ge=1, le=50000)
):
    """Get option flow by expiration date — 1-min intervals.
    session=current returns today's open session (or most recent if closed); session=prior returns the previous full session."""
    try:
        data = await db_manager.get_flow_by_expiration(symbol, session, limit)
        return [FlowByExpirationPoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching flow by expiration: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/flow/smart-money", response_model=List[SmartMoneyFlowPoint], tags=["Options Flow"])
async def get_smart_money_flow(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$"),
    limit: int = Query(default=20, le=100)
):
    """Get unusual activity / smart money flow — 1-min intervals.
    session=current returns today's open session (or most recent if closed); session=prior returns the previous full session."""
    try:
        data = await db_manager.get_smart_money_flow(symbol, session, limit)
        return [SmartMoneyFlowPoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching smart money flow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/flow/buying-pressure", response_model=List[FlowBuyingPressurePoint], tags=["Options Flow"])
async def get_flow_buying_pressure(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=20, ge=1, le=500)
):
    """Get underlying buying/selling pressure"""
    try:
        data = await db_manager.get_flow_buying_pressure(symbol, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No buying pressure data available")

        return [FlowBuyingPressurePoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching buying pressure: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ============================================================================
# Market Session Helper
# ============================================================================

_ET = pytz.timezone("US/Eastern")
_SOFT_CLOSE_WINDOW = timedelta(seconds=30)


def _load_nyse_holidays() -> set[date_type]:
    """Load NYSE holiday dates from the NYSE_HOLIDAYS env var (comma-separated YYYY-MM-DD)."""
    raw = os.getenv("NYSE_HOLIDAYS", "")
    holidays: set[date_type] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            holidays.add(date_type.fromisoformat(token))
        except ValueError:
            logger.warning(f"Invalid date in NYSE_HOLIDAYS env var, skipping: {token!r}")
    if not holidays:
        logger.warning("NYSE_HOLIDAYS env var is empty — no holiday filtering will occur")
    return holidays


_NYSE_HOLIDAYS: set[date_type] = _load_nyse_holidays()


def _to_et(ts: datetime) -> datetime:
    """Normalize a datetime (UTC-aware or naive) to US/Eastern."""
    if ts.tzinfo is None:
        return _ET.localize(ts)
    return ts.astimezone(_ET)


def get_market_session(asset_type: Optional[str], latest_timestamp: Optional[datetime]) -> str:
    """Return the current US equity market session label.

    Boundaries (all times ET, exact to the second):
      pre-market   04:00:00 – 09:29:59   (non-INDEX only)
      open         09:30:00 – 15:59:59
      after-hours  16:00:00 – 19:59:59   (non-INDEX only)
      closed       all other times, weekends, NYSE holidays

    Soft-close behaviour:
      INDEX        : [16:00:00, 16:00:30) — stay "open" while latest quote
                     was updated at or after 16:00:00; otherwise "closed".
      non-INDEX    : [20:00:00, 20:00:30) — stay "after-hours" while latest
                     quote was updated at or after 20:00:00; otherwise "closed".
    """
    now_et = datetime.now(_ET)
    today = now_et.date()

    # Weekends and NYSE holidays are always closed
    if today.weekday() >= 5 or today in _NYSE_HOLIDAYS:
        return "closed"

    # Build exact-second ET boundary datetimes for today
    def _boundary(h: int, m: int, s: int = 0) -> datetime:
        return _ET.localize(datetime(today.year, today.month, today.day, h, m, s))

    pre_open_dt    = _boundary(4, 0)
    market_open_dt = _boundary(9, 30)
    market_close_dt = _boundary(16, 0)
    ah_close_dt    = _boundary(20, 0)

    is_index = asset_type == "INDEX"

    # Cash session — same for all instrument types
    if market_open_dt <= now_et < market_close_dt:
        return "open"

    if is_index:
        # Soft close: [16:00:00, 16:00:30)
        if market_close_dt <= now_et < market_close_dt + _SOFT_CLOSE_WINDOW:
            if latest_timestamp and _to_et(latest_timestamp) >= market_close_dt:
                return "open"
            return "closed"
        return "closed"

    # Non-index extended sessions
    if pre_open_dt <= now_et < market_open_dt:
        return "pre-market"

    if market_close_dt <= now_et < ah_close_dt:
        return "after-hours"

    # Soft close: [20:00:00, 20:00:30)
    if ah_close_dt <= now_et < ah_close_dt + _SOFT_CLOSE_WINDOW:
        if latest_timestamp and _to_et(latest_timestamp) >= ah_close_dt:
            return "after-hours"
        return "closed"

    return "closed"


# ============================================================================
# Market Data Endpoints
# ============================================================================

@app.get("/api/market/quote", response_model=UnderlyingQuote, tags=["Market Data"])
async def get_current_quote(symbol: str = Query(default="SPY")):
    """Get current underlying quote"""
    try:
        data = await db_manager.get_latest_quote(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="No quote data available")

        data = dict(data)
        asset_type = data.pop("asset_type", None)
        data["session"] = get_market_session(asset_type, data.get("timestamp"))
        return UnderlyingQuote(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching quote: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/market/session-closes", response_model=SessionCloses, tags=["Market Data"])
async def get_session_closes(symbol: str = Query(default="SPY")):
    """
    Get the two most recently completed regular session closes (4:00 PM ET bars).

    - current_session_close: last completed 4pm ET bar.
      During market hours Wednesday → Tuesday's close.
      During Wednesday after-hours or Thursday pre-market → Wednesday's close.
    - prior_session_close: the session close immediately before current_session_close.
    """
    try:
        data = await db_manager.get_session_closes(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="No session close data available")

        return SessionCloses(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching session closes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/market/historical", response_model=List[UnderlyingQuote], tags=["Market Data"])
async def get_historical_quotes(
    symbol: str = Query(default="SPY"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window_units: int = Query(default=90, ge=1, le=90),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min")
):
    """Get historical quotes"""
    try:
        # Parse dates if provided
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        data = await db_manager.get_historical_quotes(symbol, start_dt, end_dt, window_units, timeframe)
        if not data:
            raise HTTPException(status_code=404, detail="No historical data available")

        return [UnderlyingQuote(**row) for row in data]
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error fetching historical quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/option/quote", response_model=OptionQuote, tags=["Market Data"])
async def get_option_quote(
    underlying: str = Query(default="SPY", description="Underlying symbol, e.g. SPY"),
    strike: Optional[float] = Query(default=None, description="Strike price"),
    expiration: Optional[str] = Query(default=None, description="Expiration date (YYYY-MM-DD)"),
    type: Optional[Literal["C", "P"]] = Query(default=None, description="Option type: C for Call, P for Put"),
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
        logger.error(f"Error fetching option quote: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/max-pain/timeseries", response_model=List[MaxPainTimeseriesPoint], tags=["Max Pain"])
async def get_max_pain_timeseries(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="5min"),
    window_units: int = Query(default=90, ge=1, le=90)
):
    """Get max pain over time aggregated by timeframe."""
    try:
        data = await db_manager.get_max_pain_timeseries(symbol, timeframe, window_units)
        if not data:
            raise HTTPException(status_code=404, detail="No max pain data available")
        return [MaxPainTimeseriesPoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching max pain timeseries: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/max-pain/current", response_model=MaxPainCurrent, tags=["Max Pain"])
async def get_max_pain_current(
    symbol: str = Query(default="SPY"),
    strike_limit: int = Query(default=200, ge=10, le=1000)
):
    """Get current max pain and strike-by-strike call/put payout notional."""
    try:
        data = await db_manager.get_max_pain_current(symbol, strike_limit)
        if not data:
            raise HTTPException(status_code=404, detail="No max pain data available")
        return MaxPainCurrent(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching current max pain: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Day Trading Endpoints
# ============================================================================

@app.get("/api/trading/vwap-deviation", tags=["Day Trading"])
async def get_vwap_deviation(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90)
):
    """Get VWAP deviation for mean reversion signals"""
    try:
        data = await db_manager.get_vwap_deviation(symbol, timeframe, window_units)
        if not data:
            raise HTTPException(status_code=404, detail="No VWAP data available")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching VWAP deviation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/trading/opening-range", tags=["Day Trading"])
async def get_opening_range(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90)
):
    """Get opening range breakout status"""
    try:
        data = await db_manager.get_opening_range_breakout(symbol, timeframe, window_units)
        if not data:
            raise HTTPException(status_code=404, detail="No ORB data available")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching ORB: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/trading/dealer-hedging", tags=["Day Trading"])
async def get_dealer_hedging(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=20, le=100)
):
    """Get dealer hedging pressure"""
    try:
        data = await db_manager.get_dealer_hedging_pressure(symbol, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No hedging data available")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching dealer hedging: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/trading/volume-spikes", tags=["Day Trading"])
async def get_volume_spikes(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=20, le=100)
):
    """Get unusual volume spikes"""
    try:
        data = await db_manager.get_unusual_volume_spikes(symbol, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No volume data available")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching volume spikes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/trading/momentum-divergence", response_model=List[MomentumDivergencePoint], tags=["Day Trading"])
async def get_momentum_divergence(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90)
):
    """Get momentum divergence signals"""
    try:
        data = await db_manager.get_momentum_divergence(symbol, timeframe, window_units)
        if not data:
            raise HTTPException(status_code=404, detail="No divergence data available")
        return [MomentumDivergencePoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching momentum divergence: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")




# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(404)
async def not_found_handler(request, exc):
    detail = getattr(exc, "detail", None)
    if not detail or detail == "Not Found":
        detail = "Endpoint not found"
    return JSONResponse(
        status_code=404,
        content={"detail": detail}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
