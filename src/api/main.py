#!/usr/bin/env python3
"""
ZeroGEX API Server
FastAPI backend for serving analytics data to the frontend
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import logging
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
    PreviousClose,
    HealthStatus,
    MaxPainCurrent,
    MaxPainTimeseriesPoint,
)

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
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Health Check
# ============================================================================

@app.get("/api/health", response_model=HealthStatus)
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

@app.get("/api/gex/summary", response_model=GEXSummary)
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

@app.get("/api/gex/by-strike", response_model=List[GEXByStrike])
async def get_gex_by_strike(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=50, le=200)
):
    """Get GEX breakdown by strike"""
    try:
        data = await db_manager.get_gex_by_strike(symbol, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No GEX data available")

        return [GEXByStrike(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching GEX by strike: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/gex/historical", response_model=List[GEXSummary])
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

@app.get("/api/gex/heatmap")
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

@app.get("/api/flow/by-type", response_model=List[FlowByTypePoint])
async def get_flow_by_type(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90)
):
    """Get option flow by type (calls vs puts)"""
    try:
        data = await db_manager.get_flow_by_type(symbol, timeframe, window_units)
        if not data:
            raise HTTPException(status_code=404, detail="No flow data available")

        return [FlowByTypePoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching flow by type: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/flow/by-strike", response_model=List[FlowByStrikePoint])
async def get_flow_by_strike(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
    limit: int = Query(default=20, ge=1, le=50000)
):
    """Get option flow by strike level"""
    try:
        data = await db_manager.get_flow_by_strike(symbol, timeframe, window_units, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No flow data available")

        return [FlowByStrikePoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching flow by strike: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/flow/by-expiration", response_model=List[FlowByExpirationPoint])
async def get_flow_by_expiration(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
    limit: int = Query(default=20, ge=1, le=50000)
):
    """Get option flow by expiration date"""
    try:
        data = await db_manager.get_flow_by_expiration(symbol, timeframe, window_units, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No flow data available")

        return [FlowByExpirationPoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching flow by expiration: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/flow/smart-money", response_model=List[SmartMoneyFlowPoint])
async def get_smart_money_flow(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
    limit: int = Query(default=20, le=100)
):
    """Get unusual activity / smart money flow"""
    try:
        data = await db_manager.get_smart_money_flow(symbol, timeframe, window_units, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No unusual activity detected")

        return [SmartMoneyFlowPoint(**row) for row in data]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching smart money flow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/flow/buying-pressure", response_model=List[FlowBuyingPressurePoint])
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
# Market Data Endpoints
# ============================================================================

@app.get("/api/market/quote", response_model=UnderlyingQuote)
async def get_current_quote(symbol: str = Query(default="SPY")):
    """Get current underlying quote"""
    try:
        data = await db_manager.get_latest_quote(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="No quote data available")

        return UnderlyingQuote(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching quote: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/market/previous-close", response_model=PreviousClose)
async def get_previous_close(symbol: str = Query(default="SPY")):
    """Get previous trading day's closing price"""
    try:
        data = await db_manager.get_previous_close(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="No previous close data available")

        return PreviousClose(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching previous close: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/market/historical", response_model=List[UnderlyingQuote])
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

@app.get("/api/max-pain/timeseries", response_model=List[MaxPainTimeseriesPoint])
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


@app.get("/api/max-pain/current", response_model=MaxPainCurrent)
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

@app.get("/api/trading/vwap-deviation")
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

@app.get("/api/trading/opening-range")
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

@app.get("/api/trading/gamma-levels")
async def get_gamma_levels(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=20, le=100)
):
    """Get gamma exposure levels (support/resistance)"""
    try:
        data = await db_manager.get_gamma_exposure_levels(symbol, limit)
        if not data:
            raise HTTPException(status_code=404, detail="No gamma data available")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching gamma levels: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/trading/dealer-hedging")
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

@app.get("/api/trading/volume-spikes")
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

@app.get("/api/trading/momentum-divergence", response_model=List[MomentumDivergencePoint])
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
    return JSONResponse(
        status_code=404,
        content={"detail": "Endpoint not found"}
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
