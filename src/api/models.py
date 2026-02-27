"""
Pydantic models for API request/response validation
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from decimal import Decimal

class GEXSummary(BaseModel):
    """GEX Summary Response"""
    timestamp: datetime
    symbol: str
    spot_price: Decimal
    total_call_gex: Decimal
    total_put_gex: Decimal
    net_gex: Decimal
    gamma_flip: Optional[Decimal] = None
    max_pain: Optional[Decimal] = None
    call_wall: Optional[Decimal] = None
    put_wall: Optional[Decimal] = None
    total_call_oi: Optional[int] = None
    total_put_oi: Optional[int] = None
    put_call_ratio: Optional[Decimal] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }

class GEXByStrike(BaseModel):
    """GEX by Strike Response"""
    timestamp: datetime
    symbol: str
    strike: Decimal
    call_oi: int
    put_oi: int
    call_volume: int
    put_volume: int
    call_gex: Decimal
    put_gex: Decimal
    net_gex: Decimal
    spot_price: Decimal
    distance_from_spot: Decimal

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }

class OptionFlow(BaseModel):
    """Option Flow Response"""
    time_window_start: datetime
    time_window_end: datetime
    symbol: str
    option_type: Optional[str] = None  # 'CALL' or 'PUT'
    strike: Optional[Decimal] = None
    total_volume: int
    total_premium: Decimal
    avg_iv: Optional[Decimal] = None
    net_delta: Optional[Decimal] = None
    sentiment: Optional[str] = None  # 'bullish', 'bearish', 'neutral'
    unusual_activity_score: Optional[Decimal] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }

class UnderlyingQuote(BaseModel):
    """Underlying Quote Response"""
    timestamp: datetime
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Optional[int] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }

class PreviousClose(BaseModel):
    """Previous Close Response"""
    symbol: str
    previous_close: Decimal
    timestamp: datetime

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }

class HealthStatus(BaseModel):
    """Health Check Response"""
    status: str = Field(..., description="healthy, degraded, or unhealthy")
    database_connected: bool
    last_data_update: Optional[datetime] = None
    data_age_seconds: Optional[int] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
        }
