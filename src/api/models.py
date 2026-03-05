"""
Pydantic models for API request/response validation
"""

from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional, Dict
from decimal import Decimal


class GEXSummary(BaseModel):
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
    time_window_start: datetime
    time_window_end: datetime
    interval_timestamp: Optional[datetime] = None
    symbol: str
    option_type: Optional[str] = None
    strike: Optional[Decimal] = None
    total_volume: int
    total_premium: Decimal
    avg_iv: Optional[Decimal] = None
    net_delta: Optional[Decimal] = None
    sentiment: Optional[str] = None
    unusual_activity_score: Optional[Decimal] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class FlowByTypeBucket(BaseModel):
    time_window_start: datetime
    time_window_end: datetime
    timestamp: datetime
    symbol: str
    total_volume: Dict[str, int]
    total_premium: Dict[str, Decimal]


class FlowByStrikeBucket(BaseModel):
    time_window_start: datetime
    time_window_end: datetime
    timestamp: datetime
    symbol: str
    strike: Decimal
    total_volume: Dict[str, int]
    total_premium: Dict[str, Decimal]


class FlowByExpiryBucket(BaseModel):
    time_window_start: datetime
    time_window_end: datetime
    timestamp: datetime
    symbol: str
    total_volume: Dict[str, int]
    total_premium: Dict[str, Decimal]


class UnderlyingQuote(BaseModel):
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
    symbol: str
    previous_close: Decimal
    timestamp: datetime

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class MaxPainPoint(BaseModel):
    expiration: date | None = None
    settlement_price: Decimal
    call_notional: Decimal
    put_notional: Decimal
    total_notional: Decimal


class MaxPainExpiration(BaseModel):
    expiration: date
    max_pain: Decimal
    difference_from_underlying: Decimal
    strikes: list[MaxPainPoint]


class MaxPainCurrent(BaseModel):
    timestamp: datetime
    symbol: str
    underlying_price: Decimal
    max_pain: Decimal
    difference: Decimal
    expirations: list[MaxPainExpiration]


class MaxPainTimeseriesPoint(BaseModel):
    timestamp: datetime
    symbol: str
    max_pain: Decimal


class HealthStatus(BaseModel):
    status: str = Field(..., description="healthy, degraded, or unhealthy")
    database_connected: bool
    last_data_update: Optional[datetime] = None
    data_age_seconds: Optional[int] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
        }
