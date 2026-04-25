"""
Pydantic models for API request/response validation
"""

from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional
from decimal import Decimal


class GEXSummary(BaseModel):
    timestamp: datetime
    symbol: str
    spot_price: Decimal
    total_call_gex: Decimal
    total_put_gex: Decimal
    net_gex: Decimal
    gamma_flip: Optional[Decimal] = None
    flip_distance: Optional[Decimal] = None
    local_gex: Optional[Decimal] = None
    convexity_risk: Optional[Decimal] = None
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
    expiration: date
    call_oi: int
    put_oi: int
    call_volume: int
    put_volume: int
    call_gex: Decimal
    put_gex: Decimal
    net_gex: Decimal
    vanna_exposure: Optional[Decimal] = None
    charm_exposure: Optional[Decimal] = None
    spot_price: Decimal
    distance_from_spot: Decimal

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
            date: lambda v: v.isoformat() if v is not None else None,
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


class UnderlyingQuote(BaseModel):
    timestamp: datetime
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    up_volume: Optional[int] = None
    down_volume: Optional[int] = None
    volume: Optional[int] = None
    session: Optional[str] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class FlowCallPutTotals(BaseModel):
    puts: Decimal | int = 0
    calls: Decimal | int = 0


class FlowBucketResponse(BaseModel):
    timestamp: datetime
    symbol: str
    total_volume: FlowCallPutTotals
    total_premium: FlowCallPutTotals

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class FlowMapBucketResponse(BaseModel):
    timestamp: datetime
    symbol: str
    total_volume: dict[str, int]
    total_premium: dict[str, float]

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class FlowPoint(BaseModel):
    """Per-contract 5-min-bucketed flow row with session-cumulative values.

    One row per (option_type, strike, expiration) per 5-min bucket. Values
    are day-to-date cumulative for THIS contract as of the end of the
    bucket, with the session resetting at 09:30 ET (TradeStation RTH open).

    raw_volume / raw_premium: total session volume and flow-weighted premium
    regardless of buy/sell direction.
    net_volume / net_premium: session buys minus sells (classified via the
    ask/bid volume ratio from each tick), scaled so unclassified volume is
    attributed proportionally.
    """

    timestamp: datetime
    symbol: str
    option_type: str
    strike: Decimal
    expiration: date
    dte: int
    raw_volume: int
    raw_premium: Decimal
    net_volume: int
    net_premium: Decimal
    underlying_price: Optional[Decimal] = None


class FlowSeriesPoint(BaseModel):
    """Server-accumulated 5-minute flow bar from /api/flow/series.

    One row per bar from 09:30 ET through the latest bar covered by the
    resolved session. Carry-forward synthetic rows fill quiet bars so the
    series is contiguous — the ``is_synthetic`` flag distinguishes them.
    """

    timestamp: str
    bar_start: str
    bar_end: str
    call_premium_cum: float
    put_premium_cum: float
    call_volume_cum: int
    put_volume_cum: int
    net_volume_cum: int
    raw_volume_cum: int
    call_position_cum: int
    put_position_cum: int
    net_premium_cum: float
    put_call_ratio: Optional[float] = None
    underlying_price: Optional[float] = None
    contract_count: int
    is_synthetic: bool


class FlowContractsResponse(BaseModel):
    """Distinct strikes and expirations that traded in the resolved session."""

    strikes: list[float]
    expirations: list[str]


class SmartMoneyFlowPoint(BaseModel):
    timestamp: datetime
    symbol: str
    contract: str
    strike: Decimal
    expiration: date
    dte: int
    option_type: str
    flow: int
    notional: Decimal
    trade_side: str
    delta: Optional[Decimal] = None
    score: Optional[Decimal] = None
    notional_class: str
    size_class: str
    underlying_price: Optional[Decimal] = None


class MomentumDivergencePoint(BaseModel):
    timestamp: datetime
    symbol: str
    price: Decimal
    chg_5m: Decimal
    opt_flow: Decimal
    divergence_signal: str


class FlowBuyingPressurePoint(BaseModel):
    timestamp: datetime
    symbol: str
    price: Decimal
    volume: int
    buy_pct: Decimal
    period_buy_pct: Decimal
    price_chg: Optional[Decimal] = None
    momentum: str


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


class SessionCloses(BaseModel):
    symbol: str
    current_session_close: Decimal
    current_session_close_ts: Optional[datetime]
    prior_session_close: Decimal
    prior_session_close_ts: Optional[datetime]

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


class OptionQuote(BaseModel):
    timestamp: datetime
    underlying: str
    strike: Decimal
    expiration: date
    option_type: str
    bid: Optional[Decimal] = None
    ask: Optional[Decimal] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class OpenInterestRecord(BaseModel):
    timestamp: datetime
    underlying: str
    strike: Decimal
    expiration: date
    option_type: str
    open_interest: int
    exposure: Decimal
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
            date: lambda v: v.isoformat() if v is not None else None,
        }


class OpenInterestResponse(BaseModel):
    underlying: str
    spot_price: Decimal
    contracts: list[OpenInterestRecord]

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
            date: lambda v: v.isoformat() if v is not None else None,
        }


class HealthStatus(BaseModel):
    status: str = Field(..., description="healthy, degraded, or unhealthy")
    database_connected: bool
    last_data_update: Optional[datetime] = None
    data_age_seconds: Optional[int] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


from enum import Enum  # already imported via pydantic internals but be explicit
from typing import List  # already imported above, included here for clarity


class SignalDirection(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class SignalStrength(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TradeType(str, Enum):
    SHORT_CALL_SPREAD = "short_call_spread"
    SHORT_PUT_SPREAD = "short_put_spread"
    LONG_CALL_SPREAD = "long_call_spread"
    LONG_PUT_SPREAD = "long_put_spread"
    IRON_CONDOR = "iron_condor"
    NO_TRADE = "no_trade"


class Timeframe(str, Enum):
    INTRADAY = "intraday"
    SWING = "swing"
    MULTI_DAY = "multi_day"


class SignalComponent(BaseModel):
    name: str
    weight: int
    score: int
    description: str
    value: Optional[float] = None
    applicable: bool = True


class TradeIdea(BaseModel):
    trade_type: TradeType
    rationale: str
    target_expiry: str
    suggested_strikes: str
    estimated_win_pct: float


class TradeSignalResponse(BaseModel):
    symbol: str
    timeframe: Timeframe
    timestamp: datetime
    current_price: float
    composite_score: int
    max_possible_score: int
    normalized_score: float
    direction: SignalDirection
    strength: SignalStrength
    estimated_win_pct: float
    components: List[SignalComponent]
    trade_idea: TradeIdea
    net_gex: Optional[float] = None
    gamma_flip: Optional[float] = None
    price_vs_flip: Optional[float] = None
    vwap: Optional[float] = None
    vwap_deviation_pct: Optional[float] = None
    put_call_ratio: Optional[float] = None
    dealer_net_delta: Optional[float] = None
    smart_money_direction: Optional[SignalDirection] = None
    unusual_volume_detected: bool = False
    orb_breakout_direction: Optional[SignalDirection] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class PositionOptimizerDirection(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class PositionOptimizerCandidateComponent(BaseModel):
    name: str
    weight: int
    raw_score: int
    weighted_score: int
    description: str
    value: Optional[float] = None


class PositionOptimizerSizingProfile(BaseModel):
    profile: str
    contracts: int
    max_risk_dollars: float
    expected_value_dollars: float
    constrained_by: str


class PositionOptimizerCandidate(BaseModel):
    rank: int
    strategy_type: str
    expiry: date
    dte: int
    strikes: str
    option_type: str
    entry_debit: float
    entry_credit: float
    width: float
    max_profit: float
    max_loss: float
    risk_reward_ratio: float
    probability_of_profit: float
    expected_value: float
    sharpe_like_ratio: float
    liquidity_score: float
    net_delta: float
    net_gamma: float
    net_theta: float
    premium_efficiency: float
    market_structure_fit: float
    greek_alignment_score: float
    edge_score: float
    kelly_fraction: float
    sizing_profiles: list[PositionOptimizerSizingProfile]
    components: list[PositionOptimizerCandidateComponent]
    reasoning: list[str]


class PositionOptimizerSignalResponse(BaseModel):
    symbol: str
    timestamp: datetime
    signal_timestamp: datetime
    signal_timeframe: Timeframe
    signal_direction: PositionOptimizerDirection
    signal_strength: SignalStrength
    trade_type: str
    current_price: float
    composite_score: float
    max_possible_score: int
    normalized_score: float
    top_strategy_type: str
    top_expiry: date
    top_dte: int
    top_strikes: str
    top_probability_of_profit: float
    top_expected_value: float
    top_max_profit: float
    top_max_loss: float
    top_kelly_fraction: float
    top_sharpe_like_ratio: Optional[float] = None
    top_liquidity_score: Optional[float] = None
    top_market_structure_fit: Optional[float] = None
    top_reasoning: list[str]
    candidates: list[PositionOptimizerCandidate]

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
            date: lambda v: v.isoformat() if v is not None else None,
        }
