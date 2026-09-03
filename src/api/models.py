"""
Pydantic models for API request/response validation
"""

from pydantic import BaseModel, Field
from datetime import datetime, date
from enum import Enum
from typing import Dict, List, Optional
from decimal import Decimal


class WallLevel(BaseModel):
    """One rung of the Call/Put Wall ladder (``C1``/``C2``/``P1``/``P2``…).

    Produced by :func:`src.analytics.walls.compute_wall_ladder`, which ranks
    the eligible strikes on each side of spot by aggregated dollar gamma.
    Rank 1 is by construction the canonical Call/Put Wall — the same value
    the sibling ``call_wall`` / ``put_wall`` scalar carries — so a client can
    draw the ladder without the primary wall ever disagreeing with it.

    ``label`` ships from the server (``C1``, ``P2``, …) so every surface —
    chart page, dashboard widget, any future export — spells a wall the same
    way instead of each re-deriving the name.  ``strength`` is the dollar
    gamma at the strike on the canonical ``γ × 100 × S² × 0.01`` scale,
    letting a client dim or size a marker by how much book is actually there.
    """

    rank: int
    label: str
    strike: Decimal
    strength: Optional[Decimal] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
        }


class GEXSummary(BaseModel):
    timestamp: datetime
    symbol: str
    spot_price: Decimal
    total_call_gex: Decimal
    total_put_gex: Decimal
    net_gex: Decimal
    net_gex_at_spot: Optional[Decimal] = None
    gamma_flip: Optional[Decimal] = None
    # Raw nearest zero-crossing on the UN-DTE-weighted gamma profile —
    # the "nearest crossing to spot" convention competitor dashboards
    # publish.  ``gamma_flip`` applies a horizon-occupancy ramp (down-
    # weighting near-dated 0DTE walls); this raw value drops it, so near-
    # dated walls can pull the crossing toward spot and it can sit much
    # closer to spot than the structural flip.  Secondary reference only —
    # no structural-significance gate.  NULL when the profile is one-signed
    # / degraded.
    gamma_flip_raw: Optional[Decimal] = None
    # Fraction of spot the resolver's grid was widened to in order to
    # land ``gamma_flip``.  ``GAMMA_PROFILE_SPAN_LADDER[0]`` (default
    # 0.20) means the default rung qualified — a stable regime level.
    # Larger means the default rung had no qualifying interior crossing
    # and the ladder fell through to an expansion rung; treat such
    # flips as marginal (passed a wider geometric search; the
    # structural floor is held constant across rungs as of the
    # canonical-reference refactor, so this is purely a geometry
    # signal — but a value that only resolves at ±35% / ±50% still
    # means the chain has no near-spot regime boundary).
    gamma_flip_span_used: Optional[Decimal] = None
    flip_distance: Optional[Decimal] = None
    local_gex: Optional[Decimal] = None
    convexity_risk: Optional[Decimal] = None
    max_pain: Optional[Decimal] = None
    call_wall: Optional[Decimal] = None
    put_wall: Optional[Decimal] = None
    # Ranked wall ladders — the OPTIONAL secondary/tertiary walls (C2/C3,
    # P2/P3) charts can draw beside the primary.  ``call_walls[0]`` is the
    # same strike as ``call_wall`` above (both come from the one ranking in
    # :mod:`src.analytics.walls`), so the two can never disagree; a client
    # that only wants the headline wall keeps reading the scalar and ignores
    # these.  Shorter than the requested depth when the chain has fewer
    # eligible strikes on that side, and empty when the side has no wall at
    # all — render what arrives, never pad.
    call_walls: List[WallLevel] = Field(default_factory=list)
    put_walls: List[WallLevel] = Field(default_factory=list)
    # GEX King — the strike carrying the largest |net dealer gamma| with the
    # per-strike totals aggregated across ALL expirations (the SpotGamma /
    # SqueezeMetrics convention; see ``_calculate_gex_summary``).  Whole-chain
    # by construction, so it is the heavy, slow structural node — deliberately
    # NOT the 0DTE Pin Strike below, which is reachability-weighted and
    # same-day.  Already stored on ``gex_summary``; surfaced here so the chart
    # can draw it beside the walls/flip/pin.  Nullable — hide, don't zero.
    max_gamma_strike: Optional[Decimal] = None
    total_call_oi: Optional[int] = None
    total_put_oi: Optional[int] = None
    put_call_ratio: Optional[Decimal] = None
    # Pin Strike — the reachable 0DTE strike with the strongest modeled
    # POSITIVE (restoring) dealer gamma into expiration.  Distinct from the
    # walls / flip / max-pain / king-node: it simulates spot AT each candidate
    # strike, keeps only locally-concentrated positive gamma, and weights by
    # the probability price reaches that strike before the 0DTE close (see
    # src/analytics/pin_strike.py).  Nullable — hide, don't zero.
    # ``pin_score`` is the raw maximum pin score (restoring gamma × reachability);
    # ``pin_confidence`` its dominance over all viable pins (0..1); together they
    # let a client classify pin strength.  ``pin_strike_reason`` carries a
    # REASON_* code when there is no active pin (e.g. NO_0DTE_EXPIRATION,
    # NO_POSITIVE_RESTORING_GAMMA) and is null when a pin is present.
    pin_strike: Optional[Decimal] = None
    pin_score: Optional[float] = None
    pin_confidence: Optional[float] = None
    pin_strike_reason: Optional[str] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class PinStabilityResponse(BaseModel):
    """What the Pin Strike has *done* during the session.

    The third question in the reading order — regime, then confidence, then
    stability — and the only one the API could not previously answer.  A pin
    that has held one strike since the open and a pin that has migrated thirty
    points are different signals wearing the same label; without this a drifting
    pin reads as a level that failed rather than one tracking a repricing book.

    ``current_*`` is the pin standing right now, ``held_*`` the most recent
    value that has SETTLED, and ``current_established`` says whether they are
    the same. Migration is measured between settled levels only, so a
    one-sample tick at the bell is not reported as a move; ``net_migration`` is
    signed (negative = the pin has walked down).  Sample counts, not minutes:
    the analytics cycle is ~60s but is not guaranteed to be, so the honest unit
    is "stored frames".  Null-bodied (``stability`` omitted) when the session
    carried no active pin at all — hide, don't zero.
    """

    symbol: str
    current_pin: float
    current_since: datetime
    current_samples: int
    current_established: bool
    held_pin: float
    held_since: datetime
    held_samples: int
    session_open_pin: float
    net_migration: float
    distinct_values: int
    quiet_samples: int
    total_samples: int


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


class GEXProfilePoint(BaseModel):
    # One point on the spot-shift dealer dollar-gamma curve. ``price`` is
    # a hypothetical underlying price (grid x-axis); ``gex`` is the dealer
    # dollar GEX evaluated at that price ($ per 1% spot move).
    price: float
    gex: float


class GEXProfile(BaseModel):
    """Spot-shift dealer dollar-gamma curve.

    The shared primitive whose zero crossing is ``gamma_flip`` and whose
    value at ``spot_price`` is ``net_gex_at_spot`` — the curve consumed
    by the GEX-Profile overlay on the per-strike chart.
    """

    timestamp: datetime
    symbol: str
    spot_price: Decimal
    span_pct: Optional[float] = None
    profile: List[GEXProfilePoint]
    # Convenience: the headline reference levels associated with this
    # snapshot, so the frontend can render the flip line / walls without
    # a second round-trip to /api/gex/summary.
    gamma_flip: Optional[Decimal] = None
    net_gex_at_spot: Optional[Decimal] = None
    call_wall: Optional[Decimal] = None
    put_wall: Optional[Decimal] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class GEXHistoricalWindow(BaseModel):
    """One historical window (e.g. 30d, all_time) for a single metric.

    ``percentile`` is the interpolated rank of the LIVE value across the
    stored p05/p25/p50/p75/p95 anchors.  ``z_score`` is the standard
    z-score against the same window's mean/std.  ``regime`` is the
    user-facing bucket label — extreme_high / elevated / normal / low /
    extreme_low / unknown — derived purely from z-score so the band
    bucketing stays explainable.

    ``is_record_high`` / ``is_record_low`` are independent boolean flags
    fired when the live value exceeds the stored min/max for this window.
    They're carried alongside the regime so the frontend can stamp a
    trophy icon next to the badge label without losing the regime word.
    When a record fires we ALSO promote the regime to the matching
    extreme label, so the badge color stays consistent with the trophy
    even if z-score alone would have left the regime at "elevated"
    (happens when the historical distribution is very tight).

    ``tod_bucket_used``:
        * 0..77 — a time-of-day-aware bucket (5-min RTH index, 0=09:30 ET)
        * -1    — the flat (non-TOD) fallback for the same window
        * None  — neither bucket was available (no stats row yet)
    """

    p05: Optional[float] = None
    p25: Optional[float] = None
    p50: Optional[float] = None
    p75: Optional[float] = None
    p95: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None
    sample_size: int
    percentile: Optional[float] = None
    z_score: Optional[float] = None
    regime: str
    is_record_high: bool = False
    is_record_low: bool = False
    tod_bucket_used: Optional[int] = None


class GEXHistoricalMetric(BaseModel):
    """Live value plus one entry per known window."""

    current: Optional[float] = None
    windows: Dict[str, Optional[GEXHistoricalWindow]]


class GEXHistoricalContext(BaseModel):
    """Response shape for ``/api/gex/historical-context``.

    Headline GEX figures from the latest ``gex_summary`` row alongside the
    historical-distribution context (30d + all_time, TOD-aware) for each.
    The frontend uses this to render "P82 vs 30d", "EXTREME HIGH",
    "elevated" style badges on the live MetricCards (with a trophy icon
    overlay when the value is a record for that window) and a dedicated
    /gamma-pulse page.

    ``tracking_started_at`` is the earliest ``gex_summary.timestamp`` for
    the symbol — the "since YYYY-MM-DD" date the all-time-record trophy
    tooltip cites.
    """

    symbol: str
    timestamp: datetime
    tod_bucket: Optional[int] = None
    in_rth: bool
    tracking_started_at: Optional[datetime] = None
    metrics: Dict[str, GEXHistoricalMetric]

    class Config:
        from_attributes = True
        json_encoders = {
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
    # DISPLAY-only index→future swap (see market_calendar.should_display_future).
    # These are ADDITIVE: the base OHLC/close/session stay the cash index so
    # every downstream consumer of the quote (GEX spot, greeks, options
    # calculator, heatmap) is unaffected. Only the header quote, the quote
    # card, and the candlestick chart read the futures fields.
    #   display_source: 'futures' when the future should be shown, else None.
    #   data_symbol:    the future's UI ticker (e.g. 'ES'), else None.
    #   futures_close:  the future's last price (the number those surfaces show).
    #   futures_reference_close: the future's price at the 16:00 ET cash close
    #       — the baseline for the overnight change, measured futures-vs-futures
    #       (the future's own 16:00 print) so it never mixes in the index↔future
    #       basis.
    display_source: Optional[str] = None
    data_symbol: Optional[str] = None
    futures_close: Optional[Decimal] = None
    futures_reference_close: Optional[Decimal] = None
    # FEED freshness, for the natively-served futures quote (ES / NQ).
    # Deliberately separate from `session`, which describes the CME calendar:
    # folding the two together made a stale feed read as a closed market, and
    # the frontend answers "closed" by swapping the headline price for the
    # last cash close — so a late ES print was published as Friday's close
    # with Friday's day change. See _native_futures_quote.
    #   stale:            newest bar older than FUTURES_QUOTE_STALE_MINUTES.
    #                     Chart surfaces gate tip-candle merging on this so a
    #                     dead feed still cannot paint ghost data on the tip.
    #   data_age_seconds: age of that bar, for a "delayed" marker.
    # Both absent on the cash index / ETF path.
    stale: Optional[bool] = None
    data_age_seconds: Optional[int] = None

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


class MarketTideComponent(BaseModel):
    symbol: str
    flow_score: float
    gamma_score: float
    amplifier: float
    weight: float
    contribution: float


class MarketTideResponse(BaseModel):
    """Cross-symbol directional options pressure adjusted by dealer gamma."""

    timestamp: datetime
    score: Optional[float] = None
    label: str
    flow_direction: float
    gamma_regime: float
    gamma_label: str
    bullish_breadth_pct: float
    bearish_breadth_pct: float
    neutral_breadth_pct: float
    participation_pct: float
    eligible_symbols: int
    configured_symbols: int
    stale_symbols: List[str]
    # Full per-symbol breakdown (all eligible names, ranked by contribution) for
    # the flow-vs-gamma map. Defaults empty so snapshots persisted before this
    # field existed still validate on read-back.
    components: List[MarketTideComponent] = []
    leaders: List[MarketTideComponent]
    laggards: List[MarketTideComponent]


class MarketTideHistoryPoint(BaseModel):
    """One point on the persisted Market Tide series (a 5-min bucket, or a
    session close in daily mode)."""

    timestamp: datetime
    session_date: date
    score: Optional[float] = None
    label: str
    participation_pct: float
    flow_direction: Optional[float] = None
    gamma_regime: Optional[float] = None


class MarketTideHistoryResponse(BaseModel):
    """Market Tide over time for a window — intraday (today's 5-min series) or
    daily (one close per session)."""

    window_minutes: int
    mode: str
    points: List[MarketTideHistoryPoint]


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
    # Newest flow event in the whole session, not just among the rows returned.
    # These rows are ranked by notional, so their own timestamps say when the
    # BIGGEST prints landed, not how current the feed is; this is the recency
    # signal. Additive, so existing v1 consumers are unaffected.
    session_latest_at: Optional[datetime] = None


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


class SessionLevels(BaseModel):
    """Pre-market + previous-session high/low for the chart level overlays.

    ``is_index`` is True for cash indexes (SPX, NDX, …) — they have no
    pre-market print, so all level fields are null and the frontend skips
    drawing.  ``trading_date`` is the ET date whose pre-market the
    ``premarket_*`` fields describe; ``prev_session_*`` describe the
    regular session of ``prev_session_date``.  ``source`` records
    provenance (``captured`` / ``live`` / ``captured+live``).
    """

    symbol: str
    is_index: bool = False
    trading_date: Optional[date] = None
    premarket_high: Optional[Decimal] = None
    premarket_low: Optional[Decimal] = None
    prev_session_date: Optional[date] = None
    prev_session_high: Optional[Decimal] = None
    prev_session_low: Optional[Decimal] = None
    source: Optional[str] = None
    updated_at: Optional[datetime] = None

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


class StrikeProfileStrike(BaseModel):
    """Per-strike row inside a Strike-Profile-Timeseries bucket.

    ``call_gamma`` / ``put_gamma`` / ``net_gamma`` carry the same dollar
    gamma exposure quantities that ``/api/gex/by-strike`` returns under
    ``call_gex`` / ``put_gex`` / ``net_gex`` (``γ × OI × 100 × S² × 0.01``,
    "$ per 1% spot move"), evaluated against this bucket's close price.
    Names follow the request shape — readers that already speak the
    by-strike units can map them straight through.
    """

    strike: Decimal
    call_gamma: Decimal
    put_gamma: Decimal
    net_gamma: Decimal
    call_oi: int
    put_oi: int

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
        }


class StrikeProfileBucket(BaseModel):
    """One time bucket of the Strike-Profile-Timeseries.

    ``timestamp`` is the bucket start (ET-session aligned via the same
    bucket expression every historical endpoint uses).  ``open`` /
    ``high`` / ``low`` / ``close`` are the underlying OHLC for the
    bucket; ``close`` is the canonical "spot" used to compute the
    per-strike dollar-gamma values below.  ``gamma_flip`` is the
    analytics-engine value from the bucket's representative
    ``gex_summary`` row.  ``call_wall`` / ``put_wall`` are computed
    live for the bucket from the same (expiration-filtered,
    summed-by-strike) gamma rows the ``strikes`` payload renders, via
    the canonical :func:`src.analytics.walls.compute_call_put_walls`
    helper, evaluated against this bucket's ``close``.  Wall scope
    therefore follows the request's ``expirations`` filter:
    ``expirations=all`` yields the cross-expiration aggregate walls
    (same basis as the live ``/api/gex/summary``);
    ``expirations=<YYYY-MM-DD>`` yields walls scoped to that
    expiration's gamma alone.  ``call_wall`` / ``put_wall`` are
    ``None`` when the bucket has no strikes or no underlying close.
    ``pin_strike`` / ``pin_confidence`` are the Pin Strike (the reachable
    0DTE strike with the strongest modeled positive/restoring dealer gamma
    into expiration — see :mod:`src.analytics.pin_strike`) and its 0..1
    dominance over the other viable pins, as of this bucket's close.  They
    are read verbatim from the bucket's representative ``gex_summary`` row
    and, unlike the walls and the flip, are NOT scoped by ``expirations``:
    the pin is 0DTE-by-construction and whole-chain by definition, so it
    reads the same in every expiration scope — matching the live surfaces,
    where the pin does not move when the Expiry selector changes.  Both are
    ``None`` when the bucket has no active pin and on rows written before
    the pin columns shipped; a ``None`` pin is drawn as no line, never as
    ``0``.

    ``call_walls`` / ``put_walls`` are the ranked ladders (``C1``/``C2``/…,
    ``P1``/``P2``/…) behind the optional secondary-wall levels, computed from
    the same rows and the same ``close`` as the scalars, so they follow the
    ``expirations`` filter identically and rank 1 equals the scalar.

    ``strikes`` is the per-strike payload; one row per strike
    available in this bucket's snapshot universe (after the optional
    expiration filter).
    """

    timestamp: datetime
    symbol: str
    open: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    close: Optional[Decimal] = None
    gamma_flip: Optional[Decimal] = None
    call_wall: Optional[Decimal] = None
    put_wall: Optional[Decimal] = None
    # Ranked ladders for this bucket, computed from the same rows and the
    # same bucket ``close`` as the scalars above, so they follow the
    # ``expirations`` filter identically and rank 1 equals the scalar.
    call_walls: List[WallLevel] = Field(default_factory=list)
    put_walls: List[WallLevel] = Field(default_factory=list)
    pin_strike: Optional[Decimal] = None
    # DOUBLE PRECISION in gex_summary (a 0..1 ratio, not a price), so it
    # stays a float rather than joining the NUMERIC price fields above —
    # no Decimal round-trip for a value that is never money.
    pin_confidence: Optional[float] = None
    strikes: list[StrikeProfileStrike]

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
