"""
Volatility Gauge Router

GET /api/market/volatility?ticker=VIX|VXN
    Reads the rolling window of 5-minute bars for the requested index
    ($VIX.X from `vix_bars`, $VXN.X from `vxn_bars`) and returns two
    scored dimensions:

      - level    (0–10): Current index reading expressed on a log scale
                         anchored to historical percentiles — where the
                         index sits right now.
      - momentum (0–10): Weighted rate-of-change across five time scales,
                         normalised to ±4σ — which direction and how fast
                         the index is moving relative to its own recent
                         behaviour.

Only bars within the 2 most-recent regular trading sessions are used for
scoring.  The per-ticker bars tables are populated by the streaming
ingesters in src/ingestion/{vix,vxn}_ingester.py.
"""

import math
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Literal

import pytz
from fastapi import APIRouter, Depends, HTTPException, Query

from pydantic import BaseModel, Field

from src.api.database import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/market", tags=["Market Data"])

ET = pytz.timezone("US/Eastern")


def get_db() -> DatabaseManager:
    from src.api.main import db_manager

    if db_manager is None:
        from fastapi import status

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not initialized",
        )
    return db_manager


# ============================================================================
# Session helpers
# ============================================================================


def _session_start(d: date) -> datetime:
    """ET-localized 9:30 AM open for date *d*."""
    return ET.localize(datetime(d.year, d.month, d.day, 9, 30, 0))  # type: ignore[no-any-return]


def _two_session_cutoff() -> datetime:
    """
    Return the start of the older of the 2 most-recent regular trading
    sessions (weekdays only; no holiday calendar adjustment).
    """
    now = datetime.now(ET)
    count = 0
    d = now.date()
    while True:
        if d.weekday() < 5:  # Monday–Friday
            count += 1
            if count == 2:
                break
        d -= timedelta(days=1)
    return _session_start(d)


# ============================================================================
# Scoring
# ============================================================================


def _level(index_close: float) -> float:
    """
    Map an index level → 0–10 using a log scale anchored to historical
    VIX-style percentiles (VXN trades in a similar range so the scale
    transfers cleanly).

    Approximate readings:
      10  →  0.0  (record low territory)
      15  →  2.0
      20  →  3.6  (long-run median ~17)
      25  →  5.0
      30  →  6.2  (elevated / high-fear threshold)
      40  →  8.0
      50  → 10.0  (extreme panic)
    """
    if index_close <= 0:
        return 0.0
    lo = math.log(10.0)  # floor    → score 0
    hi = math.log(50.0)  # ceiling  → score 10
    score = 10.0 * (math.log(index_close) - lo) / (hi - lo)
    return round(max(0.0, min(10.0, score)), 2)


def _momentum(bars: List[Dict[str, Any]]) -> float:
    """
    Map index momentum → 0–10.

    Steps:
    1. Compute a weighted composite rate-of-change (RoC) across five lookback
       windows.  Weights are distributed across short and medium-term horizons
       so that a single noisy bar does not dominate the reading.
    2. Normalise the composite RoC by the rolling 1-bar RoC std derived from
       the window itself (realised per-bar volatility of the index).
    3. Map the z-score using a ±4σ range so that only truly extreme moves
       reach the extremes:
         z = –4 → 0   (sharply falling)
         z =  0 → 5   (flat / stable)
         z = +4 → 10  (sharply rising)
       Clamped to [0, 10].

    Lookback windows (5-min bars) and weights:
      1 bar  ( 5 min) → 0.15
      3 bars (15 min) → 0.20
      6 bars (30 min) → 0.25
      12 bars ( 1 hr) → 0.25
      26 bars ( 2 hr) → 0.15
    """
    if len(bars) < 2:
        return 5.0  # neutral — not enough data

    closes = [b["close"] for b in bars]
    current = closes[-1]

    windows = [(1, 0.15), (3, 0.20), (6, 0.25), (12, 0.25), (26, 0.15)]
    composite_roc = 0.0
    total_weight = 0.0

    for n, w in windows:
        if len(closes) > n:
            prev = closes[-(n + 1)]
            if prev and prev != 0.0:
                composite_roc += w * (current - prev) / prev
                total_weight += w

    if total_weight == 0.0:
        return 5.0

    # Re-normalise so weights of available windows sum to 1
    composite_roc /= total_weight

    # Rolling 1-bar RoC standard deviation (from all bars in window)
    one_bar_rocs = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        if prev and prev != 0.0:
            one_bar_rocs.append((closes[i] - prev) / prev)

    if len(one_bar_rocs) < 3:
        # Fallback: VIX/VXN typically move ~0.5 % per 5-min bar in normal markets
        sigma = 0.005
    else:
        n = len(one_bar_rocs)
        mean = sum(one_bar_rocs) / n
        variance = sum((x - mean) ** 2 for x in one_bar_rocs) / (n - 1)
        sigma = max(math.sqrt(variance) if variance > 0 else 0.001, 0.001)

    # Map ±4σ → 0–10
    z_score = composite_roc / sigma
    score = 5.0 + 1.25 * z_score
    return round(max(0.0, min(10.0, score)), 2)


def _level_label(score: float) -> str:
    if score < 2.0:
        return "Subdued"
    if score < 4.0:
        return "Low"
    if score < 6.0:
        return "Moderate"
    if score < 8.0:
        return "Elevated"
    return "Extreme"


def _momentum_label(score: float) -> str:
    if score < 2.0:
        return "Collapsing"
    if score < 4.0:
        return "Easing"
    if score < 6.0:
        return "Stable"
    if score < 8.0:
        return "Rising"
    return "Surging"


# ============================================================================
# Response models
# ============================================================================


class VolatilityBar(BaseModel):
    timestamp: datetime
    close: float

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class VolatilityIndexResponse(BaseModel):
    timestamp: datetime = Field(description="Timestamp of the latest bar (ET)")
    index: float = Field(description="Current index close (e.g. $VIX.X or $VXN.X)")

    level: float = Field(
        description=(
            "Index level mapped to 0–10 (log scale). "
            "0 = ultra-calm (~10), 5 = ~25, 10 = extreme fear (~50+)."
        )
    )
    level_label: str = Field(
        description="Human-readable label: Subdued / Low / Moderate / Elevated / Extreme"
    )

    momentum: float = Field(
        description=(
            "Index rate-of-change mapped to 0–10 (±4σ range). "
            "0 = collapsing, 5 = stable, 10 = surging."
        )
    )
    momentum_label: str = Field(
        description="Human-readable label: Collapsing / Easing / Stable / Rising / Surging"
    )

    cache_bars: int = Field(
        description="5-min bars used from the per-ticker bars table for this response"
    )
    latest_bars: List[VolatilityBar] = Field(
        description="Most-recent 10 bars for debugging / charting", default_factory=list
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ============================================================================
# Endpoint
# ============================================================================


@router.get("/volatility", response_model=VolatilityIndexResponse)
async def get_volatility_index(
    ticker: Literal["VIX", "VXN"] = Query(
        default="VIX",
        description="Volatility index to score: 'VIX' ($VIX.X) or 'VXN' ($VXN.X).",
    ),
    db: DatabaseManager = Depends(get_db),
):
    """
    Returns volatility-index metrics as two scored dimensions, for either
    the CBOE S&P 500 Volatility Index (``ticker=VIX``, default) or the CBOE
    Nasdaq-100 Volatility Index (``ticker=VXN``).

    **Level** — *where is the index right now?*
    Maps the current reading to a 0–10 log scale anchored to historical
    percentiles:
    - `0–2`  → Subdued  (~10–15, historically quiet)
    - `2–4`  → Low      (~15–19, below-average vol)
    - `4–6`  → Moderate (~19–27, near long-run average)
    - `6–8`  → Elevated (~27–38, above-average fear)
    - `8–10` → Extreme  (~38+, crisis-level fear)

    **Momentum** — *which direction and how fast is the index moving?*
    Weighted composite rate-of-change across five time scales (5 min through
    2 hrs), normalised against realised per-bar volatility of the index.

    **Data source** — reads the rolling 5-min bar window maintained by the
    per-ticker streaming ingester (VIX → ``vix_bars``, VXN → ``vxn_bars``).
    """
    cutoff = _two_session_cutoff()

    try:
        bars = await db.get_volatility_index_bars(ticker, cutoff, ET)
    except Exception as exc:
        logger.error("%s DB read failed: %s", ticker, exc)
        raise HTTPException(
            status_code=503,
            detail=f"Unable to read {ticker} data from database.",
        )

    if not bars:
        table = "vix_bars" if ticker == "VIX" else "vxn_bars"
        raise HTTPException(
            status_code=503,
            detail=(
                f"{ticker} data unavailable — {table} table is empty. "
                f"Check that the ingestion engine's {ticker} poller is running."
            ),
        )

    latest = bars[-1]
    index_close = latest["close"]

    lvl = _level(index_close)
    mom = _momentum(bars)

    recent_bars = [
        VolatilityBar(timestamp=b["timestamp"], close=b["close"]) for b in reversed(bars[-10:])
    ]

    return VolatilityIndexResponse(
        timestamp=latest["timestamp"],
        index=round(index_close, 2),
        level=lvl,
        level_label=_level_label(lvl),
        momentum=mom,
        momentum_label=_momentum_label(mom),
        cache_bars=len(bars),
        latest_bars=recent_bars,
    )
