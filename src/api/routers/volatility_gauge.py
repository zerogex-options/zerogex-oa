"""
Volatility Gauge Router

GET /api/market/vix

Returns $VIX.X metrics as two scored dimensions:
  - level    (0–10): Current VIX reading expressed on a log scale anchored to
                     historical percentiles — where VIX sits right now.
  - momentum (0–10): Weighted rate-of-change of VIX across five time scales,
                     normalised to ±4σ — which direction and how fast VIX is
                     moving relative to its own recent behaviour.

Data source
-----------
* Reads the rolling window of 5-minute VIX bars from the `vix_bars` table.
* The table is populated by the ingestion engine (see
  src/ingestion/vix_ingester.py), so this endpoint never calls TradeStation
  directly.
* Only bars within the 2 most-recent regular trading sessions are used for
  scoring.
"""

import asyncio
import math
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.database import db_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/market", tags=["Market Data"])

ET = pytz.timezone("US/Eastern")


# ============================================================================
# Session helpers
# ============================================================================


def _session_start(d: date) -> datetime:
    """ET-localized 9:30 AM open for date *d*."""
    return ET.localize(datetime(d.year, d.month, d.day, 9, 30, 0))


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
# DB read
# ============================================================================


def _load_bars_from_db(cutoff: datetime) -> List[Dict[str, Any]]:
    """
    Load VIX 5-min bars >= *cutoff* from the database, sorted ascending.

    Returned dicts use the same shape as the previous in-memory cache so the
    scoring functions can stay untouched.
    """
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT timestamp, open, high, low, close
            FROM vix_bars
            WHERE timestamp >= %s
            ORDER BY timestamp ASC
            """,
            (cutoff,),
        )
        rows = cursor.fetchall()

    bars: List[Dict[str, Any]] = []
    for ts, op, hi, lo, cl in rows:
        # Postgres TIMESTAMPTZ already carries tz info; normalise to ET.
        ts_et = ts.astimezone(ET) if ts.tzinfo else ET.localize(ts)
        bars.append(
            {
                "timestamp": ts_et,
                "open": float(op) if op is not None else None,
                "high": float(hi) if hi is not None else None,
                "low": float(lo) if lo is not None else None,
                "close": float(cl),
            }
        )
    return bars


# ============================================================================
# Scoring
# ============================================================================


def _level(vix_close: float) -> float:
    """
    Map VIX level → 0–10 using a log scale anchored to historical percentiles.

    Approximate readings:
      VIX 10  →  0.0  (record low territory)
      VIX 15  →  2.0
      VIX 20  →  3.6  (long-run median ~17)
      VIX 25  →  5.0
      VIX 30  →  6.2  (elevated / high-fear threshold)
      VIX 40  →  8.0
      VIX 50  → 10.0  (extreme panic)
    """
    if vix_close <= 0:
        return 0.0
    lo = math.log(10.0)  # VIX floor  → score 0
    hi = math.log(50.0)  # VIX ceiling → score 10
    score = 10.0 * (math.log(vix_close) - lo) / (hi - lo)
    return round(max(0.0, min(10.0, score)), 2)


def _momentum(bars: List[Dict[str, Any]]) -> float:
    """
    Map VIX momentum → 0–10.

    Steps:
    1. Compute a weighted composite rate-of-change (RoC) across five lookback
       windows.  Weights are distributed across short and medium-term horizons
       so that a single noisy bar does not dominate the reading.
    2. Normalise the composite RoC by the rolling 1-bar RoC std derived from
       the window itself (realised per-bar volatility of VIX).
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
        # Fallback: VIX typically moves ~0.5 % per 5-min bar in normal markets
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


class VIXBar(BaseModel):
    timestamp: datetime
    close: float

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class VolatilityGaugeResponse(BaseModel):
    timestamp: datetime = Field(description="Timestamp of the latest VIX bar (ET)")
    vix: float = Field(description="Current $VIX.X close")

    level: float = Field(
        description=(
            "VIX level mapped to 0–10 (log scale). "
            "0 = ultra-calm (VIX ~10), 5 = VIX ~25, 10 = extreme fear (VIX ~50+)."
        )
    )
    level_label: str = Field(
        description="Human-readable label: Subdued / Low / Moderate / Elevated / Extreme"
    )

    momentum: float = Field(
        description=(
            "VIX rate-of-change mapped to 0–10 (±4σ range). "
            "0 = collapsing, 5 = stable, 10 = surging."
        )
    )
    momentum_label: str = Field(
        description="Human-readable label: Collapsing / Easing / Stable / Rising / Surging"
    )

    cache_bars: int = Field(description="5-min bars used from vix_bars for this response")
    latest_bars: List[VIXBar] = Field(
        description="Most-recent 10 bars for debugging / charting", default_factory=list
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ============================================================================
# Endpoint
# ============================================================================


@router.get("/vix", response_model=VolatilityGaugeResponse)
async def get_volatility_gauge():
    """
    Returns $VIX.X volatility metrics as two scored dimensions.

    **Level** — *where is VIX right now?*
    Maps the current $VIX.X reading to a 0–10 log scale anchored to
    historical percentiles:
    - `0–2`  → Subdued  (VIX ~10–15, historically quiet)
    - `2–4`  → Low      (VIX ~15–19, below-average vol)
    - `4–6`  → Moderate (VIX ~19–27, near long-run average)
    - `6–8`  → Elevated (VIX ~27–38, above-average fear)
    - `8–10` → Extreme  (VIX ~38+, crisis-level fear)

    **Momentum** — *which direction and how fast is VIX moving?*
    Weighted composite rate-of-change across five time scales (5 min through
    2 hrs), normalised against realised per-bar volatility of VIX.

    **Data source** — reads the rolling 5-min VIX bar window maintained by
    the ingestion engine's VIX poller from the `vix_bars` table.
    """
    cutoff = _two_session_cutoff()

    loop = asyncio.get_event_loop()
    try:
        bars = await loop.run_in_executor(None, _load_bars_from_db, cutoff)
    except Exception as exc:
        logger.error("VIX DB read failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Unable to read VIX data from database.",
        )

    if not bars:
        raise HTTPException(
            status_code=503,
            detail=(
                "VIX data unavailable — vix_bars table is empty. "
                "Check that the ingestion engine's VIX poller is running."
            ),
        )

    latest = bars[-1]
    vix_close = latest["close"]

    lvl = _level(vix_close)
    mom = _momentum(bars)

    recent_bars = [VIXBar(timestamp=b["timestamp"], close=b["close"]) for b in bars[-10:]]

    return VolatilityGaugeResponse(
        timestamp=latest["timestamp"],
        vix=round(vix_close, 2),
        level=lvl,
        level_label=_level_label(lvl),
        momentum=mom,
        momentum_label=_momentum_label(mom),
        cache_bars=len(bars),
        latest_bars=recent_bars,
    )
