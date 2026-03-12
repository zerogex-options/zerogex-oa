"""
Volatility Gauge Router

GET /api/volatility/gauge

Returns VIX.X metrics as car dashboard gauges:
  - speedometer (0–10): Current VIX level — how fast the market is moving
  - tachometer  (0–10): VIX rate of change across time scales — how fast
                         volatility itself is accelerating or decelerating

Cache behaviour
---------------
* First call after startup: fetches ~2 trading sessions (≈156 bars) of 5-min
  VIX.X bars from TradeStation and stores them in memory.
* Subsequent calls: fetches only the latest 5-min bar and appends it.
* Cache is trimmed after every update so it never reaches back further than
  2 regular trading sessions (Mon–Fri, 9:30–16:00 ET).
"""

import os
import asyncio
import threading
import math
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.ingestion.tradestation_client import TradeStationClient
from src.validation import safe_float

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/volatility", tags=["Volatility"])

ET = pytz.timezone("US/Eastern")

# ============================================================================
# In-memory cache
# ============================================================================

_vix_bars: List[Dict[str, Any]] = []   # sorted ascending by "timestamp"
_cache_initialized: bool = False
_cache_lock = threading.Lock()          # guards _vix_bars and _cache_initialized

# asyncio-level lock prevents concurrent initial fetches when multiple requests
# arrive before the cache is warm.  Created lazily on first request so we stay
# inside the running event loop.
_async_init_lock: Optional[asyncio.Lock] = None


def _get_async_init_lock() -> asyncio.Lock:
    global _async_init_lock
    if _async_init_lock is None:
        _async_init_lock = asyncio.Lock()
    return _async_init_lock


# ============================================================================
# TradeStation client factory
# ============================================================================

def _make_ts_client() -> TradeStationClient:
    return TradeStationClient(
        client_id=os.getenv("TRADESTATION_CLIENT_ID", ""),
        client_secret=os.getenv("TRADESTATION_CLIENT_SECRET", ""),
        refresh_token=os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true",
    )


# ============================================================================
# Cache helpers
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


def _parse_bar(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a raw TradeStation bar dict into our internal format."""
    ts_str = raw.get("TimeStamp") or raw.get("timestamp")
    if not ts_str:
        return None
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        ts = ts.astimezone(ET)
    except (ValueError, TypeError):
        return None

    close = safe_float(raw.get("Close"), field_name="Close")
    if close is None:
        return None

    return {
        "timestamp": ts,
        "open":  safe_float(raw.get("Open"),  field_name="Open"),
        "high":  safe_float(raw.get("High"),  field_name="High"),
        "low":   safe_float(raw.get("Low"),   field_name="Low"),
        "close": close,
    }


def _dedup_and_sort() -> None:
    """Deduplicate by timestamp and sort ascending. Caller must hold _cache_lock."""
    global _vix_bars
    by_ts: Dict[datetime, Dict[str, Any]] = {}
    for b in _vix_bars:
        by_ts[b["timestamp"]] = b
    _vix_bars = sorted(by_ts.values(), key=lambda x: x["timestamp"])


def _trim_cache() -> None:
    """Drop bars older than 2 trading sessions. Caller must hold _cache_lock."""
    global _vix_bars
    cutoff = _two_session_cutoff()
    _vix_bars = [b for b in _vix_bars if b["timestamp"] >= cutoff]


# ============================================================================
# Blocking fetch functions (run in executor so they don't stall the loop)
# ============================================================================

def _do_initial_fetch() -> None:
    """Fetch ~2 sessions (156 bars) of 5-min VIX.X bars and populate cache."""
    global _vix_bars, _cache_initialized
    client = _make_ts_client()
    result = client.get_bars(
        symbol="VIX.X",
        interval=5,
        unit="Minute",
        barsback=156,
        sessiontemplate="Default",
        warn_if_closed=False,
    )
    raw_bars = result.get("Bars", [])
    parsed = [b for b in (_parse_bar(r) for r in raw_bars) if b is not None]
    with _cache_lock:
        _vix_bars = parsed
        _dedup_and_sort()
        _trim_cache()
        _cache_initialized = True
    logger.info("VIX cache initialised with %d bars", len(_vix_bars))


def _do_incremental_fetch() -> None:
    """Fetch the latest 5-min bar and append it to the cache if it is new."""
    client = _make_ts_client()
    result = client.get_bars(
        symbol="VIX.X",
        interval=5,
        unit="Minute",
        barsback=2,          # grab 2 to guard against partial-bar edge cases
        sessiontemplate="Default",
        warn_if_closed=False,
    )
    raw_bars = result.get("Bars", [])
    parsed = [b for b in (_parse_bar(r) for r in raw_bars) if b is not None]
    if not parsed:
        return
    with _cache_lock:
        last_ts = _vix_bars[-1]["timestamp"] if _vix_bars else None
        for bar in parsed:
            if last_ts is None or bar["timestamp"] > last_ts:
                _vix_bars.append(bar)
                last_ts = bar["timestamp"]
        _trim_cache()
    logger.debug("VIX cache updated, now %d bars", len(_vix_bars))


# ============================================================================
# Rating system
# ============================================================================

def _speedometer(vix_close: float) -> float:
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
    lo = math.log(10.0)   # VIX floor  → score 0
    hi = math.log(50.0)   # VIX ceiling → score 10
    score = 10.0 * (math.log(vix_close) - lo) / (hi - lo)
    return round(max(0.0, min(10.0, score)), 2)


def _tachometer(bars: List[Dict[str, Any]]) -> float:
    """
    Map VIX acceleration → 0–10.

    Steps:
    1. Compute a weighted composite rate-of-change (RoC) across five lookback
       windows.  Shorter windows carry more weight so the reading responds
       quickly to recent moves.
    2. Normalise the composite RoC by the rolling 1-bar RoC std derived from
       the cache itself (i.e. the realised per-bar volatility of VIX).
    3. Map the z-score linearly:  z = -1 → 0, z = 0 → 5, z = +1 → 10.
       Clamped to [0, 10].

    Lookback windows (5-min bars) and weights:
      1 bar  (5 min)  → 0.35
      3 bars (15 min) → 0.25
      6 bars (30 min) → 0.20
      12 bars (1 hr)  → 0.12
      26 bars (2 hr)  → 0.08
    """
    if len(bars) < 2:
        return 5.0   # neutral — not enough data

    closes = [b["close"] for b in bars]
    current = closes[-1]

    windows = [(1, 0.35), (3, 0.25), (6, 0.20), (12, 0.12), (26, 0.08)]
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

    # Rolling 1-bar RoC standard deviation (from all bars in cache)
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

    z_score = composite_roc / sigma
    tach = 5.0 + 5.0 * z_score
    return round(max(0.0, min(10.0, tach)), 2)


def _speed_label(score: float) -> str:
    if score < 2.0:
        return "Idle"
    if score < 4.0:
        return "Cruising"
    if score < 6.0:
        return "Elevated"
    if score < 8.0:
        return "High Speed"
    return "Redline"


def _tach_label(score: float) -> str:
    if score < 2.0:
        return "Hard Braking"
    if score < 4.0:
        return "Decelerating"
    if score < 6.0:
        return "Steady"
    if score < 8.0:
        return "Accelerating"
    return "Full Throttle"


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
    vix: float = Field(description="Current VIX.X close")

    speedometer: float = Field(
        description=(
            "VIX level mapped to 0–10 (log scale). "
            "0 = ultra-calm (VIX ~10), 5 = VIX ~25, 10 = extreme fear (VIX ~50+)."
        )
    )
    speedometer_label: str = Field(
        description="Human-readable label: Idle / Cruising / Elevated / High Speed / Redline"
    )

    tachometer: float = Field(
        description=(
            "VIX acceleration mapped to 0–10. "
            "0 = sharply decelerating (–1σ), 5 = steady, 10 = sharply accelerating (+1σ)."
        )
    )
    tachometer_label: str = Field(
        description="Human-readable label: Hard Braking / Decelerating / Steady / Accelerating / Full Throttle"
    )

    cache_bars: int = Field(description="5-min bars currently held in the in-memory cache")
    latest_bars: List[VIXBar] = Field(
        description="Most-recent 10 bars for debugging / charting", default_factory=list
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ============================================================================
# Endpoint
# ============================================================================

@router.get("/gauge", response_model=VolatilityGaugeResponse)
async def get_volatility_gauge():
    """
    Returns VIX.X volatility metrics as a two-dial car dashboard.

    **Speedometer** — *how fast are we going?*
    Maps the current VIX.X level to a 0–10 scale using a log curve that
    matches the historical distribution of VIX readings:
    - `0–2`  → Idle (VIX ~10–14, ultra-low volatility)
    - `2–4`  → Cruising (VIX ~14–19, below-average vol)
    - `4–6`  → Elevated (VIX ~19–27, above-average vol)
    - `6–8`  → High Speed (VIX ~27–38, high fear)
    - `8–10` → Redline (VIX ~38+, extreme panic)

    **Tachometer** — *how fast is the speed changing?*
    Measures the rate of change of VIX across five time scales (5 min through
    2 hrs), weighted toward recent moves, then normalises by the realised
    per-bar volatility of VIX itself so that +1σ maps to 10 and –1σ maps to 0:
    - `0–2`  → Hard Braking (fear collapsing fast)
    - `2–4`  → Decelerating (vol trending lower)
    - `4–6`  → Steady (no meaningful acceleration)
    - `6–8`  → Accelerating (vol climbing)
    - `8–10` → Full Throttle (fear spiking hard)

    **Cache behaviour** — on the first call after startup the endpoint fetches
    ≈2 full trading sessions of 5-min VIX bars and stores them in memory.
    Every subsequent call fetches only the latest bar and appends it; bars
    older than 2 trading sessions are automatically pruned.
    """
    global _cache_initialized

    loop = asyncio.get_event_loop()
    init_lock = _get_async_init_lock()

    if not _cache_initialized:
        async with init_lock:
            # Re-check inside the lock — another coroutine may have finished
            # the initial fetch while we were waiting.
            if not _cache_initialized:
                try:
                    await loop.run_in_executor(None, _do_initial_fetch)
                except Exception as exc:
                    logger.error("VIX initial fetch failed: %s", exc)
                    raise HTTPException(
                        status_code=503,
                        detail="Unable to initialise VIX data cache. Check TradeStation credentials."
                    )
    else:
        try:
            await loop.run_in_executor(None, _do_incremental_fetch)
        except Exception as exc:
            # Log but don't fail — serve stale data rather than 503
            logger.warning("VIX incremental fetch failed (serving cached data): %s", exc)

    with _cache_lock:
        bars_snapshot = list(_vix_bars)

    if not bars_snapshot:
        raise HTTPException(
            status_code=503,
            detail="VIX data unavailable — cache is empty"
        )

    latest = bars_snapshot[-1]
    vix_close = latest["close"]

    speed = _speedometer(vix_close)
    tach  = _tachometer(bars_snapshot)

    recent_bars = [
        VIXBar(timestamp=b["timestamp"], close=b["close"])
        for b in bars_snapshot[-10:]
    ]

    return VolatilityGaugeResponse(
        timestamp=latest["timestamp"],
        vix=round(vix_close, 2),
        speedometer=speed,
        speedometer_label=_speed_label(speed),
        tachometer=tach,
        tachometer_label=_tach_label(tach),
        cache_bars=len(bars_snapshot),
        latest_bars=recent_bars,
    )
