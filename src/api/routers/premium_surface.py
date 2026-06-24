"""
Premium Surface Router  (Beta)

GET /api/gex/premium_surface?symbol=SPY&dte_max=60&strike_count=30&option_type=C

Returns the options *premium* surface (strike × expiration) for a single
option type, where the z-value of interest is the **extrinsic** (time)
value: ``premium − intrinsic``.

  * premium    = mid quote ((bid+ask)/2), falling back to the stored mid,
                 then last trade when the quote is one-sided/stale.
  * intrinsic  = max(0, spot − strike)  for calls
                 max(0, strike − spot)  for puts
  * extrinsic  = max(0, premium − intrinsic)   (clamped: stale/crossed
                 marks below intrinsic would otherwise show negative time
                 value, which is an artefact rather than signal)

This powers the 3D premium heat-map page on the website. It is a regime
snapshot (latest stable chain), not a per-contract pricing tool.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Any, Literal
from collections import OrderedDict
import asyncio
import logging

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gex/premium_surface", tags=["GEX", "Beta"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class StrikePremium(BaseModel):
    strike: float
    # Quoted premium used (mid, or last as fallback); null when no usable
    # price exists at this (strike, expiration).
    premium: Optional[float] = None
    intrinsic: Optional[float] = None
    # premium − intrinsic, clamped at 0. This is the z-axis of the surface.
    extrinsic: Optional[float] = None


class ExpirationSlice(BaseModel):
    expiration: date
    dte: int
    strikes: List[StrikePremium]


class PremiumSurfaceResponse(BaseModel):
    symbol: str
    option_type: Literal["C", "P"]
    spot_price: float
    timestamp: datetime
    expirations: List[date]
    strikes: List[float]
    surface: List[ExpirationSlice]
    # Available bounds at the snapshot (unfiltered by the dte_max/strike_count
    # query params) so the client can size its dropdowns to the real chain.
    # max_dte is clamped to the endpoint's 365-day ceiling; strike_count to
    # its 100 cap.
    available_max_dte: int
    available_strike_count: int

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
            date: lambda v: v.isoformat() if v is not None else None,
        }


# ---------------------------------------------------------------------------
# Cache (30-second TTL, keyed by query params) — mirrors vol_surface.
# ---------------------------------------------------------------------------

_cache: "OrderedDict[tuple, Dict[str, Any]]" = OrderedDict()
_cache_lock = asyncio.Lock()
_CACHE_TTL = 30  # seconds
_CACHE_MAX_SIZE = 64


async def _get_cached(key: tuple) -> Optional[PremiumSurfaceResponse]:
    async with _cache_lock:
        entry = _cache.get(key)
        if entry and (datetime.now(timezone.utc) - entry["ts"]).total_seconds() < _CACHE_TTL:
            return entry["data"]  # type: ignore[no-any-return]
        if entry is not None:
            del _cache[key]
    return None


async def _set_cached(key: tuple, data: PremiumSurfaceResponse) -> None:
    async with _cache_lock:
        if key in _cache:
            del _cache[key]
        _cache[key] = {"data": data, "ts": datetime.now(timezone.utc)}
        while len(_cache) > _CACHE_MAX_SIZE:
            _cache.popitem(last=False)


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


def get_db() -> DatabaseManager:
    from ..main import db_manager

    assert db_manager is not None, "db_manager not initialized"
    return db_manager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_premium(row: dict) -> Optional[float]:
    """Best available premium for a contract row.

    Prefer the live mid of the two-sided quote; fall back to the stored
    ``mid`` column, then the last trade. Returns None when nothing usable
    is present (so the surface carries a gap rather than a fabricated 0).
    """
    bid = row.get("bid")
    ask = row.get("ask")
    if bid is not None and ask is not None:
        b, a = float(bid), float(ask)
        # Only trust a two-sided quote that isn't crossed/empty.
        if a > 0 and b >= 0 and a >= b:
            return (a + b) / 2.0
    mid = row.get("mid")
    if mid is not None and float(mid) > 0:
        return float(mid)
    last = row.get("last")
    if last is not None and float(last) > 0:
        return float(last)
    return None


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get("", response_model=PremiumSurfaceResponse)
async def get_premium_surface(
    symbol: str = Query(default="SPY", description="Underlying symbol (e.g. SPY)"),
    option_type: Literal["C", "P"] = Query(
        default="C", description="Option type: C (call) or P (put)"
    ),
    dte_max: int = Query(
        default=60, ge=1, le=365, description="Max days to expiration to include"
    ),
    strike_count: int = Query(
        default=30, ge=5, le=100, description="Number of strikes centered on spot"
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Return the options premium (extrinsic-value) surface for one option type.

    **Beta** — contract may change. x = strike, y = expiration, z = extrinsic
    (premium − intrinsic, clamped at 0).
    """

    sym = symbol.upper()
    cache_key = (sym, option_type, dte_max, strike_count)
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        data = await db.get_premium_surface_data(sym, dte_max, strike_count, option_type)
    except Exception as e:
        logger.error(f"Error fetching premium surface for {sym}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    if not data or not data.get("rows"):
        raise HTTPException(status_code=404, detail=f"No premium surface data for {sym}")

    spot = float(data["spot_price"])
    timestamp = data["timestamp"]
    rows = data["rows"]
    today = date.today()

    # Group rows by expiration
    by_exp: Dict[date, List[dict]] = {}
    for r in rows:
        by_exp.setdefault(r["expiration"], []).append(r)

    expirations_sorted = sorted(by_exp.keys())
    strikes_sorted = sorted({float(r["strike"]) for r in rows})

    surface: List[ExpirationSlice] = []
    for exp in expirations_sorted:
        dte = (exp - today).days
        strike_points: List[StrikePremium] = []
        for r in by_exp[exp]:
            strike = float(r["strike"])
            premium = _resolve_premium(r)
            if premium is None:
                strike_points.append(StrikePremium(strike=strike))
                continue
            if option_type == "C":
                intrinsic = max(0.0, spot - strike)
            else:
                intrinsic = max(0.0, strike - spot)
            extrinsic = max(0.0, premium - intrinsic)
            strike_points.append(
                StrikePremium(
                    strike=strike,
                    premium=round(premium, 4),
                    intrinsic=round(intrinsic, 4),
                    extrinsic=round(extrinsic, 4),
                )
            )
        strike_points.sort(key=lambda p: p.strike)
        surface.append(ExpirationSlice(expiration=exp, dte=dte, strikes=strike_points))

    # Available dropdown bounds, clamped to the endpoint's query-param ceilings
    # (dte_max <= 365, strike_count <= 100) so the client never offers a value
    # the API would reject.
    max_exp = data.get("available_max_expiration")
    available_max_dte = max(0, min(365, (max_exp - today).days)) if max_exp else 0
    available_strike_count = min(100, int(data.get("available_strike_count") or 0))

    response = PremiumSurfaceResponse(
        symbol=sym,
        option_type=option_type,
        spot_price=spot,
        timestamp=timestamp,
        expirations=expirations_sorted,
        strikes=strikes_sorted,
        surface=surface,
        available_max_dte=available_max_dte,
        available_strike_count=available_strike_count,
    )

    await _set_cached(cache_key, response)
    return response
