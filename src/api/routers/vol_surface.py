"""
Vol Surface Router

GET /api/gex/vol_surface?symbol=SPY&dte_max=60&strike_count=30

Returns the implied-volatility surface (strike × expiration), ATM term
structure, and 25-delta skew for the requested underlying.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Any
import logging
import threading

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gex/vol_surface", tags=["GEX"])

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class StrikeIV(BaseModel):
    strike: float
    call_iv: Optional[float] = None
    put_iv: Optional[float] = None


class ExpirationSlice(BaseModel):
    expiration: date
    dte: int
    ivs: List[StrikeIV]


class ATMTermPoint(BaseModel):
    dte: int
    atm_iv: Optional[float] = None


class Skew25dPoint(BaseModel):
    dte: int
    skew: Optional[float] = None


class VolSurfaceResponse(BaseModel):
    symbol: str
    spot_price: float
    timestamp: datetime
    expirations: List[date]
    strikes: List[float]
    surface: List[ExpirationSlice]
    atm_term_structure: List[ATMTermPoint]
    skew_25d: List[Skew25dPoint]

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
            date: lambda v: v.isoformat() if v is not None else None,
        }

# ---------------------------------------------------------------------------
# Cache (30-second TTL, keyed by query params)
# ---------------------------------------------------------------------------

_cache: Dict[tuple, Dict[str, Any]] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 30  # seconds
_CACHE_MAX_SIZE = 64  # max number of cached entries


def _get_cached(key: tuple) -> Optional[VolSurfaceResponse]:
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (datetime.now(timezone.utc) - entry["ts"]).total_seconds() < _CACHE_TTL:
            return entry["data"]
    return None


def _set_cached(key: tuple, data: VolSurfaceResponse) -> None:
    with _cache_lock:
        # Evict expired entries when approaching max size
        if len(_cache) >= _CACHE_MAX_SIZE:
            now = datetime.now(timezone.utc)
            expired = [k for k, v in _cache.items()
                       if (now - v["ts"]).total_seconds() >= _CACHE_TTL]
            for k in expired:
                del _cache[k]
            # If still over limit after evicting expired, drop oldest entries
            if len(_cache) >= _CACHE_MAX_SIZE:
                oldest = sorted(_cache.items(), key=lambda x: x[1]["ts"])
                for k, _ in oldest[:len(_cache) - _CACHE_MAX_SIZE + 1]:
                    del _cache[k]
        _cache[key] = {"data": data, "ts": datetime.now(timezone.utc)}

# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------

def get_db() -> DatabaseManager:
    from ..main import db_manager
    return db_manager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iv_or_null(row: dict) -> Optional[float]:
    """Return IV as float, or None if IV is missing/non-positive."""
    iv = row.get("implied_volatility")
    if iv is None or iv <= 0:
        return None
    return float(iv)


def _interpolate_atm_iv(
    strikes_ivs: List[dict],
    spot: float,
) -> Optional[float]:
    """Linear-interpolate IV at spot from sorted (strike, iv) pairs.

    Each entry: {"strike": float, "iv": float|None}.  Uses the average
    of call and put IV when both are present.
    """
    valid = [(s["strike"], s["iv"]) for s in strikes_ivs if s["iv"] is not None]
    if not valid:
        return None

    # Exact hit
    for k, iv in valid:
        if k == spot:
            return round(iv, 6)

    below = [(k, iv) for k, iv in valid if k <= spot]
    above = [(k, iv) for k, iv in valid if k > spot]

    if below and above:
        k_lo, iv_lo = below[-1]
        k_hi, iv_hi = above[0]
        if k_hi == k_lo:
            return round(iv_lo, 6)
        frac = (spot - k_lo) / (k_hi - k_lo)
        return round(iv_lo + (iv_hi - iv_lo) * frac, 6)
    elif below:
        return round(below[-1][1], 6)
    else:
        return round(above[0][1], 6)


def _compute_25d_skew(rows_for_exp: List[dict]) -> Optional[float]:
    """25-delta put-call IV spread for a single expiration."""
    calls = [
        (float(r["delta"]), float(r["implied_volatility"]))
        for r in rows_for_exp
        if r["option_type"] == "C"
        and r["delta"] is not None
        and r["implied_volatility"] is not None
        and r.get("open_interest") and r["open_interest"] > 0
    ]
    puts = [
        (float(r["delta"]), float(r["implied_volatility"]))
        for r in rows_for_exp
        if r["option_type"] == "P"
        and r["delta"] is not None
        and r["implied_volatility"] is not None
        and r.get("open_interest") and r["open_interest"] > 0
    ]
    if not calls or not puts:
        return None

    call_25d = min(calls, key=lambda x: abs(x[0] - 0.25))
    put_25d = min(puts, key=lambda x: abs(x[0] + 0.25))

    # Reject if delta too far from 0.25 target
    if abs(call_25d[0] - 0.25) > 0.15 or abs(put_25d[0] + 0.25) > 0.15:
        return None

    return round(put_25d[1] - call_25d[1], 6)

# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("", response_model=VolSurfaceResponse)
async def get_vol_surface(
    symbol: str = Query(default="SPY", description="Underlying symbol (e.g. SPY)"),
    dte_max: int = Query(default=60, ge=1, le=365, description="Max days to expiration to include"),
    strike_count: int = Query(default=30, ge=5, le=100, description="Number of strikes centered on spot"),
    db: DatabaseManager = Depends(get_db),
):
    """Return the implied-volatility surface, ATM term structure, and 25-delta skew."""

    cache_key = (symbol.upper(), dte_max, strike_count)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        data = await db.get_vol_surface_data(symbol.upper(), dte_max, strike_count)
    except Exception as e:
        logger.error(f"Error fetching vol surface for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    if not data or not data.get("rows"):
        raise HTTPException(status_code=404, detail=f"No vol surface data for {symbol}")

    spot = float(data["spot_price"])
    timestamp = data["timestamp"]
    rows = data["rows"]
    today = date.today()

    # Group rows by expiration
    by_exp: Dict[date, List[dict]] = {}
    for r in rows:
        by_exp.setdefault(r["expiration"], []).append(r)

    # Sorted unique expirations and strikes
    expirations_sorted = sorted(by_exp.keys())
    strikes_sorted = sorted({float(r["strike"]) for r in rows})

    # Build surface slices, ATM term structure, and 25d skew
    surface: List[ExpirationSlice] = []
    atm_term: List[ATMTermPoint] = []
    skew_25d: List[Skew25dPoint] = []

    for exp in expirations_sorted:
        dte = (exp - today).days
        exp_rows = by_exp[exp]

        # Group by strike for this expiration
        by_strike: Dict[float, dict] = {}
        for r in exp_rows:
            k = float(r["strike"])
            if k not in by_strike:
                by_strike[k] = {"call_iv": None, "put_iv": None}
            iv = _iv_or_null(r)
            if r["option_type"] == "C":
                by_strike[k]["call_iv"] = iv
            else:
                by_strike[k]["put_iv"] = iv

        # Build IV list for this expiration (only include strikes in our set)
        ivs = [
            StrikeIV(strike=k, call_iv=by_strike[k]["call_iv"], put_iv=by_strike[k]["put_iv"])
            for k in strikes_sorted
            if k in by_strike
        ]
        surface.append(ExpirationSlice(expiration=exp, dte=dte, ivs=ivs))

        # ATM IV: average call/put IV per strike, then interpolate at spot
        atm_points = []
        for k in strikes_sorted:
            if k not in by_strike:
                continue
            c_iv = by_strike[k]["call_iv"]
            p_iv = by_strike[k]["put_iv"]
            vals = [v for v in (c_iv, p_iv) if v is not None]
            avg = sum(vals) / len(vals) if vals else None
            atm_points.append({"strike": k, "iv": avg})

        atm_iv = _interpolate_atm_iv(atm_points, spot)
        atm_term.append(ATMTermPoint(dte=dte, atm_iv=atm_iv))

        # 25-delta skew
        skew = _compute_25d_skew(exp_rows)
        skew_25d.append(Skew25dPoint(dte=dte, skew=skew))

    response = VolSurfaceResponse(
        symbol=symbol.upper(),
        spot_price=spot,
        timestamp=timestamp,
        expirations=expirations_sorted,
        strikes=strikes_sorted,
        surface=surface,
        atm_term_structure=atm_term,
        skew_25d=skew_25d,
    )

    _set_cached(cache_key, response)
    return response
