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
from collections import OrderedDict
import asyncio
import logging
import os

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gex/vol_surface", tags=["GEX"])

# ---------------------------------------------------------------------------
# Data quality filters
# ---------------------------------------------------------------------------
# The IV solver clamps at IV_MAX (default 5.0 = 500%) and is also prone to
# numerical artefacts on options approaching settlement: with T → 0 the BS
# vega collapses, so a stale post-close mark of even a few dollars on a
# near-the-money strike can solve to an IV of 1.5–4.0 that has no bearing
# on the live volatility surface.  The vol-surface endpoint is a regime
# snapshot, not a per-contract pricing tool, so we filter these out at
# the API layer:
#
# - VOL_SURFACE_IV_MAX caps individual IVs.  Anything above the cap is
#   nulled (treated as "no usable IV at this strike"); the surface is
#   re-computed without it so the ATM-IV interpolation and the 25-delta
#   skew don't pick up the artefact.  Default 2.0 (200%) is well above
#   even severe SPY stress (COVID 2020 saw VIX ~80 → ATM IVs ~80%), so
#   it never trims legitimate values, but it always catches the
#   near-expiry solver blow-ups (1.5+ on SPY at any DTE > 0 is virtually
#   always an artefact).
# - VOL_SURFACE_MIN_STRIKE_COVERAGE drops whole expirations when, after
#   the per-IV filter, fewer than this fraction of the requested strikes
#   carry at least one valid IV.  An expiration whose snapshot is so
#   sparse that the surface row is dominated by nulls (the 0DTE
#   post-close "ghost" expiration is the canonical case) is more noise
#   than signal — dropping it is preferable to handing the frontend a
#   row that would render as a near-empty line.
def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


VOL_SURFACE_IV_MAX = _env_float("VOL_SURFACE_IV_MAX", 2.0)
VOL_SURFACE_MIN_STRIKE_COVERAGE = _env_float("VOL_SURFACE_MIN_STRIKE_COVERAGE", 0.30)

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
#
# FastAPI endpoints are async, so we use asyncio.Lock rather than
# threading.Lock to avoid blocking the event loop.  OrderedDict gives O(1)
# FIFO eviction when the size cap is reached (no sort needed).
# ---------------------------------------------------------------------------

_cache: "OrderedDict[tuple, Dict[str, Any]]" = OrderedDict()
_cache_lock = asyncio.Lock()
_CACHE_TTL = 30  # seconds
_CACHE_MAX_SIZE = 64  # max number of cached entries


async def _get_cached(key: tuple) -> Optional[VolSurfaceResponse]:
    async with _cache_lock:
        entry = _cache.get(key)
        if entry and (datetime.now(timezone.utc) - entry["ts"]).total_seconds() < _CACHE_TTL:
            return entry["data"]
        if entry is not None:
            # Stale — drop it so it doesn't linger in the oldest slot.
            del _cache[key]
    return None


async def _set_cached(key: tuple, data: VolSurfaceResponse) -> None:
    async with _cache_lock:
        # Refresh key position so it counts as newest.
        if key in _cache:
            del _cache[key]
        _cache[key] = {"data": data, "ts": datetime.now(timezone.utc)}
        # Drop oldest entries until under the size cap (O(1) each).
        while len(_cache) > _CACHE_MAX_SIZE:
            _cache.popitem(last=False)


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
    """Return IV as float, or None if IV is missing/non-positive/outlier.

    The upper bound (``VOL_SURFACE_IV_MAX``) filters out IV solver
    artefacts on near-expiry / stale-mark options that round-trip the
    IV_MAX clamp (5.0) or land at the 1.5-4.0 "couldn't converge"
    plateau.  These values are not real volatility — they're the
    solver hitting a pricing surface with vanishing vega — and they
    silently corrupt the ATM-IV interpolation and the 25-delta skew
    when they slip through.
    """
    iv = row.get("implied_volatility")
    if iv is None or iv <= 0:
        return None
    iv_f = float(iv)
    if iv_f > VOL_SURFACE_IV_MAX:
        return None
    return iv_f


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
    """25-delta put-call IV spread for a single expiration.

    Reuses ``_iv_or_null`` so the outlier filter applies here too: a
    near-expiry put with IV solving to 3.5 would otherwise dominate the
    25-delta skew with a fictional spread.
    """
    calls = []
    puts = []
    for r in rows_for_exp:
        if r["delta"] is None or not r.get("open_interest") or r["open_interest"] <= 0:
            continue
        iv = _iv_or_null(r)
        if iv is None:
            continue
        d = float(r["delta"])
        if r["option_type"] == "C":
            calls.append((d, iv))
        elif r["option_type"] == "P":
            puts.append((d, iv))
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
    strike_count: int = Query(
        default=30, ge=5, le=100, description="Number of strikes centered on spot"
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Return the implied-volatility surface, ATM term structure, and 25-delta skew."""

    cache_key = (symbol.upper(), dte_max, strike_count)
    cached = await _get_cached(cache_key)
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

    # Build surface slices, ATM term structure, and 25d skew.
    # ``min_strikes_required`` enforces the per-expiration coverage
    # floor: with default 30% of e.g. 30 requested strikes that's 9
    # strikes that must carry at least one valid IV.  Anything less is
    # treated as a degraded snapshot and excluded from the response
    # entirely (the canonical case is a 0DTE expiration whose
    # post-close snapshot has only a handful of artefact IVs at strikes
    # bracketing spot — the kind of row that silently corrupts the
    # ATM-IV term structure if surfaced).
    min_strikes_required = max(
        1, int(len(strikes_sorted) * VOL_SURFACE_MIN_STRIKE_COVERAGE)
    )

    surface: List[ExpirationSlice] = []
    atm_term: List[ATMTermPoint] = []
    skew_25d: List[Skew25dPoint] = []
    kept_expirations: List[date] = []

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

        # Per-expiration coverage check: count strikes that, after the
        # outlier filter, carry at least one valid IV.  Skip the whole
        # expiration when coverage falls below the configured floor.
        usable_strike_count = sum(
            1
            for v in by_strike.values()
            if v["call_iv"] is not None or v["put_iv"] is not None
        )
        if usable_strike_count < min_strikes_required:
            logger.info(
                "vol_surface[%s]: dropping expiration %s (DTE=%d) — "
                "only %d/%d strikes carry a valid IV (< %.0f%% floor); "
                "snapshot is degraded.",
                symbol.upper(),
                exp.isoformat(),
                dte,
                usable_strike_count,
                len(strikes_sorted),
                VOL_SURFACE_MIN_STRIKE_COVERAGE * 100,
            )
            continue

        # Build IV list for this expiration (only include strikes in our set)
        ivs = [
            StrikeIV(strike=k, call_iv=by_strike[k]["call_iv"], put_iv=by_strike[k]["put_iv"])
            for k in strikes_sorted
            if k in by_strike
        ]
        surface.append(ExpirationSlice(expiration=exp, dte=dte, ivs=ivs))
        kept_expirations.append(exp)

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

    if not surface:
        raise HTTPException(
            status_code=404,
            detail=f"No vol surface data for {symbol} after data-quality filters",
        )

    response = VolSurfaceResponse(
        symbol=symbol.upper(),
        spot_price=spot,
        timestamp=timestamp,
        expirations=kept_expirations,
        strikes=strikes_sorted,
        surface=surface,
        atm_term_structure=atm_term,
        skew_25d=skew_25d,
    )

    await _set_cached(cache_key, response)
    return response
