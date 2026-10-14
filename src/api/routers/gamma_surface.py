"""Gamma Surface router.

GET /api/gex/gamma-surface?symbol=SPX&expirations=all

Net dealer gamma across hypothetical price (``grid``) and forward time
(``times_days``, from the snapshot to the first selected settlement), for the
expirations the caller picks: ``all`` by default, or a comma-separated set of
YYYY-MM-DD dates (the same filter shape /api/gex/strike-profile-timeseries
takes). The model is in :mod:`src.analytics.gamma_surface`.

Computed on demand from the latest option snapshot -- the same
``_get_snapshot`` the analytics cycle and the flip surfaces read, with the same
on-demand engine tuning. One copy of the snapshot per symbol per worker is
shared across expiration selections for ``_BOOK_TTL_SECONDS``, so switching the
filter re-prices instead of re-reading the chain, and a response is cached only
until the snapshot behind it turns that old.

The Call/Put Walls are deliberately NOT in this payload. The chart reads them
from the same sources every other chart uses for the same selection
(/api/gex/summary for all, the strike-profile-timeseries bucket for a subset),
so the ceiling and floor it draws can never disagree with the walls beside it.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.analytics.forced_flow import ContractLeg
from src.analytics.gamma_surface import (
    DEFAULT_PRICE_POINTS,
    DEFAULT_TIME_STEPS,
    compute_gamma_surface,
)

from .gex_flip_horizon import _prepare_endpoint_engine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gex", tags=["GEX"])

# How long one loaded snapshot serves every selection for a symbol. Matches the
# flip surfaces' response TTL: the analytics tick is 60s, so a 30s-old book is
# as current as the persisted numbers on the same page.
_BOOK_TTL_SECONDS = 30.0
# A handful of symbols in practice.
_BOOK_CACHE_MAX = 16
# Keys are (symbol, selection, grid params); the selection is caller-supplied,
# so the cache is bounded.
_RESPONSE_CACHE_MAX = 128
# Ceiling on price_points x time_steps. The defaults (121 x 41) are ~5k cells.
_MAX_CELLS = 12_000

_book_cache: Dict[str, Dict[str, Any]] = {}
_book_cache_lock = threading.Lock()
_book_tasks: Dict[str, "asyncio.Task"] = {}
_response_cache: Dict[tuple, Dict[str, Any]] = {}
_response_cache_lock = threading.Lock()


class GammaSurfaceResponse(BaseModel):
    symbol: str
    spot: float
    timestamp: datetime
    # Expirations actually priced (live, with open interest), ascending.
    expirations: List[date]
    # The expiration whose settlement ends the time axis, and that instant.
    settling_expiration: date
    settles_at: datetime
    grid: List[float]
    times_days: List[float]
    # profiles[i][j]: dealer dollar gamma per 1% move at times_days[i], grid[j]
    # (calls +, puts -).
    profiles: List[List[float]]
    contract_count: int


def _expiration_key(value: Any) -> str:
    """``YYYY-MM-DD`` for a snapshot expiration (a ``date``; a ``datetime`` or a
    string is tolerated)."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return str(value)[:10]


def _load_book(symbol: str) -> Optional[Dict[str, Any]]:
    """Latest snapshot as priceable legs grouped by expiration, or None."""
    engine = _prepare_endpoint_engine(symbol)
    snapshot = engine._get_snapshot()
    if not snapshot or not snapshot.get("options"):
        return None
    spot = snapshot.get("underlying_price")
    if not spot or spot <= 0:
        return None
    timestamp = snapshot["timestamp"]
    by_expiration: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for opt in snapshot["options"]:
        by_expiration[_expiration_key(opt.get("expiration"))].append(opt)
    # The engine's own leg builder: settlement-anchored time to expiry (AM- vs
    # PM-settled on a shared date) and the null-IV / zero-OI skip.
    legs_by_expiration: Dict[str, List[ContractLeg]] = {}
    for expiration, rows in by_expiration.items():
        legs = engine._build_forced_flow_legs(rows, timestamp)
        if legs:
            legs_by_expiration[expiration] = legs
    if not legs_by_expiration:
        return None
    return {
        "spot": float(spot),
        "timestamp": timestamp,
        "r": engine.risk_free_rate,
        "q": engine.dividend_yield,
        "legs_by_expiration": legs_by_expiration,
    }


def _book_get(symbol: str) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    with _book_cache_lock:
        book = _book_cache.get(symbol)
    if book is not None and now - book["loaded_at"] < _BOOK_TTL_SECONDS:
        return book
    return None


def _book_put(symbol: str, book: Dict[str, Any]) -> None:
    with _book_cache_lock:
        _book_cache.pop(symbol, None)
        _book_cache[symbol] = book
        while len(_book_cache) > _BOOK_CACHE_MAX:
            _book_cache.pop(next(iter(_book_cache)))


async def _shared_book(symbol: str) -> Optional[Dict[str, Any]]:
    """The cached book for ``symbol``, loading it at most once per TTL however
    many requests ask. Concurrent misses await one shared load (no thread is
    held while waiting); a degraded snapshot (None) is not cached."""
    book = _book_get(symbol)
    if book is not None:
        return book
    task = _book_tasks.get(symbol)
    if task is None:

        async def _fetch() -> Optional[Dict[str, Any]]:
            try:
                fresh = await asyncio.to_thread(_load_book, symbol)
                if fresh is not None:
                    fresh["loaded_at"] = time.monotonic()
                    _book_put(symbol, fresh)
                return fresh
            finally:
                _book_tasks.pop(symbol, None)

        task = asyncio.create_task(_fetch())
        _book_tasks[symbol] = task
    # Shielded: one cancelled request must not cancel the load others share.
    return await asyncio.shield(task)


def _response_get(key: tuple) -> Optional[GammaSurfaceResponse]:
    now = time.monotonic()
    with _response_cache_lock:
        entry = _response_cache.get(key)
    if entry is not None and now - entry["as_of"] < _BOOK_TTL_SECONDS:
        return entry["data"]  # type: ignore[no-any-return]
    return None


def _response_put(key: tuple, data: GammaSurfaceResponse, as_of: float) -> None:
    # Aged from the book's load time, so a response priced from a 29s-old book
    # is not served for another full TTL.
    with _response_cache_lock:
        _response_cache.pop(key, None)
        _response_cache[key] = {"as_of": as_of, "data": data}
        while len(_response_cache) > _RESPONSE_CACHE_MAX:
            _response_cache.pop(next(iter(_response_cache)))


def _parse_expirations(raw: Optional[str]) -> Optional[Tuple[str, ...]]:
    """``None`` for all expirations, else the sorted, de-duplicated dates.

    Same contract as /api/gex/strike-profile-timeseries: ``all`` (any case) or
    a comma-separated list of YYYY-MM-DD dates; an all-blank list is All, and
    any unparseable entry is a 400.
    """
    text = (raw or "all").strip()
    if text.lower() == "all":
        return None
    parsed = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            parsed.add(date.fromisoformat(part).isoformat())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="expirations must be 'all' or a comma-separated list of YYYY-MM-DD dates",
            )
    return tuple(sorted(parsed)) or None


def _build_response(
    book: Dict[str, Any],
    symbol: str,
    selection: Optional[Tuple[str, ...]],
    span_pct: Optional[float],
    price_points: int,
    time_steps: int,
) -> Optional[GammaSurfaceResponse]:
    """Price the surface for one selection from a loaded book; None when the
    selection holds no live contract."""
    legs_by_expiration: Dict[str, List[ContractLeg]] = book["legs_by_expiration"]
    expirations = sorted(
        legs_by_expiration if selection is None else set(selection) & set(legs_by_expiration)
    )
    legs = [leg for expiration in expirations for leg in legs_by_expiration[expiration]]
    surface = compute_gamma_surface(
        legs,
        book["spot"],
        book["r"],
        book["q"],
        span_pct=span_pct,
        price_points=price_points,
        time_steps=time_steps,
    )
    if surface is None:
        return None

    settling_expiration = min(
        expirations, key=lambda e: min(leg.tte_years for leg in legs_by_expiration[e])
    )
    timestamp = book["timestamp"]
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return GammaSurfaceResponse(
        symbol=symbol,
        spot=book["spot"],
        timestamp=timestamp,
        expirations=[date.fromisoformat(e) for e in expirations],
        settling_expiration=date.fromisoformat(settling_expiration),
        settles_at=timestamp + timedelta(days=surface["settles_in_years"] * 365.0),
        grid=[round(p, 2) for p in surface["grid"]],
        times_days=[round(t * 365.0, 6) for t in surface["times_years"]],
        # Whole dollars: these run to billions, so the cents are noise on the wire.
        profiles=[[float(round(v)) for v in row] for row in surface["profiles"]],
        contract_count=len(legs),
    )


@router.get("/gamma-surface", response_model=GammaSurfaceResponse)
async def get_gamma_surface(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    expirations: str = Query(
        default="all",
        description=(
            "'all' for every live expiration, or a comma-separated list of "
            "YYYY-MM-DD expiration dates to price only that set."
        ),
    ),
    span_pct: Optional[float] = Query(
        default=None,
        ge=0.005,
        le=0.15,
        description=(
            "Half-width of the price grid as a fraction of spot. Omit to frame "
            "the selected contracts' strikes."
        ),
    ),
    price_points: int = Query(default=DEFAULT_PRICE_POINTS, ge=21, le=241),
    time_steps: int = Query(default=DEFAULT_TIME_STEPS, ge=2, le=97),
):
    """Net dealer gamma across price and time to settlement (see module docstring)."""
    selection = _parse_expirations(expirations)
    if price_points * time_steps > _MAX_CELLS:
        raise HTTPException(
            status_code=400,
            detail=f"price_points x time_steps must be at most {_MAX_CELLS}",
        )
    symbol_upper = symbol.upper()
    key = (symbol_upper, selection, span_pct, price_points, time_steps)

    cached = _response_get(key)
    if cached is not None:
        return cached

    try:
        book = await _shared_book(symbol_upper)
        response = (
            await asyncio.to_thread(
                _build_response,
                book,
                symbol_upper,
                selection,
                span_pct,
                price_points,
                time_steps,
            )
            if book is not None
            else None
        )
    except Exception as e:
        logger.error("gamma-surface compute failed for %s: %s", key, e, exc_info=True)
        raise HTTPException(status_code=500, detail="gamma-surface compute failed")

    if book is None:
        raise HTTPException(status_code=404, detail=f"No usable option snapshot for {symbol_upper}")
    if response is None:
        raise HTTPException(
            status_code=404,
            detail="No live contracts with open interest for the selected expirations",
        )
    _response_put(key, response, as_of=book["loaded_at"])
    return response
