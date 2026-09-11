"""Spread Monitor — quoted bid/ask width and liquidity across an option chain.

    GET /api/market/spreads          — the current chain, reduced
    GET /api/market/spreads/series   — how today's widths moved, per bucket
    GET /api/market/spreads/compare  — the same reading across symbols
    GET /api/market/spreads/history  — the trailing daily rollup

Every other analytics surface here reads the book to say what it *means*.
This one reads the same rows to say whether you can get filled in it: a
gamma wall three points away is worth nothing to a trader whose put is
quoted 12.40 x 15.80.  The inputs are the ``bid`` / ``ask`` columns already
ingested on every ``option_chains`` row — no new data, just the summary
nobody had computed.

The arithmetic lives in :mod:`src.analytics.spread_stats`, which is also
what the analytics writer and the historical backfill call.  That is not
tidiness: the headline number here is scored against the daily rollup, and
a comparison computed by two implementations of the same statistic is a
comparison of the implementations.

--------------------------------------------------------------------------
Three numbers, because one cannot do the job
--------------------------------------------------------------------------

``median_relative_spread_pct``
    Width as a share of the option's own mid.  The headline: it is what
    makes a cheap option untradeable, since a put quoted 0.05 x 0.35 costs
    150% of its premium to cross.

``median_spread_bps_underlying``
    Width in basis points of the index level.  The only measure here that
    is comparable ACROSS symbols — SPX near 6,800 and NDX near 25,000 are
    not on one dollar scale and never will be.

``zero_bid_pct``
    The failure that has no width at all.  A contract quoted 0.00 x 2.40
    has no market; it is excluded from every median (there is nothing to
    take a median of) and counted here instead.  Without this number a
    chain would appear to TIGHTEN as its wings went no-bid, because only
    the still-quoted contracts would remain in the sample.

--------------------------------------------------------------------------
What these endpoints do not claim
--------------------------------------------------------------------------

* **Quoted, not effective.**  Effective spread measures fills against the
  midpoint at the time of the fill.  That needs per-trade prints and an
  NBBO to compare them to; neither is stored.  Every response carries a
  ``disclosure`` saying so, and surfaces must render it.
* **No depth.**  The feed carries no bid/ask sizes, so a tight quote for
  one contract and a tight quote for a thousand are indistinguishable here.
* **Futures are refused, not projected.**  ES / NQ carry no option chain of
  their own — their surfaces are SPX / NDX levels carried onto the futures
  price axis — so there is no futures quote to measure a width from.  The
  futures middleware answers 400 (see ``_UNSUPPORTED_PREFIXES`` in
  ``src/api/futures_middleware.py``); inventing a width by scaling an SPX
  quote would be a fabricated answer to the one question this page exists
  to answer honestly.

--------------------------------------------------------------------------
Why this rides MARKET_RAW despite publishing only aggregates
--------------------------------------------------------------------------

Nothing below is per-contract — every figure is a median or a p90 over a
population, and a median does not invert to the values behind it.  But the
CALLER picks the population: ``moneyness_band_pct`` goes to 0.25,
``dte_max`` to 0, and each bucket reports its own ``tradable_count``.  Narrow
one to a single contract and the quote falls out by arithmetic, since
``median_spread`` is then ``ask - bid`` and ``median_relative_spread_pct`` is
``200 * (ask - bid) / (ask + bid)`` for that one contract — two equations,
two unknowns, and the response names the expiration, strike band and option
type it belongs to.

Which is the premium surface's failure mode wearing an aggregate's clothes,
so it gets the premium surface's answer: gate the route, because there is no
field to redact that closes it, and suppressing thin buckets would not either
(a caller can vary the band and difference the results).  See the
``src/api/scopes.py`` docstring for where that line is drawn, and
``tests/test_market_data_scope_boundary.py`` for its enforcement.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.analytics import spread_stats as spread_stats_mod
from src.config import SPREAD_STATS_DTE_MAX, SPREAD_STATS_MONEYNESS_BAND_PCT
from src.market_calendar import is_spx_am_settled_expiration

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/market/spreads", tags=["Market Data", "Beta"])


#: Rendered on every response.  The distinction between quoted and effective
#: spread is the one a reader is most likely to get wrong, and getting it
#: wrong turns a real observation into an overstated one.
DISCLOSURE = (
    "Quoted (NBBO) spreads, not effective spreads: this measures the width "
    "market makers are showing, not what trades actually filled at. Sizes "
    "are not carried by the feed, so a tight quote for one contract and a "
    "tight quote for a thousand look identical here."
)

#: The symbols with an option chain of their own.  ES / NQ are absent
#: because they have none here — see the module docstring.
COMPARABLE_SYMBOLS = ("SPX", "NDX", "SPY", "QQQ")

#: Trailing sessions used for the "is this unusual?" percentile.  A quarter
#: of sessions is long enough to span a vol regime and short enough that a
#: structural change (a new listing schedule, a fee change) doesn't sit in
#: the window forever.
DEFAULT_HISTORY_DAYS = 60


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SpreadAggregateModel(BaseModel):
    """One population's quote quality. Widths are null when nothing is quoted."""

    contract_count: int
    tradable_count: int
    two_sided_pct: float
    zero_bid_pct: float
    crossed_or_locked_pct: float
    no_quote_pct: float
    median_spread: Optional[float] = None
    median_relative_spread_pct: Optional[float] = None
    p90_relative_spread_pct: Optional[float] = None
    median_spread_bps_underlying: Optional[float] = None
    p90_spread_bps_underlying: Optional[float] = None
    total_open_interest: int = 0
    total_volume: int = 0


class SpreadScope(BaseModel):
    """The population measured. Two readings are only comparable if these match."""

    dte_max: int
    moneyness_band_pct: float
    strike_low: float
    strike_high: float
    contract_count: int


class MoneynessBucket(SpreadAggregateModel):
    moneyness_low_pct: float
    moneyness_high_pct: float
    label: str


class ExpirationSlice(BaseModel):
    expiration: date
    dte: int
    calls: SpreadAggregateModel
    puts: SpreadAggregateModel
    all: SpreadAggregateModel


class HistoryContext(BaseModel):
    """Where today's reading sits in the symbol's own trailing history.

    Percentiles are null — not 50, not 0 — when the rollup has too little
    history to rank against. "No comparison available" and "an ordinary
    day" must never render identically.
    """

    sessions: int
    calls_percentile: Optional[float] = None
    puts_percentile: Optional[float] = None
    all_percentile: Optional[float] = None
    puts_median_over_window: Optional[float] = None
    calls_median_over_window: Optional[float] = None
    #: Today's put width divided by its median over the window. 2.0 means
    #: puts are quoted twice as wide as a typical session in the window.
    puts_vs_window_ratio: Optional[float] = None


class SpreadSnapshotResponse(BaseModel):
    symbol: str
    spot_price: float
    timestamp: datetime
    session_date: date
    basis: str = "quoted_nbbo"
    disclosure: str = DISCLOSURE
    scope: SpreadScope
    calls: SpreadAggregateModel
    puts: SpreadAggregateModel
    all: SpreadAggregateModel
    #: Put median width / call median width. Above 1 means puts are the
    #: expensive side to trade — the shape the "index puts have gone
    #: bonkers" complaint describes. Null when either side has no market.
    put_call_width_ratio: Optional[float] = None
    history: Optional[HistoryContext] = None
    calls_by_moneyness: List[MoneynessBucket]
    puts_by_moneyness: List[MoneynessBucket]
    by_expiration: List[ExpirationSlice]


class SeriesBucket(BaseModel):
    bucket_start: datetime
    #: The chain snapshot the bucket was read at — the LAST one inside it.
    anchor_ts: datetime
    spot: float
    calls: Optional[SpreadAggregateModel] = None
    puts: Optional[SpreadAggregateModel] = None


class SpreadSeriesResponse(BaseModel):
    symbol: str
    session: str
    bucket_minutes: int
    dte_max: int
    moneyness_band_pct: float
    basis: str = "quoted_nbbo"
    disclosure: str = DISCLOSURE
    bars: List[SeriesBucket]


class CompareRow(BaseModel):
    symbol: str
    spot_price: Optional[float] = None
    timestamp: Optional[datetime] = None
    #: Null with a populated `unavailable` when the chain could not be read.
    calls: Optional[SpreadAggregateModel] = None
    puts: Optional[SpreadAggregateModel] = None
    put_call_width_ratio: Optional[float] = None
    puts_percentile: Optional[float] = None
    unavailable: Optional[str] = None


class SpreadCompareResponse(BaseModel):
    symbols: List[str]
    dte_max: int
    moneyness_band_pct: float
    basis: str = "quoted_nbbo"
    disclosure: str = DISCLOSURE
    rows: List[CompareRow]


class HistoryRow(BaseModel):
    trading_date: date
    option_type: str
    spot_price: float
    contract_count: int
    tradable_count: int
    two_sided_pct: float
    zero_bid_pct: float
    crossed_or_locked_pct: float
    median_spread: Optional[float] = None
    median_relative_spread_pct: Optional[float] = None
    p90_relative_spread_pct: Optional[float] = None
    median_spread_bps_underlying: Optional[float] = None
    p90_spread_bps_underlying: Optional[float] = None


class SpreadHistoryResponse(BaseModel):
    symbol: str
    option_type: str
    dte_max: int
    moneyness_band_pct: float
    basis: str = "quoted_nbbo"
    disclosure: str = DISCLOSURE
    #: Oldest first, so a chart can render it without reversing.
    rows: List[HistoryRow]


# ---------------------------------------------------------------------------
# Cache — mirrors premium_surface / vol_surface
# ---------------------------------------------------------------------------

_cache: "OrderedDict[tuple, Dict[str, Any]]" = OrderedDict()
_cache_lock = asyncio.Lock()
_CACHE_TTL = 30  # seconds
_CACHE_MAX_SIZE = 64


async def _get_cached(key: tuple) -> Optional[Any]:
    async with _cache_lock:
        entry = _cache.get(key)
        if entry and (
            datetime.now(timezone.utc) - entry["ts"]
        ).total_seconds() < _CACHE_TTL:
            return entry["data"]
        if entry is not None:
            del _cache[key]
    return None


async def _set_cached(key: tuple, data: Any) -> None:
    async with _cache_lock:
        if key in _cache:
            del _cache[key]
        _cache[key] = {"data": data, "ts": datetime.now(timezone.utc)}
        while len(_cache) > _CACHE_MAX_SIZE:
            _cache.popitem(last=False)


def get_db() -> DatabaseManager:
    from ..main import db_manager

    assert db_manager is not None, "db_manager not initialized"
    return db_manager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _keep_contract(symbol: str, row: Dict[str, Any], session_date: date) -> bool:
    """Drop same-day SPX AM-settled contracts from the measured chain.

    Their SOQ happens at ~09:30 ET, so for the rest of the session they are
    dead instruments whose rows linger with whatever marks the feed last
    carried — reliably no-bid or absurdly wide.  Counting them would report
    a chain-wide liquidity event every third Friday.

    SPXW (weekly, PM-settled) shares the ``$SPX.X`` underlying and must NOT
    be dropped, so the option symbol decides whenever it is available.  The
    analytics snapshot applies the same rule, which is what keeps the live
    reading and the rollup measuring the same instruments.
    """
    expiration = row.get("expiration")
    if expiration != session_date:
        return True
    if (row.get("option_symbol") or "").upper().startswith("SPXW"):
        return True
    return not is_spx_am_settled_expiration(symbol, expiration)


def _ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """Guarded ratio: null unless both sides are real, positive numbers."""
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 3)


def _round(value: Any, places: int) -> Optional[float]:
    """Round a DB numeric to ``places``, preserving null as null."""
    if value is None:
        return None
    try:
        return round(float(value), places)
    except (TypeError, ValueError):
        return None


def _bps(median_spread: Any, spot: Any) -> Optional[float]:
    """Convert a bucket's median dollar width to basis points of the underlying.

    Exact rather than approximate: spot is constant within a bucket, so
    ``median(10000 * w / S)`` and ``10000 * median(w) / S`` are the same
    number.  Deriving it here saves a second percentile pass in SQL over the
    identical ordering.
    """
    width = _round(median_spread, 8)
    price = _round(spot, 8)
    if width is None or price is None or price <= 0:
        return None
    return round(10_000.0 * width / price, 3)


async def _reduce_chain(
    db: DatabaseManager, symbol: str, dte_max: int, band_pct: float
) -> Optional[Dict[str, Any]]:
    """Fetch the latest stable chain for ``symbol`` and reduce it.

    Returns None when the symbol has no readable chain — no spot, no
    snapshot, or nothing left in scope after the AM-settled filter.  The
    callers turn that into a 404 (single symbol) or an ``unavailable`` row
    (the comparison), never into zeros.
    """
    data = await db.get_spread_snapshot_chain(symbol, dte_max, band_pct)
    if not data or not data.get("rows"):
        return None

    session_date = data["session_date"]
    rows = [r for r in data["rows"] if _keep_contract(symbol, r, session_date)]
    if not rows:
        return None

    spot = float(data["spot_price"])
    spreads = spread_stats_mod.contract_spreads(rows, spot)
    if not spreads:
        return None

    dte_of = {
        r["expiration"]: (r["expiration"] - session_date).days
        for r in rows
        if r.get("expiration") is not None
    }

    return {
        "spot": spot,
        "snapshot_ts": data["snapshot_ts"],
        "session_date": session_date,
        "rows": rows,
        "spreads": spreads,
        "by_type": spread_stats_mod.aggregate_by_option_type(spreads),
        "dte_of": dte_of,
    }


async def _history_context(
    db: DatabaseManager,
    symbol: str,
    reduced: Dict[str, Any],
    days: int,
) -> Optional[HistoryContext]:
    """Rank today's widths against the symbol's own trailing sessions.

    Compares like with like or not at all: rows measured under a different
    ``dte_max`` / moneyness band are excluded rather than blended in, since
    a percentile across two different populations ranks the populations.

    Today's own rollup row is excluded from the population it is ranked
    against — including it would drag every reading toward the middle of
    its own window, most visibly on the day it matters, when today is the
    outlier.

    Returns None when nothing comparable survives.  The page then shows the
    live reading with no verdict attached, which is the honest state on a
    fresh deployment.
    """
    by_type = reduced["by_type"]
    session_date = reduced["session_date"]

    async def _window(option_type: str) -> List[float]:
        rows = await db.get_daily_spread_history(symbol, option_type, days)
        return [
            float(r["median_relative_spread_pct"])
            for r in rows
            if r.get("median_relative_spread_pct") is not None
            and r.get("trading_date") != session_date
            and int(r.get("dte_max") or -1) == int(SPREAD_STATS_DTE_MAX)
            and float(r.get("moneyness_band_pct") or -1.0)
            == float(SPREAD_STATS_MONEYNESS_BAND_PCT)
        ]

    try:
        calls_window, puts_window, all_window = await asyncio.gather(
            _window("C"), _window("P"), _window("A")
        )
    except Exception as exc:
        # The rollup is a convenience, not the reading. A missing table on a
        # partially-migrated deployment must not take the whole page down.
        logger.warning("Spread history unavailable for %s: %s", symbol, exc)
        return None

    sessions = max(len(calls_window), len(puts_window), len(all_window))
    if sessions == 0:
        return None

    puts_now = by_type["puts"].median_relative_spread_pct
    calls_now = by_type["calls"].median_relative_spread_pct
    all_now = by_type["all"].median_relative_spread_pct

    puts_median = spread_stats_mod.percentile(puts_window, 50)
    calls_median = spread_stats_mod.percentile(calls_window, 50)

    return HistoryContext(
        sessions=sessions,
        calls_percentile=(
            spread_stats_mod.percentile_rank(calls_now, calls_window)
            if calls_now is not None
            else None
        ),
        puts_percentile=(
            spread_stats_mod.percentile_rank(puts_now, puts_window)
            if puts_now is not None
            else None
        ),
        all_percentile=(
            spread_stats_mod.percentile_rank(all_now, all_window)
            if all_now is not None
            else None
        ),
        puts_median_over_window=(
            None if puts_median is None else round(puts_median, 3)
        ),
        calls_median_over_window=(
            None if calls_median is None else round(calls_median, 3)
        ),
        puts_vs_window_ratio=_ratio(puts_now, puts_median),
    )


def _agg_model(agg: spread_stats_mod.SpreadAggregate) -> SpreadAggregateModel:
    return SpreadAggregateModel(**agg.to_dict())


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=SpreadSnapshotResponse)
async def get_spread_snapshot(
    symbol: str = Query(default="SPX", description="Underlying symbol"),
    dte_max: int = Query(
        default=SPREAD_STATS_DTE_MAX,
        ge=0,
        le=90,
        description="Max days to expiration to include",
    ),
    moneyness_band_pct: float = Query(
        default=SPREAD_STATS_MONEYNESS_BAND_PCT,
        ge=0.25,
        le=25.0,
        description="Half-width of the strike band around spot, in percent",
    ),
    history_days: int = Query(
        default=DEFAULT_HISTORY_DAYS,
        ge=0,
        le=180,
        description="Trailing sessions to rank today's reading against (0 to skip)",
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Current quoted-width and liquidity across one symbol's near-dated chain.

    **Beta** — contract may change.
    """
    sym = symbol.upper()
    cache_key = ("snapshot", sym, dte_max, moneyness_band_pct, history_days)
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        reduced = await _reduce_chain(db, sym, dte_max, moneyness_band_pct)
    except Exception as e:
        logger.error(f"Error fetching spread snapshot for {sym}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    if reduced is None:
        raise HTTPException(
            status_code=404, detail=f"No quoted option chain available for {sym}"
        )

    spot = reduced["spot"]
    by_type = reduced["by_type"]
    spreads = reduced["spreads"]
    band = float(moneyness_band_pct) / 100.0

    history = None
    if history_days > 0:
        history = await _history_context(db, sym, reduced, history_days)

    response = SpreadSnapshotResponse(
        symbol=sym,
        spot_price=round(spot, 4),
        timestamp=reduced["snapshot_ts"],
        session_date=reduced["session_date"],
        scope=SpreadScope(
            dte_max=dte_max,
            moneyness_band_pct=moneyness_band_pct,
            strike_low=round(spot * (1.0 - band), 4),
            strike_high=round(spot * (1.0 + band), 4),
            contract_count=len(spreads),
        ),
        calls=_agg_model(by_type["calls"]),
        puts=_agg_model(by_type["puts"]),
        all=_agg_model(by_type["all"]),
        put_call_width_ratio=_ratio(
            by_type["puts"].median_relative_spread_pct,
            by_type["calls"].median_relative_spread_pct,
        ),
        history=history,
        calls_by_moneyness=[
            MoneynessBucket(**b)
            for b in spread_stats_mod.aggregate_by_moneyness(spreads, option_type="C")
        ],
        puts_by_moneyness=[
            MoneynessBucket(**b)
            for b in spread_stats_mod.aggregate_by_moneyness(spreads, option_type="P")
        ],
        by_expiration=[
            ExpirationSlice(
                expiration=slice_["expiration"],
                dte=slice_["dte"],
                calls=SpreadAggregateModel(**slice_["calls"]),
                puts=SpreadAggregateModel(**slice_["puts"]),
                all=SpreadAggregateModel(**slice_["all"]),
            )
            for slice_ in spread_stats_mod.aggregate_by_expiration(
                spreads, reduced["dte_of"]
            )
        ],
    )

    await _set_cached(cache_key, response)
    return response


@router.get("/series", response_model=SpreadSeriesResponse)
async def get_spread_series(
    symbol: str = Query(default="SPX", description="Underlying symbol"),
    session: str = Query(
        default="current", pattern="^(current|prior)$", description="Trading session"
    ),
    bucket_minutes: int = Query(
        default=15, ge=1, le=60, description="Bucket size in minutes"
    ),
    dte_max: int = Query(default=SPREAD_STATS_DTE_MAX, ge=0, le=90),
    moneyness_band_pct: float = Query(
        default=SPREAD_STATS_MONEYNESS_BAND_PCT, ge=0.25, le=25.0
    ),
    db: DatabaseManager = Depends(get_db),
):
    """How today's quoted widths moved through the session, puts against calls.

    One reading per bucket, taken at the last chain snapshot inside it.
    Calls and puts are returned separately and no blended row is computed:
    the whole point of the series is the divergence between the two, and a
    combined median reports roughly half of it.

    **Beta** — contract may change.
    """
    sym = symbol.upper()
    cache_key = (
        "series",
        sym,
        session,
        bucket_minutes,
        dte_max,
        moneyness_band_pct,
    )
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        rows = await db.get_spread_intraday_series(
            sym, dte_max, moneyness_band_pct, bucket_minutes, session
        )
    except Exception as e:
        logger.error(f"Error fetching spread series for {sym}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # The query groups by (bucket, option_type); fold the two rows of each
    # bucket back into one bar.
    buckets: "OrderedDict[Any, Dict[str, Any]]" = OrderedDict()
    for row in rows:
        key = row["bucket_start"]
        bar = buckets.setdefault(
            key,
            {
                "bucket_start": row["bucket_start"],
                "anchor_ts": row["anchor_ts"],
                "spot": float(row["spot"]),
                "calls": None,
                "puts": None,
            },
        )
        total = int(row["contract_count"] or 0)
        if total == 0:
            continue

        def _pct(count: Any) -> float:
            return round(100.0 * int(count or 0) / total, 2)

        agg = SpreadAggregateModel(
            contract_count=total,
            tradable_count=int(row["tradable_count"] or 0),
            two_sided_pct=_pct(row["tradable_count"]),
            zero_bid_pct=_pct(row["zero_bid_count"]),
            crossed_or_locked_pct=_pct(row["crossed_or_locked_count"]),
            no_quote_pct=round(
                max(
                    0.0,
                    100.0
                    - _pct(row["tradable_count"])
                    - _pct(row["zero_bid_count"])
                    - _pct(row["crossed_or_locked_count"]),
                ),
                2,
            ),
            median_spread=_round(row["median_spread"], 4),
            median_relative_spread_pct=_round(row["median_relative_spread_pct"], 3),
            p90_relative_spread_pct=_round(row["p90_relative_spread_pct"], 3),
            median_spread_bps_underlying=_bps(row["median_spread"], row["spot"]),
            p90_spread_bps_underlying=None,
            total_open_interest=int(row["total_open_interest"] or 0),
            total_volume=int(row["total_volume"] or 0),
        )
        bar["calls" if row["option_type"] == "C" else "puts"] = agg

    response = SpreadSeriesResponse(
        symbol=sym,
        session=session,
        bucket_minutes=bucket_minutes,
        dte_max=dte_max,
        moneyness_band_pct=moneyness_band_pct,
        bars=[SeriesBucket(**b) for b in buckets.values()],
    )

    await _set_cached(cache_key, response)
    return response


@router.get("/compare", response_model=SpreadCompareResponse)
async def compare_spreads(
    symbols: str = Query(
        default=",".join(COMPARABLE_SYMBOLS),
        description="Comma-separated underlyings to compare",
    ),
    dte_max: int = Query(default=SPREAD_STATS_DTE_MAX, ge=0, le=90),
    moneyness_band_pct: float = Query(
        default=SPREAD_STATS_MONEYNESS_BAND_PCT, ge=0.25, le=25.0
    ),
    history_days: int = Query(default=DEFAULT_HISTORY_DAYS, ge=0, le=180),
    db: DatabaseManager = Depends(get_db),
):
    """The same reading side by side across symbols.

    Read ``median_spread_bps_underlying`` for the cross-symbol comparison,
    not ``median_spread``: index levels differ by 4x between SPX and NDX, so
    their dollar widths are not on one scale.

    A symbol whose chain cannot be read comes back as a row with
    ``unavailable`` set rather than being dropped — a missing row would read
    as "not compared", and a zeroed one as "perfectly tight".

    **Beta** — contract may change.
    """
    requested = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not requested:
        raise HTTPException(status_code=400, detail="No symbols requested")
    if len(requested) > 8:
        raise HTTPException(status_code=400, detail="At most 8 symbols per request")

    cache_key = (
        "compare",
        tuple(requested),
        dte_max,
        moneyness_band_pct,
        history_days,
    )
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    async def _row(sym: str) -> CompareRow:
        try:
            reduced = await _reduce_chain(db, sym, dte_max, moneyness_band_pct)
        except Exception as exc:
            logger.warning("Spread comparison failed for %s: %s", sym, exc)
            return CompareRow(symbol=sym, unavailable="lookup failed")

        if reduced is None:
            return CompareRow(symbol=sym, unavailable="no quoted chain")

        by_type = reduced["by_type"]
        history = None
        if history_days > 0:
            history = await _history_context(db, sym, reduced, history_days)

        return CompareRow(
            symbol=sym,
            spot_price=round(reduced["spot"], 4),
            timestamp=reduced["snapshot_ts"],
            calls=_agg_model(by_type["calls"]),
            puts=_agg_model(by_type["puts"]),
            put_call_width_ratio=_ratio(
                by_type["puts"].median_relative_spread_pct,
                by_type["calls"].median_relative_spread_pct,
            ),
            puts_percentile=history.puts_percentile if history else None,
        )

    rows = await asyncio.gather(*(_row(sym) for sym in requested))

    response = SpreadCompareResponse(
        symbols=requested,
        dte_max=dte_max,
        moneyness_band_pct=moneyness_band_pct,
        rows=list(rows),
    )

    await _set_cached(cache_key, response)
    return response


@router.get("/history", response_model=SpreadHistoryResponse)
async def get_spread_history(
    symbol: str = Query(default="SPX", description="Underlying symbol"),
    option_type: str = Query(
        default="P",
        pattern="^[CPA]$",
        description="C (calls), P (puts) or A (blended chain)",
    ),
    days: int = Query(default=DEFAULT_HISTORY_DAYS, ge=1, le=180),
    db: DatabaseManager = Depends(get_db),
):
    """Trailing daily quoted-width history from the ``daily_spread_stats`` rollup.

    This is what turns "spreads are 6.2% wide" into "spreads are wider than
    they have been all quarter".  Rows are oldest first.

    An empty ``rows`` list is a normal answer on a deployment where neither
    the analytics writer nor ``src.tools.daily_spread_stats_backfill`` has
    run yet — not an error, and the caller should render the live reading
    without a historical verdict rather than an error state.

    **Beta** — contract may change.
    """
    sym = symbol.upper()
    cache_key = ("history", sym, option_type, days)
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        rows = await db.get_daily_spread_history(sym, option_type, days)
    except Exception as e:
        logger.error(f"Error fetching spread history for {sym}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    response = SpreadHistoryResponse(
        symbol=sym,
        option_type=option_type,
        dte_max=int(SPREAD_STATS_DTE_MAX),
        moneyness_band_pct=float(SPREAD_STATS_MONEYNESS_BAND_PCT),
        rows=[
            HistoryRow(
                trading_date=r["trading_date"],
                option_type=r["option_type"],
                spot_price=float(r["spot_price"]),
                contract_count=int(r["contract_count"] or 0),
                tradable_count=int(r["tradable_count"] or 0),
                two_sided_pct=float(r["two_sided_pct"] or 0.0),
                zero_bid_pct=float(r["zero_bid_pct"] or 0.0),
                crossed_or_locked_pct=float(r["crossed_or_locked_pct"] or 0.0),
                median_spread=_round(r["median_spread"], 4),
                median_relative_spread_pct=_round(
                    r["median_relative_spread_pct"], 3
                ),
                p90_relative_spread_pct=_round(r["p90_relative_spread_pct"], 3),
                median_spread_bps_underlying=_round(
                    r["median_spread_bps_underlying"], 3
                ),
                p90_spread_bps_underlying=_round(r["p90_spread_bps_underlying"], 3),
            )
            for r in rows
        ],
    )

    await _set_cached(cache_key, response)
    return response
