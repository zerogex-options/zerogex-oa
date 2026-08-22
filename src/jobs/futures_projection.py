"""Carry SPX / NDX dealer levels across to the ES / NQ price axis.

ZeroGEX computes gamma from INDEX option chains and never from options on
futures.  ES and SPX track the same index, so the dealer book behind an ES
chart *is* the SPX book — what differs is the price axis, because a future
trades at a cost-of-carry premium to cash::

    F = S x e^((r - q) x T)        T = time to the future's quarterly expiry

So an ES surface is the SPX surface with its PRICE-SPACE fields carried
across by the ratio ``F / S``.  Two rules keep that honest, and they are the
whole design:

1. **Levels are projected; dollars are not.**  Strikes, walls, the gamma
   flip, max pain and the pin all move to the futures axis.  ``net_gex``,
   wall strengths, OI and volume do NOT — dealer exposure is a property of
   the option book, not of the axis you plot it on.  Rescaling them would
   invent exposure that nobody holds.
2. **Spot is never projected.**  The live ES print comes from the futures
   feed (``futures_quotes``), not from ``SPX x ratio``.  Overnight SPX is
   frozen at its 16:00 close, so a projected spot would report where ES was
   at the bell rather than where it is now — while the projected *levels*
   remain correct, because they describe an option book that likewise has
   not changed since the close.  Reading live ES against projected walls is
   exactly how an overnight futures trader uses these levels.

The ratio is MEASURED, not modelled: the futures ingester now runs the whole
CME session, so ``futures_quotes`` and ``underlying_quotes`` both print
during the cash session and the ratio can be read straight off the tape as
the median of the last several concurrent pairs.  Measuring beats modelling
here because it needs no view on repo rates or dividend timing and it
self-corrects through a quarterly roll.  The theoretical carry ratio remains
as a fallback for a cold cache or a dead feed.

Sanity bound: a measured ratio more than ``FUTURES_BASIS_MAX_DEVIATION``
away from 1.0 is rejected.  That is the guard against a back-adjusted
continuous contract — if ``@ES`` is ever configured to a back-adjusted
series its "price" is not a tradable level and the ratio would be nonsense;
better to fall back to carry than to publish levels off a phantom axis.

PROJECTION IS READ-SIDE ONLY.  Nothing here is persisted, and no projected
value ever reaches GEX, greeks, signals, settlement or any DB write — those
all continue to key on the cash index.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import exp
from statistics import median
from typing import Any, Dict, Iterable, Optional

from src.config import RISK_FREE_RATE, _getenv_float, _getenv_int, resolve_dividend_yield
from src.symbols import (
    is_futures_symbol,
    resolve_futures_alias,
    resolve_futures_index,
    resolve_futures_tick,
    resolve_index_future,
    round_to_tick,
)

logger = logging.getLogger("zerogex.futures_projection")


# A measured ratio further than this from 1.0 is treated as a bad feed
# rather than a real basis.  Equity-index carry is well under 1% over a
# quarter; 3% leaves generous headroom while still catching a back-adjusted
# or wrong-symbol series.
_MAX_DEVIATION = _getenv_float("FUTURES_BASIS_MAX_DEVIATION", 0.03)

# How far back to hunt for a concurrent pair.  Must span a long weekend so
# Monday pre-open still projects off Friday's measurement, but stay well
# under a quarter so a measurement can never survive a contract roll.
_LOOKBACK_MINUTES = _getenv_int("FUTURES_BASIS_LOOKBACK_MINUTES", 5760)  # 4 days

# Median over this many recent pairs — enough to discard a single bad print
# without smearing across a genuine intraday drift in carry.
_SAMPLE_COUNT = _getenv_int("FUTURES_BASIS_SAMPLES", 15)

# Beyond this age a measurement is still used but labelled ``measured_stale``
# so surfaces can disclose it.  90 minutes keeps it "fresh" through the
# 16:00-18:00 evening hold; overnight and weekends read as stale, which is
# accurate — nothing has measured the basis since the bell.
_FRESH_MINUTES = _getenv_int("FUTURES_BASIS_FRESH_MINUTES", 90)

_QUARTER_MONTHS = (3, 6, 9, 12)

# Response fields that live in PRICE space and must be carried to the
# futures axis.  Everything absent from this list passes through untouched —
# the allowlist is deliberately explicit, because silently projecting a
# dollar-exposure or a ratio field would corrupt it beyond recognition.
PRICE_FIELDS: frozenset[str] = frozenset(
    {
        # headline dealer levels
        "gamma_flip",
        "gamma_flip_point",
        "gamma_flip_raw",
        "call_wall",
        "put_wall",
        "max_pain",
        "pin_strike",
        "max_gamma_strike",
        "king_node",
        "hvl",
        # per-strike profiles / ladders
        "strike",
        "strikes",
        # price series and session levels
        "open",
        "high",
        "low",
        "close",
        "price",
        "spot",
        "spot_price",
        "underlying_price",
        "previous_close",
        "prior_close",
        "session_open",
        "session_high",
        "session_low",
        "vwap",
        "opening_range_high",
        "opening_range_low",
        # distances are in price units too
        "flip_distance",
    }
)

# Fields that look price-ish but must NEVER be projected.  Kept as an
# explicit denylist so that if one is ever added to PRICE_FIELDS by mistake
# the contradiction is visible in review rather than silent in production.
NEVER_PROJECT: frozenset[str] = frozenset(
    {
        "net_gex",
        "total_net_gex",
        "net_gex_at_spot",
        "call_gex",
        "put_gex",
        "total_call_gex",
        "total_put_gex",
        "call_wall_strength",
        "put_wall_strength",
        "local_gex",
        "convexity_risk",
        "put_call_ratio",
        "pin_score",
        "pin_confidence",
        "implied_volatility",
        "iv",
        "delta",
        "gamma",
        "vanna",
        "charm",
        "volume",
        "open_interest",
    }
)


@dataclass(frozen=True)
class FuturesBasis:
    """The index -> future carry ratio in force for one projection."""

    index_symbol: str  # e.g. "SPX"
    futures_symbol: str  # e.g. "ES"
    ratio: float  # F / S
    source: str  # "measured" | "measured_stale" | "carry"
    observed_at: Optional[datetime]
    sample_count: int
    feed_symbol: Optional[str]  # TradeStation series, e.g. "@ES"

    def project(self, value: float, *, tick: Optional[float] = None) -> float:
        """Carry one cash-index price to the futures axis."""
        return round_to_tick(value * self.ratio, tick)

    def unproject(self, value: float) -> float:
        """Inverse of :meth:`project` — futures price back to cash-index."""
        if self.ratio <= 0:
            return value
        return value / self.ratio

    def offset_at(self, spot: float) -> float:
        """The basis in points at a given cash level (for display copy)."""
        return spot * (self.ratio - 1.0)

    @property
    def is_measured(self) -> bool:
        return self.source.startswith("measured")

    def as_audit(self) -> Dict[str, Any]:
        """Compact dict for response metadata and log lines."""
        return {
            "index_symbol": self.index_symbol,
            "futures_symbol": self.futures_symbol,
            "ratio": round(self.ratio, 8),
            "source": self.source,
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "sample_count": self.sample_count,
            "feed_symbol": self.feed_symbol,
        }


def _third_friday(year: int, month: int) -> date:
    """Third Friday of ``month`` — CME equity-index quarterly expiry."""
    first = date(year, month, 1)
    return first + timedelta(days=(4 - first.weekday()) % 7 + 14)


def next_quarterly_expiry(at: Optional[datetime] = None) -> date:
    """Expiry of the front quarterly contract on/after ``at``."""
    today = (at or datetime.now(timezone.utc)).date()
    for year in (today.year, today.year + 1):
        for month in _QUARTER_MONTHS:
            expiry = _third_friday(year, month)
            if expiry >= today:
                return expiry
    # Unreachable for any real date; keeps the return type honest.
    return _third_friday(today.year + 1, _QUARTER_MONTHS[0])


def theoretical_ratio(index_symbol: str, at: Optional[datetime] = None) -> float:
    """Cost-of-carry ratio ``e^((r - q) T)`` for the front quarterly future.

    The fallback when no concurrent print pair is available.  Uses the
    configured ``RISK_FREE_RATE`` and the per-symbol dividend yield, so it is
    only as good as those assumptions — which is exactly why the measured
    ratio is preferred whenever the tape offers one.
    """
    now = at or datetime.now(timezone.utc)
    days = max((next_quarterly_expiry(now) - now.date()).days, 0)
    years = days / 365.0
    q = resolve_dividend_yield(index_symbol)
    return exp((RISK_FREE_RATE - q) * years)


def _accept(ratio: float) -> bool:
    """True when a candidate ratio is inside the sanity bound."""
    return ratio > 0 and abs(ratio - 1.0) <= _MAX_DEVIATION


def _normalize(symbol: str) -> Optional[tuple[str, str]]:
    """Resolve ``symbol`` to ``(futures_symbol, index_symbol)``, or None.

    Accepts either side of the pair, so callers can pass whichever they hold:
    ``"ES"`` and ``"SPX"`` both resolve to ``("ES", "SPX")``.
    """
    normalized = (symbol or "").strip().upper()
    if not normalized:
        return None
    if is_futures_symbol(normalized):
        index = resolve_futures_index(normalized)
        return (normalized, index) if index else None
    future = resolve_futures_alias(normalized)
    return (future, normalized) if future else None


async def resolve_basis(
    db: Any, symbol: str, *, at: Optional[datetime] = None
) -> Optional[FuturesBasis]:
    """Basis in force for ``symbol`` (either ``ES`` or ``SPX``), or None.

    ``None`` means "not a projectable pair" — the caller should treat the
    symbol as an ordinary underlying.  A pair that IS projectable always
    yields a basis: if the tape offers no usable print pair the theoretical
    carry ratio is returned with ``source="carry"``, so a projection never
    fails outright and never silently falls back to a ratio of 1.0 (which
    would publish cash levels on a futures chart).

    Requires ``db`` to expose ``get_futures_basis_samples`` — the
    ``DatabaseManager`` used by the API does.
    """
    pair = _normalize(symbol)
    if pair is None:
        return None
    futures_symbol, index_symbol = pair

    samples: list[Dict[str, Any]] = []
    try:
        samples = await db.get_futures_basis_samples(
            index_symbol, lookback_minutes=_LOOKBACK_MINUTES, limit=_SAMPLE_COUNT
        )
    except Exception as e:  # a dead read must degrade, not 500 the endpoint
        logger.warning("basis sample read failed for %s: %s", index_symbol, e)

    ratios: list[float] = []
    newest_at: Optional[datetime] = None
    feed_symbol: Optional[str] = None
    for row in samples or []:
        try:
            future_close = float(row.get("future_close"))
            index_close = float(row.get("index_close"))
        except (TypeError, ValueError):
            continue
        if index_close <= 0 or future_close <= 0:
            continue
        ratio = future_close / index_close
        if not _accept(ratio):
            continue
        ratios.append(ratio)
        if newest_at is None:  # rows arrive newest-first
            newest_at = row.get("observed_at")
            feed_symbol = row.get("future_symbol")

    if ratios:
        now = at or datetime.now(timezone.utc)
        age_minutes = None
        if newest_at is not None:
            observed = newest_at
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            age_minutes = (now - observed).total_seconds() / 60.0
        source = (
            "measured"
            if age_minutes is not None and age_minutes <= _FRESH_MINUTES
            else "measured_stale"
        )
        return FuturesBasis(
            index_symbol=index_symbol,
            futures_symbol=futures_symbol,
            ratio=median(ratios),
            source=source,
            observed_at=newest_at,
            sample_count=len(ratios),
            feed_symbol=feed_symbol or resolve_index_future(index_symbol),
        )

    logger.info(
        "no usable basis print pair for %s; falling back to cost-of-carry",
        index_symbol,
    )
    return FuturesBasis(
        index_symbol=index_symbol,
        futures_symbol=futures_symbol,
        ratio=theoretical_ratio(index_symbol, at),
        source="carry",
        observed_at=None,
        sample_count=0,
        feed_symbol=resolve_index_future(index_symbol),
    )


def project_value(key: str, value: Any, basis: FuturesBasis, *, tick: Optional[float]) -> Any:
    """Project one field if it is a price, else return it unchanged."""
    if key in NEVER_PROJECT or key not in PRICE_FIELDS:
        return value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value
    return basis.project(float(value), tick=tick)


def project_payload(payload: Any, basis: FuturesBasis, *, tick: Optional[float] = None) -> Any:
    """Recursively carry every price field in a JSON payload to the futures axis.

    Walks dicts and lists so nested shapes (a summary with an embedded strike
    ladder, a list of bars) are handled without each endpoint enumerating its
    own fields.  Only keys in :data:`PRICE_FIELDS` are touched.
    """
    if isinstance(payload, dict):
        return {
            key: (
                project_payload(value, basis, tick=tick)
                if isinstance(value, (dict, list))
                else project_value(key, value, basis, tick=tick)
            )
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [project_payload(item, basis, tick=tick) for item in payload]
    return payload


def projection_tick(futures_symbol: str) -> Optional[float]:
    """Tick size to round projected levels to, for ``futures_symbol``."""
    return resolve_futures_tick(futures_symbol)


def projection_metadata(basis: FuturesBasis) -> Dict[str, Any]:
    """The ``projection`` block attached to every projected response.

    Present so a consumer can always tell that a level was derived rather
    than observed, and on what ratio — a chart that draws an ES wall should
    be able to say where the number came from.
    """
    return {
        "symbol": basis.futures_symbol,
        "derived_from": basis.index_symbol,
        "basis_ratio": round(basis.ratio, 8),
        "basis_source": basis.source,
        "basis_observed_at": (basis.observed_at.isoformat() if basis.observed_at else None),
        "note": (
            f"Levels are {basis.index_symbol} option-derived, carried to the "
            f"{basis.futures_symbol} price axis. Exposures are not rescaled."
        ),
    }


def project_levels(
    levels: Iterable[Optional[float]], basis: FuturesBasis, *, tick: Optional[float] = None
) -> list[Optional[float]]:
    """Project a bare sequence of levels, preserving ``None`` holes."""
    return [None if lvl is None else basis.project(float(lvl), tick=tick) for lvl in levels]
