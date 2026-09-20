"""Consolidated dealer-positioning levels — the versioned public contract.

``GET /api/v1/levels/{symbol}``

A single, stable snapshot of the four headline dealer-positioning levels
(gamma flip, call wall, put wall, max pain) plus the per-strike gamma
profile — the exact primitives a third-party charting integration overlays
on a price chart (the TradingView Charting Library widget, a NinjaScript
indicator, an embeddable partner widget).

Why this exists alongside the internal ``/api/gex/*`` surface:

* **Versioned** (``/api/v1/``).  The field names here are a committed
  contract external consumers code against; the internal ``/api/gex/*``
  models can churn without breaking integrations.
* **Consolidated.**  One round-trip returns what previously required
  stitching ``/api/gex/summary`` (flip + walls + max pain) and
  ``/api/gex/by-strike`` (the profile) — and it aggregates the profile
  across expirations so it shares the walls' cross-expiration basis.
* **Derived-only, ``gex``-scoped.**  No raw per-contract quotes, so it
  belongs to the broadly-redistributable ``analytics`` tier (see
  ``src/api/scopes.py``); ``market_raw`` is never touched here.
* **Freshness-aware.**  ``as_of`` + ``age_seconds`` let a consumer — or a
  future delayed-vs-live tier gate — reason about staleness explicitly.
  The metrics refresh on the ~60s analytics cycle (see the caches on
  ``DatabaseManager``); this is snapshot-poll, not a stream.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel

from src.analytics.main_engine import (
    FLIP_REASON_BEYOND_MAX_DISTANCE,
    FLIP_REASON_NO_PROFILE,
    FLIP_REASON_ONE_SIDED,
)
from src.config import GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT

from ..database import DatabaseManager
from ..errors import handle_api_errors

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/levels", tags=["Levels (v1)"])


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


def get_db() -> DatabaseManager:
    from ..main import db_manager

    assert db_manager is not None, "db_manager not initialized"
    return db_manager


# ---------------------------------------------------------------------------
# Response models — the committed v1 contract
# ---------------------------------------------------------------------------


class StrikeGamma(BaseModel):
    """One bar of the per-strike gamma profile (dollar GEX per 1% move).

    Aggregated across expirations; ``net_gex == call_gex + put_gex`` by
    construction. Call exposure is signed positive and put exposure
    negative under the traditional modeled dealer-positioning convention
    (dealers modeled net long calls / net short puts). This is a modeled
    sign convention, not a direct observation of dealer inventory.
    """

    strike: float
    net_gex: float
    call_gex: float
    put_gex: float


class DealerLevels(BaseModel):
    """The four headline horizontal levels.

    Any field may be ``null`` when the analytics engine could not resolve
    it for the latest snapshot (e.g. an unresolved gamma flip on a thin
    chain) — consumers should hide, not zero, a null level.
    """

    gamma_flip: Optional[float] = None
    #: Why ``gamma_flip`` is absent, when it is: NULL whenever a flip was
    #: published, otherwise one of NO_PROFILE / ONE_SIDED / EDGE_ONLY /
    #: BEYOND_MAX_DISTANCE / BELOW_STRUCTURAL_FLOOR. A client that draws
    #: nothing for a null flip can now say WHY it is drawing nothing --
    #: three of those codes mean the chain was read correctly and the
    #: level is simply not where a chart can show it.
    gamma_flip_reason: Optional[str] = None
    #: A short, ready-to-draw label for a client that has no room to
    #: interpret ``gamma_flip_reason`` itself, e.g. ``Flip >8%\u2193``.
    #: NULL whenever a flip was published.
    #:
    #: Derived HERE rather than in the client on purpose. The
    #: NinjaTrader indicator is the hardest artifact in this system to
    #: update -- it is compiled by hand on a tester's own machine from a
    #: file sent by email -- so anything that can change (the 8%, which
    #: is GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT, or the wording) has to
    #: ship from the server or it ships never. The client draws the
    #: string and makes no decisions about it.
    gamma_flip_label: Optional[str] = None
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    max_pain: Optional[float] = None
    # Pin Strike — reachable 0DTE strike with the strongest modeled positive
    # (restoring) dealer gamma into expiration.  A fifth drawable horizontal
    # level, distinct from the walls/flip/max-pain above (see
    # src/analytics/pin_strike.py).  ``null`` when no meaningful pin exists —
    # hide, don't zero; the reason and score/confidence metadata for that case
    # live at the response top level.
    pin_strike: Optional[float] = None


class LevelsResponse(BaseModel):
    symbol: str
    spot: Optional[float] = None
    as_of: datetime
    age_seconds: Optional[int] = None
    # When the analytics engine last wrote this snapshot (server clock), or
    # null on rows that predate the column. ``as_of`` is the chain bucket the
    # numbers were computed FROM; this is when they were PRODUCED. They differ
    # by the cycle's phase within the minute plus its own duration, and a
    # sub-minute cadence rewrites the same minute row, so this is also the
    # one field that changes on a rewrite. Freshness stays measured from
    # ``as_of`` (see src/api/freshness.py); this is additive.
    computed_at: Optional[datetime] = None
    # What the numbers are actually as of: the newest quote write the engine
    # read for this snapshot. ``as_of`` is the minute bucket the snapshot is
    # filed under, and a bucket is already up to a minute old when the cycle
    # reads it, so measuring staleness from it overstated every snapshot's age
    # by the cycle's phase in the minute (26-59s measured) while the quotes
    # inside were under 5s old. ``age_seconds`` is measured from this when
    # present, and from ``as_of`` on rows that predate the column.
    data_as_of: Optional[datetime] = None
    net_gex_at_spot: Optional[float] = None
    levels: DealerLevels
    # Pin Strike metadata (scalars, not drawable lines): the raw maximum pin
    # score, its dominance over all viable pins (0..1), and — when there is no
    # active pin — a REASON_* code (``pin_strike`` in ``levels`` is then null).
    pin_score: Optional[float] = None
    pin_confidence: Optional[float] = None
    pin_strike_reason: Optional[str] = None
    profile: List[StrikeGamma]


def _flip_label(
    reason: Optional[str],
    raw: Optional[float],
    spot: Optional[float],
) -> Optional[str]:
    """One short line for a client whose only alternative is an em dash.

    ``reason`` is the stable enum; this is the disposable presentation of it.
    Keeping them separate means the codes stay aggregatable in SQL while the
    wording can be changed without a migration or a client rebuild.

    Direction for the far-flip case comes from ``gamma_flip_raw``, the
    un-gated nearest crossing on the same cycle: it sits on the same side of
    spot as the crossing the distance gate rejected. When it is absent the
    label drops the arrow rather than guessing a direction.
    """
    if not reason:
        return None
    if reason == FLIP_REASON_BEYOND_MAX_DISTANCE:
        pct = f"{GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT * 100:g}"
        if raw is not None and spot is not None and spot > 0:
            return f"Flip >{pct}%" + ("\u2193" if raw < spot else "\u2191")
        return f"Flip >{pct}% away"
    if reason == FLIP_REASON_ONE_SIDED:
        return "Flip out of range"
    if reason == FLIP_REASON_NO_PROFILE:
        return "Flip no data"
    # EDGE_ONLY / BELOW_STRUCTURAL_FLOOR: a crossing exists but the engine will
    # not stand behind it. "Unresolved" is the honest word and deliberately
    # does not imply a fault, because there is not one.
    return "Flip unresolved"


def _maybe_float(value: object) -> Optional[float]:
    """Coerce a DB ``Decimal``/number to ``float``; pass ``None`` through."""
    return float(value) if value is not None else None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get("/{symbol}", response_model=LevelsResponse)
@handle_api_errors("GET /api/v1/levels/{symbol}")
async def get_levels(
    symbol: str = Path(
        ...,
        min_length=1,
        max_length=16,
        pattern=r"^[A-Za-z0-9.^-]+$",
        description="Underlying symbol, e.g. SPY, SPX, QQQ.",
    ),
    strikes: int = Query(
        default=40,
        ge=1,
        le=200,
        description=(
            "Number of strikes nearest to spot to include in the gamma "
            "profile (aggregated across expirations)."
        ),
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Consolidated dealer-positioning levels for one underlying.

    Returns the latest gamma flip, call/put walls and max pain, plus the
    per-strike gamma profile nearest to spot — the derived, redistributable
    primitives a charting integration draws as horizontal lines and a
    histogram.  ``404`` when no snapshot exists for ``symbol`` yet.
    """
    sym = symbol.upper()

    summary = await db.get_latest_gex_summary(sym)
    if not summary:
        raise HTTPException(
            status_code=404,
            detail=f"No dealer-positioning levels available for {sym}",
        )

    strike_rows = await db.get_latest_strike_gamma_profile(sym, strikes)
    # DB returns nearest-to-spot first; render ascending by strike so the
    # histogram reads left-to-right in price order.
    profile = sorted(
        (
            StrikeGamma(
                strike=float(row["strike"]),
                net_gex=float(row["net_gex"]),
                call_gex=float(row["call_gex"]),
                put_gex=float(row["put_gex"]),
            )
            for row in strike_rows
        ),
        key=lambda bar: bar.strike,
    )

    as_of = summary["timestamp"]
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    data_as_of = summary.get("data_as_of")
    if data_as_of is not None and data_as_of.tzinfo is None:
        data_as_of = data_as_of.replace(tzinfo=timezone.utc)
    # Staleness is measured from the quotes, not from the bucket they are
    # filed under (see LevelsResponse.data_as_of).
    freshness_anchor = data_as_of if data_as_of is not None else as_of
    age_seconds = max(0, int((datetime.now(timezone.utc) - freshness_anchor).total_seconds()))

    return LevelsResponse(
        symbol=sym,
        spot=_maybe_float(summary.get("spot_price")),
        as_of=as_of,
        age_seconds=age_seconds,
        computed_at=summary.get("computed_at"),
        data_as_of=data_as_of,
        net_gex_at_spot=_maybe_float(summary.get("net_gex_at_spot")),
        levels=DealerLevels(
            gamma_flip=_maybe_float(summary.get("gamma_flip")),
            gamma_flip_reason=summary.get("gamma_flip_reason"),
            gamma_flip_label=_flip_label(
                summary.get("gamma_flip_reason"),
                _maybe_float(summary.get("gamma_flip_raw")),
                _maybe_float(summary.get("spot_price")),
            ),
            call_wall=_maybe_float(summary.get("call_wall")),
            put_wall=_maybe_float(summary.get("put_wall")),
            max_pain=_maybe_float(summary.get("max_pain")),
            pin_strike=_maybe_float(summary.get("pin_strike")),
        ),
        pin_score=_maybe_float(summary.get("pin_score")),
        pin_confidence=_maybe_float(summary.get("pin_confidence")),
        pin_strike_reason=summary.get("pin_strike_reason"),
        profile=profile,
    )
