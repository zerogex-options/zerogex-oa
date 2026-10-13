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
* **Expiration-scopable.**  ``?expiration=0dte`` or ``?expiration=YYYY-MM-DD``
  scopes the walls, flip, max pain and profile to one expiration, by the same
  rules the website's Expiry selector uses (see :func:`_scoped_levels`).  The
  default, ``all``, is the whole-chain payload this contract always served.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List, Optional, Union
from zoneinfo import ZoneInfo

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

_ET = ZoneInfo("America/New_York")

# When a same-day expiration stops being the one a trader means by "0DTE".
# The cash options settle at the 16:00 ET close; after it, ES and NQ keep
# trading into the next session, and a rolling 0DTE pick that still drew the
# settled expiry would be showing a book that no longer exists.
_ZERO_DTE_ROLL_ET = time(16, 0)

# The expiration universe the website's Expiry selector offers
# (useChartExpirations asks /api/gex/expirations for the same window), so a
# date a member can pick there resolves here too, and the two share a cache
# entry.
_EXPIRATIONS_LOOKBACK_HOURS = 168

# The scoped read is the website Gamma Ladder's own: a three-bucket 5-minute
# tip of the strike-profile timeseries, falling back to one session of buckets
# when the tip carries no positioning (an ETF after its options close, where
# the extended-hours cycles write over an empty book). Same keys as the
# website, so a scope a member already has open costs nothing extra here.
_SCOPED_TIMEFRAME = "5min"
_SCOPED_TIP_UNITS = 3
_SCOPED_REACH_BACK_UNITS = 78

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
    #: Whole chain only: ``null`` under an expiration scope, where the engine's
    #: spot-shift curve (which this is read from) cannot be rebuilt.
    net_gex_at_spot: Optional[float] = None
    #: The expiration the levels and profile are scoped to (``YYYY-MM-DD``), or
    #: ``null`` for the whole chain.  A ``0dte`` request reports the date it
    #: resolved to, so a client can show which expiry it is drawing.
    expiration: Optional[date] = None
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
# Expiration scope
# ---------------------------------------------------------------------------


def _parse_expiration(raw: Optional[str]) -> Union[str, date]:
    """``"all"``, ``"0dte"``, or a concrete date; 400 on anything else.

    Case-insensitive, because the NinjaTrader setting this was added for is
    free text that a trader types by hand.
    """
    text = (raw or "").strip().lower()
    if text in ("", "all"):
        return "all"
    if text == "0dte":
        return "0dte"
    if _ISO_DATE.match(text):
        try:
            return date.fromisoformat(text)
        except ValueError:
            pass
    raise HTTPException(
        status_code=400,
        detail="expiration must be 'all', '0dte', or a date as YYYY-MM-DD",
    )


def _resolve_zero_dte(listed: List[date], now: datetime) -> Optional[date]:
    """The expiration a rolling ``0dte`` pick means at ``now``.

    Today's expiry until the 16:00 ET settlement, then the next one listed --
    so a futures chart in the evening session, or on a weekend, draws the book
    that settles next rather than the one that already has.  ``None`` when
    nothing upcoming is listed.
    """
    now_et = now.astimezone(_ET)
    today = now_et.date()
    settled_today = now_et.time() >= _ZERO_DTE_ROLL_ET
    for exp in sorted(set(listed)):
        if exp > today or (exp == today and not settled_today):
            return exp
    return None


def _latest_live_bucket(buckets: Optional[List[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    """Newest strike-profile bucket that carries any positioning at all.

    The website ladder's own rule (``latestLiveBucket`` in useGammaLadder): an
    empty or all-zero tip must not blank the levels.
    """
    for bucket in reversed(buckets or []):
        if any(_maybe_float(s.get("net_gamma")) for s in bucket.get("strikes") or []):
            return bucket
    return None


def _scoped_profile(
    strikes: List[Dict[str, Any]], anchor: Optional[float], limit: int
) -> List[StrikeGamma]:
    """The ``limit`` strikes nearest ``anchor``, ascending by strike.

    The timeseries names its dollar-GEX columns ``*_gamma`` (see
    ``StrikeProfileStrike``); the values are the same ``γ × OI × 100 × S² ×
    0.01`` quantities, puts negative, that the whole-chain profile serves.
    """
    bars = [
        StrikeGamma(
            strike=float(s["strike"]),
            net_gex=_maybe_float(s.get("net_gamma")) or 0.0,
            call_gex=_maybe_float(s.get("call_gamma")) or 0.0,
            put_gex=_maybe_float(s.get("put_gamma")) or 0.0,
        )
        for s in strikes
        if s.get("strike") is not None
    ]
    if anchor is not None:
        bars.sort(key=lambda bar: abs(bar.strike - anchor))
    return sorted(bars[:limit], key=lambda bar: bar.strike)


async def _scoped_levels(
    db: DatabaseManager,
    sym: str,
    scope: Union[str, date],
    strikes: int,
    spot: Optional[float],
    now: datetime,
) -> Dict[str, Any]:
    """Walls, flip, max pain and profile for ONE expiration.

    Nothing here is a new calculation.  The walls and flip come from the same
    expiration-filtered strike-profile read the website's Expiry selector
    draws from (``/api/gex/strike-profile-timeseries``), so a 0DTE chart in
    NinjaTrader and a 0DTE view on the site agree to the strike.  Under a
    filter that read re-ranks the walls from the filtered gamma on the
    engine's own re-pick minutes, and recomputes the flip as the cumulative
    net-GEX crossing, because the canonical spot-shift flip cannot be rebuilt
    for a subset of the chain.  Max pain is that expiration's own settlement
    from the engine's per-expiration breakdown.

    The Pin Strike is deliberately NOT scoped: it is 0DTE by construction and
    stays put when the website's Expiry selector moves, so it does here too.

    Raises 404 with a sentence a trader can act on when the expiration is not
    one the chain lists, has already expired, or carries no positioning.
    """
    listed = await db.get_gex_expirations(sym, _EXPIRATIONS_LOOKBACK_HOURS)
    if scope == "0dte":
        exp = _resolve_zero_dte(listed, now)
        if exp is None:
            raise HTTPException(
                status_code=404,
                detail=f"No upcoming {sym} expiration is listed",
            )
    else:
        exp = scope  # type: ignore[assignment]
        if exp < now.astimezone(_ET).date():
            raise HTTPException(
                status_code=404,
                detail=f"{sym} {exp.isoformat()} has expired; pick a later date or 0DTE",
            )
        if exp not in set(listed):
            raise HTTPException(
                status_code=404,
                detail=f"{sym} has no {exp.isoformat()} expiration listed",
            )

    bucket = _latest_live_bucket(
        await db.get_strike_profile_timeseries(sym, _SCOPED_TIMEFRAME, _SCOPED_TIP_UNITS, [exp])
    )
    if bucket is None:
        bucket = _latest_live_bucket(
            await db.get_strike_profile_timeseries(
                sym, _SCOPED_TIMEFRAME, _SCOPED_REACH_BACK_UNITS, [exp]
            )
        )
    if bucket is None:
        raise HTTPException(
            status_code=404,
            detail=f"No {sym} {exp.isoformat()} positioning in the latest snapshot",
        )

    flip = _maybe_float(bucket.get("gamma_flip"))
    anchor = spot if spot is not None else _maybe_float(bucket.get("close"))
    return {
        "expiration": exp,
        "gamma_flip": flip,
        # The filtered flip has no FLIP_REASON_* code to give, so the reason
        # stays null and the label is the deliberately fault-free "unresolved"
        # that the whole-chain path uses for a crossing it will not stand
        # behind.
        "gamma_flip_label": None if flip is not None else "Flip unresolved",
        "call_wall": _maybe_float(bucket.get("call_wall")),
        "put_wall": _maybe_float(bucket.get("put_wall")),
        "max_pain": await db.get_latest_max_pain_for_expiration(sym, exp),
        "profile": _scoped_profile(bucket.get("strikes") or [], anchor, strikes),
    }


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
            "profile (aggregated across expirations, or within the one "
            "``expiration`` selects)."
        ),
    ),
    expiration: str = Query(
        default="all",
        max_length=10,
        description=(
            "'all' (default) for the whole chain; '0dte' for the nearest "
            "expiration that has not yet settled (today's until 16:00 ET, then "
            "the next one listed); or one expiration as YYYY-MM-DD. A scoped "
            "request recomputes the walls, flip, max pain and profile from that "
            "expiration alone; the Pin Strike is 0DTE by construction and is "
            "not scoped."
        ),
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Consolidated dealer-positioning levels for one underlying.

    Returns the latest gamma flip, call/put walls and max pain, plus the
    per-strike gamma profile nearest to spot — the derived, redistributable
    primitives a charting integration draws as horizontal lines and a
    histogram.  ``404`` when no snapshot exists for ``symbol`` yet, or when
    ``expiration`` names one the chain does not list (or that has expired).
    """
    sym = symbol.upper()
    scope = _parse_expiration(expiration)

    summary = await db.get_latest_gex_summary(sym)
    if not summary:
        raise HTTPException(
            status_code=404,
            detail=f"No dealer-positioning levels available for {sym}",
        )

    spot = _maybe_float(summary.get("spot_price"))
    if scope == "all":
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
        levels = DealerLevels(
            gamma_flip=_maybe_float(summary.get("gamma_flip")),
            gamma_flip_reason=summary.get("gamma_flip_reason"),
            gamma_flip_label=_flip_label(
                summary.get("gamma_flip_reason"),
                _maybe_float(summary.get("gamma_flip_raw")),
                spot,
            ),
            call_wall=_maybe_float(summary.get("call_wall")),
            put_wall=_maybe_float(summary.get("put_wall")),
            max_pain=_maybe_float(summary.get("max_pain")),
            pin_strike=_maybe_float(summary.get("pin_strike")),
        )
        scoped_expiration: Optional[date] = None
        net_gex_at_spot = _maybe_float(summary.get("net_gex_at_spot"))
    else:
        scoped = await _scoped_levels(db, sym, scope, strikes, spot, datetime.now(timezone.utc))
        profile = scoped["profile"]
        levels = DealerLevels(
            gamma_flip=scoped["gamma_flip"],
            gamma_flip_label=scoped["gamma_flip_label"],
            call_wall=scoped["call_wall"],
            put_wall=scoped["put_wall"],
            max_pain=scoped["max_pain"],
            pin_strike=_maybe_float(summary.get("pin_strike")),
        )
        scoped_expiration = scoped["expiration"]
        net_gex_at_spot = None

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
        spot=spot,
        as_of=as_of,
        age_seconds=age_seconds,
        computed_at=summary.get("computed_at"),
        data_as_of=data_as_of,
        net_gex_at_spot=net_gex_at_spot,
        expiration=scoped_expiration,
        levels=levels,
        pin_score=_maybe_float(summary.get("pin_score")),
        pin_confidence=_maybe_float(summary.get("pin_confidence")),
        pin_strike_reason=summary.get("pin_strike_reason"),
        profile=profile,
    )
