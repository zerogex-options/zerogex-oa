"""Gamma Regime Shift endpoints — the read behind the overhauled Gamma Shift page.

Three routes, one per question the page has to answer:

``GET /api/gex/regime-shift``
    How much did dealer gamma move between two points in time, at every
    strike — and what does that mean (stabilizing / fragile, supportive /
    capping)?

``GET /api/gex/expiry-rolloff``
    How much dealer gamma disappears at the next expiration, by strike and in
    total — and is that a lot for this symbol, or a normal Friday?

``GET /api/gex/regime-history``
    What did previous sessions read as?  Renders the history strip and, more
    importantly, supplies the trailing distribution that turns a raw dollar
    shift into a defensible "dramatically".

The maths all live in :mod:`src.analytics.regime_shift`, which is pure and
unit-tested; this module is the I/O shell around it — resolve two timestamps,
fetch two chains, normalize against stored history, shape the payload.

Why the computation is server-side
----------------------------------
The website could difference two ``/api/replay/frame`` payloads in the
browser, and an earlier iteration of Gamma Shift did exactly that.  It has to
move here for three reasons: the z-score denominator is a trailing window of
stored sessions the browser cannot see; the stored per-session read has to be
produced by the same code path that serves the live one or the history strip
slowly drifts away from the card above it; and an expiry-aware diff is a
correctness property (see the module docstring), not a rendering choice, so it
belongs where every consumer inherits it.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from zoneinfo import ZoneInfo

from src.analytics import regime_session as rsess
from src.analytics import regime_shift as rs

from ..database import DatabaseManager
from .trade_signals import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gex", tags=["GEX"])

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

#: Lookback presets.  The intraday ones are simple offsets; the two
#: cross-session ones need a session calendar and are resolved separately.
#:
#: ``prev_same_time`` is the honest day-over-day comparison and the default
#: for cross-session work: dealer gamma has strong time-of-day seasonality
#: (0DTE pinning at 15:55 is structurally nothing like 09:35), so comparing
#: 12:05 today against yesterday's 16:00 close measures the clock as much as
#: the book.
_INTRADAY_OFFSETS: dict[str, timedelta] = {
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
}
_CROSS_SESSION = ("prev_close", "prev_same_time", "week")
LOOKBACKS = ("session", *_INTRADAY_OFFSETS.keys(), *_CROSS_SESSION)

#: Re-exported from the shared session module so the live card and the stored
#: history normalize against the same window by construction.
SIGMA_WINDOW_SESSIONS = rsess.SIGMA_WINDOW_SESSIONS

#: Roll-off percentile -> the word the card prints.  Cut at the same places
#: the adverb ladder cuts, so "heavy" and "dramatically" mean comparable
#: things to a reader moving between the two panels.
_ROLLOFF_VERDICTS: tuple[tuple[float, str], ...] = (
    (0.10, "very light"),
    (0.30, "light"),
    (0.70, "typical"),
    (0.90, "above average"),
    (1.01, "heavy"),
)


# ---------------------------------------------------------------------------
# Shaping helpers
# ---------------------------------------------------------------------------


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and abs(out) != float("inf") else None


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _session_date_of(ts: datetime) -> date:
    return ts.astimezone(ET).date()


def _summary_levels(summary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The level fields the before/after column renders.

    ``get_gex_summary_at_ts`` aliases the storage columns for its replay
    consumers (``gamma_flip_point AS gamma_flip``, ``total_net_gex AS
    net_gex``), so both vocabularies are accepted here — a raw ``gex_summary``
    row and a shaped one both work, and neither silently reads as null.
    """
    if not summary:
        return {}

    def pick(*names: str) -> Optional[float]:
        for name in names:
            if summary.get(name) is not None:
                return _f(summary.get(name))
        return None

    return {
        "gamma_flip": pick("gamma_flip", "gamma_flip_point"),
        "call_wall": pick("call_wall"),
        "put_wall": pick("put_wall"),
        "max_pain": pick("max_pain"),
        "net_gex_at_spot": pick("net_gex_at_spot"),
        "total_net_gex": pick("net_gex", "total_net_gex"),
    }


def _parse_expirations(raw: Optional[str]) -> Optional[List[date]]:
    """``"all"`` / empty -> None (no restriction); else a parsed date list."""
    text = (raw or "all").strip()
    if not text or text.lower() == "all":
        return None
    out: List[date] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(date.fromisoformat(part))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=(
                    "expirations must be 'all' or a comma-separated list of "
                    "YYYY-MM-DD expiration dates"
                ),
            )
    return out or None


def rolloff_verdict(percentile: Optional[float]) -> Optional[str]:
    """Percentile -> the plain word.  None when there is no distribution yet."""
    if percentile is None:
        return None
    for ceiling, word in _ROLLOFF_VERDICTS:
        if percentile < ceiling:
            return word
    return _ROLLOFF_VERDICTS[-1][1]


# ---------------------------------------------------------------------------
# Snapshot resolution
#
# Normalization lives in src.analytics.regime_session (trailing_sigmas), which
# the per-session refresh job also calls — one window, one bootstrap ladder,
# so the history strip and the card above it cannot disagree.
# ---------------------------------------------------------------------------


async def _resolve_from_ts(
    db: DatabaseManager, symbol: str, lookback: str, to_ts: datetime
) -> tuple[Optional[datetime], Optional[str]]:
    """The 'A' timestamp for a lookback preset, plus a human label.

    Returns ``(None, reason)`` when the preset cannot be resolved — most
    often a cross-session preset on a symbol whose history does not reach
    back that far.  The caller turns that into a 409 naming the preset rather
    than silently substituting a different window.
    """
    if lookback in _INTRADAY_OFFSETS:
        return to_ts - _INTRADAY_OFFSETS[lookback], "Earlier today"

    session = _session_date_of(to_ts)
    if lookback == "session":
        return rsess.session_open_utc(session), "Since the open"

    # Cross-session presets need the symbol's own trading calendar: a naive
    # "minus one day" lands on a weekend or a holiday and reads an empty
    # chain, which the diff would then report as a total wipeout.
    want_back = 5 if lookback == "week" else 1
    rows = await db.get_replay_session_dates(symbol, limit=want_back + 6)
    prior = [
        r["session_date"]
        for r in rows
        if isinstance(r.get("session_date"), date) and r["session_date"] < session
    ]
    if len(prior) < want_back:
        return None, f"no session history {want_back} trading day(s) back"
    target = prior[want_back - 1]

    if lookback == "prev_close":
        # 16:00 ET; get_gex_chain_at_ts anchors at-or-before, so this lands on
        # that session's final snapshot even when it stopped early.
        anchor = datetime.combine(target, time(16, 0), tzinfo=ET).astimezone(UTC)
        return anchor, f"{target.isoformat()} close"

    # prev_same_time / week: same wall-clock instant, N sessions back.
    to_et = to_ts.astimezone(ET)
    anchor = datetime.combine(target, to_et.timetz()).astimezone(UTC)
    label = "Same time yesterday" if lookback == "prev_same_time" else "Same time last week"
    return anchor, label


async def _snapshot(
    db: DatabaseManager, symbol: str, at_ts: datetime
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """The full chain plus its ``gex_summary`` headline at one instant."""
    chain = await db.get_gex_chain_at_ts(symbol, at_ts)
    summary = None
    if chain.get("timestamp") is not None:
        summary = await db.get_gex_summary_at_ts(symbol, chain["timestamp"])
    return chain, summary


# ---------------------------------------------------------------------------
# GET /api/gex/regime-shift
# ---------------------------------------------------------------------------


@router.get("/regime-shift")
async def get_regime_shift(
    symbol: str = Query(default="SPY", max_length=10),
    lookback: str = Query(
        default="session",
        description=(
            "session | 30m | 1h | 2h | 4h | prev_close | prev_same_time | week. "
            "prev_same_time is the honest day-over-day comparison; prev_close "
            "mixes in time-of-day seasonality."
        ),
    ),
    expirations: str = Query(
        default="all",
        description=(
            "'all', or a comma-separated YYYY-MM-DD list restricting the diff "
            "to those expirations (intersected with what both snapshots have)."
        ),
    ),
    lens: str = Query(
        default="net",
        description=(
            "'net' scores the total change; 'positioning' scores only the "
            "open-interest-driven component (contracts actually traded)."
        ),
    ),
    strike_limit: int = Query(default=80, ge=10, le=400),
    db: DatabaseManager = Depends(get_db),
):
    """Per-strike dealer-gamma change between two points in time, plus the read.

    The response is the whole card: the two snapshots' levels, the per-strike
    diff, the two-axis scores with their normalization, the concentration
    band, and the expiry scope the diff actually compared.  One request, one
    render — the page never has to reconcile several feeds that moved
    independently between fetches.
    """
    symbol = symbol.upper()
    if lookback not in LOOKBACKS:
        raise HTTPException(
            status_code=422,
            detail=f"lookback must be one of: {', '.join(LOOKBACKS)}",
        )
    if lens not in ("net", "positioning"):
        raise HTTPException(status_code=422, detail="lens must be 'net' or 'positioning'")
    wanted_expirations = _parse_expirations(expirations)

    now = datetime.now(UTC)
    chain_b, summary_b = await _snapshot(db, symbol, now)
    if chain_b["timestamp"] is None:
        raise HTTPException(status_code=404, detail=f"No GEX data for {symbol} yet.")

    from_ts, label = await _resolve_from_ts(db, symbol, lookback, chain_b["timestamp"])
    if from_ts is None:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot resolve lookback '{lookback}' for {symbol}: {label}.",
        )
    chain_a, summary_a = await _snapshot(db, symbol, from_ts)
    if chain_a["timestamp"] is None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"No {symbol} snapshot at or before {from_ts.isoformat()} — "
                f"lookback '{lookback}' reaches past the stored history."
            ),
        )
    if chain_a["timestamp"] == chain_b["timestamp"]:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Lookback '{lookback}' resolves to the same snapshot as now; "
                "there is nothing to compare yet this session."
            ),
        )

    diff = rs.shift_rows(
        chain_a["rows"], chain_b["rows"], restrict_expirations=wanted_expirations
    )
    spot = chain_b["spot"] or _f((summary_b or {}).get("spot_price")) or 0.0
    use_positioning = lens == "positioning"
    scores = rs.weighted_scores(diff.rows, spot, use_positioning=use_positioning)
    band = rs.concentration_band(
        diff.rows, spot=spot, use_positioning=use_positioning
    )

    session = _session_date_of(chain_b["timestamp"])
    # The window this read actually spans, in trading hours.  Every stored
    # session read is a full since-open measurement, so without this a 1h
    # lookback is measured against a 6.5h yardstick (and reads QUIET however
    # violent it was), while "vs last week" is measured against a fifth of
    # its own horizon.
    window_hours = rsess.rth_hours_between(chain_a["timestamp"], chain_b["timestamp"])
    lean_sigma, stab_sigma, normalization, n_sessions = await rsess.trailing_sigmas(
        db, symbol, before=session, scores=scores, hours=window_hours
    )
    read = rs.classify(
        rs.zscore(scores.lean, lean_sigma), rs.zscore(scores.stability, stab_sigma)
    )

    rolloff = rs.rolloff_tranches(chain_b["rows"], as_of=session)

    # The ladder renders a window around spot, not the whole chain: send the
    # largest movers so a truncated payload never hides the story, and let the
    # client sort them back into strike order.
    ordered = sorted(
        diff.rows,
        key=lambda r: abs(r.positioning if use_positioning else r.d_net),
        reverse=True,
    )[:strike_limit]

    return {
        "symbol": symbol,
        "lookback": lookback,
        "lookback_label": label,
        "lens": lens,
        "spot": spot,
        "session_date": session.isoformat(),
        "from": {
            "timestamp": _iso(chain_a["timestamp"]),
            "spot": chain_a["spot"],
            **_summary_levels(summary_a),
        },
        "to": {
            "timestamp": _iso(chain_b["timestamp"]),
            "spot": chain_b["spot"],
            **_summary_levels(summary_b),
        },
        "read": {
            "state": read.state,
            "adverb": read.adverb,
            "lean_z": read.lean_z,
            "stability_z": read.stability_z,
            "magnitude": read.magnitude,
            "meaning": read.meaning,
            "normalization": normalization,
            "sessions_in_window": n_sessions,
        },
        "scores": {
            "lean": scores.lean,
            "stability": scores.stability,
            "net_shift": scores.net_shift,
            "gross_shift": scores.gross_shift,
            "sigma_price": scores.sigma_price,
            "near_spot_stock": scores.near_spot_stock,
            "window_hours": window_hours,
        },
        # Whether the 'positioning' lens can answer anything in THIS window.
        #
        # Open interest is a once-a-day settlement figure — the feed
        # republishes the same number all session — so an intraday A->B pair
        # has an identical OI at every strike and the OI-driven component is
        # zero by construction.  Rendering that as a flat line at every
        # strike says "dealers did nothing", which is a claim the data cannot
        # support; the surface needs to say "this window cannot see that"
        # instead, and point at the cross-session lookbacks that can.
        "positioning": {
            "resolved": diff.oi_moved_strikes > 0,
            "oi_moved_strikes": diff.oi_moved_strikes,
            "strike_count": len(diff.rows),
        },
        "band": {
            "low": band.low,
            "high": band.high,
            "share": band.share,
            "resolved": band.resolved,
        },
        "strikes": [
            {
                "strike": r.strike,
                "net_a": r.net_a,
                "net_b": r.net_b,
                "d_net": r.d_net,
                "positioning": r.positioning,
                "repricing": r.repricing,
                "call_oi_delta": r.call_oi_b - r.call_oi_a,
                "put_oi_delta": r.put_oi_b - r.put_oi_a,
            }
            for r in sorted(ordered, key=lambda r: r.strike)
        ],
        "strikes_truncated": len(diff.rows) > len(ordered),
        # What the diff compared, and what it deliberately left out.  The
        # frontend surfaces expired_* verbatim: a reader who is told
        # "$1.4B expired overnight and is excluded" trusts the rest of the
        # card far more than one who is told nothing.
        "expiry_scope": {
            "common": [e.isoformat() for e in diff.common_expirations],
            "common_count": len(diff.common_expirations),
            "expired": [e.isoformat() for e in diff.expired_expirations],
            "expired_net_gex": diff.expired_net_gex,
            "expired_abs_gex": diff.expired_abs_gex,
            "added": [e.isoformat() for e in diff.added_expirations],
            "added_net_gex": diff.added_net_gex,
        },
        "rolloff": _rolloff_summary(rolloff),
    }


def _rolloff_summary(rolloff: rs.Rolloff) -> Optional[Dict[str, Any]]:
    """The compact roll-off block carried on the regime-shift card."""
    if rolloff.next_tranche is None:
        return None
    t = rolloff.next_tranche
    return {
        "expiration": t.expiration.isoformat(),
        "dte": t.dte,
        "net_gex": t.net_gex,
        "abs_gex": t.abs_gex,
        "share": t.share,
        "chain_abs_gex": rolloff.total_abs_gex,
    }


# ---------------------------------------------------------------------------
# GET /api/gex/expiry-rolloff
# ---------------------------------------------------------------------------


@router.get("/expiry-rolloff")
async def get_expiry_rolloff(
    symbol: str = Query(default="SPY", max_length=10),
    max_tranches: int = Query(default=8, ge=2, le=20),
    strike_limit: int = Query(default=24, ge=5, le=120),
    db: DatabaseManager = Depends(get_db),
):
    """Dealer gamma grouped by expiration — what leaves, and when.

    The nearest tranche is the single largest *scheduled* change to the
    surface and the one thing a same-day snapshot cannot show: at its close
    that gamma stops existing whether or not anybody trades.  A trader who
    knows 38% of the book expires tonight reads tomorrow's map differently.

    ``share`` is computed on ABSOLUTE gamma so an internally offsetting
    tranche (a big call wall against a big put wall) still reads as large —
    all of it is leaving, and the offset does not survive the expiry.

    "Is that a lot?" is answered by ``context``: the percentile of today's
    share against this symbol's stored sessions, plus the plain word for it.
    Until enough sessions are stored, ``percentile`` is null and the raw
    share stands on its own — which is still interpretable, just not ranked.
    """
    symbol = symbol.upper()
    now = datetime.now(UTC)
    chain, summary = await _snapshot(db, symbol, now)
    if chain["timestamp"] is None:
        raise HTTPException(status_code=404, detail=f"No GEX data for {symbol} yet.")

    session = _session_date_of(chain["timestamp"])
    rolloff = rs.rolloff_tranches(
        chain["rows"],
        as_of=session,
        max_tranches=max_tranches,
        next_strike_limit=strike_limit,
    )
    if rolloff.next_tranche is None:
        raise HTTPException(
            status_code=404,
            detail=f"No dated option chain for {symbol} in the latest snapshot.",
        )

    history = await db.get_regime_sessions(
        symbol, limit=SIGMA_WINDOW_SESSIONS, before_date=session
    )
    population = [
        v for v in (_f(h.get("rolloff_share")) for h in history) if v is not None
    ]
    percentile = (
        rs.percentile_of(rolloff.next_tranche.share, population)
        if len(population) >= rs.MIN_SESSIONS_FOR_SIGMA
        else None
    )

    return {
        "symbol": symbol,
        "as_of": _iso(chain["timestamp"]),
        "session_date": session.isoformat(),
        "spot": chain["spot"] or _f((summary or {}).get("spot_price")),
        "total_abs_gex": rolloff.total_abs_gex,
        "total_net_gex": rolloff.total_net_gex,
        "next": {
            "expiration": rolloff.next_tranche.expiration.isoformat(),
            "dte": rolloff.next_tranche.dte,
            "net_gex": rolloff.next_tranche.net_gex,
            "abs_gex": rolloff.next_tranche.abs_gex,
            "share": rolloff.next_tranche.share,
        },
        "context": {
            "percentile": percentile,
            "verdict": rolloff_verdict(percentile),
            "sessions_in_window": len(population),
        },
        "tranches": [
            {
                "expiration": t.expiration.isoformat(),
                "dte": t.dte,
                "net_gex": t.net_gex,
                "abs_gex": t.abs_gex,
                "share": t.share,
            }
            for t in rolloff.tranches
        ],
        # Folded tail marker: the last ladder row aggregates everything beyond
        # ``max_tranches``, so the client labels it "and later" rather than
        # naming a single date it does not solely represent.
        "tranches_folded": len(rolloff.tranches) > max_tranches,
        "next_by_strike": [
            {"strike": strike, "net_gex": net} for strike, net in rolloff.next_by_strike
        ],
    }


# ---------------------------------------------------------------------------
# GET /api/gex/regime-history
# ---------------------------------------------------------------------------


@router.get("/regime-history")
async def get_regime_history(
    symbol: str = Query(default="SPY", max_length=10),
    limit: int = Query(default=30, ge=1, le=180),
    db: DatabaseManager = Depends(get_db),
):
    """Stored per-session regime reads, most recent first.

    The z-scores are RECOMPUTED here from each row's stored raw scores against
    the *current* full window, rather than served as written.  A row written
    on day 12 was normalized against 11 sessions; one written on day 90 against
    60.  Serving those side by side would put a strip on screen whose bars are
    not comparable to each other — which is the one thing a history strip
    exists to make possible.
    """
    symbol = symbol.upper()
    sessions = await db.get_regime_sessions(symbol, limit=max(limit, SIGMA_WINDOW_SESSIONS))
    if not sessions:
        return {
            "symbol": symbol,
            "sessions": [],
            "normalization": "none",
            "sessions_in_window": 0,
        }

    leans = [v for v in (_f(s.get("lean_raw")) for s in sessions) if v is not None]
    stabs = [v for v in (_f(s.get("stability_raw")) for s in sessions) if v is not None]
    # The SAME estimator trailing_sigmas uses, not merely a similar one: the
    # strip sits directly under the card, and a denominator that differed
    # even slightly would put a bar on screen disagreeing with the headline
    # above it every day.
    lean_sigma = rs.robust_scale(leans)
    stab_sigma = rs.robust_scale(stabs)
    normalization = (
        "trailing" if len(leans) >= rs.MIN_SESSIONS_FOR_SIGMA and lean_sigma else "none"
    )

    out: List[Dict[str, Any]] = []
    for s in sessions[:limit]:
        lean_raw = _f(s.get("lean_raw")) or 0.0
        stab_raw = _f(s.get("stability_raw")) or 0.0
        if normalization == "trailing":
            lean_z = rs.zscore(lean_raw, lean_sigma)
            stab_z = rs.zscore(stab_raw, stab_sigma or lean_sigma)
            read = rs.classify(lean_z, stab_z)
            state, adverb, magnitude = read.state, read.adverb, read.magnitude
        else:
            # No usable window: fall back to what the writer stored, and say so
            # via ``normalization`` rather than inventing a comparable number.
            lean_z = _f(s.get("lean_z")) or 0.0
            stab_z = _f(s.get("stability_z")) or 0.0
            state = s.get("state") or "QUIET"
            magnitude = _f(s.get("magnitude")) or 0.0
            adverb = rs.adverb_for(magnitude)
        out.append(
            {
                "session_date": _iso(s.get("session_date")),
                "state": state,
                "adverb": adverb,
                "lean_z": lean_z,
                "stability_z": stab_z,
                "magnitude": magnitude,
                "lean_raw": lean_raw,
                "stability_raw": stab_raw,
                "net_shift": _f(s.get("net_shift")),
                "spot_open": _f(s.get("spot_open")),
                "spot_close": _f(s.get("spot_close")),
                "band_low": _f(s.get("band_low")),
                "band_high": _f(s.get("band_high")),
                "band_resolved": bool(s.get("band_resolved")),
                "rolloff_share": _f(s.get("rolloff_share")),
                "rolloff_expiration": _iso(s.get("rolloff_expiration")),
                "gamma_flip_open": _f(s.get("gamma_flip_open")),
                "gamma_flip_close": _f(s.get("gamma_flip_close")),
                "expired_expiration_count": s.get("expired_expiration_count"),
            }
        )

    return {
        "symbol": symbol,
        "sessions": out,
        "normalization": normalization,
        "sessions_in_window": len(leans),
    }
