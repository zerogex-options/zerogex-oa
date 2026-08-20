"""GEX Replay endpoints — Phase 4.

YouTube-style scrubbing over historical dealer-gamma snapshots. The data
already exists in ``gex_summary`` + ``gex_by_strike`` per minute; this
router just shapes per-minute frames into a format the website's scrubber
can swap in <100 ms and the snapshot OG image can render branded
"highlight from this exact moment" cards from.

MP4 export of arbitrary 15-second windows is intentionally a v2 feature
— the renderer needs a Playwright + ffmpeg worker that we don't deploy
in v1. The POST /api/replay/clip endpoint exists in the surface so
clients can detect it, but returns 503 with a stable message until the
worker ships.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from zoneinfo import ZoneInfo

from ..database import DatabaseManager, ReplayFramesUnavailable
from .trade_signals import get_db

router = APIRouter(prefix="/api/replay", tags=["Replay"])

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid date '{raw}'; expected YYYY-MM-DD."
        ) from exc


def _parse_ts(raw: str) -> datetime:
    """Parse an ISO timestamp. Naïve timestamps are interpreted as UTC so
    the scrubber's URL params behave predictably across clients."""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid timestamp '{raw}'; expected ISO-8601."
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _et_session_window(session_date: date) -> tuple[datetime, datetime]:
    """09:30 ET → 16:00 ET window for one trading day, returned in UTC."""
    start_et = datetime.combine(session_date, time(9, 30), tzinfo=ET)
    end_et = datetime.combine(session_date, time(16, 0), tzinfo=ET)
    return start_et.astimezone(UTC), end_et.astimezone(UTC)


def _f(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _iso_date(value: Any) -> str | None:
    """DATE/`datetime` column → "YYYY-MM-DD", or None when absent."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value)
    return text[:10] if len(text) >= 10 else text


def _shape_summary(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """gex_summary row → frame headline dict."""
    if row is None:
        return None
    return {
        "timestamp": row["timestamp"].isoformat() if row.get("timestamp") else None,
        "spot": _f(row.get("spot_price")),
        "call_wall": _f(row.get("call_wall")),
        "put_wall": _f(row.get("put_wall")),
        "gamma_flip": _f(row.get("gamma_flip")),
        "max_pain": _f(row.get("max_pain")),
        # Pin Strike — reachable 0DTE positive-gamma pin (distinct from the
        # walls/flip/max-pain). pin_strike is the drawable level; pin_score /
        # pin_confidence classify strength; pin_strike_reason carries a REASON_*
        # code when there's no active pin (and is null when a pin is present).
        # All null on rows written before pin_strike shipped.
        "pin_strike": _f(row.get("pin_strike")),
        "pin_score": _f(row.get("pin_score")),
        "pin_confidence": _f(row.get("pin_confidence")),
        "pin_strike_reason": row.get("pin_strike_reason"),
        "net_gex": _f(row.get("net_gex")),
        "net_gex_at_spot": _f(row.get("net_gex_at_spot")),
        "put_call_ratio": _f(row.get("put_call_ratio")),
    }


def _shape_strikes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "strike": _f(r.get("strike")),
            # ``gex_by_strike`` is keyed per (strike, expiration), so a strike
            # appears once per expiration and the query already returns the
            # column — it was simply dropped here. Carrying it lets the snapshot
            # chart colour-grade each strike's bar by time-to-expiry (nearest
            # expiration boldest) instead of only summing the rows together.
            "expiration": _iso_date(r.get("expiration")),
            "call_gex": _f(r.get("call_gex")),
            "put_gex": _f(r.get("put_gex")),
            "net_gex": _f(r.get("net_gex")),
            "distance_from_spot": _f(r.get("distance_from_spot")),
        }
        for r in rows
    ]


def _shape_range_strike(
    row: dict[str, Any], shares: dict[str, list[float]] | None
) -> dict[str, Any]:
    """One ``/range`` strike bar, optionally carrying its expiration mix.

    ``call_shares`` / ``put_shares`` are fractions summing to 1, aligned
    positionally to the response's top-level ``expirations`` legend. They only
    SUBDIVIDE ``call_gex`` / ``put_gex``; the totals are unchanged, so a client
    that ignores them renders exactly what it always has. Omitted entirely when
    a side has no expiration breakdown at this strike (the client then draws a
    single solid bar for that side).
    """
    out = {
        "strike": _f(row.get("strike")),
        "net_gex": _f(row.get("net_gex")),
        "call_gex": _f(row.get("call_gex")),
        "put_gex": _f(row.get("put_gex")),
    }
    if shares:
        if shares.get("call"):
            out["call_shares"] = shares["call"]
        if shares.get("put"):
            out["put_shares"] = shares["put"]
    return out


def _shape_candle(row: dict[str, Any]) -> dict[str, Any]:
    """underlying_quotes row → replay candle dict.

    Volume columns fall back to zero so the frontend can render up/down
    volume splits without null-guarding every field.
    """
    return {
        "timestamp": row["timestamp"].isoformat() if row.get("timestamp") else None,
        "open": _f(row.get("open")),
        "high": _f(row.get("high")),
        "low": _f(row.get("low")),
        "close": _f(row.get("close")),
        "up_volume": int(row["up_volume"]) if row.get("up_volume") is not None else 0,
        "down_volume": int(row["down_volume"]) if row.get("down_volume") is not None else 0,
        "volume": int(row["volume"]) if row.get("volume") is not None else 0,
    }


@router.get("/sessions")
async def list_replay_sessions(
    symbol: str = Query(default="SPY", max_length=10),
    limit: int = Query(default=30, ge=1, le=180),
    db: DatabaseManager = Depends(get_db),
):
    """Recent trading days with replayable GEX data for the symbol.

    Used by the /replay date picker. Bar-count is the number of
    ``gex_summary`` rows in the cash session (09:30–16:00 ET) — a full
    session is ~390 minutes; partial sessions surface as such.
    """
    rows = await db.get_replay_session_dates(symbol.upper(), limit=limit)
    return {
        "symbol": symbol.upper(),
        "count": len(rows),
        "sessions": [
            {
                "date": r["session_date"].isoformat()
                if isinstance(r["session_date"], date)
                else r["session_date"],
                "bar_count": int(r["bar_count"]) if r.get("bar_count") is not None else 0,
                "first_ts": r["first_ts"].isoformat() if r.get("first_ts") else None,
                "last_ts": r["last_ts"].isoformat() if r.get("last_ts") else None,
            }
            for r in rows
        ],
    }


@router.get("/frame")
async def get_replay_frame(
    symbol: str = Query(default="SPY", max_length=10),
    ts: str = Query(..., description="ISO-8601 timestamp to render."),
    # The limit counts (strike, expiration) ROWS, not strikes — gex_by_strike is
    # keyed per expiration, so one strike contributes as many rows as it has
    # listed expirations (~10-40).  The default keeps the scrubber's payload
    # small; the 4000 ceiling exists for the session-baseline use (the Pair
    # Comparison ladder's Δ-since-open indicator fetches ONE frame per symbol
    # per session and needs every expiration row across its ±20-strike window,
    # which the old 200-row cap could not span on a dense chain).
    strike_limit: int = Query(default=60, ge=10, le=4000),
    db: DatabaseManager = Depends(get_db),
):
    """Single per-minute replay frame at-or-before ``ts``.

    Combines the gex_summary headline (spot/walls/flip/max-pain) with the
    per-strike GEX bars for the same minute. Optimised for the scrubber:
    ≤100 ms target so a fast drag feels fluid.
    """
    sym = symbol.upper()
    at_ts = _parse_ts(ts)
    summary = await db.get_gex_summary_at_ts(sym, at_ts)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail=f"No GEX data at or before {ts} for {sym}.",
        )
    strikes = await db.get_gex_by_strike_at_ts(sym, summary["timestamp"], limit=strike_limit)
    return {
        "symbol": sym,
        "requested_ts": at_ts.isoformat(),
        "frame_ts": summary["timestamp"].isoformat(),
        "summary": _shape_summary(summary),
        "strikes": _shape_strikes(strikes),
    }


@router.get("/range")
async def get_replay_range(
    symbol: str = Query(default="SPY", max_length=10),
    session_date: str = Query(
        ..., alias="date", description="Trading day YYYY-MM-DD (ET)."
    ),
    timeframe: str = Query(default="1min", pattern="^(1min|5min|15min)$"),
    strike_band_pct: float = Query(default=0.04, ge=0.005, le=0.10),
    include_expirations: bool = Query(
        default=False,
        description=(
            "Attach the per-strike expiration mix (shares) so a client can "
            "colour-grade each bar by time-to-expiry. Off by default: it is a "
            "second session-wide scan and it grows the payload."
        ),
    ),
    max_expirations: int = Query(default=6, ge=1, le=12),
    db: DatabaseManager = Depends(get_db),
):
    """All replay frames for one session — bundled for the playhead buffer.

    Returns per-minute ``gex_summary`` + ``gex_by_strike`` bars for the
    requested ET session date so a single round-trip pre-warms an entire
    session into the browser. The scrubber then renders locally without
    a per-frame fetch.

    ``strike_band_pct`` filters strikes to a ±band around each bar's
    spot so the payload stays bounded — a full-chain SPX session would
    be ~40k rows otherwise. Default 4% covers the strikes that actually
    matter for a same-day dealer-positioning view.

    ``timeframe`` is accepted but currently ignored: we always return
    1-min frames. 5-min / 15-min down-sampling is a v2 optimization
    when payload size becomes a real problem.

    ``include_expirations`` opts into the per-strike expiration mix that
    drives the scrubber's expiry colour gradient. Each strike then also
    carries ``call_shares`` / ``put_shares`` — fractions summing to 1,
    aligned positionally to the response's top-level ``expirations`` legend
    (nearest-first, with a trailing "far" slot when the chain runs deeper
    than ``max_expirations``). Shares only ever SUBDIVIDE the call/put totals
    already in the payload, so a client that ignores them renders exactly what
    it renders today. Off by default because it costs a second session-wide
    scan and grows the bundle; callers that only need the aggregate ladder
    (e.g. the pair-comparison scrubber) should leave it off.
    """
    sym = symbol.upper()
    target = _parse_date(session_date)
    today_et = datetime.now(tz=ET).date()
    is_today = target == today_et

    # A failed frames read is NOT an empty session.  The read raises rather
    # than returning [] precisely so the two stay apart here: a 200 carrying
    # ``count: 0`` is this endpoint asserting the session has no frames, and
    # the page renders copy that says so ("the analytics engine didn't write
    # that day").  Letting a timeout borrow that answer makes an outage look
    # like a data gap to the visitor, and leaves whoever is asked why the
    # replay is blank with a screenshot that rules nothing out.
    try:
        raw_frames = await db.get_gex_frames_for_session(
            sym, target, strike_band_pct=strike_band_pct,
        )
    except ReplayFramesUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "frames_unavailable",
                "message": (
                    f"Replay frames for {sym} on {target.isoformat()} could not be "
                    "read right now. This is a backend failure, not an empty session."
                ),
            },
        ) from exc
    raw_candles = await db.get_underlying_candles_for_session(sym, target)

    # Per-(minute, strike) expiration mix for the expiry colour gradient. Kept
    # as a SEPARATE lookup rather than folded into the frames query so the
    # aggregate ladder above is byte-for-byte what it has always been — an
    # empty / failed shares fetch simply means the client draws plain bars.
    exp_mix: dict[str, Any] = {"expirations": [], "far_bucket": False, "rows": {}}
    if include_expirations:
        exp_mix = await db.get_gex_expiration_shares_for_session(
            sym,
            target,
            strike_band_pct=strike_band_pct,
            max_expirations=max_expirations,
        )
    share_rows = exp_mix.get("rows") or {}

    frames = [
        {
            "timestamp": bar["timestamp"].isoformat(),
            "gamma_flip": _f(bar.get("gamma_flip")),
            # Canonical call/put walls for the minute — same gex_summary
            # columns the /frame + snapshot views read, so the scrubber's
            # level lines agree with the shareable snapshot. Null on older
            # rows written before the wall columns were backfilled; the
            # frontend simply omits the line when null.
            "call_wall": _f(bar.get("call_wall")),
            "put_wall": _f(bar.get("put_wall")),
            # Max pain rides along too so the scrubber can draw the same
            # five-level set (spot, flip, call/put walls, max pain) the live
            # view shows. Null on rows written before max_pain was recorded;
            # the frontend omits the marker when null.
            "max_pain": _f(bar.get("max_pain")),
            # Pin Strike rides along per-minute too — the reachable 0DTE strike
            # with the strongest modeled positive (restoring) dealer gamma into
            # expiration (distinct from the walls/flip/max-pain). pin_confidence
            # (0..1) travels with it so the scrubber can indicate pin strength.
            # Both null on rows written before pin_strike shipped; the frontend
            # omits the line when null.
            "pin_strike": _f(bar.get("pin_strike")),
            "pin_confidence": _f(bar.get("pin_confidence")),
            # Per-strike net plus the call/put split (dollar GEX). call_gex/
            # put_gex let the scrubber render the Split / Combined gamma views
            # like the Strike Profile chart; they're null on rows too old to
            # carry the gamma columns, and the frontend falls back to Net-only.
            "strikes": [
                _shape_range_strike(s, share_rows.get((bar["timestamp"], _f(s.get("strike")))))
                for s in (bar.get("strikes") or [])
            ],
        }
        for bar in raw_frames
    ]
    candles = [_shape_candle(row) for row in raw_candles]
    payload: dict[str, Any] = {
        "symbol": sym,
        "date": target.isoformat(),
        "timeframe": timeframe,
        "is_today": is_today,
        "count": len(frames),
        "frames": frames,
        "candles": candles,
    }
    if include_expirations:
        # Nearest-first legend the per-strike share arrays index into. The
        # trailing "far" label (present only when the chain runs deeper than
        # max_expirations) is a catch-all bucket, not a date — clients rank by
        # position, so it always sorts last, which is exactly where the faintest
        # shade belongs.
        legend = list(exp_mix.get("expirations") or [])
        if exp_mix.get("far_bucket"):
            legend.append("far")
        payload["expirations"] = legend
    return payload


@router.get("/diff")
async def get_replay_diff(
    symbol: str = Query(default="SPY", max_length=10),
    ts_a: str = Query(..., description="ISO-8601 'before' timestamp."),
    ts_b: str = Query(..., description="ISO-8601 'after' timestamp."),
    strike_limit: int = Query(default=60, ge=10, le=200),
    db: DatabaseManager = Depends(get_db),
):
    """Strike-by-strike net GEX delta between two replay timestamps.

    Powers the two-pin diff overlay: drop a 'before' marker and an
    'after' marker on the scrubber and the result is a red/green bar
    chart of which strikes dealers re-hedged into / out of in between.
    """
    sym = symbol.upper()
    a = _parse_ts(ts_a)
    b = _parse_ts(ts_b)
    if a == b:
        raise HTTPException(status_code=422, detail="ts_a and ts_b must differ.")

    summary_a = await db.get_gex_summary_at_ts(sym, a)
    summary_b = await db.get_gex_summary_at_ts(sym, b)
    if summary_a is None or summary_b is None:
        raise HTTPException(
            status_code=404,
            detail=f"Missing GEX data for one or both timestamps on {sym}.",
        )
    strikes_a = await db.get_gex_by_strike_at_ts(
        sym, summary_a["timestamp"], limit=strike_limit
    )
    strikes_b = await db.get_gex_by_strike_at_ts(
        sym, summary_b["timestamp"], limit=strike_limit
    )

    # Index by strike for the subtraction; carry per-side bars too so
    # the frontend can split the delta into call vs put migration.
    by_strike_a = {
        float(r["strike"]): r for r in strikes_a if r.get("strike") is not None
    }
    by_strike_b = {
        float(r["strike"]): r for r in strikes_b if r.get("strike") is not None
    }
    all_strikes = sorted(by_strike_a.keys() | by_strike_b.keys())
    delta_rows = []
    for k in all_strikes:
        a_row = by_strike_a.get(k, {})
        b_row = by_strike_b.get(k, {})
        a_net = _f(a_row.get("net_gex")) or 0.0
        b_net = _f(b_row.get("net_gex")) or 0.0
        delta_rows.append(
            {
                "strike": k,
                "net_gex_a": a_net,
                "net_gex_b": b_net,
                "delta": b_net - a_net,
            }
        )
    return {
        "symbol": sym,
        "ts_a": summary_a["timestamp"].isoformat(),
        "ts_b": summary_b["timestamp"].isoformat(),
        "summary_a": _shape_summary(summary_a),
        "summary_b": _shape_summary(summary_b),
        "deltas": delta_rows,
    }


@router.post("/clip", status_code=503)
async def request_replay_clip():
    """MP4 export of a replay window — v2 feature, not yet deployed.

    The server-side renderer is intentionally not shipped in v1: a real
    Playwright + ffmpeg worker is needed for cross-browser-compatible
    MP4 generation, and the additional deployment / storage surface is
    out of scope for the first replay release. The endpoint exists so
    clients can feature-detect; until the worker lands this returns 503
    with a stable, machine-readable status string.
    """
    raise HTTPException(
        status_code=503,
        detail={
            "status": "not_implemented_v1",
            "message": "MP4 export is a v2 feature. Today, share a static snapshot via /replay/{date}/snapshot/{HHMM}.",
        },
    )
