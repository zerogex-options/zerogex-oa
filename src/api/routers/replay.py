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

from ..database import DatabaseManager
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
        "net_gex": _f(row.get("net_gex")),
        "net_gex_at_spot": _f(row.get("net_gex_at_spot")),
        "put_call_ratio": _f(row.get("put_call_ratio")),
    }


def _shape_strikes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "strike": _f(r.get("strike")),
            "call_gex": _f(r.get("call_gex")),
            "put_gex": _f(r.get("put_gex")),
            "net_gex": _f(r.get("net_gex")),
            "distance_from_spot": _f(r.get("distance_from_spot")),
        }
        for r in rows
    ]


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
    strike_limit: int = Query(default=60, ge=10, le=200),
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
    """
    sym = symbol.upper()
    target = _parse_date(session_date)
    today_et = datetime.now(tz=ET).date()
    is_today = target == today_et

    raw_frames = await db.get_gex_frames_for_session(
        sym, target, strike_band_pct=strike_band_pct,
    )

    frames = [
        {
            "timestamp": bar["timestamp"].isoformat(),
            "gamma_flip": _f(bar.get("gamma_flip")),
            "strikes": [
                {"strike": _f(s.get("strike")), "net_gex": _f(s.get("net_gex"))}
                for s in (bar.get("strikes") or [])
            ],
        }
        for bar in raw_frames
    ]
    return {
        "symbol": sym,
        "date": target.isoformat(),
        "timeframe": timeframe,
        "is_today": is_today,
        "count": len(frames),
        "frames": frames,
    }


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
