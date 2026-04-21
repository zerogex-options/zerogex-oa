"""Signal/trade APIs backed by unified signal tables."""

from __future__ import annotations

import math
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from src.config import SIGNALS_PORTFOLIO_SIZE

from ..database import DatabaseManager

router = APIRouter(prefix="/api/signals", tags=["Trade Signals"])


def _scale_signed_100(value: Any) -> Any:
    """Scale a signed [-1, 1] metric into [-100, 100].

    All score columns in the signal tables are produced by ComponentBase
    implementations that contractually return values in [-1, +1].  The
    API tier multiplies by 100 so the UI gets percentage-style numbers
    without heuristic "scale guessing" (which silently corrupted any
    component whose honest output happened to exceed ±1).

    Non-numeric values pass through unchanged.  NaN / inf collapse to 0.
    Result is clamped to [-100, 100] and rounded to 4 decimals.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value

    raw = float(value)
    if math.isnan(raw) or math.isinf(raw):
        return 0.0

    scaled = max(-100.0, min(100.0, raw * 100.0))
    return round(scaled, 4)



def _normalize_signal_components(value: Any) -> Any:
    """Scale unified-signal component score fields to [-100, 100]."""
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key == "score":
                out[key] = _scale_signed_100(item)
            else:
                out[key] = _normalize_signal_components(item)
        return out
    if isinstance(value, list):
        return [_normalize_signal_components(v) for v in value]
    return value


def _normalize_signal_score_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize consolidated signal-score payload to [-100, 100].

    Also extracts the ``__aggregation__`` block from ``components`` into a
    top-level ``aggregation`` key so the API response keeps signal components
    and aggregation diagnostics cleanly separated.
    """
    out = dict(row)
    out["composite_score"] = _scale_signed_100(out.get("composite_score"))
    out["normalized_score"] = _scale_signed_100(out.get("normalized_score"))
    if "components" in out and isinstance(out["components"], dict):
        components = dict(out["components"])
        aggregation = components.pop("__aggregation__", None)
        out["components"] = _normalize_signal_components(components)
        if aggregation is not None:
            out["aggregation"] = aggregation
    return out



def get_db() -> DatabaseManager:
    from ..main import db_manager
    return db_manager


@router.get("/trades-history")
async def get_signal_history(
    limit: int = Query(default=500, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    rows = await db.get_closed_signal_trades(limit=limit)
    total_pnl = round(sum(float(r.get("total_pnl") or 0) for r in rows), 4)
    wins = sum(1 for r in rows if r.get("outcome") == "win")
    return {
        "portfolio_size": SIGNALS_PORTFOLIO_SIZE,
        "trades": rows,
        "summary": {
            "total_trades": len(rows),
            "wins": wins,
            "losses": sum(1 for r in rows if r.get("outcome") == "loss"),
            "win_rate": round(wins / len(rows), 4) if rows else None,
            "total_pnl": total_pnl,
        },
    }


@router.get("/trades-live")
async def get_live_signals(db: DatabaseManager = Depends(get_db)):
    rows = await db.get_live_signal_trades()
    return {
        "trades": rows,
        "count": len(rows),
    }


@router.get("/score")
async def get_latest_score(
    underlying: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    row = await db.get_latest_signal_score_enriched(underlying.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No score rows found for {underlying.upper()}")
    return _normalize_signal_score_row(row)


@router.get("/score-history")
async def get_score_history(
    underlying: str = Query(default="SPY"),
    limit: int = Query(default=90, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    rows = await db.get_signal_score_history(underlying.upper(), limit)
    normalized_rows = [_normalize_signal_score_row(row) for row in rows]
    return {
        "underlying": underlying.upper(),
        "rows": normalized_rows,
        "count": len(normalized_rows),
    }


@router.get("/vol-expansion")
async def get_vol_expansion_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest vol-expansion component score from the unified signal engine.

    Returns two trader-facing dimensions alongside the composite score:
      * **expansion** (0–100): How likely is vol to expand? (GEX-driven)
      * **direction** (-100–+100): If it expands, which way? (momentum-driven)
      * **score** (-100–+100): Combined composite contribution
    """
    row = await db.get_vol_expansion_signal(symbol.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No vol-expansion score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["expansion"] = ctx.get("expansion")
    row["direction_score"] = ctx.get("direction")
    row["magnitude"] = ctx.get("magnitude")
    row["expected_5min_move_bps"] = ctx.get("expected_5min_move_bps")
    return row


@router.get("/eod-pressure")
async def get_eod_pressure_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest end-of-day pressure score from the unified signal engine.

    Combines charm-at-spot flow, gamma-gated pin gravity, and a calendar
    amplifier (OpEx, quad-witching) into a single directional forecast
    for the last ~75 minutes of the cash session.

    The component is gated off before 14:30 ET; outside the window the
    response returns ``score == 0`` with ``time_ramp == 0``.

    Top-level fields:
      * **score** (-100..+100): Positive => bullish close, negative => bearish.
      * **direction**: "bullish" / "bearish" / "neutral".
      * **charm_at_spot**: Signed charm exposure summed across strikes
        within ±1% of spot.
      * **pin_target**: Heavy-GEX strike (or max_pain fallback).
      * **pin_distance_pct**: (pin - spot) / spot.
      * **gamma_regime**: "positive" / "negative" (flips pin-gravity sign).
      * **time_ramp** (0..1): Time-to-close scale (0 before T-90min,
        1.0 by T-15min).
      * **calendar_flags**: {opex, quad_witching}.
    """
    row = await db.get_eod_pressure_signal(symbol.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No eod-pressure score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["charm_at_spot"] = ctx.get("charm_at_spot")
    row["pin_target"] = ctx.get("pin_target")
    row["pin_distance_pct"] = ctx.get("pin_distance_pct")
    row["gamma_regime"] = ctx.get("gamma_regime")
    row["time_ramp"] = ctx.get("time_ramp")
    row["calendar_flags"] = ctx.get("calendar_flags")
    return row


@router.get("/squeeze-setup")
async def get_squeeze_setup_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest independent squeeze-setup alert (not part of composite score)."""
    row = await db.get_independent_signal(symbol.upper(), "squeeze_setup")
    if not row:
        raise HTTPException(status_code=404, detail=f"No squeeze-setup signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["call_flow_delta"] = ctx.get("call_flow_delta")
    row["put_flow_delta"] = ctx.get("put_flow_delta")
    row["call_flow_z"] = ctx.get("call_flow_z")
    row["put_flow_z"] = ctx.get("put_flow_z")
    row["momentum_z"] = ctx.get("momentum_z")
    row["vix_regime"] = ctx.get("vix_regime")
    return row


@router.get("/trap-detection")
async def get_trap_detection_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest independent trap-detection/fade signal (not part of composite score)."""
    row = await db.get_independent_signal(symbol.upper(), "trap_detection")
    if not row:
        raise HTTPException(status_code=404, detail=f"No trap-detection signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["breakout_up"] = ctx.get("breakout_up", False)
    row["breakout_down"] = ctx.get("breakout_down", False)
    row["net_gex_delta"] = ctx.get("net_gex_delta")
    row["net_gex_delta_pct"] = ctx.get("net_gex_delta_pct")
    row["resistance_level"] = ctx.get("resistance_level")
    row["support_level"] = ctx.get("support_level")
    row["breakout_buffer_pct"] = ctx.get("breakout_buffer_pct")
    row["wall_migrated_up"] = ctx.get("wall_migrated_up")
    row["wall_migrated_down"] = ctx.get("wall_migrated_down")
    return row


@router.get("/0dte-position-imbalance")
async def get_zero_dte_position_imbalance_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest independent 0DTE position-imbalance indicator."""
    row = await db.get_independent_signal(symbol.upper(), "zero_dte_position_imbalance")
    if not row:
        raise HTTPException(status_code=404, detail=f"No 0DTE position-imbalance signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "balanced")
    row["flow_imbalance"] = ctx.get("flow_imbalance")
    row["smart_imbalance"] = ctx.get("smart_imbalance")
    return row


@router.get("/gamma-vwap-confluence")
async def get_gamma_vwap_confluence_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest independent gamma+VWAP confluence indicator."""
    row = await db.get_independent_signal(symbol.upper(), "gamma_vwap_confluence")
    if not row:
        raise HTTPException(status_code=404, detail=f"No gamma+VWAP confluence signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["confluence_level"] = ctx.get("confluence_level")
    row["cluster_gap_pct"] = ctx.get("cluster_gap_pct")
    row["expected_target"] = ctx.get("expected_target")
    return row


_VALID_SIGNAL_EVENT_NAMES = {
    "vol_expansion",
    "eod_pressure",
    "squeeze_setup",
    "trap_detection",
    "zero_dte_position_imbalance",
    "gamma_vwap_confluence",
    "positioning_trap",
    "vanna_charm_flow",
}


@router.get("/{signal_name}/events")
async def get_signal_events(
    signal_name: str,
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=100, ge=1, le=1000),
    horizon: str = Query(default="60m", pattern="^(30m|60m|120m)$"),
    db: DatabaseManager = Depends(get_db),
):
    """Component event stream with direction-sign flips and input snapshots."""
    if signal_name not in _VALID_SIGNAL_EVENT_NAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown signal_name '{signal_name}'.",
        )
    sym = symbol.upper()
    rows = await db.get_signal_component_events(
        sym, signal_name, limit=limit, horizon=horizon
    )

    flips = [r for r in rows if r.get("direction_flip")]
    bullish = sum(1 for r in rows if r.get("direction") == "bullish")
    bearish = sum(1 for r in rows if r.get("direction") == "bearish")
    neutral = sum(1 for r in rows if r.get("direction") == "neutral")

    return {
        "underlying": sym,
        "signal_name": signal_name,
        "horizon": horizon,
        "rows": rows,
        "count": len(rows),
        "summary": {
            "flips": len(flips),
            "bullish": bullish,
            "bearish": bearish,
            "neutral": neutral,
            "latest_timestamp": rows[0]["timestamp"] if rows else None,
            "latest_direction": rows[0]["direction"] if rows else "neutral",
        },
    }


@router.get("/confluence-matrix")
async def get_confluence_matrix(
    symbol: str = Query(default="SPY"),
    lookback: int = Query(default=120, ge=10, le=2000),
    db: DatabaseManager = Depends(get_db),
):
    """Component-level agreement/disagreement matrix over a rolling lookback."""
    matrix = await db.get_signal_confluence_matrix(symbol.upper(), lookback=lookback)

    return {
        "underlying": symbol.upper(),
        "lookback": lookback,
        "components": matrix.get("components", []),
        "matrix": matrix.get("matrix", []),
        "row_order": matrix.get("components", []),
        "sample_count": matrix.get("sample_count", 0),
        "latest_timestamp": matrix.get("latest_timestamp"),
    }
