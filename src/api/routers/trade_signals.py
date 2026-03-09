"""
Trade Signals Router — reads pre-computed rows from trade_signals table.

GET /api/signals/trade?symbol=SPY&timeframe=intraday
GET /api/signals/accuracy?symbol=SPY&lookback_days=30

The AnalyticsEngine writes fresh signal rows every ~5 minutes.
The API simply reads the latest row; no scoring logic lives here.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import datetime, timezone
import logging

from ..database import DatabaseManager
from ..models import (
    TradeSignalResponse,
    SignalComponent,
    SignalDirection,
    SignalStrength,
    TradeIdea,
    TradeType,
    Timeframe,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/signals", tags=["Trade Signals"])

# Fallback win-pct defaults (used only when signal_accuracy table is empty)
_WIN_PCT_DEFAULTS: dict[str, dict[str, float]] = {
    "intraday":  {"high": 0.68, "medium": 0.60, "low": 0.50},
    "swing":     {"high": 0.65, "medium": 0.58, "low": 0.50},
    "multi_day": {"high": 0.63, "medium": 0.57, "low": 0.50},
}

STALE_THRESHOLD_SECONDS = 600  # warn if signal is >10 min old


def get_db() -> DatabaseManager:
    from ..main import db_manager
    return db_manager


def _map_trade_type(raw: str) -> TradeType:
    try:
        return TradeType(raw)
    except ValueError:
        return TradeType.NO_TRADE


def _map_direction(raw: str) -> SignalDirection:
    try:
        return SignalDirection(raw)
    except ValueError:
        return SignalDirection.NEUTRAL


def _map_strength(raw: str) -> SignalStrength:
    try:
        return SignalStrength(raw)
    except ValueError:
        return SignalStrength.LOW


def _map_components(raw_list: list) -> list[SignalComponent]:
    """Convert JSONB component dicts back to SignalComponent models."""
    out = []
    for item in (raw_list or []):
        if not isinstance(item, dict):
            continue
        out.append(SignalComponent(
            name=item.get("name", ""),
            weight=item.get("weight", 0),
            score=item.get("score", 0),
            description=item.get("description", ""),
            value=item.get("value"),
            applicable=item.get("applicable", True),
        ))
    return out


@router.get("/trade", response_model=TradeSignalResponse)
async def get_trade_signal(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    timeframe: Timeframe = Query(
        default=Timeframe.INTRADAY,
        description=(
            "intraday (0DTE, same-session), "
            "swing (1-2DTE, balanced), "
            "multi_day (2-5DTE, structural)"
        ),
    ),
    db: DatabaseManager = Depends(get_db),
):
    """
    Returns the latest pre-computed trade signal for the requested symbol
    and timeframe. Signals are written by the AnalyticsEngine every ~5 min.
    """
    row = await db.get_trade_signal(symbol, timeframe.value)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No trade signal found for {symbol} / {timeframe.value}. "
                "The AnalyticsEngine may not have run yet, or no market data "
                "is available for this symbol."
            ),
        )

    # Staleness warning in logs (doesn't fail the request)
    ts: datetime = row["timestamp"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - ts).total_seconds()
    if age_seconds > STALE_THRESHOLD_SECONDS:
        logger.warning(
            f"Trade signal for {symbol}/{timeframe.value} is "
            f"{age_seconds:.0f}s old (threshold: {STALE_THRESHOLD_SECONDS}s)"
        )

    # Re-apply calibrated win_pct if a fresher value exists in signal_accuracy
    accuracy = await db.get_signal_accuracy(symbol, lookback_days=30)
    strength_str = row.get("strength", "low")
    tf_str = timeframe.value
    calibrated = (
        accuracy.get(tf_str, {})
                .get(strength_str, {})
                .get("win_pct")
    )
    win_pct = calibrated if calibrated is not None else (
        row.get("estimated_win_pct")
        or _WIN_PCT_DEFAULTS[tf_str][strength_str]
    )

    components = _map_components(row.get("components") or [])

    trade_idea = TradeIdea(
        trade_type=_map_trade_type(row.get("trade_type", "no_trade")),
        rationale=row.get("trade_rationale", ""),
        target_expiry=row.get("target_expiry", "N/A"),
        suggested_strikes=row.get("suggested_strikes", "N/A"),
        estimated_win_pct=round(win_pct, 4),
    )

    orb_raw = row.get("orb_breakout_direction")
    sm_raw  = row.get("smart_money_direction")

    return TradeSignalResponse(
        symbol=row["underlying"],
        timeframe=timeframe,
        timestamp=row["timestamp"],
        current_price=float(row.get("current_price") or 0),
        composite_score=row.get("composite_score", 0),
        max_possible_score=row.get("max_possible_score", 1),
        normalized_score=float(row.get("normalized_score") or 0),
        direction=_map_direction(row.get("direction", "neutral")),
        strength=_map_strength(strength_str),
        estimated_win_pct=round(win_pct, 4),
        components=components,
        trade_idea=trade_idea,
        net_gex=row.get("net_gex"),
        gamma_flip=row.get("gamma_flip"),
        price_vs_flip=float(row["price_vs_flip"]) if row.get("price_vs_flip") else None,
        vwap=float(row["vwap"]) if row.get("vwap") else None,
        vwap_deviation_pct=float(row["vwap_deviation_pct"]) if row.get("vwap_deviation_pct") else None,
        put_call_ratio=row.get("put_call_ratio"),
        dealer_net_delta=float(row["dealer_net_delta"]) if row.get("dealer_net_delta") else None,
        smart_money_direction=_map_direction(sm_raw) if sm_raw else None,
        unusual_volume_detected=bool(row.get("unusual_volume_detected")),
        orb_breakout_direction=_map_direction(orb_raw) if orb_raw else None,
    )


@router.get("/accuracy")
async def get_signal_accuracy(
    symbol: str = Query(default="SPY"),
    lookback_days: int = Query(default=30, ge=7, le=365),
    db: DatabaseManager = Depends(get_db),
):
    """
    Returns historically calibrated win rates per timeframe × strength bucket
    over the requested lookback window.
    """
    accuracy = await db.get_signal_accuracy(symbol, lookback_days)
    if not accuracy:
        return {
            "symbol": symbol,
            "lookback_days": lookback_days,
            "note": "Insufficient historical data. Defaults in use.",
            "defaults": _WIN_PCT_DEFAULTS,
        }
    return {
        "symbol": symbol,
        "lookback_days": lookback_days,
        "accuracy": accuracy,
    }
