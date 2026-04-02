"""
Trade Signals Router — reads pre-computed rows from trade_signals table.

GET /api/signals/trade?symbol=SPY&timeframe=intraday
GET /api/signals/history?symbol=SPY&limit=100
GET /api/signals/vol-expansion?symbol=SPY

The standalone Signal Engine writes fresh signal rows every ~5 minutes.
The API simply reads the latest row; no scoring logic lives here.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import datetime, timezone
import enum
import logging

from src.signals.position_optimizer_engine import ASSUMED_ACCOUNT_EQUITY, RISK_PROFILE_BUDGETS

from ..database import DatabaseManager
from ..models import (
    TradeSignalResponse,
    SignalComponent,
    SignalDirection,
    SignalStrength,
    TradeIdea,
    TradeType,
    Timeframe,
    VolExpansionSignalResponse,
    VolExpansionComponent,
    VolExpansionDirection,
    PositionOptimizerSignalResponse,
    PositionOptimizerDirection,
    PositionOptimizerCandidate,
    PositionOptimizerCandidateComponent,
    PositionOptimizerSizingProfile,
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


def _map_enum(raw: str, enum_cls: type[enum.Enum], default: enum.Enum) -> enum.Enum:
    """Map a raw string to an enum value, returning *default* on mismatch."""
    try:
        return enum_cls(raw)
    except ValueError:
        return default


def _map_trade_type(raw: str) -> TradeType:
    return _map_enum(raw, TradeType, TradeType.NO_TRADE)


def _map_direction(raw: str) -> SignalDirection:
    return _map_enum(raw, SignalDirection, SignalDirection.NEUTRAL)


def _map_strength(raw: str) -> SignalStrength:
    return _map_enum(raw, SignalStrength, SignalStrength.LOW)


def _map_vol_direction(raw: str) -> VolExpansionDirection:
    return _map_enum(raw, VolExpansionDirection, VolExpansionDirection.NEUTRAL)


def _map_position_direction(raw: str) -> PositionOptimizerDirection:
    return _map_enum(raw, PositionOptimizerDirection, PositionOptimizerDirection.NEUTRAL)


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


def _rescaled_sizing_profiles(
    candidate: PositionOptimizerCandidate,
    portfolio_value: float,
) -> list[PositionOptimizerSizingProfile]:
    effective_risk = max(candidate.max_loss, candidate.entry_debit, 1.0)
    sizing_profiles: list[PositionOptimizerSizingProfile] = []

    for profile, heat_pct in RISK_PROFILE_BUDGETS.items():
        budget = portfolio_value * heat_pct
        kelly_adjusted_budget = max(
            budget * max(candidate.kelly_fraction, 0.10),
            min(budget, effective_risk),
        )
        contracts = max(1, int(kelly_adjusted_budget // effective_risk)) if candidate.expected_value > 0 else 0
        constrained_by = (
            "edge filter"
            if candidate.expected_value <= 0
            else ("kelly fraction" if kelly_adjusted_budget < budget else "portfolio heat cap")
        )
        sizing_profiles.append(
            PositionOptimizerSizingProfile(
                profile=profile,
                contracts=contracts,
                max_risk_dollars=round(contracts * effective_risk, 2),
                expected_value_dollars=round(contracts * candidate.expected_value, 2),
                constrained_by=constrained_by,
            )
        )

    return sizing_profiles


def _apply_portfolio_value_to_candidates(
    candidates: list[PositionOptimizerCandidate],
    portfolio_value: float | None,
) -> list[PositionOptimizerCandidate]:
    if portfolio_value is None or portfolio_value == ASSUMED_ACCOUNT_EQUITY:
        return candidates

    for candidate in candidates:
        candidate.sizing_profiles = _rescaled_sizing_profiles(candidate, portfolio_value)
    return candidates


def _map_position_candidates(raw_list: list) -> list[PositionOptimizerCandidate]:
    out = []
    for item in (raw_list or []):
        if not isinstance(item, dict):
            continue
        components = []
        for comp in (item.get("components") or []):
            if not isinstance(comp, dict):
                continue
            components.append(PositionOptimizerCandidateComponent(
                name=comp.get("name", ""),
                weight=comp.get("weight", 0),
                raw_score=comp.get("raw_score", 0),
                weighted_score=comp.get("weighted_score", 0),
                description=comp.get("description", ""),
                value=comp.get("value"),
            ))
        sizing_profiles = []
        for profile in (item.get("sizing_profiles") or []):
            if not isinstance(profile, dict):
                continue
            sizing_profiles.append(PositionOptimizerSizingProfile(
                profile=profile.get("profile", ""),
                contracts=profile.get("contracts", 0),
                max_risk_dollars=float(profile.get("max_risk_dollars") or 0),
                expected_value_dollars=float(profile.get("expected_value_dollars") or 0),
                constrained_by=profile.get("constrained_by", ""),
            ))
        out.append(PositionOptimizerCandidate(
            rank=item.get("rank", 0),
            strategy_type=item.get("strategy_type", ""),
            expiry=item.get("expiry"),
            dte=item.get("dte", 0),
            strikes=item.get("strikes", ""),
            option_type=item.get("option_type", ""),
            entry_debit=float(item.get("entry_debit") or 0),
            entry_credit=float(item.get("entry_credit") or 0),
            width=float(item.get("width") or 0),
            max_profit=float(item.get("max_profit") or 0),
            max_loss=float(item.get("max_loss") or 0),
            risk_reward_ratio=float(item.get("risk_reward_ratio") or 0),
            probability_of_profit=float(item.get("probability_of_profit") or 0),
            expected_value=float(item.get("expected_value") or 0),
            sharpe_like_ratio=float(item.get("sharpe_like_ratio") or 0),
            liquidity_score=float(item.get("liquidity_score") or 0),
            net_delta=float(item.get("net_delta") or 0),
            net_gamma=float(item.get("net_gamma") or 0),
            net_theta=float(item.get("net_theta") or 0),
            premium_efficiency=float(item.get("premium_efficiency") or 0),
            market_structure_fit=float(item.get("market_structure_fit") or 0),
            greek_alignment_score=float(item.get("greek_alignment_score") or 0),
            edge_score=float(item.get("edge_score") or 0),
            kelly_fraction=float(item.get("kelly_fraction") or 0),
            sizing_profiles=sizing_profiles,
            components=components,
            reasoning=[str(reason) for reason in (item.get("reasoning") or [])],
        ))
    return out

def _map_vol_components(raw_list: list) -> list[VolExpansionComponent]:
    out = []
    for item in (raw_list or []):
        if not isinstance(item, dict):
            continue
        out.append(VolExpansionComponent(
            name=item.get("name", ""),
            weight=item.get("weight", 0),
            raw_score=item.get("raw_score", 0),
            weighted_score=item.get("weighted_score", 0),
            description=item.get("description", ""),
            value=item.get("value"),
        ))
    return out


@router.get("/trade")
async def get_trade_signal(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    timeframe: Timeframe = Query(default=Timeframe.INTRADAY),
    db: DatabaseManager = Depends(get_db),
):
    """Return current consolidated signal plus active trade status from Signal Engine."""
    row = await db.get_current_signal_with_trades(symbol, timeframe.value)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No trade signal found for {symbol} / {timeframe.value}. "
                "The Signal Engine may not have run yet, or no market data is available."
            ),
        )

    ts: datetime = row["timestamp"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - ts).total_seconds()
    if age_seconds > STALE_THRESHOLD_SECONDS:
        logger.warning(
            f"Trade signal for {symbol}/{timeframe.value} is {age_seconds:.0f}s old "
            f"(threshold: {STALE_THRESHOLD_SECONDS}s)"
        )

    accuracy = await db.get_signal_accuracy(symbol, lookback_days=30)
    strength_str = row.get("strength", "low")
    tf_str = timeframe.value
    calibrated = accuracy.get(tf_str, {}).get(strength_str, {}).get("win_pct")
    win_pct = calibrated if calibrated is not None else (row.get("estimated_win_pct") or _WIN_PCT_DEFAULTS[tf_str][strength_str])

    return {
        "symbol": row["underlying"],
        "timeframe": timeframe.value,
        "timestamp": row["timestamp"],
        "status": "in_position" if row.get("has_active_trade") else "monitoring",
        "direction": row.get("direction", "neutral"),
        "strength": strength_str,
        "estimated_win_pct": round(float(win_pct), 4),
        "trade_idea": {
            "trade_type": row.get("trade_type", "no_trade"),
            "rationale": row.get("trade_rationale", ""),
            "target_expiry": row.get("target_expiry", "N/A"),
            "suggested_strikes": row.get("suggested_strikes", "N/A"),
        },
        "active_trades": row.get("active_trades", []),
        "components": row.get("components", {}),
    }


@router.get("/history")
async def get_signal_history(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: DatabaseManager = Depends(get_db),
):
    """Past trade details with win/loss and realized/unrealized/total P&L."""
    rows = await db.get_signal_history(symbol, limit)
    total_pnl = round(sum(float(r.get("total_pnl") or 0) for r in rows), 2)
    wins = sum(1 for r in rows if r.get("outcome") == "win")
    losses = sum(1 for r in rows if r.get("outcome") == "loss")
    return {
        "symbol": symbol,
        "trades": rows,
        "summary": {
            "total_trades": len(rows),
            "wins": wins,
            "losses": losses,
            "win_rate": round((wins / len(rows)), 4) if rows else None,
            "total_pnl": total_pnl,
        },
    }


@router.get("/vol-expansion", response_model=VolExpansionSignalResponse)
async def get_vol_expansion_signal(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    db: DatabaseManager = Depends(get_db),
):
    """Return the latest volatility-expansion / large-move prediction for the symbol."""
    row = await db.get_vol_expansion_signal(symbol)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No volatility expansion signal found for {symbol}. "
                "The Signal Engine may not have run yet, or no market data is available."
            ),
        )

    ts: datetime = row["timestamp"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - ts).total_seconds()
    if age_seconds > STALE_THRESHOLD_SECONDS:
        logger.warning(
            f"Vol expansion signal for {symbol} is {age_seconds:.0f}s old "
            f"(threshold: {STALE_THRESHOLD_SECONDS}s)"
        )

    return VolExpansionSignalResponse(
        symbol=row["underlying"],
        timestamp=row["timestamp"],
        composite_score=row.get("composite_score", 0),
        max_possible_score=row.get("max_possible_score", 1),
        normalized_score=float(row.get("normalized_score") or 0),
        move_probability=float(row.get("move_probability") or 0),
        expected_direction=_map_vol_direction(row.get("expected_direction", "neutral")),
        expected_magnitude_pct=float(row.get("expected_magnitude_pct") or 0),
        confidence=_map_strength(row.get("confidence", "low")),
        catalyst_type=row.get("catalyst_type", "mixed"),
        time_horizon=row.get("time_horizon", "intraday"),
        strategy_type=row.get("strategy_type", "wait"),
        entry_window=row.get("entry_window"),
        current_price=float(row["current_price"]) if row.get("current_price") is not None else None,
        net_gex=float(row["net_gex"]) if row.get("net_gex") is not None else None,
        gamma_flip=float(row["gamma_flip"]) if row.get("gamma_flip") is not None else None,
        max_pain=float(row["max_pain"]) if row.get("max_pain") is not None else None,
        put_call_ratio=float(row["put_call_ratio"]) if row.get("put_call_ratio") is not None else None,
        dealer_net_delta=float(row["dealer_net_delta"]) if row.get("dealer_net_delta") is not None else None,
        smart_money_direction=_map_vol_direction(row.get("smart_money_direction", "neutral")) if row.get("smart_money_direction") else None,
        vwap_deviation_pct=float(row["vwap_deviation_pct"]) if row.get("vwap_deviation_pct") is not None else None,
        hours_to_next_expiry=float(row["hours_to_next_expiry"]) if row.get("hours_to_next_expiry") is not None else None,
        components=_map_vol_components(row.get("components") or []),
    )


@router.get("/position-optimizer", response_model=PositionOptimizerSignalResponse)
async def get_position_optimizer_signal(
    symbol: str = Query(default="SPY", description="Underlying symbol"),
    portfolio_value: float | None = Query(
        default=None,
        gt=0,
        description=(
            "Optional account equity override used to rescale candidate sizing profiles. "
            f"Defaults to the optimizer's assumed equity of ${ASSUMED_ACCOUNT_EQUITY:,.0f}."
        ),
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Return the latest position-optimizer spread ranking for the symbol."""
    row = await db.get_position_optimizer_signal(symbol)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No position optimizer signal found for {symbol}. "
                "The Signal Engine may not have run yet, or no market data is available."
            ),
        )

    ts: datetime = row["timestamp"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - ts).total_seconds()
    if age_seconds > STALE_THRESHOLD_SECONDS:
        logger.warning(
            f"Position optimizer signal for {symbol} is {age_seconds:.0f}s old "
            f"(threshold: {STALE_THRESHOLD_SECONDS}s)"
        )

    candidates = _apply_portfolio_value_to_candidates(
        _map_position_candidates(row.get("candidates") or []),
        portfolio_value,
    )

    return PositionOptimizerSignalResponse(
        symbol=row["underlying"],
        timestamp=row["timestamp"],
        signal_timestamp=row["signal_timestamp"],
        signal_timeframe=Timeframe(row.get("signal_timeframe", "intraday")),
        signal_direction=_map_position_direction(row.get("signal_direction", "neutral")),
        signal_strength=_map_strength(row.get("signal_strength", "low")),
        trade_type=row.get("trade_type", "no_trade"),
        current_price=float(row.get("current_price") or 0),
        composite_score=float(row.get("composite_score") or 0),
        max_possible_score=row.get("max_possible_score", 1),
        normalized_score=float(row.get("normalized_score") or 0),
        top_strategy_type=row.get("top_strategy_type", ""),
        top_expiry=row.get("top_expiry"),
        top_dte=row.get("top_dte", 0),
        top_strikes=row.get("top_strikes", ""),
        top_probability_of_profit=float(row.get("top_probability_of_profit") or 0),
        top_expected_value=float(row.get("top_expected_value") or 0),
        top_max_profit=float(row.get("top_max_profit") or 0),
        top_max_loss=float(row.get("top_max_loss") or 0),
        top_kelly_fraction=float(row.get("top_kelly_fraction") or 0),
        top_sharpe_like_ratio=float(row["top_sharpe_like_ratio"]) if row.get("top_sharpe_like_ratio") is not None else None,
        top_liquidity_score=float(row["top_liquidity_score"]) if row.get("top_liquidity_score") is not None else None,
        top_market_structure_fit=float(row["top_market_structure_fit"]) if row.get("top_market_structure_fit") is not None else None,
        top_reasoning=[str(reason) for reason in (row.get("top_reasoning") or [])],
        candidates=candidates,
    )


