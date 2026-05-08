"""
Option Calculator Tool Router

GET /api/tools/option-calculator?underlying=SPY&strike=730&expiration=2026-05-08
                              &option_type=P&num_contracts=50&steps=20
                              &step_pct=0.001&fee_per_contract=0.5

Projects intrinsic-value P&L for an option position across a fan of
underlying-price moves.  For a Put the fan walks down from spot in
``step_pct`` increments; for a Call it walks up.  The response includes
position cost (with brokerage fees), breakeven, and per-step P&L.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import List, Literal, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tools", tags=["Tools"])

# Bounds enforced by both query validation and downstream logic so a hand-
# crafted request can't drive the computation outside calibrated ranges.
_STEPS_MIN = 1
_STEPS_MAX = 100
_STEP_PCT_MIN = 1e-6
_STEP_PCT_MAX = 0.01  # 1.0%
_NUM_CONTRACTS_MIN = 1
_FEE_MIN = 0.0


class OptionCalculatorScenario(BaseModel):
    pct_move: float = Field(..., description="Percent move applied to spot for this step.")
    underlying_price: float = Field(..., description="Underlying price after the move.")
    intrinsic_per_contract: float = Field(
        ..., description="Intrinsic value of one contract at this underlying price."
    )
    intrinsic_position: float = Field(
        ..., description="Intrinsic value of the whole position (per_contract × contracts × 100)."
    )
    pnl: float = Field(
        ...,
        description="P&L assuming intrinsic value at expiration (intrinsic_position − total_cost).",
    )


class OptionCalculatorResponse(BaseModel):
    underlying: str
    strike: float
    expiration: date
    option_type: Literal["C", "P"]
    num_contracts: int
    steps: int
    step_pct: float
    fee_per_contract: float
    spot_price: float
    entry_price: float
    entry_price_source: str = Field(
        ...,
        description="Which option-chain field was used as the entry price (mid/last/bid_ask_avg).",
    )
    quote_timestamp: Optional[datetime] = None
    total_cost: float = Field(
        ..., description="entry_price × num_contracts × 100 + num_contracts × fee_per_contract"
    )
    total_fees: float
    breakeven_price: float
    pct_move_to_breakeven: float
    scenarios: List[OptionCalculatorScenario]


def get_db() -> DatabaseManager:
    from ..main import db_manager

    return db_manager


def _coerce_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_entry_price(row: dict) -> tuple[Optional[float], str]:
    """Pick the most reliable entry price from the latest option-chain row.

    Priority: mid → last → midpoint of bid/ask.  Returns (price, source).
    """
    mid = _coerce_float(row.get("mid"))
    if mid and mid > 0:
        return mid, "mid"
    last = _coerce_float(row.get("last"))
    if last and last > 0:
        return last, "last"
    bid = _coerce_float(row.get("bid"))
    ask = _coerce_float(row.get("ask"))
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2.0, "bid_ask_avg"
    if last is not None and last > 0:
        return last, "last"
    return None, "unavailable"


@router.get("/option-calculator", response_model=OptionCalculatorResponse)
async def option_calculator(
    underlying: str = Query(..., description="Underlying symbol, e.g. SPY"),
    strike: float = Query(..., gt=0, description="Strike price"),
    expiration: str = Query(..., description="Expiration date (YYYY-MM-DD)"),
    option_type: Literal["C", "P"] = Query(..., description="Option type: C or P"),
    num_contracts: int = Query(
        1,
        ge=_NUM_CONTRACTS_MIN,
        description="Number of contracts (each contract = 100 shares).",
    ),
    steps: int = Query(
        20,
        ge=_STEPS_MIN,
        le=_STEPS_MAX,
        description=f"Number of price-move scenarios to project ({_STEPS_MIN}–{_STEPS_MAX}).",
    ),
    step_pct: float = Query(
        0.001,
        gt=0,
        le=_STEP_PCT_MAX,
        description=f"Percent move per step as a fraction (max {_STEP_PCT_MAX}).",
    ),
    fee_per_contract: float = Query(
        0.5,
        ge=_FEE_MIN,
        description="Brokerage fee per contract.",
    ),
    db: DatabaseManager = Depends(get_db),
):
    """Project intrinsic-value P&L across a fan of underlying-price moves."""
    underlying_sym = underlying.upper()
    rows = await db.get_option_contract_history(underlying_sym, strike, expiration, option_type)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=(f"No data found for {underlying_sym} {strike} {expiration} {option_type}"),
        )

    # Latest minute bar wins — the contract history is already sorted ASC.
    latest = rows[-1]
    entry_price, entry_source = _resolve_entry_price(latest)
    if entry_price is None or entry_price <= 0:
        raise HTTPException(
            status_code=409,
            detail=(
                "Could not resolve an entry price from the latest option-chain row "
                "(mid/last/bid/ask all missing or non-positive)."
            ),
        )

    spot_quote = await db.get_latest_quote(underlying_sym)
    spot_price = _coerce_float(spot_quote.get("close")) if spot_quote else None
    if spot_price is None or spot_price <= 0:
        raise HTTPException(
            status_code=404,
            detail=f"No spot price available for {underlying_sym}",
        )

    expiration_date = datetime.strptime(expiration, "%Y-%m-%d").date()

    contract_multiplier = 100  # standard equity-option multiplier
    total_fees = num_contracts * fee_per_contract
    total_cost = entry_price * num_contracts * contract_multiplier + total_fees

    if option_type == "C":
        breakeven_price = strike + entry_price
        direction = 1.0  # walk underlying up
    else:
        breakeven_price = strike - entry_price
        direction = -1.0  # walk underlying down

    # Strike-relative formulation: matches the user's spec ("-5 / 730 => -0.68%").
    # Equivalent to ±entry_price / strike with the sign tracking option_type.
    pct_move_to_breakeven = (breakeven_price - strike) / strike if strike > 0 else 0.0

    scenarios: List[OptionCalculatorScenario] = []
    for i in range(1, steps + 1):
        pct_move = direction * step_pct * i
        underlying_price = spot_price * (1.0 + pct_move)
        if option_type == "C":
            intrinsic_per_contract = max(0.0, underlying_price - strike)
        else:
            intrinsic_per_contract = max(0.0, strike - underlying_price)
        intrinsic_position = intrinsic_per_contract * num_contracts * contract_multiplier
        pnl = intrinsic_position - total_cost
        scenarios.append(
            OptionCalculatorScenario(
                pct_move=round(pct_move, 6),
                underlying_price=round(underlying_price, 4),
                intrinsic_per_contract=round(intrinsic_per_contract, 4),
                intrinsic_position=round(intrinsic_position, 2),
                pnl=round(pnl, 2),
            )
        )

    return OptionCalculatorResponse(
        underlying=underlying_sym,
        strike=strike,
        expiration=expiration_date,
        option_type=option_type,
        num_contracts=num_contracts,
        steps=steps,
        step_pct=step_pct,
        fee_per_contract=fee_per_contract,
        spot_price=round(spot_price, 4),
        entry_price=round(entry_price, 4),
        entry_price_source=entry_source,
        quote_timestamp=latest.get("timestamp"),
        total_cost=round(total_cost, 2),
        total_fees=round(total_fees, 2),
        breakeven_price=round(breakeven_price, 4),
        pct_move_to_breakeven=round(pct_move_to_breakeven, 6),
        scenarios=scenarios,
    )
