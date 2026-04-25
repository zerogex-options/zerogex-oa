"""
Option Contract History Router

GET /api/option/contract?underlying=SPY&strike=500&expiration=2025-01-17&option_type=C

Returns all intraday rows for the specified option contract for today's
trading session (if the market is currently open) or the most recent
available date in the database.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from decimal import Decimal
from datetime import datetime, date
from typing import List, Optional, Literal
import logging

from ..database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/option", tags=["Market Data"])


class OptionContractRow(BaseModel):
    timestamp: datetime
    underlying: str
    strike: Decimal
    expiration: date
    option_type: str
    last: Optional[Decimal] = None
    bid: Optional[Decimal] = None
    ask: Optional[Decimal] = None
    mid: Optional[Decimal] = None
    volume: Optional[int] = None
    volume_delta: Optional[int] = None
    open_interest: Optional[int] = None
    ask_volume: Optional[int] = None
    mid_volume: Optional[int] = None
    bid_volume: Optional[int] = None
    implied_volatility: Optional[Decimal] = None
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


def get_db() -> DatabaseManager:
    from ..main import db_manager

    return db_manager


@router.get("/contract", response_model=List[OptionContractRow])
async def get_option_contract(
    underlying: str = Query(..., description="Underlying symbol, e.g. SPY"),
    strike: float = Query(..., description="Strike price"),
    expiration: str = Query(..., description="Expiration date (YYYY-MM-DD)"),
    option_type: Literal["C", "P"] = Query(..., description="Option type: C or P"),
    db: DatabaseManager = Depends(get_db),
):
    """
    Returns all rows for the specified option contract for today's trading
    session if the market is currently open, otherwise for the most recent
    date that has data for this contract.
    """
    try:
        rows = await db.get_option_contract_history(underlying, strike, expiration, option_type)
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for {underlying} {strike} {expiration} {option_type}",
            )
        return [OptionContractRow(**row) for row in rows]
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid parameter: {e}")
    except Exception as e:
        logger.error(f"Error fetching option contract history: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
