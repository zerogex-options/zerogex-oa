"""Realistic execution model for option-spread fills.

Long legs are bought at the ask; short legs are sold at the bid.  On exit the
sides invert (the long leg is liquidated at bid, the short leg is bought back
at ask).  A slippage fraction widens each side symmetrically to model adverse
selection on top of the quoted spread.
"""

from __future__ import annotations

from typing import Optional

from src.config import SIGNALS_EXECUTION_SLIPPAGE_PCT


def _mid_fallback(bid: float, ask: float, last: float) -> float:
    """Best-effort price when one side of the quote is missing.

    Used only as a last resort so callers always receive a non-negative price
    and don't have to special-case half-populated rows.
    """
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return max(last, ask, bid, 0.0)


def _is_buy(side: str, action: str) -> bool:
    """True when the leg is being purchased at this action.

    Opening a long leg = buy; closing a short leg = buy-to-close.
    """
    side = side.lower()
    action = action.lower()
    if action not in {"open", "close"}:
        raise ValueError(f"action must be 'open' or 'close', got {action!r}")
    if side not in {"long", "short"}:
        raise ValueError(f"side must be 'long' or 'short', got {side!r}")
    return (side == "long") == (action == "open")


def leg_fill_price(
    *,
    bid: float,
    ask: float,
    last: float = 0.0,
    side: str,
    action: str,
    slippage_pct: Optional[float] = None,
) -> float:
    """Per-share fill price for a single leg under the realistic model."""
    slip = SIGNALS_EXECUTION_SLIPPAGE_PCT if slippage_pct is None else max(slippage_pct, 0.0)
    if _is_buy(side, action):
        if ask > 0:
            return ask * (1.0 + slip)
        return _mid_fallback(bid, ask, last)
    if bid > 0:
        return bid * (1.0 - slip)
    return _mid_fallback(bid, ask, last)


def leg_fill_price_from_row(
    row: dict,
    *,
    side: str,
    action: str,
    slippage_pct: Optional[float] = None,
) -> float:
    return leg_fill_price(
        bid=float(row.get("bid") or 0.0),
        ask=float(row.get("ask") or 0.0),
        last=float(row.get("last") or 0.0),
        side=side,
        action=action,
        slippage_pct=slippage_pct,
    )
