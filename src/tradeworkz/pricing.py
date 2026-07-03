"""Fill-price resolution for TradeWorkz.

Two situations:

* **At entry** — the engine has a :class:`TradeSignal` with legs but no
  fill price. We look up the current option_chains quote for each leg's
  option symbol; the long side pays the ask, the short side collects the
  bid, plus configurable slippage.
* **At mark-to-market** — the reconciler needs a current mid / liquidation
  price for the open position. We resolve the same way but with
  ``action='close'``, so the long leg is sold at bid and the short at ask.

This module deliberately shells out to :func:`src.signals.execution.leg_fill_price`
so the execution model stays consistent with the (now retired) signaled-
trade engine's semantics.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.signals.execution import leg_fill_price
from src.tradeworkz import config as tw_config

logger = logging.getLogger(__name__)


def _latest_quote(conn: Any, option_symbol: str) -> Optional[Tuple[float, float, float]]:
    """Return (bid, ask, last) for the latest tick of ``option_symbol``.

    Falls back to ``None`` when the row is older than the configured
    ``OPTION_QUOTE_MAX_AGE_SECONDS`` — a stale quote is not a fill.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bid, ask, last, timestamp
        FROM option_chains
        WHERE option_symbol = %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (option_symbol,),
    )
    row = cur.fetchone()
    if not row:
        return None
    bid, ask, last, ts = row
    age = (datetime.utcnow().replace(tzinfo=ts.tzinfo) - ts).total_seconds() if ts else 0
    if age > tw_config.OPTION_QUOTE_MAX_AGE_SECONDS:
        logger.debug("stale quote for %s (age=%.0fs)", option_symbol, age)
        return None
    return (
        float(bid or 0.0),
        float(ask or 0.0),
        float(last or 0.0),
    )


def spread_price(
    conn: Any,
    legs: List[Dict[str, Any]],
    *,
    action: str,
    slippage_pct: Optional[float] = None,
) -> Optional[float]:
    """Per-share fair debit / credit for the whole structure.

    ``action`` is ``'open'`` (entering) or ``'close'`` (marking / exiting).
    Returns the signed per-share cost — positive for debit, negative
    (rare) for a credit structure. Returns ``None`` if any leg has no
    usable quote.
    """
    slip = tw_config.EXECUTION_SLIPPAGE_PCT if slippage_pct is None else slippage_pct
    long_sum = 0.0
    short_sum = 0.0
    for leg in legs:
        sym = leg.get("option_symbol")
        if not sym:
            return None
        quote = _latest_quote(conn, sym)
        if quote is None:
            return None
        bid, ask, last = quote
        side = (leg.get("side") or "").lower()
        if side not in {"long", "short"}:
            return None
        price = leg_fill_price(
            bid=bid, ask=ask, last=last, side=side, action=action, slippage_pct=slip
        )
        if side == "long":
            long_sum += price
        else:
            short_sum += price
    return long_sum - short_sum
