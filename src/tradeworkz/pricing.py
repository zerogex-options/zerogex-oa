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
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from src.signals.execution import leg_fill_price
from src.tradeworkz import config as tw_config

logger = logging.getLogger(__name__)


def _parse_leg_expiration(leg: Dict[str, Any]) -> Optional[date]:
    """Parse ``leg['expiration']`` (ISO ``YYYY-MM-DD``) to a date object."""
    exp_str = leg.get("expiration") or ""
    if not isinstance(exp_str, str) or len(exp_str) < 10:
        return None
    try:
        return date.fromisoformat(exp_str[:10])
    except ValueError:
        return None


def _underlying_from_option_symbol(sym: str) -> Optional[str]:
    """Extract the underlying root from a TradeStation option symbol.

    The ingest layer writes ``option_chains.option_symbol`` in the
    ``{root} {YYMMDD}{C|P}{strike}`` format — the token before the
    space is the option root (e.g., ``SPY``, ``SPXW``). For our
    single-underlying fleet this is also the underlying_quotes symbol.
    Index chains that trade under a weekly root would need a reverse
    OPTION_ROOT_ALIASES lookup, but nothing in the current bots hits
    that path — SPXW → SPX would go here when we add index bots.
    """
    if not isinstance(sym, str):
        return None
    root = sym.split(" ", 1)[0].strip()
    return root or None


def _latest_underlying_spot(conn: Any, underlying: str) -> Optional[float]:
    """Latest close price on ``underlying`` from ``underlying_quotes``.

    Used as the intrinsic-value anchor when an expired 0DTE option has
    no fillable option_chains quote. The underlying keeps ticking during
    after-hours even though the options market has closed, so this is
    the right price to settle against.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT close
        FROM underlying_quotes
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (underlying,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    try:
        return float(row[0])
    except (TypeError, ValueError):
        return None


def _leg_intrinsic(conn: Any, leg: Dict[str, Any], today: date) -> Optional[float]:
    """Per-share intrinsic value for an expired option leg.

    Returns ``None`` unless the leg is past (or on) its expiration
    date; refuses to compute intrinsic for options still trading, so
    the caller cannot accidentally short-circuit a live quote path
    with an ITM number that ignores time value.

    For a long call: ``max(0, spot - strike)``.
    For a long put:  ``max(0, strike - spot)``.
    Short positions negate the intrinsic (they OWE the settlement value
    to whoever exercises).
    """
    exp = _parse_leg_expiration(leg)
    if exp is None or exp > today:
        return None
    sym = leg.get("option_symbol")
    if not isinstance(sym, str):
        return None
    underlying = _underlying_from_option_symbol(sym)
    if underlying is None:
        return None
    spot = _latest_underlying_spot(conn, underlying)
    if spot is None:
        return None
    try:
        strike = float(leg.get("strike"))
    except (TypeError, ValueError):
        return None
    opt_type = (leg.get("option_type") or "").lower()
    if opt_type.startswith("c"):
        intrinsic = max(0.0, spot - strike)
    elif opt_type.startswith("p"):
        intrinsic = max(0.0, strike - spot)
    else:
        return None
    side = (leg.get("side") or "").lower()
    if side == "short":
        intrinsic = -intrinsic
    return intrinsic


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

    Expired-option fallback: on ``action == 'close'``, when a leg's
    option_chains row is missing or stale AND the leg's expiration date
    is on or before today, the per-leg contribution falls back to the
    intrinsic value ``max(0, spot - strike)`` (or ``max(0, strike -
    spot)`` for puts, negated for shorts). This keeps mark_position and
    close_position honest after 4 pm ET on expiration day when the
    options market is shut but the underlying is still ticking — the
    trade should book its cash-settlement P&L, not sit as an open
    position indefinitely with a stale mark. Never applies to
    ``action == 'open'``: we don't enter positions off intrinsic.
    """
    slip = tw_config.EXECUTION_SLIPPAGE_PCT if slippage_pct is None else slippage_pct
    long_sum = 0.0
    short_sum = 0.0
    fallback_today = date.today()
    for leg in legs:
        sym = leg.get("option_symbol")
        if not sym:
            return None
        side = (leg.get("side") or "").lower()
        if side not in {"long", "short"}:
            return None
        quote = _latest_quote(conn, sym)
        if quote is None:
            if action == "close":
                # _leg_intrinsic already applies the short-side sign, so it
                # returns the leg's per-share P&L contribution directly (long
                # gets +max(0, ...), short gets -max(0, ...)). Feed that back
                # through the same long_sum − short_sum equation the quote
                # path uses so the return-value convention stays consistent.
                intrinsic = _leg_intrinsic(conn, leg, fallback_today)
                if intrinsic is not None:
                    logger.info(
                        "spread_price: intrinsic fallback for expired leg %s = %.4f",
                        sym, intrinsic,
                    )
                    if side == "long":
                        long_sum += intrinsic
                    else:
                        # intrinsic is already negative for short legs;
                        # subtracting a negative through the return statement
                        # ((-) - short_sum) accumulates the correct debit.
                        short_sum += -intrinsic
                    continue
            return None
        bid, ask, last = quote
        price = leg_fill_price(
            bid=bid, ask=ask, last=last, side=side, action=action, slippage_pct=slip
        )
        if side == "long":
            long_sum += price
        else:
            short_sum += price
    return long_sum - short_sum
