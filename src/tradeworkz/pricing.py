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
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from src.signals.execution import leg_fill_price
from src.tradeworkz import config as tw_config

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_REGULAR_CLOSE_HHMM = time(16, 0)  # SPY regular-session settlement time


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
    """Resolve the ``underlying_quotes`` symbol from a TS option symbol.

    The ingest layer writes ``option_chains.option_symbol`` as
    ``{root} {YYMMDD}{C|P}{strike}`` — the token before the space is the
    option root (``SPY``, ``SPXW``). For settlement we need the symbol the
    UNDERLYING price is stored under, which for an index differs from the
    option root: SPX's 0DTE options carry root ``SPXW`` but settle against
    the ``SPX`` cash-index close. ``resolve_underlying_from_option_root``
    reverses the SYMBOL_ALIASES -> OPTION_ROOT_ALIASES chain so the
    intrinsic fallback looks up ``underlying_quotes`` under ``SPX``; ETF
    roots (already their own underlying) pass through unchanged.
    """
    if not isinstance(sym, str):
        return None
    root = sym.split(" ", 1)[0].strip()
    if not root:
        return None
    from src.symbols import resolve_underlying_from_option_root

    return resolve_underlying_from_option_root(root)


def _settlement_spot(
    conn: Any, underlying: str, expiration: date
) -> Optional[float]:
    """Underlying spot at the regular-session close of ``expiration``.

    SPY options are American, physically-settled, and settle on the
    4:00 PM ET equity close of their expiration day. That means an
    option that expires today with SPY closing at $745.30 settles
    against $745.30 — the after-hours drift down to $743.55 is
    irrelevant to how the option is priced at settlement.

    We therefore query for the last ``underlying_quotes`` tick strictly
    BEFORE 16:00 ET on the expiration date. That excludes after-hours
    ticks that came in later. If no such row exists (fresh DB, or the
    expiration is somehow in the future), returns ``None`` so the
    caller fails closed instead of intrinsic-settling against a wrong
    price.

    ``zoneinfo`` handles DST — 16:00 ET is UTC-4 in EDT (20:00 UTC)
    and UTC-5 in EST (21:00 UTC).
    """
    close_et = datetime.combine(expiration, _REGULAR_CLOSE_HHMM, tzinfo=_ET)
    close_utc = close_et.astimezone(timezone.utc)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT close
        FROM underlying_quotes
        WHERE symbol = %s
          AND timestamp < %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (underlying, close_utc),
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

    The settlement anchor is the underlying's REGULAR-SESSION close
    price on the leg's expiration date — not the latest tick. This
    matters when the engine settles an expired 0DTE after 4 PM ET
    while the equity keeps drifting in after-hours: we must use the
    16:00 ET print, not the current AH print, or we'd over-book
    profit on a put that had already expired OTM at the actual close.

    For a long call: ``max(0, close_spot - strike)``.
    For a long put:  ``max(0, strike - close_spot)``.
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
    spot = _settlement_spot(conn, underlying, exp)
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
