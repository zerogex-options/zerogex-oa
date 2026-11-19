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
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from src.signals.execution import leg_fill_price
from src.tradeworkz import config as tw_config

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


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

    On an early-close day (``NYSE_HALF_DAYS``) the close is 13:00 ET, and
    SPY/QQQ keep printing in the after-hours session from then on — a
    16:00 cutoff would settle against an after-hours print, not the close.
    """
    from src.market_calendar import regular_session_close

    close_et = datetime.combine(expiration, regular_session_close(expiration), tzinfo=_ET)
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

    Fill model: every quoted leg fills at its natural side (long pays the
    ask, short collects the bid on open; the reverse on close), and the
    slippage is then charged once on the structure's NET price — see
    :func:`charge_net_slippage`. A single option prices exactly as it always
    has; a spread no longer pays slippage on each leg's full price.
    """
    slip = tw_config.EXECUTION_SLIPPAGE_PCT if slippage_pct is None else slippage_pct
    quoted_net = 0.0
    intrinsic_net = 0.0
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
                # IS the leg's per-share contribution to the net (long gets
                # +max(0, ...), short gets -max(0, ...)). Settlement is exact:
                # no market is crossed, so no slippage is charged on it.
                intrinsic = _leg_intrinsic(conn, leg, fallback_today)
                if intrinsic is not None:
                    logger.info(
                        "spread_price: intrinsic fallback for expired leg %s = %.4f",
                        sym, intrinsic,
                    )
                    intrinsic_net += intrinsic
                    continue
            return None
        bid, ask, last = quote
        price = leg_fill_price(
            bid=bid, ask=ask, last=last, side=side, action=action, slippage_pct=0.0
        )
        quoted_net += price if side == "long" else -price
    return charge_net_slippage(quoted_net, action, slip) + intrinsic_net


def charge_net_slippage(natural_net: float, action: str, slippage_pct: float) -> float:
    """Charge execution slippage on a structure's NET price, against the trader.

    ``natural_net`` is the signed net at the natural fills (positive = we pay,
    negative = we collect). On ``open`` we pay ``slippage_pct`` x |net| more
    (or collect that much less); on ``close`` we collect that much less (or
    pay that much more).

    Why the net and not each leg (changed 2026-11-18): slippage used to be
    charged on every leg's FULL price. That is mild for one option, but a
    spread on an expensive underlying paid it on both legs' whole premiums —
    an SPX vertical with $20 and $14 legs paid ~$0.70 a side on a ~$6.50
    debit. Together with crossing both bid/asks it opened ~34% under water,
    and the forward test's four SPX premium stops all fired within a minute
    at -43% to -46%: the stop was measuring the fill model, not the market.
    Charged on the net, that spread opens ~17% under water. For a single
    option the net IS the leg's price, so every single-leg fill is unchanged.
    """
    adj = max(0.0, float(slippage_pct or 0.0)) * abs(natural_net)
    return natural_net + adj if action == "open" else natural_net - adj


def entry_cost_fraction(open_net: Optional[float], close_now: Optional[float]) -> Optional[float]:
    """Share of the premium lost the instant a position opens, or None.

    ``open_net`` is what the structure costs to open and ``close_now`` what it
    could be closed for at the same moment, both signed as :func:`spread_price`
    returns them. ``open_net - close_now`` is the round trip through the bid/ask
    and slippage — positive for debits and credits alike — taken as a share of
    the premium paid or collected. None when either side is unpriced or the
    open is zero.
    """
    if open_net is None or close_now is None or not open_net:
        return None
    return (float(open_net) - float(close_now)) / abs(float(open_net))
