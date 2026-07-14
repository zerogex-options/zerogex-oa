"""Symbol-aware option strike-grid helpers.

Listed option strikes are not uniformly $1-wide across underlyings. ETFs
like SPY / QQQ / IWM list ~$1 strikes near the money, while cash indices
(SPX, NDX, RUT, DJX) list a coarser grid — SPX prints strikes every 5
points. The fleet historically picked its strike with a bare
``round(snap.spot)``, which is fine on a $1 grid but produces a
non-existent strike on SPX: ``round(7529.4) == 7529`` is not a listed SPX
contract, so the synthetic ``option_symbol`` matched no ``option_chains``
row, ``spread_price`` returned ``None``, and the trade silently never
filled.

``nearest_strike`` snaps a price to the symbol's actual grid so the built
leg maps to a real, quotable contract. The increment is resolved per
symbol (each judged on its own terms):

* ``TRADEWORKZ_STRIKE_INCREMENTS`` env override — ``SYM=inc`` pairs, e.g.
  ``SPX=5,NDX=25`` — wins when present, so an operator can pin an unusual
  grid without a code change;
* otherwise cash indices default to a 5-point grid and everything else to
  a $1 grid.

For a $1 grid ``nearest_strike(x, 1.0)`` is exactly ``round(x)`` (same
round-half-to-even), so swapping the fleet's ``round(snap.spot)`` for this
helper is behaviour-preserving for every ETF the fleet trades today and
only changes what an index symbol would do.
"""

from __future__ import annotations

import logging
import os
from typing import Dict

from src.symbols import is_cash_index

logger = logging.getLogger(__name__)

_DEFAULT_ETF_INCREMENT = 1.0
_DEFAULT_INDEX_INCREMENT = 5.0


def _configured_increments() -> Dict[str, float]:
    """Parse ``TRADEWORKZ_STRIKE_INCREMENTS`` (``SYM=inc,SYM2=inc2``)."""
    raw = os.getenv("TRADEWORKZ_STRIKE_INCREMENTS", "")
    out: Dict[str, float] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        sym, inc = pair.split("=", 1)
        try:
            value = float(inc)
        except (TypeError, ValueError):
            continue
        if value > 0:
            out[sym.strip().upper()] = value
    return out


def default_strike_increment(underlying: str) -> float:
    """Resolve the listed strike increment for ``underlying``.

    Env override first, then instrument type: cash index -> 5, else -> 1.
    """
    sym = (underlying or "").strip().upper()
    override = _configured_increments().get(sym)
    if override:
        return override
    return _DEFAULT_INDEX_INCREMENT if is_cash_index(sym) else _DEFAULT_ETF_INCREMENT


def nearest_strike(price: float, increment: float) -> float:
    """Round ``price`` to the nearest multiple of ``increment``.

    A non-positive / missing ``increment`` falls back to a $1 grid so the
    caller can never divide by zero. On a $1 grid the result equals
    ``round(price)`` exactly.
    """
    if not increment or increment <= 0:
        increment = _DEFAULT_ETF_INCREMENT
    steps = round(price / increment)
    return round(steps * increment, 4)
