"""What each tradeable symbol is, in the three senses this study needs.

A symbol here is three different things at once, and conflating them is the
main way an ES or NQ result goes quietly wrong:

* **A price axis** — the bars the opening range and every touch are measured
  on.  For ES/NQ that is the future's own feed (``futures_quotes``), because
  that is the tape the trader is watching.
* **An option book** — whose dealer positioning the gamma levels describe.
  ZeroGEX computes gamma from INDEX chains and never from options on futures
  (``src/jobs/futures_projection.py``), so NQ's book is NDX's.
* **A contract** — tick size and dollar point value, which only matter once
  Phase 4 prices a simulated trade.

The bar-source routing is not restated here: it is
:data:`research.msi_regime_excursion.sources.INSTRUMENTS`, which already maps
all six symbols and already encodes the futures/cash split.  This module adds
only the two axes that table does not carry — the option book, and the
contract spec — and re-exports the rest.

Micros (MNQ/MES) are deliberately NOT added to ``src/symbols.py``.  They share
their parent's price series exactly and differ only in multiplier, so they are
a research-side contract spec.  Editing the production symbol map to add them
would change live display behaviour for a research convenience.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from research.msi_regime_excursion.sources import INSTRUMENTS as BAR_INSTRUMENTS
from research.msi_regime_excursion.sources import Instrument as BarInstrument
from src.symbols import resolve_futures_tick

__all__ = [
    "InstrumentSpec",
    "SPECS",
    "DEFAULT_SYMBOLS",
    "spec",
    "known_symbols",
]


@dataclass(frozen=True)
class InstrumentSpec:
    """One research instrument: price axis + option book + contract."""

    key: str
    #: Symbol whose ``gex_summary`` / ``gex_by_strike`` rows carry the gamma.
    gamma_symbol: str
    #: ``cash`` -> ``underlying_quotes.symbol``; ``futures`` -> ``futures_quotes.index_symbol``.
    bar_source: str
    #: The value to filter the bar table on.  ``futures_quotes`` is keyed by
    #: the CASH index, so NQ's bar_symbol is ``NDX``.
    bar_symbol: str
    #: Minimum price increment.  ``None`` for a cash index, which has no tick.
    tick: Optional[float]
    #: Dollars per index point per contract.  Only futures have one; a cash
    #: symbol's P&L is not modelled in this study.
    point_value: Optional[float] = None
    #: True when gamma levels must be carried onto this symbol's price axis by
    #: the production basis ratio before any distance is computed.
    needs_basis: bool = False

    @property
    def is_futures(self) -> bool:
        return self.bar_source == "futures"

    @property
    def borrows_book(self) -> bool:
        """True when the gamma comes from a different symbol's option chain."""
        return self.gamma_symbol != self.key

    def to_dict(self) -> dict:
        return {
            "symbol": self.key,
            "gamma_symbol": self.gamma_symbol,
            "bar_source": self.bar_source,
            "bar_symbol": self.bar_symbol,
            "tick": self.tick,
            "point_value": self.point_value,
            "needs_basis": self.needs_basis,
        }


def _from_bar_instrument(
    inst: BarInstrument,
    *,
    tick: Optional[float],
    point_value: Optional[float] = None,
) -> InstrumentSpec:
    """Lift an msi_regime_excursion instrument, keeping its bar routing."""
    return InstrumentSpec(
        key=inst.key,
        # That table's ``score_symbol`` IS the option book for every row it
        # holds: the MSI is computed from the same chain the gamma is.
        gamma_symbol=inst.score_symbol,
        bar_source=inst.bar_source,
        bar_symbol=inst.bar_symbol,
        tick=tick,
        point_value=point_value,
        needs_basis=inst.bar_source == "futures",
    )


#: Ticks: ETFs quote in pennies; cash indices have no tradeable tick at all
#: (``None``, so the touch band falls back to the basis-point rule alone);
#: futures come from production's own table.
_TICKS: dict[str, Optional[float]] = {
    "SPY": 0.01,
    "QQQ": 0.01,
    "SPX": None,
    "NDX": None,
    "ES": resolve_futures_tick("ES"),
    "NQ": resolve_futures_tick("NQ"),
}

#: CME dollar value of one index point, per contract.
_POINT_VALUES: dict[str, float] = {"ES": 50.0, "NQ": 20.0, "MES": 5.0, "MNQ": 2.0}

SPECS: dict[str, InstrumentSpec] = {
    key: _from_bar_instrument(inst, tick=_TICKS.get(key), point_value=_POINT_VALUES.get(key))
    for key, inst in BAR_INSTRUMENTS.items()
}

# Micros: same tape and same book as the parent, different multiplier only.
for _micro, _parent in (("MNQ", "NQ"), ("MES", "ES")):
    _p = SPECS[_parent]
    SPECS[_micro] = InstrumentSpec(
        key=_micro,
        gamma_symbol=_p.gamma_symbol,
        bar_source=_p.bar_source,
        bar_symbol=_p.bar_symbol,
        tick=_p.tick,
        point_value=_POINT_VALUES[_micro],
        needs_basis=_p.needs_basis,
    )

#: The six symbols the study runs over by default — the product's own picker
#: set (``zerogex-web/frontend/core/symbols.ts``).
DEFAULT_SYMBOLS: tuple[str, ...] = ("SPY", "QQQ", "SPX", "NDX", "ES", "NQ")


def known_symbols() -> list[str]:
    return sorted(SPECS)


def spec(symbol: str) -> InstrumentSpec:
    try:
        return SPECS[(symbol or "").strip().upper()]
    except KeyError:
        raise SystemExit(
            f"unknown symbol {symbol!r}; known: {', '.join(known_symbols())}"
        ) from None
