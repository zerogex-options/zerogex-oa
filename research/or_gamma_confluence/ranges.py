"""The opening range and the extension ladder.  Pure — no database, no clock.

Three things here are easy to get wrong and are therefore stated explicitly.

**Bar timestamps are period-START.**  TradeStation close-stamps a 1-minute bar
at its end boundary; the ingester re-buckets to the minute the bar actually
covers (``src/ingestion/main_engine.py:530-541``, and the same one-line
transform in ``src/ingestion/futures_underlying_ingester.py:116``).  So the
09:30 bar is stamped 09:30 and a 5-minute opening range is the bars stamped
09:30..09:34 — the half-open window ``[start, start + N minutes)``.  Reading
these as close-stamps would fold the 09:29 bar into the range and drop the
09:34 one.

**The range freezes.**  ``ORH``/``ORL``/``R`` are computed once, at the close of
the OR window, from bars inside it.  Nothing recomputes them later.  That is
the property ``tests/test_or_gamma_confluence.py`` pins first, because a
"live" opening range is a look-ahead bug that reads as a strong result.

**The cash-index 09:30 bar can be a phantom.**  SPX and NDX are computed from
constituents that have not opened at 09:30:00, so the index's opening print is
the stale opening-rotation value (≈ the prior close), and TradeStation folds it
into the bar's OHLC — painting a full-range candle from the stale value to the
real level.  On a 5-minute opening range that single bar is 20% of the sample
and frequently sets ``ORH`` or ``ORL`` outright, which would put the entire
extension ladder in the wrong place.  ``src/tools/cash_index_open_repair.py``
is production's rule for this; :func:`dephantom_open_bar` calls it rather than
restating it, so research and production cannot disagree about what a phantom
is.  The live ingester repairs new rows in place, but history stored before
that fix is only clean if ``make cash-index-open-repair`` has been run — which
is why this is applied defensively at read time rather than assumed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

from research.msi_regime_excursion.excursion import ET, Bar
from research.or_gamma_confluence.config import MODE_BOUNDARY, MODE_OPEN, ResearchConfig
from src.symbols import is_cash_index
from src.tools.cash_index_open_repair import reconstruct_session_open

__all__ = [
    "OpeningRange",
    "Rung",
    "ExtensionLadder",
    "SIDE_UP",
    "SIDE_DOWN",
    "session_window",
    "dephantom_open_bar",
    "build_opening_range",
    "build_ladder",
    "format_rung_label",
]

SIDE_UP = "up"
SIDE_DOWN = "down"


def session_window(session: date, cfg: ResearchConfig) -> tuple[datetime, datetime]:
    """The cash session as tz-aware ET instants ``[start, end]``."""
    return (
        datetime.combine(session, cfg.session_start, tzinfo=ET),
        datetime.combine(session, cfg.session_end, tzinfo=ET),
    )


def dephantom_open_bar(
    bars: Sequence[Bar], symbol: str, session_open: datetime
) -> tuple[list[Bar], bool]:
    """Rebuild the cash-index session-open bar if it is a phantom.

    Returns ``(bars, repaired)``.  A no-op — and ``repaired=False`` — for any
    non-cash-index symbol, for a missing open bar, and for an open bar that is
    not a phantom, so it is safe to call unconditionally.  The arithmetic is
    :func:`src.tools.cash_index_open_repair.reconstruct_session_open`, which is
    idempotent (after a repair ``open == close``, which the rule excludes).

    ``symbol`` is the BAR symbol, not the instrument key: ES/NQ bars come from
    the futures feed and are real traded prices, so they must not be routed
    through this even though their instrument borrows the NDX/SPX option book.
    """
    if not bars or not is_cash_index(symbol):
        return list(bars), False
    out = list(bars)
    for i, bar in enumerate(out):
        if bar.ts != session_open:
            continue
        rebuilt = reconstruct_session_open(bar.open, bar.high, bar.low, bar.close)
        if rebuilt is None:
            return out, False
        new_open, new_high, new_low = rebuilt
        out[i] = Bar(
            ts=bar.ts,
            open=float(new_open),
            high=float(new_high),
            low=float(new_low),
            close=bar.close,
        )
        return out, True
    return out, False


@dataclass(frozen=True)
class OpeningRange:
    """The frozen opening range for one session.

    Every field is derived from bars inside ``[start, end)`` and never changes
    afterwards.  ``width`` is ``R`` — the unit every extension is measured in.
    """

    symbol: str
    session: date
    #: First instant of the OR window (inclusive).
    start: datetime
    #: First instant AFTER the OR window (exclusive) — the moment the range
    #: freezes and the ladder becomes tradeable.
    end: datetime
    high: float
    low: float
    #: Session opening price: the ``open`` of the first bar in the window.
    #: Mode B anchors on this.
    open_price: float
    n_bars: int
    #: True when the cash-index open-bar phantom repair fired for this session.
    open_bar_repaired: bool = False

    @property
    def width(self) -> float:
        """``R`` — the opening-range width in the instrument's price units."""
        return self.high - self.low

    @property
    def width_bp(self) -> float:
        base = self.open_price if self.open_price > 0 else self.high
        return 10_000.0 * self.width / base if base > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "session": self.session.isoformat(),
            "or_start": self.start.isoformat(),
            "or_end": self.end.isoformat(),
            "or_high": self.high,
            "or_low": self.low,
            "or_width": self.width,
            "or_width_bp": self.width_bp,
            "session_open": self.open_price,
            "or_bars": self.n_bars,
            "or_open_bar_repaired": self.open_bar_repaired,
        }


def format_rung_label(k: float) -> str:
    """``0.5 -> '+50'``, ``-3.0 -> '-300'`` — the brief's percent notation."""
    pct = k * 100.0
    body = f"{abs(pct):g}"
    return f"{'+' if k >= 0 else '-'}{body}"


@dataclass(frozen=True)
class Rung:
    """One rung of the extension ladder."""

    #: Signed step count.  ``+1`` is the first rung above, ``-1`` the first
    #: below.  ``0`` is never a rung — see :class:`ExtensionLadder`.
    index: int
    #: Signed multiple of ``R``.  ``index * cfg.extension_step``.
    k: float
    label: str
    side: str  # SIDE_UP | SIDE_DOWN
    price: float

    @property
    def depth(self) -> float:
        """Unsigned distance from the anchor, in units of ``R``."""
        return abs(self.k)


class ExtensionLadder:
    """The frozen price ladder for one session, with neighbour lookup.

    The **anchor** (``index == 0``) is a price, not an event.  It exists so
    rung ``±1`` has a "previous extension" to revert to, and it is never itself
    a touch event — the brief's ladder starts at ``±50``.  Making it touchable
    would require inventing a "previous" for it (the opposite boundary? the
    open?), and no reading of that is more defensible than the others.

    In :data:`~research.or_gamma_confluence.config.MODE_BOUNDARY` the two sides
    have DIFFERENT anchors (``ORH`` above, ``ORL`` below), so the ladder is not
    symmetric about a point and the two sides are separated by the range
    itself.  In :data:`~research.or_gamma_confluence.config.MODE_OPEN` both
    sides anchor on the session open, so ``+1`` and ``-1`` sit one step either
    side of a single price.
    """

    __slots__ = ("orange", "cfg", "anchor_up", "anchor_down", "_by_index", "_rungs")

    def __init__(self, orange: OpeningRange, cfg: ResearchConfig) -> None:
        self.orange = orange
        self.cfg = cfg
        if cfg.extension_mode == MODE_BOUNDARY:
            self.anchor_up = orange.high
            self.anchor_down = orange.low
        elif cfg.extension_mode == MODE_OPEN:
            self.anchor_up = orange.open_price
            self.anchor_down = orange.open_price
        else:  # pragma: no cover - validated in ResearchConfig
            raise ValueError(f"unknown extension_mode {cfg.extension_mode!r}")

        r = orange.width
        rungs: list[Rung] = []
        for i, k in enumerate(cfg.rungs(), start=1):
            rungs.append(
                Rung(
                    index=i,
                    k=k,
                    label=format_rung_label(k),
                    side=SIDE_UP,
                    price=self.anchor_up + k * r,
                )
            )
            rungs.append(
                Rung(
                    index=-i,
                    k=-k,
                    label=format_rung_label(-k),
                    side=SIDE_DOWN,
                    price=self.anchor_down - k * r,
                )
            )
        rungs.sort(key=lambda x: x.price)
        self._rungs: tuple[Rung, ...] = tuple(rungs)
        self._by_index: dict[int, Rung] = {x.index: x for x in rungs}

    # ── Access ───────────────────────────────────────────────────────

    @property
    def rungs(self) -> tuple[Rung, ...]:
        """Every rung, ascending by price.  Excludes the anchor(s)."""
        return self._rungs

    def __len__(self) -> int:
        return len(self._rungs)

    def at(self, index: int) -> Optional[Rung]:
        return self._by_index.get(index)

    def anchor_for(self, side: str) -> float:
        return self.anchor_up if side == SIDE_UP else self.anchor_down

    # ── Neighbours: the dependent variable's whole definition ────────

    def previous_price(self, rung: Rung) -> float:
        """The rung one step CLOSER to the anchor; the anchor itself at ``±1``.

        This is the reversion target: "does −250 trade before −350" when the
        touch is at −300.
        """
        inner = self._by_index.get(rung.index - (1 if rung.index > 0 else -1))
        return inner.price if inner is not None else self.anchor_for(rung.side)

    def next_price(self, rung: Rung) -> Optional[float]:
        """The rung one step FURTHER from the anchor, or ``None`` past the end.

        ``None`` means the ladder ran out, not that price cannot go further.
        An event whose ``next`` is ``None`` can only resolve as reversion or
        censored, and must be recorded as such rather than counted as a
        reversion by default.
        """
        outer = self._by_index.get(rung.index + (1 if rung.index > 0 else -1))
        return outer.price if outer is not None else None

    def previous(self, rung: Rung) -> Optional[Rung]:
        return self._by_index.get(rung.index - (1 if rung.index > 0 else -1))

    def next(self, rung: Rung) -> Optional[Rung]:
        return self._by_index.get(rung.index + (1 if rung.index > 0 else -1))


def build_opening_range(
    bars: Sequence[Bar],
    session: date,
    symbol: str,
    cfg: ResearchConfig,
) -> tuple[Optional[OpeningRange], Optional[str]]:
    """Freeze the opening range for one session.

    Returns ``(opening_range, None)`` on success, or ``(None, reason)`` — the
    reason string is carried into the run's ``.meta.json`` so a thin sample
    reads as a coverage problem rather than as an absence of events.

    ``bars`` may span the whole session; only ``[start, start + N)`` is used.
    The phantom repair is applied here rather than by the caller so that no
    path can build a range from an unrepaired cash-index open bar.
    """
    start, _ = session_window(session, cfg)
    end = start + timedelta(minutes=cfg.opening_range_minutes)

    repaired_bars, repaired = dephantom_open_bar(bars, symbol, start)
    window = [b for b in repaired_bars if start <= b.ts < end]
    if not window:
        return None, "no_bars_in_or_window"
    if len(window) < cfg.min_or_bars:
        return None, f"or_bars_{len(window)}_below_min_{cfg.min_or_bars}"

    high = max(b.high for b in window)
    low = min(b.low for b in window)
    open_price = window[0].open
    if not (high > 0 and low > 0 and open_price > 0):
        return None, "non_positive_or_prices"
    if high < low:  # pragma: no cover - schema CHECK forbids it
        return None, "inverted_or"

    orange = OpeningRange(
        symbol=symbol,
        session=session,
        start=start,
        end=end,
        high=high,
        low=low,
        open_price=open_price,
        n_bars=len(window),
        open_bar_repaired=repaired,
    )
    if orange.width_bp < cfg.min_or_width_bp:
        return None, f"or_width_{orange.width_bp:.2f}bp_below_min_{cfg.min_or_width_bp}bp"
    return orange, None


def build_ladder(orange: OpeningRange, cfg: ResearchConfig) -> ExtensionLadder:
    """The frozen extension ladder for a frozen opening range."""
    return ExtensionLadder(orange, cfg)
