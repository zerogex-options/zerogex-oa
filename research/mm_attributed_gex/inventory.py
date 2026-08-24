"""Reconstruct Market Maker inventory per option series from classified flow.

The recursion
-------------
For one series, walking buckets in time order::

    MM_long [t] = MM_long [t-1] + MM_opening_buys  - MM_closing_sells
    MM_short[t] = MM_short[t-1] + MM_opening_sells - MM_closing_buys
    MM_net      = MM_long - MM_short

That is the model the experiment was specified with, and it is implemented
literally — but only after checking what it implies, because the check turns
out to matter.

**The net position is invariant to open/close misclassification.**  Expand it::

    net = (OB - CS) - (OS - CB) = (OB + CB) - (OS + CS) = all_buys - all_sells

The open/close designation cancels exactly.  This is worth stating plainly
because the position-effect flag is the *least* trustworthy field in an
Open-Close feed for the Market Maker category specifically: the designation
originates from the clearing position effect the originating firm marks, and
market makers do not mark it with the discipline a customer account does.
Gamma exposure is a function of ``net`` alone, so:

* ``mm_net_contracts`` — the number the GEX calculation consumes — depends only
  on correct **buy/sell** classification and on **complete history**.
* the ``long`` / ``short`` decomposition — reported for diagnostics — does
  depend on the open/close flag, and should be read with that in mind.

Both estimators are computed and compared (:attr:`MMPosition.net_flow_contracts`
vs :attr:`MMPosition.net_contracts`) so any divergence — which can only come
from records the open/close estimator had to drop, e.g. ``UNKNOWN`` effect
buckets — is visible rather than assumed away.

Left-censoring
--------------
If the Open-Close history starts after a contract was already trading, the
reconstructed level is missing an unknown pre-window position.  This is NOT
treated as zero.  Three things happen instead:

1. The series is flagged :attr:`MMPosition.left_censored`.
2. A *lower bound* on the missing inventory is derived from floor breaches: if
   the running long inventory reaches −50, at least 50 contracts of long
   inventory must have existed before the window opened.  The deepest breach
   over the series' life is the tightest such bound
   (:attr:`MMPosition.implied_prior_long` / ``implied_prior_short``).
3. :mod:`~.confidence` scores it, and the dataset builder can exclude it from
   the clean experiment or route it to a separately labelled partial-data arm.

Expiration
----------
SPX is European-style and cash-settled: no early assignment, so a series simply
ceases to exist at its settlement instant.  :meth:`InventoryBook.retire_expired`
drops series past settlement, using ZeroGEX's own settlement-aware calendar
(``SPX`` 3rd-Friday monthlies settle at the 09:30 ET SOQ; ``SPXW`` weeklies and
everything else at 16:00 ET).
"""

from __future__ import annotations

import copy
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from src.market_calendar import load_nyse_holidays, settlement_close_time_for_contract

from research.mm_attributed_gex.schema import (
    MARKET_MAKER_LIKE,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    SeriesKey,
    Side,
)

__all__ = [
    "MMPosition",
    "InventoryBook",
    "InventoryReconstructor",
    "ReconstructionReport",
    "settlement_instant",
    "expected_sessions",
]

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
    _ET_IS_PYTZ = False
except Exception:  # pragma: no cover - defensive
    import pytz

    ET = pytz.timezone("US/Eastern")  # type: ignore[assignment]
    _ET_IS_PYTZ = True


def _localize_et(naive: datetime) -> datetime:
    if _ET_IS_PYTZ:  # pragma: no cover - fallback path
        return ET.localize(naive)  # type: ignore[attr-defined]
    return naive.replace(tzinfo=ET)


def settlement_instant(
    symbol: str, expiration: date, option_root: Optional[str] = None
) -> datetime:
    """UTC instant at which a series settles and its inventory retires.

    Delegates the AM-vs-PM decision to ZeroGEX's production calendar so the
    experiment cannot drift from the settlement convention the live gamma
    engine uses.  ``option_root`` disambiguates a 3rd-Friday shared by SPX
    (AM-settled) and SPXW (PM-settled).
    """
    close_text = settlement_close_time_for_contract(symbol, option_root, expiration)
    hh, mm, ss = (int(part) for part in close_text.split(":"))
    return _localize_et(datetime.combine(expiration, time(hh, mm, ss))).astimezone(timezone.utc)


def expected_sessions(start: date, end: date) -> list[date]:
    """US equity trading sessions in ``[start, end]`` inclusive.

    Uses ZeroGEX's own NYSE holiday table so a "missing session" in the
    Open-Close history is measured against the same calendar the rest of the
    platform uses, not against a naive weekday count.
    """
    if end < start:
        return []
    holidays = load_nyse_holidays()
    out: list[date] = []
    cursor = start
    one = timedelta(days=1)
    while cursor <= end:
        if cursor.weekday() < 5 and cursor not in holidays:
            out.append(cursor)
        cursor += one
    return out


@dataclass
class MMPosition:
    """Reconstructed Market Maker inventory for one option series."""

    key: SeriesKey
    symbol: str
    expiration: date
    strike: float
    option_type: str
    option_root: Optional[str] = None

    # Running inventory (contracts).
    long_contracts: float = 0.0
    short_contracts: float = 0.0

    # Raw flow totals, kept so both estimators stay auditable.
    opening_buys: float = 0.0
    opening_sells: float = 0.0
    closing_buys: float = 0.0
    closing_sells: float = 0.0
    unknown_effect_buys: float = 0.0
    unknown_effect_sells: float = 0.0

    # Observation window and coverage.
    first_activity: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    first_activity_date: Optional[date] = None
    last_activity_date: Optional[date] = None
    active_sessions: set[date] = field(default_factory=set)
    buckets_observed: int = 0

    # Censoring evidence.
    long_floor_breaches: int = 0
    short_floor_breaches: int = 0
    implied_prior_long: float = 0.0
    implied_prior_short: float = 0.0

    # Set by the reconstructor once the whole stream has been consumed.
    left_censored: bool = True
    left_censor_reason: str = "not_evaluated"
    listing_date: Optional[date] = None
    missing_sessions: tuple[date, ...] = ()
    retired: bool = False

    # -- derived ------------------------------------------------------

    @property
    def net_contracts(self) -> float:
        """MM net position from the long/short recursion (open/close aware)."""
        return self.long_contracts - self.short_contracts

    @property
    def net_flow_contracts(self) -> float:
        """MM net position ignoring the open/close flag entirely.

        Equal to :attr:`net_contracts` whenever every record carried a known
        position effect (see the module docstring for the cancellation), so a
        difference isolates exactly the volume whose effect was ``UNKNOWN``.
        """
        buys = self.opening_buys + self.closing_buys + self.unknown_effect_buys
        sells = self.opening_sells + self.closing_sells + self.unknown_effect_sells
        return buys - sells

    @property
    def gross_contracts(self) -> float:
        return self.long_contracts + self.short_contracts

    @property
    def total_volume(self) -> float:
        return (
            self.opening_buys
            + self.opening_sells
            + self.closing_buys
            + self.closing_sells
            + self.unknown_effect_buys
            + self.unknown_effect_sells
        )

    @property
    def unknown_effect_share(self) -> float:
        total = self.total_volume
        if total <= 0:
            return 0.0
        return (self.unknown_effect_buys + self.unknown_effect_sells) / total

    @property
    def estimator_disagreement(self) -> float:
        """|net − net_flow| in contracts.  Zero when effects are complete."""
        return abs(self.net_contracts - self.net_flow_contracts)

    def dte(self, as_of: date) -> int:
        return (self.expiration - as_of).days

    def snapshot(self) -> "MMPosition":
        """A detached copy with the running values frozen as they are now.

        The book holds ONE mutable position object per series and keeps
        updating it as the record stream advances.  Anything that captures a
        position for later — the replay in :mod:`~.dataset`, a caller holding
        a list across snapshots — must take a copy, or it ends up reading the
        inventory as of whenever it looked rather than as of the instant it
        asked about.  The mutable collections are copied too, so the snapshot
        is genuinely independent.
        """
        clone = copy.copy(self)
        clone.active_sessions = set(self.active_sessions)
        return clone

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "expiration": self.expiration.isoformat(),
            "strike": self.strike,
            "option_type": self.option_type,
            "option_root": self.option_root,
            "mm_long": self.long_contracts,
            "mm_short": self.short_contracts,
            "mm_net": self.net_contracts,
            "mm_net_flow": self.net_flow_contracts,
            "opening_buys": self.opening_buys,
            "opening_sells": self.opening_sells,
            "closing_buys": self.closing_buys,
            "closing_sells": self.closing_sells,
            "unknown_effect_share": self.unknown_effect_share,
            "first_activity_date": (
                self.first_activity_date.isoformat() if self.first_activity_date else None
            ),
            "last_activity_date": (
                self.last_activity_date.isoformat() if self.last_activity_date else None
            ),
            "active_sessions": len(self.active_sessions),
            "buckets_observed": self.buckets_observed,
            "left_censored": self.left_censored,
            "left_censor_reason": self.left_censor_reason,
            "implied_prior_long": self.implied_prior_long,
            "implied_prior_short": self.implied_prior_short,
            "missing_sessions": len(self.missing_sessions),
            "retired": self.retired,
        }


@dataclass
class ReconstructionReport:
    """Dataset-wide provenance for one reconstruction pass."""

    records_consumed: int = 0
    mm_records_consumed: int = 0
    series_tracked: int = 0
    observed_sessions: set[date] = field(default_factory=set)
    observed_buckets: dict[date, set[time]] = field(default_factory=lambda: defaultdict(set))
    window_start: Optional[date] = None
    window_end: Optional[date] = None
    session_gaps: tuple[date, ...] = ()
    participants_seen: set[str] = field(default_factory=set)
    unknown_effect_contracts: float = 0.0
    mm_contracts: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "records_consumed": self.records_consumed,
            "mm_records_consumed": self.mm_records_consumed,
            "series_tracked": self.series_tracked,
            "observed_sessions": len(self.observed_sessions),
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None,
            "session_gaps": [d.isoformat() for d in self.session_gaps],
            "session_gap_count": len(self.session_gaps),
            "participants_seen": sorted(self.participants_seen),
            "mm_contracts": self.mm_contracts,
            "unknown_effect_contracts": self.unknown_effect_contracts,
            "buckets_per_session_median": self._median_buckets(),
        }

    def _median_buckets(self) -> Optional[float]:
        counts = sorted(len(v) for v in self.observed_buckets.values())
        if not counts:
            return None
        mid = len(counts) // 2
        if len(counts) % 2:
            return float(counts[mid])
        return (counts[mid - 1] + counts[mid]) / 2.0


class InventoryBook:
    """Mutable collection of :class:`MMPosition` keyed by series.

    Plain dict lookups on a tuple key: one pass over the record stream is
    ``O(records)`` with no per-series rescanning, which is what lets a
    full-chain 1-minute day fold in a single sweep.
    """

    __slots__ = ("_positions", "symbol")

    def __init__(self, symbol: str = "SPX") -> None:
        self.symbol = symbol
        self._positions: dict[SeriesKey, MMPosition] = {}

    def __len__(self) -> int:
        return len(self._positions)

    def __iter__(self) -> Iterator[MMPosition]:
        return iter(self._positions.values())

    def __contains__(self, key: SeriesKey) -> bool:
        return key in self._positions

    def get(self, key: SeriesKey) -> Optional[MMPosition]:
        return self._positions.get(key)

    def positions(self) -> list[MMPosition]:
        return list(self._positions.values())

    def ensure(self, rec: ParticipantActivity) -> MMPosition:
        key = rec.key
        pos = self._positions.get(key)
        if pos is None:
            pos = MMPosition(
                key=key,
                symbol=rec.symbol,
                expiration=rec.expiration,
                strike=float(rec.strike),
                option_type=rec.option_type,
                option_root=rec.option_root,
            )
            self._positions[key] = pos
        elif pos.option_root is None and rec.option_root:
            pos.option_root = rec.option_root
        return pos

    def live_positions(
        self,
        as_of: datetime,
        *,
        include_zero: bool = False,
    ) -> list[MMPosition]:
        """Positions whose series has not yet settled at ``as_of``.

        ``include_zero`` keeps series whose net is exactly zero; the default
        drops them because a zero net contributes nothing to gamma and only
        inflates the contract counts in the diagnostics.
        """
        out: list[MMPosition] = []
        for pos in self._positions.values():
            if as_of >= settlement_instant(pos.symbol, pos.expiration, pos.option_root):
                continue
            if not include_zero and pos.net_contracts == 0.0:
                continue
            out.append(pos)
        return out

    def retire_expired(self, as_of: datetime) -> int:
        """Mark and drop series that have settled.  Returns how many.

        SPX cash settlement means there is no residual position to model after
        the settlement instant — the contract is gone, not assigned into stock.
        """
        expired = [
            key
            for key, pos in self._positions.items()
            if as_of >= settlement_instant(pos.symbol, pos.expiration, pos.option_root)
        ]
        for key in expired:
            self._positions[key].retired = True
            del self._positions[key]
        return len(expired)


class InventoryReconstructor:
    """Folds a stream of :class:`ParticipantActivity` into an inventory book.

    Single-pass and streaming: records are consumed in the order given, which
    must be chronological.  The reconstructor deliberately does *not* sort —
    sorting would require materializing the entire stream, and the loader
    already yields files in chronological order.  :meth:`finalize` performs
    the after-the-fact classification (left-censoring, session gaps) that can
    only be decided once the whole window is known.

    ``participants`` selects which categories count as "Market Maker".  The
    default is the strict :data:`~research.mm_attributed_gex.schema.MARKET_MAKER_LIKE`
    (Market Maker only).  Widening it is a labelled sensitivity arm, never the
    headline result.
    """

    def __init__(
        self,
        symbol: str = "SPX",
        *,
        participants: Sequence[ParticipantType] = MARKET_MAKER_LIKE,
        listing_dates: Optional[Mapping[SeriesKey, date]] = None,
        censor_buffer_sessions: int = 1,
    ) -> None:
        self.symbol = symbol
        self.participants = frozenset(participants)
        self.listing_dates = dict(listing_dates or {})
        #: A series first seen on the very first session of the window is
        #: assumed censored — it was plausibly already trading.  Requiring the
        #: first activity to fall at least this many sessions inside the
        #: window is the conservative reading.
        self.censor_buffer_sessions = max(0, int(censor_buffer_sessions))
        self.book = InventoryBook(symbol=symbol)
        self.report = ReconstructionReport()
        self._finalized = False

    # -- ingestion ----------------------------------------------------

    def consume(self, records: Iterable[ParticipantActivity]) -> "InventoryReconstructor":
        """Fold records into the book.  Chronological order is the caller's job."""
        if self._finalized:
            raise RuntimeError("consume() after finalize(); build a new reconstructor")
        rep = self.report
        wanted = self.participants
        for rec in records:
            rep.records_consumed += 1
            rep.participants_seen.add(rec.participant.value)
            rep.observed_sessions.add(rec.trading_date)
            rep.observed_buckets[rec.trading_date].add(rec.timestamp.astimezone(ET).time())
            if rep.window_start is None or rec.trading_date < rep.window_start:
                rep.window_start = rec.trading_date
            if rep.window_end is None or rec.trading_date > rep.window_end:
                rep.window_end = rec.trading_date
            if rec.position_effect is PositionEffect.UNKNOWN:
                rep.unknown_effect_contracts += rec.contracts

            if rec.participant not in wanted:
                continue
            rep.mm_records_consumed += 1
            rep.mm_contracts += rec.contracts
            self._apply(rec)
        return self

    def _apply(self, rec: ParticipantActivity) -> None:
        pos = self.book.ensure(rec)
        qty = float(rec.contracts)

        if pos.first_activity is None or rec.timestamp < pos.first_activity:
            pos.first_activity = rec.timestamp
            pos.first_activity_date = rec.trading_date
        if pos.last_activity is None or rec.timestamp > pos.last_activity:
            pos.last_activity = rec.timestamp
            pos.last_activity_date = rec.trading_date
        pos.active_sessions.add(rec.trading_date)
        pos.buckets_observed += 1

        effect = rec.position_effect
        if effect is PositionEffect.OPEN:
            if rec.side is Side.BUY:
                pos.opening_buys += qty
                pos.long_contracts += qty
            else:
                pos.opening_sells += qty
                pos.short_contracts += qty
        elif effect is PositionEffect.CLOSE:
            if rec.side is Side.BUY:
                # Buying to close retires an existing SHORT.
                pos.closing_buys += qty
                pos.short_contracts -= qty
                if pos.short_contracts < 0.0:
                    pos.short_floor_breaches += 1
                    pos.implied_prior_short = max(pos.implied_prior_short, -pos.short_contracts)
            else:
                # Selling to close retires an existing LONG.
                pos.closing_sells += qty
                pos.long_contracts -= qty
                if pos.long_contracts < 0.0:
                    pos.long_floor_breaches += 1
                    pos.implied_prior_long = max(pos.implied_prior_long, -pos.long_contracts)
        else:
            # UNKNOWN effect: recorded so net_flow_contracts stays complete,
            # but NOT applied to the long/short recursion — attributing it to
            # either leg would invent a decomposition the data does not
            # support.  net_contracts and net_flow_contracts then differ by
            # exactly this volume, which is the signal that the feed's
            # position-effect coverage is incomplete.
            if rec.side is Side.BUY:
                pos.unknown_effect_buys += qty
            else:
                pos.unknown_effect_sells += qty

    # -- finalization -------------------------------------------------

    def finalize(self) -> "InventoryReconstructor":
        """Classify censoring and gaps once the full window is known."""
        rep = self.report
        rep.series_tracked = len(self.book)

        if rep.window_start and rep.window_end:
            observed = rep.observed_sessions
            rep.session_gaps = tuple(
                d for d in expected_sessions(rep.window_start, rep.window_end) if d not in observed
            )

        sessions = sorted(rep.observed_sessions)
        # A series is only credibly complete if its first activity falls far
        # enough inside the window that the window itself is not the reason we
        # never saw earlier trades.  A window with no session strictly after
        # the buffer establishes nothing at all — every series in it could
        # have been trading before the data starts — so the cutoff is None and
        # everything is censored unless an explicit listing date says otherwise.
        cutoff = (
            sessions[self.censor_buffer_sessions]
            if self.censor_buffer_sessions < len(sessions)
            else None
        )

        gap_set = set(rep.session_gaps)
        for pos in self.book:
            pos.listing_date = self.listing_dates.get(pos.key)
            pos.left_censored, pos.left_censor_reason = self._classify_censoring(pos, cutoff)
            if pos.first_activity_date and pos.last_activity_date:
                pos.missing_sessions = tuple(
                    d
                    for d in expected_sessions(pos.first_activity_date, pos.last_activity_date)
                    if d in gap_set
                )
        self._finalized = True
        return self

    def _classify_censoring(self, pos: MMPosition, cutoff: Optional[date]) -> tuple[bool, str]:
        """Decide whether a series' inventory starts from a known zero.

        Ordered strongest evidence first.  An explicit listing date beats the
        window heuristic; a floor breach beats everything, because a breach is
        direct proof that inventory existed before the data started.
        """
        if pos.long_floor_breaches or pos.short_floor_breaches:
            return True, "closing_volume_exceeded_observed_opens"
        if pos.listing_date is not None:
            if self.report.window_start is None:
                return True, "no_window"
            if pos.listing_date >= self.report.window_start:
                return False, "observed_from_listing_date"
            return True, "listed_before_data_window"
        if pos.first_activity_date is None:
            return True, "no_activity"
        if cutoff is None:
            return True, "window_too_short_to_establish_origin"
        if pos.first_activity_date >= cutoff:
            return False, "first_trade_inside_window"
        return True, "first_trade_at_window_edge"

    # -- convenience --------------------------------------------------

    def positions(self) -> list[MMPosition]:
        return self.book.positions()

    def clean_positions(self) -> list[MMPosition]:
        """Series whose inventory is reconstructable from a known zero."""
        return [p for p in self.book if not p.left_censored]

    def summary(self) -> dict[str, Any]:
        positions = self.book.positions()
        clean = [p for p in positions if not p.left_censored]
        return {
            **self.report.as_dict(),
            "series_total": len(positions),
            "series_clean": len(clean),
            "series_left_censored": len(positions) - len(clean),
            "clean_share": (len(clean) / len(positions)) if positions else 0.0,
            "net_contracts_total": sum(p.net_contracts for p in positions),
            "net_contracts_clean": sum(p.net_contracts for p in clean),
        }
