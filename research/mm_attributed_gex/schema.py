"""Normalized, exchange-agnostic representation of participant-classified flow.

The whole point of this module is that *nothing downstream of it knows what a
Cboe file looks like*.  Inventory reconstruction, gamma calculation, the
dataset builder and the backtests all consume :class:`ParticipantActivity`
records.  Supporting a second exchange's Open-Close feed later means writing
one more loader that emits these records — no change to any other layer.

Field semantics are deliberately narrow:

``exchange``        venue that classified the activity (``"CBOE_C1"``).
``symbol``          canonical underlying (``"SPX"``), not the option root.
``option_root``     the listed root (``"SPX"`` / ``"SPXW"``) when known.  Kept
                    because SPX 3rd-Friday monthlies settle AM (09:30 ET SOQ)
                    while SPXW settles PM (16:00 ET), and ZeroGEX's
                    time-to-expiration is settlement-aware.
``expiration``      contract expiration date.
``strike``          strike price.
``option_type``     ``"C"`` or ``"P"``.
``participant``     :class:`ParticipantType`.
``side``            :class:`Side` — did the participant BUY or SELL.
``position_effect`` :class:`PositionEffect` — OPEN, CLOSE, or UNKNOWN.
``contracts``       non-negative contract count for this bucket.
``timestamp``       end of the interval the bucket covers (tz-aware, UTC).
``interval``        :class:`Interval` — how wide that bucket is.
``trading_date``    ET session date the bucket belongs to.

Sign conventions live in exactly one place — :mod:`~.inventory` — so that this
module stays a pure data carrier.  ``contracts`` here is ALWAYS >= 0; direction
is carried by ``side`` and ``position_effect``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Iterable, Iterator, Mapping, Optional

__all__ = [
    "Exchange",
    "ParticipantType",
    "Side",
    "PositionEffect",
    "Interval",
    "ParticipantActivity",
    "SeriesKey",
    "normalize_option_type",
    "series_key",
    "MARKET_MAKER_LIKE",
]


class Exchange(str, Enum):
    """Venue whose classification produced the record."""

    CBOE_C1 = "CBOE_C1"
    UNKNOWN = "UNKNOWN"


class ParticipantType(str, Enum):
    """Participant categories carried by exchange Open-Close products.

    ``CUSTOMER`` covers public customers.  Cboe additionally splits customer
    and professional-customer volume by order size (<100 / 100-199 / >199);
    loaders aggregate those into one participant and keep the bucket in
    :attr:`ParticipantActivity.attrs` as provenance.
    ``PROFESSIONAL_CUSTOMER`` is the separately reported "professional
    customer" category, distinct from ``CUSTOMER``.

    ``MARKET_MAKER`` is the category this experiment is about.  Everything
    else exists so the loader never has to silently discard a column and so
    the open-interest reconciliation in :mod:`~.reconcile` can check that all
    participant categories sum to the total.
    """

    MARKET_MAKER = "MARKET_MAKER"
    CUSTOMER = "CUSTOMER"
    PROFESSIONAL_CUSTOMER = "PROFESSIONAL_CUSTOMER"
    BROKER_DEALER = "BROKER_DEALER"
    FIRM = "FIRM"
    UNKNOWN = "UNKNOWN"


#: Categories that are plausibly liquidity-providing.  ``MARKET_MAKER`` is the
#: experiment's subject; the others are listed so a sensitivity run can widen
#: the definition ("what if some MM flow is booked through a broker-dealer?")
#: without editing the reconstruction code.  Widening the definition is a
#: SEPARATE, LABELLED experiment arm — never the default.
MARKET_MAKER_LIKE: tuple[ParticipantType, ...] = (ParticipantType.MARKET_MAKER,)


class Side(str, Enum):
    """Direction of the participant's transaction."""

    BUY = "BUY"
    SELL = "SELL"


class PositionEffect(str, Enum):
    """Effect on the *reporting participant's* own position.

    ``UNKNOWN`` is a first-class value, not an error.  Some feeds report
    volume that carries no reliable position-effect designation, and the
    reconstruction must be able to say so rather than silently assuming OPEN.
    """

    OPEN = "OPEN"
    CLOSE = "CLOSE"
    UNKNOWN = "UNKNOWN"


class Interval(str, Enum):
    """Width of the reporting bucket a record covers.

    ``MINUTE_1`` and ``MINUTE_10`` are the cadences Cboe's Open-Close Volume
    Summary actually offers intraday; ``SESSION`` is its end-of-day form.
    ``MINUTE_5`` and ``MINUTE_30`` exist so another venue's feed can be
    described without inventing a new enum.
    """

    MINUTE_1 = "1min"
    MINUTE_5 = "5min"
    MINUTE_10 = "10min"
    MINUTE_30 = "30min"
    SESSION = "session"  # one row per trading day (end-of-day summary)
    UNKNOWN = "unknown"


#: Identity of one option series: everything needed to price it and to carry
#: an inventory forward across sessions.
SeriesKey = tuple[str, date, float, str]  # (symbol, expiration, strike, option_type)


def normalize_option_type(raw: object) -> str:
    """Coerce a feed's call/put marker to ``"C"`` / ``"P"``.

    Accepts the spellings real files use: ``C``/``P``, ``CALL``/``PUT``,
    ``c``/``p``.  Raises :class:`ValueError` on anything else rather than
    guessing — a mis-mapped option type would silently invert gamma sign for
    half the chain, which is exactly the class of error this experiment must
    not make.
    """
    if raw is None:
        raise ValueError("option_type is required (got None)")
    text = str(raw).strip().upper()
    if text in ("C", "CALL", "CALLS"):
        return "C"
    if text in ("P", "PUT", "PUTS"):
        return "P"
    raise ValueError(f"Unrecognized option_type {raw!r}; expected C/CALL or P/PUT")


@dataclass(frozen=True, slots=True)
class ParticipantActivity:
    """One (series, participant, side, position-effect, interval) volume bucket.

    Immutable and hashable so records can be grouped, deduplicated and cached
    freely.  ``attrs`` carries loader-specific provenance (source filename,
    original column name, customer size bucket, …) without polluting the
    normalized surface every downstream layer depends on.
    """

    exchange: Exchange
    symbol: str
    expiration: date
    strike: float
    option_type: str
    participant: ParticipantType
    side: Side
    position_effect: PositionEffect
    contracts: int
    timestamp: datetime
    trading_date: date
    interval: Interval = Interval.UNKNOWN
    option_root: Optional[str] = None
    attrs: Mapping[str, object] = field(default_factory=dict, compare=False, hash=False)

    def __post_init__(self) -> None:
        if self.contracts < 0:
            raise ValueError(
                f"contracts must be non-negative (got {self.contracts}); "
                "direction is carried by side/position_effect, not by sign"
            )
        if self.option_type not in ("C", "P"):
            raise ValueError(f"option_type must be 'C' or 'P' (got {self.option_type!r})")
        if self.strike <= 0:
            raise ValueError(f"strike must be positive (got {self.strike})")
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC)")

    @property
    def key(self) -> SeriesKey:
        """The series this bucket belongs to."""
        return (self.symbol, self.expiration, float(self.strike), self.option_type)

    @property
    def is_market_maker(self) -> bool:
        return self.participant in MARKET_MAKER_LIKE


def series_key(symbol: str, expiration: date, strike: float, option_type: str) -> SeriesKey:
    """Build a :data:`SeriesKey` with the same coercions the dataclass applies."""
    return (symbol, expiration, float(strike), normalize_option_type(option_type))


def iter_market_maker(records: Iterable[ParticipantActivity]) -> Iterator[ParticipantActivity]:
    """Yield only the Market-Maker-classified records."""
    for rec in records:
        if rec.is_market_maker:
            yield rec
