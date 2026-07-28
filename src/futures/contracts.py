"""Futures instrument master — dated contracts, option series & option contracts.

This is the domain model behind first-class ES/NQ support.  It deliberately
mirrors the security-definition fields a real CME/vendor feed publishes, so
the ingestion layer can populate these objects verbatim rather than parsing
critical properties out of display ticker strings at runtime (spec rule #14).

Two hard rules enforced by the model:

* **Exact expiration timestamps, not just dates.**  ``expiration`` on every
  contract/series/option is a timezone-aware ``datetime`` at the actual
  settlement instant.  0DTE classification, Black-76 time-to-expiry and the
  CME calendar all depend on the instant, not the calendar day.
* **Decimal-safe economics.**  Notional / tick-value conversions go through
  :func:`notional_value` / :func:`tick_value`, which use :class:`Decimal` so
  a $50 multiplier on a 6000.25 price never accumulates float error (spec
  rule #15).

Dataclasses (not pydantic) are used for the domain objects to match the
``src/backtesting`` convention; pydantic lives at the API boundary only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class PutCall(str, Enum):
    CALL = "C"
    PUT = "P"


class ContractStatus(str, Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    #: Listed by the exchange but not yet the front/tradable focus.
    PENDING = "pending"
    DELISTED = "delisted"


class ExerciseStyle(str, Enum):
    AMERICAN = "american"
    EUROPEAN = "european"


class SettlementType(str, Enum):
    #: Exercise/assignment delivers the underlying futures position.
    FUTURES = "futures"
    #: Cash-settled against a reference (rare for ES/NQ standard options).
    CASH = "cash"


class OptionListingType(str, Enum):
    """Which listing cycle an option series belongs to.

    CME equity-index options list several parallel cycles — daily/weekly
    (Mon-Fri EW1-EW4), end-of-month (EOM), and quarterly (the standard
    option on the quarterly future).  They have *different* option roots and
    can differ in exercise/settlement, so the model must not assume every
    ES option behaves identically (spec rule).
    """

    DAILY = "daily"
    WEEKLY = "weekly"
    END_OF_MONTH = "end_of_month"
    QUARTERLY = "quarterly"


# ---------------------------------------------------------------------------
# Month codes
# ---------------------------------------------------------------------------

#: CME/standard futures month codes.  ES & NQ standard futures list only the
#: quarterly months (Mar/Jun/Sep/Dec) but options reference every month, so
#: the full map is kept.
MONTH_CODES: dict[int, str] = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}
_CODE_TO_MONTH: dict[str, int] = {v: k for k, v in MONTH_CODES.items()}

#: The quarterly cycle ES/NQ standard futures trade on.
QUARTERLY_MONTHS: tuple[int, ...] = (3, 6, 9, 12)


def month_code(month: int) -> str:
    """Return the single-letter CME month code for a 1-12 month number."""
    try:
        return MONTH_CODES[month]
    except KeyError:  # pragma: no cover - defensive
        raise ValueError(f"month must be 1-12, got {month!r}")


def month_from_code(code: str) -> int:
    """Inverse of :func:`month_code`."""
    try:
        return _CODE_TO_MONTH[code.strip().upper()]
    except KeyError:
        raise ValueError(f"unknown month code {code!r}")


def contract_label(product_symbol: str, year: int, month: int) -> str:
    """Human-readable contract label, e.g. ``ESZ25`` for ES Dec 2025.

    Uses a two-digit year (CME convention on modern roots).  This is a
    DISPLAY label only — never parsed back for authoritative fields; the
    dated-contract object carries the exact expiration instead.
    """
    return f"{product_symbol.upper()}{month_code(month)}{year % 100:02d}"


# ---------------------------------------------------------------------------
# Decimal-safe economics
# ---------------------------------------------------------------------------


def _to_decimal(value: object) -> Decimal:
    """Coerce a number to Decimal via str() to avoid binary-float artifacts."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def notional_value(price: float | Decimal, multiplier: int | float | Decimal) -> Decimal:
    """Contract notional = price × point-value multiplier, decimal-safe.

    ES at 6000.25 with a $50 multiplier → Decimal('300012.50'), exact.
    """
    return _to_decimal(price) * _to_decimal(multiplier)


def tick_value(tick_size: float | Decimal, multiplier: int | float | Decimal) -> Decimal:
    """Dollar value of one minimum price increment.

    ES: tick 0.25 × $50 = $12.50.  NQ: 0.25 × $20 = $5.00.
    """
    return _to_decimal(tick_size) * _to_decimal(multiplier)


# ---------------------------------------------------------------------------
# Domain objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FuturesProduct:
    """A logical futures product (ES, NQ) — the continuous instrument face.

    Contract economics live here (multiplier, tick) so the resolver and the
    GEX engine read them from an authoritative object rather than a scattered
    literal.
    """

    symbol: str  # logical product, e.g. "ES"
    name: str
    exchange: str  # "CME"
    currency: str
    multiplier: int  # point value ($50 for ES, $20 for NQ)
    tick_size: float  # 0.25 for both
    #: Family this product belongs to (mirrors instruments.InstrumentFamily
    #: values: "sp500" / "nasdaq100"); kept as a plain str to avoid a hard
    #: import cycle with src.instruments.
    family: str

    @property
    def tick_value(self) -> Decimal:
        return tick_value(self.tick_size, self.multiplier)


@dataclass(frozen=True)
class FuturesContract:
    """A single dated futures contract (e.g. ES Dec 2025 / ESZ25).

    The continuous symbol ``ES`` always resolves to one of these — analytics
    are attributed to the dated contract, never to the abstract continuous
    face (spec Phase 3).
    """

    product_symbol: str  # "ES"
    contract_month: int  # 1-12
    contract_year: int  # e.g. 2025
    expiration: datetime  # exact settlement instant (tz-aware)
    multiplier: int
    tick_size: float
    currency: str = "USD"
    exchange: str = "CME"
    #: Exchange-native dated symbol (e.g. "ESZ25"); DISPLAY/label use.
    exchange_symbol: Optional[str] = None
    #: Vendor-native symbol (e.g. TradeStation "ESZ25" or "@ES=..."); kept
    #: distinct so a vendor swap never rewrites analytics keys.
    vendor_symbol: Optional[str] = None
    first_trade_date: Optional[datetime] = None
    last_trade_date: Optional[datetime] = None
    settlement_date: Optional[datetime] = None
    status: ContractStatus = ContractStatus.ACTIVE
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    is_front_contract: bool = False
    is_next_contract: bool = False
    #: Stable identity independent of any vendor string.  Defaults to the
    #: canonical label; a persistence layer may override with a DB id.
    contract_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.expiration.tzinfo is None:
            raise ValueError(
                "FuturesContract.expiration must be timezone-aware "
                "(exact settlement instant, not a naive date)"
            )
        if self.contract_id is None:
            object.__setattr__(self, "contract_id", self.label)
        if self.exchange_symbol is None:
            object.__setattr__(self, "exchange_symbol", self.label)

    @property
    def label(self) -> str:
        return contract_label(self.product_symbol, self.contract_year, self.contract_month)

    @property
    def contract_month_code(self) -> str:
        return month_code(self.contract_month)

    def is_expired(self, as_of: datetime) -> bool:
        return as_of >= self.expiration

    def notional(self, price: float | Decimal) -> Decimal:
        return notional_value(price, self.multiplier)


@dataclass(frozen=True)
class FuturesOptionSeries:
    """A series of futures options sharing a root, expiry and underlying.

    Multiple roots coexist per product (daily EW, weekly, EOM, quarterly);
    each series pins the *specific* underlying futures contract it settles
    into so options across maturities are never blindly co-aggregated.
    """

    product_symbol: str  # "ES"
    option_root: str  # e.g. "EW", "E1A", "ES" (quarterly)
    expiration: datetime  # exact expiration instant (tz-aware)
    underlying_contract_id: str  # FuturesContract.contract_id it settles into
    multiplier: int
    listing_type: OptionListingType
    exercise_style: ExerciseStyle = ExerciseStyle.AMERICAN
    settlement_type: SettlementType = SettlementType.FUTURES
    currency: str = "USD"
    vendor_symbol: Optional[str] = None
    status: ContractStatus = ContractStatus.ACTIVE
    series_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.expiration.tzinfo is None:
            raise ValueError("FuturesOptionSeries.expiration must be tz-aware")
        if self.series_id is None:
            object.__setattr__(
                self,
                "series_id",
                f"{self.option_root}:{self.expiration.date().isoformat()}",
            )


@dataclass(frozen=True)
class FuturesOptionContract:
    """A single strike/put-call option on a dated future.

    Carries its own multiplier and exact expiration so downstream GEX/Greeks
    never re-derive them from a display string.  ``underlying_contract_id``
    binds it to the exact futures maturity it references.
    """

    series_id: str
    product_symbol: str
    strike: float
    put_call: PutCall
    expiration: datetime  # exact expiration instant (tz-aware)
    underlying_contract_id: str
    multiplier: int
    exercise_style: ExerciseStyle = ExerciseStyle.AMERICAN
    settlement_type: SettlementType = SettlementType.FUTURES
    status: ContractStatus = ContractStatus.ACTIVE
    #: Vendor's stable contract id (authoritative live-lookup key).
    vendor_contract_id: Optional[str] = None
    open_interest: Optional[int] = None
    volume: Optional[int] = None
    #: Optional cached market/greeks context — populated by analytics, not
    #: the security definition.
    metadata: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.expiration.tzinfo is None:
            raise ValueError("FuturesOptionContract.expiration must be tz-aware")
        if self.strike <= 0:
            raise ValueError(f"strike must be positive, got {self.strike!r}")

    @property
    def is_call(self) -> bool:
        return self.put_call is PutCall.CALL

    def intrinsic(self, futures_price: float) -> float:
        if self.is_call:
            return max(0.0, futures_price - self.strike)
        return max(0.0, self.strike - futures_price)
