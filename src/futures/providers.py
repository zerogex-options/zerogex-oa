"""Vendor-neutral CME market-data ingestion interfaces + fixtures.

Defines the Protocols the futures ingestion layer talks to for quotes,
trades, option quotes/trades, open interest and settlements, plus normalized
record types carrying full provenance (source, timestamps, trade date,
logical product, contract ids, price/size, sequence, quality flags).

**No real CME feed is contacted here.**  The concrete
``*FixtureProvider`` classes replay deterministic sample data so the pipeline,
persistence, health plumbing and tests all work end-to-end without licensed
market data.  The real, licensed provider adapter is intentionally left
pending — see :data:`REAL_PROVIDER_STATUS` and
``docs/futures-support-architecture.md``.  Wiring a fabricated adapter in as
if it were live is explicitly disallowed (spec rules #5, #6, #13).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import AsyncIterator, Optional, Protocol, Sequence

#: Status advertised by /health and the docs: the real adapter is not built.
REAL_PROVIDER_STATUS = "pending_licensed_cme_adapter"


class DataQualityFlag(str, Enum):
    OK = "ok"
    STALE = "stale"
    CROSSED = "crossed"  # bid > ask
    SEQUENCE_GAP = "sequence_gap"
    SYNTHETIC_FIXTURE = "synthetic_fixture"  # never set on a real feed


@dataclass(frozen=True)
class NormalizedEvent:
    """Common envelope for every normalized market-data event.

    Concrete records (quote/trade/OI/settlement) extend this so downstream
    handlers can treat provenance uniformly and enforce idempotency on
    ``(contract_id, exchange_timestamp, sequence)``.
    """

    source: str  # provider id (e.g. "fixture", "<vendor>")
    received_at: datetime  # when ingestion received it (tz-aware)
    exchange_timestamp: datetime  # exchange-stamped instant (tz-aware)
    trade_date: str  # CME trade date (YYYY-MM-DD)
    logical_product: str  # "ES" / "NQ"
    contract_id: str  # dated contract / option contract id
    vendor_contract_id: Optional[str] = None
    sequence: Optional[int] = None
    quality: DataQualityFlag = DataQualityFlag.OK


@dataclass(frozen=True)
class FuturesQuoteEvent(NormalizedEvent):
    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None


@dataclass(frozen=True)
class FuturesTradeEvent(NormalizedEvent):
    price: float = 0.0
    size: int = 0
    aggressor: Optional[str] = None  # "buy"/"sell"/None when unknown


@dataclass(frozen=True)
class FuturesOptionQuoteEvent(NormalizedEvent):
    strike: float = 0.0
    put_call: str = "C"
    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    implied_volatility: Optional[float] = None


@dataclass(frozen=True)
class OpenInterestEvent(NormalizedEvent):
    open_interest: int = 0


@dataclass(frozen=True)
class SettlementEvent(NormalizedEvent):
    settlement_price: float = 0.0


@dataclass
class IngestionMetrics:
    """Observability counters surfaced to /health and structured logs."""

    messages_received: int = 0
    messages_dropped: int = 0
    sequence_gaps: int = 0
    stale_events: int = 0
    reconnects: int = 0
    errors: int = 0
    last_event_at: Optional[datetime] = None

    def record(self, event: NormalizedEvent) -> None:
        self.messages_received += 1
        self.last_event_at = event.received_at
        if event.quality is DataQualityFlag.STALE:
            self.stale_events += 1
        elif event.quality is DataQualityFlag.SEQUENCE_GAP:
            self.sequence_gaps += 1

    def snapshot(self) -> dict:
        return {
            "messages_received": self.messages_received,
            "messages_dropped": self.messages_dropped,
            "sequence_gaps": self.sequence_gaps,
            "stale_events": self.stale_events,
            "reconnects": self.reconnects,
            "errors": self.errors,
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
        }


@dataclass
class ProviderHealth:
    provider_id: str
    connected: bool
    real_feed: bool
    status: str
    metrics: IngestionMetrics = field(default_factory=IngestionMetrics)

    def as_dict(self) -> dict:
        return {
            "provider_id": self.provider_id,
            "connected": self.connected,
            "real_feed": self.real_feed,
            "status": self.status,
            "metrics": self.metrics.snapshot(),
        }


# ---------------------------------------------------------------------------
# Provider protocols
# ---------------------------------------------------------------------------


class FuturesQuoteProvider(Protocol):
    def stream_futures_quotes(
        self, products: Sequence[str]
    ) -> AsyncIterator[FuturesQuoteEvent]: ...


class FuturesOptionQuoteProvider(Protocol):
    def stream_option_quotes(
        self, contracts: Sequence[str]
    ) -> AsyncIterator[FuturesOptionQuoteEvent]: ...


class FuturesOpenInterestProvider(Protocol):
    async def fetch_open_interest(self, trade_date: date) -> Sequence[OpenInterestEvent]: ...


class FuturesSettlementProvider(Protocol):
    async def fetch_settlements(self, trade_date: date) -> Sequence[SettlementEvent]: ...


# ---------------------------------------------------------------------------
# Fixture (deterministic, offline) implementations
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    # Real "now" is fine here (fixtures aren't resume-cached); tests pass
    # explicit timestamps into records instead of relying on this.
    return datetime.now(timezone.utc)


class FixtureFuturesQuoteProvider:
    """Replays a fixed list of quote events (synthetic, offline)."""

    provider_id = "fixture"

    def __init__(self, events: Sequence[FuturesQuoteEvent]):
        self._events = list(events)
        self.metrics = IngestionMetrics()

    async def stream_futures_quotes(
        self, products: Sequence[str]
    ) -> AsyncIterator[FuturesQuoteEvent]:
        wanted = {p.upper() for p in products}
        for ev in self._events:
            if ev.logical_product.upper() in wanted:
                self.metrics.record(ev)
                yield ev

    def health(self) -> ProviderHealth:
        return ProviderHealth(
            provider_id=self.provider_id,
            connected=True,
            real_feed=False,
            status=DataQualityFlag.SYNTHETIC_FIXTURE.value,
            metrics=self.metrics,
        )


class FixtureOpenInterestProvider:
    """Returns a fixed OI snapshot for a trade date (synthetic, offline)."""

    provider_id = "fixture"

    def __init__(self, events: Sequence[OpenInterestEvent]):
        self._events = list(events)

    async def fetch_open_interest(self, trade_date: date) -> Sequence[OpenInterestEvent]:
        return [e for e in self._events if e.trade_date == trade_date.isoformat()]


def make_fixture_quote(
    product: str,
    contract_id: str,
    *,
    bid: float,
    ask: float,
    trade_date: str,
    exchange_timestamp: Optional[datetime] = None,
    sequence: Optional[int] = None,
) -> FuturesQuoteEvent:
    """Build a normalized synthetic quote (marked SYNTHETIC_FIXTURE)."""
    ts = exchange_timestamp or _now_utc()
    quality = DataQualityFlag.CROSSED if bid > ask else DataQualityFlag.SYNTHETIC_FIXTURE
    return FuturesQuoteEvent(
        source="fixture",
        received_at=ts,
        exchange_timestamp=ts,
        trade_date=trade_date,
        logical_product=product.upper(),
        contract_id=contract_id,
        sequence=sequence,
        quality=quality,
        bid=bid,
        ask=ask,
        last=round((bid + ask) / 2.0, 4),
    )


class RealCMEProviderNotConfigured(RuntimeError):
    """Raised if code attempts to use the (intentionally absent) real feed.

    This is the guardrail behind spec rule #13: production must never silently
    fall back to fabricated data.  Operators must wire a licensed adapter and
    flip ``ENABLE_CME_INGESTION`` before any real analytics are served.
    """


class PendingRealCMEProvider:
    """Placeholder for the licensed CME/vendor adapter — intentionally inert.

    Every method raises :class:`RealCMEProviderNotConfigured` so a
    misconfiguration fails loudly instead of emitting fake ticks.
    """

    provider_id = "cme_real_pending"

    async def stream_futures_quotes(self, products):  # noqa: D401 - see class doc
        raise RealCMEProviderNotConfigured(REAL_PROVIDER_STATUS)
        yield  # pragma: no cover - makes this an async generator signature

    async def fetch_open_interest(self, trade_date: date):
        raise RealCMEProviderNotConfigured(REAL_PROVIDER_STATUS)

    def health(self) -> ProviderHealth:
        return ProviderHealth(
            provider_id=self.provider_id,
            connected=False,
            real_feed=True,
            status=REAL_PROVIDER_STATUS,
        )
