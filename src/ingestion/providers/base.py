"""Provider-agnostic market data interface.

ZeroGEX ingests six symbol families from one upstream vendor:

* option chains for SPY / QQQ / SPXW / NDXP (the product),
* 1-minute underlying bars for SPY / QQQ / SPX / NDX,
* 5-minute index-value bars for VIX / VXN,
* 1-minute futures bars for @ES / @NQ (the basis engine).

Until now the only implementation was TradeStation, and its shape leaked
all the way into ``stream_manager`` (``"Bid"`` / ``"DailyOpenInterest"``
capitalised dict keys), ``volatility_index_ingester`` (a hand-rolled
``requests.get`` against ``marketdata/stream/barcharts``) and
``futures_underlying_ingester``.  This module is the seam that lets a
second vendor land as one new file instead of a refactor.

**What this module deliberately does NOT do.**  It does not rewire the
live ingestion path.  ``StreamManager`` still constructs
``OptionStreamAccumulator`` directly and still reads raw TradeStation
dicts, so production behaviour on this branch is bit-for-bit what it was
before.  The TradeStation provider here is an *adapter over the existing
client*, exercised by the comparison harness and by tests.  Switching the
hot path onto the interface is a separate, reviewable change once a
second provider is actually contracted.

Design notes
------------

**Normalised records, not vendor dicts.**  :class:`OptionQuote` and
:class:`Bar` carry exactly the fields ``option_chains`` /
``underlying_quotes`` persist, in the units the Greeks and flow code
expect.  Field names match the DB columns so a loader is a
``dataclasses.asdict`` away.

**Streams are objects, not generators.**  The existing accumulators are
long-lived background readers that the main loop samples at its own
cadence (``snapshot`` / ``drain``), and reconnect underneath without the
caller noticing.  A generator cannot express "reconnect and keep the
sticky open-interest state", so the stream protocols below mirror the
accumulator lifecycle that already works.

**Capabilities are declared, not discovered.**  No vendor covers all six
families.  ThetaData sells no CME futures; Databento's catalogue carries
no index feed at all (``MAIN.CGIF`` 404s), so it cannot serve SPX, VIX,
VXN or NDX values at any price.  :class:`ProviderCapabilities` makes that
explicit at construction time, so a deployment fails loudly at startup
rather than silently running a symbol family with no data behind it.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

__all__ = [
    "Bar",
    "MarketDataProvider",
    "OptionQuote",
    "OptionBarStream",
    "OptionQuoteStream",
    "ProviderCapabilities",
    "ProviderCapabilityError",
    "BarStream",
]


class ProviderCapabilityError(RuntimeError):
    """Raised when a caller asks a provider for a feed it does not carry.

    Deliberately a hard error rather than a silent ``None``: a symbol
    family with no data behind it produces empty tables, which surfaces
    hours later as "why is VIX stale" instead of at startup as "this
    vendor has no index feed".
    """


@dataclass(frozen=True)
class ProviderCapabilities:
    """What a provider can actually serve.

    Every flag defaults to ``False`` so a new implementation has to opt
    in per feed; forgetting one produces a loud
    :class:`ProviderCapabilityError`, never a quiet gap.
    """

    #: OPRA (or equivalent) option chain quotes.
    option_quotes: bool = False
    #: Chain discovery — expirations and strikes for an underlying.
    option_chain_discovery: bool = False
    #: Daily open interest, whether via a snapshot call or a statistics feed.
    option_open_interest: bool = False
    #: Consolidated equity/ETF bars or trades for SPY / QQQ.
    underlying_bars: bool = False
    #: Cash index VALUES — SPX, NDX, VIX, VXN. Licensed separately from
    #: the option chains that reference them; several vendors carry the
    #: chains and not the values.
    index_bars: bool = False
    #: CME futures — ES, NQ.
    futures_bars: bool = False
    #: Buy/sell volume split delivered by the vendor rather than derived
    #: locally. TradeStation ships ``UpVolume``/``DownVolume``; most
    #: vendors do not, and the caller must classify from trades instead.
    signed_underlying_volume: bool = False

    def require(self, feed: str) -> None:
        """Raise unless ``feed`` (an attribute name above) is supported."""
        if not getattr(self, feed, False):
            raise ProviderCapabilityError(
                f"provider does not support {feed!r}. "
                "Configure a second provider for this feed, or derive it "
                "locally (e.g. imply an index value from the chain by "
                "put-call parity)."
            )


@dataclass(frozen=True)
class OptionQuote:
    """One contract's latest quote, normalised across vendors.

    Field names and units match ``option_chains`` so persisting is a
    direct mapping.  Every price field is optional because a real feed
    routinely delivers one-sided or absent quotes, and the ingestion
    engine already has fallback logic for that; fabricating a zero here
    would defeat it.

    ``implied_volatility`` is present only because some vendors ship it
    for free.  ZeroGEX computes its own IV and Greeks
    (``iv_calculator``, ``greeks_calculator``), so a provider that omits
    it costs nothing and should not be paid extra for it.
    """

    option_symbol: str
    timestamp: Optional[datetime] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    mid: Optional[float] = None
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    #: Cumulative session volume for the contract, not an interval delta.
    #: ``_classify_volume_chunk`` differences successive snapshots itself.
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    implied_volatility: Optional[float] = None

    def effective_mid(self) -> Optional[float]:
        """``mid`` when the vendor supplies it, else the bid/ask midpoint."""
        if self.mid is not None and self.mid > 0:
            return self.mid
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2.0
        return None


@dataclass(frozen=True)
class Bar:
    """One OHLCV bar, normalised across vendors.

    ``up_volume`` / ``down_volume`` are the tick-test split.  TradeStation
    delivers them on the bar; every other vendor evaluated does not, in
    which case an implementation either derives them from a trade feed or
    leaves them ``None`` and declares
    ``signed_underlying_volume=False``.  ``None`` is distinct from ``0``
    here: zero means "no signed volume this bar", ``None`` means "this
    feed cannot tell you", and the ingestion engine must not persist the
    latter as if it were the former.
    """

    symbol: str
    timestamp: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    up_volume: Optional[int] = None
    down_volume: Optional[int] = None


class OptionQuoteStream(abc.ABC):
    """Long-lived background reader for a set of option contracts.

    Mirrors ``OptionStreamAccumulator``'s lifecycle exactly, because that
    lifecycle is load-bearing: the reader reconnects underneath, keeps
    open interest and IV *sticky* (only overwritten by positive values,
    since stream deltas routinely send zero), and the main loop samples
    it at whatever cadence it likes.
    """

    @abc.abstractmethod
    def start(self, seed_from_snapshot: bool = True) -> None:
        """Begin reading. When ``seed_from_snapshot``, populate open
        interest and prices from a REST/batch call first so the first
        poll cycle sees a full chain rather than only contracts that
        happened to tick."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop readers and close connections. Must be idempotent."""

    @abc.abstractmethod
    def is_alive(self) -> bool:
        """True while at least one reader thread is running."""

    @abc.abstractmethod
    def snapshot(self) -> Dict[str, OptionQuote]:
        """Every contract's latest state, without clearing dirty flags."""

    @abc.abstractmethod
    def drain(self) -> Dict[str, OptionQuote]:
        """Contracts updated since the last drain, clearing dirty flags."""

    @property
    @abc.abstractmethod
    def updates_received(self) -> int:
        """Monotonic count of merged updates, for staleness watchdogs."""


class BarStream(abc.ABC):
    """Long-lived background reader for one symbol's bars.

    Used for all three bar families (underlying, index value, futures).
    They differ only in interval and session window, which the provider
    resolves; the consumer sees the same object either way.
    """

    @abc.abstractmethod
    def start(self) -> None:
        """Begin reading."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop the reader and close connections. Must be idempotent."""

    @abc.abstractmethod
    def is_alive(self) -> bool:
        """True while the reader thread is running."""

    @abc.abstractmethod
    def drain(self) -> Optional[Bar]:
        """Latest bar if it changed since the last drain, else ``None``."""

    @property
    @abc.abstractmethod
    def updates_received(self) -> int:
        """Monotonic count of merged bars, for staleness watchdogs."""


#: Alias kept for readability at call sites that stream option *bars*
#: rather than quotes; the protocol is identical.
OptionBarStream = BarStream


class MarketDataProvider(abc.ABC):
    """One vendor's feed, expressed as the six things ZeroGEX consumes.

    Implementations are constructed from environment configuration by
    :func:`src.ingestion.providers.get_provider` and are expected to be
    cheap to build and safe to share across threads for the discovery
    calls.  Stream objects are per-caller and own their own threads.
    """

    #: Short stable identifier, also the ``MARKET_DATA_PROVIDER`` value.
    name: str = "base"

    @property
    @abc.abstractmethod
    def capabilities(self) -> ProviderCapabilities:
        """What this provider can serve. Checked before every stream."""

    # -- streams -----------------------------------------------------------

    @abc.abstractmethod
    def stream_option_quotes(
        self,
        option_symbols: Sequence[str],
        *,
        wakeup: Any = None,
        max_symbols_per_connection: Optional[int] = None,
    ) -> OptionQuoteStream:
        """Stream quotes for ``option_symbols`` (vendor-native symbology).

        ``wakeup`` is an optional ``threading.Event`` the stream sets when
        data arrives, so the main loop can react immediately instead of
        sleeping a fixed interval.
        """

    @abc.abstractmethod
    def stream_underlying_bars(
        self,
        symbol: str,
        *,
        db_symbol: Optional[str] = None,
        interval: int = 1,
        unit: str = "Minute",
        wakeup: Any = None,
    ) -> BarStream:
        """Stream bars for an equity/ETF or cash index used as spot."""

    @abc.abstractmethod
    def stream_index_bars(
        self,
        symbol: str,
        *,
        db_symbol: Optional[str] = None,
        interval: int = 5,
        unit: str = "Minute",
        initial_barsback: int = 160,
        poll_barsback: int = 3,
    ) -> BarStream:
        """Stream a volatility-index VALUE series (VIX, VXN).

        Separate from :meth:`stream_underlying_bars` because index values
        are a distinct licensed product from the equity tape, and several
        vendors carry one and not the other.
        """

    @abc.abstractmethod
    def stream_futures_bars(
        self,
        symbol: str,
        *,
        db_symbol: Optional[str] = None,
        interval: int = 1,
        unit: str = "Minute",
        initial_barsback: int = 960,
        poll_barsback: int = 3,
    ) -> BarStream:
        """Stream a CME equity-index future (front month or continuous)."""

    # -- discovery and snapshots -------------------------------------------

    @abc.abstractmethod
    def get_option_expirations(
        self, underlying: str, strike_price: Optional[float] = None
    ) -> List[date]:
        """Listed expirations for ``underlying``, ascending."""

    @abc.abstractmethod
    def get_option_strikes(self, underlying: str, expiration: Optional[str] = None) -> List[float]:
        """Listed strikes for ``underlying``, optionally one expiration."""

    @abc.abstractmethod
    def snapshot_option_quotes(self, option_symbols: Sequence[str]) -> Dict[str, OptionQuote]:
        """One-shot quotes for ``option_symbols``.

        This is the open-interest and IV seed.  Open interest updates once
        daily at settlement and stream deltas frequently omit it, so the
        engine seeds from here at start and on strike recalibration.
        """

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Release any shared connections. Default is a no-op."""

    def build_option_symbol(
        self,
        underlying: str,
        expiration: date,
        strike: float,
        option_type: str,
    ) -> str:
        """Vendor-native symbol for one contract.

        Defaults to the OCC 21-character form, which OPRA-sourced vendors
        accept directly.  Override only where a vendor uses its own
        spelling.
        """
        root = underlying.upper().lstrip("$").split(".")[0]
        yy_mm_dd = expiration.strftime("%y%m%d")
        cp = "C" if option_type.upper().startswith("C") else "P"
        strike_thousandths = int(round(float(strike) * 1000))
        return f"{root:<6}{yy_mm_dd}{cp}{strike_thousandths:08d}"
