"""Template for a second market data vendor.

Copy this file, rename the class, fill in the ``NotImplementedError``
bodies, register it in :mod:`src.ingestion.providers`, and the ingestion
engine can run on it.  Nothing else in the codebase needs to change.

The point of shipping a stub rather than a half-finished ThetaData or
Databento client is that neither vendor is contracted yet, and the two
have materially different shapes:

* **ThetaData** exposes a gRPC client (``thetadata`` on PyPI, Python 3.12+)
  and historically a local Theta Terminal for streaming.  It sells options,
  stocks and indices, and has **no CME futures product at all**, so a
  ThetaData provider must declare ``futures_bars=False`` and the deployment
  must pair it with a second provider for @ES / @NQ.
* **Databento** exposes a raw TCP live gateway with an official Python
  client, subscribes by parent symbol (``SPY.OPT`` delivers the whole
  chain, which sidesteps per-contract subscription counting entirely), and
  its catalogue contains **no index feed** — ``MAIN.CGIF`` 404s — so a
  Databento provider must declare ``index_bars=False`` and SPX / NDX / VIX
  / VXN values have to come from elsewhere or be implied from the chain by
  put-call parity.

Both facts are why :class:`ProviderCapabilities` exists.  Declare them
honestly and the engine fails at startup instead of writing empty tables.

Implementation notes for whoever fills this in
----------------------------------------------

**Sticky fields.**  Open interest updates once a day at settlement and
volume is cumulative for the session.  Whatever merge you write must only
overwrite these when the incoming value is positive, or a mid-session
zero from the feed erases the accumulated figure.  This is the single
most repeated bug in the TradeStation path and the reason
``OptionStreamAccumulator`` documents it three times.

**Timestamps.**  Drop a record whose timestamp will not parse rather than
stamping it with ``now()``.  A misdated bar overwrites the current
minute's real bar and corrupts the spot price the Greeks are computed
against.

**Reconnects.**  Use exponential backoff with jitter and reset the failure
count only after a connection has stayed healthy.  A flat retry across
many reader threads is what exhausted the per-account stream cap on the
incumbent feed.

**Signed volume.**  Only TradeStation ships ``UpVolume``/``DownVolume`` on
the bar.  If your vendor does not, leave ``up_volume``/``down_volume`` as
``None`` (not ``0``) and declare ``signed_underlying_volume=False``; the
caller then classifies from a trade feed instead.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Sequence

from src.ingestion.providers.base import (
    BarStream,
    MarketDataProvider,
    OptionQuote,
    OptionQuoteStream,
    ProviderCapabilities,
)

__all__ = ["StubProvider"]

_TODO = (
    "StubProvider is a template. Copy src/ingestion/providers/stub.py, "
    "implement this method against your vendor's client, and register the "
    "new class in src/ingestion/providers/__init__.py."
)


class StubProvider(MarketDataProvider):
    """A provider that declares no capabilities and implements nothing.

    Selecting it via ``MARKET_DATA_PROVIDER=stub`` is useful for exactly
    one thing: proving that the engine's provider wiring is reached, and
    that a capability gap fails loudly at startup rather than silently
    producing empty tables.
    """

    name = "stub"

    def __init__(self, capabilities: Optional[ProviderCapabilities] = None):
        # Default: supports nothing. A subclass or a test overrides this
        # to assert the capability gate behaves.
        self._capabilities = capabilities or ProviderCapabilities()

    @property
    def capabilities(self) -> ProviderCapabilities:
        return self._capabilities

    def stream_option_quotes(
        self,
        option_symbols: Sequence[str],
        *,
        wakeup: Any = None,
        max_symbols_per_connection: Optional[int] = None,
    ) -> OptionQuoteStream:
        self._capabilities.require("option_quotes")
        raise NotImplementedError(_TODO)

    def stream_underlying_bars(
        self,
        symbol: str,
        *,
        db_symbol: Optional[str] = None,
        interval: int = 1,
        unit: str = "Minute",
        wakeup: Any = None,
    ) -> BarStream:
        self._capabilities.require("underlying_bars")
        raise NotImplementedError(_TODO)

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
        self._capabilities.require("index_bars")
        raise NotImplementedError(_TODO)

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
        self._capabilities.require("futures_bars")
        raise NotImplementedError(_TODO)

    def get_option_expirations(
        self, underlying: str, strike_price: Optional[float] = None
    ) -> List[date]:
        self._capabilities.require("option_chain_discovery")
        raise NotImplementedError(_TODO)

    def get_option_strikes(self, underlying: str, expiration: Optional[str] = None) -> List[float]:
        self._capabilities.require("option_chain_discovery")
        raise NotImplementedError(_TODO)

    def snapshot_option_quotes(self, option_symbols: Sequence[str]) -> Dict[str, OptionQuote]:
        self._capabilities.require("option_open_interest")
        raise NotImplementedError(_TODO)
