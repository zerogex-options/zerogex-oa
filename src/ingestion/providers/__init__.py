"""Market data provider registry.

``MARKET_DATA_PROVIDER`` selects the implementation, defaulting to
``tradestation`` so an unset environment behaves exactly as it did before
this package existed.

Registering a new vendor is two lines in :data:`_REGISTRY` plus the module
itself; no consumer changes.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Optional

from src.ingestion.providers.base import (
    Bar,
    BarStream,
    MarketDataProvider,
    OptionQuote,
    OptionQuoteStream,
    ProviderCapabilities,
    ProviderCapabilityError,
)

__all__ = [
    "Bar",
    "BarStream",
    "MarketDataProvider",
    "OptionQuote",
    "OptionQuoteStream",
    "ProviderCapabilities",
    "ProviderCapabilityError",
    "DEFAULT_PROVIDER",
    "available_providers",
    "get_provider",
    "register_provider",
]

#: Unset environment keeps the incumbent feed. Changing this default is a
#: production cutover, not a refactor.
DEFAULT_PROVIDER = "tradestation"


def _build_tradestation(**kwargs: Any) -> MarketDataProvider:
    from src.ingestion.providers.tradestation import TradeStationProvider

    client = kwargs.pop("client", None)
    if client is not None:
        return TradeStationProvider(client)
    return TradeStationProvider.from_env(
        futures_credentials=bool(kwargs.pop("futures_credentials", False))
    )


def _build_thetadata(**kwargs: Any) -> MarketDataProvider:
    from src.ingestion.providers.thetadata import ThetaDataProvider

    client = kwargs.pop("client", None)
    if client is not None:
        return ThetaDataProvider(client, **kwargs)
    return ThetaDataProvider.from_env(**kwargs)


def _build_thetadata_mv(**kwargs: Any) -> MarketDataProvider:
    """ThetaData pointed at the Market Value terminal.

    Registered as its own name so the comparison harness can run the two
    stages against each other -- ``--incumbent thetadata --candidate
    thetadata_mv`` measures exactly what the penny adjustment costs in
    published levels, which is the question the evaluation exists to
    answer. Selection is by port, not by endpoint; see the provider module.
    """
    kwargs.setdefault("stage", "mv")
    return _build_thetadata(**kwargs)


def _build_stub(**kwargs: Any) -> MarketDataProvider:
    from src.ingestion.providers.stub import StubProvider

    return StubProvider(**kwargs)


#: Factories are lazy so importing this package does not import every
#: vendor SDK. A missing optional dependency then fails only for the
#: deployment that actually selected that vendor.
_REGISTRY: Dict[str, Callable[..., MarketDataProvider]] = {
    "tradestation": _build_tradestation,
    "thetadata": _build_thetadata,
    "thetadata_mv": _build_thetadata_mv,
    "stub": _build_stub,
}


def register_provider(name: str, factory: Callable[..., MarketDataProvider]) -> None:
    """Add a provider factory. Used by tests and out-of-tree vendors."""
    _REGISTRY[name.strip().lower()] = factory


def available_providers() -> list:
    """Registered provider names, sorted."""
    return sorted(_REGISTRY)


def get_provider(name: Optional[str] = None, **kwargs: Any) -> MarketDataProvider:
    """Build the configured provider.

    :param name: explicit provider name; defaults to ``MARKET_DATA_PROVIDER``
        and then to :data:`DEFAULT_PROVIDER`.
    :param kwargs: passed to the factory. ``client=`` reuses an existing
        ``TradeStationClient`` so a process keeps one client and its shared
        token/stream bookkeeping rather than opening a second.
    :raises ValueError: on an unknown name, listing what is registered.
        Deliberately fatal: a typo in ``MARKET_DATA_PROVIDER`` that
        silently fell back to the default would be a cutover that appears
        to succeed while still reading the old feed.
    """
    resolved = (
        (name or os.getenv("MARKET_DATA_PROVIDER", "").strip() or DEFAULT_PROVIDER).strip().lower()
    )
    factory = _REGISTRY.get(resolved)
    if factory is None:
        raise ValueError(
            f"unknown MARKET_DATA_PROVIDER {resolved!r}; "
            f"registered providers are {available_providers()}"
        )
    return factory(**kwargs)
