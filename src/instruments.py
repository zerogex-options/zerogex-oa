"""Centralized instrument registry and asset-class abstraction.

This module is the single source of truth for *what an instrument is* —
its asset class, product family, exchange, analytics source, market
calendar, and contract economics (multiplier / tick).  It exists so the
rest of the codebase can ask capability/metadata questions
(``is_future(sym)``, ``contract_multiplier(sym)``, ``analytics_source(sym)``)
instead of scattering ``symbol in ("ES", "NQ")`` conditionals — see rule #8
of the futures support spec.

Design constraints
------------------
* **Additive & backward compatible.**  SPY / SPX / QQQ / NDX keep the exact
  metadata (multiplier 100, OPRA analytics) they behave with today; nothing
  here changes an existing calculation.  The registry is a *new lookup
  layer*, not a rewrite of how analytics key on symbols.
* **Availability is feature-flagged.**  ES / NQ are always *defined* so the
  API can describe them, but their analytics/ingestion capabilities are
  gated by environment flags (``ENABLE_CME_INGESTION``, ``ENABLE_ES_ANALYTICS``,
  ``ENABLE_NQ_ANALYTICS``, ``ENABLE_ES_REFERENCE_MODE``,
  ``ENABLE_NQ_REFERENCE_MODE``).  An unavailable future must never *look*
  fully operational (rule #6 / rule "Do not make unavailable futures
  instruments appear fully operational").
* **No vendor identifiers leak.**  The registry deals in logical symbols
  (``ES``) and public exchange metadata only.  The TradeStation continuous
  symbol (``@ES``) and any other vendor form stay in ``src/symbols.py`` /
  ingestion, never surfaced here.

The frontend ships a parallel registry (``core/instruments/registry.ts``);
the two are kept intentionally consistent, and ``/api/instruments`` lets the
frontend reconcile against server-side availability at runtime.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class AssetClass(str, Enum):
    ETF = "etf"
    INDEX = "index"
    FUTURE = "future"


class AnalyticsSource(str, Enum):
    #: OPRA-listed equity / index options (SPY, QQQ, SPX, NDX today).
    OPRA_EQUITY_OPTIONS = "opra_equity_options"
    #: CME futures options — the *true* futures-options analytics source.
    CME_FUTURES_OPTIONS = "cme_futures_options"
    #: Cash-index option levels projected onto a futures price scale
    #: (the interim SPX-derived ES reference mode).  NEVER to be presented
    #: as actual CME futures-options GEX.
    CASH_INDEX_PROJECTION = "cash_index_projection"


class MarketCalendarId(str, Enum):
    #: NYSE/Nasdaq equity hours (regular 09:30-16:00 ET, extended 04:00-20:00).
    US_EQUITIES = "us_equities"
    #: Cash-index hours — underlying prints only during 09:30-16:00 ET even
    #: though the options trade extended hours.  Kept distinct because the
    #: existing codebase already treats cash indices differently
    #: (``src.symbols.is_cash_index``).
    US_CASH_INDEX = "us_cash_index"
    #: CME Globex equity-index futures: Sun 18:00 ET -> Fri 17:00 ET with a
    #: daily 17:00-18:00 maintenance break.
    CME_EQUITY_INDEX = "cme_equity_index"


class InstrumentFamily(str, Enum):
    SP500 = "sp500"
    NASDAQ100 = "nasdaq100"


@dataclass(frozen=True)
class InstrumentCapabilities:
    """Which product surfaces an instrument can serve.

    Booleans here are the *static* capability of the asset class (does GEX
    even make sense for it?).  Runtime *availability* — is the data actually
    being ingested right now — is layered on top by :func:`is_available`
    and the per-capability feature flags, so a capability can be ``True``
    (the surface exists) while the instrument is still gated off because its
    CME feed is not enabled.
    """

    quotes: bool = False
    historical_quotes: bool = False
    gex: bool = False
    option_flow: bool = False
    vanna_charm: bool = False
    max_pain: bool = False
    signals: bool = False
    replay: bool = False
    backtesting: bool = False
    strategy_builder: bool = False
    #: Cash-index-projected reference levels (SPX-derived ES). Distinct from
    #: ``gex``: an instrument can offer reference levels without any true
    #: futures-options analytics.
    futures_reference_mode: bool = False


@dataclass(frozen=True)
class InstrumentDefinition:
    """Immutable metadata describing one tradable instrument."""

    symbol: str
    display_symbol: str
    name: str
    family: InstrumentFamily
    asset_class: AssetClass
    exchange: str
    analytics_source: AnalyticsSource
    market_calendar: MarketCalendarId
    capabilities: InstrumentCapabilities
    currency: str = "USD"
    #: Options contract multiplier / point value.  100 for equity & index
    #: options (1 contract = 100 shares/index-units); 50 for ES, 20 for NQ.
    contract_multiplier: int = 100
    #: Minimum price increment of the *underlying* (informational; option
    #: strikes/ticks differ).  0.25 for ES/NQ, left None for equities.
    price_increment: Optional[float] = None
    #: Cash-index instrument whose option-derived levels seed this
    #: instrument's reference mode (SPX for ES).  None when N/A.
    reference_source_symbol: Optional[str] = None
    #: Number of decimal places to render prices with on display surfaces.
    price_precision: int = 2

    @property
    def is_future(self) -> bool:
        return self.asset_class is AssetClass.FUTURE

    @property
    def is_cash_index(self) -> bool:
        return self.asset_class is AssetClass.INDEX


# ---------------------------------------------------------------------------
# Static registry.  Definitions are ALWAYS present so the API can describe
# every instrument; availability (below) is what gates them at runtime.
# ---------------------------------------------------------------------------

_EQUITY_CAPS = InstrumentCapabilities(
    quotes=True,
    historical_quotes=True,
    gex=True,
    option_flow=True,
    vanna_charm=True,
    max_pain=True,
    signals=True,
    replay=True,
    backtesting=True,
    strategy_builder=True,
    futures_reference_mode=False,
)

# Futures capability *surfaces*.  These say "GEX/flow/etc. are meaningful for
# a CME future" — NOT that the data is live.  Runtime availability gates the
# actual serving.  strategy_builder stays False: the options strategy builder
# assumes equity-option symbology and must be recalibrated before it is
# claimed for futures (see spec Phase 11).
_FUTURE_CAPS = InstrumentCapabilities(
    quotes=True,
    historical_quotes=True,
    gex=True,
    option_flow=True,
    vanna_charm=True,
    max_pain=True,
    signals=False,
    replay=True,
    backtesting=True,
    strategy_builder=False,
    futures_reference_mode=True,
)

_REGISTRY: Dict[str, InstrumentDefinition] = {
    "SPY": InstrumentDefinition(
        symbol="SPY",
        display_symbol="SPY",
        name="SPDR S&P 500 ETF Trust",
        family=InstrumentFamily.SP500,
        asset_class=AssetClass.ETF,
        exchange="ARCA",
        analytics_source=AnalyticsSource.OPRA_EQUITY_OPTIONS,
        market_calendar=MarketCalendarId.US_EQUITIES,
        capabilities=_EQUITY_CAPS,
        contract_multiplier=100,
        price_precision=2,
    ),
    "SPX": InstrumentDefinition(
        symbol="SPX",
        display_symbol="SPX",
        name="S&P 500 Index",
        family=InstrumentFamily.SP500,
        asset_class=AssetClass.INDEX,
        exchange="CBOE",
        analytics_source=AnalyticsSource.OPRA_EQUITY_OPTIONS,
        market_calendar=MarketCalendarId.US_CASH_INDEX,
        capabilities=_EQUITY_CAPS,
        contract_multiplier=100,
        price_precision=2,
    ),
    "QQQ": InstrumentDefinition(
        symbol="QQQ",
        display_symbol="QQQ",
        name="Invesco QQQ Trust",
        family=InstrumentFamily.NASDAQ100,
        asset_class=AssetClass.ETF,
        exchange="NASDAQ",
        analytics_source=AnalyticsSource.OPRA_EQUITY_OPTIONS,
        market_calendar=MarketCalendarId.US_EQUITIES,
        capabilities=_EQUITY_CAPS,
        contract_multiplier=100,
        price_precision=2,
    ),
    "NDX": InstrumentDefinition(
        symbol="NDX",
        display_symbol="NDX",
        name="Nasdaq-100 Index",
        family=InstrumentFamily.NASDAQ100,
        asset_class=AssetClass.INDEX,
        exchange="NASDAQ",
        analytics_source=AnalyticsSource.OPRA_EQUITY_OPTIONS,
        market_calendar=MarketCalendarId.US_CASH_INDEX,
        capabilities=_EQUITY_CAPS,
        contract_multiplier=100,
        price_precision=2,
    ),
    "ES": InstrumentDefinition(
        symbol="ES",
        display_symbol="ES",
        name="E-mini S&P 500 Futures",
        family=InstrumentFamily.SP500,
        asset_class=AssetClass.FUTURE,
        exchange="CME",
        analytics_source=AnalyticsSource.CME_FUTURES_OPTIONS,
        market_calendar=MarketCalendarId.CME_EQUITY_INDEX,
        capabilities=_FUTURE_CAPS,
        contract_multiplier=50,
        price_increment=0.25,
        reference_source_symbol="SPX",
        price_precision=2,
    ),
    "NQ": InstrumentDefinition(
        symbol="NQ",
        display_symbol="NQ",
        name="E-mini Nasdaq-100 Futures",
        family=InstrumentFamily.NASDAQ100,
        asset_class=AssetClass.FUTURE,
        exchange="CME",
        analytics_source=AnalyticsSource.CME_FUTURES_OPTIONS,
        market_calendar=MarketCalendarId.CME_EQUITY_INDEX,
        capabilities=_FUTURE_CAPS,
        contract_multiplier=20,
        price_increment=0.25,
        # NQ reference mode intentionally has NO cash-index source wired: a
        # naive QQQ->NQ additive map is explicitly forbidden by the spec, and
        # NDX-derived projection is pending NDX analytics.  Left None so
        # reference mode stays unavailable for NQ until researched.
        reference_source_symbol=None,
        price_precision=2,
    ),
}

#: The equity/index instruments that ship enabled today.  Preserving this
#: exact set (order included) is what keeps the existing product unchanged.
DEFAULT_ENABLED_SYMBOLS: List[str] = ["SPY", "SPX", "QQQ", "NDX"]


def _flag(name: str, default: bool = False) -> bool:
    """Read a boolean feature flag from the environment.

    Deliberately self-contained (no import of :mod:`src.config`) so this
    module stays import-light and usable from tooling/tests that stub the
    environment directly.  Accepts the same truthy tokens as config's
    ``_getenv_bool``.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.split("#", 1)[0].strip().lower() in {"true", "1", "yes", "on"}


def get_instrument(symbol: str) -> Optional[InstrumentDefinition]:
    """Return the definition for ``symbol`` (case-insensitive), or None."""
    if not symbol:
        return None
    return _REGISTRY.get(symbol.strip().upper())


def require_instrument(symbol: str) -> InstrumentDefinition:
    """Return the definition for ``symbol`` or raise ``KeyError``."""
    inst = get_instrument(symbol)
    if inst is None:
        raise KeyError(f"Unknown instrument: {symbol!r}")
    return inst


def all_instruments() -> List[InstrumentDefinition]:
    """Every defined instrument, in registry (display) order."""
    return list(_REGISTRY.values())


def is_registered(symbol: str) -> bool:
    return get_instrument(symbol) is not None


def is_future(symbol: str) -> bool:
    inst = get_instrument(symbol)
    return inst is not None and inst.is_future


def asset_class_of(symbol: str) -> Optional[AssetClass]:
    inst = get_instrument(symbol)
    return inst.asset_class if inst else None


def contract_multiplier(symbol: str, default: int = 100) -> int:
    """Options contract multiplier / point value for ``symbol``.

    Falls back to the equity-option default (100) for unknown symbols so
    legacy call-sites that pass an unregistered ticker keep their current
    behavior rather than raising.
    """
    inst = get_instrument(symbol)
    return inst.contract_multiplier if inst else default


def analytics_source_of(symbol: str) -> Optional[AnalyticsSource]:
    inst = get_instrument(symbol)
    return inst.analytics_source if inst else None


def market_calendar_of(symbol: str) -> Optional[MarketCalendarId]:
    inst = get_instrument(symbol)
    return inst.market_calendar if inst else None


# ---------------------------------------------------------------------------
# Runtime availability — feature-flag driven.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InstrumentAvailability:
    """Runtime availability of an instrument, resolved from feature flags.

    ``available`` is the headline: can a user select this instrument and get
    *something* real?  ``true_analytics`` distinguishes genuine CME
    futures-options analytics from reference-only (projection) availability,
    so the UI can label them honestly.
    """

    symbol: str
    available: bool
    true_analytics: bool
    reference_mode: bool
    reasons: List[str] = field(default_factory=list)


def _es_analytics_enabled() -> bool:
    # True CME ES analytics require both the ingestion master switch and the
    # per-product analytics flag — a defense in depth so flipping one on
    # without the other never half-enables a product.
    return _flag("ENABLE_CME_INGESTION") and _flag("ENABLE_ES_ANALYTICS")


def _nq_analytics_enabled() -> bool:
    return _flag("ENABLE_CME_INGESTION") and _flag("ENABLE_NQ_ANALYTICS")


def get_availability(symbol: str) -> InstrumentAvailability:
    """Resolve runtime availability for ``symbol`` from the environment."""
    inst = get_instrument(symbol)
    if inst is None:
        return InstrumentAvailability(
            symbol=(symbol or "").strip().upper(),
            available=False,
            true_analytics=False,
            reference_mode=False,
            reasons=["unknown instrument"],
        )

    if inst.asset_class is not AssetClass.FUTURE:
        # Equities/indices ship enabled — unchanged behavior.
        return InstrumentAvailability(
            symbol=inst.symbol,
            available=True,
            true_analytics=True,
            reference_mode=False,
            reasons=[],
        )

    reasons: List[str] = []
    if inst.symbol == "ES":
        true_analytics = _es_analytics_enabled()
        reference_mode = _flag("ENABLE_ES_REFERENCE_MODE") and (
            inst.reference_source_symbol is not None
        )
    elif inst.symbol == "NQ":
        true_analytics = _nq_analytics_enabled()
        # NQ reference mode additionally requires a wired cash-index source,
        # which is intentionally absent until NDX-derived projection is built.
        reference_mode = _flag("ENABLE_NQ_REFERENCE_MODE") and (
            inst.reference_source_symbol is not None
        )
        if _flag("ENABLE_NQ_REFERENCE_MODE") and inst.reference_source_symbol is None:
            reasons.append(
                "NQ reference mode requested but no cash-index source is wired "
                "(naive QQQ->NQ mapping is disallowed; NDX-derived pending)"
            )
    else:  # pragma: no cover - only ES/NQ futures defined today
        true_analytics = False
        reference_mode = False

    if not true_analytics:
        reasons.append("CME futures-options analytics not enabled")
    if not reference_mode and not true_analytics:
        reasons.append("no reference-mode source available")

    return InstrumentAvailability(
        symbol=inst.symbol,
        available=bool(true_analytics or reference_mode),
        true_analytics=true_analytics,
        reference_mode=reference_mode,
        reasons=reasons,
    )


def enabled_symbols() -> List[str]:
    """Symbols a user may currently select, in registry order.

    Always includes the four equity/index instruments; adds ES/NQ only when
    their availability resolves truthy.
    """
    out: List[str] = []
    for inst in all_instruments():
        if inst.asset_class is not AssetClass.FUTURE:
            out.append(inst.symbol)
        elif get_availability(inst.symbol).available:
            out.append(inst.symbol)
    return out


def effective_capabilities(symbol: str) -> Dict[str, bool]:
    """Capabilities intersected with runtime availability.

    A capability is only reported ``True`` when the asset class supports it
    *and* the instrument is actually available for that mode.  This is what
    API/UI layers should consult to decide whether to render a surface or a
    clear "unavailable" state (rule #7 / spec Phase 11).
    """
    inst = get_instrument(symbol)
    if inst is None:
        return {}
    caps = inst.capabilities
    base = {
        "quotes": caps.quotes,
        "historical_quotes": caps.historical_quotes,
        "gex": caps.gex,
        "option_flow": caps.option_flow,
        "vanna_charm": caps.vanna_charm,
        "max_pain": caps.max_pain,
        "signals": caps.signals,
        "replay": caps.replay,
        "backtesting": caps.backtesting,
        "strategy_builder": caps.strategy_builder,
        "futures_reference_mode": caps.futures_reference_mode,
    }
    if inst.asset_class is not AssetClass.FUTURE:
        return base

    avail = get_availability(symbol)
    # For futures, analytics surfaces require *true* CME analytics; the
    # reference-mode capability is governed separately.
    for key in (
        "quotes",
        "historical_quotes",
        "gex",
        "option_flow",
        "vanna_charm",
        "max_pain",
        "signals",
        "replay",
        "backtesting",
        "strategy_builder",
    ):
        base[key] = base[key] and avail.true_analytics
    base["futures_reference_mode"] = caps.futures_reference_mode and avail.reference_mode
    return base
