"""Helpers for resolving user-friendly symbol aliases to TradeStation symbols."""

import os
from typing import Dict, List


def _parse_alias_mapping(raw_mapping: str) -> Dict[str, str]:
    """Parse mapping env vars in the format: KEY=VALUE,KEY2=VALUE2."""
    mapping: Dict[str, str] = {}

    if not raw_mapping:
        return mapping

    for pair in raw_mapping.split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue

        alias, symbol = pair.split("=", 1)
        alias = alias.strip().upper()
        symbol = symbol.strip().upper()
        if alias and symbol:
            mapping[alias] = symbol

    return mapping


def _parse_float_mapping(raw_mapping: str) -> Dict[str, float]:
    """Parse a KEY=FLOAT,KEY2=FLOAT2 env var, skipping unparseable entries.

    A malformed value is dropped rather than raised on, so one bad operator
    edit degrades to the built-in default for that key instead of taking the
    process down at import time.
    """
    mapping: Dict[str, float] = {}
    for key, value in _parse_alias_mapping(raw_mapping).items():
        try:
            mapping[key] = float(value)
        except (TypeError, ValueError):
            continue
    return mapping


def get_symbol_aliases() -> Dict[str, str]:
    """Return alias mapping from env (SYMBOL_ALIASES)."""
    return _parse_alias_mapping(os.getenv("SYMBOL_ALIASES", ""))


def get_option_root_aliases() -> Dict[str, str]:
    """Return option-root mapping from env (OPTION_ROOT_ALIASES)."""
    return _parse_alias_mapping(os.getenv("OPTION_ROOT_ALIASES", ""))


# Cash indices (SPX, NDX, RUT, DJX) have no transactional volume of their
# own — only the constituent stocks and the index options trade.  When we
# need a "volume" series for VWAP-style calculations on an index, we fall
# back to a tracking ETF whose intraday volume profile is essentially
# identical to the index's price action.  Override the default mapping
# with the ``INDEX_VOLUME_PROXIES`` env var (same KEY=VALUE,KEY2=VALUE2
# format as ``SYMBOL_ALIASES``).
_DEFAULT_INDEX_VOLUME_PROXIES: Dict[str, str] = {
    "SPX": "SPY",
    "NDX": "QQQ",
    "RUT": "IWM",
    "DJX": "DIA",
}


def get_index_volume_proxies() -> Dict[str, str]:
    """Return the index → ETF volume-proxy mapping (env-overridable)."""
    overrides = _parse_alias_mapping(os.getenv("INDEX_VOLUME_PROXIES", ""))
    merged = dict(_DEFAULT_INDEX_VOLUME_PROXIES)
    merged.update(overrides)
    return merged


def resolve_volume_proxy(symbol: str) -> str | None:
    """Return the ETF whose volume profile should stand in for ``symbol``.

    Returns None when ``symbol`` already has its own volume (equities/ETFs)
    or is not in the proxy map.
    """
    normalized = (symbol or "").strip().upper()
    if not normalized:
        return None
    return get_index_volume_proxies().get(normalized)


def is_cash_index(symbol: str) -> bool:
    """True when ``symbol`` is a cash index (SPX, NDX, RUT, DJX, …).

    A cash index has no transactional volume of its own and — unlike a
    tracking ETF — has underlying price data only during the regular cash
    session (09:30–16:00 ET), even though its options trade extended /
    global hours.  The volume-proxy map is the single source of truth for
    "this symbol is a cash index", so callers stay consistent with the
    rest of the codebase (and any ``INDEX_VOLUME_PROXIES`` override).
    """
    return resolve_volume_proxy(symbol) is not None


# Cash indices print an underlying level only during the regular cash
# session (09:30–16:00 ET).  For "what is it doing right now" display
# surfaces OUTSIDE that session, we substitute the equivalent CME
# equity-index future (SPX→ES, NDX→NQ, …), which trades nearly 24h.
#
# This is a DISPLAY-ONLY substitution: it feeds the header quote, the
# quote cards, and the candlestick charts, and NOTHING else.  GEX,
# Greeks, signals, settlement, session-levels and every DB write continue
# to key on the cash-index symbol.  The futures data is fetched at request
# time and is NOT persisted.
#
# Override with the ``INDEX_FUTURES_MAP`` env var (same KEY=VALUE format as
# ``SYMBOL_ALIASES``).  Values are TradeStation CONTINUOUS-contract symbols
# (the ``@`` form, e.g. ``@ES``) so contract roll is handled upstream by
# the provider.
_DEFAULT_INDEX_FUTURES: Dict[str, str] = {
    "SPX": "@ES",
    "NDX": "@NQ",
    "RUT": "@RTY",
    "DJX": "@YM",
}


def get_index_futures() -> Dict[str, str]:
    """Return the index → continuous-future display mapping (env-overridable)."""
    overrides = _parse_alias_mapping(os.getenv("INDEX_FUTURES_MAP", ""))
    merged = dict(_DEFAULT_INDEX_FUTURES)
    merged.update(overrides)
    return merged


def resolve_index_future(symbol: str) -> str | None:
    """Return the futures symbol to DISPLAY for ``symbol`` outside cash hours.

    Returns None when ``symbol`` is not a cash index with a mapped future —
    i.e. there is nothing to substitute.  DISPLAY-ONLY: callers must keep
    using the original ``symbol`` for every non-display purpose (GEX,
    Greeks, signals, settlement, persistence).
    """
    normalized = (symbol or "").strip().upper()
    if not normalized:
        return None
    return get_index_futures().get(normalized)


# ---------------------------------------------------------------------------
# Futures as first-class underlyings (ES / NQ)
# ---------------------------------------------------------------------------
#
# ``_DEFAULT_INDEX_FUTURES`` above answers "which future do I DISPLAY in
# place of this index overnight".  This section answers the INVERSE — "which
# index's option chain describes this future's dealer positioning" — and it
# is what makes ES / NQ selectable symbols in their own right rather than a
# night-time skin on SPX / NDX.
#
# ZeroGEX computes gamma from INDEX option chains (SPX, NDX) and never from
# options on futures: ES and SPX track the same index, so it is the same
# dealer book.  What differs is the price axis — ES trades at a cost-of-carry
# premium to SPX.  Every ES / NQ surface is therefore the corresponding index
# surface with its PRICE-SPACE fields (strikes, walls, flip, max pain, spot)
# carried across by the live basis ratio.  The dollar exposures are NOT
# rescaled: dealer exposure is a property of the option book, not of the axis
# you plot it on.  The ratio itself lives in ``src/jobs/futures_projection.py``.
#
# Override with ``FUTURES_UNDERLYINGS_MAP`` (same KEY=VALUE format as
# ``SYMBOL_ALIASES``), e.g. ``FUTURES_UNDERLYINGS_MAP=ES=SPX,NQ=NDX``.
_DEFAULT_FUTURES_UNDERLYINGS: Dict[str, str] = {
    "ES": "SPX",
    "NQ": "NDX",
    "RTY": "RUT",
    "YM": "DJX",
}

# Minimum price increment of each future.  Projected levels are rounded to
# this so a published level is a price a trader can actually work an order
# at.  Override with ``FUTURES_TICK_SIZES`` (KEY=VALUE, float values).
_DEFAULT_FUTURES_TICK_SIZES: Dict[str, float] = {
    "ES": 0.25,
    "NQ": 0.25,
    "RTY": 0.10,
    "YM": 1.0,
}


def get_futures_underlyings() -> Dict[str, str]:
    """Return the future → backing cash-index mapping (env-overridable)."""
    overrides = _parse_alias_mapping(os.getenv("FUTURES_UNDERLYINGS_MAP", ""))
    merged = dict(_DEFAULT_FUTURES_UNDERLYINGS)
    merged.update(overrides)
    return merged


def get_futures_tick_sizes() -> Dict[str, float]:
    """Return the future → tick-size mapping (env-overridable)."""
    overrides = _parse_float_mapping(os.getenv("FUTURES_TICK_SIZES", ""))
    merged = dict(_DEFAULT_FUTURES_TICK_SIZES)
    merged.update(overrides)
    return merged


def is_futures_symbol(symbol: str) -> bool:
    """True when ``symbol`` is a first-class future (ES, NQ, …).

    Note this is the CANONICAL alias (``ES``), not the TradeStation
    continuous-contract symbol (``@ES``) that ``resolve_index_future``
    returns — the two namespaces are deliberately distinct so a projected
    ES surface can never be confused with the raw futures feed.
    """
    normalized = (symbol or "").strip().upper()
    if not normalized:
        return False
    return normalized in get_futures_underlyings()


def resolve_futures_index(symbol: str) -> str | None:
    """Return the cash index whose options back ``symbol``, or None.

    ``ES -> SPX``, ``NQ -> NDX``.  Returns None for anything that is not a
    configured future, so callers can treat "not a future" and "a future
    with no backing index" identically.
    """
    normalized = (symbol or "").strip().upper()
    if not normalized:
        return None
    return get_futures_underlyings().get(normalized)


def resolve_futures_alias(index_symbol: str) -> str | None:
    """Inverse of :func:`resolve_futures_index`: ``SPX -> ES``, ``NDX -> NQ``.

    Returns None when the index has no first-class future configured.  When
    two futures map to the same index (a misconfiguration), the first in
    map order wins deterministically.
    """
    normalized = (index_symbol or "").strip().upper()
    if not normalized:
        return None
    for future, index in get_futures_underlyings().items():
        if index == normalized:
            return future
    return None


def resolve_futures_tick(symbol: str) -> float | None:
    """Tick size for a first-class future, or None when unknown."""
    normalized = (symbol or "").strip().upper()
    if not normalized:
        return None
    return get_futures_tick_sizes().get(normalized)


def round_to_tick(value: float, tick: float | None) -> float:
    """Round ``value`` to the nearest multiple of ``tick``.

    A non-positive or missing tick returns ``value`` unchanged, so callers
    can pass an unknown symbol's tick straight through without branching.
    """
    if tick is None or tick <= 0:
        return value
    return round(round(value / tick) * tick, 10)


def resolve_symbol(symbol_or_alias: str) -> str:
    """Resolve a symbol or alias (case-insensitive) to TradeStation symbol."""
    normalized = symbol_or_alias.strip().upper()
    if not normalized:
        return normalized

    aliases = get_symbol_aliases()
    return aliases.get(normalized, normalized)


def parse_underlyings(raw_underlyings: str) -> List[str]:
    """Parse comma-separated underlyings and resolve aliases."""
    resolved: List[str] = []
    seen = set()

    for item in raw_underlyings.split(","):
        item = item.strip()
        if not item:
            continue

        symbol = resolve_symbol(item)
        if symbol and symbol not in seen:
            seen.add(symbol)
            resolved.append(symbol)

    return resolved


def get_canonical_symbol(ts_symbol: str) -> str:
    """Reverse-lookup: return the user alias for a resolved TradeStation symbol.

    e.g. "$SPX.X" → "SPX" if SYMBOL_ALIASES contains SPX=$SPX.X, else returns ts_symbol unchanged.
    """
    normalized = ts_symbol.strip().upper()
    if not normalized:
        return normalized
    reverse = {v: k for k, v in get_symbol_aliases().items()}
    return reverse.get(normalized, normalized)


def resolve_option_root(underlying: str) -> str:
    """Resolve option root for a given underlying, defaulting to underlying itself."""
    normalized = underlying.strip().upper()
    if not normalized:
        return normalized

    option_roots = get_option_root_aliases()
    return option_roots.get(normalized, normalized)


def resolve_underlying_from_option_root(option_root: str) -> str:
    """Inverse of the option-symbol root resolution.

    Building an option symbol resolves the chain
    ``underlying -> resolve_symbol (SYMBOL_ALIASES) -> TS symbol ->
    resolve_option_root (OPTION_ROOT_ALIASES) -> root``. This reverses both
    hops to recover the ``underlying_quotes`` symbol a contract settles
    against — needed by the expired-leg intrinsic fallback, which prices
    against the underlying's close, not the (non-existent) option-root quote.

    Example: ``SPXW -> SPX`` (SPX's 0DTE weekly chain settles against the SPX
    cash index). Roots that are already their own underlying (``SPY`` /
    ``QQQ`` / ``IWM``, and the monthly ``SPX`` root) carry no alias and are
    returned unchanged.
    """
    root = (option_root or "").strip().upper()
    if not root:
        return root

    # Hop 1: option root -> TS symbol(s), via reversed OPTION_ROOT_ALIASES.
    ts_for_root = [
        ts
        for ts, mapped_root in get_option_root_aliases().items()
        if mapped_root.strip().upper() == root
    ]
    if not ts_for_root:
        return root

    # Hop 2: TS symbol -> canonical underlying, via reversed SYMBOL_ALIASES
    # (plus the monthly-chain alias map that wires the AM-settled root).
    reverse_symbol: Dict[str, str] = {}
    for canon, ts in get_symbol_aliases().items():
        reverse_symbol.setdefault(ts.strip().upper(), canon)
    for canon, ts in get_monthly_underlying_aliases().items():
        reverse_symbol.setdefault(ts.strip().upper(), canon)

    for ts in ts_for_root:
        canon = reverse_symbol.get(ts.strip().upper())
        if canon:
            return canon
    return root


def get_monthly_underlying_aliases() -> Dict[str, str]:
    """Return canonical-symbol -> TS-monthly-chain-symbol mapping.

    Source: ``INGEST_MONTHLY_UNDERLYING_ALIASES`` (same KEY=VAL,KEY2=VAL2
    format as ``SYMBOL_ALIASES``).

    Index options are listed under TWO separate TradeStation chains: the
    weekly PM-settled chain (e.g. ``$SPXW.X`` -> root ``SPXW``) and the
    monthly AM-settled chain (e.g. ``$SPX.X`` -> root ``SPX``). The primary
    weekly chain is wired via ``SYMBOL_ALIASES`` + ``OPTION_ROOT_ALIASES``;
    this map tells the ingestion engine which TS symbol to query for the
    *monthly* expansion when ``INGEST_MONTHLY_EXPIRATIONS`` > 0.

    Example: ``INGEST_MONTHLY_UNDERLYING_ALIASES=SPX=$SPX.X,NDX=$NDX.X``
    """
    return _parse_alias_mapping(os.getenv("INGEST_MONTHLY_UNDERLYING_ALIASES", ""))


def resolve_monthly_underlying(canonical_symbol: str) -> str | None:
    """Return the TS symbol used to query the monthly chain, or None.

    ``canonical_symbol`` is the user-facing alias (e.g. ``SPX``), NOT the
    TS symbol. Returns None when no monthly chain is mapped — the caller
    should skip the monthly expansion in that case.
    """
    normalized = (canonical_symbol or "").strip().upper()
    if not normalized:
        return None
    return get_monthly_underlying_aliases().get(normalized)
