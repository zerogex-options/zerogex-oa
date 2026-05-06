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
