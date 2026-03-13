"""Helpers for resolving user-friendly symbol aliases to TradeStation symbols."""

import os
from typing import Dict, List


def _parse_alias_mapping(raw_mapping: str) -> Dict[str, str]:
    """Parse SYMBOL_ALIASES env var in the format: ALIAS=SYMBOL,ALIAS2=SYMBOL2."""
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

