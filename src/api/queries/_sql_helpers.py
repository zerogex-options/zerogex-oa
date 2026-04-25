"""Shared SQL fragments and helpers for query mixins.

Lives in this leaf module (rather than ``src.api.database``) so that both
``DatabaseManager`` and the query mixins it composes can import from a
single source without a circular import.

Every constant in this file is a closed allowlist of literal SQL fragments.
Callers may pick a key (a timeframe name, a sort mode, etc.) but the SQL
fragment itself is never built from user input. If you find yourself
wanting to interpolate anything else into an f-string SQL query, route it
through ``$N`` bind parameters instead.
"""

from __future__ import annotations

from typing import Final, Mapping

_TIMEFRAME_ALIASES: Final[Mapping[str, str]] = {
    "1hour": "1hr",
}

_BUCKET_EXPRS: Final[Mapping[str, str]] = {
    "1min": "date_trunc('minute', timestamp)",
    "5min": (
        "date_trunc('hour', timestamp) + "
        "FLOOR(EXTRACT(MINUTE FROM timestamp) / 5) * INTERVAL '5 minutes'"
    ),
    "15min": (
        "date_trunc('hour', timestamp) + "
        "FLOOR(EXTRACT(MINUTE FROM timestamp) / 15) * INTERVAL '15 minutes'"
    ),
    "1hr": "date_trunc('hour', timestamp)",
    "1day": "date_trunc('day', timestamp)",
}

_INTERVAL_EXPRS: Final[Mapping[str, str]] = {
    "1min": "INTERVAL '1 minute'",
    "5min": "INTERVAL '5 minutes'",
    "15min": "INTERVAL '15 minutes'",
    "1hr": "INTERVAL '1 hour'",
    "1day": "INTERVAL '1 day'",
}

_VIEW_SUFFIXES: Final[Mapping[str, str]] = {
    "1min": "1min",
    "5min": "5min",
    "15min": "15min",
    "1hr": "1hr",
    "1day": "1day",
}

_GEX_BY_STRIKE_ORDER_CLAUSES: Final[Mapping[str, str]] = {
    "distance": "ORDER BY ABS(g.strike - spot.close) ASC",
    "impact": "ORDER BY ABS(g.net_gex) DESC",
}


def _normalize_timeframe(timeframe: str) -> str:
    normalized = (timeframe or "1min").lower()
    return _TIMEFRAME_ALIASES.get(normalized, normalized)


def _bucket_expr(timeframe: str) -> str:
    """Return a literal SQL fragment that buckets the ``timestamp`` column.

    Raises ValueError for any timeframe outside the allowlist; the result
    is always one of the fixed strings in ``_BUCKET_EXPRS``, so callers can
    safely interpolate it into an f-string.
    """
    timeframe = _normalize_timeframe(timeframe)
    try:
        return _BUCKET_EXPRS[timeframe]
    except KeyError:
        raise ValueError(f"Unsupported timeframe: {timeframe}") from None


def _interval_expr(timeframe: str) -> str:
    """Return a literal ``INTERVAL '...'`` fragment for the given timeframe."""
    timeframe = _normalize_timeframe(timeframe)
    try:
        return _INTERVAL_EXPRS[timeframe]
    except KeyError:
        raise ValueError(f"Unsupported timeframe: {timeframe}") from None


def _timeframe_view_suffix(timeframe: str) -> str:
    timeframe = _normalize_timeframe(timeframe)
    try:
        return _VIEW_SUFFIXES[timeframe]
    except KeyError:
        raise ValueError(f"Unsupported timeframe: {timeframe}") from None


def _gex_by_strike_order_clause(sort_by: str) -> str:
    """Return a literal ORDER BY fragment for /api/gex/by-strike sorting."""
    try:
        return _GEX_BY_STRIKE_ORDER_CLAUSES[sort_by]
    except KeyError:
        raise ValueError(f"Unsupported sort_by: {sort_by!r}") from None
