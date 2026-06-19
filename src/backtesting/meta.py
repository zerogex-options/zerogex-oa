"""Catalog metadata for the backtest configuration form.

Sources the pattern list from the live PlaybookEngine discovery (so the
backtester and the live engine never drift), the tradable underlyings from
config, and the available data window from the DB.
"""

from __future__ import annotations

import logging

from src.config import DATA_RETENTION_DAYS, SIGNALS_UNDERLYINGS

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "capital": 25_000.0,
    "risk_per_trade_pct": 2.0,
    "slippage_pct": 0.01,
    "commission_per_contract": 0.65,
    "max_concurrent": 3,
}


def _pattern_catalog() -> list[dict]:
    """Discover the built-in playbook patterns and describe each."""
    try:
        from src.signals.playbook.engine import PlaybookEngine

        patterns = PlaybookEngine._discover_builtin_patterns()
    except Exception:  # pragma: no cover - discovery is best-effort for the form
        logger.warning("backtest meta: pattern discovery failed", exc_info=True)
        return []
    out = []
    for p in patterns:
        doc = (getattr(p, "__doc__", "") or type(p).__doc__ or "").strip()
        description = doc.split("\n", 1)[0][:200] if doc else ""
        out.append(
            {
                "id": getattr(p, "id", "") or "",
                "name": getattr(p, "name", "") or getattr(p, "id", ""),
                "tier": getattr(p, "tier", "") or "n/a",
                "description": description,
            }
        )
    out.sort(key=lambda d: (d["tier"], d["name"]))
    return out


def _underlyings() -> list[str]:
    raw = SIGNALS_UNDERLYINGS or "SPY"
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def _data_window(conn) -> dict:
    """Earliest/latest option_chains timestamps available to a backtest."""
    earliest = latest = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM option_chains")
        row = cur.fetchone()
        if row:
            earliest = row[0].date().isoformat() if row[0] else None
            latest = row[1].date().isoformat() if row[1] else None
    except Exception:  # pragma: no cover
        logger.warning("backtest meta: data window query failed", exc_info=True)
    return {
        "earliest": earliest,
        "latest": latest,
        "retention_days": DATA_RETENTION_DAYS,
    }


def build_meta(conn) -> dict:
    return {
        "underlyings": _underlyings(),
        "patterns": _pattern_catalog(),
        "data_window": _data_window(conn),
        "defaults": dict(_DEFAULTS),
    }
