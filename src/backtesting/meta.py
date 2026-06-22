"""Catalog metadata for the backtest configuration form.

Sources the pattern list from the live PlaybookEngine discovery (so the
backtester and the live engine never drift), the tradable underlyings from
config, and the available data window from the DB.
"""

from __future__ import annotations

import logging

from src.config import (
    BACKTEST_SIGNAL_COOLDOWN_MINUTES,
    DATA_RETENTION_DAYS,
    SIGNALS_UNDERLYINGS,
)

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "capital": 25_000.0,
    "risk_per_trade_pct": 2.0,
    "slippage_pct": 0.01,
    "commission_per_contract": 0.65,
    "max_concurrent": 3,
    # Greeks-aware sizing caps (Phase 5b); null ⇒ off.
    "max_net_delta": None,
    "max_net_vega": None,
    "cooldown_minutes": BACKTEST_SIGNAL_COOLDOWN_MINUTES,
    # Option-premium exit overlay (Phase 2); null ⇒ off, resolve on Card levels.
    "profit_target_pct": None,
    "stop_loss_pct": None,
    # Custom-strategy structure (Phase 4/5).
    "structure": "single",
    "width": 5,
    "wing": 5,
}

# Defined-risk structures a custom strategy can trade. ``neutral`` structures
# are non-directional and exit on the premium overlay; directional ones take a
# bullish/bearish direction.
STRATEGY_STRUCTURES = [
    {"id": "single", "label": "Single option (ATM)", "kind": "directional"},
    {"id": "vertical", "label": "Vertical spread (defined risk)", "kind": "directional"},
    {"id": "straddle", "label": "Long straddle (ATM call+put)", "kind": "neutral"},
    {"id": "strangle", "label": "Long strangle (OTM call+put)", "kind": "neutral"},
    {"id": "condor", "label": "Iron condor (sell strangle, buy wings)", "kind": "neutral"},
]

# Parameters a sweep can vary (Phase 6). ``scope`` is "any" (applies to any
# spec) or "strategy" (only custom-strategy specs). ``unit`` is a display hint.
# ``as_fraction`` marks axes whose spec value is a FRACTION (0.5), so the UI
# accepts a percent and divides by 100 — exactly like the single-run form's
# pctToFraction. Params without it take their raw number as typed (e.g.
# ``risk_per_trade_pct`` is stored as a percent number like 2.0, not a fraction).
SWEEP_PARAMS = [
    {"param": "profit_target_pct", "label": "Take profit", "unit": "%",
     "scope": "any", "as_fraction": True},
    {"param": "stop_loss_pct", "label": "Stop loss", "unit": "%",
     "scope": "any", "as_fraction": True},
    {"param": "risk_per_trade_pct", "label": "Risk / trade", "unit": "%", "scope": "any"},
    {"param": "max_concurrent", "label": "Max concurrent", "unit": "", "scope": "any"},
    {"param": "max_hold_minutes", "label": "Max hold", "unit": "min", "scope": "any"},
    {"param": "slippage_pct", "label": "Slippage", "unit": "", "scope": "any"},
    {"param": "max_net_delta", "label": "Max net Δ", "unit": "", "scope": "any"},
    {"param": "max_net_vega", "label": "Max net vega", "unit": "", "scope": "any"},
    {"param": "dte", "label": "DTE", "unit": "", "scope": "strategy"},
    {"param": "width", "label": "Spread width", "unit": "pts", "scope": "strategy"},
    {"param": "wing", "label": "Wing width", "unit": "pts", "scope": "strategy"},
    {"param": "target_offset_pct", "label": "Target offset", "unit": "%",
     "scope": "strategy", "as_fraction": True},
    {"param": "stop_offset_pct", "label": "Stop offset", "unit": "%",
     "scope": "strategy", "as_fraction": True},
]


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


# Catalog for the custom-strategy condition builder. Each entry describes one
# selectable field, its type, operators, an optional unit hint, and (for
# categorical fields) the allowed values.
def _strategy_fields() -> list[dict]:
    from src.backtesting.models import STRATEGY_CATEGORICAL_FIELDS

    numeric = [
        ("price", "Underlying price", "$"),
        ("msi", "MSI composite (0–100)", ""),
        ("net_gex", "Net GEX (total)", ""),
        ("net_gex_at_spot", "Net GEX at spot", ""),
        ("flip_distance_pct", "Distance to gamma flip", "%"),
        ("dist_to_call_wall_pct", "Distance to call wall (+ = above)", "%"),
        ("dist_to_put_wall_pct", "Distance to put wall (+ = below)", "%"),
        ("put_call_ratio", "Put/call ratio", ""),
        ("convexity_risk", "Convexity risk", ""),
        ("gamma_flip_point", "Gamma flip level", "$"),
        ("call_wall", "Call wall level", "$"),
        ("put_wall", "Put wall level", "$"),
        ("max_pain", "Max pain level", "$"),
        ("flip_distance", "Flip distance (raw)", ""),
    ]
    out = [
        {"field": f, "label": label, "type": "numeric",
         "ops": ["<", "<=", ">", ">=", "==", "!="], "unit": unit}
        for f, label, unit in numeric
    ]
    labels = {
        "net_gex_sign": "Net GEX sign",
        "msi_regime": "MSI regime",
    }
    for field, values in STRATEGY_CATEGORICAL_FIELDS.items():
        out.append({
            "field": field, "label": labels.get(field, field), "type": "categorical",
            "ops": ["==", "!="], "values": list(values),
        })
    return out


def build_meta(conn) -> dict:
    return {
        "underlyings": _underlyings(),
        "patterns": _pattern_catalog(),
        "strategy_fields": _strategy_fields(),
        "strategy_structures": list(STRATEGY_STRUCTURES),
        "sweep_params": list(SWEEP_PARAMS),
        "data_window": _data_window(conn),
        "defaults": dict(_DEFAULTS),
    }
