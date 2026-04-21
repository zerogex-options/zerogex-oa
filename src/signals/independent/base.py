"""Shared primitives for independent (non-composite) signals."""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.components.utils import realized_sigma

# Default normalizers (fallback when per-symbol cache is unavailable).
DEFAULT_FLOW_FLUX_NORM = float(os.getenv("SIGNAL_FLOW_FLUX_NORM", "250000"))
DEFAULT_NET_GEX_DELTA_NORM = float(os.getenv("SIGNAL_NET_GEX_DELTA_NORM", "500000000"))

# Confluence quality cutoff: >= this fraction counts as clustered enough.
CONFLUENCE_MAX_GAP_PCT = float(os.getenv("SIGNAL_CONFLUENCE_MAX_GAP_PCT", "0.005"))

# Vol-scaled breakout buffer controls trap-detection noise floor.
BREAKOUT_BUFFER_MIN = float(os.getenv("SIGNAL_BREAKOUT_BUFFER_MIN", "0.001"))
BREAKOUT_BUFFER_VOL_MULT = float(os.getenv("SIGNAL_BREAKOUT_BUFFER_VOL_MULT", "0.15"))


@dataclass
class IndependentSignalResult:
    name: str
    score: float
    context: dict


def nearest_above(levels: list[Optional[float]], close: float) -> Optional[float]:
    candidates = [lv for lv in levels if lv is not None and lv > close]
    return min(candidates) if candidates else None


def nearest_below(levels: list[Optional[float]], close: float) -> Optional[float]:
    candidates = [lv for lv in levels if lv is not None and lv < close]
    return max(candidates) if candidates else None


def tanh_scaled(x: float) -> float:
    """Cheap tanh that clips to ±1 and avoids overflow."""
    if x > 20.0:
        return 1.0
    if x < -20.0:
        return -1.0
    return math.tanh(x)


def flow_flux_norm(ctx: MarketContext) -> float:
    norms = (ctx.extra or {}).get("normalizers") or {}
    val = norms.get("call_flow_delta") or norms.get("put_flow_delta")
    if val and val > 0:
        return float(val)
    return DEFAULT_FLOW_FLUX_NORM


def gex_delta_norm(ctx: MarketContext) -> float:
    norms = (ctx.extra or {}).get("normalizers") or {}
    val = norms.get("net_gex_delta")
    if val and val > 0:
        return float(val)
    return DEFAULT_NET_GEX_DELTA_NORM


def realized_pct_sigma(ctx: MarketContext) -> float:
    """Per-bar realized sigma of returns, as a fraction."""
    return realized_sigma(ctx.recent_closes, 60)


def vix_regime(vix_level: Optional[float]) -> str:
    if vix_level is None:
        return "unknown"
    if vix_level < 15.0:
        return "dead"
    if vix_level < 22.0:
        return "normal"
    if vix_level < 30.0:
        return "elevated"
    return "panic"
