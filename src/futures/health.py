"""Futures observability — the /health futures section.

Produces a structured, secret-free snapshot of futures readiness: whether CME
ingestion is enabled, the (pending) real-feed status, and per-product
availability with reasons.  Consumed by the API health surface and alert-ready
metrics (spec Phase 16).  Deliberately dependency-light so it can be called
from a health check without touching the DB.
"""

from __future__ import annotations

from typing import Any, Dict

from src import instruments as reg
from src.futures.providers import REAL_PROVIDER_STATUS


def futures_health_section() -> Dict[str, Any]:
    """Structured futures health for /api/health (no secrets, no DB)."""
    products: Dict[str, Any] = {}
    for symbol in ("ES", "NQ"):
        avail = reg.get_availability(symbol)
        products[symbol] = {
            "available": avail.available,
            "true_analytics": avail.true_analytics,
            "reference_mode": avail.reference_mode,
            "reasons": avail.reasons,
            # Until a licensed adapter is wired, the real feed is pending — so a
            # health check never implies live CME data that isn't there.
            "real_feed_status": REAL_PROVIDER_STATUS,
        }
    return {
        "enabled": reg._flag("ENABLE_CME_INGESTION"),
        "real_feed": REAL_PROVIDER_STATUS,
        "products": products,
    }
