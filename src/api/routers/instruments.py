"""Instrument metadata API — the registry surfaced to clients.

``GET /api/instruments``          — all instruments, capabilities, availability
``GET /api/instruments/{symbol}`` — one instrument (+ resolved contracts for futures)

This lets the frontend reconcile its own (mirror) registry against the
server's runtime feature-flag state, so a futures instrument that is defined
but not yet enabled is described honestly rather than appearing operational
(spec Phase 8).  Pure metadata + calendar math — no market data, no DB.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from src import instruments as reg
from src.futures import security_definitions as sd
from src.futures.resolver import ContractResolver

router = APIRouter(prefix="/api/instruments", tags=["Instruments"])


def _instrument_dict(inst: reg.InstrumentDefinition) -> Dict[str, Any]:
    avail = reg.get_availability(inst.symbol)
    return {
        "symbol": inst.symbol,
        "display_symbol": inst.display_symbol,
        "name": inst.name,
        "family": inst.family.value,
        "asset_class": inst.asset_class.value,
        "exchange": inst.exchange,
        "currency": inst.currency,
        "analytics_source": inst.analytics_source.value,
        "market_calendar": inst.market_calendar.value,
        "contract_multiplier": inst.contract_multiplier,
        "price_increment": inst.price_increment,
        "price_precision": inst.price_precision,
        "reference_source_symbol": inst.reference_source_symbol,
        "capabilities": reg.effective_capabilities(inst.symbol),
        "availability": {
            "available": avail.available,
            "true_analytics": avail.true_analytics,
            "reference_mode": avail.reference_mode,
            "reasons": avail.reasons,
        },
    }


def _feature_flags() -> Dict[str, bool]:
    # Read live so the endpoint reflects current env without a restart.
    return {
        "ENABLE_CME_INGESTION": reg._flag("ENABLE_CME_INGESTION"),
        "ENABLE_ES_ANALYTICS": reg._flag("ENABLE_ES_ANALYTICS"),
        "ENABLE_NQ_ANALYTICS": reg._flag("ENABLE_NQ_ANALYTICS"),
        "ENABLE_ES_REFERENCE_MODE": reg._flag("ENABLE_ES_REFERENCE_MODE"),
        "ENABLE_NQ_REFERENCE_MODE": reg._flag("ENABLE_NQ_REFERENCE_MODE"),
    }


@router.get("")
async def list_instruments() -> Dict[str, Any]:
    return {
        "instruments": [_instrument_dict(i) for i in reg.all_instruments()],
        "enabled_symbols": reg.enabled_symbols(),
        "feature_flags": _feature_flags(),
    }


def _resolved_contracts(symbol: str, as_of: Optional[date] = None) -> Dict[str, Any]:
    """Front/next dated contracts for a futures product (calendar-derived).

    These are contract *definitions* from the known quarterly listing cycle —
    calendar math, not market data — so returning them fabricates nothing.
    Volume/OI are omitted (only a real feed provides those).
    """
    product = sd.get_product(symbol)
    if product is None:
        return {}
    today = as_of or datetime.now(timezone.utc).date()
    contracts = sd.generate_quarterly_futures(symbol, today, count=4)
    resolver = ContractResolver(contracts)
    now = datetime.now(timezone.utc)
    front = resolver.resolve_front_contract(symbol, now)
    nxt = resolver.resolve_next_contract(symbol, now)

    def _c(c: Optional[object]) -> Optional[Dict[str, Any]]:
        if c is None:
            return None
        return {
            "contract_id": c.contract_id,  # type: ignore[attr-defined]
            "label": c.label,  # type: ignore[attr-defined]
            "expiration": c.expiration.isoformat(),  # type: ignore[attr-defined]
            "multiplier": c.multiplier,  # type: ignore[attr-defined]
            "tick_size": c.tick_size,  # type: ignore[attr-defined]
        }

    return {
        "front_contract": _c(front),
        "next_contract": _c(nxt),
        "available_contracts": [_c(c) for c in contracts],
        "contract_source": "cme_quarterly_listing_schedule",
    }


@router.get("/{symbol}")
async def get_instrument(symbol: str) -> Dict[str, Any]:
    inst = reg.get_instrument(symbol)
    if inst is None:
        raise HTTPException(status_code=404, detail=f"Unknown instrument: {symbol}")
    payload = _instrument_dict(inst)

    if inst.is_future:
        payload["contracts"] = _resolved_contracts(inst.symbol)
        avail = reg.get_availability(inst.symbol)
        limitations: List[str] = []
        if not avail.true_analytics:
            limitations.append(
                "True CME futures-options analytics are not enabled; no live "
                "GEX/flow/greeks are served for this instrument."
            )
        if inst.symbol == "NQ" and inst.reference_source_symbol is None:
            limitations.append(
                "NQ reference mode is unavailable (no NDX-derived source wired; "
                "a naive QQQ->NQ mapping is disallowed)."
            )
        payload["known_limitations"] = limitations
    return payload
