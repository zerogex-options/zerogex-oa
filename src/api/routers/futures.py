"""Futures contract & reference-level API.

``GET /api/futures/{product}/contracts``    — dated contract schedule
``GET /api/futures/{product}/roll-state``   — front/next + roll recommendation
``GET /api/futures/reference-levels``        — SPX-derived ES reference levels

Contracts and roll-state are derived from the known CME quarterly listing
schedule (calendar math) — they fabricate no market data (volume/OI are only
populated from a real feed).  Reference-levels projects *real* SPX
option-derived levels onto the ES price scale via a synchronized basis and is
tagged ``cash_index_projection`` so it is never mistaken for CME
futures-options GEX (spec Phases 8 & 10).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from src import instruments as reg
from src.futures import reference_mode as rm
from src.futures import security_definitions as sd
from src.futures.resolver import ContractResolver, RollPolicy

router = APIRouter(prefix="/api/futures", tags=["Futures"])


def _get_db():
    from ..main import db_manager

    return db_manager


def _require_future(product: str) -> reg.InstrumentDefinition:
    inst = reg.get_instrument(product)
    if inst is None or not inst.is_future:
        raise HTTPException(status_code=404, detail=f"Unknown futures product: {product}")
    return inst


def _contract_payload(c: Any) -> Dict[str, Any]:
    return {
        "contract_id": c.contract_id,
        "label": c.label,
        "product_symbol": c.product_symbol,
        "exchange_symbol": c.exchange_symbol,
        "contract_month": c.contract_month,
        "contract_year": c.contract_year,
        "expiration": c.expiration.isoformat(),
        "multiplier": c.multiplier,
        "tick_size": c.tick_size,
        "currency": c.currency,
        "status": c.status.value,
        "is_front_contract": c.is_front_contract,
        "is_next_contract": c.is_next_contract,
        # volume/OI intentionally null — only a real feed provides them.
        "volume": c.volume,
        "open_interest": c.open_interest,
    }


@router.get("/{product}/contracts")
async def list_contracts(product: str, count: int = Query(4, ge=1, le=12)) -> Dict[str, Any]:
    inst = _require_future(product)
    today = datetime.now(timezone.utc).date()
    contracts = sd.generate_quarterly_futures(inst.symbol, today, count=count)
    avail = reg.get_availability(inst.symbol)
    return {
        "product": inst.symbol,
        "asset_class": inst.asset_class.value,
        "exchange": inst.exchange,
        "analytics_source": inst.analytics_source.value,
        "contract_source": "cme_quarterly_listing_schedule",
        "availability": {
            "available": avail.available,
            "true_analytics": avail.true_analytics,
            "reference_mode": avail.reference_mode,
        },
        "contracts": [_contract_payload(c) for c in contracts],
    }


@router.get("/{product}/roll-state")
async def roll_state(product: str) -> Dict[str, Any]:
    inst = _require_future(product)
    today = datetime.now(timezone.utc).date()
    now = datetime.now(timezone.utc)
    contracts = sd.generate_quarterly_futures(inst.symbol, today, count=4)
    resolver = ContractResolver(contracts, policy=RollPolicy.HYBRID)
    state = resolver.get_roll_state(inst.symbol, now)
    if state is None:
        raise HTTPException(status_code=404, detail="No active contract")
    return {
        "product": state.product,
        "policy": state.policy.value,
        "roll_window_active": state.roll_window_active,
        "reason": state.reason,
        "front_contract": _contract_payload(state.front_contract),
        "next_contract": (_contract_payload(state.next_contract) if state.next_contract else None),
        "recommended_display_contract": state.recommended_display_contract.contract_id,
        "calendar_spread": state.calendar_spread,
        "volume_ratio": state.volume_ratio,
        "open_interest_ratio": state.open_interest_ratio,
        "timestamp": state.timestamp.isoformat(),
    }


def _extract_levels(summary: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    if not summary:
        return {}
    keys = {
        "gamma_flip": ("gamma_flip", "gamma_flip_point"),
        "call_wall": ("call_wall",),
        "put_wall": ("put_wall",),
        "max_pain": ("max_pain",),
    }
    out: Dict[str, Optional[float]] = {}
    for name, candidates in keys.items():
        for k in candidates:
            if k in summary and summary[k] is not None:
                try:
                    out[name] = float(summary[k])
                except (TypeError, ValueError):
                    out[name] = None
                break
    return out


def _to_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


@router.get("/reference-levels")
async def reference_levels(
    source: str = Query("SPX"),
    target: str = Query("ES"),
    contract: str = Query("front"),
) -> Dict[str, Any]:
    """Project cash-index (SPX) option levels onto the ES futures scale.

    Requires the target's reference mode to be enabled AND a wired cash-index
    source.  Reads REAL SPX analytics + the ES display price already ingested;
    never fabricates.  Returns an explicit ``unavailable`` quality when the
    inputs are missing or unsynchronized rather than a misleading number.
    """
    target_u = target.strip().upper()
    inst = reg.get_instrument(target_u)
    if inst is None or not inst.is_future:
        raise HTTPException(status_code=404, detail=f"Unknown futures product: {target}")

    avail = reg.get_availability(target_u)
    if not avail.reference_mode:
        # Honest 409: the feature exists but is not enabled / has no source.
        raise HTTPException(
            status_code=409,
            detail={
                "error": "reference_mode_unavailable",
                "target": target_u,
                "reasons": avail.reasons or ["reference mode not enabled for this instrument"],
            },
        )

    source_symbol = rm.reference_source_for(target_u)
    if source_symbol is None or source_symbol.upper() != source.strip().upper():
        raise HTTPException(
            status_code=400,
            detail=f"{target_u} reference mode requires source={source_symbol}",
        )

    db = _get_db()
    summary: Optional[Dict[str, Any]] = None
    future_quote: Optional[Dict[str, Any]] = None
    if db is not None:
        try:
            summary = await db.get_latest_gex_summary(source_symbol)
        except Exception:
            summary = None
        try:
            # ES bars are stored keyed by the cash index symbol (SPX).
            future_quote = await db.get_latest_future_quote(source_symbol, None)
        except Exception:
            future_quote = None

    levels = _extract_levels(summary)
    source_price = None
    source_ts = None
    if summary:
        source_price = summary.get("underlying_price") or summary.get("spot")
        source_ts = _to_dt(summary.get("timestamp") or summary.get("as_of"))
    target_price = future_quote.get("close") if future_quote else None
    target_ts = (
        _to_dt(future_quote.get("timestamp") or future_quote.get("updated_at"))
        if future_quote
        else None
    )

    # Resolve the ES contract label for provenance.
    today = datetime.now(timezone.utc).date()
    contracts = sd.generate_quarterly_futures(target_u, today, count=4)
    resolver = ContractResolver(contracts)
    resolved = resolver.resolve_contract(target_u, contract, datetime.now(timezone.utc))
    target_contract = resolved.contract_id if resolved else None

    result = rm.build_reference_levels(
        target_symbol=target_u,
        source_levels=levels,
        source_price=float(source_price) if source_price is not None else None,
        source_timestamp=source_ts,
        target_price=float(target_price) if target_price is not None else None,
        target_timestamp=target_ts,
        target_contract=target_contract,
    )
    return {
        "analytics_source": result.analytics_source,
        "source_symbol": result.source_symbol,
        "target_symbol": result.target_symbol,
        "target_contract": result.target_contract,
        "basis": result.basis,
        "basis_timestamp": result.basis_timestamp,
        "source_timestamp": result.source_timestamp,
        "target_timestamp": result.target_timestamp,
        "timestamp_skew_ms": result.timestamp_skew_ms,
        "quality_status": result.quality_status,
        "quality_reasons": result.quality_reasons,
        "projected_levels": result.projected_levels,
        "disclaimer": result.disclaimer,
    }
