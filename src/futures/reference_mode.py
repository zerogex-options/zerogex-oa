"""Interim SPX-derived ES reference mode (cash-index projection).

This maps cash-index option-derived levels (gamma flip, walls, max pain, …)
onto a futures price scale using a synchronized additive basis::

    basis            = ES_futures_price - SPX_cash_price   (synchronized)
    projected_ES_lvl = SPX_level + basis

It is **explicitly not** true ES futures-options analytics.  Every result is
tagged ``analytics_source = "cash_index_projection"`` and carries the basis,
its timestamp, the timestamp skew, and a disclaimer so no surface can present
an SPX-derived number as CME futures-options GEX (spec Phase 10, rules #6/#7).

NQ guard: a naive QQQ→NQ additive mapping is forbidden.  NQ reference mode
requires a cash-index source (NDX) that is not yet wired, so
:func:`reference_source_for` returns ``None`` for NQ and the caller must
disable NQ reference mode rather than substitute QQQ.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

ANALYTICS_SOURCE = "cash_index_projection"
DISCLAIMER = "SPX-derived ES reference — not calculated from ES futures options"

#: Default maximum tolerated skew between the source (SPX) and target (ES)
#: quote timestamps before the basis is considered unsynchronized.
DEFAULT_MAX_SKEW_MS = 2000.0


#: Which cash index seeds each futures product's reference mode.  ES<-SPX is
#: the only wired pair; NQ is intentionally None (no naive QQQ mapping).
_REFERENCE_SOURCE: Dict[str, Optional[str]] = {
    "ES": "SPX",
    "NQ": None,
}


def reference_source_for(target_symbol: str) -> Optional[str]:
    return _REFERENCE_SOURCE.get((target_symbol or "").strip().upper())


@dataclass(frozen=True)
class BasisResult:
    source_symbol: str
    target_symbol: str
    target_contract: Optional[str]
    basis: Optional[float]
    basis_timestamp: Optional[str]
    source_price: Optional[float]
    target_price: Optional[float]
    source_timestamp: Optional[str]
    target_timestamp: Optional[str]
    timestamp_skew_ms: Optional[float]
    quality_status: str
    quality_reasons: List[str] = field(default_factory=list)

    @property
    def is_usable(self) -> bool:
        return self.quality_status == "good" and self.basis is not None


@dataclass(frozen=True)
class ReferenceLevels:
    analytics_source: str
    source_symbol: str
    target_symbol: str
    target_contract: Optional[str]
    basis: Optional[float]
    basis_timestamp: Optional[str]
    source_timestamp: Optional[str]
    target_timestamp: Optional[str]
    timestamp_skew_ms: Optional[float]
    quality_status: str
    quality_reasons: List[str]
    projected_levels: Dict[str, Optional[float]]
    disclaimer: str = DISCLAIMER


def compute_basis(
    *,
    source_symbol: str,
    target_symbol: str,
    source_price: Optional[float],
    source_timestamp: Optional[datetime],
    target_price: Optional[float],
    target_timestamp: Optional[datetime],
    target_contract: Optional[str] = None,
    max_skew_ms: float = DEFAULT_MAX_SKEW_MS,
) -> BasisResult:
    """Compute the synchronized additive basis (target − source).

    Rejects (quality != "good") when either price is missing/non-positive or
    the two quote timestamps are further apart than ``max_skew_ms`` — a stale
    or unsynchronized basis must never be silently used.
    """
    reasons: List[str] = []
    skew_ms: Optional[float] = None

    if source_price is None or source_price <= 0:
        reasons.append("missing/invalid source (SPX) price")
    if target_price is None or target_price <= 0:
        reasons.append("missing/invalid target (ES) price")

    if source_timestamp is not None and target_timestamp is not None:
        skew_ms = abs((target_timestamp - source_timestamp).total_seconds()) * 1000.0
        if skew_ms > max_skew_ms:
            reasons.append(f"timestamp skew {skew_ms:.0f}ms exceeds {max_skew_ms:.0f}ms tolerance")
    else:
        reasons.append("missing source/target timestamp for synchronization")

    basis: Optional[float] = None
    basis_ts: Optional[str] = None
    if source_price and target_price and source_price > 0 and target_price > 0:
        basis = round(target_price - source_price, 6)
        # Anchor the basis timestamp to the *later* of the two quotes.
        if source_timestamp and target_timestamp:
            basis_ts = max(source_timestamp, target_timestamp).isoformat()

    quality = "good" if not reasons else "unavailable"
    return BasisResult(
        source_symbol=source_symbol.upper(),
        target_symbol=target_symbol.upper(),
        target_contract=target_contract,
        basis=basis if quality == "good" else None,
        basis_timestamp=basis_ts if quality == "good" else None,
        source_price=source_price,
        target_price=target_price,
        source_timestamp=source_timestamp.isoformat() if source_timestamp else None,
        target_timestamp=target_timestamp.isoformat() if target_timestamp else None,
        timestamp_skew_ms=skew_ms,
        quality_status=quality,
        quality_reasons=reasons,
    )


def project_levels(
    source_levels: Dict[str, Optional[float]], basis: BasisResult
) -> ReferenceLevels:
    """Project cash-index levels onto the futures scale via the basis.

    ``source_levels`` is a mapping like ``{"gamma_flip": 6000, "call_wall":
    6100, ...}`` in SPX points.  When the basis is unusable, every projected
    level is ``None`` and the quality is propagated — the caller must render an
    explicit unavailable state, never a misleading number.
    """
    projected: Dict[str, Optional[float]] = {}
    for name, lvl in source_levels.items():
        if basis.is_usable and lvl is not None:
            projected[name] = round(lvl + basis.basis, 6)  # type: ignore[operator]
        else:
            projected[name] = None

    return ReferenceLevels(
        analytics_source=ANALYTICS_SOURCE,
        source_symbol=basis.source_symbol,
        target_symbol=basis.target_symbol,
        target_contract=basis.target_contract,
        basis=basis.basis,
        basis_timestamp=basis.basis_timestamp,
        source_timestamp=basis.source_timestamp,
        target_timestamp=basis.target_timestamp,
        timestamp_skew_ms=basis.timestamp_skew_ms,
        quality_status=basis.quality_status,
        quality_reasons=basis.quality_reasons,
        projected_levels=projected,
    )


def build_reference_levels(
    *,
    target_symbol: str,
    source_levels: Dict[str, Optional[float]],
    source_price: Optional[float],
    source_timestamp: Optional[datetime],
    target_price: Optional[float],
    target_timestamp: Optional[datetime],
    target_contract: Optional[str] = None,
    max_skew_ms: float = DEFAULT_MAX_SKEW_MS,
) -> ReferenceLevels:
    """End-to-end helper: validate the pair, compute basis, project levels.

    Raises ``ValueError`` if ``target_symbol`` has no wired cash-index source
    (e.g. NQ) — the caller must not fall back to a naive mapping.
    """
    source_symbol = reference_source_for(target_symbol)
    if source_symbol is None:
        raise ValueError(
            f"{target_symbol.upper()} has no wired cash-index reference source "
            "(naive ETF->future mapping is disallowed)"
        )
    basis = compute_basis(
        source_symbol=source_symbol,
        target_symbol=target_symbol,
        source_price=source_price,
        source_timestamp=source_timestamp,
        target_price=target_price,
        target_timestamp=target_timestamp,
        target_contract=target_contract,
        max_skew_ms=max_skew_ms,
    )
    return project_levels(source_levels, basis)
