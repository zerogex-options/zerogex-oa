"""Build a PlaybookContext from current DB state.

Extracted so both the ``/api/signals/action`` endpoint and (later) the
signal cycle loop can construct a context the same way.

Network/DB calls are async; pattern evaluation is sync — the engine
runs against the assembled context once it's built.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import pytz

from src.signals.components.base import MarketContext
from src.signals.playbook.context import OpenPosition, PlaybookContext, SignalSnapshot

logger = logging.getLogger(__name__)


# Names the context builder will fetch and surface to patterns.
# Adding a name here makes it available via ``ctx.advanced(...)`` /
# ``ctx.basic(...)``.  Keep these aligned with the spec's catalog.
ADVANCED_SIGNAL_NAMES = (
    "trap_detection",
    "gamma_vwap_confluence",
    "range_break_imminence",
    "eod_pressure",
    "vol_expansion",
    "squeeze_setup",
    "zero_dte_position_imbalance",
)

BASIC_SIGNAL_NAMES = (
    "tape_flow_bias",
    "dealer_delta_pressure",
    "gex_gradient",
    "positioning_trap",
    "skew_delta",
    "vanna_charm_flow",
)

# MSI components also addressable as signals for confluence checks
# (e.g. order_flow_imbalance is both an MSI component and a logical
# "signal" patterns may want to check directly).
MSI_COMPONENT_SIGNAL_NAMES = (
    "order_flow_imbalance",
    "dealer_delta_pressure",  # also a basic signal; the basic snapshot wins via merge order
)


_LEVEL_FIELDS_BY_SIGNAL = {
    # Each advanced signal that surfaces structural levels in its context_values.
    "gamma_vwap_confluence": (
        "call_wall",
        "max_pain",
        "max_gamma",
        "gamma_flip",
        "vwap",
    ),
    "trap_detection": ("resistance_level", "support_level"),
}


def _snapshot_from_row(name: str, row: Optional[dict]) -> Optional[SignalSnapshot]:
    if not row:
        return None
    clamped = float(row.get("clamped_score") or 0.0)
    score = float(row.get("score") or clamped * 100.0)
    ctx_vals = row.get("context_values") or {}
    if not isinstance(ctx_vals, dict):
        ctx_vals = {}
    return SignalSnapshot(
        name=name,
        score=score,
        clamped_score=clamped,
        triggered=bool(ctx_vals.get("triggered", False)),
        signal=ctx_vals.get("signal"),
        direction=row.get("direction"),
        context_values=ctx_vals,
    )


def _msi_component_snapshot(name: str, components: dict) -> Optional[SignalSnapshot]:
    """Synthesize a SignalSnapshot from an MSI component entry."""
    payload = components.get(name)
    if not isinstance(payload, dict):
        return None
    score = payload.get("score")
    if not isinstance(score, (int, float)):
        return None
    clamped = float(score)
    ctx_vals = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    return SignalSnapshot(
        name=name,
        score=clamped * 100.0,
        clamped_score=clamped,
        triggered=False,
        signal=None,
        direction=None,
        context_values=ctx_vals,
    )


def _extract_levels(
    market_extra: dict,
    advanced: dict[str, SignalSnapshot],
) -> dict[str, Optional[float]]:
    """Pull structural levels from wherever they appear in current state."""
    levels: dict[str, Optional[float]] = {}

    # First-class fields from the market extra (most reliable).
    for key in (
        "call_wall",
        "put_wall",
        "max_gamma_strike",
        "opening_range_high",
        "opening_range_low",
    ):
        if market_extra.get(key) is not None:
            levels[key] = float(market_extra[key])

    # Fall back to whatever the advanced signals exposed in their context.
    for sig_name, fields in _LEVEL_FIELDS_BY_SIGNAL.items():
        snap = advanced.get(sig_name)
        if not snap:
            continue
        for f in fields:
            if levels.get(f) is None and snap.context_values.get(f) is not None:
                try:
                    levels[f] = float(snap.context_values[f])
                except (TypeError, ValueError):
                    continue

    # Common alias: gamma_vwap_confluence calls it "max_gamma";
    # patterns reference it as "max_gamma_strike".
    if "max_gamma" in levels and "max_gamma_strike" not in levels:
        levels["max_gamma_strike"] = levels["max_gamma"]

    return levels


def _build_market_context(
    *,
    underlying: str,
    timestamp: datetime,
    score_row: Optional[dict],
    advanced: dict[str, SignalSnapshot],
) -> MarketContext:
    """Reconstruct the most useful MarketContext we can from persisted state.

    PR-2 reads only what's already on the latest signal_score and signal
    rows.  PR-3 will wire the live MarketContext through directly.
    """
    components = (score_row or {}).get("components") or {}
    if not isinstance(components, dict):
        components = {}

    def _comp_ctx(name: str) -> dict:
        c = components.get(name)
        if isinstance(c, dict):
            ctx = c.get("context")
            if isinstance(ctx, dict):
                return ctx
        return {}

    net_gex_ctx = _comp_ctx("net_gex_sign")
    net_gex = float(net_gex_ctx.get("net_gex") or 0.0)

    ofi_ctx = _comp_ctx("order_flow_imbalance")
    smart_call = float(ofi_ctx.get("smart_call_premium") or 0.0)
    smart_put = float(ofi_ctx.get("smart_put_premium") or 0.0)

    pcr_ctx = _comp_ctx("put_call_ratio")
    pcr = float(pcr_ctx.get("put_call_ratio") or 1.0)

    vol_ctx = _comp_ctx("volatility_regime")
    vix = vol_ctx.get("vix_level")

    ddp_ctx = _comp_ctx("dealer_delta_pressure")
    dealer_net_delta = float(ddp_ctx.get("dealer_net_delta_estimated") or 0.0)

    gamma_anchor_ctx = _comp_ctx("gamma_anchor")
    # gamma_anchor doesn't surface flip directly; pull from gvc snapshot if available.
    gvc = advanced.get("gamma_vwap_confluence")
    gvc_vals = gvc.context_values if gvc else {}
    gamma_flip = gvc_vals.get("gamma_flip")
    vwap = gvc_vals.get("vwap")
    max_pain = gvc_vals.get("max_pain")

    # `close` doesn't appear directly on the score row — patterns that
    # need it should also consult `levels`.  We fall back to vwap as a
    # proxy when no explicit close is available.
    close = float(gvc_vals.get("close") or vwap or 0.0)

    extra: dict[str, Any] = {}
    if vix is not None:
        try:
            extra["vix_level"] = float(vix)
        except (TypeError, ValueError):
            pass
    for sig_name, fields in _LEVEL_FIELDS_BY_SIGNAL.items():
        snap = advanced.get(sig_name)
        if not snap:
            continue
        for f in fields:
            if extra.get(f) is None and snap.context_values.get(f) is not None:
                try:
                    extra[f] = float(snap.context_values[f])
                except (TypeError, ValueError):
                    continue

    return MarketContext(
        timestamp=timestamp,
        underlying=underlying,
        close=close,
        net_gex=net_gex,
        gamma_flip=float(gamma_flip) if gamma_flip is not None else None,
        put_call_ratio=pcr,
        max_pain=float(max_pain) if max_pain is not None else None,
        smart_call=smart_call,
        smart_put=smart_put,
        recent_closes=[],  # PR-3 will wire this through; for PR-2 patterns fall back gracefully
        iv_rank=None,
        dealer_net_delta=dealer_net_delta,
        vwap=float(vwap) if vwap is not None else None,
        vwap_deviation_pct=None,
        orb_status=None,
        extra=extra,
    )


async def build_playbook_context(
    *,
    db,  # DatabaseManager (typed loosely to avoid circular import)
    underlying: str,
) -> Optional[PlaybookContext]:
    """Assemble a PlaybookContext from the most recent persisted state.

    Returns ``None`` when there's no signal_score row for the symbol —
    callers should treat that as a 404.
    """
    score_row = await db.get_latest_signal_score(underlying)
    if not score_row:
        return None

    timestamp = score_row.get("timestamp") or datetime.now(pytz.UTC)
    if isinstance(timestamp, str):
        try:
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            timestamp = datetime.now(pytz.UTC)
    if timestamp.tzinfo is None:
        timestamp = pytz.UTC.localize(timestamp)

    # Fetch every known advanced + basic signal in parallel-ish (sequential
    # but cheap; PR-3 can batch into a single query if it matters).
    advanced: dict[str, SignalSnapshot] = {}
    for name in ADVANCED_SIGNAL_NAMES:
        try:
            row = await db.get_advanced_signal(underlying, name)
        except Exception as exc:
            logger.warning("get_advanced_signal(%s, %s) failed: %s", underlying, name, exc)
            row = None
        snap = _snapshot_from_row(name, row)
        if snap:
            advanced[name] = snap

    basic: dict[str, SignalSnapshot] = {}
    for name in BASIC_SIGNAL_NAMES:
        try:
            row = await db.get_basic_signal(underlying, name)
        except Exception as exc:
            logger.warning("get_basic_signal(%s, %s) failed: %s", underlying, name, exc)
            row = None
        snap = _snapshot_from_row(name, row)
        if snap:
            basic[name] = snap

    # MSI components addressable as pseudo-signals (so patterns can do
    # `ctx.signal("order_flow_imbalance")` regardless of whether it's an
    # MSI component or a basic signal).
    components = score_row.get("components") or {}
    if isinstance(components, dict):
        for name in MSI_COMPONENT_SIGNAL_NAMES:
            if name in basic or name in advanced:
                continue
            snap = _msi_component_snapshot(name, components)
            if snap:
                basic[name] = snap

    market = _build_market_context(
        underlying=underlying,
        timestamp=timestamp,
        score_row=score_row,
        advanced=advanced,
    )
    levels = _extract_levels(market.extra or {}, advanced)

    composite = score_row.get("composite_score")
    regime = score_row.get("direction")

    open_positions: list[OpenPosition] = []  # PR-3 will populate from portfolio_engine state.

    return PlaybookContext(
        market=market,
        msi_score=float(composite) if isinstance(composite, (int, float)) else None,
        msi_regime=str(regime) if regime else None,
        msi_components=components if isinstance(components, dict) else {},
        advanced_signals=advanced,
        basic_signals=basic,
        levels=levels,
        open_positions=open_positions,
        recently_emitted={},  # No persistence in PR-2; hysteresis is a no-op
    )
