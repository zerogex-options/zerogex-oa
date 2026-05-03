"""Synchronous cycle integration for the Playbook Engine.

The signal cycle (``UnifiedSignalEngine.run_cycle``) runs synchronously
with psycopg2 connections; the public Playbook API (used by
``/api/signals/action``) is async with asyncpg.  This module bridges
the gap so the cycle can compute and persist Action Cards on every
tick without round-tripping through the async stack.

PR-13 scope: compute + persist only.  Wiring Cards into
``portfolio_engine`` consumption is a separate, riskier change deferred
to a later PR.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.engine import PlaybookEngine
from src.signals.playbook.types import ActionCard, ActionEnum

# AdvancedSignalResult is imported lazily inside the helpers — pulling
# `src.signals.advanced` at module load time triggers the package
# __init__'s eager engine imports and creates a circular load with
# `src.signals.basic.engine`.  We only need duck-typed access (.name,
# .score, .context) anyway.

logger = logging.getLogger(__name__)


# Names whose direction-style label lives at context["signal"], mirrored
# from the async context_builder.  We keep this map local so cycle.py
# stays self-contained.
_DIRECTIONAL_LABEL_SIGNALS = {
    "trap_detection",
    "gamma_vwap_confluence",
    "range_break_imminence",
    "eod_pressure",
    "vol_expansion",
    "squeeze_setup",
    "zero_dte_position_imbalance",
}


# Levels we surface from advanced-signal contexts (mirror of
# context_builder._LEVEL_FIELDS_BY_SIGNAL).
_LEVEL_FIELDS_BY_SIGNAL = {
    "gamma_vwap_confluence": (
        "call_wall",
        "max_pain",
        "max_gamma",
        "gamma_flip",
        "vwap",
    ),
    "trap_detection": ("resistance_level", "support_level"),
}


def _snapshot_from_result(result) -> SignalSnapshot:
    """Convert a signal result (advanced or basic) into a SignalSnapshot.

    Duck-typed: ``result`` only needs ``.name``, ``.score``, ``.context``
    attributes.  Avoids importing ``AdvancedSignalResult`` at module load
    time (which would trigger circular package init).
    """
    ctx = getattr(result, "context", None) or {}
    clamped = float(getattr(result, "score", 0.0))
    name = getattr(result, "name", "")
    return SignalSnapshot(
        name=name,
        score=clamped * 100.0,
        clamped_score=clamped,
        triggered=bool(ctx.get("triggered", abs(clamped) >= 0.20)),
        signal=ctx.get("signal") if name in _DIRECTIONAL_LABEL_SIGNALS else None,
        direction=None,
        context_values=ctx,
    )


def _msi_components_to_dict(score) -> dict[str, Any]:
    """Mirror the API normalizer: extract per-component context dicts."""
    out: dict[str, Any] = {}
    components = getattr(score, "components", None) or {}
    for name, payload in components.items():
        if not isinstance(payload, dict):
            continue
        entry: dict[str, Any] = {
            "score": payload.get("score"),
            "max_points": payload.get("max_points"),
            "contribution": payload.get("contribution"),
        }
        sub_ctx = payload.get("context")
        if isinstance(sub_ctx, dict):
            entry["context"] = sub_ctx
        out[name] = entry
    return out


def _extract_levels(
    market_extra: dict,
    advanced: dict[str, SignalSnapshot],
) -> dict[str, Optional[float]]:
    levels: dict[str, Optional[float]] = {}
    for key in (
        "call_wall",
        "put_wall",
        "max_gamma_strike",
        "opening_range_high",
        "opening_range_low",
    ):
        if market_extra.get(key) is not None:
            levels[key] = float(market_extra[key])
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
    if "max_gamma" in levels and "max_gamma_strike" not in levels:
        levels["max_gamma_strike"] = levels["max_gamma"]
    return levels


def build_context_from_cycle(
    *,
    market_context: MarketContext,
    score,
    advanced_results: Iterable,
    basic_results: Iterable,
) -> PlaybookContext:
    """Build a PlaybookContext from in-memory cycle state.

    Skips the DB round-trips the async builder does — we have everything
    in memory.  ``score_history`` on each snapshot is left empty;
    history-needy patterns fall back to "accept current trigger".  A
    later PR can add sync history loading via ``db_connection`` if
    we decide history is worth the cycle latency.
    """
    advanced: dict[str, SignalSnapshot] = {}
    for r in advanced_results or ():
        advanced[r.name] = _snapshot_from_result(r)
    basic: dict[str, SignalSnapshot] = {}
    for r in basic_results or ():
        basic[r.name] = _snapshot_from_result(r)

    return PlaybookContext(
        market=market_context,
        msi_score=float(getattr(score, "composite_score", 0.0) or 0.0),
        msi_regime=getattr(score, "direction", None),
        msi_components=_msi_components_to_dict(score),
        advanced_signals=advanced,
        basic_signals=basic,
        levels=_extract_levels(market_context.extra or {}, advanced),
        open_positions=[],  # PR-15 will wire portfolio state through.
        recently_emitted={},  # Cycle hysteresis: read from signal_action_cards below.
    )


def insert_action_card_sync(conn, card: dict[str, Any]) -> None:
    """psycopg2 INSERT for signal_action_cards.

    Mirrors the async ``SignalsQueriesMixin.insert_action_card`` shape.
    Best-effort: errors are logged but never raised so a DB hiccup
    can't break the signal cycle.
    """
    if not card or card.get("action") == ActionEnum.STAND_DOWN.value:
        return
    ts = card.get("timestamp")
    underlying = card.get("underlying")
    if ts is None or not underlying:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO signal_action_cards
                (underlying, timestamp, pattern, action, tier,
                 direction, confidence, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                underlying,
                ts,
                card.get("pattern"),
                card.get("action"),
                card.get("tier") or "n/a",
                card.get("direction") or "non_directional",
                float(card.get("confidence") or 0.0),
                json.dumps(card, default=str),
            ),
        )
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("insert_action_card_sync failed (%s): %s", card.get("pattern"), exc)


def evaluate_and_persist(
    *,
    engine: PlaybookEngine,
    market_context: MarketContext,
    score,
    advanced_results: Iterable,
    basic_results: Iterable,
    conn=None,
) -> ActionCard:
    """End-to-end cycle integration: build ctx, evaluate, persist.

    Returns the ActionCard so the caller can log it.  Persistence is
    best-effort; STAND_DOWN cards are not persisted.
    """
    ctx = build_context_from_cycle(
        market_context=market_context,
        score=score,
        advanced_results=advanced_results,
        basic_results=basic_results,
    )
    card = engine.evaluate(ctx)
    if conn is not None:
        insert_action_card_sync(conn, card.to_dict())
    return card
