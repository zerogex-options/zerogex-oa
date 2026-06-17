"""Tool handlers for the Copilot chat agent.

Each handler implements one entry in ``grounding_tools.TOOL_CATALOG``.
Handlers are async (the LLM loop is async; DB calls are async via
``DatabaseManager``) and return plain JSON-serializable dicts that
match the tool's ``output_schema``.

Wiring pattern: ``build_handlers(db)`` produces a name→callable map
ready to dispatch from the agent loop. The agent never touches a
``DatabaseManager`` directly — that surface lives entirely here.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


# Symbols we expose. Sourced from the tool catalog's enum so we can't
# accept anything the spec doesn't list.
_VALID_SYMBOLS = {"SPY", "SPX", "QQQ"}


HandlerFn = Callable[..., Awaitable[Any]]


# ---------------------------------------------------------------------------
# Database surface
#
# The DatabaseManager is large; we use a small Protocol so tests can
# inject a fake without instantiating the whole connection pool. Each
# handler accesses exactly the methods named here.
# ---------------------------------------------------------------------------


class CopilotDatabase:
    """Minimum DB surface the handlers expect.

    The real ``src.api.database.DatabaseManager`` satisfies this by
    structural typing — ``acquire_connection()`` is the connection
    context manager pattern documented in the engine map. We never
    import the concrete type here so the Copilot stays decoupled.
    """

    def acquire_connection(self): ...  # async context manager


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


async def get_current_regime(db: CopilotDatabase, *, symbol: str) -> dict[str, Any]:
    """Return the latest ``regime_narratives`` row as a structured dict."""
    _require_symbol(symbol)
    async with db.acquire_connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT payload
            FROM regime_narratives
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            symbol,
        )
    if row is None:
        return _undefined_regime_stub(symbol)
    payload = row["payload"]
    return _coerce_jsonb(payload)


async def get_active_cards(db: CopilotDatabase, *, symbol: str) -> list[dict[str, Any]]:
    """All NoviceCards with status=ACTIVE for the symbol."""
    _require_symbol(symbol)
    async with db.acquire_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT card_id, regime_label, regime_confidence,
                   action_card, novice_fields, emitted_at, expires_at, status
            FROM novice_cards
            WHERE underlying = $1 AND status = 'ACTIVE'
            ORDER BY emitted_at DESC
            """,
            symbol,
        )
    return [_card_row_to_dict(row) for row in rows]


async def get_card_by_id(db: CopilotDatabase, *, card_id: str) -> dict[str, Any]:
    """Lookup a NoviceCard by id."""
    async with db.acquire_connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT card_id, regime_label, regime_confidence,
                   action_card, novice_fields, emitted_at, expires_at, status,
                   closed_at, realized_pnl_dollars
            FROM novice_cards
            WHERE card_id = $1
            """,
            card_id,
        )
    if row is None:
        return {"error": "not_found", "card_id": card_id}
    return _card_row_to_dict(row)


async def get_recent_card_history(
    db: CopilotDatabase,
    *,
    pattern_id: str,
    days: int = 30,
) -> list[dict[str, Any]]:
    """Outcomes of recently closed cards for a pattern."""
    days = max(1, min(int(days), 365))
    async with db.acquire_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT card_id, pattern_id, emitted_at, closed_at, status,
                   realized_pnl_dollars
            FROM novice_cards
            WHERE pattern_id = $1
              AND status IN ('TARGET_HIT', 'STOPPED', 'INVALIDATED', 'EXPIRED')
              AND closed_at >= NOW() - ($2 || ' days')::interval
            ORDER BY closed_at DESC
            LIMIT 200
            """,
            pattern_id,
            str(days),
        )
    return [
        {
            "card_id": str(r["card_id"]),
            "pattern_id": r["pattern_id"],
            "emitted_at": _iso(r["emitted_at"]),
            "closed_at": _iso(r["closed_at"]),
            "status": r["status"],
            "realized_pnl_dollars": _float_or_none(r["realized_pnl_dollars"]),
        }
        for r in rows
    ]


async def get_levels_snapshot(db: CopilotDatabase, *, symbol: str) -> dict[str, Any]:
    """Current spot + structural levels for the symbol.

    Sources from the latest ``regime_narratives.payload.inputs`` so the
    Copilot's "levels" match exactly what the classifier saw — no
    independent recompute that could drift from the regime label.
    """
    _require_symbol(symbol)
    async with db.acquire_connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT spot, payload, timestamp
            FROM regime_narratives
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            symbol,
        )
    if row is None:
        return {
            "symbol": symbol,
            "timestamp": None,
            "spot": None,
            "gamma_flip": None,
            "max_pain": None,
            "call_wall": None,
            "put_wall": None,
            "net_gex": None,
        }
    payload = _coerce_jsonb(row["payload"])
    inputs = payload.get("inputs", {}) if isinstance(payload, dict) else {}
    return {
        "symbol": symbol,
        "timestamp": _iso(row["timestamp"]),
        "spot": _float_or_none(row["spot"]),
        "gamma_flip": _float_or_none(inputs.get("gamma_flip")),
        "max_pain": _float_or_none(inputs.get("max_pain")),
        "call_wall": _float_or_none(inputs.get("call_wall")),
        "put_wall": _float_or_none(inputs.get("put_wall")),
        "net_gex": _float_or_none(inputs.get("net_gex")),
    }


async def get_position_context(
    db: CopilotDatabase,
    *,
    symbol: str,
    strike: float,
    right: str,
    expiry: str,
) -> dict[str, Any]:
    """Where a user position sits relative to current levels & regime."""
    _require_symbol(symbol)
    if right not in {"C", "P"}:
        raise ValueError(f"right must be 'C' or 'P', got {right!r}")
    levels = await get_levels_snapshot(db, symbol=symbol)
    regime = await get_current_regime(db, symbol=symbol)

    spot = levels.get("spot")
    distance_to_wall = _distance_pct(
        spot, levels.get("call_wall") if right == "C" else levels.get("put_wall")
    )
    distance_to_flip = _distance_pct(spot, levels.get("gamma_flip"))
    distance_to_max_pain = _distance_pct(spot, levels.get("max_pain"))

    pin_risk = _categorize_pin_risk(distance_to_max_pain, regime.get("label"))

    narrative = _compose_position_narrative(
        symbol=symbol,
        strike=strike,
        right=right,
        expiry=expiry,
        regime_label=regime.get("label", "UNDEFINED"),
        distance_to_wall=distance_to_wall,
        distance_to_max_pain=distance_to_max_pain,
        pin_risk=pin_risk,
    )

    return {
        "position": {
            "symbol": symbol,
            "strike": float(strike),
            "right": right,
            "expiry": expiry,
        },
        "regime": regime,
        "distance_to_nearest_wall_pct": distance_to_wall,
        "distance_to_gamma_flip_pct": distance_to_flip,
        "distance_to_max_pain_pct": distance_to_max_pain,
        "current_pin_risk": pin_risk,
        "narrative": narrative,
    }


async def narrate_recent_changes(
    db: CopilotDatabase,
    *,
    symbol: str,
    lookback_minutes: int = 60,
) -> list[dict[str, Any]]:
    """Regime transitions and card emissions in the lookback window."""
    _require_symbol(symbol)
    lookback_minutes = max(5, min(int(lookback_minutes), 390))
    events: list[dict[str, Any]] = []

    async with db.acquire_connection() as conn:
        regime_rows = await conn.fetch(
            """
            SELECT timestamp, label, confidence
            FROM regime_narratives
            WHERE underlying = $1
              AND timestamp >= NOW() - ($2 || ' minutes')::interval
            ORDER BY timestamp ASC
            """,
            symbol,
            str(lookback_minutes),
        )
        card_rows = await conn.fetch(
            """
            SELECT card_id, pattern_id, emitted_at, status, closed_at
            FROM novice_cards
            WHERE underlying = $1
              AND emitted_at >= NOW() - ($2 || ' minutes')::interval
            ORDER BY emitted_at ASC
            """,
            symbol,
            str(lookback_minutes),
        )

    prior_label: Optional[str] = None
    for row in regime_rows:
        label = row["label"]
        if prior_label is not None and label != prior_label:
            events.append({
                "kind": "regime_change",
                "timestamp": _iso(row["timestamp"]),
                "summary": f"Regime changed from {prior_label} to {label}",
                "details": {
                    "from": prior_label,
                    "to": label,
                    "confidence": _float_or_none(row["confidence"]),
                },
            })
        prior_label = label

    for row in card_rows:
        events.append({
            "kind": "card_emitted",
            "timestamp": _iso(row["emitted_at"]),
            "summary": f"Card emitted for pattern {row['pattern_id']}",
            "details": {
                "card_id": str(row["card_id"]),
                "pattern_id": row["pattern_id"],
            },
        })
        if row["closed_at"] is not None:
            events.append({
                "kind": "card_closed",
                "timestamp": _iso(row["closed_at"]),
                "summary": f"Card {row['card_id']} closed: {row['status']}",
                "details": {
                    "card_id": str(row["card_id"]),
                    "status": row["status"],
                },
            })

    events.sort(key=lambda e: e["timestamp"] or "")
    return events


async def get_regime_history(
    db: CopilotDatabase,
    *,
    symbol: str,
    days: int = 5,
) -> list[dict[str, Any]]:
    """Last N days of regime labels for the symbol (one per day at EOD)."""
    _require_symbol(symbol)
    days = max(1, min(int(days), 30))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with db.acquire_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (date_trunc('day', timestamp))
                   timestamp, payload
            FROM regime_narratives
            WHERE underlying = $1
              AND timestamp >= $2
            ORDER BY date_trunc('day', timestamp) DESC, timestamp DESC
            LIMIT $3
            """,
            symbol,
            cutoff,
            days,
        )
    return [_coerce_jsonb(row["payload"]) for row in rows]


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


def build_handlers(db: CopilotDatabase) -> dict[str, HandlerFn]:
    """Return the name→handler map the agent loop dispatches against.

    Keys must exactly match ``grounding_tools.TOOL_CATALOG`` entries.
    A missing tool here at agent-init time fails fast (see ``agent.py``).
    """

    async def _wrap_get_current_regime(**kwargs):
        return await get_current_regime(db, **kwargs)

    async def _wrap_get_active_cards(**kwargs):
        return await get_active_cards(db, **kwargs)

    async def _wrap_get_card_by_id(**kwargs):
        return await get_card_by_id(db, **kwargs)

    async def _wrap_get_recent_card_history(**kwargs):
        return await get_recent_card_history(db, **kwargs)

    async def _wrap_get_levels_snapshot(**kwargs):
        return await get_levels_snapshot(db, **kwargs)

    async def _wrap_get_position_context(**kwargs):
        return await get_position_context(db, **kwargs)

    async def _wrap_narrate_recent_changes(**kwargs):
        return await narrate_recent_changes(db, **kwargs)

    async def _wrap_get_regime_history(**kwargs):
        return await get_regime_history(db, **kwargs)

    return {
        "get_current_regime": _wrap_get_current_regime,
        "get_active_cards": _wrap_get_active_cards,
        "get_card_by_id": _wrap_get_card_by_id,
        "get_recent_card_history": _wrap_get_recent_card_history,
        "get_levels_snapshot": _wrap_get_levels_snapshot,
        "get_position_context": _wrap_get_position_context,
        "narrate_recent_changes": _wrap_narrate_recent_changes,
        "get_regime_history": _wrap_get_regime_history,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require_symbol(symbol: str) -> None:
    if symbol not in _VALID_SYMBOLS:
        raise ValueError(f"symbol must be one of {sorted(_VALID_SYMBOLS)}; got {symbol!r}")


def _coerce_jsonb(payload: Any) -> dict[str, Any]:
    """Some asyncpg drivers return JSONB as a dict, others as a str."""
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except (ValueError, TypeError):
            return {}
    return {}


def _card_row_to_dict(row: Any) -> dict[str, Any]:
    novice = _coerce_jsonb(row["novice_fields"])
    action_card = _coerce_jsonb(row["action_card"])
    return {
        "card_id": str(row["card_id"]),
        "status": row["status"],
        "regime": {
            "label": row["regime_label"],
            "confidence": _float_or_none(row["regime_confidence"]),
        },
        "action_card": action_card,
        "thesis": novice.get("thesis", {}),
        "risk": novice.get("risk", {}),
        "credibility": novice.get("credibility", {}),
        "emitted_at": _iso(row["emitted_at"]),
        "expires_at": _iso(row["expires_at"]),
        "closed_at": _iso(row["closed_at"]) if "closed_at" in row.keys() else None,
        "realized_pnl_dollars": (
            _float_or_none(row["realized_pnl_dollars"])
            if "realized_pnl_dollars" in row.keys()
            else None
        ),
    }


def _undefined_regime_stub(symbol: str) -> dict[str, Any]:
    return {
        "timestamp": None,
        "symbol": symbol,
        "label": "UNDEFINED",
        "confidence": 0.0,
        "spot": None,
        "expected_behavior": "No regime classification available yet for this symbol.",
        "favored_patterns": [],
        "avoid": [],
        "what_would_flip_it": "Wait for the next analytics cycle to produce a snapshot.",
        "msi_regime": "",
        "inputs": {},
    }


def _iso(ts: Optional[datetime]) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    return ts.isoformat() if isinstance(ts, datetime) else str(ts)


def _float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _distance_pct(spot: Optional[float], level: Optional[float]) -> Optional[float]:
    if spot is None or level is None or spot == 0:
        return None
    return (level - spot) / spot * 100.0


def _categorize_pin_risk(
    distance_pct: Optional[float],
    regime_label: Optional[str],
) -> str:
    if distance_pct is None or regime_label is None:
        return "LOW"
    proximity = abs(distance_pct)
    if regime_label == "LONG_GAMMA_PIN" and proximity < 0.3:
        return "HIGH"
    if regime_label in {"CHARM_DRIFT", "LONG_GAMMA_PIN"} and proximity < 0.6:
        return "MEDIUM"
    return "LOW"


def _compose_position_narrative(
    *,
    symbol: str,
    strike: float,
    right: str,
    expiry: str,
    regime_label: str,
    distance_to_wall: Optional[float],
    distance_to_max_pain: Optional[float],
    pin_risk: str,
) -> str:
    side = "call" if right == "C" else "put"
    wall_phrase = (
        f"about {abs(distance_to_wall):.2f}% from the nearest {side} wall"
        if distance_to_wall is not None
        else "no wall data available"
    )
    mp_phrase = (
        f"{abs(distance_to_max_pain):.2f}% from max pain"
        if distance_to_max_pain is not None
        else "no max pain data"
    )
    return (
        f"Your {symbol} {strike} {right} expiring {expiry} sits in a "
        f"{regime_label.replace('_', ' ').lower()} regime, {wall_phrase} and "
        f"{mp_phrase}. Current pin risk: {pin_risk}."
    )
