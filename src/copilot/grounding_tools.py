"""Tool contract for the Copilot LLM agent.

This is the *entire* surface the LLM is allowed to touch. The system prompt
forbids any factual claim that isn't grounded in a tool result from the
current turn. The schemas here are what the LLM sees when it decides which
tool to call.

Each ``ToolSpec`` carries:

  * ``name`` — what the LLM addresses
  * ``description`` — one-paragraph purpose; the LLM uses this for routing
  * ``input_schema`` — JSON Schema for arguments
  * ``output_schema`` — JSON Schema for the return value (for documentation;
    the actual return is validated by the handler)

Handlers (the functions that actually fetch data) live alongside as
``handle_<tool_name>``. They are thin wrappers over existing analytics
endpoints — see ``docs/design/gex_copilot_architecture.md`` §5–6.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ToolSpec:
    """One tool the LLM may invoke. Pure metadata — no implementation."""

    name: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]


# ---------------------------------------------------------------------------
# Shared schemas
# ---------------------------------------------------------------------------


_SYMBOL_ENUM = ["SPY", "SPX", "QQQ"]


_REGIME_NARRATIVE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "string", "format": "date-time"},
        "symbol": {"type": "string"},
        "label": {
            "type": "string",
            "enum": [
                "LONG_GAMMA_PIN",
                "SHORT_GAMMA_TREND",
                "VOL_EXPANSION",
                "VANNA_GLIDE",
                "CHARM_DRIFT",
                "TRANSITION",
                "UNDEFINED",
            ],
        },
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 0.95},
        "spot": {"type": "number"},
        "expected_behavior": {"type": "string"},
        "favored_patterns": {"type": "array", "items": {"type": "string"}},
        "avoid": {"type": "array", "items": {"type": "string"}},
        "what_would_flip_it": {"type": "string"},
        "msi_regime": {"type": "string"},
        "inputs": {"type": "object"},
    },
    "required": ["timestamp", "symbol", "label", "confidence", "spot"],
}


_NOVICE_CARD_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "card_id": {"type": "string"},
        "status": {
            "type": "string",
            "enum": [
                "ACTIVE",
                "FILLED",
                "STOPPED",
                "TARGET_HIT",
                "INVALIDATED",
                "EXPIRED",
            ],
        },
        "regime": _REGIME_NARRATIVE_SCHEMA,
        "action_card": {"type": "object"},
        "thesis": {
            "type": "object",
            "properties": {
                "one_line": {"type": "string"},
                "what_could_go_wrong": {"type": "string"},
                "invalidation": {"type": "string"},
            },
            "required": ["one_line"],
        },
        "risk": {
            "type": "object",
            "properties": {
                "account_size": {"type": "number"},
                "risk_dollars": {"type": "number"},
                "risk_pct_of_account": {"type": "number"},
                "target_dollars": {"type": "number"},
                "payoff_ratio": {"type": "number"},
            },
        },
        "credibility": {
            "type": "object",
            "properties": {
                "historical_hit_rate": {"type": ["number", "null"]},
                "historical_sample_size": {"type": "integer"},
                "historical_avg_winner_pct": {"type": ["number", "null"]},
                "historical_avg_loser_pct": {"type": ["number", "null"]},
                "experimental": {"type": "boolean"},
            },
        },
        "emitted_at": {"type": "string", "format": "date-time"},
        "expires_at": {"type": "string", "format": "date-time"},
    },
    "required": ["card_id", "status", "regime", "action_card", "thesis"],
}


_LEVELS_SNAPSHOT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "symbol": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "spot": {"type": "number"},
        "gamma_flip": {"type": ["number", "null"]},
        "max_pain": {"type": ["number", "null"]},
        "call_wall": {"type": ["number", "null"]},
        "put_wall": {"type": ["number", "null"]},
        "net_gex": {"type": "number"},
    },
}


_POSITION_CONTEXT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "position": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "strike": {"type": "number"},
                "right": {"type": "string", "enum": ["C", "P"]},
                "expiry": {"type": "string", "format": "date"},
            },
            "required": ["symbol", "strike", "right", "expiry"],
        },
        "regime": _REGIME_NARRATIVE_SCHEMA,
        "distance_to_nearest_wall_pct": {"type": "number"},
        "distance_to_gamma_flip_pct": {"type": "number"},
        "distance_to_max_pain_pct": {"type": "number"},
        "current_pin_risk": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
        "narrative": {"type": "string"},
    },
}


_EVENT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "kind": {"type": "string", "enum": ["regime_change", "card_emitted", "card_closed"]},
        "timestamp": {"type": "string", "format": "date-time"},
        "summary": {"type": "string"},
        "details": {"type": "object"},
    },
}


# ---------------------------------------------------------------------------
# Tool specs
# ---------------------------------------------------------------------------


TOOL_GET_CURRENT_REGIME = ToolSpec(
    name="get_current_regime",
    description=(
        "Return the latest RegimeNarrative for a symbol. Use this first whenever the "
        "user asks 'what's happening', 'what's the market doing', or any directional "
        "question. Every regime-flavored claim in your reply must cite the result."
    ),
    input_schema={
        "type": "object",
        "properties": {"symbol": {"type": "string", "enum": _SYMBOL_ENUM}},
        "required": ["symbol"],
    },
    output_schema=_REGIME_NARRATIVE_SCHEMA,
)


TOOL_GET_ACTIVE_CARDS = ToolSpec(
    name="get_active_cards",
    description=(
        "Return all currently ACTIVE NoviceCards for a symbol. Use this whenever "
        "the user asks 'what should I do', 'is there a trade', or 'is now a good time'. "
        "If the result is empty, the correct answer is 'sit out — here's why' citing "
        "the current regime."
    ),
    input_schema={
        "type": "object",
        "properties": {"symbol": {"type": "string", "enum": _SYMBOL_ENUM}},
        "required": ["symbol"],
    },
    output_schema={"type": "array", "items": _NOVICE_CARD_SCHEMA},
)


TOOL_GET_CARD_BY_ID = ToolSpec(
    name="get_card_by_id",
    description=(
        "Look up one NoviceCard by id. Use this when the user references a card "
        "they're already tracking ('what about that SPY put card from earlier')."
    ),
    input_schema={
        "type": "object",
        "properties": {"card_id": {"type": "string"}},
        "required": ["card_id"],
    },
    output_schema=_NOVICE_CARD_SCHEMA,
)


TOOL_GET_RECENT_CARD_HISTORY = ToolSpec(
    name="get_recent_card_history",
    description=(
        "Return outcomes of recently closed NoviceCards for a pattern. Use this "
        "when the user asks how a setup has performed, or when you need to "
        "ground a credibility claim (e.g. 'this pattern hit ~64% of recent fires')."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "pattern_id": {"type": "string"},
            "days": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30},
        },
        "required": ["pattern_id"],
    },
    output_schema={
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "card_id": {"type": "string"},
                "pattern_id": {"type": "string"},
                "emitted_at": {"type": "string", "format": "date-time"},
                "closed_at": {"type": "string", "format": "date-time"},
                "status": {"type": "string"},
                "realized_pnl_dollars": {"type": "number"},
            },
        },
    },
)


TOOL_GET_LEVELS_SNAPSHOT = ToolSpec(
    name="get_levels_snapshot",
    description=(
        "Return current spot plus the structural levels (gamma flip, max pain, "
        "call/put wall, net GEX) for a symbol. Use this whenever a number "
        "appears in your reply — every level you cite must come from this call."
    ),
    input_schema={
        "type": "object",
        "properties": {"symbol": {"type": "string", "enum": _SYMBOL_ENUM}},
        "required": ["symbol"],
    },
    output_schema=_LEVELS_SNAPSHOT_SCHEMA,
)


TOOL_GET_POSITION_CONTEXT = ToolSpec(
    name="get_position_context",
    description=(
        "Given a position (symbol + strike + right + expiry), return where it "
        "sits relative to current levels and what the current regime implies "
        "for it. Use this for 'should I worry about my X position' questions."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "symbol": {"type": "string", "enum": _SYMBOL_ENUM},
            "strike": {"type": "number"},
            "right": {"type": "string", "enum": ["C", "P"]},
            "expiry": {"type": "string", "format": "date"},
        },
        "required": ["symbol", "strike", "right", "expiry"],
    },
    output_schema=_POSITION_CONTEXT_SCHEMA,
)


TOOL_NARRATE_RECENT_CHANGES = ToolSpec(
    name="narrate_recent_changes",
    description=(
        "Return regime transitions and card emissions/closures in the last "
        "N minutes for a symbol. Use this when the user asks 'what just happened' "
        "or 'why did the market move'."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "symbol": {"type": "string", "enum": _SYMBOL_ENUM},
            "lookback_minutes": {"type": "integer", "minimum": 5, "maximum": 390, "default": 60},
        },
        "required": ["symbol"],
    },
    output_schema={"type": "array", "items": _EVENT_SCHEMA},
)


TOOL_GET_REGIME_HISTORY = ToolSpec(
    name="get_regime_history",
    description=(
        "Return the last N days of regime labels for a symbol. Use this for "
        "comparative questions ('is today similar to last Tuesday') or to "
        "anchor seasonality observations."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "symbol": {"type": "string", "enum": _SYMBOL_ENUM},
            "days": {"type": "integer", "minimum": 1, "maximum": 30, "default": 5},
        },
        "required": ["symbol"],
    },
    output_schema={"type": "array", "items": _REGIME_NARRATIVE_SCHEMA},
)


TOOL_CATALOG: list[ToolSpec] = [
    TOOL_GET_CURRENT_REGIME,
    TOOL_GET_ACTIVE_CARDS,
    TOOL_GET_CARD_BY_ID,
    TOOL_GET_RECENT_CARD_HISTORY,
    TOOL_GET_LEVELS_SNAPSHOT,
    TOOL_GET_POSITION_CONTEXT,
    TOOL_NARRATE_RECENT_CHANGES,
    TOOL_GET_REGIME_HISTORY,
]


# ---------------------------------------------------------------------------
# Handler registration
#
# Handlers are wired in PR-3 once the API routers land. Until then, the
# catalog is sufficient to drive the system-prompt eval and unit tests.
# ---------------------------------------------------------------------------


HANDLERS: dict[str, Callable[..., Any]] = {}


def register_handler(name: str, fn: Callable[..., Any]) -> None:
    """Bind a tool name to its handler implementation."""
    if name not in {tool.name for tool in TOOL_CATALOG}:
        raise ValueError(f"Unknown tool: {name}")
    HANDLERS[name] = fn


def get_handler(name: str) -> Callable[..., Any]:
    """Resolve the handler for a tool — raises ``KeyError`` if not registered."""
    return HANDLERS[name]
