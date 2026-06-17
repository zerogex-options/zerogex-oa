"""Copilot API: regime, cards, chat.

Three endpoints under ``/api/copilot``. All gated behind the existing
SIGNALS scope (see ``src.api.main``).

* ``GET  /api/copilot/regime/{symbol}``  → latest RegimeNarrative
* ``GET  /api/copilot/cards/active``      → active NoviceCards
* ``POST /api/copilot/chat``              → grounded chat turn

The chat endpoint owns the LLM client construction so the agent module
stays free of API-key handling. The client is lazy-initialized once per
process and cached on the router state — instantiating it per request
would burn TLS handshakes on every chat turn.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.copilot.agent import CopilotAgent
from src.copilot.handlers import build_handlers, get_active_cards, get_current_regime

from ..database import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/copilot", tags=["Copilot"])


# Lazy-initialized once per process — see ``_get_agent`` below.
_AGENT: Optional[CopilotAgent] = None


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class ChatMessage(BaseModel):
    """One historical turn the caller wants the model to see."""

    role: str = Field(..., description="'user' or 'assistant'")
    content: Any = Field(..., description="Either a string or a list of content blocks.")


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    history: list[ChatMessage] = Field(
        default_factory=list,
        description=(
            "Prior turns from this session. The agent is stateless; "
            "the caller is responsible for persisting and replaying history."
        ),
    )


class ToolCallSummary(BaseModel):
    name: str
    input: dict[str, Any]
    output: Any
    is_error: bool


class ChatResponse(BaseModel):
    text: str
    stop_reason: str
    iterations: int
    tool_calls: list[ToolCallSummary]
    messages: list[dict[str, Any]] = Field(
        ..., description="Updated history the caller should persist for the next turn."
    )


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def _get_db() -> DatabaseManager:
    # Imported lazily to avoid a circular import at module load — main.py
    # imports this router during its own startup sequence.
    from ..main import db_manager

    if db_manager is None:
        raise HTTPException(503, "Database manager not initialized")
    return db_manager


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/regime/{symbol}")
async def regime(symbol: str, db: DatabaseManager = Depends(_get_db)) -> dict[str, Any]:
    """Latest regime narrative for ``symbol``."""
    try:
        return await get_current_regime(db, symbol=symbol.upper())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/cards/active")
async def cards_active(
    symbol: str = Query(..., min_length=1, max_length=6),
    db: DatabaseManager = Depends(_get_db),
) -> list[dict[str, Any]]:
    """Active novice cards for ``symbol``."""
    try:
        return await get_active_cards(db, symbol=symbol.upper())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/chat", response_model=ChatResponse)
async def chat(
    payload: ChatRequest, db: DatabaseManager = Depends(_get_db)
) -> ChatResponse:
    """Run one grounded chat turn against the Copilot LLM."""
    agent = _get_agent(db)
    if agent is None:
        raise HTTPException(
            503,
            "Copilot chat is not configured on this deployment "
            "(missing ANTHROPIC_API_KEY or anthropic SDK).",
        )

    history = [m.model_dump() for m in payload.history]
    try:
        turn = await agent.chat(payload.message, history=history)
    except Exception as exc:
        logger.exception("Copilot chat failed")
        raise HTTPException(500, f"Copilot chat error: {type(exc).__name__}") from exc

    return ChatResponse(
        text=turn.text,
        stop_reason=turn.stop_reason,
        iterations=turn.iterations,
        tool_calls=[
            ToolCallSummary(
                name=call.name,
                input=call.input,
                output=call.output,
                is_error=call.is_error,
            )
            for call in turn.tool_calls
        ],
        messages=turn.messages,
    )


# ---------------------------------------------------------------------------
# Lazy agent construction
# ---------------------------------------------------------------------------


def _get_agent(db: DatabaseManager) -> Optional[CopilotAgent]:
    """Construct the agent on first use; cache for the process lifetime.

    Returns ``None`` when the dependencies aren't available so the
    caller can return 503 — we don't want the absence of an API key at
    startup to block the rest of the API from booting.
    """
    global _AGENT
    if _AGENT is not None:
        return _AGENT

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    try:
        from anthropic import AsyncAnthropic
    except ImportError:
        logger.warning("anthropic SDK not installed; Copilot chat unavailable")
        return None

    client = AsyncAnthropic(api_key=api_key)
    handlers = build_handlers(db)
    _AGENT = CopilotAgent(client=client, handlers=handlers)
    return _AGENT


def reset_agent_cache() -> None:
    """Test helper — drop the cached agent so tests can re-init."""
    global _AGENT
    _AGENT = None
