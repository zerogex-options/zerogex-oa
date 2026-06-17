"""Claude tool-use loop for the Copilot chat agent.

Pure orchestration: takes a conversation history + a user message, drives
a tool-use loop against the Anthropic API with the eight tools from
``grounding_tools.TOOL_CATALOG``, and returns the final text plus an
audit trail of every tool call made this turn.

Groundedness is enforced *structurally* — the LLM has no internet, no
free retrieval, and no other tools. Any factual claim it makes must be
backed by one of the tool results in this turn's audit trail. The
system prompt covers the *semantic* enforcement.

Design choices baked in here:
  * Haiku 4.5 — fast, cheap, tool-capable, no `thinking`/`effort` params
    needed (those would 400 on this tier).
  * Manual loop, not the beta ``tool_runner`` — we need the audit trail
    and tight max-iteration control for the safety guarantees.
  * Async — handlers hit the DB via asyncpg; FastAPI is async natively.
  * Defensive imports — ``anthropic`` is an optional extra; importing
    this module without the dep installed should still succeed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from .grounding_tools import TOOL_CATALOG, ToolSpec
from .handlers import HandlerFn

logger = logging.getLogger(__name__)


# Haiku 4.5: 200K context, $1/$5 per MTok. Tool-use capable. No thinking
# parameter. Override via env or constructor for tests.
DEFAULT_MODEL = "claude-haiku-4-5"
DEFAULT_MAX_TOKENS = 2048
DEFAULT_MAX_ITERATIONS = 6  # safety ceiling — the loop is gated by stop_reason

_SYSTEM_PROMPT_PATH = Path(__file__).parent / "prompts" / "copilot_system.md"


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolCall:
    """One tool invocation Claude made during a chat turn."""

    name: str
    input: dict[str, Any]
    output: Any
    is_error: bool = False


@dataclass(frozen=True)
class ChatTurn:
    """The result of one user message.

    ``text`` is the final assistant reply. ``tool_calls`` is the
    complete audit trail for groundedness verification. ``messages``
    is the updated conversation history the caller should persist for
    the next turn.
    """

    text: str
    tool_calls: list[ToolCall]
    stop_reason: str
    messages: list[dict[str, Any]]
    iterations: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_system_prompt() -> str:
    """Read the canonical system prompt from disk.

    Kept as a function so tests can monkeypatch the path without
    importing the file at module load time.
    """
    return _SYSTEM_PROMPT_PATH.read_text(encoding="utf-8")


def tool_specs_to_anthropic_tools(specs: list[ToolSpec]) -> list[dict[str, Any]]:
    """Project ``ToolSpec`` into the shape ``messages.create(tools=...)`` expects."""
    return [
        {
            "name": s.name,
            "description": s.description,
            "input_schema": s.input_schema,
        }
        for s in specs
    ]


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------


class CopilotAgent:
    """Drives the tool-use loop for one Copilot chat turn.

    Stateless across turns — caller passes prior ``messages`` in and gets
    updated ``messages`` back. This keeps the agent itself testable
    without a session store.

    The ``client`` parameter is injected so tests can pass a stub. In
    production code, callers pass an ``AsyncAnthropic`` instance.
    """

    def __init__(
        self,
        *,
        client: Any,
        handlers: dict[str, HandlerFn],
        model: str = DEFAULT_MODEL,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        max_iterations: int = DEFAULT_MAX_ITERATIONS,
        system_prompt: Optional[str] = None,
    ) -> None:
        missing = {t.name for t in TOOL_CATALOG} - set(handlers.keys())
        if missing:
            raise ValueError(
                f"CopilotAgent missing handlers for tools: {sorted(missing)}"
            )
        self._client = client
        self._handlers = handlers
        self._model = model
        self._max_tokens = max_tokens
        self._max_iterations = max_iterations
        self._tools = tool_specs_to_anthropic_tools(list(TOOL_CATALOG))
        self._system = system_prompt or load_system_prompt()

    async def chat(
        self,
        user_message: str,
        *,
        history: Optional[list[dict[str, Any]]] = None,
    ) -> ChatTurn:
        """Run one tool-use loop for ``user_message`` and return the result."""
        messages: list[dict[str, Any]] = list(history or [])
        messages.append({"role": "user", "content": user_message})

        tool_calls: list[ToolCall] = []
        iterations = 0
        stop_reason = "unknown"

        while iterations < self._max_iterations:
            iterations += 1
            response = await self._client.messages.create(
                model=self._model,
                max_tokens=self._max_tokens,
                system=self._system,
                tools=self._tools,
                messages=messages,
            )
            stop_reason = response.stop_reason

            if stop_reason == "end_turn":
                messages.append({"role": "assistant", "content": _content_to_dicts(response.content)})
                return ChatTurn(
                    text=_extract_text(response.content),
                    tool_calls=tool_calls,
                    stop_reason=stop_reason,
                    messages=messages,
                    iterations=iterations,
                )

            if stop_reason == "pause_turn":
                # Server-side tool hit its iteration limit — re-send to resume.
                messages.append({"role": "assistant", "content": _content_to_dicts(response.content)})
                continue

            tool_use_blocks = [
                block for block in response.content if _is_tool_use(block)
            ]
            if not tool_use_blocks:
                # No tool calls and no end_turn — exit defensively with
                # whatever text we got rather than looping.
                messages.append({"role": "assistant", "content": _content_to_dicts(response.content)})
                return ChatTurn(
                    text=_extract_text(response.content),
                    tool_calls=tool_calls,
                    stop_reason=stop_reason,
                    messages=messages,
                    iterations=iterations,
                )

            messages.append({"role": "assistant", "content": _content_to_dicts(response.content)})

            tool_results: list[dict[str, Any]] = []
            for block in tool_use_blocks:
                call = await self._dispatch(block)
                tool_calls.append(call)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(call.output, default=str),
                        "is_error": call.is_error,
                    }
                )

            messages.append({"role": "user", "content": tool_results})

        # Iteration ceiling hit. Surface this honestly rather than silently
        # returning the last assistant text — the safety invariant is
        # "every fact comes from a tool call this turn", and we want the
        # caller to know we didn't reach a clean end_turn.
        return ChatTurn(
            text=(
                "I needed more tool calls than I'm allowed in one turn to "
                "answer that confidently. Try a more focused question."
            ),
            tool_calls=tool_calls,
            stop_reason="max_iterations",
            messages=messages,
            iterations=iterations,
        )

    async def _dispatch(self, block: Any) -> ToolCall:
        """Execute one ``tool_use`` block, capturing errors as tool results.

        The Anthropic API expects tool execution errors to come back as
        ``tool_result`` blocks with ``is_error=True`` — not as Python
        exceptions that abort the loop. Surface every failure that way
        so the model can recover (or surface it to the user clearly).
        """
        handler = self._handlers.get(block.name)
        if handler is None:
            logger.warning("Copilot: no handler registered for tool %s", block.name)
            return ToolCall(
                name=block.name,
                input=dict(block.input or {}),
                output={"error": f"Unknown tool: {block.name}"},
                is_error=True,
            )
        try:
            output = await handler(**(block.input or {}))
            return ToolCall(
                name=block.name,
                input=dict(block.input or {}),
                output=output,
                is_error=False,
            )
        except (TypeError, ValueError) as exc:
            # Input validation failures — surfaced as a recoverable tool
            # error so the model can retry with corrected arguments.
            return ToolCall(
                name=block.name,
                input=dict(block.input or {}),
                output={"error": str(exc)},
                is_error=True,
            )
        except Exception as exc:  # pragma: no cover — last-resort safety net
            logger.exception("Copilot handler %s crashed", block.name)
            return ToolCall(
                name=block.name,
                input=dict(block.input or {}),
                output={"error": f"internal error: {type(exc).__name__}"},
                is_error=True,
            )


# ---------------------------------------------------------------------------
# Content-block helpers
#
# Anthropic returns Pydantic objects in ``response.content``; we
# preserve dict-shape blocks the API can replay on the next turn.
# ---------------------------------------------------------------------------


def _is_tool_use(block: Any) -> bool:
    block_type = getattr(block, "type", None)
    return block_type == "tool_use"


def _extract_text(content: list[Any]) -> str:
    parts: list[str] = []
    for block in content:
        if getattr(block, "type", None) == "text":
            parts.append(getattr(block, "text", ""))
    return "\n".join(p for p in parts if p).strip()


def _content_to_dicts(content: list[Any]) -> list[dict[str, Any]]:
    """Convert SDK content blocks into the dict shape the API accepts."""
    out: list[dict[str, Any]] = []
    for block in content:
        block_type = getattr(block, "type", None)
        if block_type == "text":
            out.append({"type": "text", "text": getattr(block, "text", "")})
        elif block_type == "tool_use":
            out.append(
                {
                    "type": "tool_use",
                    "id": getattr(block, "id"),
                    "name": getattr(block, "name"),
                    "input": getattr(block, "input", {}) or {},
                }
            )
        else:
            # Unknown block types are preserved verbatim — the SDK may
            # already return them as serializable dicts.
            if hasattr(block, "model_dump"):
                out.append(block.model_dump())
            elif isinstance(block, dict):
                out.append(block)
    return out
