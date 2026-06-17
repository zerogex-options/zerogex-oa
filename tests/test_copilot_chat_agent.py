"""Adversarial / structural tests for the Copilot chat agent.

We can't unit-test "the LLM didn't hallucinate" without a live model.
What we *can* test is the structural enforcement: the agent loop must
terminate, dispatch tool calls correctly, surface errors as
``is_error=True`` tool results, preserve the audit trail, and cap
runaway loops.

Each test injects a fake ``anthropic`` client whose ``messages.create``
returns a scripted sequence of responses. The tool handlers themselves
are simple in-memory stubs so we never touch the database.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import pytest

from src.copilot.agent import (
    DEFAULT_MAX_ITERATIONS,
    ChatTurn,
    CopilotAgent,
    ToolCall,
    tool_specs_to_anthropic_tools,
)
from src.copilot.grounding_tools import TOOL_CATALOG


# ---------------------------------------------------------------------------
# Fake Anthropic API response blocks
# ---------------------------------------------------------------------------


@dataclass
class FakeTextBlock:
    text: str
    type: str = "text"


@dataclass
class FakeToolUseBlock:
    id: str
    name: str
    input: dict[str, Any]
    type: str = "tool_use"


@dataclass
class FakeResponse:
    content: list[Any]
    stop_reason: str


class FakeMessagesAPI:
    """Returns a scripted sequence of ``FakeResponse`` objects per call."""

    def __init__(self, responses: Iterable[FakeResponse]) -> None:
        self._responses = list(responses)
        self._calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> FakeResponse:
        self._calls.append(kwargs)
        if not self._responses:
            raise AssertionError("FakeMessagesAPI exhausted — agent called create() more times than scripted")
        return self._responses.pop(0)


class FakeAnthropic:
    def __init__(self, responses: Iterable[FakeResponse]) -> None:
        self.messages = FakeMessagesAPI(responses)


# ---------------------------------------------------------------------------
# Stub tool handlers — return deterministic strings so tests can assert
# on exactly what flowed back into the model.
# ---------------------------------------------------------------------------


def _stub_handlers(overrides: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Build a handler dict covering every tool in the catalog."""
    overrides = overrides or {}
    handlers: dict[str, Any] = {}
    for spec in TOOL_CATALOG:
        async def _h(name=spec.name, **kwargs):
            return {"_stub": True, "_tool": name, "_args": kwargs}
        handlers[spec.name] = overrides.get(spec.name, _h)
    return handlers


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_agent_rejects_missing_handlers():
    incomplete = {next(iter(TOOL_CATALOG)).name: (lambda **_: None)}
    with pytest.raises(ValueError) as excinfo:
        CopilotAgent(
            client=FakeAnthropic([]),
            handlers=incomplete,  # type: ignore[arg-type]
            system_prompt="test",
        )
    assert "missing handlers" in str(excinfo.value).lower()


def test_tool_specs_to_anthropic_tools_preserves_shape():
    converted = tool_specs_to_anthropic_tools(list(TOOL_CATALOG))
    assert len(converted) == len(TOOL_CATALOG)
    for spec, tool in zip(TOOL_CATALOG, converted):
        assert tool["name"] == spec.name
        assert tool["description"] == spec.description
        assert tool["input_schema"] == spec.input_schema


def test_direct_answer_no_tools():
    """User asks something trivial; model answers without tool calls.

    Even with no tool grounding, the loop must terminate cleanly on
    ``end_turn`` and produce a ChatTurn with empty tool_calls.
    """
    client = FakeAnthropic([
        FakeResponse(
            content=[FakeTextBlock(text="I don't predict prices.")],
            stop_reason="end_turn",
        )
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("Will SPY hit 6000?"))
    assert turn.stop_reason == "end_turn"
    assert turn.iterations == 1
    assert turn.tool_calls == []
    assert "don't predict" in turn.text


def test_single_tool_call_then_text():
    """Model calls one tool, reads the result, then answers."""
    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="tool_1", name="get_current_regime", input={"symbol": "SPY"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="Regime is LONG_GAMMA_PIN at 78% confidence.")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("What's SPY doing?"))
    assert turn.stop_reason == "end_turn"
    assert turn.iterations == 2
    assert len(turn.tool_calls) == 1
    assert turn.tool_calls[0].name == "get_current_regime"
    assert turn.tool_calls[0].input == {"symbol": "SPY"}
    assert turn.tool_calls[0].is_error is False
    assert turn.tool_calls[0].output["_tool"] == "get_current_regime"


def test_parallel_tool_calls_in_single_response():
    """Model calls two tools in one response; both dispatch + audit-trail."""
    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="t1", name="get_current_regime", input={"symbol": "SPY"}),
                FakeToolUseBlock(id="t2", name="get_levels_snapshot", input={"symbol": "SPY"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="Long gamma, spot at the wall.")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("Brief me"))
    assert turn.stop_reason == "end_turn"
    assert {c.name for c in turn.tool_calls} == {
        "get_current_regime",
        "get_levels_snapshot",
    }


def test_unknown_tool_surfaces_as_is_error():
    """Model invents a tool name; we return is_error rather than crash."""
    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="bogus", name="invented_tool", input={"foo": "bar"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="That tool doesn't exist.")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("call something fake"))
    assert turn.stop_reason == "end_turn"
    assert len(turn.tool_calls) == 1
    assert turn.tool_calls[0].is_error is True
    assert "Unknown tool" in turn.tool_calls[0].output["error"]


def test_handler_value_error_surfaces_as_is_error():
    """A handler's ValueError becomes a recoverable tool_result, not an exception."""
    async def _failing(**_):
        raise ValueError("symbol must be one of ['SPY']")

    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="t1", name="get_current_regime", input={"symbol": "MARS"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="I only cover SPY, SPX, QQQ.")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers({"get_current_regime": _failing}),
        system_prompt="test",
    )
    turn = _run(agent.chat("regime for MARS"))
    assert turn.tool_calls[0].is_error is True
    assert "MARS" not in turn.tool_calls[0].output["error"] or "symbol" in turn.tool_calls[0].output["error"]


def test_iteration_ceiling_caps_runaway_loop():
    """Model just keeps calling tools forever; we cap and return clean message."""
    forever_loop = [
        FakeResponse(
            content=[
                FakeToolUseBlock(
                    id=f"t{i}", name="get_current_regime", input={"symbol": "SPY"}
                ),
            ],
            stop_reason="tool_use",
        )
        for i in range(DEFAULT_MAX_ITERATIONS + 2)
    ]
    client = FakeAnthropic(forever_loop)
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("loop please"))
    assert turn.stop_reason == "max_iterations"
    assert turn.iterations == DEFAULT_MAX_ITERATIONS
    assert "more tool calls than I'm allowed" in turn.text


def test_no_tool_calls_and_no_end_turn_exits_defensively():
    """Model stops without end_turn or tool_use — agent must not loop forever."""
    client = FakeAnthropic([
        FakeResponse(
            content=[FakeTextBlock(text="partial")],
            stop_reason="max_tokens",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("hi"))
    assert turn.stop_reason == "max_tokens"
    assert turn.iterations == 1
    assert turn.tool_calls == []


def test_messages_history_carries_through_turn():
    """Returned ``messages`` should contain user, assistant tool_use,
    user tool_result, and assistant text — in that order."""
    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="t1", name="get_current_regime", input={"symbol": "SPY"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="Final answer.")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("brief"))
    roles = [m["role"] for m in turn.messages]
    assert roles == ["user", "assistant", "user", "assistant"]
    # The middle 'user' message must be tool_results, not free text
    middle = turn.messages[2]["content"]
    assert isinstance(middle, list)
    assert middle[0]["type"] == "tool_result"
    assert middle[0]["tool_use_id"] == "t1"


def test_history_replay_preserved():
    """Prior history is included in the messages passed to the API."""
    history = [
        {"role": "user", "content": "earlier question"},
        {"role": "assistant", "content": [{"type": "text", "text": "earlier answer"}]},
    ]
    client = FakeAnthropic([
        FakeResponse(
            content=[FakeTextBlock(text="ok")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    _run(agent.chat("follow-up", history=history))
    sent = client.messages._calls[0]
    assert sent["messages"][0] == history[0]
    assert sent["messages"][1] == history[1]
    assert sent["messages"][2] == {"role": "user", "content": "follow-up"}


def test_tool_results_are_json_serialized():
    """Tool outputs that aren't strings must be JSON-encoded for the API."""
    async def _structured(**_):
        return {"label": "LONG_GAMMA_PIN", "confidence": 0.78}

    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="t1", name="get_current_regime", input={"symbol": "SPY"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="ok")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers({"get_current_regime": _structured}),
        system_prompt="test",
    )
    _run(agent.chat("brief"))
    # Second API call should carry the JSON-serialized tool_result
    second_call = client.messages._calls[1]
    tool_result_msg = second_call["messages"][2]["content"][0]
    assert tool_result_msg["type"] == "tool_result"
    import json
    parsed = json.loads(tool_result_msg["content"])
    assert parsed == {"label": "LONG_GAMMA_PIN", "confidence": 0.78}


def test_system_prompt_and_tools_sent_every_iteration():
    """Both must be present on every messages.create call."""
    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="t1", name="get_levels_snapshot", input={"symbol": "SPY"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="ok")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="SYSTEM-PROMPT-FOR-TEST",
    )
    _run(agent.chat("brief"))
    for call in client.messages._calls:
        assert call["system"] == "SYSTEM-PROMPT-FOR-TEST"
        assert len(call["tools"]) == len(TOOL_CATALOG)


def test_handler_audit_trail_is_complete():
    """Multiple tool calls across iterations must all land in tool_calls."""
    client = FakeAnthropic([
        FakeResponse(
            content=[
                FakeToolUseBlock(id="t1", name="get_current_regime", input={"symbol": "SPY"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[
                FakeToolUseBlock(id="t2", name="get_levels_snapshot", input={"symbol": "SPY"}),
                FakeToolUseBlock(id="t3", name="get_active_cards", input={"symbol": "SPY"}),
            ],
            stop_reason="tool_use",
        ),
        FakeResponse(
            content=[FakeTextBlock(text="Done.")],
            stop_reason="end_turn",
        ),
    ])
    agent = CopilotAgent(
        client=client,
        handlers=_stub_handlers(),
        system_prompt="test",
    )
    turn = _run(agent.chat("full brief"))
    assert [c.name for c in turn.tool_calls] == [
        "get_current_regime",
        "get_levels_snapshot",
        "get_active_cards",
    ]
    assert all(not c.is_error for c in turn.tool_calls)
