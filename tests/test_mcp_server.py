"""Tests for the ZeroGEX MCP server tool logic.

These exercise the plain tool functions (HTTP composition, auth header,
error degradation, and the offline glossary) without booting the MCP
runtime — an injected fake client stands in for ``httpx.AsyncClient`` so
no network or ``mcp`` package is required.
"""

from __future__ import annotations

import pytest

from src.mcp import server as mcp_server


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class _FakeClient:
    """Records the last request and returns a scripted response."""

    def __init__(self, response: _FakeResponse):
        self._response = response
        self.last_url = None
        self.last_params = None
        self.last_headers = None
        self.closed = False

    async def get(self, url, params=None, headers=None):
        self.last_url = url
        self.last_params = params
        self.last_headers = headers
        return self._response

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_api_get_sends_bearer_and_returns_json(monkeypatch):
    monkeypatch.setenv("ZEROGEX_API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("ZEROGEX_API_KEY", "secret-key")
    client = _FakeClient(_FakeResponse(200, {"composite_score": 63.4}))

    out = await mcp_server._api_get("/api/signals/score", {"underlying": "SPY"}, client=client)

    assert out == {"composite_score": 63.4}
    assert client.last_url == "https://api.example.com/api/signals/score"
    assert client.last_params == {"underlying": "SPY"}
    assert client.last_headers["Authorization"] == "Bearer secret-key"
    # Injected client is not closed by _api_get (caller owns it).
    assert client.closed is False


@pytest.mark.asyncio
async def test_api_get_no_key_omits_auth_header(monkeypatch):
    monkeypatch.delenv("ZEROGEX_API_KEY", raising=False)
    monkeypatch.setenv("ZEROGEX_API_BASE_URL", "https://api.example.com")
    client = _FakeClient(_FakeResponse(200, {"ok": True}))

    await mcp_server._api_get("/api/gex/summary", {"symbol": "SPY"}, client=client)

    assert "Authorization" not in client.last_headers


@pytest.mark.asyncio
async def test_api_get_error_status_degrades_to_dict(monkeypatch):
    monkeypatch.setenv("ZEROGEX_API_BASE_URL", "https://api.example.com")
    client = _FakeClient(_FakeResponse(403, {"detail": "API key lacks the required scope"}))

    out = await mcp_server._api_get("/api/signals/action", {"underlying": "SPY"}, client=client)

    assert "error" in out
    assert "403" in out["error"]
    assert out["detail"] == "API key lacks the required scope"
    assert out["path"] == "/api/signals/action"


@pytest.mark.asyncio
async def test_tool_maps_to_expected_endpoint(monkeypatch):
    captured = {}

    async def _fake_api_get(path, params=None, *, client=None):
        captured["path"] = path
        captured["params"] = params
        return {"ok": True}

    monkeypatch.setattr(mcp_server, "_api_get", _fake_api_get)

    await mcp_server.get_market_context("qqq")
    assert captured == {"path": "/api/ai/context", "params": {"underlying": "qqq"}}

    await mcp_server.get_gex_levels("SPX")
    assert captured == {"path": "/api/gex/summary", "params": {"symbol": "SPX"}}

    await mcp_server.get_action_card("SPY")
    assert captured == {"path": "/api/signals/action", "params": {"underlying": "SPY"}}


def test_explain_concept_known_and_unknown():
    known = mcp_server.explain_concept("Gamma Flip")
    assert known["topic"] == "gamma_flip"
    assert "zero" in known["explanation"].lower()

    unknown = mcp_server.explain_concept("nonsense")
    assert unknown["error"] == "Unknown topic"
    assert "gex" in unknown["known_topics"]
