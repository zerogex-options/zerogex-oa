"""ZeroGEX Model Context Protocol (MCP) server.

Turns the ZeroGEX derived-analytics API into MCP tools so any MCP-capable
AI client can pull live, grounded options-market structure on demand. The
server is intentionally a *thin client* over the existing REST API: each
tool issues one authenticated HTTP request to an endpoint that already
enforces scopes (``require_scopes``) and usage metering, so the platform's
auth, tiering, and billing apply unchanged to MCP traffic.

Configuration (environment):

* ``ZEROGEX_API_BASE_URL`` — base URL of the ZeroGEX API
  (default ``http://127.0.0.1:8000``).
* ``ZEROGEX_API_KEY`` — Bearer key sent on every upstream call. Provision
  an external key with a derived tier:
  ``python -m src.api.admin_keys create <customer> --name <x> --tier analytics``
  (or ``--tier signals`` to include the MSI / Action Card tools). The MCP
  surface never requests ``market_raw``.
* ``ZEROGEX_MCP_TRANSPORT`` — ``stdio`` (default) or ``streamable-http``
  for a hosted multi-tenant deployment.

The tool *logic* lives in plain ``async`` functions so it can be unit
tested without the MCP runtime; :func:`build_server` registers them with a
FastMCP instance (imported lazily so importing this module never requires
the ``mcp`` package).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
_DEFAULT_TIMEOUT_SECONDS = 15.0


def _config() -> tuple[str, Optional[str]]:
    """Return ``(base_url, api_key)`` from the environment."""
    base_url = (os.getenv("ZEROGEX_API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
    api_key = (os.getenv("ZEROGEX_API_KEY") or "").strip() or None
    return base_url, api_key


async def _api_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    client: Any = None,
) -> Dict[str, Any]:
    """GET ``path`` from the ZeroGEX API and return parsed JSON.

    Sends the configured Bearer key. On a non-2xx response, returns a
    structured ``{"error": ...}`` dict rather than raising, so a tool call
    degrades into a readable message for the LLM instead of crashing the
    MCP session. ``client`` is injectable for tests; when omitted a
    short-lived ``httpx.AsyncClient`` is created per call.
    """
    base_url, api_key = _config()
    url = f"{base_url}{path}"
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    owns_client = client is None
    if owns_client:
        import httpx

        client = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT_SECONDS)
    try:
        resp = await client.get(url, params=params or {}, headers=headers)
        if resp.status_code >= 400:
            detail: Any
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            return {
                "error": f"ZeroGEX API returned {resp.status_code}",
                "detail": detail,
                "path": path,
            }
        return resp.json()
    except Exception as exc:  # network/timeout/parse — keep the session alive
        logger.warning("MCP upstream call failed for %s: %r", path, exc)
        return {"error": "ZeroGEX API request failed", "detail": str(exc), "path": path}
    finally:
        if owns_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Tool implementations (plain functions — registered in build_server)
# ---------------------------------------------------------------------------


async def get_market_context(symbol: str = "SPY") -> Dict[str, Any]:
    """Full grounded market-structure snapshot for an underlying.

    Composes GEX structure, the Market State Index regime, and the latest
    Playbook Action Card into one payload — the recommended single call for
    "what's going on in <symbol> right now?". Requires the ``signals`` tier.
    """
    return await _api_get("/api/ai/context", {"underlying": symbol})


async def get_gex_levels(symbol: str = "SPY") -> Dict[str, Any]:
    """Headline gamma-exposure levels: spot, net GEX, gamma flip, walls, max pain."""
    return await _api_get("/api/gex/summary", {"symbol": symbol})


async def get_flow(symbol: str = "SPY") -> Dict[str, Any]:
    """Session options-flow series (call/put premium & net volume, put/call ratio)."""
    return await _api_get("/api/flow/series", {"symbol": symbol})


async def get_signal(symbol: str = "SPY") -> Dict[str, Any]:
    """Market State Index composite (0-100) and its component breakdown."""
    return await _api_get("/api/signals/score", {"underlying": symbol})


async def get_action_card(symbol: str = "SPY") -> Dict[str, Any]:
    """Latest Playbook Action Card: a single decisive trade instruction or STAND_DOWN."""
    return await _api_get("/api/signals/action", {"underlying": symbol})


# Self-contained glossary so the explain tool needs no upstream call. Keeps
# definitions terse and aligned with the platform's education library.
EXPLANATIONS: Dict[str, str] = {
    "gex": (
        "Gamma Exposure (GEX) estimates the net gamma dealers hold from "
        "options open interest. Positive net GEX means dealers buy dips and "
        "sell rips (price-dampening / pinning); negative net GEX means they "
        "sell dips and buy rips (price-amplifying / trending)."
    ),
    "gamma_flip": (
        "The gamma flip is the underlying price where aggregate dealer gamma "
        "crosses zero — the boundary between a pinning regime (above, when "
        "positive) and an amplifying regime (below, when negative)."
    ),
    "call_wall": (
        "The call wall is the strike with the largest call gamma concentration "
        "above spot; it often acts as resistance because dealer hedging there "
        "dampens upside."
    ),
    "put_wall": (
        "The put wall is the strike with the largest put gamma concentration "
        "below spot; it often acts as support."
    ),
    "max_pain": (
        "Max pain is the strike at which the total payout to option holders is "
        "minimized at expiration — the level price is sometimes said to gravitate "
        "toward into expiry."
    ),
    "vanna": (
        "Vanna is the sensitivity of an option's delta to changes in implied "
        "volatility. Dealer vanna hedging can drive directional flows as IV moves."
    ),
    "charm": (
        "Charm (delta decay) is the change in delta as time passes. It drives "
        "predictable end-of-day and end-of-week dealer hedging drift."
    ),
    "msi": (
        "The Market State Index (MSI) is ZeroGEX's composite 0-100 regime gauge "
        "blending net-GEX sign, gamma anchoring, put/call ratio, volatility "
        "regime, order-flow imbalance, and dealer-delta pressure. >=70 trend, "
        "40-70 controlled trend, 20-40 chop, <20 high-risk reversal."
    ),
    "action_card": (
        "An Action Card is a single, fully specified trade instruction emitted "
        "by the Playbook Engine — instrument, entry, target, stop, size, "
        "confidence, and rationale — or STAND_DOWN when no pattern matches."
    ),
}


def explain_concept(topic: str) -> Dict[str, Any]:
    """Define a ZeroGEX / options-market-structure concept in plain English.

    ``topic`` is matched case-insensitively against the glossary keys
    (e.g. ``gex``, ``gamma_flip``, ``call_wall``, ``max_pain``, ``vanna``,
    ``charm``, ``msi``, ``action_card``).
    """
    key = topic.strip().lower().replace(" ", "_").replace("-", "_")
    explanation = EXPLANATIONS.get(key)
    if explanation is None:
        return {
            "topic": topic,
            "error": "Unknown topic",
            "known_topics": sorted(EXPLANATIONS),
        }
    return {"topic": key, "explanation": explanation}


def build_server() -> Any:
    """Construct and return a FastMCP server with all ZeroGEX tools registered.

    The ``mcp`` package is imported here (not at module top) so that this
    module — and the tool functions above — remain importable for unit tests
    in environments without the MCP SDK installed.
    """
    from mcp.server.fastmcp import FastMCP

    server = FastMCP(
        name="zerogex",
        instructions=(
            "ZeroGEX provides real-time, derived options-market structure for "
            "index underlyings (e.g. SPY, SPX, QQQ): gamma exposure, gamma "
            "flip, call/put walls, max pain, options flow, a composite Market "
            "State Index, and decisive Playbook Action Cards. Call "
            "get_market_context for a full grounded snapshot. All outputs are "
            "informational only and not financial advice."
        ),
    )
    for fn in (
        get_market_context,
        get_gex_levels,
        get_flow,
        get_signal,
        get_action_card,
        explain_concept,
    ):
        server.tool()(fn)
    return server


def main() -> None:
    """Console entry point (``zerogex-mcp``)."""
    transport = (os.getenv("ZEROGEX_MCP_TRANSPORT") or "stdio").strip().lower()
    server = build_server()
    logger.info("Starting ZeroGEX MCP server (transport=%s)", transport)
    server.run(transport=transport)


if __name__ == "__main__":  # pragma: no cover
    main()
