# ZeroGEX MCP Server

The ZeroGEX **Model Context Protocol (MCP)** server exposes the platform's
derived options-market analytics as tools that any MCP-capable AI client
(Claude, ChatGPT, Perplexity, custom agents, partner fintech apps) can call
to ground its answers in live ZeroGEX data instead of training-set priors.

It is a thin, authenticated HTTP client over the existing ZeroGEX REST API:
every tool maps to an endpoint that already enforces API-key **scopes**
(`require_scopes`) and **usage metering**, so distribution and billing reuse
the platform's existing controls. The MCP surface never exposes raw,
license-restricted market data (`market_raw`).

## Tools

| Tool | Upstream endpoint | Tier required | Returns |
|------|-------------------|---------------|---------|
| `get_market_context(symbol)` | `GET /api/ai/context` | `signals` | Composed snapshot: GEX structure + MSI regime + Action Card. The recommended single call. |
| `get_gex_levels(symbol)` | `GET /api/gex/summary` | `analytics` | Spot, net GEX, gamma flip, call/put walls, max pain. |
| `get_flow(symbol)` | `GET /api/flow/series` | `analytics` | Session options-flow series (premium, net volume, put/call ratio). |
| `get_signal(symbol)` | `GET /api/signals/score` | `signals` | Market State Index composite (0–100) + components. |
| `get_action_card(symbol)` | `GET /api/signals/action` | `signals` | Single decisive trade instruction, or `STAND_DOWN`. |
| `explain_concept(topic)` | _(offline glossary)_ | none | Plain-English definition of a market-structure concept. |

## Configuration

| Env var | Default | Purpose |
|---------|---------|---------|
| `ZEROGEX_API_BASE_URL` | `http://127.0.0.1:8000` | Base URL of the ZeroGEX API the tools call. |
| `ZEROGEX_API_KEY` | _(none)_ | Bearer key sent on every upstream request. |
| `ZEROGEX_MCP_TRANSPORT` | `stdio` | `stdio` for local clients, `streamable-http` for a hosted multi-tenant deployment. |

## Provisioning a customer key

External customers get a **derived-only** tier — never `full`/`market_raw`:

```bash
# Analytics only (GEX / flow / max pain / technicals tools)
python -m src.api.admin_keys create acme-corp --name "Acme MCP" --tier analytics

# Add the signal engine (MSI + Action Card + market-context tools)
python -m src.api.admin_keys create acme-corp --name "Acme MCP" --tier signals
```

To actually enforce the tier and meter usage for billing, enable the
(otherwise dormant) controls on the API server:

```bash
API_SCOPE_ENFORCEMENT=1      # run dry-run first; watch logs, then enforce
API_USAGE_METERING_ENABLED=1 # populates api_usage_daily per (day, caller, key, end-user)
```

## Running

```bash
pip install -e '.[mcp]'

# Local stdio server (e.g. for Claude Desktop / MCP Inspector)
ZEROGEX_API_BASE_URL=https://api.zerogex.io \
ZEROGEX_API_KEY=zgx_... \
python -m src.mcp.server          # or: zerogex-mcp

# Hosted, multi-tenant
ZEROGEX_MCP_TRANSPORT=streamable-http zerogex-mcp
```

## Verifying

- Connect from the MCP Inspector or a Claude client and call
  `get_market_context` — confirm it returns the composed snapshot.
- Provision an `analytics`-tier key and confirm `get_signal` /
  `get_action_card` are denied (403 surfaced as a structured `error`)
  while `get_gex_levels` succeeds — i.e. scope enforcement holds.
- With `API_USAGE_METERING_ENABLED=1`, confirm `api_usage_daily`
  increments per call.
