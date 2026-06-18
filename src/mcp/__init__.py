"""ZeroGEX MCP server package.

Exposes the platform's derived options-market analytics as Model Context
Protocol tools so any MCP client (Claude, ChatGPT, Perplexity, custom
agents, partner fintech apps) can ground its answers in live ZeroGEX data
instead of training-set priors.

The server is a thin, authenticated HTTP client over the existing ZeroGEX
REST API — every tool maps to an endpoint that already enforces API-key
scopes and usage metering, so distribution and billing reuse the platform's
existing controls. External keys are provisioned with derived-only tiers
(``analytics`` / ``signals``); the MCP surface never exposes raw,
license-restricted market data.
"""
