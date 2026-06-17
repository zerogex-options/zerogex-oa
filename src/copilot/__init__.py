"""GEX Copilot — novice-facing narrative + grounded chat layer.

Sits on top of the existing Playbook Engine and analytics. The Copilot does
not compute new signals; it *translates* what the engines already produce
into a form a novice can act on, and exposes that translation as a strict
tool contract to a grounded LLM agent.

See ``docs/design/gex_copilot_architecture.md`` for the full spec.

Public surface:
    - RegimeNarrative, classify_regime  (regime_narrative)
    - NoviceCard, wrap_action_card      (novice_card)
    - TOOL_CATALOG                      (grounding_tools)
"""

from .regime_narrative import RegimeNarrative, classify_regime
from .novice_card import NoviceCard, wrap_action_card
from .grounding_tools import TOOL_CATALOG, ToolSpec

__all__ = [
    "RegimeNarrative",
    "classify_regime",
    "NoviceCard",
    "wrap_action_card",
    "TOOL_CATALOG",
    "ToolSpec",
]
