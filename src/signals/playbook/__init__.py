"""Playbook Engine: pattern catalog → Action Cards.

See ``docs/playbook_catalog.md`` for the spec this module implements.
"""

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext, SignalSnapshot, OpenPosition
from src.signals.playbook.engine import PlaybookEngine
from src.signals.playbook.types import (
    ActionCard,
    ActionEnum,
    Entry,
    Leg,
    Stop,
    Target,
)

__all__ = [
    "ActionCard",
    "ActionEnum",
    "Entry",
    "Leg",
    "OpenPosition",
    "PatternBase",
    "PlaybookContext",
    "PlaybookEngine",
    "SignalSnapshot",
    "Stop",
    "Target",
]
