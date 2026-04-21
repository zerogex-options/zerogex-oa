"""Backward-compatible import shim for advanced signal engine."""
from src.signals.advanced import AdvancedSignalEngine, AdvancedSignalResult

# Legacy aliases retained for import compatibility.
IndependentSignalEngine = AdvancedSignalEngine
IndependentSignalResult = AdvancedSignalResult

__all__ = [
    "AdvancedSignalEngine",
    "AdvancedSignalResult",
    "IndependentSignalEngine",
    "IndependentSignalResult",
]
