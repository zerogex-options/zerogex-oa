"""Backward-compatible wrapper around the unified signal engine."""

from src.signals.unified_signal_engine import UnifiedSignalEngine


class ConsolidatedSignalEngine(UnifiedSignalEngine):
    """Deprecated alias for the unified engine."""

    pass
