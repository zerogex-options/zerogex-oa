"""Signals package with lazy imports to avoid runpy side effects."""

__all__ = [
    "SignalEngineService",
    "SignalScoringEngine",
    "HypotheticalTradeEngine",
    "PositionOptimizerEngine",
    "ProprietarySignalEngine",
    "ConsolidatedSignalEngine",
]


def __getattr__(name):
    if name == "SignalEngineService":
        from src.signals.main_engine import SignalEngineService
        return SignalEngineService
    if name == "SignalScoringEngine":
        from src.signals.signal_scoring_engine import SignalScoringEngine
        return SignalScoringEngine
    if name == "HypotheticalTradeEngine":
        from src.signals.hypothetical_trade_engine import HypotheticalTradeEngine
        return HypotheticalTradeEngine
    if name == "PositionOptimizerEngine":
        from src.signals.position_optimizer_engine import PositionOptimizerEngine
        return PositionOptimizerEngine
    if name == "ProprietarySignalEngine":
        from src.signals.proprietary_signal_engine import ProprietarySignalEngine
        return ProprietarySignalEngine
    if name == "ConsolidatedSignalEngine":
        from src.signals.consolidated_signal_engine import ConsolidatedSignalEngine
        return ConsolidatedSignalEngine
    raise AttributeError(f"module 'src.signals' has no attribute '{name}'")
