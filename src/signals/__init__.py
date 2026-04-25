"""Signals package with lazy imports to avoid runpy side effects."""

__all__ = [
    "SignalEngineService",
    "PositionOptimizerEngine",
    "ScoringEngine",
    "PortfolioEngine",
    "ComponentBase",
    "MarketContext",
]


def __getattr__(name):
    if name == "SignalEngineService":
        from src.signals.main_engine import SignalEngineService

        return SignalEngineService
    if name == "PositionOptimizerEngine":
        from src.signals.position_optimizer_engine import PositionOptimizerEngine

        return PositionOptimizerEngine
    if name == "ScoringEngine":
        from src.signals.scoring_engine import ScoringEngine

        return ScoringEngine
    if name == "PortfolioEngine":
        from src.signals.portfolio_engine import PortfolioEngine

        return PortfolioEngine
    if name == "ComponentBase":
        from src.signals.components.base import ComponentBase

        return ComponentBase
    if name == "MarketContext":
        from src.signals.components.base import MarketContext

        return MarketContext
    if name == "BasicSignalModule":
        from src.signals import basic as BasicSignalModule

        return BasicSignalModule
    if name == "BasicSignalEngine":
        from src.signals.basic import BasicSignalEngine

        return BasicSignalEngine
    if name == "AdvancedSignalEngine":
        from src.signals.advanced import AdvancedSignalEngine

        return AdvancedSignalEngine
    raise AttributeError(f"module 'src.signals' has no attribute '{name}'")
