"""Data ingestion package.

Intentionally avoids importing `main_engine` at package import time to prevent
`python -m src.ingestion.main_engine` runpy warnings.
"""

__all__ = [
    "TradeStationAuth",
    "TradeStationClient",
    "StreamManager",
    "IngestionEngine",
    "GreeksCalculator",
    "IVCalculator",
]


def __getattr__(name):
    """Lazy-load modules to avoid circular/module-run side effects."""
    if name == "TradeStationAuth":
        from src.ingestion.tradestation_auth import TradeStationAuth

        return TradeStationAuth
    if name == "TradeStationClient":
        from src.ingestion.tradestation_client import TradeStationClient

        return TradeStationClient
    if name == "StreamManager":
        from src.ingestion.stream_manager import StreamManager

        return StreamManager
    if name == "IngestionEngine":
        from src.ingestion.main_engine import IngestionEngine

        return IngestionEngine
    if name == "GreeksCalculator":
        from src.ingestion.greeks_calculator import GreeksCalculator

        return GreeksCalculator
    if name == "IVCalculator":
        from src.ingestion.iv_calculator import IVCalculator

        return IVCalculator

    raise AttributeError(f"module 'src.ingestion' has no attribute '{name}'")
