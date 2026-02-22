"""
Data ingestion modules for ZeroGEX

Components:
- TradeStationAuth: OAuth2 authentication manager
- TradeStationClient: Market data API client with retry logic
- BackfillManager: Historical data fetching (yields data to MainEngine)
- StreamManager: Real-time data fetching (yields data to MainEngine)
- MainEngine: Orchestration, aggregation, and storage
"""

from src.ingestion.tradestation_auth import TradeStationAuth
from src.ingestion.tradestation_client import TradeStationClient
from src.ingestion.backfill_manager import BackfillManager
from src.ingestion.stream_manager import StreamManager
from src.ingestion.main_engine import MainEngine

__all__ = [
    "TradeStationAuth",
    "TradeStationClient", 
    "BackfillManager",
    "StreamManager",
    "MainEngine",
]
