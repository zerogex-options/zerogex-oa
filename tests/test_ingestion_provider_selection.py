"""MARKET_DATA_PROVIDER must actually select the feed.

It was defined in config.py and in .env.example from the day the provider
seam was built, and nothing on the ingestion path ever read it: both
ingestion modules constructed a TradeStationClient directly. Setting it
changed nothing, which is the worst possible failure mode for a cutover
switch -- it reports success while still reading the old feed.
"""

from __future__ import annotations

import pytest

from src.ingestion.main_engine import IngestionEngine
from src.ingestion.providers import available_providers, get_provider


def test_an_unknown_feed_name_is_fatal(monkeypatch):
    """Never a silent fallback. A typo that defaulted to TradeStation would
    be a cutover that appears to succeed while reading the old vendor."""
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "thetdata")  # transposed
    with pytest.raises(ValueError) as exc:
        get_provider()
    assert "thetdata" in str(exc.value)
    # And it says what IS valid, so the fix is obvious from the log.
    assert "thetadata" in str(exc.value)


def test_the_registry_offers_both_feeds():
    names = available_providers()
    assert "tradestation" in names
    assert "thetadata" in names and "thetadata_mv" in names


def test_the_engine_hands_its_provider_to_the_stream_manager(monkeypatch):
    """The load-bearing wire. StreamManager takes its streams from the
    provider, so an engine that built one and then failed to pass it would
    leave the manager defaulting straight back to TradeStation."""
    captured = {}

    class _StubManager:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def initialize(self):
            return False

    monkeypatch.setattr("src.ingestion.main_engine.StreamManager", _StubManager)

    sentinel = object()
    engine = IngestionEngine.__new__(IngestionEngine)
    engine.client = None
    engine.provider = sentinel
    engine.underlying = "SPY"
    engine.db_symbol = "SPY"
    engine.num_expirations = 3
    engine.strike_count_max = 40
    engine.strike_pct_range = 3.0
    engine.num_monthly_expirations = 0
    engine.monthly_underlying = None
    engine._active_stream_manager = None

    try:
        engine.run_streaming()
    except Exception:
        # initialize() returning False ends the session; anything past that
        # is not what this test is about.
        pass

    assert captured.get("provider") is sentinel, (
        "the engine's provider never reached StreamManager, so the manager "
        "would fall back to TradeStation regardless of MARKET_DATA_PROVIDER"
    )


def test_a_non_tradestation_feed_needs_no_tradestation_credentials():
    """The entrypoint builds a TradeStationClient only for TradeStation.

    Those three credentials stop existing when TradeStation is
    decommissioned, so constructing one unconditionally would make the new
    feed depend on the old one's secrets to start at all.
    """
    import inspect

    from src.ingestion import main_engine

    src = inspect.getsource(main_engine)
    anchor = src.index('provider_name = (os.getenv("MARKET_DATA_PROVIDER"')
    block = src[anchor : anchor + 1200]
    guard = block.index('if provider_name == "tradestation":')
    creds = block.index("TRADESTATION_CLIENT_ID")
    assert guard < creds, "the client is constructed outside the TradeStation guard"
