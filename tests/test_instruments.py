"""Tests for the centralized instrument registry (src/instruments.py).

Covers the static metadata contract (multipliers, asset classes, analytics
sources) and the feature-flag-driven runtime availability that gates ES/NQ.
"""

from __future__ import annotations

import importlib

import pytest

import src.instruments as instruments
from src.instruments import (
    AnalyticsSource,
    AssetClass,
    InstrumentFamily,
    MarketCalendarId,
)


@pytest.fixture(autouse=True)
def _clear_futures_flags(monkeypatch):
    """Every test starts with all futures flags OFF unless it sets them.

    The registry reads flags live from ``os.getenv`` so no reload is needed;
    we just ensure a clean baseline.
    """
    for var in (
        "ENABLE_CME_INGESTION",
        "ENABLE_ES_ANALYTICS",
        "ENABLE_NQ_ANALYTICS",
        "ENABLE_ES_REFERENCE_MODE",
        "ENABLE_NQ_REFERENCE_MODE",
    ):
        monkeypatch.delenv(var, raising=False)
    yield


# ---------------------------------------------------------------------------
# Static metadata
# ---------------------------------------------------------------------------


def test_all_six_instruments_registered():
    syms = {i.symbol for i in instruments.all_instruments()}
    assert syms == {"SPY", "SPX", "QQQ", "NDX", "ES", "NQ"}


def test_lookup_is_case_insensitive():
    assert instruments.get_instrument("es").symbol == "ES"
    assert instruments.get_instrument("  Spy ").symbol == "SPY"
    assert instruments.get_instrument("tsla") is None


def test_equity_multipliers_are_100():
    for sym in ("SPY", "SPX", "QQQ", "NDX"):
        assert instruments.contract_multiplier(sym) == 100


def test_futures_multipliers_are_cme_point_values():
    # ES = $50/pt, NQ = $20/pt — the whole point of the registry.
    assert instruments.contract_multiplier("ES") == 50
    assert instruments.contract_multiplier("NQ") == 20


def test_unknown_symbol_multiplier_defaults_to_100():
    # Legacy call-sites passing an unregistered ticker keep equity behavior.
    assert instruments.contract_multiplier("IWM") == 100


def test_asset_classes():
    assert instruments.asset_class_of("SPY") is AssetClass.ETF
    assert instruments.asset_class_of("SPX") is AssetClass.INDEX
    assert instruments.asset_class_of("ES") is AssetClass.FUTURE
    assert instruments.is_future("NQ") is True
    assert instruments.is_future("QQQ") is False


def test_analytics_source_distinguishes_cme_from_opra():
    assert instruments.analytics_source_of("SPX") is AnalyticsSource.OPRA_EQUITY_OPTIONS
    assert instruments.analytics_source_of("ES") is AnalyticsSource.CME_FUTURES_OPTIONS
    assert instruments.analytics_source_of("NQ") is AnalyticsSource.CME_FUTURES_OPTIONS


def test_calendars_and_families():
    assert instruments.market_calendar_of("SPY") is MarketCalendarId.US_EQUITIES
    assert instruments.market_calendar_of("SPX") is MarketCalendarId.US_CASH_INDEX
    assert instruments.market_calendar_of("ES") is MarketCalendarId.CME_EQUITY_INDEX
    assert instruments.get_instrument("ES").family is InstrumentFamily.SP500
    assert instruments.get_instrument("NQ").family is InstrumentFamily.NASDAQ100


def test_futures_tick_size():
    assert instruments.get_instrument("ES").price_increment == 0.25
    assert instruments.get_instrument("NQ").price_increment == 0.25


def test_es_reference_source_is_spx_nq_has_none():
    # ES projects from SPX; NQ has no wired cash source (no naive QQQ map).
    assert instruments.get_instrument("ES").reference_source_symbol == "SPX"
    assert instruments.get_instrument("NQ").reference_source_symbol is None


# ---------------------------------------------------------------------------
# Runtime availability (feature flags)
# ---------------------------------------------------------------------------


def test_equities_always_available():
    for sym in ("SPY", "SPX", "QQQ", "NDX"):
        avail = instruments.get_availability(sym)
        assert avail.available is True
        assert avail.true_analytics is True


def test_futures_unavailable_by_default():
    for sym in ("ES", "NQ"):
        avail = instruments.get_availability(sym)
        assert avail.available is False
        assert avail.true_analytics is False
        assert avail.reference_mode is False


def test_enabled_symbols_excludes_futures_by_default():
    assert instruments.enabled_symbols() == instruments.DEFAULT_ENABLED_SYMBOLS


def test_es_true_analytics_requires_both_flags(monkeypatch):
    # One flag alone must NOT enable true analytics (defense in depth).
    monkeypatch.setenv("ENABLE_ES_ANALYTICS", "true")
    assert instruments.get_availability("ES").true_analytics is False
    monkeypatch.setenv("ENABLE_CME_INGESTION", "true")
    assert instruments.get_availability("ES").true_analytics is True
    assert "ES" in instruments.enabled_symbols()


def test_es_reference_mode_flag(monkeypatch):
    monkeypatch.setenv("ENABLE_ES_REFERENCE_MODE", "true")
    avail = instruments.get_availability("ES")
    assert avail.reference_mode is True
    assert avail.available is True  # reference-only still counts as available
    assert avail.true_analytics is False  # but NOT true CME analytics


def test_nq_reference_mode_stays_off_without_source(monkeypatch):
    # Even with the flag on, NQ has no cash-index source wired -> stays off,
    # and the reason is surfaced (no silent naive QQQ mapping).
    monkeypatch.setenv("ENABLE_NQ_REFERENCE_MODE", "true")
    avail = instruments.get_availability("NQ")
    assert avail.reference_mode is False
    assert any("naive QQQ" in r for r in avail.reasons)


def test_effective_capabilities_gate_on_availability(monkeypatch):
    # Disabled ES: gex capability reported False despite the surface existing.
    caps = instruments.effective_capabilities("ES")
    assert caps["gex"] is False
    assert caps["futures_reference_mode"] is False

    # Enable true analytics -> gex True, but strategy_builder stays False
    # (not recalibrated for futures) and reference_mode still needs its flag.
    monkeypatch.setenv("ENABLE_CME_INGESTION", "true")
    monkeypatch.setenv("ENABLE_ES_ANALYTICS", "true")
    caps = instruments.effective_capabilities("ES")
    assert caps["gex"] is True
    assert caps["strategy_builder"] is False
    assert caps["futures_reference_mode"] is False

    monkeypatch.setenv("ENABLE_ES_REFERENCE_MODE", "true")
    assert instruments.effective_capabilities("ES")["futures_reference_mode"] is True


def test_equity_capabilities_unchanged():
    caps = instruments.effective_capabilities("SPY")
    assert caps["gex"] is True
    assert caps["signals"] is True
    assert caps["strategy_builder"] is True
    assert caps["futures_reference_mode"] is False


def test_module_reimport_is_stable():
    # Guard against import-time env capture: the module must resolve flags
    # lazily, so re-importing doesn't freeze availability.
    importlib.reload(instruments)
    assert instruments.get_availability("ES").available is False
