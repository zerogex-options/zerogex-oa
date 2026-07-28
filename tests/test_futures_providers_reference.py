"""Tests for the ingestion provider fixtures and the SPX-derived reference mode."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

from src.futures import providers
from src.futures import reference_mode as rm

# -- providers --------------------------------------------------------------


def test_fixture_quote_provider_streams_and_records_metrics():
    ev = providers.make_fixture_quote(
        "ES", "ESH26", bid=5999.75, ask=6000.25, trade_date="2026-03-02"
    )
    provider = providers.FixtureFuturesQuoteProvider([ev])

    async def _collect():
        return [e async for e in provider.stream_futures_quotes(["ES"])]

    out = asyncio.run(_collect())
    assert len(out) == 1
    assert out[0].contract_id == "ESH26"
    assert out[0].last == pytest.approx(6000.0)
    assert provider.metrics.messages_received == 1
    health = provider.health()
    assert health.real_feed is False
    assert health.status == "synthetic_fixture"


def test_fixture_quote_provider_filters_by_product():
    es = providers.make_fixture_quote("ES", "ESH26", bid=6000, ask=6000.5, trade_date="2026-03-02")
    nq = providers.make_fixture_quote(
        "NQ", "NQH26", bid=21500, ask=21500.5, trade_date="2026-03-02"
    )
    provider = providers.FixtureFuturesQuoteProvider([es, nq])

    async def _collect():
        return [e async for e in provider.stream_futures_quotes(["ES"])]

    out = asyncio.run(_collect())
    assert [e.logical_product for e in out] == ["ES"]


def test_crossed_market_flagged():
    ev = providers.make_fixture_quote("ES", "ESH26", bid=6001, ask=6000, trade_date="2026-03-02")
    assert ev.quality is providers.DataQualityFlag.CROSSED


def test_open_interest_fixture_by_trade_date():
    oi = providers.OpenInterestEvent(
        source="fixture",
        received_at=datetime.now(timezone.utc),
        exchange_timestamp=datetime.now(timezone.utc),
        trade_date="2026-03-02",
        logical_product="ES",
        contract_id="ESH26",
        open_interest=123456,
    )
    provider = providers.FixtureOpenInterestProvider([oi])
    out = asyncio.run(provider.fetch_open_interest(date(2026, 3, 2)))
    assert out[0].open_interest == 123456
    assert asyncio.run(provider.fetch_open_interest(date(2026, 3, 3))) == []


def test_real_provider_is_pending_and_raises():
    real = providers.PendingRealCMEProvider()
    assert real.health().real_feed is True
    assert real.health().status == providers.REAL_PROVIDER_STATUS

    async def _drain():
        async for _ in real.stream_futures_quotes(["ES"]):
            pass

    with pytest.raises(providers.RealCMEProviderNotConfigured):
        asyncio.run(_drain())
    with pytest.raises(providers.RealCMEProviderNotConfigured):
        asyncio.run(real.fetch_open_interest(date(2026, 3, 2)))


# -- reference mode ---------------------------------------------------------


def test_reference_source_mapping():
    assert rm.reference_source_for("ES") == "SPX"
    assert rm.reference_source_for("NQ") is None


def test_basis_and_projection_happy_path():
    ts = datetime(2026, 3, 2, 15, 0, tzinfo=timezone.utc)
    levels = rm.build_reference_levels(
        target_symbol="ES",
        source_levels={"gamma_flip": 6000.0, "call_wall": 6100.0, "put_wall": 5900.0},
        source_price=5990.0,  # SPX
        source_timestamp=ts,
        target_price=6025.0,  # ES  -> basis = +35
        target_timestamp=ts + timedelta(milliseconds=200),
        target_contract="ESH26",
    )
    assert levels.analytics_source == "cash_index_projection"
    assert levels.basis == pytest.approx(35.0)
    assert levels.quality_status == "good"
    assert levels.projected_levels["gamma_flip"] == pytest.approx(6035.0)
    assert levels.projected_levels["call_wall"] == pytest.approx(6135.0)
    assert "not calculated from es futures options" in levels.disclaimer.lower()


def test_stale_basis_is_rejected():
    ts = datetime(2026, 3, 2, 15, 0, tzinfo=timezone.utc)
    levels = rm.build_reference_levels(
        target_symbol="ES",
        source_levels={"gamma_flip": 6000.0},
        source_price=5990.0,
        source_timestamp=ts,
        target_price=6025.0,
        target_timestamp=ts + timedelta(seconds=10),  # 10s skew >> tolerance
        target_contract="ESH26",
    )
    assert levels.quality_status == "unavailable"
    assert levels.basis is None
    assert levels.projected_levels["gamma_flip"] is None
    assert any("skew" in r for r in levels.quality_reasons)


def test_missing_price_rejected():
    ts = datetime(2026, 3, 2, 15, 0, tzinfo=timezone.utc)
    levels = rm.build_reference_levels(
        target_symbol="ES",
        source_levels={"gamma_flip": 6000.0},
        source_price=None,
        source_timestamp=ts,
        target_price=6025.0,
        target_timestamp=ts,
    )
    assert levels.quality_status == "unavailable"
    assert levels.projected_levels["gamma_flip"] is None


def test_nq_reference_is_disallowed():
    ts = datetime(2026, 3, 2, 15, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="naive"):
        rm.build_reference_levels(
            target_symbol="NQ",
            source_levels={"gamma_flip": 21500.0},
            source_price=21400.0,
            source_timestamp=ts,
            target_price=21550.0,
            target_timestamp=ts,
        )
