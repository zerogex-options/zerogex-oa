"""Tests for futures backtest economics and the futures health section."""

from __future__ import annotations

from decimal import Decimal

import pytest

from src.futures.backtest_economics import (
    FuturesTradeEconomics,
    TradeSide,
    compute_futures_pnl,
)
from src.futures.health import futures_health_section

# -- backtest economics -----------------------------------------------------


def test_long_es_pnl_uses_50x_point_value():
    econ = FuturesTradeEconomics(symbol="ES", multiplier=50, tick_size=0.25)
    r = compute_futures_pnl(
        entry_price=6000.0, exit_price=6010.0, contracts=2, side=TradeSide.LONG, econ=econ
    )
    assert r.gross_points == Decimal("10")
    assert r.net_pnl == Decimal("1000.00")  # 10 pts * $50 * 2


def test_short_es_pnl():
    econ = FuturesTradeEconomics(symbol="ES", multiplier=50, tick_size=0.25)
    r = compute_futures_pnl(
        entry_price=6000.0, exit_price=5990.0, contracts=1, side=TradeSide.SHORT, econ=econ
    )
    assert r.net_pnl == Decimal("500.00")  # short profits when price falls


def test_long_nq_uses_20x_point_value():
    econ = FuturesTradeEconomics(symbol="NQ", multiplier=20, tick_size=0.25)
    r = compute_futures_pnl(
        entry_price=21500.0, exit_price=21550.0, contracts=1, side=TradeSide.LONG, econ=econ
    )
    assert r.net_pnl == Decimal("1000.00")  # 50 pts * $20


def test_tick_value_is_decimal_safe():
    econ = FuturesTradeEconomics(symbol="ES", multiplier=50, tick_size=0.25)
    assert econ.tick_value == Decimal("12.50")
    nq = FuturesTradeEconomics(symbol="NQ", multiplier=20, tick_size=0.25)
    assert nq.tick_value == Decimal("5.00")


def test_fees_and_slippage_are_round_trip():
    econ = FuturesTradeEconomics(
        symbol="ES",
        multiplier=50,
        tick_size=0.25,
        commission_per_contract=2.50,
        exchange_fee_per_contract=1.18,
        slippage_ticks=1.0,
    )
    r = compute_futures_pnl(
        entry_price=6000.0, exit_price=6010.0, contracts=2, side=TradeSide.LONG, econ=econ
    )
    # fees = (2.50 + 1.18) * 2 contracts * 2 sides = 14.72
    assert r.fees == Decimal("14.72")
    # slippage = 1 tick * $12.50 * 2 contracts * 2 sides = 50.00
    assert r.slippage_cost == Decimal("50.00")
    assert r.net_pnl == Decimal("1000.00") - Decimal("14.72") - Decimal("50.00")


def test_for_symbol_reads_registry_multiplier():
    econ = FuturesTradeEconomics.for_symbol("ES", commission_per_contract=0.62)
    assert econ.multiplier == 50
    assert econ.tick_size == 0.25
    assert econ.commission_per_contract == 0.62


def test_for_symbol_rejects_non_future():
    with pytest.raises(ValueError):
        FuturesTradeEconomics.for_symbol("SPY")


def test_zero_contracts_rejected():
    econ = FuturesTradeEconomics(symbol="ES", multiplier=50, tick_size=0.25)
    with pytest.raises(ValueError):
        compute_futures_pnl(
            entry_price=6000, exit_price=6010, contracts=0, side=TradeSide.LONG, econ=econ
        )


# -- health section ---------------------------------------------------------


def test_health_section_defaults_disabled(monkeypatch):
    for f in ("ENABLE_CME_INGESTION", "ENABLE_ES_ANALYTICS"):
        monkeypatch.delenv(f, raising=False)
    section = futures_health_section()
    assert section["enabled"] is False
    assert section["real_feed"] == "pending_licensed_cme_adapter"
    assert section["products"]["ES"]["available"] is False
    assert section["products"]["ES"]["real_feed_status"] == "pending_licensed_cme_adapter"


def test_health_section_reflects_flags(monkeypatch):
    monkeypatch.setenv("ENABLE_CME_INGESTION", "true")
    monkeypatch.setenv("ENABLE_ES_ANALYTICS", "true")
    section = futures_health_section()
    assert section["enabled"] is True
    assert section["products"]["ES"]["available"] is True
    assert section["products"]["ES"]["true_analytics"] is True
    # NQ still off.
    assert section["products"]["NQ"]["available"] is False
