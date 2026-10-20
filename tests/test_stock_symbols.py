"""Single stocks (NVDA) as a fourth asset class beside ETFs, indexes and futures.

A stock is ingested exactly like an ETF -- its own share price and volume, its
own equity option chain -- so most of the pipeline never asks which it is. The
places that do are pinned here:

* the classifier itself, and the ``symbols.asset_type`` value it produces;
* the option-chain session, which closes at 16:00 for a stock (equity options
  have no 16:15 late session, unlike SPY / QQQ);
* the per-symbol strike band, which lets a stock that moves several times as
  far as SPY in a day ingest a wider chain without touching the others.
"""

import importlib
from datetime import date, time

import pytest

import src.config as cfg
from src.market_calendar import option_chain_feed_window
from src.symbols import (
    asset_class,
    get_etf_symbols,
    is_stock_symbol,
    symbols_table_asset_type,
)


@pytest.fixture(autouse=True)
def _default_maps(monkeypatch):
    for name in ("ETF_SYMBOLS", "INDEX_VOLUME_PROXIES", "FUTURES_UNDERLYINGS_MAP"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    "symbol, expected",
    [
        ("SPY", "ETF"),
        ("QQQ", "ETF"),
        ("SPX", "INDEX"),
        ("NDX", "INDEX"),
        ("ES", "FUTURE"),
        ("NQ", "FUTURE"),
        ("NVDA", "STOCK"),
        ("nvda", "STOCK"),
        # TradeStation's decoration marks a cash index and only a cash index.
        ("$SPXW.X", "INDEX"),
        ("$NDXP.X", "INDEX"),
    ],
)
def test_every_symbol_has_one_class(symbol, expected):
    assert asset_class(symbol) == expected


def test_only_stocks_are_stocks():
    assert is_stock_symbol("NVDA")
    for symbol in ("SPY", "QQQ", "SPX", "NDX", "ES", "NQ", "$SPXW.X", "", None):
        assert not is_stock_symbol(symbol), symbol


def test_the_symbols_table_records_a_stock_as_equity():
    # The table's CHECK constraint is ('EQUITY', 'INDEX', 'ETF').
    assert symbols_table_asset_type("NVDA") == "EQUITY"
    assert symbols_table_asset_type("SPY") == "ETF"
    assert symbols_table_asset_type("QQQ") == "ETF"
    assert symbols_table_asset_type("$SPXW.X") == "INDEX"
    assert symbols_table_asset_type("SPX") == "INDEX"


def test_the_etf_list_is_overridable(monkeypatch):
    monkeypatch.setenv("ETF_SYMBOLS", "spy, qqq, xlk")
    assert get_etf_symbols() == frozenset({"SPY", "QQQ", "XLK"})
    assert asset_class("XLK") == "ETF"
    assert asset_class("IWM") == "STOCK"
    monkeypatch.setenv("ETF_SYMBOLS", " , ")
    assert "IWM" in get_etf_symbols(), "a blank override keeps the defaults"


def test_a_stock_chain_closes_at_four_not_four_fifteen():
    tuesday = date(2026, 10, 20)
    assert option_chain_feed_window("NVDA", tuesday) == (time(9, 30), time(16, 0))
    # The ETF chains keep their late session, and the indexes stay at 16:00.
    assert option_chain_feed_window("SPY", tuesday) == (time(9, 30), time(16, 15))
    assert option_chain_feed_window("QQQ", tuesday) == (time(9, 30), time(16, 15))
    assert option_chain_feed_window("SPX", tuesday) == (time(9, 30), time(16, 0))


def _reload_config(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("INGEST_STRIKE_PCT_RANGE_BY_SYMBOL", raising=False)
    else:
        monkeypatch.setenv("INGEST_STRIKE_PCT_RANGE_BY_SYMBOL", value)
    return importlib.reload(cfg)


def test_a_stock_can_ingest_a_wider_strike_band(monkeypatch):
    c = _reload_config(monkeypatch, '{"nvda": 8}')
    assert c.resolve_strike_pct_range("NVDA", 3.0) == 8.0
    # Everything not in the map keeps the shared band.
    assert c.resolve_strike_pct_range("SPY", 3.0) == 3.0
    assert c.resolve_strike_pct_range("SPX", 2.5) == 2.5


def test_the_strike_band_map_is_optional_and_bounded(monkeypatch):
    c = _reload_config(monkeypatch, None)
    assert c.resolve_strike_pct_range("NVDA", 3.0) == 3.0
    c = _reload_config(monkeypatch, "not-json")
    assert c.resolve_strike_pct_range("NVDA", 3.0) == 3.0
    c = _reload_config(monkeypatch, '{"NVDA": 500, "AMD": 0}')
    assert c.resolve_strike_pct_range("NVDA", 3.0) == 50.0
    assert c.resolve_strike_pct_range("AMD", 3.0) == 0.5


def teardown_module(module):
    importlib.reload(cfg)
