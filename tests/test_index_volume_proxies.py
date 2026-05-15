"""Tests for index→ETF volume-proxy resolution."""

from __future__ import annotations

import os
from unittest.mock import patch

from src import symbols


def test_default_proxies_cover_major_cash_indices():
    proxies = symbols.get_index_volume_proxies()
    assert proxies["SPX"] == "SPY"
    assert proxies["NDX"] == "QQQ"
    assert proxies["RUT"] == "IWM"
    assert proxies["DJX"] == "DIA"


def test_resolve_volume_proxy_returns_none_for_equities_and_etfs():
    assert symbols.resolve_volume_proxy("SPY") is None
    assert symbols.resolve_volume_proxy("AAPL") is None
    assert symbols.resolve_volume_proxy("") is None
    assert symbols.resolve_volume_proxy("   ") is None


def test_resolve_volume_proxy_is_case_insensitive():
    assert symbols.resolve_volume_proxy("spx") == "SPY"
    assert symbols.resolve_volume_proxy("  Ndx ") == "QQQ"


def test_env_overrides_default_proxies():
    with patch.dict(os.environ, {"INDEX_VOLUME_PROXIES": "SPX=VOO,XSP=SPY"}):
        proxies = symbols.get_index_volume_proxies()
        # Override wins for SPX, but the other defaults remain.
        assert proxies["SPX"] == "VOO"
        assert proxies["NDX"] == "QQQ"
        # New mappings from the env var get merged in.
        assert proxies["XSP"] == "SPY"


def test_is_cash_index_true_for_cash_indices():
    assert symbols.is_cash_index("SPX") is True
    assert symbols.is_cash_index("NDX") is True
    assert symbols.is_cash_index("RUT") is True
    assert symbols.is_cash_index("DJX") is True
    # Case-insensitive / whitespace tolerant, like resolve_volume_proxy.
    assert symbols.is_cash_index("spx") is True
    assert symbols.is_cash_index("  Ndx ") is True


def test_is_cash_index_false_for_etfs_and_equities():
    assert symbols.is_cash_index("SPY") is False
    assert symbols.is_cash_index("QQQ") is False
    assert symbols.is_cash_index("AAPL") is False
    assert symbols.is_cash_index("") is False
    assert symbols.is_cash_index("   ") is False


def test_is_cash_index_follows_env_overrides():
    # A symbol newly mapped to a volume proxy via env is treated as a
    # cash index, keeping the heatmap session-filter logic consistent.
    with patch.dict(os.environ, {"INDEX_VOLUME_PROXIES": "XSP=SPY"}):
        assert symbols.is_cash_index("XSP") is True
