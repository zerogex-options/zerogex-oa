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
