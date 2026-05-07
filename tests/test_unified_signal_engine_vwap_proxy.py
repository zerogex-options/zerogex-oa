"""Tests for VWAP proxy substitution for cash indices.

Cash indices (SPX, NDX, RUT, DJX) carry no transactional volume of their
own, so the standard ``underlying_vwap_deviation`` view returns NULL VWAP
for them.  ``UnifiedSignalEngine._fetch_market_context`` must route those
symbols through the proxy-based VWAP computation (mirroring the
``/api/technicals/vwap-deviation`` endpoint) so downstream signals such
as ``gamma_vwap_confluence`` receive a usable VWAP.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


def _reload_engine(monkeypatch):
    monkeypatch.setenv("SIGNALS_GEX_STALE_BUFFER_SECONDS", "0")
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
    return use


def _make_engine(use_module, db_symbol: str):
    with patch.object(use_module, "get_canonical_symbol", return_value=db_symbol):
        return use_module.UnifiedSignalEngine(db_symbol)


def _stub_cursor(now_ts):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (
            now_ts,
            500.0,
            now_ts,
            -1.0e9,
            499.0,
            0.002,
            5.0e8,
            0.05,
            1.0,
            500.0,
            10000,
            10000,
        ),
    ] + [None] * 50
    cursor.fetchall.return_value = []
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _stub_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _find_vwap_query(cursor):
    for call in cursor.execute.call_args_list:
        sql, params = call.args
        if "vwap" in sql.lower():
            return sql, params
    return None, None


class TestVwapProxyForCashIndices:
    """SPX/NDX/RUT/DJX should route through the proxy-based VWAP query."""

    def test_spx_uses_spy_volume_proxy(self, monkeypatch):
        use_module = _reload_engine(monkeypatch)
        eng = _make_engine(use_module, "SPX")
        now = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
        cursor = _stub_cursor(now)
        conn = _stub_conn(cursor)

        with patch.object(
            use_module,
            "db_connection",
            return_value=MagicMock(__enter__=lambda self: conn, __exit__=lambda *a: False),
        ):
            try:
                eng._fetch_market_context(conn=conn)
            except Exception:
                pass

        sql, params = _find_vwap_query(cursor)
        assert sql is not None, "expected a VWAP query to be executed"
        # Proxy path computes VWAP from underlying_quotes joined with
        # the proxy ETF's volume; the canonical view name should NOT
        # appear in the proxy query.
        assert "underlying_vwap_deviation" not in sql
        assert "index_quotes" in sql
        assert "proxy_volume" in sql
        # SPX is the index, SPY is the configured volume proxy.
        assert "SPX" in params
        assert "SPY" in params

    def test_ndx_uses_qqq_volume_proxy(self, monkeypatch):
        use_module = _reload_engine(monkeypatch)
        eng = _make_engine(use_module, "NDX")
        now = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
        cursor = _stub_cursor(now)
        conn = _stub_conn(cursor)

        with patch.object(
            use_module,
            "db_connection",
            return_value=MagicMock(__enter__=lambda self: conn, __exit__=lambda *a: False),
        ):
            try:
                eng._fetch_market_context(conn=conn)
            except Exception:
                pass

        _, params = _find_vwap_query(cursor)
        assert "NDX" in params
        assert "QQQ" in params


class TestVwapNoProxyForEquitiesAndEtfs:
    """SPY/QQQ/AAPL should keep using the canonical view directly."""

    def test_spy_uses_canonical_view(self, monkeypatch):
        use_module = _reload_engine(monkeypatch)
        eng = _make_engine(use_module, "SPY")
        now = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
        cursor = _stub_cursor(now)
        conn = _stub_conn(cursor)

        with patch.object(
            use_module,
            "db_connection",
            return_value=MagicMock(__enter__=lambda self: conn, __exit__=lambda *a: False),
        ):
            try:
                eng._fetch_market_context(conn=conn)
            except Exception:
                pass

        sql, params = _find_vwap_query(cursor)
        assert sql is not None
        assert "underlying_vwap_deviation" in sql
        assert "index_quotes" not in sql
        assert "proxy_volume" not in sql
        assert params == ("SPY", now)


def teardown_module(_module):
    import os

    os.environ.pop("SIGNALS_GEX_STALE_BUFFER_SECONDS", None)
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
