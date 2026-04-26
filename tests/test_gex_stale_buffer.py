"""Tests for the Phase 2.5 look-ahead defensive buffer.

Verifies that ``SIGNALS_GEX_STALE_BUFFER_SECONDS`` is correctly threaded
into the two queries that read GEX-derived data in
``UnifiedSignalEngine._fetch_market_context``:

  * the lateral join against ``gex_summary`` (SQL-level INTERVAL math)
  * the ``WITH latest`` query against ``gex_by_strike`` (Python-level
    timedelta on the anchor timestamp)

Default 0 must be a no-op for live trading; positive values must
subtract from the anchor in both paths.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


def _reload_engine(monkeypatch, *, buffer_seconds: int):
    """Reload config + unified_signal_engine so the env var takes effect."""
    monkeypatch.setenv("SIGNALS_GEX_STALE_BUFFER_SECONDS", str(buffer_seconds))
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
    return use


def _make_engine(use_module):
    with patch.object(use_module, "get_canonical_symbol", return_value="SPY"):
        return use_module.UnifiedSignalEngine("SPY")


def _stub_cursor(now_ts):
    """A cursor whose first fetchone returns a valid (uq, gs) row.

    Subsequent execute calls (vwap, gex_by_strike, flow_by_type, ...) are
    swallowed and return empty results so the test exits cleanly without
    invoking the full pipeline.
    """
    cursor = MagicMock()
    # Configure fetchone for the first SELECT (uq + gs join):
    # 12 columns matching the order in unified_signal_engine.py:100-111
    cursor.fetchone.side_effect = [
        (
            now_ts,        # uq.timestamp
            500.0,         # uq.close
            now_ts,        # gs.timestamp
            -1.0e9,        # gs.total_net_gex
            499.0,         # gs.gamma_flip_point
            0.002,         # gs.flip_distance
            5.0e8,         # gs.local_gex
            0.05,          # gs.convexity_risk
            1.0,           # gs.put_call_ratio
            500.0,         # gs.max_pain
            10000,         # gs.total_call_oi
            10000,         # gs.total_put_oi
        ),
    ] + [None] * 50  # subsequent fetchones return None → empty paths
    cursor.fetchall.return_value = []
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _stub_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestStaleBufferDefault:
    """Default 0 → buffer subtraction is a no-op."""

    def test_default_passes_zero_to_first_query(self, monkeypatch):
        use_module = _reload_engine(monkeypatch, buffer_seconds=0)
        eng = _make_engine(use_module)
        now = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
        cursor = _stub_cursor(now)
        conn = _stub_conn(cursor)

        with patch.object(use_module, "db_connection", return_value=MagicMock(
            __enter__=lambda self: conn, __exit__=lambda *a: False
        )):
            eng._fetch_market_context(conn=conn)

        first_call = cursor.execute.call_args_list[0]
        sql, params = first_call.args
        # The lateral-join query takes (symbol, buffer_seconds, symbol).
        assert params == ("SPY", 0, "SPY")
        # SQL still references the INTERVAL math even at buffer=0 — the
        # subtraction just collapses to a no-op.
        assert "INTERVAL '1 second'" in sql

    def test_default_passes_anchor_unmodified_to_gex_by_strike(self, monkeypatch):
        use_module = _reload_engine(monkeypatch, buffer_seconds=0)
        eng = _make_engine(use_module)
        now = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
        cursor = _stub_cursor(now)
        conn = _stub_conn(cursor)

        with patch.object(use_module, "db_connection", return_value=MagicMock(
            __enter__=lambda self: conn, __exit__=lambda *a: False
        )):
            try:
                eng._fetch_market_context(conn=conn)
            except Exception:
                # Pipeline may bail out partway through with the empty stubs;
                # we only care about the queries that did execute.
                pass

        # Find the gex_by_strike query among captured execute calls.
        for call in cursor.execute.call_args_list:
            sql, params = call.args
            if "gex_by_strike" in sql and "MAX(timestamp)" in sql:
                # params are (symbol, anchor_ts, symbol, strike_lo, strike_hi)
                assert params[1] == now  # buffer=0 → anchor unchanged
                return
        raise AssertionError("gex_by_strike query never executed")


class TestStaleBufferPositive:
    """Positive buffer values shift both queries' anchors backward."""

    def test_60_second_buffer_threads_to_lateral_join(self, monkeypatch):
        use_module = _reload_engine(monkeypatch, buffer_seconds=60)
        eng = _make_engine(use_module)
        now = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
        cursor = _stub_cursor(now)
        conn = _stub_conn(cursor)

        with patch.object(use_module, "db_connection", return_value=MagicMock(
            __enter__=lambda self: conn, __exit__=lambda *a: False
        )):
            eng._fetch_market_context(conn=conn)

        first_call = cursor.execute.call_args_list[0]
        _, params = first_call.args
        assert params == ("SPY", 60, "SPY")

    def test_60_second_buffer_subtracts_from_gex_by_strike_anchor(self, monkeypatch):
        use_module = _reload_engine(monkeypatch, buffer_seconds=60)
        eng = _make_engine(use_module)
        now = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
        cursor = _stub_cursor(now)
        conn = _stub_conn(cursor)

        with patch.object(use_module, "db_connection", return_value=MagicMock(
            __enter__=lambda self: conn, __exit__=lambda *a: False
        )):
            try:
                eng._fetch_market_context(conn=conn)
            except Exception:
                pass

        for call in cursor.execute.call_args_list:
            sql, params = call.args
            if "gex_by_strike" in sql and "MAX(timestamp)" in sql:
                assert params[1] == now - timedelta(seconds=60)
                return
        raise AssertionError("gex_by_strike query never executed")


def teardown_module(_module):
    """Restore default config so downstream tests aren't poisoned."""
    import os
    os.environ.pop("SIGNALS_GEX_STALE_BUFFER_SECONDS", None)
    import src.config as config
    import src.signals.unified_signal_engine as use
    importlib.reload(config)
    importlib.reload(use)
