"""Regression: ``_fetch_market_context`` must NOT cascade-fail when a
defensive sub-query errors.

Failure mode in production (logs from May 21):

    WARNING - UnifiedSignalEngine [QQQ]: skew fetch failed: canceling
        statement due to statement timeout
    WARNING - UnifiedSignalEngine [QQQ]: signed smart-money fetch failed:
        current transaction is aborted, commands ignored until end of
        transaction block
    ERROR - SignalEngineService cycle failed: current transaction is
        aborted, commands ignored until end of transaction block

A single defensive try/except in ``_fetch_market_context`` swallowed the
underlying timeout, but left the transaction in InFailedSqlTransaction
state.  The next defensive query then ALSO failed with the cascaded
"transaction is aborted" message, and the unguarded fallback inside the
smart-money except block raised out, taking the whole signals cycle
with it.

Fix: every defensive ``except`` in ``_fetch_market_context`` resets the
transaction (``self._reset_tx(conn)``) BEFORE running anything else, so
the next sub-query starts in a clean tx.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import psycopg2


def _reload_engine(monkeypatch):
    monkeypatch.setenv("SIGNALS_GEX_STALE_BUFFER_SECONDS", "0")
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
    return use


def _make_engine(use_module, db_symbol: str = "SPY"):
    with patch.object(use_module, "get_canonical_symbol", return_value=db_symbol):
        return use_module.UnifiedSignalEngine(db_symbol)


def _stub_cursor_with_skew_timeout(now_ts):
    """Cursor that succeeds on the underlying/gex_summary read, then raises
    a QueryCanceled on the FIRST defensive sub-query that hits
    option_chains (the skew fetch).  Any subsequent execute must NOT see
    a transaction-aborted error -- if it does the test fails."""

    cursor = MagicMock()
    # Seed enough fetchone rows for the unguarded queries that precede the
    # defensive ones (underlying+gex_summary at idx 0, vwap at idx 1).
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
        (500.0, 0.001),    # vwap + dev
    ] + [None] * 50
    cursor.fetchall.return_value = []

    call_state = {
        "skew_raised": False,
        "executes_after_skew_failure": 0,
    }

    def execute_side_effect(sql, params=None):
        # Trigger the failure on the skew query (first option_chains
        # SELECT that filters by strike BETWEEN).
        if (
            not call_state["skew_raised"]
            and "option_chains" in sql
            and "strike BETWEEN" in sql
            and "implied_volatility" in sql
        ):
            call_state["skew_raised"] = True
            raise psycopg2.errors.QueryCanceled(
                "canceling statement due to statement timeout"
            )
        # After the skew failure, any execute that ISN'T a ROLLBACK must
        # not raise InFailedSqlTransaction -- success means the fix works.
        if call_state["skew_raised"]:
            call_state["executes_after_skew_failure"] += 1
        return None

    cursor.execute.side_effect = execute_side_effect
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor, call_state


def _stub_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    # _reset_tx calls conn.rollback() -- track invocations.
    conn.rollback = MagicMock()
    return conn


def test_skew_timeout_does_not_break_subsequent_defensive_queries(monkeypatch):
    use_module = _reload_engine(monkeypatch)
    eng = _make_engine(use_module, "SPY")

    now = datetime(2026, 5, 21, 14, 35, tzinfo=timezone.utc)
    cursor, state = _stub_cursor_with_skew_timeout(now)
    conn = _stub_conn(cursor)

    # Drive the function; even if downstream stages return None, the
    # function must not propagate the cascaded transaction-aborted
    # exception out of _fetch_market_context.
    try:
        eng._fetch_market_context(conn=conn)
    except Exception as exc:
        # Any cascaded exception is a regression.
        if "current transaction is aborted" in str(exc):
            raise AssertionError(
                "Defensive sub-query failure cascaded into a "
                "transaction-abort error: " + repr(exc)
            )
        # Other exceptions (e.g. from incomplete mock data) are fine --
        # we're only asserting the cascade doesn't happen.

    # The skew failure must have triggered a rollback so subsequent
    # defensive queries see a clean transaction.
    assert state["skew_raised"], "test setup: skew timeout never fired"
    assert conn.rollback.called, (
        "expected _reset_tx (conn.rollback) inside the skew except block, "
        "so the next defensive sub-query starts on a clean transaction"
    )


def teardown_module(_module):
    import os

    os.environ.pop("SIGNALS_GEX_STALE_BUFFER_SECONDS", None)
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
