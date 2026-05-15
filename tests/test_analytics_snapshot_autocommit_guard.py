"""Tests for the SET LOCAL autocommit defensive guard in
``AnalyticsEngine._run_snapshot_query``.

The cold-start statement_timeout is raised via ``SET LOCAL
statement_timeout``.  ``SET LOCAL`` is a silent no-op outside a
transaction, so if the connection pool is ever switched to autocommit
the cold-start ceiling would silently vanish and reintroduce the
May-13-style snapshot wedge with zero signal.  ``_run_snapshot_query``
emits a WARNING (it does NOT hard-fail) when it detects autocommit so
the regression is at least observable.
"""

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from src.analytics.main_engine import AnalyticsEngine

_WARN_MARKER = "AUTOCOMMIT"


def _cursor_with_autocommit(autocommit_value):
    cursor = MagicMock()
    cursor.connection.autocommit = autocommit_value
    cursor.fetchall.return_value = []
    return cursor


def _call(engine, cursor, statement_timeout_ms):
    ts = datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc)
    return engine._run_snapshot_query(
        cursor,
        ts,
        lookback_hours=2,
        min_expiration=(ts - timedelta(days=1)).date(),
        row_cap=50000,
        statement_timeout_ms=statement_timeout_ms,
    )


def test_warns_when_autocommit_enabled(caplog):
    engine = AnalyticsEngine(underlying="SPY")
    cursor = _cursor_with_autocommit(True)

    with caplog.at_level(logging.WARNING):
        _call(engine, cursor, statement_timeout_ms=180000)

    assert any(
        _WARN_MARKER in r.message and r.levelno == logging.WARNING for r in caplog.records
    ), f"expected an autocommit WARNING; got {[r.message for r in caplog.records]}"
    # The guard does NOT hard-fail: SET LOCAL is still issued.
    assert any(
        c[0][0].startswith("SET LOCAL statement_timeout") for c in cursor.execute.call_args_list
    )


def test_no_warning_when_autocommit_disabled(caplog):
    engine = AnalyticsEngine(underlying="SPY")
    cursor = _cursor_with_autocommit(False)

    with caplog.at_level(logging.WARNING):
        _call(engine, cursor, statement_timeout_ms=180000)

    assert not any(_WARN_MARKER in r.message for r in caplog.records)
    # SET LOCAL still issued on the (correct) non-autocommit path.
    assert any(
        c[0][0].startswith("SET LOCAL statement_timeout") for c in cursor.execute.call_args_list
    )


def test_no_warning_and_no_set_local_when_timeout_zero(caplog):
    """statement_timeout_ms == 0 (steady-state path): no SET LOCAL at all,
    so the autocommit guard is irrelevant and must stay silent even if the
    connection happens to be in autocommit mode."""
    engine = AnalyticsEngine(underlying="SPY")
    cursor = _cursor_with_autocommit(True)

    with caplog.at_level(logging.WARNING):
        _call(engine, cursor, statement_timeout_ms=0)

    assert not any(_WARN_MARKER in r.message for r in caplog.records)
    assert not any(
        c[0][0].startswith("SET LOCAL statement_timeout") for c in cursor.execute.call_args_list
    )
