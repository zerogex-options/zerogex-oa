"""Tests for ``ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS``.

The pool-wide ``DB_STATEMENT_TIMEOUT_MS`` (default 90s) is sized for
sub-second API queries.  The steady-state snapshot is usually fast but
can spike past 90s under autovacuum + concurrent-ingestion bursts -- in
which case the pool kills every cycle, the snapshot exits at the
``if not snapshot`` guard, and the downstream flow_series_5min
refresh is never run (surfaced by the API as
``flow_series_5min shortfall`` warnings).

``ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS`` lets operators relax just
the snapshot query via ``SET LOCAL`` without raising the pool ceiling.
Default 0 = no override (current behavior).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _row(option_symbol, strike, expiration, option_type, quote_ts, *, gamma=0.01, oi=100):
    return (
        option_symbol, strike, expiration, option_type,
        1.0, 0.99, 1.01, 10, oi, 0.5, gamma, -0.05, 0.1, 0.2, quote_ts,
    )


def _mock_db_connection(latest_ts, underlying_price, option_rows):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(latest_ts,), (underlying_price,)]
    cursor.fetchall.return_value = option_rows
    cursor.execute.side_effect = lambda *a, **kw: None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, conn, cursor


def _set_local_calls(cursor):
    return [
        c
        for c in cursor.execute.call_args_list
        if "statement_timeout" in c[0][0] and "SET LOCAL" in c[0][0]
    ]


def _fresh_ts():
    return datetime.now(timezone.utc) - timedelta(minutes=1)


def test_steady_state_uses_pool_default_when_env_unset(monkeypatch):
    """Default (env unset) preserves current behavior: no SET LOCAL on the
    cheap steady-state path, so the pool-wide statement_timeout applies."""
    monkeypatch.delenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", raising=False)
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_statement_timeout_ms == 0

    ts = _fresh_ts()
    expiration = ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", ts)]
    cm, _, cursor = _mock_db_connection(ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    assert _set_local_calls(cursor) == []


def test_steady_state_applies_configured_statement_timeout(monkeypatch):
    """When ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS>0, the steady-state
    snapshot issues a SET LOCAL statement_timeout before the scan."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", "150000")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_statement_timeout_ms == 150000

    ts = _fresh_ts()
    expiration = ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", ts)]
    cm, _, cursor = _mock_db_connection(ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    set_local = _set_local_calls(cursor)
    assert len(set_local) == 1
    assert set_local[0][0][1] == ("150000",)


def test_steady_state_timeout_negative_floored_to_zero(monkeypatch):
    """Negative values floor to 0 -- guards against operator typos that
    would otherwise be silently negative-cast to a positive uint by libpq."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", "-1")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_statement_timeout_ms == 0
