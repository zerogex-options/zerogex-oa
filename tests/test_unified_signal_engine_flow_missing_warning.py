"""The "no flow_contract_facts rows in last 30 minutes" diagnostic must
fire during the trading day (so a real ingestion gap surfaces in journal
logs) but stay silent outside extended market hours (so an operator
restart on a Friday evening / weekend / holiday doesn't re-fire the
warning for every symbol when zero flow rows is the expected state).
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


def _make_engine(use_module, db_symbol: str = "SPY"):
    with patch.object(use_module, "get_canonical_symbol", return_value=db_symbol):
        return use_module.UnifiedSignalEngine(db_symbol)


def _stub_cursor_with_no_flow(now_ts):
    """Cursor where the underlying+gex_summary fetch succeeds but every
    subsequent fetchone/fetchall returns nothing — so flow deltas and
    smart-money premium all collapse to 0.0 and the diagnostic check is
    reached with its full condition satisfied except for the new
    market-hours gate."""

    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (
            now_ts,  # uq.timestamp
            500.0,  # uq.close
            now_ts,  # gs.timestamp
            -1.0e9,  # gs.total_net_gex
            499.0,  # gs.gamma_flip_point
            0.002,  # gs.flip_distance
            5.0e8,  # gs.local_gex
            0.05,  # gs.convexity_risk
            1.0,  # gs.put_call_ratio
            500.0,  # gs.max_pain
            10000,  # gs.total_call_oi
            10000,  # gs.total_put_oi
        ),
    ] + [None] * 200
    cursor.fetchall.return_value = []
    cursor.execute.return_value = None
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _stub_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.rollback = MagicMock()
    return conn


def _flow_missing_warnings(logger_mock) -> int:
    """Count logger.warning calls that match the flow-missing diagnostic."""
    count = 0
    for call in logger_mock.warning.call_args_list:
        args, _kwargs = call
        if args and isinstance(args[0], str) and "no flow_contract_facts rows" in args[0]:
            count += 1
    return count


def test_flow_missing_warning_fires_during_market_hours(monkeypatch):
    use_module = _reload_engine(monkeypatch)
    eng = _make_engine(use_module, "SPY")

    now = datetime(2026, 6, 5, 14, 35, tzinfo=timezone.utc)  # Fri 10:35 ET — RTH
    cursor = _stub_cursor_with_no_flow(now)
    conn = _stub_conn(cursor)

    with (
        patch.object(use_module, "is_market_hours", return_value=True),
        patch.object(use_module, "logger") as logger_mock,
    ):
        try:
            eng._fetch_market_context(conn=conn)
        except Exception:
            # Downstream mocking gaps may raise; we only care about the
            # warning emitted before that point.
            pass

    assert _flow_missing_warnings(logger_mock) == 1, (
        "expected the flow-missing diagnostic to fire once during market hours; "
        f"warning calls were: {logger_mock.warning.call_args_list!r}"
    )
    assert (
        eng._flow_missing_logged.get("SPY") is True
    ), "latch must be set after the warning fires so the 1Hz cycle doesn't spam"


def test_flow_missing_warning_suppressed_when_market_closed(monkeypatch):
    use_module = _reload_engine(monkeypatch)
    eng = _make_engine(use_module, "SPY")

    now = datetime(2026, 6, 6, 2, 8, tzinfo=timezone.utc)  # Fri 22:08 ET == Sat 02:08 UTC — closed
    cursor = _stub_cursor_with_no_flow(now)
    conn = _stub_conn(cursor)

    with (
        patch.object(use_module, "is_market_hours", return_value=False),
        patch.object(use_module, "logger") as logger_mock,
    ):
        try:
            eng._fetch_market_context(conn=conn)
        except Exception:
            pass

    assert _flow_missing_warnings(logger_mock) == 0, (
        "expected NO flow-missing diagnostic when market is closed; "
        f"warning calls were: {logger_mock.warning.call_args_list!r}"
    )
    assert eng._flow_missing_logged.get("SPY") in (None, False), (
        "latch must NOT be set when the warning was suppressed, so a real "
        "ingestion gap at next market open still surfaces"
    )


def teardown_module(_module):
    import os

    os.environ.pop("SIGNALS_GEX_STALE_BUFFER_SECONDS", None)
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
