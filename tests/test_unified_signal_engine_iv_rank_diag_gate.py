"""The ``iv_rank read returned null components`` diagnostic must stay
silent during the pre-market RTH-grace window (04:00–10:00 ET) — the
analytics engine legitimately hasn't UPSERTed today's ``daily_atm_iv``
row yet because no Greek-bearing options data is flowing. A worker
restart in that window previously false-fired the warning once per
symbol; gating on ``is_rth_settled`` defers it until the data should
actually be present.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


def _reload_engine(monkeypatch):
    monkeypatch.setenv("SIGNALS_GEX_STALE_BUFFER_SECONDS", "0")
    monkeypatch.setenv("SIGNAL_IV_RANK_ENABLED", "true")
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
    return use


def _make_engine(use_module, db_symbol: str = "SPY"):
    with patch.object(use_module, "get_canonical_symbol", return_value=db_symbol):
        return use_module.UnifiedSignalEngine(db_symbol)


def _stub_cursor_with_null_today_iv(now_ts):
    """Cursor where the initial market-context fetch succeeds, the
    iv_rank query returns ``(current_iv=None, iv_low=0.18, iv_high=0.39,
    days=20)`` (today's row not yet UPSERTed, history present), and
    every other fetchone returns None so the path lands cleanly on the
    diag branch at line ~1104.
    """
    cursor = MagicMock()
    market_ctx = (
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
        10_000,  # gs.total_call_oi
        10_000,  # gs.total_put_oi
    )
    # The iv_rank fetchone is preceded by several other fetchones in
    # _fetch_market_context (vwap, walls, flow_delta, prev_row, vix,
    # wrow). Return None for those; the engine's downstream code
    # handles them as missing. The iv_rank fetchone gets the NULL-today
    # row we care about.
    null_today_row = (None, 0.18, 0.39, 20)
    cursor.fetchone.side_effect = [market_ctx] + [None] * 6 + [null_today_row] + [None] * 200
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


def _iv_rank_diag_warnings(logger_mock) -> int:
    count = 0
    for call in logger_mock.warning.call_args_list:
        args, _kwargs = call
        if args and isinstance(args[0], str) and "iv_rank read returned null components" in args[0]:
            count += 1
    return count


def test_iv_rank_diag_suppressed_during_premarket_rth_grace(monkeypatch):
    """At 04:19 ET Tue (the production false-positive scenario) the
    analytics engine has not yet UPSERTed today's daily_atm_iv row.
    The diagnostic must stay silent so the operator doesn't get a
    one-shot warning every worker restart in the pre-market window."""
    use_module = _reload_engine(monkeypatch)
    eng = _make_engine(use_module, "SPY")

    now = datetime(2026, 6, 16, 8, 19, tzinfo=timezone.utc)  # Tue 04:19 ET
    cursor = _stub_cursor_with_null_today_iv(now)
    conn = _stub_conn(cursor)

    with (
        patch.object(use_module, "is_rth_settled", return_value=False),
        patch.object(use_module, "logger") as logger_mock,
    ):
        try:
            eng._fetch_market_context(conn=conn)
        except Exception:
            # Downstream mocking gaps may raise; we only care about the
            # iv_rank diag emitted before that point.
            pass

    assert _iv_rank_diag_warnings(logger_mock) == 0, (
        "expected NO iv_rank diag during the 04:00–10:00 ET RTH grace; "
        f"warning calls were: {logger_mock.warning.call_args_list!r}"
    )
    assert eng._iv_rank_diag_logged.get("SPY") in (None, False), (
        "latch must stay un-armed so a real ingestion gap once RTH " "settles still surfaces"
    )


def test_iv_rank_diag_fires_once_after_rth_settles(monkeypatch):
    """Same NULL-today scenario but past 10:00 ET — by now today's
    daily_atm_iv row really should exist, so the diag surfaces the gap
    (exactly once per symbol per process via the latch)."""
    use_module = _reload_engine(monkeypatch)
    eng = _make_engine(use_module, "SPY")

    now = datetime(2026, 6, 16, 15, 30, tzinfo=timezone.utc)  # Tue 11:30 ET
    cursor = _stub_cursor_with_null_today_iv(now)
    conn = _stub_conn(cursor)

    with (
        patch.object(use_module, "is_rth_settled", return_value=True),
        patch.object(use_module, "logger") as logger_mock,
    ):
        try:
            eng._fetch_market_context(conn=conn)
        except Exception:
            pass

    assert _iv_rank_diag_warnings(logger_mock) == 1, (
        "expected the iv_rank diag to fire once during settled RTH; "
        f"warning calls were: {logger_mock.warning.call_args_list!r}"
    )
    assert eng._iv_rank_diag_logged.get("SPY") is True, (
        "latch must be set after the warning fires so the 1Hz cycle " "doesn't spam"
    )


def teardown_module(_module):
    import os

    os.environ.pop("SIGNALS_GEX_STALE_BUFFER_SECONDS", None)
    os.environ.pop("SIGNAL_IV_RANK_ENABLED", None)
    import src.config as config
    import src.signals.unified_signal_engine as use

    importlib.reload(config)
    importlib.reload(use)
