from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine


def test_gex_by_strike_weights_gamma_by_open_interest():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 3, 27, 15, 55, tzinfo=timezone.utc)

    # Two call rows at same strike/expiry simulate split snapshots/contracts.
    options = [
        {
            "strike": 500.0,
            "expiration": ts.date(),
            "option_type": "C",
            "gamma": 0.01,
            "open_interest": 10,
            "volume": 1,
            "implied_volatility": 0.2,
        },
        {
            "strike": 500.0,
            "expiration": ts.date(),
            "option_type": "C",
            "gamma": 0.02,
            "open_interest": 20,
            "volume": 1,
            "implied_volatility": 0.2,
        },
    ]

    result = engine._calculate_gex_by_strike(options, underlying_price=500.0, timestamp=ts)
    row = result[0]

    expected_weighted_gamma = (0.01 * 10) + (0.02 * 20)
    # Industry-standard dollar GEX per 1% move: γ × OI × 100 × S² × 0.01.
    expected_call_gex = expected_weighted_gamma * 100 * 500.0 * 500.0 * 0.01

    assert row["call_gamma"] == expected_weighted_gamma
    assert row["net_gex"] == expected_call_gex


def test_max_pain_minimizes_total_intrinsic_payout():
    engine = AnalyticsEngine(underlying="SPY")
    options = [
        {
            "strike": 100.0,
            "option_type": "C",
            "open_interest": 100,
            "volume": 0,
            "gamma": 0,
            "expiration": datetime(2026, 3, 27).date(),
            "implied_volatility": 0.2,
        },
        {
            "strike": 110.0,
            "option_type": "P",
            "open_interest": 100,
            "volume": 0,
            "gamma": 0,
            "expiration": datetime(2026, 3, 27).date(),
            "implied_volatility": 0.2,
        },
    ]
    # At settlement 100 => put payout 1000; at 110 => call payout 1000; tie picks lower strike due sort.
    assert engine._calculate_max_pain(options) == 100.0


def test_gamma_flip_interpolates_between_sign_change_strikes():
    engine = AnalyticsEngine(underlying="SPY")
    gex = [
        {"strike": 100.0, "net_gex": -10.0},
        {"strike": 110.0, "net_gex": 10.0},
    ]
    flip = engine._calculate_gamma_flip_point(gex, underlying_price=105.0)
    assert flip == 105.0


def test_store_gex_summary_carries_forward_previous_gamma_flip_when_missing():
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()
    cursor.fetchone.return_value = (501.25,)

    summary = {
        "underlying": "SPY",
        "timestamp": datetime(2026, 4, 17, 14, 30, tzinfo=timezone.utc),
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1234.0,
        "gamma_flip_point": None,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 555.0,
    }

    engine._store_gex_summary(summary, cursor)

    # First execute fetches prior non-null gamma flip.
    assert cursor.execute.call_count >= 2
    insert_args = cursor.execute.call_args_list[-1][0][1]
    assert insert_args[4] == 501.25


def test_store_gex_summary_keeps_current_gamma_flip_when_present():
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()

    summary = {
        "underlying": "SPY",
        "timestamp": datetime(2026, 4, 17, 14, 31, tzinfo=timezone.utc),
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1234.0,
        "gamma_flip_point": 499.75,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 555.0,
    }

    engine._store_gex_summary(summary, cursor)

    # No carry-forward SELECT when current gamma flip exists.
    insert_args = cursor.execute.call_args_list[-1][0][1]
    assert insert_args[4] == 499.75


def test_gex_summary_includes_flip_distance_local_gex_and_convexity():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 4, 21, 14, 30, tzinfo=timezone.utc)
    spot = 500.0
    options = [
        {
            "strike": 500.0,
            "expiration": ts.date(),
            "option_type": "C",
            "gamma": 0.02,
            "open_interest": 10,
            "volume": 1,
            "implied_volatility": 0.2,
        }
    ]
    gex_by_strike = [
        {"strike": 495.0, "net_gex": -2_000_000.0},
        {"strike": 500.0, "net_gex": 3_000_000.0},
        {"strike": 505.0, "net_gex": 1_000_000.0},
    ]

    summary = engine._calculate_gex_summary(
        gex_by_strike=gex_by_strike,
        options=options,
        underlying_price=spot,
        timestamp=ts,
    )

    # Crossing between 495 (-2M) and 500 (+3M):
    # flip = 495 + 5*(2/5) = 497.
    assert summary["gamma_flip_point"] == 497.0
    assert summary["flip_distance"] == (spot - 497.0) / spot
    # ±1% band around spot includes strikes [495, 505].
    assert summary["local_gex"] == abs(-2_000_000.0) + abs(3_000_000.0) + abs(1_000_000.0)
    expected_convexity = abs(summary["total_net_gex"]) / abs(summary["flip_distance"])
    assert summary["convexity_risk"] == expected_convexity


def _full_gex_row(ts):
    return {
        "underlying": "SPY",
        "timestamp": ts,
        "strike": 500.0,
        "expiration": ts.date(),
        "total_gamma": 0.3,
        "call_gamma": 0.2,
        "put_gamma": 0.1,
        "net_gex": 1_000_000.0,
        "call_volume": 10,
        "put_volume": 5,
        "call_oi": 100,
        "put_oi": 50,
        "vanna_exposure": 1.0,
        "charm_exposure": 2.0,
        "call_vanna_exposure": 0.5,
        "put_vanna_exposure": 0.5,
        "call_charm_exposure": 1.0,
        "put_charm_exposure": 1.0,
        "dealer_vanna_exposure": -1.0,
        "dealer_charm_exposure": -2.0,
        "expiration_bucket": "0dte",
    }


def _full_summary(ts):
    return {
        "underlying": "SPY",
        "timestamp": ts,
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1_000_000.0,
        # Provide a non-None gamma_flip so _store_gex_summary skips the
        # carry-forward SELECT and the very first cursor.execute() is the
        # summary INSERT — i.e. the failure lands mid-transaction, AFTER
        # the by-strike write has already been issued.
        "gamma_flip_point": 499.0,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 1_000_000.0,
    }


def test_store_calculation_results_is_atomic_on_mid_transaction_failure(monkeypatch):
    """C1: by-strike + summary must commit together (all rows land or none).

    Simulate the summary write blowing up AFTER the by-strike rows were
    already issued on the shared cursor.  The whole transaction must roll
    back (never commit), so the by-strike rows do not persist — proving
    the single-transaction grouping survived the conn/cursor refactor.
    """
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 4, 17, 14, 30, tzinfo=timezone.utc)

    cursor = MagicMock()
    # Every cursor.execute() raises; with gamma_flip_point set the first
    # (and only) execute in _store_gex_summary is the summary INSERT.
    cursor.execute.side_effect = RuntimeError("summary insert blew up")
    conn = MagicMock()
    conn.cursor.return_value = cursor

    by_strike_writes = []

    def fake_execute_values(cur, sql, rows):
        # _store_gex_by_strike succeeds: record that the by-strike INSERT
        # was issued into this (soon-to-be-rolled-back) transaction.
        assert cur is cursor
        by_strike_writes.append(rows)

    monkeypatch.setattr(main_engine, "execute_values", fake_execute_values)

    @contextmanager
    def fake_db_connection():
        # Mirror src/database/connection.py: commit on clean exit,
        # rollback on any exception.
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    monkeypatch.setattr(main_engine, "db_connection", fake_db_connection)

    with pytest.raises(RuntimeError, match="summary insert blew up"):
        engine._store_calculation_results([_full_gex_row(ts)], _full_summary(ts))

    # The by-strike INSERT WAS issued (it ran before the summary failure)…
    assert len(by_strike_writes) == 1
    # …but exactly one connection/transaction was used…
    conn.cursor.assert_called_once()
    # …and it was rolled back, never committed: the by-strike rows that
    # were written in this transaction do NOT persist. All-or-nothing.
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    assert engine.errors_count == 1


def test_store_calculation_results_commits_once_on_success(monkeypatch):
    """Happy path: both writes land in a single committed transaction."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 4, 17, 14, 31, tzinfo=timezone.utc)

    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor

    monkeypatch.setattr(main_engine, "execute_values", lambda cur, sql, rows: None)

    committed = []

    @contextmanager
    def fake_db_connection():
        try:
            yield conn
            conn.commit()
            committed.append(True)
        except Exception:
            conn.rollback()
            raise

    monkeypatch.setattr(main_engine, "db_connection", fake_db_connection)

    engine._store_calculation_results([_full_gex_row(ts)], _full_summary(ts))

    conn.cursor.assert_called_once()  # one connection => one transaction
    conn.rollback.assert_not_called()
    assert conn.commit.called  # committed (explicit + CM are harmless dups)
    assert committed == [True]
    assert engine.errors_count == 0
