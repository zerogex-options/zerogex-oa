from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
import pytest
from scipy import stats

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine


def _opt(strike, otype, *, oi=1000, iv=0.20, exp=None, gamma=0.0, volume=0):
    """Minimal option-chain row for the spot-shift gamma profile."""
    return {
        "strike": strike,
        "option_type": otype,
        "open_interest": oi,
        "implied_volatility": iv,
        "expiration": exp,
        "gamma": gamma,
        "volume": volume,
    }


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
    # Settlement @100 => put payout 1000; @110 => call payout 1000;
    # tie picks the lower strike via the sort.
    assert engine._calculate_max_pain(options) == 100.0


def test_bs_gamma_matches_closed_form_and_degenerates():
    engine = AnalyticsEngine(underlying="SPY")
    S, K, T, r, sigma = 100.0, 100.0, 0.5, 0.05, 0.2
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    expected = stats.norm.pdf(d1) / (S * sigma * np.sqrt(T))
    assert abs(engine._calculate_bs_gamma(S, K, T, r, sigma) - expected) < 1e-12

    # Vectorised over a price grid == element-wise scalar calls.
    grid = np.array([80.0, 100.0, 130.0])
    arr = engine._calculate_bs_gamma(grid, K, T, r, sigma)
    assert isinstance(arr, np.ndarray)
    for s, g in zip(grid, arr):
        assert abs(float(g) - engine._calculate_bs_gamma(float(s), K, T, r, sigma)) < 1e-12
    assert arr[1] > arr[0] and arr[1] > arr[2]  # gamma peaks near ATM

    # Degenerate inputs => 0 / zeros, never NaN.
    assert engine._calculate_bs_gamma(100.0, 100.0, 0.0, r, sigma) == 0.0
    assert engine._calculate_bs_gamma(100.0, 100.0, T, r, 0.0) == 0.0
    assert list(engine._calculate_bs_gamma(grid, K, -1.0, r, sigma)) == [0.0, 0.0, 0.0]


def test_gamma_profile_resolves_interior_flip_and_is_sign_consistent():
    """Put mass below, call mass above => the spot-shift profile is short
    gamma near the puts and long near the calls, so it has a genuine
    interior zero crossing (the old cumulative-by-strike curve, given only
    these strikes, was one-signed => None => carry-forward freeze)."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    options = [
        _opt(92.0, "P", oi=8000, iv=0.30, exp=exp),
        _opt(112.0, "C", oi=8000, iv=0.30, exp=exp),
    ]
    profile = engine._gamma_exposure_profile(options, spot, ts)
    assert profile
    xs = [s for s, _ in profile]
    assert xs == sorted(xs)
    assert xs[0] <= spot * 0.80 + 1e-6 and xs[-1] >= spot * 1.20 - 1e-6
    # Negative near the put strike, positive near the call strike =>
    # exactly one interior crossing.
    assert profile[0][1] < 0 < profile[-1][1]
    flip = engine._calculate_gamma_flip_point(profile, spot)
    assert flip is not None
    assert 92.0 < flip < 112.0  # between the put and the call

    # The core invariant (independent of which side spot lands on): the
    # profile is short gamma strictly below the flip and long strictly
    # above it, and net_gex_at_spot's sign tracks the spot-vs-flip side.
    d = spot * 0.01
    assert engine._net_gex_at_spot(profile, flip - d) < 0
    assert engine._net_gex_at_spot(profile, flip + d) > 0
    assert (engine._net_gex_at_spot(profile, spot) < 0) == (spot < flip)


def test_gamma_flip_none_when_profile_one_signed():
    """A pure long-call book is dealer-long-gamma at every grid price (no
    crossing). The flip is NOT clamped to a grid edge — it returns None so
    the caller can mark it unresolved (degraded/one-sided chain) instead
    of fabricating a level or letting the carry-forward re-freeze."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    profile = engine._gamma_exposure_profile(
        [_opt(100.0, "C", oi=5000, iv=0.25, exp=exp)], spot, ts
    )
    assert all(v > 0 for _, v in profile)  # long gamma everywhere
    assert engine._calculate_gamma_flip_point(profile, spot) is None


def test_gamma_profile_none_when_no_usable_contracts():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    assert engine._gamma_exposure_profile([], 100.0, ts) == []
    assert engine._gamma_exposure_profile([_opt(100.0, "C", exp=exp)], 0.0, ts) == []
    # σ<=0 and OI<=0 contracts are skipped => no usable contracts.
    bad = [
        _opt(100.0, "C", oi=0, exp=exp),
        _opt(100.0, "P", iv=0.0, exp=exp),
    ]
    assert engine._gamma_exposure_profile(bad, 100.0, ts) == []
    assert engine._calculate_gamma_flip_point([], 100.0) is None
    assert engine._net_gex_at_spot([], 100.0) is None


def test_net_gex_at_spot_interpolates_and_clamps_generic_curve():
    """_net_gex_at_spot piecewise-linearly samples the profile and clamps
    to its endpoints outside the grid."""
    engine = AnalyticsEngine(underlying="SPY")
    profile = [(100.0, -14.0), (105.0, -14.0), (110.0, 6.0)]
    # Between 105 (-14) and 110 (+6): -14 + 20*(107-105)/5 = -6.
    assert abs(engine._net_gex_at_spot(profile, 107.0) - (-6.0)) < 1e-9
    assert abs(engine._net_gex_at_spot(profile, 109.0) - 2.0) < 1e-9
    assert engine._net_gex_at_spot(profile, 90.0) == -14.0  # clamp low edge
    assert engine._net_gex_at_spot(profile, 120.0) == 6.0  # clamp high edge


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


def test_store_gex_summary_persists_null_when_flip_unresolved():
    """Degraded/one-sided chain => gamma_flip_unresolved is set, so the
    carry-forward is SKIPPED and NULL is persisted (a visible gap),
    instead of silently re-freezing the last level (the original bug)."""
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()
    cursor.fetchone.return_value = (501.25,)  # a prior exists; must NOT be used

    summary = {
        "underlying": "SPY",
        "timestamp": datetime(2026, 4, 17, 14, 32, tzinfo=timezone.utc),
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1234.0,
        "gamma_flip_point": None,
        "gamma_flip_unresolved": True,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 555.0,
    }

    engine._store_gex_summary(summary, cursor)

    # No carry-forward SELECT — only the INSERT — and flip persists NULL.
    assert cursor.execute.call_count == 1
    insert_args = cursor.execute.call_args_list[-1][0][1]
    assert insert_args[4] is None


def test_store_gex_summary_persists_net_gex_at_spot():
    """End-to-end: net_gex_at_spot from the summary dict reaches the INSERT
    params (regression: it was dropped between compute and persist, so the
    column was always written NULL)."""
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
        "net_gex_at_spot": -1_234_567.0,
    }

    engine._store_gex_summary(summary, cursor)

    insert_args = cursor.execute.call_args_list[-1][0][1]
    # Param order: ... total_net_gex (11), net_gex_at_spot (12), flip_distance (13) ...
    assert insert_args[4] == 499.75  # gamma_flip_point index unchanged
    assert insert_args[11] == 555.0
    assert insert_args[12] == -1_234_567.0


def test_gex_summary_includes_flip_distance_local_gex_and_convexity():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 4, 21, 14, 30, tzinfo=timezone.utc)
    spot = 500.0
    exp = datetime(2026, 7, 17).date()
    # Put mass at/below spot, call block above => spot-shift profile is
    # short-gamma at spot and crosses to long above => a real flip in
    # (spot, call strike).
    options = [
        _opt(485.0, "P", oi=6000, iv=0.22, exp=exp, volume=10),
        _opt(520.0, "C", oi=6000, iv=0.22, exp=exp, volume=12),
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

    flip = summary["gamma_flip_point"]
    assert flip is not None
    assert 485.0 < flip < 520.0  # between the put and the call strike
    # flip_distance / convexity use the produced flip with the same formulas.
    assert summary["flip_distance"] == pytest.approx((spot - flip) / spot)
    # local_gex still comes from gex_by_strike (±1% of spot => [495,505]).
    assert summary["local_gex"] == 2_000_000.0 + 3_000_000.0 + 1_000_000.0
    expected_convexity = abs(summary["total_net_gex"]) / max(abs(summary["flip_distance"]), 1e-6)
    assert summary["convexity_risk"] == pytest.approx(expected_convexity)
    # net_gex_at_spot is read off the SAME profile, so its sign tracks the
    # spot-vs-flip regime (short gamma iff spot is below the flip).
    assert "net_gex_at_spot" in summary
    assert (summary["net_gex_at_spot"] < 0) == (spot < flip)


def test_gex_summary_marks_flip_unresolved_on_one_sided_chain(caplog):
    """One-signed (degraded) usable chain => no crossing => the summary
    reports gamma_flip_point=None, gamma_flip_unresolved=True, and WARNs
    (so it's visible in analytics-health, not a silent clamp/freeze)."""
    import logging

    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 500.0
    # Pure long-call book => dealer-long-gamma everywhere => no crossing.
    options = [_opt(500.0, "C", oi=5000, iv=0.25, exp=exp, volume=3)]
    gex_by_strike = [
        {"strike": 495.0, "net_gex": 1_000_000.0},
        {"strike": 505.0, "net_gex": 2_000_000.0},
    ]

    with caplog.at_level(logging.WARNING):
        summary = engine._calculate_gex_summary(
            gex_by_strike=gex_by_strike,
            options=options,
            underlying_price=spot,
            timestamp=ts,
        )

    assert summary["gamma_flip_point"] is None
    assert summary["gamma_flip_unresolved"] is True
    assert summary["flip_distance"] is None  # no flip => no distance
    assert "net_gex_at_spot" in summary  # still sampled from the profile
    assert any(
        "Gamma flip UNRESOLVED" in r.message and r.levelno == logging.WARNING
        for r in caplog.records
    )


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
