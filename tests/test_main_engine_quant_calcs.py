from datetime import datetime, timezone
from unittest.mock import MagicMock

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
    expected_call_gex = expected_weighted_gamma * 100 * 500.0

    assert row["call_gamma"] == expected_weighted_gamma
    assert row["net_gex"] == expected_call_gex


def test_max_pain_minimizes_total_intrinsic_payout():
    engine = AnalyticsEngine(underlying="SPY")
    options = [
        {"strike": 100.0, "option_type": "C", "open_interest": 100, "volume": 0, "gamma": 0, "expiration": datetime(2026, 3, 27).date(), "implied_volatility": 0.2},
        {"strike": 110.0, "option_type": "P", "open_interest": 100, "volume": 0, "gamma": 0, "expiration": datetime(2026, 3, 27).date(), "implied_volatility": 0.2},
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
    conn = MagicMock()
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

    engine._store_gex_summary(summary, conn=conn, cursor=cursor, commit=False)

    # First execute fetches prior non-null gamma flip.
    assert cursor.execute.call_count >= 2
    insert_args = cursor.execute.call_args_list[-1][0][1]
    assert insert_args[4] == 501.25


def test_store_gex_summary_keeps_current_gamma_flip_when_present():
    engine = AnalyticsEngine(underlying="SPY")
    conn = MagicMock()
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

    engine._store_gex_summary(summary, conn=conn, cursor=cursor, commit=False)

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
