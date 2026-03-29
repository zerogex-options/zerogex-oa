from datetime import datetime, timezone

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
