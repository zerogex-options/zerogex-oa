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

