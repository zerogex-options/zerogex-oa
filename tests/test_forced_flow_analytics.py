"""Phase 2 -- analytics integration of the forced-flow engine.

Covers the pure leg-builder and the AnalyticsEngine wiring that computes the
per-snapshot forced-flow curve + levels (no DB needed).
"""

from datetime import date, datetime

import pytz

from src.analytics.forced_flow import ContractLeg, build_legs
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def test_build_legs_filters_and_maps():
    rows = [
        {"strike": 100, "option_type": "C", "open_interest": 500, "implied_volatility": 0.20},
        {
            "strike": 105,
            "option_type": "P",
            "open_interest": 300,
            "implied_volatility": None,
        },  # no IV
        {"strike": 95, "option_type": "C", "open_interest": 0, "implied_volatility": 0.20},  # no OI
        {
            "strike": 110,
            "option_type": "P",
            "open_interest": 400,
            "implied_volatility": 0.0,
        },  # bad IV
        {
            "strike": 90,
            "option_type": "C",
            "open_interest": 200,
            "implied_volatility": 0.25,
        },  # expired
    ]

    # tte: expired for the strike-90 row, 30d otherwise.
    def tte_of(o):
        return 0.0 if o["strike"] == 90 else 30 / 365.0

    legs = build_legs(rows, tte_of)
    # Only the strike-100 call clears every filter (null IV, zero OI, bad IV,
    # and the expired row are all dropped).
    assert len(legs) == 1
    assert isinstance(legs[0], ContractLeg)
    assert legs[0].strike == 100.0
    assert legs[0].iv == 0.20 and legs[0].open_interest == 500.0


def test_session_days_remaining():
    eng = AnalyticsEngine("SPY")
    two_pm = ET.localize(datetime(2026, 7, 13, 14, 0))
    assert abs(eng._session_days_remaining(two_pm) - 2 / 24) < 1e-6
    after_close = ET.localize(datetime(2026, 7, 13, 17, 0))
    assert eng._session_days_remaining(after_close) == 0.0
    pre_open = ET.localize(datetime(2026, 7, 13, 9, 30))
    assert eng._session_days_remaining(pre_open) > eng._session_days_remaining(two_pm)


def _snapshot():
    ts = ET.localize(datetime(2026, 7, 13, 14, 0))
    exp = date(2026, 8, 12)

    def opt(K, ot, oi, iv=0.18):
        return {
            "strike": K,
            "option_type": ot,
            "open_interest": oi,
            "implied_volatility": iv,
            "expiration": exp,
            "option_symbol": f"SPY 260812{ot}{int(K)}",
        }

    options = [
        opt(495, "C", 800),
        opt(500, "C", 1500),
        opt(505, "C", 1200),
        opt(510, "C", 600),
        opt(490, "P", 700),
        opt(495, "P", 1300),
        opt(500, "P", 1400),
        opt(505, "P", 500),
    ]
    summary = {"underlying": "SPY", "timestamp": ts, "underlying_price": 500.0}
    return options, summary


def test_calculate_forced_flow_shape():
    eng = AnalyticsEngine("SPY")
    options, summary = _snapshot()
    ff = eng._calculate_forced_flow(options, summary)
    assert ff is not None
    assert ff["span_pct"] == 0.05
    assert 0.0 < ff["session_days"] < 1.0
    assert len(ff["curve"]) == 21
    prices = [p for p, _ in ff["curve"]]
    assert prices == sorted(prices)
    # levels are float or None, and within the grid span when present.
    for key in ("charm_flip", "vanna_flip", "zero_flow_level"):
        v = ff[key]
        assert v is None or (500.0 * 0.95 <= v <= 500.0 * 1.05)
    # zero-flow sits near current spot for a near-term horizon.
    assert ff["zero_flow_level"] is not None
    assert abs(ff["zero_flow_level"] - 500.0) < 25.0


def test_calculate_forced_flow_degraded_returns_none():
    eng = AnalyticsEngine("SPY")
    options, summary = _snapshot()
    assert eng._calculate_forced_flow(options, {**summary, "underlying_price": 0.0}) is None
    assert eng._calculate_forced_flow([], summary) is None
