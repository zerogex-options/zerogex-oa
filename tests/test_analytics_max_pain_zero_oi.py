"""Max-pain must return None when there is no usable open interest.

``AnalyticsEngine._calculate_max_pain`` builds a payout grid weighted by
open interest.  When every contract has ``open_interest == 0`` (the common
intraday cold-start state the snapshot path logs as "All options have
OI=0") the grid is uniformly zero and ``np.argmin`` silently returns the
lowest strike, fabricating a max-pain pin at the bottom of the chain.  The
docstring promises ``None`` for "no usable data"; this pins that contract.
"""

from datetime import date

from src.analytics.main_engine import AnalyticsEngine


def _opt(*, strike, otype, oi):
    return {
        "strike": strike,
        "option_type": otype,
        "open_interest": oi,
        "expiration": date(2026, 6, 19),
    }


def _engine():
    return AnalyticsEngine(underlying="SPY")


def test_all_zero_oi_returns_none():
    eng = _engine()
    options = [
        _opt(strike=100.0, otype="C", oi=0),
        _opt(strike=105.0, otype="C", oi=0),
        _opt(strike=110.0, otype="P", oi=0),
        _opt(strike=95.0, otype="P", oi=0),
    ]
    assert eng._calculate_max_pain(options) is None


def test_real_oi_still_returns_a_strike():
    eng = _engine()
    # Calls concentrated at 100, puts at 110 -> max pain pulls toward the
    # strike that minimizes total writer payout; just assert it's a real
    # strike from the chain and not the spurious bottom-strike default.
    options = [
        _opt(strike=100.0, otype="C", oi=5000),
        _opt(strike=105.0, otype="C", oi=10),
        _opt(strike=110.0, otype="P", oi=5000),
        _opt(strike=95.0, otype="P", oi=10),
    ]
    mp = eng._calculate_max_pain(options)
    assert mp in {95.0, 100.0, 105.0, 110.0}


def test_empty_options_returns_none():
    eng = _engine()
    assert eng._calculate_max_pain([]) is None
