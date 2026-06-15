"""The IV solver must return None (not the clamped bound) on saturation.

A deep-OTM / near-expiry quote whose Newton iterate pins to IV_MIN or
IV_MAX never reaches option_price, so the bound is a solver *failure*, not
a converged IV. Returning the bound persisted a fake IV pinned at the
floor/ceiling into option_chains, polluting the vol surface and the
analytics vanna/charm + re-greeked gamma that read it.
"""

from datetime import date, datetime, timedelta, timezone

from src.ingestion.iv_calculator import IVCalculator


def _solver():
    return IVCalculator(max_iterations=100, min_iv=0.01, max_iv=5.0)


def test_unsolvable_high_price_returns_none_not_ceiling():
    iv = _solver()
    exp = (datetime.now(timezone.utc) + timedelta(days=7)).date()
    now = datetime.now(timezone.utc)
    # Option priced far above any BS value reachable below IV_MAX for a
    # deep-OTM strike -> solver saturates the ceiling. Must be None.
    result = iv.calculate_iv(
        option_price=900.0,
        underlying_price=100.0,
        strike=200.0,
        expiration=exp,
        option_type="C",
        current_time=now,
        risk_free_rate=0.05,
    )
    assert result is None


def test_normal_quote_still_solves():
    iv = _solver()
    exp = (datetime.now(timezone.utc) + timedelta(days=30)).date()
    now = datetime.now(timezone.utc)
    # Round-trip: price an ATM call at a known IV, then recover it.
    target_iv = 0.20
    price = iv._black_scholes_price(100.0, 100.0, 30 / 365.0, 0.05, target_iv, "C")
    result = iv.calculate_iv(
        option_price=price,
        underlying_price=100.0,
        strike=100.0,
        expiration=exp,
        option_type="C",
        current_time=now,
        risk_free_rate=0.05,
    )
    assert result is not None
    # Recovered IV should be close to target and strictly inside the bounds.
    assert iv.min_iv < result < iv.max_iv
    assert abs(result - target_iv) < 0.05


def test_result_never_equals_a_bound():
    iv = _solver()
    exp = date(2026, 12, 18)
    now = datetime(2026, 6, 13, tzinfo=timezone.utc)
    # Whatever the solver returns for an adversarial quote, it must never be
    # exactly the floor or ceiling (those are failure sentinels -> None).
    result = iv.calculate_iv(
        option_price=0.0001,
        underlying_price=100.0,
        strike=100.0,
        expiration=exp,
        option_type="C",
        current_time=now,
        risk_free_rate=0.05,
    )
    assert result not in (iv.min_iv, iv.max_iv)
