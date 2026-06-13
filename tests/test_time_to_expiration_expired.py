"""Time-to-expiration must report 0 for already-expired contracts.

The 30-minute TTE floor exists to tame the BS ``1/√T`` gamma spike for
*still-alive* near-expiry contracts. Applied to an already-expired
contract it fabricated ~30 min of life, so the ``T <= 0`` guards in the
Greeks/IV/gamma code never fired and dead contracts (whose option_chains
rows linger after settlement) produced non-NULL Greeks.
"""

from datetime import date, datetime

import pytz

from src.market_calendar import calculate_time_to_expiration

ET = pytz.timezone("US/Eastern")


def test_expired_pm_contract_returns_zero():
    exp = date(2026, 6, 12)
    # One day after a 16:00 PM settlement.
    now = ET.localize(datetime(2026, 6, 13, 10, 0))
    assert calculate_time_to_expiration(now, exp, market_close_time="16:00:00") == 0.0


def test_am_settled_after_soq_returns_zero():
    exp = date(2026, 6, 19)
    # 10:00 ET on expiration morning, AM-settled (09:30 SOQ already passed).
    now = ET.localize(datetime(2026, 6, 19, 10, 0))
    assert calculate_time_to_expiration(now, exp, market_close_time="09:30:00") == 0.0


def test_alive_near_expiry_still_floored_positive():
    exp = date(2026, 6, 19)
    # 15:59 ET on PM expiration day: 1 minute of real life remains, but the
    # floor keeps T strictly positive so near-expiry gamma doesn't spike.
    now = ET.localize(datetime(2026, 6, 19, 15, 59))
    t = calculate_time_to_expiration(now, exp, market_close_time="16:00:00")
    assert t > 0.0
    # Floored to ~30 min (1/17520 yr), not the raw ~1 min.
    assert t >= 1.0 / 17520.0 - 1e-12


def test_future_contract_unaffected():
    exp = date(2026, 7, 17)
    now = ET.localize(datetime(2026, 6, 13, 10, 0))
    t = calculate_time_to_expiration(now, exp, market_close_time="16:00:00")
    # ~34 days out -> well above the floor.
    assert t > 0.08
