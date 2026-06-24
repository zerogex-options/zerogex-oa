"""Premium-surface (Beta) helpers: premium resolution + extrinsic clamp.

The premium surface returns the extrinsic (time) value of an option,
``max(0, premium - intrinsic)``, where ``premium`` is the best available
quote.  These tests pin the two pieces of pure logic that decide what the
z-axis shows:

  * ``_resolve_premium`` — prefer a clean two-sided mid, fall back to the
    stored ``mid`` column, then ``last``; return None (a surface gap) when
    nothing usable exists or the quote is crossed/empty.
  * the extrinsic computation — intrinsic differs for calls vs puts and a
    stale/crossed mark below intrinsic must clamp to 0 rather than show
    negative time value.
"""

from src.api.routers.premium_surface import _resolve_premium


def test_resolve_premium_prefers_two_sided_mid():
    # Clean two-sided quote → mid of bid/ask.
    assert _resolve_premium({"bid": 1.0, "ask": 3.0, "mid": 5.0, "last": 9.0}) == 2.0


def test_resolve_premium_falls_back_to_mid_then_last():
    # One-sided / missing ask → skip live mid, use stored mid column.
    assert _resolve_premium({"bid": 1.0, "ask": None, "mid": 4.0, "last": 9.0}) == 4.0
    # No usable bid/ask and no mid → last trade.
    assert _resolve_premium({"bid": None, "ask": None, "mid": None, "last": 7.5}) == 7.5


def test_resolve_premium_rejects_crossed_and_empty():
    # Crossed quote (ask < bid) is not trusted; falls through to mid.
    assert _resolve_premium({"bid": 5.0, "ask": 1.0, "mid": 3.0, "last": 9.0}) == 3.0
    # Nothing usable anywhere → None (rendered as a gap, not a fabricated 0).
    assert _resolve_premium({"bid": None, "ask": None, "mid": None, "last": None}) is None
    assert _resolve_premium({"bid": 0, "ask": 0, "mid": 0, "last": 0}) is None


def _extrinsic(premium, strike, spot, option_type):
    """Mirror of the endpoint's per-strike extrinsic computation."""
    if option_type == "C":
        intrinsic = max(0.0, spot - strike)
    else:
        intrinsic = max(0.0, strike - spot)
    return max(0.0, premium - intrinsic)


def test_extrinsic_call_and_put_intrinsic():
    spot = 100.0
    # ITM call: premium 7, intrinsic 5 → extrinsic 2.
    assert _extrinsic(7.0, 95.0, spot, "C") == 2.0
    # ITM put: premium 7, intrinsic 5 → extrinsic 2.
    assert _extrinsic(7.0, 105.0, spot, "P") == 2.0
    # OTM call: intrinsic 0 → extrinsic equals premium.
    assert _extrinsic(1.5, 110.0, spot, "C") == 1.5


def test_extrinsic_clamps_negative_to_zero():
    spot = 100.0
    # Stale ITM call marked below intrinsic (premium 4 < intrinsic 10).
    assert _extrinsic(4.0, 90.0, spot, "C") == 0.0
    # Same for puts.
    assert _extrinsic(4.0, 110.0, spot, "P") == 0.0
