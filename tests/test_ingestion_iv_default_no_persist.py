"""IV default fallback must not be persisted into option_data.

Pre-fix, ``GreeksCalculator.enrich_option_data`` substituted
``IMPLIED_VOLATILITY_DEFAULT`` (0.20) for missing IV *and* wrote it back
into ``option_data["implied_volatility"]``. That sentinel then flowed
through to ``option_chains.implied_volatility`` and polluted the vol
surface: the router's ``_iv_or_null`` only rejects None / non-positive,
and the frontend averages call+put IV blindly, so a real OTM put IV
(e.g. 0.46) averaged against a 0.20 ITM-call sentinel produced a
meaningless ~0.33 average — visible as jagged pre-market vol curves on
SPY/QQQ (cash indices were unaffected because their queries clamp to
RTH).

Post-fix, the default is used only locally for the Greeks math; the
row's ``implied_volatility`` stays None so the column persists as NULL
and ``_iv_or_null`` filters it correctly.
"""

from datetime import datetime, timedelta, timezone

from src.ingestion.greeks_calculator import GreeksCalculator


def _row(iv):
    return {
        "implied_volatility": iv,
        "strike": 730.0,
        "expiration": (datetime.now(timezone.utc) + timedelta(days=1)).date(),
        "option_type": "C",
        "timestamp": datetime.now(timezone.utc),
    }


def _gc():
    gc = GreeksCalculator()
    # Bypass IVCalculator so the test exercises only the Greeks-side
    # fallback (the pre-fix bug). IVCalculator has its own None-handling
    # tested elsewhere via the ingestion pipeline.
    gc.iv_calculator = None
    return gc


def test_missing_iv_is_not_overwritten_with_default():
    gc = _gc()
    row = _row(iv=None)
    out = gc.enrich_option_data(row, underlying_price=736.4)
    assert out["implied_volatility"] is None, (
        f"IV default (0.20) leaked back into option_data as "
        f"{out['implied_volatility']!r}; this is the regression that "
        f"polluted vol_surface."
    )


def test_missing_iv_still_yields_greeks_via_local_default():
    # The local default must still drive the Greeks math so delta/gamma/
    # theta/vega aren't blank for rows whose IV the solver couldn't pin.
    gc = _gc()
    out = gc.enrich_option_data(_row(iv=None), underlying_price=736.4)
    for k in ("delta", "gamma", "theta", "vega"):
        assert out[k] is not None, f"{k} should be computed using local default IV"


def test_real_iv_is_passed_through_unchanged():
    gc = _gc()
    out = gc.enrich_option_data(_row(iv=0.18), underlying_price=736.4)
    assert out["implied_volatility"] == 0.18
