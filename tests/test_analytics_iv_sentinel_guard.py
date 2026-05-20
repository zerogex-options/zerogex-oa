"""Analytics IV-sentinel guard for vanna/charm.

``AnalyticsEngine._calculate_gex_by_strike`` previously substituted
``IMPLIED_VOLATILITY_DEFAULT`` (0.20) in-memory at the read site
(``analytics/main_engine.py:502``) whenever ``option_chains.
implied_volatility`` was NULL, then fed that sentinel into the
vanna/charm calculations. After the companion ingestion fix that lets
the column legitimately be NULL for strikes the IV solver can't pin
(typical of deep ITM options pre-market), the in-memory sentinel would
silently fabricate vanna/charm exposure for those contracts — invisible
in aggregate but real in the gex_by_strike row.

Both layers now skip rows without a reliable IV instead of pretending
0.20 is real. Gamma/GEX were already correctly skipped via the
``sigma <= 0`` guard in ``_build_gamma_profile``; this test pins the
matching behavior for vanna/charm.
"""

from datetime import datetime, timezone

from src.analytics.main_engine import AnalyticsEngine


def _opt(*, strike, otype, iv, oi=1000, gamma=0.01, expiration, volume=0):
    return {
        "strike": strike,
        "option_type": otype,
        "open_interest": oi,
        "implied_volatility": iv,
        "expiration": expiration,
        "gamma": gamma,
        "volume": volume,
    }


def _engine():
    return AnalyticsEngine(underlying="SPY")


def _ts_and_exp():
    # ~2 DTE so vanna/charm are dimensionally non-trivial for the
    # sanity-check case where IV is valid.
    ts = datetime(2026, 5, 20, 15, 30, tzinfo=timezone.utc)
    exp = datetime(2026, 5, 22).date()
    return ts, exp


def test_missing_iv_contributes_no_vanna_or_charm():
    ts, exp = _ts_and_exp()
    options = [_opt(strike=500.0, otype="C", iv=None, expiration=exp)]

    [row] = _engine()._calculate_gex_by_strike(options, underlying_price=500.0, timestamp=ts)

    assert row["call_vanna_exposure"] == 0.0
    assert row["call_charm_exposure"] == 0.0
    assert row["vanna_exposure"] == 0.0
    assert row["charm_exposure"] == 0.0
    assert row["dealer_vanna_exposure"] == 0.0
    assert row["dealer_charm_exposure"] == 0.0


def test_zero_iv_is_also_skipped():
    ts, exp = _ts_and_exp()
    options = [_opt(strike=500.0, otype="P", iv=0.0, expiration=exp)]

    [row] = _engine()._calculate_gex_by_strike(options, underlying_price=500.0, timestamp=ts)

    assert row["put_vanna_exposure"] == 0.0
    assert row["put_charm_exposure"] == 0.0


def test_valid_iv_still_contributes():
    """Sanity: a real IV still produces non-zero vanna/charm — the new
    guard must not over-skip rows whose IV is genuinely populated."""
    ts, exp = _ts_and_exp()
    options = [_opt(strike=500.0, otype="C", iv=0.20, expiration=exp)]

    [row] = _engine()._calculate_gex_by_strike(options, underlying_price=500.0, timestamp=ts)

    # Vanna and charm for a short-dated ATM call are both non-zero; at
    # least one must be, or the guard has over-skipped.
    assert row["call_vanna_exposure"] != 0.0 or row["call_charm_exposure"] != 0.0


def test_mixed_chain_only_valid_iv_contributes():
    """Mixed chain: one valid-IV row and one None-IV row at the same
    strike. The aggregate exposure must equal the valid-row contribution
    exactly — no phantom add-on from the None row."""
    ts, exp = _ts_and_exp()
    valid = [_opt(strike=500.0, otype="C", iv=0.20, expiration=exp)]
    polluted = valid + [_opt(strike=500.0, otype="C", iv=None, expiration=exp)]
    engine = _engine()

    [row_valid] = engine._calculate_gex_by_strike(valid, underlying_price=500.0, timestamp=ts)
    [row_polluted] = engine._calculate_gex_by_strike(polluted, underlying_price=500.0, timestamp=ts)

    assert row_valid["call_vanna_exposure"] == row_polluted["call_vanna_exposure"]
    assert row_valid["call_charm_exposure"] == row_polluted["call_charm_exposure"]
