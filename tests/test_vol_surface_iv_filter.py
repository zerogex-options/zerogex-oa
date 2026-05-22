"""Vol-surface IV outlier + per-expiration coverage filter.

The pre-fix endpoint surfaced 0DTE-after-close "ghost" expirations whose
IV solver had blown up on stale closing marks: most strikes carried
NULL IVs and the surviving handful sat at 1.5–3.7 (150-370%) — values
that have no bearing on the live vol surface but silently corrupted
the ATM-IV interpolation (one canonical case produced an ATM IV of
2.52 for SPY at DTE=0).  Post-fix, ``_iv_or_null`` rejects IVs above
``VOL_SURFACE_IV_MAX`` and the per-expiration loop drops slices whose
post-filter strike coverage falls below
``VOL_SURFACE_MIN_STRIKE_COVERAGE`` so the row never reaches the
response.
"""

import os

# Set env before importing the module (constants are read at import time).
os.environ.setdefault("VOL_SURFACE_IV_MAX", "2.0")
os.environ.setdefault("VOL_SURFACE_MIN_STRIKE_COVERAGE", "0.30")

from src.api.routers.vol_surface import (  # noqa: E402
    VOL_SURFACE_IV_MAX,
    VOL_SURFACE_MIN_STRIKE_COVERAGE,
    _compute_25d_skew,
    _iv_or_null,
)


def test_iv_or_null_rejects_outliers_above_cap():
    # Real values pass through unchanged.
    assert _iv_or_null({"implied_volatility": 0.20}) == 0.20
    # IV solver clamp + plateau values are stripped.
    assert _iv_or_null({"implied_volatility": VOL_SURFACE_IV_MAX + 0.01}) is None
    assert _iv_or_null({"implied_volatility": 3.74}) is None  # canonical artefact
    # None / 0 / negative are still filtered (pre-existing contract).
    assert _iv_or_null({"implied_volatility": None}) is None
    assert _iv_or_null({"implied_volatility": 0}) is None
    assert _iv_or_null({"implied_volatility": -0.5}) is None


def test_25d_skew_ignores_outlier_ivs():
    # Each side gets one realistic strike and one outlier; the outlier
    # is closer to the 0.25 target so a naive implementation would pick
    # it. The filter must reject the outlier so the realistic strike
    # wins on each side.
    rows = [
        {"option_type": "C", "delta": 0.26, "implied_volatility": 3.5, "open_interest": 1, "strike": 750},
        {"option_type": "C", "delta": 0.20, "implied_volatility": 0.15, "open_interest": 1, "strike": 745},
        {"option_type": "P", "delta": -0.26, "implied_volatility": 4.2, "open_interest": 1, "strike": 730},
        {"option_type": "P", "delta": -0.20, "implied_volatility": 0.20, "open_interest": 1, "strike": 735},
    ]
    skew = _compute_25d_skew(rows)
    # Post-filter the only usable pair is put 0.20 - call 0.15 = 0.05.
    assert abs(skew - 0.05) < 1e-6


def test_coverage_floor_threshold_is_configured():
    # Sanity: defaults are loaded.  Used as documentation of the
    # configured policy more than as a behavioural test.
    assert VOL_SURFACE_IV_MAX > 0
    assert 0 < VOL_SURFACE_MIN_STRIKE_COVERAGE <= 1
