"""Tests for the counterfactual tuner (src/jobs/cone_tune.py).

The tuner's whole claim is that it can rebuild a committed claim exactly and
then evaluate a DIFFERENT band against what the tape actually did. If the
reconstruction drifts from compute_cone even slightly, every number it prints
is a fit to a model that was never deployed — so the reconstruction fidelity
is the first thing asserted here, against claims produced by compute_cone
itself rather than against hand-written fixtures.
"""

from __future__ import annotations

import pytest

from src.jobs.cone_tune import _rebuild, _score, _summary
from src.jobs.intraday_cone_model import (
    CONE_SIGMA_MULT,
    CONE_TERM_DECAY,
    ConeInputs,
    compute_cone,
)


def _claims_from(inp: ConeInputs, window=None) -> list[dict]:
    """Turn a real compute_cone result into stored-claim shape."""
    r = compute_cone(inp)
    ref = min(h.horizon_min for h in r.horizons)
    out = []
    for h in r.horizons:
        out.append({
            "symbol": inp.symbol, "session_date": "2026-09-18",
            "forecast_ts": "t", "horizon_min": h.horizon_min,
            "anchor_spot": inp.spot, "daily_sigma": r.daily_sigma,
            "elapsed_min": inp.elapsed_min, "gamma_mult": r.gamma_mult,
            "call_wall": inp.call_wall, "put_wall": inp.put_wall,
            "band_low": h.band_low, "band_high": h.band_high,
            "hold_prob": h.hold_prob, "_ref_horizon": ref,
            "window_low": window[0] if window else inp.spot - 0.1,
            "window_high": window[1] if window else inp.spot + 0.1,
            "held": True,
        })
    return out


def _inputs(**kw) -> ConeInputs:
    base = dict(symbol="SPY", spot=600.0, elapsed_min=90, implied_move=6.0,
                trailing_vol_ratios=[0.81] * 6, vol_basis_mult=0.8,
                session_high=602.0, session_low=598.5,
                call_wall=606.0, put_wall=594.0, net_gex_at_spot=4.0e8)
    base.update(kw)
    return ConeInputs(**base)


@pytest.mark.parametrize("elapsed", [15, 90, 210, 330, 360])
def test_rebuild_reproduces_the_published_band_exactly(elapsed):
    """At the committed parameters the tuner must land on the same band and
    the same probability compute_cone published — otherwise it is tuning a
    model that was never deployed."""
    claims = _claims_from(_inputs(elapsed_min=elapsed))
    assert claims, "the fixture must publish at least one horizon"
    for c in claims:
        lo, hi, p = _rebuild(c, CONE_SIGMA_MULT, CONE_TERM_DECAY, 0.5)
        assert lo == pytest.approx(float(c["band_low"]), abs=1e-4)
        assert hi == pytest.approx(float(c["band_high"]), abs=1e-4)
        assert p == pytest.approx(float(c["hold_prob"]), abs=1e-6)


def test_rebuild_reproduces_a_wall_clamped_band():
    """The wall lean is where a reimplementation is most likely to drift."""
    claims = _claims_from(_inputs(call_wall=601.0, put_wall=599.0))
    for c in claims:
        lo, hi, _ = _rebuild(c, CONE_SIGMA_MULT, CONE_TERM_DECAY, 0.5)
        assert lo == pytest.approx(float(c["band_low"]), abs=1e-4)
        assert hi == pytest.approx(float(c["band_high"]), abs=1e-4)


def test_a_wider_band_changes_the_realized_outcome_too():
    """The insight the tuner exists to encode.

    Band geometry moves predicted AND realized together, because the band is
    the thing being graded. A tuner that held the realized rate fixed would
    repeat the mistake that made the v1_4 term-decay fit miss its projection.
    """
    # A window that sits outside the tight band and inside the wide one.
    claims = _claims_from(_inputs(), window=(597.0, 603.0))
    tight = _score(claims, 1.0, CONE_TERM_DECAY, 0.5)
    wide = _score(claims, 3.0, CONE_TERM_DECAY, 0.5)
    assert sum(r["real"] for r in tight) < sum(r["real"] for r in wide), (
        "widening the band must change the graded outcome, not just the odds"
    )


def test_the_path_exponent_moves_only_the_prediction():
    """The other half: the probability knob must leave the graded outcome
    alone, which is what makes it able to close a calibration gap at all."""
    claims = _claims_from(_inputs(), window=(597.0, 603.0))
    brownian = _score(claims, CONE_SIGMA_MULT, CONE_TERM_DECAY, 0.50)
    contained = _score(claims, CONE_SIGMA_MULT, CONE_TERM_DECAY, 0.70)
    for a, b in zip(brownian, contained):
        assert a["real"] == pytest.approx(b["real"]), "outcomes must not move"
        # Note the direction: the variance fraction is below 1, so a LARGER
        # exponent means a SMALLER sub-daily sigma and more predicted holding.
        # Getting this backwards is easy and this assertion is why it was
        # caught before it reached a sweep.
        assert b["pred"] > a["pred"], "less sub-daily motion must predict more holding"


def test_summary_weights_by_sample_size():
    rows = [{"n": 100, "gap": 0.01, "brier": 0.10},
            {"n": 1, "gap": 0.90, "brier": 0.90}]
    gap, brier = _summary(rows)
    assert gap < 0.03 and brier < 0.12


def test_summary_of_nothing_is_not_a_zero():
    gap, brier = _summary([])
    assert gap == float("inf") and brier == float("inf")


def test_rebuild_declines_a_claim_missing_its_inputs():
    for missing in ("anchor_spot", "daily_sigma", "elapsed_min", "gamma_mult"):
        c = _claims_from(_inputs())[0]
        c[missing] = None
        assert _rebuild(c, CONE_SIGMA_MULT, CONE_TERM_DECAY, 0.5) is None
