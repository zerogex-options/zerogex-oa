"""The online classifier now drives trades (commit 3).

Every bot trained a per-bot logistic-regression classifier one SGD step per
closed trade, but its weights were never read back — only the aggregate
scalars (confidence_base, size_multiplier, threshold) fed decisions, so the
learned model was trained and shelved. These tests lock the wiring that
makes it actually influence entries:

* ``predict_win_prob`` is the exact inference counterpart of
  ``online_update`` (same feature vector), and returns ``None`` for an
  untrained model so the overlay is a strict no-op at cold start;
* ``compute_conviction`` applies a bounded, gated nudge toward P(win);
* end to end, a trained classifier can flip a marginal Bouncer setup from
  rejected to fired (and, with adverse weights, the reverse).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.ml import _sigmoid, predict_win_prob
from src.tradeworkz.models import BotSpec

TS = datetime(2026, 7, 14, 15, 30, tzinfo=timezone.utc)  # 11:30 ET, mins_since_open=120


# ----------------------------------------------------------------------
# predict_win_prob
# ----------------------------------------------------------------------


def test_predict_none_for_untrained_models():
    assert predict_win_prob(None, {}) is None
    assert predict_win_prob({}, {}) is None
    # All-zero weight vector is the cold-start state -> treat as untrained.
    assert predict_win_prob({"bias": 0.0, "net_gex_norm": 0.0}, {"net_gex": 1e9}) is None


def test_predict_scores_the_same_vector_training_fits():
    # net_gex_norm = 2e9 / 2e9 = 1.0; bias = 1.0; others 0.
    weights = {"net_gex_norm": 1.0, "bias": -0.5}
    p = predict_win_prob(weights, {"net_gex": 2.0e9})
    assert p == pytest.approx(_sigmoid(1.0 * 1.0 + (-0.5) * 1.0))


def test_predict_accepts_json_string_weights():
    p = predict_win_prob(json.dumps({"bias": 0.5}), {})
    assert p == pytest.approx(_sigmoid(0.5))


def test_predict_clamps_extreme_features_like_training():
    # net_gex 100e9 / 2e9 = 50 -> clamped to 3.0 by _extract_features.
    p = predict_win_prob({"net_gex_norm": 1.0}, {"net_gex": 100.0e9})
    assert p == pytest.approx(_sigmoid(3.0))


# ----------------------------------------------------------------------
# compute_conviction overlay — gating + bounded nudge
# ----------------------------------------------------------------------


def _base_bot(ml_state: Optional[Dict[str, Any]] = None, **params) -> BaseBot:
    spec = BotSpec(
        id="b",
        display_name="B",
        strategy_class="BaseBot",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params=dict(params),
    )
    return BaseBot(spec, ml_state=ml_state)


def _snap() -> MarketSnapshot:
    return MarketSnapshot(underlying="SPY", timestamp=TS, spot=755.0, net_gex=1.0e9, vix=16.0)


# Provisional blend for base=0.55 (default), quality=0.5: 0.4*0.55 + 0.6*0.5 = 0.52.
PROVISIONAL = 0.52
TRAINED = {"n_samples": 50, "weights": {"bias": 3.0}}  # P(win)=sigmoid(3)=0.9526


def test_overlay_noop_without_components():
    conv = _base_bot(TRAINED).compute_conviction(_snap(), 0.5)  # no components
    assert conv == pytest.approx(PROVISIONAL)


def test_overlay_noop_below_min_samples():
    state = {"n_samples": 5, "weights": {"bias": 3.0}}
    conv = _base_bot(state).compute_conviction(_snap(), 0.5, components={"net_gex": 1e9})
    assert conv == pytest.approx(PROVISIONAL)


def test_overlay_noop_when_untrained_weights():
    state = {"n_samples": 200, "weights": {}}  # lots of samples, no weights
    conv = _base_bot(state).compute_conviction(_snap(), 0.5, components={"net_gex": 1e9})
    assert conv == pytest.approx(PROVISIONAL)


def test_overlay_pulls_toward_high_p_win():
    conv = _base_bot(TRAINED).compute_conviction(_snap(), 0.5, components={"net_gex": 1e9})
    expected = 0.75 * PROVISIONAL + 0.25 * _sigmoid(3.0)
    assert conv == pytest.approx(expected)
    assert conv > PROVISIONAL  # classifier is bullish on the setup


def test_overlay_pulls_toward_low_p_win():
    state = {"n_samples": 50, "weights": {"bias": -3.0}}  # P(win)=sigmoid(-3)=0.047
    conv = _base_bot(state).compute_conviction(_snap(), 0.5, components={"net_gex": 1e9})
    assert conv < PROVISIONAL  # classifier vetoes the setup


def test_overlay_weight_zero_disables_nudge():
    conv = _base_bot(TRAINED, ml_conviction_weight=0.0).compute_conviction(
        _snap(), 0.5, components={"net_gex": 1e9}
    )
    assert conv == pytest.approx(PROVISIONAL)


def test_overlay_stays_in_unit_interval():
    state = {"n_samples": 50, "weights": {"bias": 50.0}}  # saturates sigmoid -> ~1.0
    conv = _base_bot(state, ml_conviction_weight=1.0).compute_conviction(
        _snap(), 1.0, components={"net_gex": 1e9}
    )
    assert 0.0 <= conv <= 1.0


# ----------------------------------------------------------------------
# End to end: the classifier flips a marginal Bouncer decision
# ----------------------------------------------------------------------


def _bouncer(ml_state: Optional[Dict[str, Any]] = None) -> PutCallWallBouncer:
    spec = BotSpec(
        id="put_call_wall_bouncer",
        display_name="Put/Call Wall Bouncer",
        strategy_class="PutCallWallBouncer",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params={"wall_proximity_pct": 0.002, "max_hold_minutes": 90},
    )
    return PutCallWallBouncer(spec, ml_state=ml_state)


def _marginal_snap() -> MarketSnapshot:
    # net_gex 1e9 -> gex_score 0.333 -> base conviction ~0.53, just under the
    # 0.55 threshold: rejected without help, fired if the classifier lifts it.
    return MarketSnapshot(
        underlying="SPY",
        timestamp=TS,
        spot=755.0,
        call_wall=755.0,
        put_wall=750.0,
        max_pain=752.0,
        net_gex=1.0e9,
    )


def test_untrained_bouncer_rejects_marginal_setup():
    assert _bouncer(ml_state=None).open_criteria(_marginal_snap()) is None


def test_trained_classifier_lifts_marginal_setup_over_the_bar():
    """A confident classifier turns the same rejected setup into a fired trade."""
    sig = _bouncer(ml_state={"n_samples": 50, "weights": {"bias": 3.0}}).open_criteria(
        _marginal_snap()
    )
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.conviction >= 0.55


def test_adverse_classifier_keeps_a_marginal_setup_rejected():
    sig = _bouncer(ml_state={"n_samples": 50, "weights": {"bias": -3.0}}).open_criteria(
        _marginal_snap()
    )
    assert sig is None
