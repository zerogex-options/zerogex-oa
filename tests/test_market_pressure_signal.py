"""Tests for the Market Pressure Index advanced signal."""

from __future__ import annotations

from datetime import datetime, timezone

# Import order matters: load ``components.base`` first so the
# ``components/__init__.py`` cascade (which pulls in basic ⇒ advanced.base)
# completes before this module asks for ``advanced.engine``. Otherwise
# the basic→advanced.base back-edge hits ``advanced.base`` mid-load and
# the import errors out. Same pattern as ``test_independent_signals.py``.
from src.signals.components.base import MarketContext  # noqa: I001
from src.signals.advanced.engine import AdvancedSignalEngine
from src.signals.advanced.market_pressure import MarketPressureSignal

# Mid-session ET (≈11:00 ET in EDT) so alpha-vanna and charm-amp are
# not at endpoints — gives the most informative diagnostics.
NOW = datetime(2026, 4, 24, 15, 0, tzinfo=timezone.utc)


def _ctx(**overrides) -> MarketContext:
    overrides = dict(overrides)
    base_extra = {
        "call_flow_delta": 0.0,
        "put_flow_delta": 0.0,
    }
    base_extra.update(overrides.pop("extra", {}))

    default_closes = [
        600.0,
        600.4,
        599.6,
        600.2,
        599.8,
        600.1,
        599.9,
        600.3,
        599.7,
        600.0,
        600.2,
        599.8,
        600.1,
        599.9,
        600.4,
        599.6,
        600.2,
        599.8,
        600.1,
        600.0,
    ]

    payload = dict(
        timestamp=NOW,
        underlying="SPY",
        close=600.0,
        net_gex=-100_000_000.0,
        gamma_flip=598.0,
        put_call_ratio=1.0,
        max_pain=600.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=default_closes,
        iv_rank=0.4,
        vwap=600.0,
        extra=base_extra,
    )
    payload.update(overrides)
    return MarketContext(**payload)


def _gex_rows_with_greeks(
    *,
    vanna: float = 0.0,
    charm: float = 0.0,
    call_oi: int = 0,
    put_oi: int = 0,
) -> list[dict]:
    """Build a minimal ``gex_by_strike`` payload that exercises the
    dealer-vanna / dealer-charm / dealer-net-delta paths."""
    return [
        {
            "strike": 600.0,
            "dealer_vanna_exposure": vanna,
            "dealer_charm_exposure": charm,
            "call_oi": call_oi,
            "put_oi": put_oi,
        }
    ]


# ---------------------------------------------------------------------------
# Engine registration & baseline
# ---------------------------------------------------------------------------


def test_registered_in_advanced_engine():
    engine = AdvancedSignalEngine()
    results = {r.name: r for r in engine.evaluate(_ctx())}
    assert "market_pressure" in results


def test_neutral_inputs_are_discharged():
    signal = MarketPressureSignal()
    result = signal.evaluate(_ctx())
    # No vanna/charm, no flow, walls absent ⇒ at most compression from
    # the flip alone (which is 2pts away), and tension floor.
    assert result.context["label"] in ("Discharged", "Building")
    assert result.context["triggered"] is False
    assert abs(result.score) < 0.3


def test_missing_optional_inputs_do_not_raise():
    signal = MarketPressureSignal()
    result = signal.evaluate(_ctx(extra={}))
    # Hedging and flow should fall back gracefully.
    assert result.context["hedging"]["magnitude"] == 0.0
    assert result.context["flow"]["magnitude"] == 0.0
    assert result.score == 0.0


# ---------------------------------------------------------------------------
# Compression sub-component
# ---------------------------------------------------------------------------


def test_compression_high_when_walls_tight_and_flip_at_spot():
    signal = MarketPressureSignal()
    ctx = _ctx(
        close=600.0,
        gamma_flip=600.0,
        extra={"call_wall": 600.5, "put_wall": 599.5},
    )
    comp = signal._compression(ctx)
    assert comp["wall_pinch"] is not None and comp["wall_pinch"] > 0.9
    assert comp["flip_proximity"] == 1.0
    assert comp["magnitude"] > 0.5


def test_compression_zero_when_walls_far_apart():
    signal = MarketPressureSignal()
    ctx = _ctx(
        close=600.0,
        gamma_flip=600.0,
        extra={"call_wall": 615.0, "put_wall": 585.0},  # 5% spread
    )
    comp = signal._compression(ctx)
    # Wall pinch saturates to 0 — geometric mean with flip_prox=1.0 is 0.
    assert comp["wall_pinch"] == 0.0
    assert comp["magnitude"] == 0.0


def test_compression_uses_flip_alone_when_walls_missing():
    signal = MarketPressureSignal()
    ctx = _ctx(close=600.0, gamma_flip=600.0, extra={})
    comp = signal._compression(ctx)
    assert comp["wall_pinch"] is None
    assert comp["flip_proximity"] == 1.0
    assert comp["magnitude"] > 0.5


def test_compression_regime_multiplier_amplifies_in_short_gamma():
    """Identical structural compression should produce a larger magnitude
    when net GEX is strongly negative (short-gamma regime)."""
    signal = MarketPressureSignal()
    base_extra = {"call_wall": 600.5, "put_wall": 599.5}
    short_gamma = _ctx(
        close=600.0,
        gamma_flip=600.0,
        net_gex=-5.0e9,
        extra=base_extra,
    )
    long_gamma = _ctx(
        close=600.0,
        gamma_flip=600.0,
        net_gex=5.0e9,
        extra=base_extra,
    )
    short_mag = signal._compression(short_gamma)["magnitude"]
    long_mag = signal._compression(long_gamma)["magnitude"]
    assert short_mag > long_mag


# ---------------------------------------------------------------------------
# Hedging vector sub-component
# ---------------------------------------------------------------------------


def test_hedging_bullish_when_vanna_and_charm_positive():
    signal = MarketPressureSignal()
    ctx = _ctx(
        dealer_net_delta=-2.5e8,  # dealers short ⇒ active hedging
        extra={"gex_by_strike": _gex_rows_with_greeks(vanna=1.2e8, charm=8.0e9)},
    )
    hedging = signal._hedging_vector(ctx)
    assert hedging["signed"] > 0
    assert hedging["magnitude"] > 0.3
    assert hedging["source"] == "dealer_exposure"


def test_hedging_bearish_when_vanna_and_charm_negative():
    signal = MarketPressureSignal()
    ctx = _ctx(
        dealer_net_delta=2.5e8,
        extra={"gex_by_strike": _gex_rows_with_greeks(vanna=-1.2e8, charm=-8.0e9)},
    )
    hedging = signal._hedging_vector(ctx)
    assert hedging["signed"] < 0
    assert hedging["magnitude"] > 0.3


def test_hedging_alignment_bonus_when_both_agree():
    """Aligned (same-sign) vanna/charm should exceed the disagree case
    at the same absolute scales."""
    signal = MarketPressureSignal()
    aligned = _ctx(
        dealer_net_delta=-3.0e8,
        extra={"gex_by_strike": _gex_rows_with_greeks(vanna=1.0e8, charm=5.0e9)},
    )
    crossed = _ctx(
        dealer_net_delta=-3.0e8,
        extra={"gex_by_strike": _gex_rows_with_greeks(vanna=1.0e8, charm=-5.0e9)},
    )
    aligned_mag = signal._hedging_vector(aligned)["magnitude"]
    crossed_mag = signal._hedging_vector(crossed)["magnitude"]
    assert aligned_mag > crossed_mag


def test_hedging_dealer_gate_mutes_when_dni_is_flat():
    """Same vanna/charm but DNI≈0 should yield smaller hedging magnitude
    than when dealers carry significant inventory."""
    signal = MarketPressureSignal()
    flat = _ctx(
        dealer_net_delta=0.0,
        extra={"gex_by_strike": _gex_rows_with_greeks(vanna=1.0e8, charm=5.0e9)},
    )
    loaded = _ctx(
        dealer_net_delta=-3.0e8,
        extra={"gex_by_strike": _gex_rows_with_greeks(vanna=1.0e8, charm=5.0e9)},
    )
    flat_mag = signal._hedging_vector(flat)["magnitude"]
    loaded_mag = signal._hedging_vector(loaded)["magnitude"]
    assert loaded_mag > flat_mag


# ---------------------------------------------------------------------------
# Flow asymmetry sub-component
# ---------------------------------------------------------------------------


def test_flow_signed_tracks_premium_skew():
    signal = MarketPressureSignal()
    bullish = _ctx(extra={"call_flow_delta": 300_000.0, "put_flow_delta": -100_000.0})
    bearish = _ctx(extra={"call_flow_delta": -100_000.0, "put_flow_delta": 300_000.0})
    assert signal._flow_asymmetry(bullish)["signed"] > 0
    assert signal._flow_asymmetry(bearish)["signed"] < 0


def test_flow_magnitude_gates_on_total_premium():
    signal = MarketPressureSignal()
    anemic = _ctx(extra={"call_flow_delta": 100.0, "put_flow_delta": -100.0})
    chunky = _ctx(extra={"call_flow_delta": 500_000.0, "put_flow_delta": -200_000.0})
    assert signal._flow_asymmetry(anemic)["magnitude"] < 0.05
    assert signal._flow_asymmetry(chunky)["magnitude"] > 0.5


def test_flow_blends_smart_money_with_premium_skew():
    signal = MarketPressureSignal()
    # Premium skew is neutral, smart-money tilts bullish.
    ctx = _ctx(
        smart_call=800_000.0,
        smart_put=200_000.0,
        extra={"call_flow_delta": 100_000.0, "put_flow_delta": 100_000.0},
    )
    flow = signal._flow_asymmetry(ctx)
    assert flow["signed"] > 0  # smart-money component pulls bullish


# ---------------------------------------------------------------------------
# Vol tension sub-component
# ---------------------------------------------------------------------------


def test_vol_tension_high_when_iv_cheap_and_realized_squeezed():
    signal = MarketPressureSignal()
    # Last 10 bars are tight; earlier 50 are noisy ⇒ realized squeeze.
    noisy = [600.0 + (1.5 if i % 2 else -1.5) for i in range(50)]
    quiet = [600.0 + (0.01 if i % 2 else -0.01) for i in range(10)]
    ctx = _ctx(close=quiet[-1], recent_closes=noisy + quiet, iv_rank=0.1)
    tension = signal._vol_tension(ctx)
    assert tension["vol_squeeze"] > 0.5
    assert tension["iv_cheapness"] is not None and tension["iv_cheapness"] > 0.8
    assert tension["magnitude"] > 0.5


def test_vol_tension_degrades_without_iv_rank():
    signal = MarketPressureSignal()
    noisy = [600.0 + (1.5 if i % 2 else -1.5) for i in range(50)]
    quiet = [600.0 + (0.01 if i % 2 else -0.01) for i in range(10)]
    ctx = _ctx(close=quiet[-1], recent_closes=noisy + quiet, iv_rank=None)
    tension = signal._vol_tension(ctx)
    # Without IV rank, magnitude is halved vs squeeze alone.
    assert tension["iv_cheapness"] is None
    assert tension["magnitude"] <= tension["vol_squeeze"] * 0.5 + 1e-9


# ---------------------------------------------------------------------------
# Full fusion — directional loaded reads
# ---------------------------------------------------------------------------


def test_full_bullish_loaded_aligned_inputs():
    """All four pillars line up bullish: tight walls + flip at spot,
    positive vanna+charm, call-flow heavy, dealers short, vol cheap+squeezed."""
    closes = [600.0 + (1.0 if i % 2 else -1.0) for i in range(50)] + [
        600.0 + (0.02 if i % 2 else -0.02) for i in range(10)
    ]
    ctx = _ctx(
        close=600.0,
        gamma_flip=600.0,
        net_gex=-2.0e9,
        recent_closes=closes,
        iv_rank=0.15,
        smart_call=600_000.0,
        smart_put=100_000.0,
        dealer_net_delta=-3.0e8,
        extra={
            "call_wall": 600.5,
            "put_wall": 599.5,
            "call_flow_delta": 400_000.0,
            "put_flow_delta": -150_000.0,
            "gex_by_strike": _gex_rows_with_greeks(vanna=1.2e8, charm=9.0e9),
        },
    )
    result = MarketPressureSignal().evaluate(ctx)
    ctx_out = result.context
    assert result.score > 0.3, ctx_out
    assert ctx_out["loading"] >= 50.0, ctx_out
    assert ctx_out["triggered"] is True
    assert ctx_out["direction_sign"] == "bullish"
    assert ctx_out["label"] in ("Loaded", "Critical")
    assert ctx_out["signal"] == "bullish_pressure"


def test_full_bearish_loaded_aligned_inputs():
    closes = [600.0 + (1.0 if i % 2 else -1.0) for i in range(50)] + [
        600.0 + (0.02 if i % 2 else -0.02) for i in range(10)
    ]
    ctx = _ctx(
        close=600.0,
        gamma_flip=600.0,
        net_gex=-2.0e9,
        recent_closes=closes,
        iv_rank=0.15,
        smart_call=100_000.0,
        smart_put=600_000.0,
        dealer_net_delta=3.0e8,
        extra={
            "call_wall": 600.5,
            "put_wall": 599.5,
            "call_flow_delta": -150_000.0,
            "put_flow_delta": 400_000.0,
            "gex_by_strike": _gex_rows_with_greeks(vanna=-1.2e8, charm=-9.0e9),
        },
    )
    result = MarketPressureSignal().evaluate(ctx)
    ctx_out = result.context
    assert result.score < -0.3, ctx_out
    assert ctx_out["loading"] >= 50.0, ctx_out
    assert ctx_out["triggered"] is True
    assert ctx_out["direction_sign"] == "bearish"
    assert ctx_out["label"] in ("Loaded", "Critical")
    assert ctx_out["signal"] == "bearish_pressure"


def test_confidence_penalizes_disagreeing_inputs():
    """Hedging bullish, flow bearish, dealers bullish ⇒ confidence_mult < 1
    relative to the all-aligned case."""
    closes = [600.0 + (1.0 if i % 2 else -1.0) for i in range(60)]
    aligned = _ctx(
        close=600.0,
        gamma_flip=600.0,
        recent_closes=closes,
        iv_rank=0.15,
        dealer_net_delta=-2.5e8,
        extra={
            "call_wall": 600.5,
            "put_wall": 599.5,
            "call_flow_delta": 400_000.0,
            "put_flow_delta": -150_000.0,
            "gex_by_strike": _gex_rows_with_greeks(vanna=1.0e8, charm=6.0e9),
        },
    )
    fighting = _ctx(
        close=600.0,
        gamma_flip=600.0,
        recent_closes=closes,
        iv_rank=0.15,
        dealer_net_delta=-2.5e8,
        extra={
            "call_wall": 600.5,
            "put_wall": 599.5,
            # Flow flipped bearish while hedging/dealer still bullish.
            "call_flow_delta": -400_000.0,
            "put_flow_delta": 150_000.0,
            "gex_by_strike": _gex_rows_with_greeks(vanna=1.0e8, charm=6.0e9),
        },
    )
    aligned_res = MarketPressureSignal().evaluate(aligned)
    fighting_res = MarketPressureSignal().evaluate(fighting)
    assert aligned_res.context["confidence_mult"] > fighting_res.context["confidence_mult"]
    # And the directional score is attenuated when inputs fight.
    assert abs(fighting_res.score) < aligned_res.score


# ---------------------------------------------------------------------------
# Label & trigger boundaries
# ---------------------------------------------------------------------------


def test_label_progression_with_loading():
    signal = MarketPressureSignal()
    # Tiny: discharged
    discharged_label, _ = signal._label_and_playbook(10.0, 0.0)
    assert discharged_label == "Discharged"
    # Mid-range: building
    building_label, _ = signal._label_and_playbook(35.0, 0.3)
    assert building_label == "Building"
    # Loaded: 50-75
    loaded_label, loaded_play = signal._label_and_playbook(65.0, 0.4)
    assert loaded_label == "Loaded"
    assert "upside" in loaded_play
    # Critical: 75+, bearish
    crit_label, crit_play = signal._label_and_playbook(85.0, -0.6)
    assert crit_label == "Critical"
    assert "downside" in crit_play


def test_triggered_requires_both_loading_and_direction():
    signal = MarketPressureSignal()
    # Lots of loading but no direction (neutral pivot) → not triggered.
    closes = [600.0 + (1.0 if i % 2 else -1.0) for i in range(50)] + [
        600.0 + (0.02 if i % 2 else -0.02) for i in range(10)
    ]
    ctx = _ctx(
        close=600.0,
        gamma_flip=600.0,
        recent_closes=closes,
        iv_rank=0.15,
        dealer_net_delta=0.0,
        extra={
            "call_wall": 600.5,
            "put_wall": 599.5,
            # Symmetric flow → flow magnitude high, signed ≈ 0.
            "call_flow_delta": 300_000.0,
            "put_flow_delta": -300_000.0,
            # Vanna/charm nearly cancel → hedging signed ≈ 0.
            "gex_by_strike": _gex_rows_with_greeks(vanna=1.0e8, charm=-6.0e9),
        },
    )
    result = signal.evaluate(ctx)
    # The direction should be small enough to not trigger.
    assert abs(result.context["direction"]) < 0.4
