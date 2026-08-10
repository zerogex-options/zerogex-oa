"""Entry-logic tests for the v4 edge-metric candidate bots.

These bots trade the data layers the retired fleet never used — aggressor
order flow, second-order forced dealer flow (vanna/charm), the modeled
close-charm flow, and the gamma-restoring Pin Strike. The tests build a
:class:`MarketSnapshot` directly (no DB) and lock in, for each bot:

* it FIRES with the right direction / structure / target on a clean setup;
* the *edge filter* that distinguishes it from the folklore version actually
  gates it (charm flow must point at the pin; vanna×ΔVIX sign; a flow
  divergence that price hasn't closed; a genuine long→short gamma transition);
* it stands down when its regime / data preconditions are absent.

Plus a registry invariant: the candidates are registered and runnable but NOT
live (empty DEFAULT_ROSTER, not retired) — they promote only via the backtest.

All timestamps are ET→UTC for July (EDT, UTC-4).
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.registry import (
    CANDIDATE_SPECS,
    DEFAULT_ROSTER,
    RETIRED_BOT_IDS,
    get_bot_class,
)

# ET wall-clock instants used below (July = EDT = UTC-4).
LATE_15ET = datetime(2026, 7, 14, 19, 0, tzinfo=timezone.utc)  # 15:00 ET, 60m to close
MID_12ET = datetime(2026, 7, 14, 16, 0, tzinfo=timezone.utc)  # 12:00 ET
LATE_MORN_11ET = datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc)  # 11:00 ET


def _bot(bot_id: str):
    spec = next(s for s in CANDIDATE_SPECS if s.id == bot_id)
    return get_bot_class(spec.strategy_class)(spec, ml_state=None)


def _rising(spot: float, pct: float = 0.0013, n: int = 6) -> list[float]:
    """n one-minute closes rising to ``spot`` by ``pct`` over the window."""
    start = spot / (1.0 + pct)
    return [round(start + (spot - start) * i / (n - 1), 4) for i in range(n)]


def _flat(spot: float, n: int = 6) -> list[float]:
    return [spot] * n


# ======================================================================
# CharmCloseMagnet
# ======================================================================


def _charm_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=LATE_15ET,
        spot=755.0,
        net_gex=3.0e9,  # positive_strong (absolute fallback)
        pin_strike=757.0,  # +0.26% above spot
        pin_confidence=0.85,
        pin_score=0.9,
        close_charm_flow=6.0e8,  # > 0 => forced BUY, agrees with pin above
        recent_closes=_flat(755.0),
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_charm_close_magnet_fires_toward_pin_when_flow_agrees():
    sig = _bot("charm_close_magnet").open_criteria(_charm_snap())
    assert sig is not None
    assert sig.direction == "bullish"
    assert "CALL" in sig.strategy_type
    assert sig.legs[0].option_type == "call"
    assert len(sig.legs) == 2  # defined-risk vertical
    assert sig.target_price == 757.0  # the pin


def test_charm_close_magnet_abstains_when_flow_opposes_pin():
    """The whole edge: if forced charm flow does NOT point at the pin, stand
    down — this is the setup the retired EodPinDrifter lost money on."""
    assert _bot("charm_close_magnet").open_criteria(_charm_snap(close_charm_flow=-6.0e8)) is None


def test_charm_close_magnet_requires_positive_gamma():
    assert _bot("charm_close_magnet").open_criteria(_charm_snap(net_gex=-3.0e9)) is None


def test_charm_close_magnet_falls_back_to_max_pain_when_no_pin():
    """Pin Strike is rarely persisted, so the magnet falls back to max_pain
    (same side as the flow). The bot must still fire on the fallback."""
    sig = _bot("charm_close_magnet").open_criteria(
        _charm_snap(pin_strike=None, pin_confidence=None, pin_score=None, max_pain=757.0)
    )
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.target_price == 757.0  # max_pain magnet


def test_charm_close_magnet_needs_some_magnet():
    """With neither a pin nor max_pain there is nothing to drift toward."""
    snap = _charm_snap(pin_strike=None, pin_confidence=None, pin_score=None, max_pain=None)
    assert _bot("charm_close_magnet").open_criteria(snap) is None


def test_charm_close_magnet_only_in_final_window():
    # 11:00 ET -> 300m to close, outside the 120m window.
    assert _bot("charm_close_magnet").open_criteria(_charm_snap(timestamp=LATE_MORN_11ET)) is None


# ======================================================================
# VannaVolCrushRider
# ======================================================================


def _vanna_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=LATE_MORN_11ET,  # 11:00 ET
        spot=755.0,
        net_gex=5.0e8,
        dealer_vanna_total=-1.2e8,  # short-vanna book
        vix=15.0,
        prior_vix=16.0,  # ΔVIX = -1.0 (vol crush)
        recent_closes=_flat(755.0),
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_vanna_vol_crush_fires_bullish_on_crush():
    """Short vanna × falling VIX => dealers must BUY => bullish melt-up."""
    sig = _bot("vanna_vol_crush_rider").open_criteria(_vanna_snap())
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.legs[0].option_type == "call"


def test_vanna_vol_crush_flips_bearish_on_spike():
    """Short vanna × rising VIX => dealers must SELL => bearish."""
    sig = _bot("vanna_vol_crush_rider").open_criteria(
        _vanna_snap(vix=15.0, prior_vix=14.0)  # ΔVIX = +1.0
    )
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.legs[0].option_type == "put"


def test_vanna_vol_crush_needs_a_real_vol_move():
    assert (
        _bot("vanna_vol_crush_rider").open_criteria(
            _vanna_snap(vix=15.0, prior_vix=15.05)  # ΔVIX = -0.05, below floor
        )
        is None
    )


def test_vanna_vol_crush_needs_meaningful_vanna():
    snap = _vanna_snap(dealer_vanna_total=1.0e7)  # tiny vanna, below floor
    assert _bot("vanna_vol_crush_rider").open_criteria(snap) is None


# ======================================================================
# AggressorFlowDivergence
# ======================================================================


def _flow_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,  # 12:00 ET
        spot=755.0,
        net_gex=5.0e8,  # positive_weak (not a strong pin)
        call_wall=760.0,
        put_wall=750.0,
        flow_net_premium=2.0e6,  # strong call-led aggression
        flow_net_premium_prev=1.0e6,  # still accelerating
        flow_net_volume=5000.0,  # volume confirms
        recent_closes=_flat(755.0),  # price hasn't moved yet (divergence)
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_aggressor_flow_fires_ahead_of_price():
    sig = _bot("aggressor_flow_divergence").open_criteria(_flow_snap())
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.legs[0].option_type == "call"
    assert sig.target_price == 760.0  # call wall


def test_aggressor_flow_stands_down_when_price_already_moved():
    """Divergence gone: price already ran with the flow -> no edge left."""
    assert (
        _bot("aggressor_flow_divergence").open_criteria(
            _flow_snap(recent_closes=_rising(755.0, pct=0.004))  # +0.4% already
        )
        is None
    )


def test_aggressor_flow_requires_volume_to_confirm():
    assert (
        _bot("aggressor_flow_divergence").open_criteria(
            _flow_snap(flow_net_volume=-5000.0)  # premium up, volume down -> conflict
        )
        is None
    )


def test_aggressor_flow_stands_down_in_strong_pin():
    assert _bot("aggressor_flow_divergence").open_criteria(_flow_snap(net_gex=3.0e9)) is None


def test_aggressor_flow_needs_size():
    snap = _flow_snap(flow_net_premium=1.0e5)  # below the min-premium floor
    assert _bot("aggressor_flow_divergence").open_criteria(snap) is None


# ======================================================================
# GammaRegimeShiftRider
# ======================================================================


def _regime_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,  # 12:00 ET
        spot=755.0,
        net_gex=2.0e8,  # just crossed toward short
        prior_net_gex=1.5e9,  # was firmly long gamma
        gamma_flip=755.0,  # spot at the flip
        flip_distance=0.0,
        convexity_risk=6.0e11,
        call_wall=760.0,
        put_wall=750.0,
        flow_net_volume=4000.0,  # bullish volume confirms the break
        recent_closes=_rising(755.0, pct=0.0014),  # breaking up
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_gamma_regime_shift_fires_on_transition():
    sig = _bot("gamma_regime_shift_rider").open_criteria(_regime_snap())
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.legs[0].option_type == "call"
    assert sig.target_price == 760.0  # far wall


def test_gamma_regime_shift_needs_prior_long_gamma():
    """The edge is the CROSSING. If dealers were already short, this is a
    continuation trade, not a regime shift -> abstain."""
    assert (
        _bot("gamma_regime_shift_rider").open_criteria(_regime_snap(prior_net_gex=-1.0e8)) is None
    )


def test_gamma_regime_shift_needs_fast_shed():
    """Shed is RELATIVE to prior net-GEX. A 5.0e8 -> 4.7e8 dip is a 6% shed,
    below the 25% floor -> no regime transition."""
    assert (
        _bot("gamma_regime_shift_rider").open_criteria(
            _regime_snap(prior_net_gex=5.0e8, net_gex=4.7e8)  # ~6% shed
        )
        is None
    )


def test_gamma_regime_shift_needs_a_transition():
    """Far from the flip AND not yet crossed to short -> no transition to ride
    (net_gex still positive at 2e8, flip 5% away)."""
    assert _bot("gamma_regime_shift_rider").open_criteria(_regime_snap(flip_distance=0.05)) is None


def test_gamma_regime_shift_fires_when_crossed_short_even_if_far_from_flip():
    """A completed crossing (net_gex <= 0) is itself the transition, so a wide
    flip distance is fine once dealers are actually short."""
    sig = _bot("gamma_regime_shift_rider").open_criteria(
        _regime_snap(net_gex=-3.0e8, flip_distance=0.05)
    )
    assert sig is not None
    assert sig.direction == "bullish"


def test_gamma_regime_shift_requires_flow_agreement():
    assert (
        _bot("gamma_regime_shift_rider").open_criteria(
            _regime_snap(flow_net_volume=-4000.0)  # volume opposes the up-break
        )
        is None
    )


# ======================================================================
# Registry invariant: candidates are registered + runnable but NOT live.
# ======================================================================


def test_candidates_are_registered_but_not_live():
    assert DEFAULT_ROSTER == (), "candidates must never enter the live roster on merge"
    active_ids = {s.id for s in DEFAULT_ROSTER}
    for spec in CANDIDATE_SPECS:
        # runnable / backtestable
        assert get_bot_class(spec.strategy_class) is not None
        # symbol-agnostic like the rest of the fleet
        assert spec.universe == "*", f"{spec.id} pinned to {spec.universe!r}"
        # not live, and not retired (so the backtest can screen + promote it)
        assert spec.id not in active_ids
        assert (
            spec.id not in RETIRED_BOT_IDS
        ), f"{spec.id} is a fresh candidate; it must not be in RETIRED_BOT_IDS"


def test_open_criteria_records_miss_reasons():
    """The backtest screen relies on per-gate miss tallies to explain a
    0-trade result. A rejected setup must name the gate it failed."""
    bot = _bot("charm_close_magnet")
    # Wrong regime -> the 'regime' gate should tick.
    bot.open_criteria(_charm_snap(net_gex=-3.0e9))
    assert bot.miss_reasons.get("regime", 0) >= 1
    # A firing setup adds no miss.
    before = sum(bot.miss_reasons.values())
    sig = bot.open_criteria(_charm_snap())
    assert sig is not None
    assert sum(bot.miss_reasons.values()) == before


def test_candidate_set_is_the_four_edge_bots():
    ids = {s.id for s in CANDIDATE_SPECS}
    assert ids == {
        "charm_close_magnet",
        "vanna_vol_crush_rider",
        "aggressor_flow_divergence",
        "gamma_regime_shift_rider",
    }


def test_backtest_screens_unprovisioned_candidates_via_registry_fallback():
    """The promotion gate must actually be runnable: a candidate is NEVER in
    ``tw_bots`` (it must not provision), so ``make tradeworkz-backtest
    --bots <id>`` has to resolve it from the registry catalog. Without the
    fallback the harness returns "no bots found" (the exact failure hit on the
    first live run)."""
    from src.tradeworkz.backtest import _load_backtest_bots

    class _EmptyCur:
        """tw_bots returns nothing — simulates the un-provisioned candidate."""

        def execute(self, sql, params=None):
            self._rows = []

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    class _EmptyConn:
        def cursor(self):
            return _EmptyCur()

    requested = [s.id for s in CANDIDATE_SPECS]
    specs = _load_backtest_bots(_EmptyConn(), requested)
    loaded = {s.id for s in specs}
    assert loaded == set(requested), (
        "every requested candidate must be screenable even though none is in "
        f"tw_bots; got {loaded}"
    )
    for spec in specs:
        assert get_bot_class(spec.strategy_class) is not None
