"""Entry/exit-logic tests for the v5 edge-metric candidate bots.

The v5 fleet trades data layers no bot tier ever consumed — the forced-flow
raw/smooth settlement residual, the dual-flip disagreement band, gex_profile
curve geometry, the delta-weighted pending hedge obligation, the per-type
put-capitulation split (first credit-exit bot), and the bucketed weekly charm
ladder (first 1DTE candidate). The tests build a :class:`MarketSnapshot`
directly (no DB) and lock in, for each bot:

* it FIRES with the right direction / structure / target on a clean setup;
* the *edge filter* that distinguishes it from prior (retired / screened-out)
  strategies actually gates it;
* it stands down when its data preconditions are absent (a None field must
  abstain, never crash or trade);
* its custom exit invalidation closes when the mechanism it priced dies.

Plus the registry invariant: candidates are registered and screenable but NOT
live (empty DEFAULT_ROSTER, not retired) — they promote only via the backtest
gate, and the base class's new credit-exit branch behaves per spec.

All timestamps are ET→UTC for August (EDT, UTC-4).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import OpenPosition
from src.tradeworkz.registry import (
    CANDIDATE_SPECS,
    DEFAULT_ROSTER,
    RETIRED_BOT_IDS,
    get_bot_class,
    known_specs,
)

# ET wall-clock instants (August = EDT = UTC-4).
MID_12ET = datetime(2026, 8, 6, 16, 0, tzinfo=timezone.utc)  # 12:00 ET
MID_1230ET = datetime(2026, 8, 6, 16, 30, tzinfo=timezone.utc)  # 12:30 ET
LATE_15ET = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)  # 15:00 ET

V5_IDS = (
    "settlement_flow_snap",
    "dual_flip_dislocation",
    "profile_shelf_breaker",
    "hedge_impulse_quiet_tape",
    "put_capitulation_credit_fade",
    "weekly_charm_grind",
)


def _bot(bot_id: str):
    spec = known_specs()[bot_id]
    return get_bot_class(spec.strategy_class)(spec, ml_state=None)


def _pos(
    bot_id: str,
    direction: str = "bullish",
    entry_price: float = 1.0,
    current_price: float = 1.0,
    components: Optional[dict] = None,
    opened_minutes_ago: float = 10.0,
) -> OpenPosition:
    now = datetime.now(timezone.utc)
    return OpenPosition(
        id=1,
        bot_id=bot_id,
        underlying="SPY",
        opened_at=now - timedelta(minutes=opened_minutes_ago),
        direction=direction,
        strategy_type="TEST",
        legs=[],
        entry_price=entry_price,
        current_price=current_price,
        quantity_open=1,
        unrealized_pnl=0.0,
        stop_price=None,
        target_price=None,
        time_stop_at=now + timedelta(hours=2),
        min_hold_until=now - timedelta(minutes=opened_minutes_ago - 0.1),
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.6,
        components_at_entry=components or {},
    )


def _flat(spot: float, n: int = 20) -> list[float]:
    return [spot] * n


# ======================================================================
# SettlementFlowSnap
# ======================================================================


def _settle_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=LATE_15ET,  # 60m to close, inside [30, 90]
        spot=755.0,
        net_gex=2.0e9,
        local_gex=1.0e9,
        close_charm_flow_raw=8.0e8,
        close_charm_flow_smooth=2.0e8,  # residual D = +6e8: dominates, big
        recent_closes=_flat(755.0),
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_settlement_snap_fires_with_the_residual():
    sig = _bot("settlement_flow_snap").open_criteria(_settle_snap())
    assert sig is not None
    assert sig.direction == "bullish"  # D > 0 => forced dealer BUYING
    assert len(sig.legs) == 2  # defined-risk vertical
    assert sig.legs[0].option_type == "call"


def test_settlement_snap_flips_bearish_on_negative_residual():
    sig = _bot("settlement_flow_snap").open_criteria(
        _settle_snap(close_charm_flow_raw=-8.0e8, close_charm_flow_smooth=-2.0e8)
    )
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.legs[0].option_type == "put"


def test_settlement_snap_needs_both_ff_fields():
    assert (
        _bot("settlement_flow_snap").open_criteria(_settle_snap(close_charm_flow_raw=None)) is None
    )


def test_settlement_snap_requires_residual_to_rival_local_gamma():
    # |D| = 1.4e8 < 0.25 * local_gex (2.5e8) — too small to move price.
    assert (
        _bot("settlement_flow_snap").open_criteria(
            _settle_snap(close_charm_flow_raw=2.4e8, close_charm_flow_smooth=1.0e8)
        )
        is None
    )


def test_settlement_snap_cedes_drift_days_to_charm_close_magnet():
    """The disjointness gate: when the smooth drift dominates the residual,
    the day belongs to charm_close_magnet — this bot must stand down."""
    assert (
        _bot("settlement_flow_snap").open_criteria(
            _settle_snap(close_charm_flow_raw=9.0e8, close_charm_flow_smooth=7.0e8)
        )
        is None
    )


def test_settlement_snap_only_in_final_window():
    assert _bot("settlement_flow_snap").open_criteria(_settle_snap(timestamp=MID_12ET)) is None


def test_settlement_snap_abstains_when_tape_fights_the_flow():
    assert (
        _bot("settlement_flow_snap").open_criteria(_settle_snap(flow_recent_premium=-3.0e6)) is None
    )


def test_settlement_snap_exit_on_flow_flip_and_fade():
    bot = _bot("settlement_flow_snap")
    pos = _pos("settlement_flow_snap", components={"settlement_residual": 6.0e8})
    flipped = _settle_snap(close_charm_flow_raw=-7.0e8, close_charm_flow_smooth=-2.0e8)
    assert bot.exit_criteria(flipped, pos).reason == "flow_flip"
    faded = _settle_snap(close_charm_flow_raw=2.5e8, close_charm_flow_smooth=1.5e8)
    assert bot.exit_criteria(faded, pos).reason == "flow_faded"
    # A data hiccup (missing pair) must NOT force an exit.
    hiccup = _settle_snap(close_charm_flow_raw=None, close_charm_flow_smooth=None)
    assert bot.exit_criteria(hiccup, pos).should_close is False


# ======================================================================
# DualFlipDislocation
# ======================================================================


def _dualflip_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,
        spot=755.0,
        net_gex=5.0e8,
        gamma_flip=750.0,  # structural
        gamma_flip_raw=756.0,  # fast/unweighted flip ABOVE — band [750, 756]
        gamma_flip_span_used=0.20,
        # Fresh down-cross of the raw flip with 2-bar momentum down.
        recent_closes=[757.0, 757.0, 756.8, 756.5, 755.0],
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_dual_flip_fires_bearish_on_fresh_down_cross():
    sig = _bot("dual_flip_dislocation").open_criteria(_dualflip_snap())
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.target_price == 750.0  # the band's far edge = structural flip
    assert sig.legs[0].option_type == "put"
    assert len(sig.legs) == 2


def test_dual_flip_fires_bullish_on_up_cross_of_structural():
    sig = _bot("dual_flip_dislocation").open_criteria(
        _dualflip_snap(spot=751.0, recent_closes=[749.0, 749.0, 749.2, 749.5, 751.0])
    )
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.target_price == 756.0  # far edge = the raw flip


def test_dual_flip_needs_both_flips():
    assert _bot("dual_flip_dislocation").open_criteria(_dualflip_snap(gamma_flip_raw=None)) is None


def test_dual_flip_requires_a_real_band():
    # 0.5-point band = 0.07% of spot < 0.2% floor.
    assert (
        _bot("dual_flip_dislocation").open_criteria(
            _dualflip_snap(gamma_flip_raw=750.5, spot=750.2, recent_closes=[751.0] * 4 + [750.2])
        )
        is None
    )


def test_dual_flip_requires_amplifying_ordering():
    """raw BELOW structural = fast book positive inside the spread —
    dampening, not amplifying. Must abstain."""
    assert (
        _bot("dual_flip_dislocation").open_criteria(
            _dualflip_snap(gamma_flip=756.0, gamma_flip_raw=750.0)
        )
        is None
    )


def test_dual_flip_distrusts_expansion_rung_flips():
    assert (
        _bot("dual_flip_dislocation").open_criteria(_dualflip_snap(gamma_flip_span_used=0.50))
        is None
    )


def test_dual_flip_never_chases_a_stale_band():
    # Spot has been inside the band all along — no fresh crossing.
    assert (
        _bot("dual_flip_dislocation").open_criteria(
            _dualflip_snap(recent_closes=[755.6, 755.5, 755.5, 755.5, 755.0])
        )
        is None
    )


def test_dual_flip_exit_on_band_collapse():
    bot = _bot("dual_flip_dislocation")
    pos = _pos("dual_flip_dislocation", direction="bearish")
    collapsed = _dualflip_snap(gamma_flip=754.9, gamma_flip_raw=755.2)
    assert bot.exit_criteria(collapsed, pos).reason == "band_collapse"
    inverted = _dualflip_snap(gamma_flip=756.0, gamma_flip_raw=750.0)
    assert bot.exit_criteria(inverted, pos).reason == "band_inverted"


# ======================================================================
# ProfileShelfBreaker
# ======================================================================


def _shelf_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,
        spot=755.0,
        net_gex=3.0e8,  # on the shoulder (|gex| <= 0.5 * local_gex)
        local_gex=1.0e9,
        profile_gex_down=-9.0e8,  # cliff just below spot
        profile_gex_up=2.0e8,  # nothing comparable above — one-sided
        profile_trough_price=749.0,
        profile_trough_gex=-1.4e9,
        # Price sliding into the shelf: -0.20% over the last 5 bars.
        recent_closes=[756.5, 756.5, 756.2, 756.0, 755.8, 755.5, 755.0],
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_shelf_breaker_fires_bearish_into_the_cliff():
    sig = _bot("profile_shelf_breaker").open_criteria(_shelf_snap())
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.target_price == 749.0  # the trough bottom — where the flow exhausts
    assert sig.legs[0].option_type == "put"
    assert len(sig.legs) == 2


def test_shelf_breaker_needs_the_profile():
    assert _bot("profile_shelf_breaker").open_criteria(_shelf_snap(profile_gex_down=None)) is None


def test_shelf_breaker_requires_one_sidedness():
    # Both sides deeply negative — a bowl, not a cliff (asymmetry < 2:1).
    assert _bot("profile_shelf_breaker").open_criteria(_shelf_snap(profile_gex_up=-8.0e8)) is None


def test_shelf_breaker_geometry_alone_is_not_a_trade():
    # Same cliff, flat tape — the map without the slide must abstain.
    assert (
        _bot("profile_shelf_breaker").open_criteria(_shelf_snap(recent_closes=_flat(755.0))) is None
    )


def test_shelf_breaker_stands_down_off_the_shoulder():
    # Spot deep in stabilizing positive territory — cliff unreachable.
    assert _bot("profile_shelf_breaker").open_criteria(_shelf_snap(net_gex=8.0e8)) is None


def test_shelf_breaker_exit_when_shelf_refills():
    bot = _bot("profile_shelf_breaker")
    pos = _pos("profile_shelf_breaker", direction="bearish")
    refilled = _shelf_snap(profile_gex_down=-2.0e8)
    assert bot.exit_criteria(refilled, pos).reason == "shelf_refilled"


# ======================================================================
# HedgeImpulseQuietTape
# ======================================================================


def _impulse_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,
        spot=755.0,
        net_gex=-2.0e9,  # negative_strong (absolute fallback) — amplifying
        hedge_impulse_shares=7.5e5,
        hedge_tape_volume=5.0e6,  # ratio = +0.15 of tape
        recent_closes=_flat(755.0),  # the defining gate: tape has NOT moved
    )
    base.update(over)
    return MarketSnapshot(**base)


def _prime(bot, snap_kwargs=None):
    """First sighting registers the impulse sign; the bot must NOT fire yet."""
    first = _impulse_snap(timestamp=MID_12ET - timedelta(minutes=5), **(snap_kwargs or {}))
    assert bot.open_criteria(first) is None  # not_persistent on first sight
    return bot


def test_hedge_impulse_fires_only_after_persistence():
    bot = _prime(_bot("hedge_impulse_quiet_tape"))
    sig = bot.open_criteria(_impulse_snap())
    assert sig is not None
    assert sig.direction == "bullish"  # positive impulse = forced dealer buying
    assert len(sig.legs) == 2


def test_hedge_impulse_bearish_on_put_side_obligation():
    bot = _prime(
        _bot("hedge_impulse_quiet_tape"),
        {"hedge_impulse_shares": -7.5e5},
    )
    sig = bot.open_criteria(_impulse_snap(hedge_impulse_shares=-7.5e5))
    assert sig is not None
    assert sig.direction == "bearish"


def test_hedge_impulse_refuses_a_moved_tape():
    """The anti-chase gate — the exact tape state the two dead flow-followers
    entered on (price already moved) must abstain here."""
    moved = [755.0 * (1.0 + 0.003 * i / 16.0) for i in range(17)]
    bot = _prime(_bot("hedge_impulse_quiet_tape"))
    assert bot.open_criteria(_impulse_snap(recent_closes=moved)) is None


def test_hedge_impulse_requires_negative_gamma():
    bot = _prime(_bot("hedge_impulse_quiet_tape"))
    assert bot.open_criteria(_impulse_snap(net_gex=2.0e9)) is None


def test_hedge_impulse_requires_meaningful_ratio():
    bot = _prime(_bot("hedge_impulse_quiet_tape"))
    assert bot.open_criteria(_impulse_snap(hedge_impulse_shares=1.0e5)) is None


def test_hedge_impulse_abstains_without_the_fetcher():
    bot = _bot("hedge_impulse_quiet_tape")
    assert bot.open_criteria(_impulse_snap(hedge_impulse_shares=None)) is None


def test_hedge_impulse_exit_on_reversal_and_expiry():
    bot = _bot("hedge_impulse_quiet_tape")
    pos = _pos(
        "hedge_impulse_quiet_tape",
        components={"hedge_impulse_ratio": 0.15, "entry_spot": 755.0},
        opened_minutes_ago=50.0,
    )
    flipped = _impulse_snap(hedge_impulse_shares=-3.0e5)  # ratio -0.06, |.| >= 0.04
    assert bot.exit_criteria(flipped, pos).reason == "impulse_flip"
    # 50 minutes in, tape still flat, impulse gone quiet -> thesis expired.
    quiet = _impulse_snap(hedge_impulse_shares=None, hedge_tape_volume=None, spot=755.2)
    assert bot.exit_criteria(quiet, pos).reason == "impulse_expired"


# ======================================================================
# PutCapitulationCreditFade
# ======================================================================


def _capit_closes() -> list[float]:
    # -0.4% displacement over 30 bars, but front-loaded: the last 10 bars are
    # nearly flat so the fleet trend-veto (10-bar, 0.2%) does not fire.
    decline = [758.0 - (758.0 - 755.3) * i / 20.0 for i in range(21)]
    tail = [755.3 - 0.3 * i / 9.0 for i in range(10)]
    return decline + tail


def _capit_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_1230ET,
        spot=755.0,
        net_gex=3.0e9,  # positive_strong (absolute fallback) — the absorber
        put_wall=752.0,  # spot 0.40% above — room to absorb
        put_panic_premium=8.0e5,
        put_panic_baseline=1.0e5,  # 8x burst
        put_panic_dominance=0.80,
        recent_closes=_capit_closes(),
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_put_capitulation_sells_the_panic():
    sig = _bot("put_capitulation_credit_fade").open_criteria(_capit_snap())
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.strategy_type == "BULL_PUT_CREDIT_SPREAD"
    assert len(sig.legs) == 2
    assert all(leg.option_type == "put" for leg in sig.legs)
    long_leg = next(leg for leg in sig.legs if leg.side == "long")
    short_leg = next(leg for leg in sig.legs if leg.side == "short")
    assert long_leg.strike < short_leg.strike < 755.0  # credit spread below spot
    assert sig.target_price is None and sig.stop_price is None  # premium exits only


def test_put_capitulation_needs_the_full_strength_absorber():
    # Merely positive gamma is not enough — the edge is the FORCED dip-buy.
    assert _bot("put_capitulation_credit_fade").open_criteria(_capit_snap(net_gex=5.0e8)) is None


def test_put_capitulation_needs_a_real_burst():
    assert (
        _bot("put_capitulation_credit_fade").open_criteria(_capit_snap(put_panic_premium=1.5e5))
        is None
    )


def test_put_capitulation_needs_one_way_fear():
    assert (
        _bot("put_capitulation_credit_fade").open_criteria(_capit_snap(put_panic_dominance=0.50))
        is None
    )


def test_put_capitulation_wall_is_a_floor_not_a_trigger():
    # No room above the put wall -> the absorber never gets to buy first.
    assert _bot("put_capitulation_credit_fade").open_criteria(_capit_snap(put_wall=754.5)) is None


def test_put_capitulation_stands_down_on_a_vol_event():
    assert (
        _bot("put_capitulation_credit_fade").open_criteria(_capit_snap(vix=19.5, prior_vix=16.0))
        is None
    )


def test_put_capitulation_abstains_without_flow_split():
    assert (
        _bot("put_capitulation_credit_fade").open_criteria(_capit_snap(put_panic_premium=None))
        is None
    )


def test_put_capitulation_exit_when_the_wall_is_lost():
    bot = _bot("put_capitulation_credit_fade")
    pos = _pos(
        "put_capitulation_credit_fade",
        entry_price=-1.0,
        current_price=-1.0,
        components={"put_wall": 752.0},
    )
    assert bot.exit_criteria(_capit_snap(spot=751.0), pos).reason == "wall_lost"


def test_credit_take_and_stop_branch():
    """The base class's new credit-exit branch, driven by this bot's params
    (credit_take_frac=0.55, credit_stop_mult=1.75)."""
    bot = _bot("put_capitulation_credit_fade")
    snap = _capit_snap()  # spot comfortably above the wall — no wall_lost
    take = _pos("put_capitulation_credit_fade", entry_price=-1.0, current_price=-0.40)
    assert bot.exit_criteria(snap, take).reason == "credit_take"  # +0.60 >= 0.55x credit
    stop = _pos("put_capitulation_credit_fade", entry_price=-1.0, current_price=-2.80)
    assert bot.exit_criteria(snap, stop).reason == "credit_stop"  # -1.80 <= -1.75x credit
    hold = _pos("put_capitulation_credit_fade", entry_price=-1.0, current_price=-0.80)
    assert bot.exit_criteria(snap, hold).should_close is False  # neither level hit


# ======================================================================
# WeeklyCharmGrind
# ======================================================================


def _grind_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,  # 150 minutes since open — inside [90, 270]
        spot=755.0,
        net_gex=3.0e9,  # positive_strong — the damped regime
        local_gex=1.2e9,
        session_high=756.0,
        session_low=752.0,  # 0.53% range — compressed
        dealer_charm_weekly=6.0e8,  # weekly book decay => dealers buy
        dealer_charm_0dte=1.0e8,  # 6x dominance
        recent_closes=_flat(755.0),
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_weekly_charm_grind_fires_1dte_with_the_decay():
    sig = _bot("weekly_charm_grind").open_criteria(_grind_snap())
    assert sig is not None
    assert sig.direction == "bullish"
    assert len(sig.legs) == 2
    assert sig.legs[0].option_type == "call"
    # The whole point of the structure: 1DTE, not same-day expiry.
    assert sig.legs[0].expiration > "2026-08-06"


def test_weekly_charm_grind_cedes_0dte_days_to_the_eod_bots():
    # 0DTE bucket comparable to weekly -> same-day settlement mechanics
    # dominate; that regime belongs to charm_close_magnet / settlement bots.
    assert _bot("weekly_charm_grind").open_criteria(_grind_snap(dealer_charm_0dte=5.0e8)) is None


def test_weekly_charm_grind_needs_the_bucket_ladder():
    assert _bot("weekly_charm_grind").open_criteria(_grind_snap(dealer_charm_weekly=None)) is None


def test_weekly_charm_grind_needs_a_compressed_session():
    assert (
        _bot("weekly_charm_grind").open_criteria(_grind_snap(session_high=760.0, session_low=750.0))
        is None
    )


def test_weekly_charm_grind_needs_positive_gamma():
    assert _bot("weekly_charm_grind").open_criteria(_grind_snap(net_gex=-2.0e9)) is None


def test_weekly_charm_grind_midday_window_only():
    early = MID_12ET - timedelta(minutes=90)  # 10:30 ET, m=60 < 90
    assert _bot("weekly_charm_grind").open_criteria(_grind_snap(timestamp=early)) is None


def test_weekly_charm_grind_exit_on_positioning_turnover():
    bot = _bot("weekly_charm_grind")
    pos = _pos("weekly_charm_grind", components={"dealer_charm_weekly": 6.0e8})
    flipped = _grind_snap(dealer_charm_weekly=-4.0e8)
    assert bot.exit_criteria(flipped, pos).reason == "charm_flip"
    mixed = _grind_snap(dealer_charm_weekly=3.0e8, dealer_charm_0dte=2.9e8)
    assert bot.exit_criteria(mixed, pos).reason == "bucket_mixed"


# ======================================================================
# Registry invariants
# ======================================================================


def test_v5_candidates_are_registered_not_live():
    """Same discipline as v4: registered + screenable by id, but nothing is
    provisioned, sized, or opened until it clears the backtest gate."""
    specs = known_specs()
    roster_ids = {s.id for s in DEFAULT_ROSTER}
    candidate_ids = {s.id for s in CANDIDATE_SPECS}
    for bot_id in V5_IDS:
        assert bot_id in specs
        assert bot_id in candidate_ids
        assert bot_id not in roster_ids
        assert bot_id not in RETIRED_BOT_IDS
        # Resolvable and constructible.
        bot = get_bot_class(specs[bot_id].strategy_class)(specs[bot_id], ml_state=None)
        assert bot.spec.id == bot_id
    assert len(DEFAULT_ROSTER) == 0  # nothing goes live on a thesis
