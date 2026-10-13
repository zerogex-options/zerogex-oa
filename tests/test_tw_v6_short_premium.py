"""Entry / exit logic for the v6 short-premium engine and the LongGammaIronFly.

The v6 family is the first in the fleet to trade MAGNITUDE rather than
direction: a defined-risk short 0DTE iron butterfly sold only when the
platform's confirmed magnitude signals say the tape will travel less than the
straddle is charging. These tests build a ``MarketSnapshot`` directly (no DB)
and lock in, for the shared engine:

* it FIRES a four-leg ATM iron butterfly with symmetric sigma-sized wings on a
  clean setup, neutral, premium-exit only, time-stopped before the bell;
* every stand-down gate abstains with its own ``_skip`` reason, and a None
  precondition abstains rather than crashing or trading;
* gates switch OFF with a falsy param (``0`` / ``""`` / ``[]``), never with
  ``None`` (which means "use the engine default") — the sweep's control arms
  depend on that convention;
* the sigma source falls back from the straddle to the forecast and no further
  by default; the gamma-blind twin is a param, not a different bot;
* the invalidation exits fire regardless of P&L and the base credit take /
  stop still works underneath them;
* the registry / catalog invariants: registered, RESEARCH, never funded, with
  the review's defaults (stop at half the credit, band and IV gates off).

All timestamps are ET -> UTC for August (EDT, UTC-4).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from src.strategies import Stage, get
from src.tradeworkz.bots.short_premium import SIGMA_FORECAST, SIGMA_REALIZED, SIGMA_STRADDLE
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import OpenPosition
from src.tradeworkz.registry import DEFAULT_ROSTER, get_bot_class, known_specs

# Thursday 2026-08-06. The gated fly's catalog window is 10:30-13:30 ET entries
# (the pre-registered one); afternoon_iron_fly's is 13:00-14:30. 13:30 is inside both.
PM_1330ET = datetime(2026, 8, 6, 17, 30, tzinfo=timezone.utc)  # 13:30 ET: 240 in, 150 to go
MID_12ET = datetime(2026, 8, 6, 16, 0, tzinfo=timezone.utc)  # 12:00 ET: 150 in, 240 to go
OPEN_0945ET = datetime(2026, 8, 6, 13, 45, tzinfo=timezone.utc)  # 09:45 ET
LATE_1430ET = datetime(2026, 8, 6, 18, 30, tzinfo=timezone.utc)  # 14:30 ET: 90 to go
LATE_1435ET = datetime(2026, 8, 6, 18, 35, tzinfo=timezone.utc)  # 14:35 ET: 85 to go

BOT_ID = "long_gamma_iron_fly"


def _bot(bot_id: str = BOT_ID, **param_over):
    spec = known_specs()[bot_id]
    if param_over:
        spec.params = {**spec.params, **param_over}
    return get_bot_class(spec.strategy_class)(spec, ml_state=None)


def _quiet_closes(spot: float, bps: float = 1.0, n: int = 31) -> list[float]:
    """Closes alternating +/- ``bps`` around spot: a low, steady realized sigma.

    Against the $3.00 straddle in :func:`_snap` (sigma $3.76) at 13:30 ET the
    realized move projected over the rest of the day reads about 0.26 of the
    straddle at 1 bp, 0.52 at 2 bps, 0.65 at 2.5 bps, 0.78 at 3 bps, 0.91 at
    3.5 bps (over the 0.85 cap) and 5.2 at 20 bps.
    """
    step = spot * bps / 10_000.0
    return [spot + (step if i % 2 else 0.0) for i in range(n)]


def _msi(magnitude: float) -> dict:
    return {"__aggregation__": {"magnitude_score": magnitude}}


def _snap(**over) -> MarketSnapshot:
    """A clean long-gamma, quiet-tape, early-afternoon SPY setup."""
    base = dict(
        underlying="SPY",
        timestamp=PM_1330ET,
        spot=656.0,
        net_gex=2.0e9,
        net_gex_pctile=85.0,
        gamma_flip=640.0,
        gamma_flip_raw=642.0,
        vix=15.0,
        prior_vix=15.1,
        recent_closes=_quiet_closes(656.0),
        msi_components=_msi(30.0),  # chop_range
        atm_strike_front=656.0,
        atm_straddle_mid_front=3.0,  # sigma = 3.0 / 0.798 ~= 3.76
        atm_straddle_bid_front=2.9,
        atm_iv_front=0.16,
        atm_iv_pctile_60d=45.0,
    )
    base.update(over)
    return MarketSnapshot(**base)


def _pos(
    components: Optional[dict] = None,
    entry_price: float = -1.40,
    current_price: float = -1.40,
    opened_minutes_ago: float = 30.0,
) -> OpenPosition:
    now = datetime.now(timezone.utc)
    return OpenPosition(
        id=1,
        bot_id=BOT_ID,
        underlying="SPY",
        opened_at=now - timedelta(minutes=opened_minutes_ago),
        direction="neutral",
        strategy_type="SHORT_IRON_BUTTERFLY",
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
        components_at_entry=components
        or {
            "center": 656.0,
            "put_short": 656.0,
            "call_short": 656.0,
            "sigma": 3.76,
            "net_gex_pctile": 85.0,
        },
    )


# ======================================================================
# Fires
# ======================================================================


def test_fires_an_atm_iron_butterfly_with_sigma_wings():
    bot = _bot()
    sig = bot.open_criteria(_snap())
    assert sig is not None, bot.miss_reasons
    assert sig.direction == "neutral"
    assert sig.strategy_type == "SHORT_IRON_BUTTERFLY"
    assert sig.target_price is None and sig.stop_price is None
    legs = sig.legs
    assert len(legs) == 4
    by = {(lg.option_type, lg.side): lg for lg in legs}
    assert by[("put", "short")].strike == 656.0 == by[("call", "short")].strike
    # sigma 3.76 x 1.25 = 4.7 -> 5 increments on the $1 grid
    assert by[("put", "long")].strike == 651.0
    assert by[("call", "long")].strike == 661.0
    assert all(lg.expiration == "2026-08-06" for lg in legs)
    comps = sig.components_at_entry
    assert comps["sigma_source"] == SIGMA_STRADDLE
    assert comps["wing_increments"] == 5
    assert comps["magnitude_band"] == "chop_range"
    # The gate's own ratio: dollars realized over dollars charged, both for
    # the rest of the day. The annualized form is carried as a diagnostic.
    assert 0.0 < comps["realized_over_straddle"] < 0.85
    assert comps["realized_remaining_move"] > 0.0
    assert comps["realized_over_implied_annualized"] is not None
    assert "realized/straddle" in sig.rationale


def test_time_stop_never_reaches_the_settlement_window():
    from src.tradeworkz.bots import base as bot_base

    bot_base.set_backtest_clock(lambda: PM_1330ET)
    try:
        sig = _bot().open_criteria(_snap())
    finally:
        bot_base.set_backtest_clock(None)
    assert sig is not None
    # (150 to close - 5 buffer) = 145 < max_hold 180 -> held to 15:55 ET, the harness cap
    assert sig.time_stop_at == PM_1330ET + timedelta(minutes=145)
    # the afternoon fly's last entry (14:30) holds 85 minutes to the same 15:55
    bot_base.set_backtest_clock(lambda: LATE_1430ET)
    try:
        bot = _bot("afternoon_iron_fly")
        sig = bot.open_criteria(_snap(timestamp=LATE_1430ET))
    finally:
        bot_base.set_backtest_clock(None)
    assert sig is not None, bot.miss_reasons
    assert sig.time_stop_at == LATE_1430ET + timedelta(minutes=90 - 5)
    # an early entry is capped by max_hold (180) long before the bell
    bot_base.set_backtest_clock(lambda: MID_12ET)
    try:
        sig = _bot().open_criteria(_snap(timestamp=MID_12ET))
    finally:
        bot_base.set_backtest_clock(None)
    assert sig is not None and sig.time_stop_at == MID_12ET + timedelta(minutes=180)
    # the sweep's early-flat arm leaves at 15:40 instead
    bot_base.set_backtest_clock(lambda: PM_1330ET)
    try:
        sig = _bot(eod_exit_buffer_minutes=20).open_criteria(_snap())
    finally:
        bot_base.set_backtest_clock(None)
    assert sig is not None and sig.time_stop_at == PM_1330ET + timedelta(minutes=130)


def test_spx_snaps_to_the_five_point_grid_and_scales_wings():
    # SPX straddle $30 -> sigma ~37.6 -> wings 1.25 x 37.6 = 47 -> 9 x 5-pt increments
    sig = _bot().open_criteria(
        _snap(
            underlying="SPX",
            spot=6702.0,
            atm_strike_front=6700.0,
            atm_straddle_mid_front=30.0,
            atm_straddle_bid_front=29.0,
            atm_iv_front=0.14,
            gamma_flip=6500.0,
            gamma_flip_raw=6520.0,
            recent_closes=_quiet_closes(6702.0),
        )
    )
    assert sig is not None
    by = {(lg.option_type, lg.side): lg for lg in sig.legs}
    assert by[("put", "short")].strike == 6700.0
    assert by[("put", "long")].strike == 6700.0 - 45.0
    assert by[("call", "long")].strike == 6700.0 + 45.0


def test_condor_variant_offsets_the_shorts():
    sig = _bot(short_offset_sigma=1.0).open_criteria(_snap())
    assert sig is not None
    assert sig.strategy_type == "SHORT_IRON_CONDOR"
    by = {(lg.option_type, lg.side): lg for lg in sig.legs}
    assert by[("put", "short")].strike == 652.0  # 656 - 3.76 -> 652
    assert by[("call", "short")].strike == 660.0
    assert by[("put", "long")].strike == 647.0 and by[("call", "long")].strike == 665.0


def test_wings_never_collapse_below_one_increment():
    # A tiny straddle (sigma ~0.25) would round the wing to zero increments.
    # It also makes any tape look busy against it, so the tail gate is off here.
    bot = _bot(max_realized_over_straddle=0)
    sig = bot.open_criteria(_snap(atm_straddle_mid_front=0.20, atm_straddle_bid_front=0.18))
    assert sig is not None, bot.miss_reasons
    by = {(lg.option_type, lg.side): lg for lg in sig.legs}
    assert by[("put", "long")].strike == 655.0 and by[("call", "long")].strike == 657.0


# ======================================================================
# Stand-down gates (each named)
# ======================================================================


def _miss(bot, snap) -> str:
    assert bot.open_criteria(snap) is None
    assert len(bot.miss_reasons) == 1, bot.miss_reasons
    return next(iter(bot.miss_reasons))


def test_window_gates_both_ends():
    assert _miss(_bot(), _snap(timestamp=OPEN_0945ET)) == "window"  # 15 min in < 60
    assert _bot().open_criteria(_snap(timestamp=MID_12ET)) is not None  # 150 in, 240 to go
    assert _miss(_bot(), _snap(timestamp=LATE_1430ET)) == "window"  # 90 to close < 150
    # The afternoon fly's window is the complement: 13:00-14:30 entries.
    pm = _bot("afternoon_iron_fly")
    assert _miss(pm, _snap(timestamp=MID_12ET)) == "window"  # 150 in < 210
    assert pm.open_criteria(_snap(timestamp=LATE_1430ET)) is not None  # 90 is the edge
    assert _miss(_bot("afternoon_iron_fly"), _snap(timestamp=LATE_1435ET)) == "window"


def test_needs_the_straddle_priced():
    assert _miss(_bot(), _snap(atm_iv_front=None)) == "no_straddle"
    assert _miss(_bot(), _snap(atm_straddle_mid_front=None)) == "no_straddle"


def test_regime_fails_closed_without_the_percentile_history():
    assert _miss(_bot(), _snap(net_gex_pctile=None)) == "no_pctile"
    assert _miss(_bot(), _snap(net_gex=None)) == "no_net_gex"
    assert _miss(_bot(), _snap(net_gex_pctile=60.0)) == "regime"  # positive_weak
    assert _miss(_bot(), _snap(net_gex=-1.0e9, net_gex_pctile=10.0)) == "regime"


def test_gamma_blind_twin_is_a_param_not_a_bot():
    bot = _bot(required_regimes="any")
    sig = bot.open_criteria(_snap(net_gex=-1.0e9, net_gex_pctile=10.0))
    assert sig is not None, bot.miss_reasons
    # ...and its conviction is not throttled by the missing regime (neutral quality term)
    assert sig.conviction >= bot.confidence_threshold()


def test_magnitude_band_gate_is_off_by_default_and_a_sweep_axis():
    # Off: the review found the band collinear with the regime gate and its
    # ratios unadjusted, so a controlled_trend read or no MSI at all still fires.
    assert _bot().open_criteria(_snap(msi_components=_msi(55.0))) is not None
    assert _bot().open_criteria(_snap(msi_components={})) is not None
    # On (the sweep's ``gated/band`` arm): bands outside the list and a
    # missing read both stand down, unless the caller says missing is fine.
    on = dict(allowed_magnitude_bands=["chop_range", "high_risk_reversal"])
    assert _miss(_bot(**on), _snap(msi_components=_msi(55.0))) == "magnitude_band"
    assert _miss(_bot(**on), _snap(msi_components={})) == "no_magnitude"
    bot = _bot(require_magnitude=False, **on)
    assert bot.open_criteria(_snap(msi_components={})) is not None, bot.miss_reasons


def test_realized_over_straddle_tail_gate():
    # Busy tape: +/- 20 bps a minute projects to ~6x what the straddle charges.
    assert _miss(_bot(), _snap(recent_closes=_quiet_closes(656.0, bps=20.0))) == "tail_realized"
    # Just over the 0.85 cap (3.5 bps -> 0.91).
    assert _miss(_bot(), _snap(recent_closes=_quiet_closes(656.0, bps=3.5))) == "tail_realized"
    assert _miss(_bot(), _snap(recent_closes=[656.0] * 4)) == "no_history"
    # The gate is a dollar ratio, so the ATM IV level alone never trips it.
    assert _bot().open_criteria(_snap(atm_iv_front=0.60)) is not None
    # ``0`` switches it off (the clock-only twin); ``None`` keeps the default.
    bot = _bot(max_realized_over_straddle=0)
    assert bot.open_criteria(_snap(recent_closes=_quiet_closes(656.0, bps=20.0))) is not None
    assert _miss(_bot(max_realized_over_straddle=None), _snap(recent_closes=[656.0] * 4)) == (
        "no_history"
    )


def test_short_gamma_down_day_stand_down_is_off_by_default_and_a_sweep_axis():
    # Tape-only twin on a short-gamma DOWN day (spot 656 under a 660 open).
    down_short = _snap(net_gex=-1.0e9, net_gex_pctile=10.0, cash_open=660.0)
    twin = _bot(required_regimes="any")
    assert twin.open_criteria(down_short) is not None, twin.miss_reasons
    on = _bot(required_regimes="any", stand_down_short_gamma_down_days=True)
    assert _miss(on, down_short) == "short_gamma_down_day"
    # Up day, long gamma, or no cash open: nothing to stand down on.
    up_short = _snap(net_gex=-1.0e9, net_gex_pctile=10.0, cash_open=650.0)
    assert on.open_criteria(up_short) is not None, on.miss_reasons
    assert on.open_criteria(_snap(cash_open=660.0)) is not None, on.miss_reasons
    assert on.open_criteria(_snap(net_gex=-1.0e9, net_gex_pctile=10.0)) is not None


def test_forecast_gates_only_when_a_forecast_exists():
    assert _miss(_bot(), _snap(forecast_expected_vol_state="expansion")) == "forecast_expansion"
    spent = _snap(forecast_implied_move=4.0, cash_high=660.0, cash_low=651.0)  # 9 > 1.0 x 4
    assert _miss(_bot(), spent) == "range_spent"
    ok = _snap(
        forecast_implied_move=4.0,
        cash_high=657.5,
        cash_low=654.0,
        forecast_expected_vol_state="normal",
    )
    assert _bot().open_criteria(ok) is not None
    # The uniform-gates arm switches both off with falsy values.
    off = _bot(forbid_forecast_vol_state="", max_cash_range_over_implied=0)
    assert off.open_criteria(_snap(forecast_expected_vol_state="expansion")) is not None
    assert off.open_criteria(spent) is not None


def test_flip_within_a_sigma_of_the_center_stands_down():
    assert _miss(_bot(), _snap(gamma_flip=654.0)) == "flip_near"  # 2 < 3.76
    assert _miss(_bot(), _snap(gamma_flip_raw=658.5)) == "flip_near"
    assert _bot().open_criteria(_snap(gamma_flip=651.0)) is not None  # 5 > 3.76


def test_tail_stand_downs():
    assert _miss(_bot(), _snap(vix=16.0, prior_vix=15.0)) == "vol_shock"  # +6.7%
    assert _bot(max_vix_change_frac=0).open_criteria(_snap(vix=16.0, prior_vix=15.0)) is not None
    # The ATM IV percentile gate is OFF by default: it ranked a calendar-
    # annualized IV against a history on another convention.
    assert _bot().open_criteria(_snap(atm_iv_pctile_60d=92.0)) is not None
    assert _bot().open_criteria(_snap(atm_iv_pctile_60d=None)) is not None
    assert _miss(_bot(max_atm_iv_pctile=85), _snap(atm_iv_pctile_60d=92.0)) == "iv_elevated"
    assert _bot(max_atm_iv_pctile=85).open_criteria(_snap(atm_iv_pctile_60d=None)) is not None
    assert (
        _miss(
            _bot(max_atm_iv_pctile=85, require_iv_pctile=True),
            _snap(atm_iv_pctile_60d=None),
        )
        == "no_iv_pctile"
    )
    assert _miss(_bot(), _snap(forecast_is_event_day=True)) == "event_day"
    assert _miss(_bot(event_dates=["2026-08-06"]), _snap()) == "event_day"


def test_sigma_falls_back_to_the_forecast_and_not_to_realized_by_default():
    # No straddle MID (so no straddle sigma) but IV + forecast present.
    bot = _bot()
    snap = _snap(atm_straddle_mid_front=None)
    assert _miss(bot, snap) == "no_straddle"  # the straddle is required as the IMPLIED side
    # With the straddle present but forced to the forecast source order:
    bot = _bot(sigma_sources=[SIGMA_FORECAST])
    assert _miss(bot, _snap()) == "no_sigma"  # no forecast on the snapshot
    sig = _bot(sigma_sources=[SIGMA_FORECAST]).open_criteria(_snap(forecast_implied_move=5.0))
    assert sig is not None and sig.components_at_entry["sigma_source"] == SIGMA_FORECAST
    sig = _bot(sigma_sources=[SIGMA_REALIZED]).open_criteria(_snap())
    assert sig is not None and sig.components_at_entry["sigma_source"] == SIGMA_REALIZED


def test_conviction_floor_is_reachable_on_a_typical_setup_and_binds_on_a_marginal_one():
    bot = _bot()
    sig = bot.open_criteria(_snap())
    assert sig is not None and sig.conviction >= bot.confidence_threshold()
    # Boundary regime (pctile 75) with realized inside the cap but well up
    # it (2.5 bps -> ~0.65 of the straddle): passes every gate, dies at conviction.
    marginal = _snap(net_gex_pctile=75.0, recent_closes=_quiet_closes(656.0, bps=2.5))
    assert 0.6 < marginal.realized_over_straddle() <= 0.85
    assert _miss(_bot(), marginal) == "conviction"


# ======================================================================
# Exits
# ======================================================================


def test_regime_loss_closes_regardless_of_pnl():
    dec = _bot().exit_criteria(_snap(net_gex=-5.0e8), _pos(current_price=-0.80))
    assert dec.should_close and dec.reason == "regime_lost"
    # The tape-only twin never entered on gamma, so it never exits on it.
    dec = _bot(required_regimes="any").exit_criteria(_snap(net_gex=-5.0e8), _pos())
    assert not dec.should_close


def test_flip_entering_the_structure_closes():
    dec = _bot().exit_criteria(_snap(gamma_flip=657.0), _pos())  # 1.0 < 0.5 x 3.76
    assert dec.should_close and dec.reason == "flip_entered"
    dec = _bot().exit_criteria(_snap(gamma_flip=651.0), _pos())
    assert not dec.should_close


def test_short_touch_and_vol_shock_exits():
    dec = _bot().exit_criteria(_snap(spot=655.5), _pos())  # beyond the 656 short put
    assert dec.should_close and dec.reason == "short_touched"
    dec = _bot(short_touch_buffer_sigma=0.5).exit_criteria(_snap(spot=655.5), _pos())
    assert not dec.should_close  # inside the half-sigma buffer
    dec = _bot().exit_criteria(_snap(vix=16.0, prior_vix=15.0), _pos())
    assert dec.should_close and dec.reason == "vol_shock"
    dec = _bot(exit_on_vol_shock=False).exit_criteria(_snap(vix=16.0, prior_vix=15.0), _pos())
    assert not dec.should_close


def test_base_credit_take_and_stop_still_run_underneath():
    # Credit 1.40 received; mark decays to -0.60 => profit 0.80 >= 0.5 x credit.
    dec = _bot().exit_criteria(_snap(), _pos(entry_price=-1.40, current_price=-0.60))
    assert dec.should_close and dec.reason == "credit_take"
    # The stop is HALF the credit (the wing is ~0.8-0.9x, so 1.0x could never
    # fire): mark -2.15 => loss 0.75 >= 0.70 stops; -1.90 => loss 0.50 holds.
    dec = _bot().exit_criteria(_snap(), _pos(entry_price=-1.40, current_price=-2.15))
    assert dec.should_close and dec.reason == "credit_stop"
    dec = _bot().exit_criteria(_snap(), _pos(entry_price=-1.40, current_price=-1.90))
    assert not dec.should_close
    dec = _bot().exit_criteria(_snap(), _pos(entry_price=-1.40, current_price=-1.30))
    assert not dec.should_close
    # ``credit_stop_mult: 0`` (the sweep's no-stop arm) leaves the wing as the stop.
    dec = _bot(credit_stop_mult=0).exit_criteria(
        _snap(), _pos(entry_price=-1.40, current_price=-2.15)
    )
    assert not dec.should_close


# ======================================================================
# Registry / catalog invariants
# ======================================================================


def test_registered_in_research_and_never_funded():
    entry = get(BOT_ID)
    assert entry.stage is Stage.RESEARCH
    assert entry.bot_class == "LongGammaIronFly"
    assert entry.direction_mode == "neutral" and entry.tier == "0DTE"
    assert not entry.is_provisionable
    assert all(s.id != BOT_ID for s in DEFAULT_ROSTER)
    spec = known_specs()[BOT_ID]
    assert spec.enabled is False
    # The shared defaults reach the bot through the catalog...
    assert spec.params["credit_take_frac"] == 0.5
    assert spec.params["premium_stop_grace_seconds"] >= 600
    assert spec.params["confidence_threshold_default"] == 0.50
    assert spec.params["eod_exit_buffer_minutes"] == 5  # hold to the harness cap
    # ...including what the adversarial review changed before the first screen.
    assert spec.params["credit_stop_mult"] == 0.5
    assert spec.params["allowed_magnitude_bands"] == []
    assert spec.params["max_atm_iv_pctile"] == 0
    assert spec.params["max_realized_over_straddle"] == 0.85
    assert spec.params["required_regimes"] == ["positive_strong"]


def test_afternoon_iron_fly_is_the_ungated_twin_as_its_own_entry():
    entry = get("afternoon_iron_fly")
    assert entry.stage is Stage.RESEARCH and entry.bot_class == "AfternoonIronFly"
    assert not entry.is_provisionable
    assert all(s.id != "afternoon_iron_fly" for s in DEFAULT_ROSTER)
    spec = known_specs()["afternoon_iron_fly"]
    assert spec.params["required_regimes"] == "any"
    assert spec.params["min_minutes_since_open"] == 210
    # Same engine, same window, no gamma gate: fires on a negative-gamma tape
    # the gated fly refuses, and never exits on regime loss.
    bot = _bot("afternoon_iron_fly")
    busy_gamma = _snap(net_gex=-1.0e9, net_gex_pctile=10.0)
    sig = bot.open_criteria(busy_gamma)
    assert sig is not None, bot.miss_reasons
    assert sig.strategy_type == "SHORT_IRON_BUTTERFLY"
    assert _miss(_bot(), busy_gamma) == "regime"
    assert not bot.exit_criteria(_snap(net_gex=-5.0e8), _pos()).should_close
    # The quiet-tape gate still binds.
    assert _miss(
        _bot("afternoon_iron_fly"), _snap(recent_closes=_quiet_closes(656.0, bps=20.0))
    ) == ("tail_realized")


# ======================================================================
# Execution sweep grid
# ======================================================================


def test_sweep_grid_resolves_to_the_short_premium_twins():
    from src.tools.tw_execution_sweep import build_specs, grid_for, short_premium_grid

    base = known_specs()[BOT_ID]
    grid = grid_for(base)  # auto: any ShortPremiumBot subclass
    assert [label for label, _ in grid] == [label for label, _ in short_premium_grid()]
    labels = [label for label, _ in grid]
    assert len(set(labels)) == len(labels)
    # The experiment: the gated default against the twins that drop one
    # conditioning at a time, the complement, and the band switched on.
    assert {
        "gated/default",
        "twin/tail_only",
        "twin/clock_only",
        "twin/negative_gamma",
        "gated/band",
        "gated/uniform_gates",
        "gated/no_stop",
        "gated/flat15:40",
        "gated/morning",
        "twin/tail_only_morning",
        "gated/afternoon",
        "twin/tail_only_afternoon",
        "twin/negative_gamma_morning",
        "gated/any_positive",
        "twin/tail_only/no_short_down",
    } <= set(labels)
    specs = build_specs(base, grid)
    assert len({s.id for s in specs}) == len(specs)
    by = {s.display_name: s for s in specs}
    assert by["gated/any_positive"].params["required_regimes"] == [
        "positive_weak",
        "positive_strong",
    ]
    assert by["twin/tail_only/no_short_down"].params["stand_down_short_gamma_down_days"] is True
    assert by["twin/tail_only"].params["required_regimes"] == "any"
    assert by["twin/clock_only"].params["max_realized_over_straddle"] == 0
    assert by["gated/band"].params["allowed_magnitude_bands"] == [
        "chop_range",
        "high_risk_reversal",
    ]
    # The control arms really drop their gates: the clock-only twin fires on
    # a busy, negative-gamma tape the gated default refuses.
    busy = _snap(net_gex=-1.0e9, net_gex_pctile=10.0, recent_closes=_quiet_closes(656.0, bps=20.0))
    cls = get_bot_class(base.strategy_class)
    assert cls(by["twin/clock_only"], ml_state=None).open_criteria(busy) is not None
    assert cls(by["gated/default"], ml_state=None).open_criteria(busy) is None
    # Both windows run as arms: the afternoon arm is a window miss at noon,
    # the morning arm (the gated fly's default) fires there.
    assert by["gated/afternoon"].params["min_minutes_since_open"] == 210
    assert by["twin/tail_only_afternoon"].params["required_regimes"] == "any"
    assert by["gated/morning"].params["min_minutes_since_open"] == 60
    pm = cls(by["gated/afternoon"], ml_state=None)
    assert _miss(pm, _snap(timestamp=MID_12ET)) == "window"
    am = cls(by["gated/morning"], ml_state=None)
    assert am.open_criteria(_snap(timestamp=MID_12ET)) is not None, am.miss_reasons
    # Every config keeps the shared defaults it did not override.
    assert all(s.params["credit_stop_mult"] in (0.5, 0) for s in specs)
    assert all(s.params["premium_stop_grace_seconds"] == 600 for s in specs)
