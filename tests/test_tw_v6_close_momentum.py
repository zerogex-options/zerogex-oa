"""Entry / exit logic for HedgingDemandCloseMomentum (v6).

The one clock-to-clock direction structure the research record says is
untested here: the cash-session return at 15:00 ET, traded into the bell, only
when dealers are short gamma. Tests build a ``MarketSnapshot`` directly (no DB)
and lock in: it fires the right vertical in the day's direction; the gamma
sign is a param so the unconditional and complement controls are the same
bot; every gate abstains with its own reason; the momentum-lost exit fires;
and the catalog / registry invariants hold.

All timestamps are ET -> UTC for August (EDT, UTC-4).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from src.strategies import Stage, get
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import OpenPosition
from src.tradeworkz.registry import DEFAULT_ROSTER, get_bot_class, known_specs

# Thursday 2026-08-06.
T_1505ET = datetime(2026, 8, 6, 19, 5, tzinfo=timezone.utc)  # 55 min to close
T_1430ET = datetime(2026, 8, 6, 18, 30, tzinfo=timezone.utc)  # 90 min to close
T_1540ET = datetime(2026, 8, 6, 19, 40, tzinfo=timezone.utc)  # 20 min to close

BOT_ID = "hedging_demand_close_momentum"


def _bot(**param_over):
    spec = known_specs()[BOT_ID]
    if param_over:
        spec.params = {**spec.params, **param_over}
    return get_bot_class(spec.strategy_class)(spec, ml_state=None)


def _snap(**over) -> MarketSnapshot:
    """An up day of ~1 implied sigma in negative gamma at 15:05 ET.

    VIX 16 -> implied daily fraction 0.16 / sqrt(252) ~= 1.008%; cash open
    650 -> spot 656.5 is +1.0%, i.e. ~0.99 sigma. First half hour closed up.
    """
    base = dict(
        underlying="SPY",
        timestamp=T_1505ET,
        spot=656.5,
        net_gex=-1.2e9,
        net_gex_pctile=20.0,
        vix=16.0,
        cash_open=650.0,
        cash_high=657.0,
        cash_low=649.5,
        first_half_hour_close=652.0,
        recent_closes=[656.0 + 0.05 * (i % 3) for i in range(30)],
    )
    base.update(over)
    return MarketSnapshot(**base)


def _pos(direction: str = "bullish", cash_open: float = 650.0) -> OpenPosition:
    now = datetime.now(timezone.utc)
    return OpenPosition(
        id=1,
        bot_id=BOT_ID,
        underlying="SPY",
        opened_at=now - timedelta(minutes=10),
        direction=direction,
        strategy_type="BULL_CALL_DEBIT_SPREAD",
        legs=[],
        entry_price=1.0,
        current_price=1.0,
        quantity_open=1,
        unrealized_pnl=0.0,
        stop_price=None,
        target_price=None,
        time_stop_at=now + timedelta(minutes=45),
        min_hold_until=now - timedelta(minutes=9),
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.6,
        components_at_entry={"cash_open": cash_open},
    )


def _miss(bot, snap) -> str:
    assert bot.open_criteria(snap) is None
    assert len(bot.miss_reasons) == 1, bot.miss_reasons
    return next(iter(bot.miss_reasons))


# ======================================================================
# Fires
# ======================================================================


def test_fires_a_bull_call_debit_vertical_on_an_up_day_in_short_gamma():
    bot = _bot()
    sig = bot.open_criteria(_snap())
    assert sig is not None, bot.miss_reasons
    assert sig.direction == "bullish"
    assert sig.strategy_type == "BULL_CALL_DEBIT_SPREAD"
    by = {(lg.option_type, lg.side): lg for lg in sig.legs}
    assert by[("call", "long")].strike == 656.0  # ATM on the $1 grid (656.5 rounds down)
    # remaining sigma from VIX: 1.008% x 656.5 x sqrt(55/390) ~= $2.49 -> 0.5 x = 1.2 -> 1 inc
    assert by[("call", "short")].strike == 657.0
    assert sig.target_price is None and sig.stop_price is None
    comps = sig.components_at_entry
    assert 0.9 < comps["day_return_sigmas"] < 1.1
    assert comps["first_half_hour_return"] is not None


def test_down_day_fires_the_bear_put_vertical():
    sig = _bot().open_criteria(
        _snap(spot=643.5, cash_high=650.5, cash_low=643.0, first_half_hour_close=648.0)
    )
    assert sig is not None
    assert sig.direction == "bearish" and sig.strategy_type == "BEAR_PUT_DEBIT_SPREAD"
    by = {(lg.option_type, lg.side): lg for lg in sig.legs}
    assert by[("put", "long")].strike == 644.0 and by[("put", "short")].strike == 643.0


def test_credit_variant_sells_the_atm_option_the_move_leaves_behind():
    sig = _bot(structure="credit_vertical").open_criteria(_snap())
    assert sig is not None and sig.strategy_type == "BULL_PUT_CREDIT_SPREAD"
    by = {(lg.option_type, lg.side): lg for lg in sig.legs}
    assert by[("put", "short")].strike == 656.0 and by[("put", "long")].strike == 655.0


def test_holds_to_the_bell():
    from src.tradeworkz.bots import base as bot_base

    bot_base.set_backtest_clock(lambda: T_1505ET)
    try:
        sig = _bot().open_criteria(_snap())
    finally:
        bot_base.set_backtest_clock(None)
    assert sig is not None
    assert sig.time_stop_at == T_1505ET + timedelta(minutes=50)  # 55 - 5 buffer -> 15:55 ET


def test_forecast_implied_move_is_preferred_over_vix():
    # Forecast says the daily sigma is $13 (2%): a 1% day is only half a sigma -> flat.
    bot = _bot()
    assert _miss(bot, _snap(forecast_implied_move=13.5)) == "day_flat"
    sig = _bot().open_criteria(_snap(forecast_implied_move=6.5))
    assert (
        sig is not None and abs(sig.components_at_entry["implied_daily_frac"] - 6.5 / 656.5) < 1e-12
    )


# ======================================================================
# Gates
# ======================================================================


def test_window_is_the_last_hour_only():
    assert _miss(_bot(), _snap(timestamp=T_1430ET)) == "window"
    assert _miss(_bot(), _snap(timestamp=T_1540ET)) == "window"


def test_gamma_sign_is_the_papers_condition_and_a_param():
    assert _miss(_bot(), _snap(net_gex=1.5e9, net_gex_pctile=80.0)) == "regime"
    assert _miss(_bot(), _snap(net_gex=None)) == "no_net_gex"
    assert _bot(required_gamma_sign="any").open_criteria(_snap(net_gex=1.5e9)) is not None
    assert _bot(required_gamma_sign="positive").open_criteria(_snap(net_gex=1.5e9)) is not None
    assert _miss(_bot(required_gamma_sign="positive"), _snap()) == "regime"
    # negative_strong only, by percentile
    assert _miss(_bot(max_net_gex_pctile=25.0), _snap(net_gex_pctile=40.0)) == "regime"
    assert _bot(max_net_gex_pctile=25.0).open_criteria(_snap(net_gex_pctile=20.0)) is not None


def test_day_move_gates():
    assert _miss(_bot(), _snap(cash_open=None)) == "no_cash_open"
    assert _miss(_bot(), _snap(vix=None)) == "no_implied"
    assert _miss(_bot(), _snap(spot=651.0)) == "day_flat"  # +0.15% ~ 0.15 sigma
    assert _miss(_bot(), _snap(spot=675.0)) == "day_exhausted"  # +3.8% ~ 3.8 sigma


def test_first_half_hour_agreement_is_optional():
    disagree = _snap(first_half_hour_close=648.0)  # down first half hour, up day
    assert _bot().open_criteria(disagree) is not None
    assert _miss(_bot(require_first_half_hour_agreement=True), disagree) == "fhh_disagrees"
    assert (
        _miss(_bot(require_first_half_hour_agreement=True), _snap(first_half_hour_close=None))
        == "no_first_half_hour"
    )


def test_wall_obstacle_check_is_off_by_default():
    snap = _snap(call_wall=657.0)
    assert _bot().open_criteria(snap) is not None
    assert _miss(_bot(wall_room_sigma_mult=1.0), snap) == "wall_in_the_way"


def test_bias_veto_is_disabled_for_the_screen_but_honored_when_enabled():
    opposed = _snap(
        trade_bias_code="SELL_RIPS", trade_bias_trend="bearish", trade_bias_confidence=60.0
    )
    assert _bot().open_criteria(opposed) is not None
    assert _miss(_bot(bias_veto_enabled=True), opposed) == "bias_veto"


def test_conviction_floor_is_reachable_and_binds_on_a_half_sigma_day():
    bot = _bot()
    sig = bot.open_criteria(_snap())
    assert sig is not None and sig.conviction >= bot.confidence_threshold()
    # Exactly half a sigma, mid-range percentile, first half hour disagreeing.
    marginal = _snap(spot=653.3, net_gex_pctile=45.0, first_half_hour_close=649.0)
    assert _miss(_bot(), marginal) == "conviction"


# ======================================================================
# Exits
# ======================================================================


def test_momentum_lost_closes_on_a_cross_back_through_the_open():
    dec = _bot().exit_criteria(_snap(spot=649.8), _pos("bullish"))
    assert dec.should_close and dec.reason == "momentum_lost"
    dec = _bot().exit_criteria(_snap(spot=651.0), _pos("bearish"))
    assert dec.should_close and dec.reason == "momentum_lost"
    dec = _bot().exit_criteria(_snap(spot=655.0), _pos("bullish"))
    assert not dec.should_close
    dec = _bot(exit_on_momentum_loss=False).exit_criteria(_snap(spot=649.8), _pos("bullish"))
    assert not dec.should_close


# ======================================================================
# Registry / catalog invariants
# ======================================================================


def test_registered_in_research_and_never_funded():
    entry = get(BOT_ID)
    assert entry.stage is Stage.RESEARCH
    assert entry.bot_class == "HedgingDemandCloseMomentum"
    assert entry.family.value == "trend" and entry.tier == "0DTE"
    assert not entry.is_provisionable
    assert all(s.id != BOT_ID for s in DEFAULT_ROSTER)
    spec = known_specs()[BOT_ID]
    assert spec.enabled is False
    assert spec.params["bias_veto_enabled"] is False
    assert spec.params["premium_stop_grace_seconds"] >= 600


# ======================================================================
# Execution sweep grid
# ======================================================================


def test_sweep_grid_is_the_gamma_sign_experiment():
    from src.tools.tw_execution_sweep import build_specs, close_momentum_grid, grid_for

    base = known_specs()[BOT_ID]
    grid = grid_for(base)
    assert [label for label, _ in grid] == [label for label, _ in close_momentum_grid()]
    signs = {over.get("required_gamma_sign") for _, over in grid if "required_gamma_sign" in over}
    assert signs == {"negative", "any", "positive"}
    specs = build_specs(base, grid)
    assert len({s.id for s in specs}) == len(specs)
    by = {s.display_name: s for s in specs}
    assert by["positive/credit"].params["structure"] == "credit_vertical"
    assert by["negative/debit/fhh"].params["require_first_half_hour_agreement"] is True
    # A wall bot still gets the wall grid.
    from src.tools.tw_execution_sweep import wall_execution_grid

    wall = grid_for(known_specs()["call_wall_rejector"])
    assert [label for label, _ in wall] == [label for label, _ in wall_execution_grid()]
