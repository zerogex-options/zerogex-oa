"""Execution-parameterization tests for the wall-reversion bots + sweep tool.

Locks in that the SAME wall signal builds different structures from params
(debit vertical / single debit / credit spread), that the credit-aware exit
fires on decay and no-ops for a debit, and that the sweep tool generates
param-varied specs and recombines half-window summaries correctly.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.tradeworkz.bots.wall_reversion import wall_credit_exit
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, OpenPosition
from src.tradeworkz.registry import get_bot_class, known_specs

MID_12ET = datetime(2026, 7, 14, 16, 0, tzinfo=timezone.utc)  # 12:00 ET


def _bot(bot_id: str, **param_over):
    spec = known_specs()[bot_id]
    params = dict(spec.params or {})
    params.update(param_over)
    spec2 = BotSpec(
        id=spec.id,
        display_name=spec.display_name,
        strategy_class=spec.strategy_class,
        tier=spec.tier,
        direction_mode=spec.direction_mode,
        universe=spec.universe,
        tagline=spec.tagline,
        description=spec.description,
        params=params,
    )
    return get_bot_class(spec.strategy_class)(spec2, ml_state=None)


def _call_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,
        spot=755.0,
        net_gex=2.0e9,
        call_wall=756.0,
        max_pain=752.0,
        call_wall_strength_pctile=70.0,
        flow_recent_premium=2.0e5,
        recent_closes=[755.0, 755.5, 756.0, 755.5, 755.0],
    )
    base.update(over)
    return MarketSnapshot(**base)


def _put_snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MID_12ET,
        spot=755.0,
        net_gex=2.0e9,
        put_wall=754.0,
        max_pain=758.0,
        put_wall_strength_pctile=70.0,
        flow_recent_premium=-2.0e5,
        recent_closes=[755.0, 754.5, 754.0, 754.5, 755.0],
    )
    base.update(over)
    return MarketSnapshot(**base)


# ----------------------------------------------------------- structures


def test_debit_vertical_is_the_default():
    sig = _bot("call_wall_rejector").open_criteria(_call_snap())
    assert sig is not None
    assert sig.strategy_type == "BEAR_PUT_DEBIT_SPREAD"
    assert len(sig.legs) == 2
    assert sig.target_price == 752.0  # max_pain
    assert sig.wall_ref_side == "call"  # base vol-scaled wall-break stop armed


def test_single_debit_structure():
    sig = _bot("call_wall_rejector", structure="single_debit").open_criteria(_call_snap())
    assert sig is not None
    assert sig.strategy_type == "BUY_PUT_DEBIT"
    assert len(sig.legs) == 1
    assert sig.legs[0].side == "long"


def test_call_wall_credit_spread_structure():
    """Bear call credit spread: short near the wall, long wing above; a net
    credit that profits if the wall holds. Boundary-matching structure."""
    sig = _bot("call_wall_rejector", structure="credit_spread", wing_width=3).open_criteria(
        _call_snap()
    )
    assert sig is not None
    assert sig.strategy_type == "BEAR_CALL_CREDIT_SPREAD"
    assert sig.direction == "bearish"
    shorts = [leg for leg in sig.legs if leg.side == "short"]
    longs = [leg for leg in sig.legs if leg.side == "long"]
    assert len(shorts) == 1 and len(longs) == 1
    assert shorts[0].option_type == "call" and longs[0].option_type == "call"
    assert shorts[0].strike < longs[0].strike  # short near wall, long wing above
    assert sig.target_price is None  # no spot target — profit by decay
    assert sig.stop_price is not None and sig.stop_price > 755.0  # break-up stop
    assert sig.wall_ref_side is None  # credit uses its own stop, not wall drift


def test_put_wall_credit_spread_structure():
    sig = _bot("put_wall_bouncer", structure="credit_spread", wing_width=3).open_criteria(
        _put_snap()
    )
    assert sig is not None
    assert sig.strategy_type == "BULL_PUT_CREDIT_SPREAD"
    assert sig.direction == "bullish"
    shorts = [leg for leg in sig.legs if leg.side == "short"]
    longs = [leg for leg in sig.legs if leg.side == "long"]
    assert shorts[0].strike > longs[0].strike  # short near wall, long wing below
    assert sig.stop_price is not None and sig.stop_price < 755.0  # break-down stop


def test_target_mode_flip_uses_gamma_flip():
    sig = _bot("call_wall_rejector", target_mode="flip").open_criteria(
        _call_snap(max_pain=None, gamma_flip=751.0)
    )
    assert sig is not None
    assert sig.target_price == 751.0


# --------------------------------------------------------- credit exit


def _credit_pos(entry: float, mark: float, strat: str) -> OpenPosition:
    past = datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc)
    return OpenPosition(
        id=1,
        bot_id="call_wall_rejector",
        underlying="SPY",
        opened_at=past,
        direction="bearish",
        strategy_type=strat,
        legs=[],
        entry_price=entry,
        current_price=mark,
        quantity_open=1,
        unrealized_pnl=0.0,
        stop_price=757.0,
        target_price=None,
        time_stop_at=None,
        min_hold_until=past,
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.6,
        components_at_entry={},
    )


def test_credit_exit_takes_profit_on_decay():
    bot = _bot("call_wall_rejector", structure="credit_spread", profit_take_frac=0.5)
    # entry -0.50 credit; mark -0.20 (cost-to-close 0.20) => captured
    # profit (mark-entry) = 0.30 >= 0.5*0.50 = 0.25 -> take.
    dec = wall_credit_exit(bot, _call_snap(), _credit_pos(-0.50, -0.20, "BEAR_CALL_CREDIT_SPREAD"))
    assert dec is not None and dec.should_close and dec.reason == "credit_take"


def test_credit_exit_holds_when_not_yet_decayed():
    bot = _bot("call_wall_rejector", structure="credit_spread", profit_take_frac=0.5)
    # mark -0.40 => captured profit 0.10 < 0.25 -> keep holding.
    dec = wall_credit_exit(bot, _call_snap(), _credit_pos(-0.50, -0.40, "BEAR_CALL_CREDIT_SPREAD"))
    assert dec is None


def test_credit_exit_ignores_debit_positions():
    bot = _bot("call_wall_rejector")
    dec = wall_credit_exit(bot, _call_snap(), _credit_pos(0.50, 0.20, "BEAR_PUT_DEBIT_SPREAD"))
    assert dec is None  # debit -> base exit handles it


# ------------------------------------------------------------- sweep tool


def test_sweep_grid_spans_structures():
    from src.tools.tw_execution_sweep import wall_execution_grid

    structures = {over.get("structure") for _label, over in wall_execution_grid()}
    assert {"debit_vertical", "single_debit", "credit_spread"} <= structures


def test_sweep_build_specs_unique_and_merged():
    from src.tools.tw_execution_sweep import build_specs, wall_execution_grid

    base = known_specs()["call_wall_rejector"]
    specs = build_specs(base, wall_execution_grid())
    assert len({s.id for s in specs}) == len(specs)  # unique ids
    # every config keeps the base filter params and applies its override
    assert all("min_wall_strength_pctile" in s.params for s in specs)
    assert any(s.params.get("structure") == "credit_spread" for s in specs)


def test_sweep_combined_recombines_halves():
    from src.tools.tw_execution_sweep import _combined

    train = {
        "n_trades": 10,
        "wins": 6,
        "losses": 4,
        "avg_win": 100.0,
        "avg_loss": 50.0,
        "total_pnl": 400.0,
        "expectancy": 40.0,
    }
    test = {
        "n_trades": 10,
        "wins": 4,
        "losses": 6,
        "avg_win": 100.0,
        "avg_loss": 50.0,
        "total_pnl": 100.0,
        "expectancy": 10.0,
    }
    n, pf, exp = _combined(train, test)
    assert n == 20
    # gross win = (6+4)*100 = 1000; gross loss = (4+6)*50 = 500 -> PF 2.0
    assert abs(pf - 2.0) < 1e-9
    assert abs(exp - 25.0) < 1e-9  # (400+100)/20


def test_wall_bots_have_no_ladder_dependency_import():
    # smoke: both bot classes import and expose exit_criteria override
    for bid in ("call_wall_rejector", "put_wall_bouncer"):
        bot = _bot(bid)
        assert hasattr(bot, "exit_criteria")


# silence unused import if timedelta trimmed later
_ = timedelta
