"""Any capped-risk structure: ``params['structure_override']`` re-expresses a
bot's signal; nothing uncapped can open.

Decided 2026-11-12: the fleet stays open to any option structure whose worst
case is capped. These lock in what each structure builds, that a structure
only re-expresses a signal of its own family, and that the open paths refuse a
naked short whatever built it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, Leg, TradeSignal
from src.tradeworkz.sizing import is_capped_risk, structure_max_loss_per_share
from src.tradeworkz.structures import DIRECTIONAL, LONG_VOL, SHORT_VOL, reexpress

EXP = "2026-11-13"
TS = datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)


def _snap(u: str = "NVDA", spot: float = 183.1, inc: float = 2.5) -> MarketSnapshot:
    return MarketSnapshot(underlying=u, timestamp=TS, spot=spot, strike_increment=inc)


def _bot(**params) -> BaseBot:
    spec = BotSpec(
        id="t_bot",
        display_name="T",
        strategy_class="X",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="",
        description="",
        params=params,
    )
    return BaseBot(spec)


def _leg(right, side, strike, exp=EXP):
    return Leg(
        option_symbol=f"NVDA X{right}{strike}",
        side=side,
        option_type=right,
        strike=strike,
        expiration=exp,
    )


def _directional(direction="bullish", target=None):
    right = "call" if direction == "bullish" else "put"
    return TradeSignal(
        bot_id="t_bot",
        underlying="NVDA",
        direction=direction,
        strategy_type="BULL_CALL_DEBIT_SPREAD",
        legs=[_leg(right, "long", 182.5), _leg(right, "short", 187.5)],
        entry_price=0.0,
        conviction=0.7,
        target_price=target,
        stop_price=180.0,
        time_stop_at=TS + timedelta(minutes=60),
        rationale="native",
        components_at_entry={"k": 1},
    )


def _neutral(short_vol: bool):
    legs = (
        [
            _leg("put", "long", 177.5),
            _leg("put", "short", 180),
            _leg("call", "short", 185),
            _leg("call", "long", 187.5),
        ]
        if short_vol
        else [_leg("call", "long", 182.5), _leg("put", "long", 182.5)]
    )
    return TradeSignal(
        bot_id="t_bot",
        underlying="NVDA",
        direction="neutral",
        strategy_type="SHORT_IRON_CONDOR" if short_vol else "LONG_STRADDLE",
        legs=legs,
        entry_price=0.0,
        conviction=0.7,
    )


def _shape(sig):
    return [(lg.option_type, lg.side, lg.strike) for lg in sig.legs]


def _dicts(sig):
    return [
        {"option_type": lg.option_type, "side": lg.side, "strike": lg.strike} for lg in sig.legs
    ]


# ----------------------------------------------------------------------
# Capped risk
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "legs",
    [
        [("call", "long", 100)],
        [("call", "long", 100), ("call", "short", 105)],
        [("put", "short", 100), ("put", "long", 95)],
        [("put", "long", 95), ("put", "short", 100), ("call", "short", 105), ("call", "long", 110)],
        [
            ("call", "long", 95),
            ("call", "short", 100),
            ("call", "short", 100),
            ("call", "long", 105),
        ],
        [("call", "long", 100), ("put", "long", 100)],
    ],
)
def test_spreads_condors_flies_and_long_premium_are_capped(legs):
    assert is_capped_risk([{"option_type": t, "side": s, "strike": k} for t, s, k in legs])


@pytest.mark.parametrize(
    "legs",
    [
        [("put", "short", 100)],  # naked put
        [("call", "short", 100), ("put", "short", 100)],  # short straddle
        [("call", "long", 105), ("put", "short", 95)],  # risk reversal
        [("call", "long", 100), ("call", "short", 105), ("call", "short", 105)],  # 1x2 ratio
        [("call", "sideways", 100)],  # unreadable fails closed
    ],
)
def test_naked_shorts_are_not_capped(legs):
    assert not is_capped_risk([{"option_type": t, "side": s, "strike": k} for t, s, k in legs])


# ----------------------------------------------------------------------
# evaluate_open
# ----------------------------------------------------------------------


@pytest.mark.parametrize("override", [None, "", "native", "NONE"])
def test_no_override_is_exactly_open_criteria(monkeypatch, override):
    bot = _bot(**({} if override is None else {"structure_override": override}))
    native = _directional()
    monkeypatch.setattr(bot, "open_criteria", lambda snap: native)
    assert bot.evaluate_open(_snap()) is native


def test_no_signal_stays_no_signal(monkeypatch):
    bot = _bot(structure_override="credit_vertical")
    monkeypatch.setattr(bot, "open_criteria", lambda snap: None)
    assert bot.evaluate_open(_snap()) is None
    assert bot.miss_reasons == {}


def test_a_structure_from_the_wrong_family_drops_the_signal(monkeypatch):
    bot = _bot(structure_override="iron_condor")
    monkeypatch.setattr(bot, "open_criteria", lambda snap: _directional())
    assert bot.evaluate_open(_snap()) is None
    assert bot.miss_reasons == {"structure_unfit:iron_condor": 1}


def test_an_unknown_structure_drops_the_signal(monkeypatch):
    bot = _bot(structure_override="naked_put")
    monkeypatch.setattr(bot, "open_criteria", lambda snap: _directional())
    assert bot.evaluate_open(_snap()) is None
    assert bot.miss_reasons == {"structure_unfit:naked_put": 1}


# ----------------------------------------------------------------------
# What each structure builds (NVDA 183.1 on a 2.50 grid -> ATM 182.5)
# ----------------------------------------------------------------------


def test_directional_structures_build_on_the_listed_grid():
    snap = _snap()
    bull, bear = _directional("bullish"), _directional("bearish")
    cases = {
        ("long_single", "bullish"): ([("call", "long", 182.5)], "LONG_CALL"),
        ("long_single", "bearish"): ([("put", "long", 182.5)], "LONG_PUT"),
        ("debit_vertical", "bullish"): (
            [("call", "long", 182.5), ("call", "short", 187.5)],
            "BULL_CALL_DEBIT_SPREAD",
        ),
        ("debit_vertical", "bearish"): (
            [("put", "long", 182.5), ("put", "short", 177.5)],
            "BEAR_PUT_DEBIT_SPREAD",
        ),
        ("credit_vertical", "bullish"): (
            [("put", "short", 182.5), ("put", "long", 177.5)],
            "BULL_PUT_CREDIT_SPREAD",
        ),
        ("credit_vertical", "bearish"): (
            [("call", "short", 182.5), ("call", "long", 187.5)],
            "BEAR_CALL_CREDIT_SPREAD",
        ),
    }
    for (kind, direction), (shape, stype) in cases.items():
        sig = reexpress(_bot(), snap, bull if direction == "bullish" else bear, kind)
        assert _shape(sig) == shape, kind
        assert sig.strategy_type == stype
        assert all(lg.expiration == EXP for lg in sig.legs)
        assert is_capped_risk(_dicts(sig))


def test_offset_moves_the_near_strike_away_from_the_money():
    sig = reexpress(_bot(structure_offset_increments=1), _snap(), _directional(), "long_single")
    assert _shape(sig) == [("call", "long", 185.0)]
    sig = reexpress(
        _bot(structure_offset_increments=1, structure_width_increments=1),
        _snap(),
        _directional(),
        "credit_vertical",
    )
    assert _shape(sig) == [("put", "short", 180.0), ("put", "long", 177.5)]


def test_a_directional_butterfly_puts_its_body_on_the_target():
    sig = reexpress(_bot(), _snap(), _directional(target=187.4), "butterfly")
    assert _shape(sig) == [
        ("call", "long", 182.5),
        ("call", "short", 187.5),
        ("call", "short", 187.5),
        ("call", "long", 192.5),
    ]
    assert sig.strategy_type == "LONG_CALL_BUTTERFLY"
    # No usable target: one width out in the trade's direction.
    sig = reexpress(_bot(), _snap(), _directional("bearish", target=None), "butterfly")
    assert [lg.strike for lg in sig.legs] == [172.5, 177.5, 177.5, 182.5]
    assert all(lg.option_type == "put" for lg in sig.legs)
    # Its worst case is the debit, never more.
    assert structure_max_loss_per_share(_dicts(sig), 0.5) == pytest.approx(0.5)


def test_neutral_signals_re_express_within_their_volatility_side():
    snap = _snap()
    short_vol, long_vol = _neutral(True), _neutral(False)
    condor = reexpress(_bot(), snap, short_vol, "iron_condor")
    assert [lg.strike for lg in condor.legs] == [172.5, 177.5, 187.5, 192.5]
    fly = reexpress(_bot(), snap, short_vol, "iron_fly")
    assert [lg.strike for lg in fly.legs] == [177.5, 182.5, 182.5, 187.5]
    assert _shape(reexpress(_bot(), snap, short_vol, "long_butterfly"))[1:3] == [
        ("call", "short", 182.5),
        ("call", "short", 182.5),
    ]
    strangle = reexpress(_bot(), snap, long_vol, "long_strangle")
    assert _shape(strangle) == [("call", "long", 187.5), ("put", "long", 177.5)]
    straddle = reexpress(_bot(), snap, long_vol, "long_straddle")
    assert _shape(straddle) == [("call", "long", 182.5), ("put", "long", 182.5)]
    # Selling premium never becomes buying it, nor the reverse.
    assert reexpress(_bot(), snap, short_vol, "long_straddle") is None
    assert reexpress(_bot(), snap, long_vol, "iron_fly") is None


def test_every_structure_is_capped_and_keeps_the_bots_own_levels():
    snap = _snap()
    for kind in DIRECTIONAL + SHORT_VOL + LONG_VOL:
        native = _directional(target=187.4) if kind in DIRECTIONAL else _neutral(kind in SHORT_VOL)
        sig = reexpress(_bot(), snap, native, kind)
        assert sig is not None, kind
        assert is_capped_risk(_dicts(sig)), kind
        assert sig.direction == native.direction
        assert sig.conviction == native.conviction
        assert sig.target_price == native.target_price
        assert sig.stop_price == native.stop_price
        assert sig.time_stop_at == native.time_stop_at
        assert sig.components_at_entry["structure"] == kind
        assert sig.components_at_entry["native_strategy_type"] == native.strategy_type
        assert len({lg.expiration for lg in sig.legs}) == 1


def test_a_credit_spread_sizes_on_its_wing_not_its_credit():
    sig = reexpress(_bot(), _snap(), _directional(), "credit_vertical")
    # $5 wide (2 x 2.50) for a $1.00 credit: $4 at risk per share.
    assert structure_max_loss_per_share(_dicts(sig), -1.0) == pytest.approx(4.0)


# ----------------------------------------------------------------------
# The open paths
# ----------------------------------------------------------------------


def test_the_harness_trades_the_configured_structure(monkeypatch):
    from dataclasses import replace

    from src.tradeworkz import backtest as bt
    from src.tradeworkz.chain import FitResult
    from src.tradeworkz.registry import known_specs

    spec = known_specs()["charm_close_magnet"]
    spec = replace(spec, params={**spec.params, "structure_override": "credit_vertical"})
    runner = bt._BotRunner(spec, slippage_pct=0.0, tolerance_min=10)
    monkeypatch.setattr(runner.bot, "open_criteria", lambda snap: _directional())
    monkeypatch.setattr(
        bt, "fit_legs_to_chain", lambda c, u, legs, as_of=None: FitResult(legs, "no_chain")
    )
    monkeypatch.setattr(bt, "historical_spread_price", lambda conn, legs, **k: -1.0)
    snap = _snap()
    snap.trade_bias_code = None
    runner._maybe_open(conn=None, u="NVDA", snap=snap, now_utc=TS)
    pos = runner.open_by_underlying["NVDA"]
    assert pos.strategy_type == "BULL_PUT_CREDIT_SPREAD"
    assert [(lg["side"], lg["strike"]) for lg in pos.legs] == [("short", 182.5), ("long", 177.5)]


def test_the_harness_refuses_an_uncapped_structure(monkeypatch):
    from src.tradeworkz import backtest as bt
    from src.tradeworkz.chain import FitResult
    from src.tradeworkz.registry import known_specs

    runner = bt._BotRunner(known_specs()["charm_close_magnet"], slippage_pct=0.0, tolerance_min=10)
    naked = _directional()
    naked.legs = [_leg("put", "short", 180)]
    monkeypatch.setattr(runner.bot, "open_criteria", lambda snap: naked)
    monkeypatch.setattr(
        bt, "fit_legs_to_chain", lambda c, u, legs, as_of=None: FitResult(legs, "no_chain")
    )
    snap = _snap()
    snap.trade_bias_code = None
    runner._maybe_open(conn=None, u="NVDA", snap=snap, now_utc=TS)
    assert runner.entry_rejects == {"uncapped_risk": 1}


def test_live_open_refuses_an_uncapped_structure(monkeypatch):
    from src.tradeworkz import reconciler
    from src.tradeworkz.chain import FitResult
    from src.tradeworkz.models import BotCapital

    class _Cur:
        def __init__(self):
            self._last = ""
            self.sqls = []

        def execute(self, sql, params=None):
            self._last = sql
            self.sqls.append(sql)

        def fetchone(self):
            if "pg_try_advisory_xact_lock" in self._last:
                return (True,)
            if "COUNT(1)" in self._last:
                return (0,)
            return None

        def fetchall(self):
            return []

    class _Conn:
        def __init__(self):
            self.cur = _Cur()

        def cursor(self):
            return self.cur

    naked = _directional()
    naked.legs = [_leg("put", "short", 180)]
    monkeypatch.setattr(
        reconciler, "fit_legs_to_chain", lambda c, u, legs, as_of=None: FitResult(legs, "no_chain")
    )
    priced = []
    monkeypatch.setattr(reconciler, "spread_price", lambda *a, **k: priced.append(1) or -1.0)
    conn = _Conn()
    cap = BotCapital("t_bot", 1e5, 1e5, 1e5, 0.06, 0.5, 0.02)
    assert reconciler.open_position(conn, naked, cap, 1.0) is None
    assert priced == []
    assert not any("INSERT INTO tw_positions" in s for s in conn.cur.sqls)
