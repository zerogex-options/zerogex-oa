"""Any analytics symbol can trade: the universe, and the contract fit.

``TRADEWORKZ_UNIVERSE='*'`` makes every symbol the analytics engine models
bot-eligible. Stocks were kept out because a bot names its contract by
arithmetic — today's date, a $1 grid — and a stock lists neither (no
expiration most weekdays, $2.50 strikes). ``src/tradeworkz/chain.py`` fits
each leg onto a listed contract instead; these lock in what it may and may
not do.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.tradeworkz import config as tw_config
from src.tradeworkz.chain import (
    MAX_EXPIRATION_ROLL_DAYS,
    FitResult,
    fetch_listed_chain,
    fit_legs,
    strike_step_from_strikes,
)

TUE = date(2026, 11, 10)
FRI = date(2026, 11, 13)


def _leg(right: str, strike: float, exp: date, side: str = "long", root: str = "NVDA") -> dict:
    return {
        "option_symbol": f"{root} {exp.strftime('%y%m%d')}{right[0].upper()}{strike:g}",
        "side": side,
        "option_type": "call" if right.lower().startswith("c") else "put",
        "strike": strike,
        "expiration": exp.isoformat(),
        "delta": None,
    }


def _chain(root: str, exps, strikes, rights=("C", "P")):
    return {
        e: {
            r: {round(k, 4): f"{root} {e.strftime('%y%m%d')}{r}{k:g}" for k in strikes}
            for r in rights
        }
        for e in exps
    }


# A Friday-only stock on a $2.50 grid around 183.
NVDA = _chain("NVDA", [FRI, FRI + timedelta(days=7)], [175 + 2.5 * i for i in range(9)])
# A daily ETF on a $1 grid.
SPY = _chain("SPY", [TUE, TUE + timedelta(days=1)], list(range(590, 611)))


# ----------------------------------------------------------------------
# Universe
# ----------------------------------------------------------------------


def test_wildcard_is_every_analytics_symbol_minus_futures(monkeypatch):
    from src import config as app_config

    monkeypatch.setattr(app_config, "ANALYTICS_UNDERLYINGS", "SPY,SPX,QQQ,NDX,NVDA,ES")
    monkeypatch.setattr(tw_config, "UNIVERSE", "*")
    assert tw_config.fleet_universes() == ("SPY", "SPX", "QQQ", "NDX", "NVDA")


def test_wildcard_can_add_a_symbol_analytics_does_not_cover(monkeypatch):
    from src import config as app_config

    monkeypatch.setattr(app_config, "ANALYTICS_UNDERLYINGS", "SPY,QQQ")
    monkeypatch.setattr(tw_config, "UNIVERSE", "*, iwm ,SPY")
    assert tw_config.fleet_universes() == ("SPY", "QQQ", "IWM")


def test_wildcard_falls_back_to_the_single_analytics_symbol(monkeypatch):
    from src import config as app_config

    monkeypatch.setattr(app_config, "ANALYTICS_UNDERLYINGS", "")
    monkeypatch.setattr(app_config, "ANALYTICS_UNDERLYING", "QQQ")
    monkeypatch.setattr(tw_config, "UNIVERSE", "ALL")
    assert tw_config.fleet_universes() == ("QQQ",)


def test_wildcard_folds_a_tradestation_symbol_back_to_its_alias(monkeypatch):
    from src import config as app_config

    monkeypatch.setenv("SYMBOL_ALIASES", "SPX=$SPXW.X")
    monkeypatch.setattr(app_config, "ANALYTICS_UNDERLYINGS", "SPY,$SPXW.X")
    monkeypatch.setattr(tw_config, "UNIVERSE", "*")
    assert tw_config.fleet_universes() == ("SPY", "SPX")


def test_an_explicit_list_is_still_honoured(monkeypatch):
    monkeypatch.setattr(tw_config, "UNIVERSE", "SPY,QQQ,SPX")
    assert tw_config.fleet_universes() == ("SPY", "QQQ", "SPX")


def test_stock_onboarding_no_longer_forbids_the_bot_universe():
    from src.tools.stock_env import FORBIDDEN_LISTS

    assert "TRADEWORKZ_UNIVERSE" not in FORBIDDEN_LISTS


# ----------------------------------------------------------------------
# Contract fit
# ----------------------------------------------------------------------


def test_listed_legs_come_back_unchanged():
    legs = [_leg("call", 600, TUE, root="SPY"), _leg("call", 602, TUE, "short", root="SPY")]
    fit = fit_legs(legs, SPY)
    assert fit.reason == "listed"
    assert not fit.changed
    assert [(lg["strike"], lg["expiration"]) for lg in fit.legs] == [
        (600, TUE.isoformat()),
        (602, TUE.isoformat()),
    ]


def test_a_stock_same_day_leg_rolls_to_the_next_listed_expiration_and_grid():
    legs = [_leg("call", 183, TUE), _leg("call", 185, TUE, "short")]
    fit = fit_legs(legs, NVDA)
    assert fit.reason == "fitted"
    assert [lg["expiration"] for lg in fit.legs] == [FRI.isoformat()] * 2
    assert [lg["strike"] for lg in fit.legs] == [182.5, 185.0]
    assert fit.legs[0]["option_symbol"] == "NVDA 261113C182.5"
    assert "2026-11-10->2026-11-13" in fit.detail
    assert "183->182.5" in fit.detail
    # Side and right never change.
    assert [lg["side"] for lg in fit.legs] == ["long", "short"]


def test_a_fit_that_would_merge_two_strikes_is_refused():
    # $1 apart on a $2.50 grid: both land on 182.5 — a zero-width "spread".
    fit = fit_legs([_leg("call", 182, TUE), _leg("call", 183, TUE, "short")], NVDA)
    assert fit.legs is None
    assert fit.reason == "strikes_merged"


def test_a_strike_off_the_edge_of_the_band_is_refused_not_dragged_in():
    fit = fit_legs([_leg("put", 160, TUE)], NVDA)  # band starts at 175
    assert fit.legs is None
    assert fit.reason == "strike_off_band"


def test_a_strike_one_step_past_the_edge_snaps_to_it():
    fit = fit_legs([_leg("put", 173, TUE)], NVDA)  # 2 below 175, inside one 2.5 step
    assert fit.legs[0]["strike"] == 175.0


def test_an_expiration_never_rolls_backward_or_too_far():
    later = FRI + timedelta(days=7)
    # Asking for a date after every listed one: nothing on/after it.
    assert fit_legs([_leg("call", 185, later + timedelta(days=1))], NVDA).reason == (
        "no_listed_expiration"
    )
    # Listed only far beyond the roll window.
    far = _chain("NVDA", [TUE + timedelta(days=MAX_EXPIRATION_ROLL_DAYS + 3)], [185.0])
    assert fit_legs([_leg("call", 185, TUE)], far).reason == "no_listed_expiration"


def test_two_requested_expirations_that_land_on_one_are_refused():
    # A same-day/next-day calendar on a Friday-only stock would collapse.
    legs = [_leg("call", 185, TUE, "short"), _leg("call", 185, TUE + timedelta(days=1))]
    assert fit_legs(legs, NVDA).reason == "expirations_merged"


def test_a_straddle_stays_a_straddle_and_a_condor_keeps_its_shape():
    straddle = fit_legs([_leg("call", 183, TUE), _leg("put", 183, TUE)], NVDA)
    assert {lg["strike"] for lg in straddle.legs} == {182.5}

    condor = fit_legs(
        [
            _leg("put", 176, TUE),
            _leg("put", 178, TUE, "short"),
            _leg("call", 188, TUE, "short"),
            _leg("call", 190, TUE),
        ],
        NVDA,
    )
    strikes = [lg["strike"] for lg in condor.legs]
    assert strikes == sorted(strikes)
    assert strikes == [175.0, 177.5, 187.5, 190.0]


def test_a_strike_listed_only_for_the_other_right_is_refused():
    calls_only = _chain("NVDA", [FRI], [180.0, 182.5, 185.0], rights=("C",))
    fit = fit_legs([_leg("put", 182.5, FRI)], calls_only)
    assert fit.reason == "not_listed_for_right"


def test_no_chain_passes_the_legs_through_untouched():
    legs = [_leg("call", 183, TUE)]
    fit = fit_legs(legs, {})
    assert fit == FitResult(legs, "no_chain")


def test_the_chain_reader_keeps_only_the_root_the_bots_trade(monkeypatch):
    monkeypatch.setenv("SYMBOL_ALIASES", "SPX=$SPXW.X")
    monkeypatch.setenv("OPTION_ROOT_ALIASES", "$SPXW.X=SPXW,$SPX.X=SPX")

    class _Cur:
        def execute(self, sql, params=None):
            self.sql = sql

        def fetchall(self):
            return [
                ("SPXW 261110C6800", TUE, 6800, "C"),
                ("SPX 261120C6800", date(2026, 11, 20), 6800, "C"),  # AM-settled monthly
                ("garbage",),
            ]

    class _Conn:
        def cursor(self):
            return _Cur()

    chain = fetch_listed_chain(_Conn(), "SPX", from_date=TUE)
    assert list(chain) == [TUE]
    assert chain[TUE]["C"] == {6800.0: "SPXW 261110C6800"}


def test_a_failing_chain_read_degrades_to_no_chain():
    from src.tradeworkz.chain import fit_legs_to_chain

    legs = [_leg("call", 183, TUE)]
    fit = fit_legs_to_chain(None, "NVDA", legs)  # no connection at all
    assert fit.reason == "no_chain"
    assert fit.legs == legs


# ----------------------------------------------------------------------
# Strike grid
# ----------------------------------------------------------------------


def test_strike_step_is_the_most_common_adjacent_gap():
    assert strike_step_from_strikes([175, 177.5, 180, 182.5, 185]) == 2.5
    assert strike_step_from_strikes([598, 599, 600, 601, 605]) == 1.0
    assert strike_step_from_strikes([600]) is None
    assert strike_step_from_strikes(["x", None]) is None


def test_snapshot_grid_prefers_override_then_listed_then_type_default(monkeypatch):
    from src.tradeworkz import context

    monkeypatch.setattr(context, "listed_strike_increment", lambda *a, **k: 2.5)
    monkeypatch.delenv("TRADEWORKZ_STRIKE_INCREMENTS", raising=False)
    assert context._resolve_strike_increment(None, "NVDA", 183.0, None) == 2.5
    # A replay keeps the type default here (the harness reads its own grid).
    as_of = datetime(2026, 11, 10, 15, tzinfo=timezone.utc)
    assert context._resolve_strike_increment(None, "NVDA", 183.0, as_of) == 1.0
    monkeypatch.setenv("TRADEWORKZ_STRIKE_INCREMENTS", "NVDA=5")
    assert context._resolve_strike_increment(None, "NVDA", 183.0, None) == 5.0
    monkeypatch.delenv("TRADEWORKZ_STRIKE_INCREMENTS")
    monkeypatch.setattr(context, "listed_strike_increment", lambda *a, **k: None)
    assert context._resolve_strike_increment(None, "SPX", 6800.0, None) == 5.0


# ----------------------------------------------------------------------
# Live open path: fitted legs, time stop on the expiration the bot asked for
# ----------------------------------------------------------------------


class _OpenCursor:
    def __init__(self):
        self.calls = []
        self._last = ""

    def execute(self, sql, params=None):
        self._last = sql
        self.calls.append((" ".join(sql.split()), params))

    def fetchone(self):
        if "pg_try_advisory_xact_lock" in self._last:
            return (True,)
        if "COUNT(1)" in self._last:
            return (0,)
        if "INSERT INTO tw_positions" in self._last:
            return (42,)
        return None

    def fetchall(self):
        return []


class _OpenConn:
    def __init__(self):
        self._cur = _OpenCursor()

    def cursor(self):
        return self._cur


def _signal(legs, time_stop_at):
    from src.tradeworkz.models import Leg, TradeSignal

    return TradeSignal(
        bot_id="charm_close_magnet",
        underlying="NVDA",
        direction="bullish",
        strategy_type="BULL_CALL_DEBIT_SPREAD",
        legs=[Leg(**lg) for lg in legs],
        entry_price=0.0,
        conviction=0.7,
        target_price=186.0,
        stop_price=181.0,
        time_stop_at=time_stop_at,
    )


def _capital():
    from src.tradeworkz.models import BotCapital

    return BotCapital("charm_close_magnet", 100_000.0, 100_000.0, 100_000.0, 0.06, 0.5, 0.02)


def test_open_fits_a_stock_and_still_closes_the_day_the_bot_asked_for(monkeypatch):
    from src.market_calendar import ET
    from src.tradeworkz import reconciler

    today = datetime.now(ET).date()
    friday = today + timedelta(days=3)
    chain = _chain("NVDA", [friday], [175 + 2.5 * i for i in range(9)])
    monkeypatch.setattr(
        reconciler, "fit_legs_to_chain", lambda conn, u, legs, as_of=None: fit_legs(legs, chain)
    )
    monkeypatch.setattr(reconciler, "spread_price", lambda conn, legs, action: 1.20)
    legs = [_leg("call", 183, today), _leg("call", 185, today, "short")]
    late = datetime.now(timezone.utc) + timedelta(days=3)
    conn = _OpenConn()

    pos_id = reconciler.open_position(conn, _signal(legs, late), _capital(), 1.0)

    assert pos_id == 42
    insert = next(p for sql, p in conn._cur.calls if sql.startswith("INSERT INTO tw_positions"))
    stored = json.loads(insert[6])
    assert [lg["expiration"] for lg in stored] == [friday.isoformat()] * 2
    assert [lg["strike"] for lg in stored] == [182.5, 185.0]
    # The bot asked for today, so it closes at today's 15:55 ET, not Friday's.
    assert insert[12] == reconciler._expiration_close_cap_utc(today)
    payload = json.loads(insert[17])
    assert "182.5" in payload["contract_fit"]


def test_open_refuses_a_trade_the_chain_does_not_list(monkeypatch):
    from src.tradeworkz import reconciler

    monkeypatch.setattr(
        reconciler,
        "fit_legs_to_chain",
        lambda conn, u, legs, as_of=None: FitResult(None, "strike_off_band"),
    )
    priced = []
    monkeypatch.setattr(reconciler, "spread_price", lambda *a, **k: priced.append(1) or 1.0)
    conn = _OpenConn()
    legs = [_leg("call", 140, TUE)]
    assert reconciler.open_position(conn, _signal(legs, None), _capital(), 1.0) is None
    assert priced == [], "a refused fit must never reach pricing"
    assert not any(sql.startswith("INSERT INTO tw_positions") for sql, _p in conn._cur.calls)


def test_backtest_tallies_a_refused_fit_as_an_entry_reject(monkeypatch):
    from src.tradeworkz import backtest as bt
    from src.tradeworkz.registry import known_specs

    spec = known_specs()["charm_close_magnet"]
    runner = bt._BotRunner(spec, slippage_pct=0.0, tolerance_min=10)
    monkeypatch.setattr(
        runner.bot, "open_criteria", lambda snap: _signal([_leg("call", 140, TUE)], None)
    )
    monkeypatch.setattr(bt, "fit_legs_to_chain", lambda *a, **k: FitResult(None, "strike_off_band"))

    class _Snap:
        spot = 183.0
        trade_bias_code = None

    runner._maybe_open(conn=None, u="NVDA", snap=_Snap(), now_utc=datetime.now(timezone.utc))
    assert runner.entry_rejects == {"no_listed_contract:strike_off_band": 1}
    assert "NVDA" not in runner.open_by_underlying


@pytest.mark.parametrize("u", ["SPY", "NVDA"])
def test_backtest_prices_the_fitted_legs(monkeypatch, u):
    from src.tradeworkz import backtest as bt
    from src.tradeworkz.registry import known_specs

    spec = known_specs()["charm_close_magnet"]
    runner = bt._BotRunner(spec, slippage_pct=0.0, tolerance_min=10)
    chain = NVDA if u == "NVDA" else SPY
    root = u
    monkeypatch.setattr(
        runner.bot,
        "open_criteria",
        lambda snap: _signal([_leg("call", 600 if u == "SPY" else 183, TUE, root=root)], None),
    )
    monkeypatch.setattr(
        bt, "fit_legs_to_chain", lambda conn, und, legs, as_of=None: fit_legs(legs, chain)
    )
    seen = []
    monkeypatch.setattr(
        bt, "historical_spread_price", lambda conn, legs, **k: seen.append(legs) or 1.0
    )

    class _Snap:
        spot = 600.0 if u == "SPY" else 183.0
        trade_bias_code = None

    runner._maybe_open(conn=None, u=u, snap=_Snap(), now_utc=datetime.now(timezone.utc))
    assert u in runner.open_by_underlying
    expected_exp = TUE if u == "SPY" else FRI
    assert seen[0][0]["expiration"] == expected_exp.isoformat()


def test_time_stop_caps_on_the_asked_for_day_unless_that_day_is_closed(monkeypatch):
    from src import market_calendar
    from src.tradeworkz.reconciler import _cap_time_stop_for_fit, _expiration_close_cap_utc

    asked = [_leg("call", 600, TUE, root="SPY")]
    rolled = [_leg("call", 600, TUE + timedelta(days=1), root="SPY")]
    late = datetime(2026, 11, 20, tzinfo=timezone.utc)
    assert _cap_time_stop_for_fit(late, asked, rolled) == _expiration_close_cap_utc(TUE)
    # The bot's weekday walk landed on a holiday: no 15:55 to close at that
    # day, so the cap falls to the expiration it rolled to.
    monkeypatch.setattr(market_calendar, "NYSE_HOLIDAYS", {TUE})
    assert _cap_time_stop_for_fit(late, asked, rolled) == _expiration_close_cap_utc(
        TUE + timedelta(days=1)
    )
