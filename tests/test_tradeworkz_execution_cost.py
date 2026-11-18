"""Execution cost: slippage on the net price, and the entry-cost gate.

The 2026-11-13..17 forward test lost $15,992 of its $22,357 to premium stops,
six of them within a minute of opening. On SPX the paper fill charged 2% of
each leg's full price on top of crossing both bid/asks, so a vertical opened
about a third under water and the 40% stop fired as soon as its grace ended.
These lock in the two fixes: slippage is charged once on the structure's net
(single options unchanged), and a trade that would open more than
MAX_ENTRY_COST_PCT under water is refused, live and in the harness alike.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.tradeworkz import config as tw_config
from src.tradeworkz import pricing
from src.tradeworkz.pricing import charge_net_slippage, entry_cost_fraction, spread_price

SLIP = 0.02
EXP = "2026-11-18"


def _leg(sym, side, right="call", strike=100.0, exp=EXP):
    return {
        "option_symbol": sym,
        "side": side,
        "option_type": right,
        "strike": strike,
        "expiration": exp,
    }


def _quotes(monkeypatch, book):
    """Serve ``book[option_symbol] = (bid, ask, last)``; missing => no quote."""
    monkeypatch.setattr(pricing, "_latest_quote", lambda conn, sym: book.get(sym))


# ----------------------------------------------------------------------
# charge_net_slippage
# ----------------------------------------------------------------------


def test_net_slippage_always_works_against_the_trader():
    assert charge_net_slippage(5.0, "open", SLIP) == pytest.approx(5.10)  # pay more
    assert charge_net_slippage(5.0, "close", SLIP) == pytest.approx(4.90)  # collect less
    assert charge_net_slippage(-1.0, "open", SLIP) == pytest.approx(-0.98)  # collect less
    assert charge_net_slippage(-1.0, "close", SLIP) == pytest.approx(-1.02)  # pay more
    assert charge_net_slippage(0.0, "open", SLIP) == 0.0
    assert charge_net_slippage(5.0, "open", 0.0) == 5.0


# ----------------------------------------------------------------------
# spread_price
# ----------------------------------------------------------------------


def test_a_single_long_option_prices_exactly_as_before(monkeypatch):
    _quotes(monkeypatch, {"C": (2.00, 2.10, 2.05)})
    legs = [_leg("C", "long")]
    assert spread_price(None, legs, action="open", slippage_pct=SLIP) == pytest.approx(2.10 * 1.02)
    assert spread_price(None, legs, action="close", slippage_pct=SLIP) == pytest.approx(2.00 * 0.98)


def test_a_single_short_option_prices_exactly_as_before(monkeypatch):
    _quotes(monkeypatch, {"P": (1.50, 1.60, 1.55)})
    legs = [_leg("P", "short", "put")]
    assert spread_price(None, legs, action="open", slippage_pct=SLIP) == pytest.approx(-1.50 * 0.98)
    assert spread_price(None, legs, action="close", slippage_pct=SLIP) == pytest.approx(
        -1.60 * 1.02
    )


def test_an_spx_vertical_no_longer_opens_a_third_under_water(monkeypatch):
    # The docstring's worked example: $20 and $14 legs, 40c wide each.
    _quotes(monkeypatch, {"L": (19.80, 20.20, 20.0), "S": (13.80, 14.20, 14.0)})
    legs = [_leg("L", "long", strike=6800), _leg("S", "short", strike=6810)]
    opened = spread_price(None, legs, action="open", slippage_pct=SLIP)
    closed = spread_price(None, legs, action="close", slippage_pct=SLIP)
    assert opened == pytest.approx((20.20 - 13.80) * 1.02)  # 6.528
    assert closed == pytest.approx((19.80 - 14.20) * 0.98)  # 5.488
    # The old per-leg model: 20.20*1.02 - 13.80*0.98 = 7.08 in, 4.92 out.
    old_cost = (7.08 - 4.92) / 7.08
    new_cost = entry_cost_fraction(opened, closed)
    assert old_cost == pytest.approx(0.305, abs=1e-3)
    assert new_cost == pytest.approx(0.159, abs=1e-3)


def test_a_credit_spread_collects_less_and_pays_more(monkeypatch):
    _quotes(monkeypatch, {"SP": (1.50, 1.60, 1.55), "LP": (0.50, 0.60, 0.55)})
    legs = [_leg("SP", "short", "put", 100), _leg("LP", "long", "put", 95)]
    assert spread_price(None, legs, action="open", slippage_pct=SLIP) == pytest.approx(
        (0.60 - 1.50) + 0.02 * 0.90
    )
    assert spread_price(None, legs, action="close", slippage_pct=SLIP) == pytest.approx(
        (0.50 - 1.60) - 0.02 * 1.10
    )


def test_expired_legs_settle_exactly_and_only_quoted_legs_pay_slippage(monkeypatch):
    _quotes(monkeypatch, {"L": (1.00, 1.10, 1.05)})  # the short leg has no quote
    monkeypatch.setattr(
        pricing,
        "_leg_intrinsic",
        lambda conn, leg, today: -0.40 if leg["side"] == "short" else None,
    )
    legs = [_leg("L", "long"), _leg("S", "short")]
    assert spread_price(None, legs, action="close", slippage_pct=SLIP) == pytest.approx(
        1.00 * 0.98 - 0.40
    )
    # Never opens off intrinsic.
    assert spread_price(None, legs, action="open", slippage_pct=SLIP) is None


def test_the_harness_prices_like_live(monkeypatch):
    from src.tradeworkz import backtest as bt

    book = {"L": (19.80, 20.20, 20.0), "S": (13.80, 14.20, 14.0)}
    _quotes(monkeypatch, book)
    monkeypatch.setattr(
        bt, "_nearest_leg_quote", lambda conn, leg, at, tol: book.get(leg["option_symbol"])
    )
    legs = [_leg("L", "long", strike=6800), _leg("S", "short", strike=6810)]
    at = datetime(2026, 11, 17, 15, 0, tzinfo=timezone.utc)
    for action in ("open", "close"):
        live = spread_price(None, legs, action=action, slippage_pct=SLIP)
        hist = bt.historical_spread_price(None, legs, at=at, action=action, slippage_pct=SLIP)
        assert hist == pytest.approx(live), action


# ----------------------------------------------------------------------
# entry_cost_fraction
# ----------------------------------------------------------------------


def test_entry_cost_is_the_round_trip_share_of_the_premium():
    assert entry_cost_fraction(7.0, 5.0) == pytest.approx(2 / 7)
    assert entry_cost_fraction(-1.0, -1.3) == pytest.approx(0.30)  # credit: same sign
    assert entry_cost_fraction(13.76, 3.32) == pytest.approx(0.7587, abs=1e-4)  # the NDX trade
    assert entry_cost_fraction(None, 5.0) is None
    assert entry_cost_fraction(7.0, None) is None
    assert entry_cost_fraction(0.0, 0.0) is None


# ----------------------------------------------------------------------
# The gate, live
# ----------------------------------------------------------------------


class _Cur:
    def __init__(self):
        self._last = ""
        self.calls = []

    def execute(self, sql, params=None):
        self._last = sql
        self.calls.append((" ".join(sql.split()), params))

    def fetchone(self):
        if "pg_try_advisory_xact_lock" in self._last:
            return (True,)
        if "COUNT(1)" in self._last:
            return (0,)
        if "INSERT INTO tw_positions" in self._last:
            return (77,)
        return None

    def fetchall(self):
        return []


class _Conn:
    def __init__(self):
        self.cur = _Cur()

    def cursor(self):
        return self.cur


def _signal():
    from src.tradeworkz.models import Leg, TradeSignal

    return TradeSignal(
        bot_id="settlement_flow_snap",
        underlying="NDX",
        direction="bearish",
        strategy_type="BEAR_PUT_DEBIT_SPREAD",
        legs=[
            Leg(
                option_symbol="NDXP 261118P20000",
                side="long",
                option_type="put",
                strike=20000,
                expiration=EXP,
            ),
            Leg(
                option_symbol="NDXP 261118P19950",
                side="short",
                option_type="put",
                strike=19950,
                expiration=EXP,
            ),
        ],
        entry_price=0.0,
        conviction=0.7,
        time_stop_at=datetime.now(timezone.utc) + timedelta(minutes=30),
    )


def _capital():
    from src.tradeworkz.models import BotCapital

    return BotCapital("settlement_flow_snap", 1e5, 1e5, 1e5, 0.06, 0.5, 0.02)


def _wire(monkeypatch, open_net, close_now):
    from src.tradeworkz import reconciler
    from src.tradeworkz.chain import FitResult

    monkeypatch.setattr(
        reconciler, "fit_legs_to_chain", lambda c, u, legs, as_of=None: FitResult(legs, "no_chain")
    )
    monkeypatch.setattr(
        reconciler,
        "spread_price",
        lambda conn, legs, action, **k: open_net if action == "open" else close_now,
    )
    return reconciler


def test_live_open_refuses_a_trade_that_opens_deep_under_water(monkeypatch):
    reconciler = _wire(monkeypatch, 13.76, 5.00)  # ~64% gone on arrival
    conn = _Conn()
    assert reconciler.open_position(conn, _signal(), _capital(), 1.0) is None
    assert not any(sql.startswith("INSERT INTO tw_positions") for sql, _p in conn.cur.calls)


def test_live_open_takes_a_cheap_entry_and_records_its_cost(monkeypatch):
    reconciler = _wire(monkeypatch, 6.53, 5.49)  # ~16%
    conn = _Conn()
    assert reconciler.open_position(conn, _signal(), _capital(), 1.0) == 77
    insert = next(p for sql, p in conn.cur.calls if sql.startswith("INSERT INTO tw_positions"))
    payload = json.loads(insert[17])
    assert payload["entry_cost_pct"] == pytest.approx(0.1593, abs=1e-4)


def test_a_zero_cap_disables_the_gate(monkeypatch):
    monkeypatch.setattr(tw_config, "MAX_ENTRY_COST_PCT", 0.0)
    reconciler = _wire(monkeypatch, 13.76, 5.00)
    assert reconciler.open_position(_Conn(), _signal(), _capital(), 1.0) == 77


# ----------------------------------------------------------------------
# The gate, in the harness
# ----------------------------------------------------------------------


def test_the_harness_tallies_an_entry_cost_rejection(monkeypatch):
    from src.tradeworkz import backtest as bt
    from src.tradeworkz.chain import FitResult
    from src.tradeworkz.registry import known_specs

    runner = bt._BotRunner(
        known_specs()["settlement_flow_snap"], slippage_pct=0.0, tolerance_min=10
    )
    monkeypatch.setattr(runner.bot, "open_criteria", lambda snap: _signal())
    monkeypatch.setattr(
        bt, "fit_legs_to_chain", lambda c, u, legs, as_of=None: FitResult(legs, "no_chain")
    )
    monkeypatch.setattr(
        bt,
        "historical_spread_price",
        lambda conn, legs, *, at, action, **k: 13.76 if action == "open" else 5.00,
    )

    class _Snap:
        spot = 20010.0
        trade_bias_code = None

    runner._maybe_open(conn=None, u="NDX", snap=_Snap(), now_utc=datetime.now(timezone.utc))
    assert runner.entry_rejects == {"entry_cost": 1}
    assert "NDX" not in runner.open_by_underlying


# ----------------------------------------------------------------------
# The scorecard keeps the two execution models apart
# ----------------------------------------------------------------------


def test_a_new_execution_model_starts_a_new_tuning(monkeypatch):
    from src.tradeworkz import engine

    params = {"dte_target": 0}
    now = engine.params_version(params)
    monkeypatch.setattr(engine, "EXECUTION_VERSION", engine.EXECUTION_VERSION + 1)
    assert engine.params_version(params) != now
