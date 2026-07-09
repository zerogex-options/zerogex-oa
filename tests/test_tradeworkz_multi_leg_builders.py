"""Regression: multi-leg option-structure builders emit correctly-typed legs.

Every builder on ``BaseBot`` (build_atm_debit, build_vertical,
build_straddle, build_strangle, build_iron_condor) must produce Leg
dataclasses whose ``option_symbol`` matches the TradeStation format the
ingest layer writes into option_chains — otherwise spread_price returns
None on every tick and no trade ever opens. This test locks the shape.

Also verifies ``resolve_expiration_iso`` walks WEEKDAYS forward for
positive DTEs so a "1DTE" entered on Friday lands on the following
Monday, matching what an options chain actually offers.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import pytest

from src.tradeworkz.bots.base import (
    BaseBot,
    resolve_expiration_iso,
)
from src.tradeworkz.models import BotSpec


def _bot() -> BaseBot:
    spec = BotSpec(
        id="unit_test_bot",
        display_name="Unit",
        strategy_class="BaseBot",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params={},
    )
    return BaseBot(spec)


# ── resolve_expiration_iso ─────────────────────────────────────────


def test_dte_zero_returns_same_day():
    assert resolve_expiration_iso(date(2026, 7, 9), 0) == "2026-07-09"


def test_dte_one_from_thursday_returns_friday():
    assert resolve_expiration_iso(date(2026, 7, 9), 1) == "2026-07-10"


def test_dte_one_from_friday_skips_weekend_to_monday():
    """A 1DTE entered Friday lands on the following Monday, not Saturday."""
    assert resolve_expiration_iso(date(2026, 7, 10), 1) == "2026-07-13"


def test_dte_five_walks_five_weekdays():
    """5 DTE from Monday 2026-07-06 → Monday 2026-07-13 (skips two weekends)."""
    # Actually 5 weekdays from Mon 7/6 is: Tue 7/7, Wed 7/8, Thu 7/9, Fri 7/10, Mon 7/13
    assert resolve_expiration_iso(date(2026, 7, 6), 5) == "2026-07-13"


def test_negative_dte_treated_as_zero():
    assert resolve_expiration_iso(date(2026, 7, 9), -3) == "2026-07-09"


# ── build_atm_debit / build_vertical ───────────────────────────────


def test_atm_debit_single_long_leg():
    legs = _bot().build_atm_debit("SPY", "put", 744, "2026-07-10", 0.0)
    assert len(legs) == 1
    assert legs[0].side == "long"
    assert legs[0].option_type == "put"
    assert legs[0].strike == 744
    assert legs[0].expiration == "2026-07-10"
    # Symbol must match TradeStation format the ingest writes:
    # "{root} {YYMMDD}{C|P}{strike}"
    assert legs[0].option_symbol == "SPY 260710P744"


def test_vertical_has_one_long_one_short_same_type():
    legs = _bot().build_vertical("SPY", "call", 745, 746, "2026-07-10")
    assert len(legs) == 2
    sides = {(leg.side, leg.strike) for leg in legs}
    assert sides == {("long", 745), ("short", 746)}
    assert all(leg.option_type == "call" for leg in legs)


# ── build_straddle ─────────────────────────────────────────────────


def test_straddle_two_long_legs_same_strike_different_type():
    legs = _bot().build_straddle("SPY", 746, "2026-07-10")
    assert len(legs) == 2
    types = sorted(leg.option_type for leg in legs)
    assert types == ["call", "put"]
    assert all(leg.side == "long" for leg in legs)
    assert all(leg.strike == 746 for leg in legs)


# ── build_strangle ─────────────────────────────────────────────────


def test_strangle_has_higher_call_strike_and_lower_put_strike():
    legs = _bot().build_strangle("SPY", call_strike=750, put_strike=742, expiration="2026-07-10")
    assert len(legs) == 2
    by_type = {leg.option_type: leg for leg in legs}
    assert by_type["call"].strike == 750
    assert by_type["put"].strike == 742
    assert all(leg.side == "long" for leg in legs)


# ── build_iron_condor ──────────────────────────────────────────────


def test_iron_condor_four_legs_correct_sides():
    """Short IC: long the wings (protection), short the near-the-money strikes."""
    legs = _bot().build_iron_condor(
        "SPY",
        put_long_strike=738,
        put_short_strike=740,
        call_short_strike=752,
        call_long_strike=754,
        expiration="2026-07-10",
    )
    assert len(legs) == 4
    by_strike = {leg.strike: leg for leg in legs}
    # Wing (long) legs
    assert by_strike[738].side == "long" and by_strike[738].option_type == "put"
    assert by_strike[754].side == "long" and by_strike[754].option_type == "call"
    # Body (short) legs
    assert by_strike[740].side == "short" and by_strike[740].option_type == "put"
    assert by_strike[752].side == "short" and by_strike[752].option_type == "call"


def test_iron_condor_symbols_all_use_tradestation_format():
    legs = _bot().build_iron_condor(
        "SPY", 738, 740, 752, 754, "2026-07-10",
    )
    for leg in legs:
        # Format: "{root} {YYMMDD}{C|P}{strike}"
        assert leg.option_symbol.startswith("SPY 260710")
        assert leg.option_symbol.split(" ")[1][:6] == "260710"


def test_iron_condor_qqq_underlying_carries_through():
    """Multi-underlying support: builder honours the underlying arg
    for symbol construction, so the same code trades QQQ chains."""
    legs = _bot().build_iron_condor(
        "QQQ", 480, 485, 510, 515, "2026-07-10",
    )
    for leg in legs:
        assert leg.option_symbol.startswith("QQQ ")
