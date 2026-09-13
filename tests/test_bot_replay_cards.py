"""Bot-rule replay produces cards the backtest engine prices like any other.

The consolidation's load-bearing claim is that a bot-only strategy and a
pattern-backed strategy in the same run are *comparable*: same forward walk,
same fill model, same sizing, one equity curve. That only holds if a
TradeSignal is translated into exactly the card shape ``engine._select_legs``
and ``engine._build_candidate`` already understand — so these tests pin the
translation, not the market logic.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

import pytest

from src.backtesting.bot_replay import (
    _leg_payload,
    _level,
    _opens_allowed,
    _signal_to_card,
    bot_backed,
    rth_timesteps,
)
from src.backtesting.engine import _relabel_to_catalog, _select_legs
from src.signals.playbook.backtest import CardRow
from src.strategies import get
from src.tradeworkz.models import Leg, TradeSignal


class _Snap:
    """Minimal stand-in for MarketSnapshot: only what card-building reads."""

    def __init__(self, spot=500.0, net_gex=1.0e9, extra=None):
        self.spot = spot
        self.net_gex = net_gex
        self.extra = extra or {}


def _signal(**kw) -> TradeSignal:
    base = dict(
        bot_id="call_wall_rejector",
        underlying="SPY",
        direction="bearish",
        strategy_type="put_debit_vertical",
        legs=[
            Leg(
                option_symbol="SPY 260714P500",
                side="long",
                option_type="put",
                strike=500.0,
                expiration="2026-07-14",
            ),
            Leg(
                option_symbol="SPY 260714P495",
                side="short",
                option_type="put",
                strike=495.0,
                expiration="2026-07-14",
            ),
        ],
        entry_price=1.85,
        conviction=0.62,
        target_price=497.0,
        stop_price=503.0,
        rationale="confirmed rejection at the call wall",
    )
    base.update(kw)
    return TradeSignal(**base)


# ---------------------------------------------------------------------------
# Leg translation
# ---------------------------------------------------------------------------


def test_long_call_leg_becomes_buy_c():
    out = _leg_payload(
        {"side": "long", "option_type": "call", "strike": 500.0, "expiration": "2026-07-14"}
    )
    assert out == {"expiry": "2026-07-14", "strike": 500.0, "right": "C", "side": "BUY"}


def test_short_put_leg_becomes_sell_p():
    out = _leg_payload(
        {"side": "short", "option_type": "put", "strike": 495.0, "expiration": "2026-07-14"}
    )
    assert out["side"] == "SELL"
    assert out["right"] == "P"


def test_missing_side_and_type_default_to_a_long_call():
    out = _leg_payload({"strike": 1.0, "expiration": "2026-07-14"})
    assert out["side"] == "BUY"
    assert out["right"] == "C"


def test_translated_legs_round_trip_through_the_engines_own_parser():
    """The real contract: whatever we emit, ``_select_legs`` must understand."""
    card = _signal_to_card(
        get("call_wall_fade"),
        _signal(),
        _Snap(),
        underlying="SPY",
        at=datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
        tier="0DTE",
        max_hold=90,
    )
    legs = _select_legs(card)
    assert legs is not None and len(legs) == 2
    assert [leg["side"] for leg in legs] == ["long", "short"]
    assert [leg["right"] for leg in legs] == ["P", "P"]
    assert [leg["strike"] for leg in legs] == [500.0, 495.0]
    assert all(leg["expiry"] == "2026-07-14" for leg in legs)


# ---------------------------------------------------------------------------
# Target / stop
# ---------------------------------------------------------------------------


def test_a_price_target_becomes_a_level_the_engine_can_resolve():
    assert _level(497.0, "bot_target") == {
        "ref_price": 497.0,
        "kind": "level",
        "level_name": "bot_target",
    }


def test_no_target_falls_through_to_the_premium_overlay():
    """Same fallback a pattern card without levels takes — not a dropped card."""
    assert _level(None, "bot_target") == {"ref_price": None, "kind": "premium_pct"}


def test_a_signal_with_no_levels_still_produces_a_usable_card():
    card = _signal_to_card(
        get("call_wall_fade"),
        _signal(target_price=None, stop_price=None),
        _Snap(),
        underlying="SPY",
        at=datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
        tier="0DTE",
        max_hold=90,
    )
    assert card.payload["target"]["kind"] == "premium_pct"
    assert card.payload["stop"]["kind"] == "premium_pct"


# ---------------------------------------------------------------------------
# Card identity and payload
# ---------------------------------------------------------------------------


def test_cards_are_keyed_on_the_canonical_catalog_id_not_the_bot_id():
    """So per-strategy rollups line up with the same strategy measured via its
    pattern binding."""
    entry = get("call_wall_fade")
    assert entry.bot_id == "call_wall_rejector"
    card = _signal_to_card(
        entry,
        _signal(),
        _Snap(),
        underlying="SPY",
        at=datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
        tier="0DTE",
        max_hold=90,
    )
    assert card.pattern == "call_wall_fade"
    assert card.payload["bot_id"] == "call_wall_rejector"
    assert card.payload["source"] == "bot_replay"


def test_card_carries_entry_direction_conviction_and_context():
    snap = _Snap(net_gex=-2.5e9, extra={"msi": 61.0, "msi_regime": "trend_expansion"})
    card = _signal_to_card(
        get("call_wall_fade"),
        _signal(),
        snap,
        underlying="SPY",
        at=datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
        tier="0DTE",
        max_hold=90,
    )
    assert card.direction == "bearish"
    assert card.confidence == pytest.approx(0.62)
    assert card.tier == "0DTE"
    assert card.payload["entry"] == {"ref_price": 1.85, "trigger": "at_market"}
    assert card.payload["context"] == {
        "net_gex": -2.5e9,
        "regime": "trend_expansion",
        "msi": 61.0,
    }


def test_a_signal_time_stop_becomes_the_cards_max_hold():
    at = datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)
    card = _signal_to_card(
        get("call_wall_fade"),
        _signal(time_stop_at=at + timedelta(minutes=45)),
        _Snap(),
        underlying="SPY",
        at=at,
        tier="0DTE",
        max_hold=999,
    )
    assert card.payload["max_hold_minutes"] == 45


def test_without_a_time_stop_the_runs_max_hold_applies():
    card = _signal_to_card(
        get("call_wall_fade"),
        _signal(time_stop_at=None),
        _Snap(),
        underlying="SPY",
        at=datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc),
        tier="0DTE",
        max_hold=120,
    )
    assert card.payload["max_hold_minutes"] == 120


# ---------------------------------------------------------------------------
# Selection and scheduling
# ---------------------------------------------------------------------------


def test_bot_backed_keeps_only_strategies_a_bot_implements():
    picked = bot_backed(
        ["gex_gradient_trend", "call_wall_fade", "settlement_flow_snap", "nonsense"]
    )
    assert [e.id for e in picked] == ["call_wall_fade", "settlement_flow_snap"]


def test_bot_backed_accepts_a_legacy_bot_id():
    assert [e.id for e in bot_backed(["call_wall_rejector"])] == ["call_wall_fade"]


def test_timesteps_stay_inside_regular_trading_hours():
    start = datetime(2026, 7, 14, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc)
    steps = rth_timesteps(start, end, 30)
    assert steps, "expected steps on a Tuesday"
    from src.market_calendar import ET

    for t in steps:
        local = t.astimezone(ET).time()
        assert time(9, 30) <= local < time(16, 0)


def test_timesteps_skip_weekends():
    saturday = datetime(2026, 7, 18, 0, 0, tzinfo=timezone.utc)
    sunday = datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc)
    assert rth_timesteps(saturday, sunday, 30) == []


def test_a_zero_interval_falls_back_to_the_default_rather_than_looping_forever():
    start = datetime(2026, 7, 14, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 14, 23, 59, tzinfo=timezone.utc)
    assert len(rth_timesteps(start, end, 0)) > 0


def test_late_session_opens_are_refused_like_the_live_engine():
    from src.market_calendar import ET

    def at(hh, mm):
        return datetime(2026, 7, 14, hh, mm, tzinfo=ET).astimezone(timezone.utc)

    assert _opens_allowed(at(10, 0), time(15, 55))
    assert not _opens_allowed(at(15, 56), time(15, 55))


# ---------------------------------------------------------------------------
# Relabelling persisted pattern cards
# ---------------------------------------------------------------------------


def _card(pattern: str) -> CardRow:
    return CardRow(
        underlying="SPY",
        timestamp=datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc),
        pattern=pattern,
        action="BUY_PUT_DEBIT",
        tier="0DTE",
        direction="bearish",
        confidence=0.5,
        payload={"direction": "bearish"},
    )


def test_relabel_rewrites_a_legacy_pattern_id_to_its_catalog_id():
    # gamma_flip_bounce IS the catalog id, so it passes through untouched;
    # the interesting case is a pattern whose catalog id differs.
    out = _relabel_to_catalog([_card("gamma_flip_bounce"), _card("call_wall_fade")])
    assert [c.pattern for c in out] == ["gamma_flip_bounce", "call_wall_fade"]


def test_relabel_leaves_an_unknown_pattern_alone():
    """An older saved config or share link must keep resolving."""
    out = _relabel_to_catalog([_card("some_retired_pattern")])
    assert [c.pattern for c in out] == ["some_retired_pattern"]


def test_relabel_preserves_every_other_field():
    src = _card("call_wall_fade")
    out = _relabel_to_catalog([src])[0]
    assert (out.underlying, out.timestamp, out.action, out.tier) == (
        src.underlying,
        src.timestamp,
        src.action,
        src.tier,
    )
    assert out.direction == src.direction
    assert out.confidence == src.confidence
    assert out.payload == src.payload
