"""PlaybookEngine with the learning loop: one Card per idea, and the entry
bar each pattern has earned per symbol."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from src import config
from src.signals.components.base import MarketContext
from src.signals.playbook import adaptive_gate
from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import OpenPosition, PlaybookContext
from src.signals.playbook.engine import PlaybookEngine
from src.signals.playbook.types import ActionCard, ActionEnum, Entry, Leg, Stop, Target

NOW = datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)  # 14:30 ET


@pytest.fixture(autouse=True)
def _clean_gate(monkeypatch):
    adaptive_gate.set_active_store(None)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_GATE_ENABLED", True)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_MIN_IDEAS", 8)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_PRIOR_WEIGHT", 10.0)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_COST_R", 0.10)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_NEUTRAL_BAR", 0.25)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_MIN_BAR", 0.20)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_MAX_BAR", 0.75)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_PROVEN_R", 0.25)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_PAUSE_R", -0.25)
    yield
    adaptive_gate.set_active_store(None)


class _Pattern(PatternBase):
    def __init__(
        self,
        *,
        id: str,
        direction: str = "bullish",
        confidence: float = 0.55,
        max_hold_minutes: int = 90,
    ):
        self.id = id
        self.name = id
        self.tier = "0DTE"
        self.direction = direction
        self.valid_regimes = ()
        self.preferred_regime = "controlled_trend"
        self.pattern_base = 0.55
        self._confidence = confidence
        self._max_hold = max_hold_minutes

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        bullish = self.direction == "bullish"
        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=ActionEnum.BUY_CALL_DEBIT if bullish else ActionEnum.BUY_PUT_DEBIT,
            pattern=self.id,
            tier=self.tier,
            direction=self.direction,
            confidence=self._confidence,
            max_hold_minutes=self._max_hold,
            legs=[
                Leg(expiry="2026-05-01", strike=678.0, right="C" if bullish else "P", side="BUY")
            ],
            entry=Entry(ref_price=678.0),
            target=Target(ref_price=681.0 if bullish else 675.0),
            stop=Stop(ref_price=676.5 if bullish else 679.5),
            rationale="stub",
            context={"close": 678.0},
        )


def _ctx(open_positions=(), underlying="SPY") -> PlaybookContext:
    market = MarketContext(
        timestamp=NOW,
        underlying=underlying,
        close=678.0,
        net_gex=1e9,
        gamma_flip=676.0,
        put_call_ratio=0.8,
        max_pain=677.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[],
        iv_rank=None,
    )
    return PlaybookContext(
        market=market, msi_regime="controlled_trend", open_positions=list(open_positions)
    )


def _idea(pattern="p", direction="bullish", minutes_ago=30, status="pending", hold=90):
    return OpenPosition(
        pattern_id=pattern,
        direction=direction,
        instrument="BUY_CALL_DEBIT",
        opened_at=NOW - timedelta(minutes=minutes_ago),
        underlying="SPY",
        max_hold_minutes=hold,
        status=status,
    )


def _record(pattern, underlying, direction, n, mean_r, wins, losses):
    return (pattern, underlying, direction, n, float(n), wins, losses, mean_r * n)


# ----------------------------------------------------------------------
# One Card per idea
# ----------------------------------------------------------------------


def test_live_idea_blocks_a_second_card():
    card = PlaybookEngine(patterns=[_Pattern(id="p")]).evaluate(_ctx([_idea()]))
    assert card.action == ActionEnum.STAND_DOWN
    assert "still live" in card.near_misses[0].missing[0]
    assert "hold window" in card.near_misses[0].missing[0]


def test_stand_down_says_the_live_card_is_still_live():
    card = PlaybookEngine(patterns=[_Pattern(id="p")]).evaluate(_ctx([_idea()]))
    assert card.rationale == "No new Card: the Card already issued by p is still live."


def test_the_idea_frees_the_slot_when_its_hold_ends():
    card = PlaybookEngine(patterns=[_Pattern(id="p")]).evaluate(
        _ctx([_idea(minutes_ago=95, hold=90)])
    )
    assert card.pattern == "p"


def test_the_idea_uses_its_own_hold_not_the_new_cards():
    # The last idea was a 3-day swing hold; the new Card's own hold is 90m.
    card = PlaybookEngine(patterns=[_Pattern(id="p", max_hold_minutes=90)]).evaluate(
        _ctx([_idea(minutes_ago=600, hold=4320)])
    )
    assert card.action == ActionEnum.STAND_DOWN


def test_a_target_hit_does_not_invite_a_chase():
    card = PlaybookEngine(patterns=[_Pattern(id="p")]).evaluate(_ctx([_idea(status="target_hit")]))
    assert card.action == ActionEnum.STAND_DOWN
    assert "already reached its target" in card.near_misses[0].missing[0]


def test_a_stopped_idea_frees_the_slot_only_for_the_other_direction():
    engine_long = PlaybookEngine(patterns=[_Pattern(id="p", direction="bullish")])
    engine_short = PlaybookEngine(patterns=[_Pattern(id="p", direction="bearish")])
    stopped_long = [_idea(direction="bullish", status="stop_hit")]
    assert engine_long.evaluate(_ctx(stopped_long)).action == ActionEnum.STAND_DOWN
    assert engine_short.evaluate(_ctx(stopped_long)).pattern == "p"


def test_other_patterns_are_not_blocked():
    engine = PlaybookEngine(patterns=[_Pattern(id="p"), _Pattern(id="q", confidence=0.5)])
    card = engine.evaluate(_ctx([_idea(pattern="p")]))
    assert card.pattern == "q"


# ----------------------------------------------------------------------
# The entry bar
# ----------------------------------------------------------------------


def test_without_a_record_the_flat_floor_applies():
    engine = PlaybookEngine(patterns=[_Pattern(id="p", confidence=0.22)])
    card, held_back = engine.evaluate_with_held_back(_ctx())
    assert card.action == ActionEnum.STAND_DOWN
    assert held_back == []  # the flat floor, not the record, dropped it


def test_published_card_carries_its_track_record():
    card = PlaybookEngine(patterns=[_Pattern(id="p")]).evaluate(_ctx())
    assert card.context["track_record"]["status"] == "learning"
    assert card.context["close"] == 678.0  # the pattern's own context survives


def test_a_losing_pattern_needs_more_confidence_and_is_held_back():
    adaptive_gate.set_active_store(
        adaptive_gate.build_store([_record("p", "SPY", "bullish", 30, -0.10, 12, 18)])
    )
    engine = PlaybookEngine(patterns=[_Pattern(id="p", confidence=0.45)])
    card, held_back = engine.evaluate_with_held_back(_ctx())
    assert card.action == ActionEnum.STAND_DOWN
    assert "needs confidence 0.60, has 0.45" in card.near_misses[0].missing[0]
    assert [c.pattern for c, _ in held_back] == ["p"]
    assert held_back[0][0].context["track_record"]["status"] == "lagging"

    # The same pattern with enough confidence still publishes.
    strong = PlaybookEngine(patterns=[_Pattern(id="p", confidence=0.70)]).evaluate(_ctx())
    assert strong.pattern == "p"
    assert strong.context["track_record"]["status"] == "lagging"


def test_a_paused_pattern_publishes_nothing_but_is_still_recorded():
    adaptive_gate.set_active_store(
        adaptive_gate.build_store([_record("p", "SPY", "bullish", 40, -0.5, 10, 30)])
    )
    engine = PlaybookEngine(patterns=[_Pattern(id="p", confidence=0.95)])
    card, held_back = engine.evaluate_with_held_back(_ctx())
    assert card.action == ActionEnum.STAND_DOWN
    assert card.near_misses[0].missing[0].startswith("paused")
    assert len(held_back) == 1


def test_the_pause_is_per_symbol():
    adaptive_gate.set_active_store(
        adaptive_gate.build_store(
            [
                _record("p", "SPY", "bullish", 40, -0.5, 10, 30),
                _record("p", "QQQ", "bullish", 60, 0.6, 40, 20),
            ]
        )
    )
    engine = PlaybookEngine(patterns=[_Pattern(id="p", confidence=0.5)])
    assert engine.evaluate(_ctx(underlying="SPY")).action == ActionEnum.STAND_DOWN
    assert engine.evaluate(_ctx(underlying="QQQ")).pattern == "p"


def test_a_proven_pattern_gets_through_at_low_confidence():
    adaptive_gate.set_active_store(
        adaptive_gate.build_store([_record("p", "SPY", "bullish", 60, 0.6, 40, 20)])
    )
    card = PlaybookEngine(patterns=[_Pattern(id="p", confidence=0.22)]).evaluate(_ctx())
    assert card.pattern == "p"
    assert card.context["track_record"]["status"] == "proven"


def test_a_repeat_is_blocked_before_the_bar_so_it_is_never_recorded():
    """An idea already live is not a new held-back idea every cycle."""
    adaptive_gate.set_active_store(
        adaptive_gate.build_store([_record("p", "SPY", "bullish", 40, -0.5, 10, 30)])
    )
    engine = PlaybookEngine(patterns=[_Pattern(id="p")])
    card, held_back = engine.evaluate_with_held_back(_ctx([_idea()]))
    assert card.action == ActionEnum.STAND_DOWN
    assert held_back == []


def test_gate_off_restores_the_flat_floor(monkeypatch):
    adaptive_gate.set_active_store(
        adaptive_gate.build_store([_record("p", "SPY", "bullish", 40, -0.5, 10, 30)])
    )
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_GATE_ENABLED", False)
    card = PlaybookEngine(patterns=[_Pattern(id="p", confidence=0.5)]).evaluate(_ctx())
    assert card.pattern == "p"
