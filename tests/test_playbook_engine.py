"""PlaybookEngine evaluation rules: gates, conflict resolution, STAND_DOWN."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext, OpenPosition
from src.signals.playbook.engine import PlaybookEngine
from src.signals.playbook.types import (
    ActionCard,
    ActionEnum,
    Entry,
    Leg,
    Stop,
    Target,
)

# ----------------------------------------------------------------------
# Test doubles
# ----------------------------------------------------------------------


class _StubPattern(PatternBase):
    """Minimal pattern that emits a fixed Card unless told to abstain."""

    def __init__(
        self,
        *,
        id: str,
        tier: str = "0DTE",
        action: ActionEnum = ActionEnum.BUY_PUT_DEBIT,
        confidence: float = 0.55,
        valid_regimes: tuple[str, ...] = ("chop_range", "high_risk_reversal"),
        preferred_regime: str = "high_risk_reversal",
        max_hold_minutes: int = 90,
        explain: Optional[list[str]] = None,
        emit: bool = True,
    ):
        # Have to set class attrs on the instance for ABC happiness.
        self.id = id
        self.name = id
        self.tier = tier
        self.direction = "bearish"
        self.valid_regimes = valid_regimes
        self.preferred_regime = preferred_regime
        self.pattern_base = 0.55
        self._action = action
        self._confidence = confidence
        self._max_hold_minutes = max_hold_minutes
        self._explain = explain or []
        self._emit = emit

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        if not self._emit:
            return None
        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=self._action,
            pattern=self.id,
            tier=self.tier,
            direction=self.direction,
            confidence=self._confidence,
            size_multiplier=0.6,
            max_hold_minutes=self._max_hold_minutes,
            legs=[Leg(expiry="2026-05-01", strike=678.0, right="P", side="BUY", qty=1)],
            entry=Entry(ref_price=678.0, trigger="at_market"),
            target=Target(ref_price=675.0, kind="level", level_name="max_pain"),
            stop=Stop(ref_price=679.5, kind="level", level_name="call_wall_break"),
            rationale=f"stub pattern {self.id}",
            context={},
        )

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return list(self._explain)


def _ctx(
    *,
    timestamp: Optional[datetime] = None,
    regime: str = "high_risk_reversal",
    open_position_for: Optional[str] = None,
    recently_emitted: Optional[dict[str, datetime]] = None,
) -> PlaybookContext:
    ts = timestamp or datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)  # 2:30 PM ET
    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=678.4,
        net_gex=7.1e9,
        gamma_flip=676.5,
        put_call_ratio=0.36,
        max_pain=675.0,
        smart_call=-765000.0,
        smart_put=-134000.0,
        recent_closes=[],
        iv_rank=None,
    )
    open_positions = []
    if open_position_for:
        open_positions = [
            OpenPosition(
                pattern_id=open_position_for,
                direction="bearish",
                instrument="BUY_PUT_DEBIT",
                opened_at=ts - timedelta(minutes=30),
                underlying="SPY",
            )
        ]
    return PlaybookContext(
        market=market,
        msi_score=0.0,
        msi_regime=regime,
        msi_components={},
        advanced_signals={},
        basic_signals={},
        levels={"call_wall": 678.0, "put_wall": 674.0, "max_pain": 675.0},
        open_positions=open_positions,
        recently_emitted=recently_emitted or {},
    )


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------


def test_no_patterns_yields_stand_down():
    engine = PlaybookEngine(patterns=[])
    card = engine.evaluate(_ctx())
    assert card.action == ActionEnum.STAND_DOWN
    assert card.pattern == "stand_down"
    assert card.confidence == 0.0


def test_single_match_emitted_unchanged():
    pat = _StubPattern(id="cwf", confidence=0.60)
    engine = PlaybookEngine(patterns=[pat])
    card = engine.evaluate(_ctx())
    assert card.action == ActionEnum.BUY_PUT_DEBIT
    assert card.pattern == "cwf"
    assert card.confidence == 0.60
    assert card.alternatives_considered == []


def test_regime_gate_drops_invalid_regime_patterns():
    # Pattern only valid in chop_range; current regime is trend_expansion.
    pat = _StubPattern(id="cwf", valid_regimes=("chop_range",))
    engine = PlaybookEngine(patterns=[pat])
    card = engine.evaluate(_ctx(regime="trend_expansion"))
    assert card.action == ActionEnum.STAND_DOWN
    # Diagnostic should mention regime mismatch.
    assert any("trend_expansion" in nm.missing[0] for nm in card.near_misses)


def test_confidence_floor_drops_low_conviction_cards():
    weak = _StubPattern(id="weak", confidence=0.21)
    engine = PlaybookEngine(patterns=[weak])
    card = engine.evaluate(_ctx())
    assert card.action == ActionEnum.STAND_DOWN


def test_higher_confidence_wins():
    a = _StubPattern(id="alpha", confidence=0.50)
    b = _StubPattern(id="bravo", confidence=0.70)
    engine = PlaybookEngine(patterns=[a, b])
    card = engine.evaluate(_ctx())
    assert card.pattern == "bravo"
    assert len(card.alternatives_considered) == 1
    assert card.alternatives_considered[0].pattern == "alpha"


def test_tie_broken_by_tier_priority_during_intraday():
    # Both 0.55; intraday window should prefer 0DTE > 1DTE > swing.
    a = _StubPattern(id="alpha_swing", tier="swing", confidence=0.55)
    b = _StubPattern(id="bravo_zdte", tier="0DTE", confidence=0.55)
    engine = PlaybookEngine(patterns=[a, b])
    # 14:00 UTC = 10:00 ET, well inside intraday window.
    ctx = _ctx(timestamp=datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc))
    card = engine.evaluate(ctx)
    assert card.pattern == "bravo_zdte"


def test_hysteresis_suppresses_recent_re_emission():
    pat = _StubPattern(id="cwf", confidence=0.60)
    engine = PlaybookEngine(patterns=[pat])
    now = datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)
    ctx = _ctx(
        timestamp=now,
        recently_emitted={"cwf": now - timedelta(minutes=2)},  # < 5min dwell for 0DTE
    )
    card = engine.evaluate(ctx)
    assert card.action == ActionEnum.STAND_DOWN
    # And after the dwell window expires, it fires again.
    ctx2 = _ctx(
        timestamp=now,
        recently_emitted={"cwf": now - timedelta(minutes=10)},
    )
    card2 = engine.evaluate(ctx2)
    assert card2.pattern == "cwf"


def test_management_card_requires_open_position():
    """TAKE_PROFIT pattern with no matching position is dropped."""
    mgmt = _StubPattern(id="tp", action=ActionEnum.TAKE_PROFIT, confidence=0.80)
    engine = PlaybookEngine(patterns=[mgmt])
    # No open position → drops to STAND_DOWN.
    assert engine.evaluate(_ctx()).action == ActionEnum.STAND_DOWN
    # With matching open position → emits the management Card.
    card = engine.evaluate(_ctx(open_position_for="tp"))
    assert card.action == ActionEnum.TAKE_PROFIT


def test_entry_card_suppressed_when_pattern_already_holding():
    entry = _StubPattern(id="cwf", confidence=0.70, max_hold_minutes=90)
    engine = PlaybookEngine(patterns=[entry])
    # Open position from same pattern, opened 30m ago (within 90m hold).
    card = engine.evaluate(_ctx(open_position_for="cwf"))
    assert card.action == ActionEnum.STAND_DOWN


def test_stand_down_carries_near_miss_diagnostics():
    pat = _StubPattern(
        id="cwf",
        emit=False,
        explain=["price 0.45% from call_wall (needs <= 0.20%)"],
    )
    engine = PlaybookEngine(patterns=[pat])
    card = engine.evaluate(_ctx())
    assert card.action == ActionEnum.STAND_DOWN
    assert len(card.near_misses) == 1
    assert card.near_misses[0].pattern == "cwf"
    assert "0.45%" in card.near_misses[0].missing[0]


def test_stand_down_card_serializes_without_trade_fields():
    pat = _StubPattern(
        id="cwf",
        emit=False,
        explain=["missing trigger"],
    )
    engine = PlaybookEngine(patterns=[pat])
    card = engine.evaluate(_ctx())
    d = card.to_dict()
    assert d["action"] == "STAND_DOWN"
    assert "legs" not in d
    assert "near_misses" in d
    assert d["near_misses"][0]["pattern"] == "cwf"


# ----------------------------------------------------------------------
# Session gate: no Cards while the options market is closed
# ----------------------------------------------------------------------


class _CountingPattern(_StubPattern):
    """A pattern that would always fire, and counts how often it was asked."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.calls = 0

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        self.calls += 1
        return super().match(ctx)


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_pre_market_print_yields_market_closed_stand_down():
    """Card #11270: a 0DTE put issued off QQQ's 04:00 ET print."""
    pat = _CountingPattern(id="max_pain_gravitation")
    engine = PlaybookEngine(patterns=[pat])
    card = engine.evaluate(_ctx(timestamp=_utc(2026, 9, 23, 8, 0)))  # 04:00 EDT
    assert card.action == ActionEnum.STAND_DOWN
    assert card.rationale.startswith("Market closed")
    assert card.near_misses == []
    assert card.context["session"] == "closed"
    assert pat.calls == 0, "no pattern may run outside the session"


def test_session_edges_follow_the_bar_stamp():
    """Bars are stamped at the start of their minute: 09:30 is the first
    regular-session bar and 16:00 the first after-hours one."""
    engine = PlaybookEngine(patterns=[_CountingPattern(id="p")])

    def action_at(ts: datetime) -> ActionEnum:
        return engine.evaluate(_ctx(timestamp=ts)).action

    # 2026-09-23 is EDT (UTC-4).
    assert action_at(_utc(2026, 9, 23, 13, 29)) == ActionEnum.STAND_DOWN  # 09:29
    assert action_at(_utc(2026, 9, 23, 13, 30)) == ActionEnum.BUY_PUT_DEBIT  # 09:30
    assert action_at(_utc(2026, 9, 23, 19, 59)) == ActionEnum.BUY_PUT_DEBIT  # 15:59
    assert action_at(_utc(2026, 9, 23, 20, 0)) == ActionEnum.STAND_DOWN  # 16:00
    assert action_at(_utc(2026, 9, 24, 0, 30)) == ActionEnum.STAND_DOWN  # 20:30


def test_session_gate_follows_standard_time():
    """In January the open is 14:30 UTC, not 13:30."""
    engine = PlaybookEngine(patterns=[_CountingPattern(id="p")])
    early = engine.evaluate(_ctx(timestamp=_utc(2026, 1, 13, 13, 30)))  # 08:30 EST
    open_ = engine.evaluate(_ctx(timestamp=_utc(2026, 1, 13, 14, 30)))  # 09:30 EST
    assert early.action == ActionEnum.STAND_DOWN
    assert open_.action == ActionEnum.BUY_PUT_DEBIT


def test_weekend_and_holiday_are_closed(monkeypatch):
    from src import market_calendar

    engine = PlaybookEngine(patterns=[_CountingPattern(id="p")])
    saturday = _utc(2026, 9, 26, 15, 0)  # 11:00 EDT
    assert engine.evaluate(_ctx(timestamp=saturday)).action == ActionEnum.STAND_DOWN

    thanksgiving = _utc(2026, 11, 26, 16, 0)  # 11:00 EST, a Thursday
    monkeypatch.setattr(market_calendar, "NYSE_HOLIDAYS", {thanksgiving.date()})
    assert engine.evaluate(_ctx(timestamp=thanksgiving)).action == ActionEnum.STAND_DOWN


def test_early_close_day_ends_at_one_pm(monkeypatch):
    from src import market_calendar

    half_day = _utc(2026, 11, 27, 17, 59).date()  # the Friday after Thanksgiving
    monkeypatch.setattr(market_calendar, "NYSE_HALF_DAYS", {half_day})
    engine = PlaybookEngine(patterns=[_CountingPattern(id="p")])
    before = engine.evaluate(_ctx(timestamp=_utc(2026, 11, 27, 17, 59)))  # 12:59 EST
    at_close = engine.evaluate(_ctx(timestamp=_utc(2026, 11, 27, 18, 0)))  # 13:00 EST
    assert before.action == ActionEnum.BUY_PUT_DEBIT
    assert at_close.action == ActionEnum.STAND_DOWN
