"""Action Card serialization tests."""

from datetime import datetime, timezone

from src.signals.playbook.types import (
    ActionCard,
    ActionEnum,
    Alternative,
    Entry,
    Leg,
    NearMiss,
    Stop,
    Target,
    clamp_confidence,
)


def _trade_card(**overrides) -> ActionCard:
    defaults = dict(
        underlying="SPY",
        timestamp=datetime(2026, 5, 1, 14, 30, tzinfo=timezone.utc),
        action=ActionEnum.SELL_CALL_SPREAD,
        pattern="call_wall_fade",
        tier="0DTE",
        direction="bearish",
        confidence=0.68,
        size_multiplier=0.6,
        max_hold_minutes=90,
        legs=[
            Leg(expiry="2026-05-01", strike=678.0, right="C", side="SELL", qty=1),
            Leg(expiry="2026-05-01", strike=683.0, right="C", side="BUY", qty=1),
        ],
        entry=Entry(ref_price=678.40, trigger="at_touch"),
        target=Target(ref_price=675.0, kind="level", level_name="max_pain"),
        stop=Stop(ref_price=680.03, kind="level", level_name="call_wall_break"),
        rationale="Fade rally into close.",
        context={"msi": 0.0},
        alternatives_considered=[Alternative(pattern="put_wall_bounce", reason="lower confidence")],
    )
    defaults.update(overrides)
    return ActionCard(**defaults)


def test_trade_card_to_dict_has_full_shape():
    card = _trade_card()
    d = card.to_dict()
    assert d["action"] == "SELL_CALL_SPREAD"
    assert d["pattern"] == "call_wall_fade"
    assert d["tier"] == "0DTE"
    assert d["confidence"] == 0.68
    assert len(d["legs"]) == 2
    assert d["legs"][0]["side"] == "SELL"
    assert d["legs"][1]["side"] == "BUY"
    assert d["entry"]["trigger"] == "at_touch"
    assert d["target"]["level_name"] == "max_pain"
    assert d["stop"]["ref_price"] == 680.03
    assert d["alternatives_considered"][0]["pattern"] == "put_wall_bounce"
    # near_misses should NOT appear on a trade Card.
    assert "near_misses" not in d


def test_stand_down_card_drops_trade_fields_and_keeps_near_misses():
    card = ActionCard(
        underlying="SPY",
        timestamp=datetime(2026, 5, 1, 14, 30, tzinfo=timezone.utc),
        action=ActionEnum.STAND_DOWN,
        pattern="stand_down",
        tier="n/a",
        direction="non_directional",
        confidence=0.0,
        rationale="No tradable structure.",
        near_misses=[NearMiss(pattern="call_wall_fade", missing=["price 0.5% from wall"])],
        context={"msi": 32.5, "regime": "chop_range"},
    )
    d = card.to_dict()
    assert d["action"] == "STAND_DOWN"
    # Trade fields stripped.
    for k in ("legs", "entry", "target", "stop", "size_multiplier", "max_hold_minutes"):
        assert k not in d, f"STAND_DOWN must not carry {k}"
    # near_misses retained, with missing list intact.
    assert d["near_misses"][0]["pattern"] == "call_wall_fade"
    assert "0.5%" in d["near_misses"][0]["missing"][0]


def test_timestamp_serialized_as_iso_string():
    card = _trade_card()
    d = card.to_dict()
    assert isinstance(d["timestamp"], str)
    assert d["timestamp"].startswith("2026-05-01T14:30")


def test_clamp_confidence_floor_and_ceiling():
    assert clamp_confidence(0.0) == 0.20
    assert clamp_confidence(0.99) == 0.95
    assert clamp_confidence(0.5) == 0.5
    # NaN guard.
    assert clamp_confidence(float("nan")) == 0.20
