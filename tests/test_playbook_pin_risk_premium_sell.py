"""Pattern test: pin_risk_premium_sell — 1DTE iron condor at max_pain."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.pin_risk_premium_sell import (
    PATTERN as PIN_RISK,
)
from src.signals.playbook.types import ActionEnum


def _calm_closes(center: float = 678.0, n: int = 35) -> list[float]:
    """Tight closes producing 30-min σ ≈ 0.05% — well below the 0.12% ceiling."""
    return [center + 0.05 * ((i % 4) - 1.5) * 0.5 for i in range(n)]


def _ctx(
    *,
    timestamp: Optional[datetime] = None,
    close: float = 678.0,
    max_pain: float = 678.0,
    net_gex: float = 3.0e9,
    rbi_label: str = "Range Fade",
    closes: Optional[list[float]] = None,
    regime: str = "chop_range",
    vol_x_triggered: bool = False,
    odpi_score: float = 5.0,
    odpi_triggered: bool = False,
    local_gamma_subscore: float = -0.7,
) -> PlaybookContext:
    # 15:35 ET = 19:35 UTC.
    ts = timestamp or datetime(2026, 5, 1, 19, 35, tzinfo=timezone.utc)
    if closes is None:
        closes = _calm_closes(center=close)
    closes = list(closes)
    closes[-1] = close

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=net_gex,
        gamma_flip=677.5,
        put_call_ratio=1.0,
        max_pain=max_pain,
        smart_call=100000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        vwap=678.0,
    )

    advanced: dict[str, SignalSnapshot] = {
        "range_break_imminence": SignalSnapshot(
            name="range_break_imminence",
            score=20.0,
            clamped_score=0.20,
            context_values={"label": rbi_label},
        ),
        "vol_expansion": SignalSnapshot(
            name="vol_expansion",
            score=10.0,
            clamped_score=0.10,
            triggered=vol_x_triggered,
            context_values={},
        ),
        "0dte_position_imbalance": SignalSnapshot(
            name="0dte_position_imbalance",
            score=odpi_score,
            clamped_score=odpi_score / 100.0,
            triggered=odpi_triggered,
            context_values={"flow_source": "zero_dte"},
        ),
    }
    basic: dict[str, SignalSnapshot] = {
        "gex_gradient": SignalSnapshot(
            name="gex_gradient",
            score=5.0,
            clamped_score=0.05,
        ),
    }

    return PlaybookContext(
        market=market,
        msi_score=30.0,
        msi_regime=regime,
        msi_components={
            "gamma_anchor": {"context": {"local_gamma_subscore": local_gamma_subscore}},
            "volatility_regime": {"score": -0.5},
        },
        advanced_signals=advanced,
        basic_signals=basic,
        levels={"max_pain": max_pain},
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# Happy path
# ----------------------------------------------------------------------


def test_matches_when_pinned_long_gamma():
    card = PIN_RISK.match(_ctx())
    assert card is not None
    assert card.action == ActionEnum.SELL_IRON_CONDOR
    assert card.direction == "non_directional"
    assert card.tier == "1DTE"
    assert card.pattern == "pin_risk_premium_sell"
    # Iron condor: 4 legs.
    assert len(card.legs) == 4
    sides = sorted([leg.side for leg in card.legs])
    rights = sorted([leg.right for leg in card.legs])
    assert sides == ["BUY", "BUY", "SELL", "SELL"]
    assert rights == ["C", "C", "P", "P"]
    # Inner short legs are nearer to center than outer long legs.
    short_call = next(leg for leg in card.legs if leg.right == "C" and leg.side == "SELL")
    long_call = next(leg for leg in card.legs if leg.right == "C" and leg.side == "BUY")
    short_put = next(leg for leg in card.legs if leg.right == "P" and leg.side == "SELL")
    long_put = next(leg for leg in card.legs if leg.right == "P" and leg.side == "BUY")
    assert short_call.strike < long_call.strike
    assert short_put.strike > long_put.strike
    # Wings symmetric around max_pain.
    assert (short_call.strike - 678.0) == (678.0 - short_put.strike)


# ----------------------------------------------------------------------
# Gates
# ----------------------------------------------------------------------


def test_too_early_skips():
    early = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)  # 15:00 ET
    assert PIN_RISK.match(_ctx(timestamp=early)) is None


def test_short_gamma_skips():
    assert PIN_RISK.match(_ctx(net_gex=-1.0e9)) is None


def test_far_from_max_pain_skips():
    # 0.50% away — past 0.30% gate.
    assert PIN_RISK.match(_ctx(close=681.5, max_pain=678.0)) is None


def test_high_realized_vol_skips():
    # Inject larger swings so 30-min σ exceeds the 0.12% ceiling.
    closes = [678.0 + ((i % 2) * 4.0 - 2.0) for i in range(35)]
    assert PIN_RISK.match(_ctx(closes=closes)) is None


def test_breakout_rbi_label_skips():
    assert PIN_RISK.match(_ctx(rbi_label="Break Watch")) is None


def test_vol_expansion_triggered_skips():
    assert PIN_RISK.match(_ctx(vol_x_triggered=True)) is None


def test_active_directional_imbalance_skips():
    assert PIN_RISK.match(_ctx(odpi_score=45.0, odpi_triggered=True)) is None


# ----------------------------------------------------------------------
# Wing math
# ----------------------------------------------------------------------


def test_wing_offset_at_least_minimum():
    """Tiny realized σ → wing falls back to _WING_MIN_POINTS (3)."""
    flat = [678.0 + 0.001 * (i % 2) for i in range(35)]
    card = PIN_RISK.match(_ctx(closes=flat))
    assert card is not None
    short_call = next(leg for leg in card.legs if leg.right == "C" and leg.side == "SELL")
    short_put = next(leg for leg in card.legs if leg.right == "P" and leg.side == "SELL")
    # Wing offset ≥ 3 by floor.
    assert short_call.strike - 678.0 >= 3.0
    assert 678.0 - short_put.strike >= 3.0


# ----------------------------------------------------------------------
# Confidence aligned-signal contributions
# ----------------------------------------------------------------------


def test_dense_local_gamma_lifts_confidence():
    weak = PIN_RISK.match(_ctx(local_gamma_subscore=0.0))
    strong = PIN_RISK.match(_ctx(local_gamma_subscore=-0.8))
    assert weak is not None and strong is not None
    assert strong.confidence >= weak.confidence


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = PIN_RISK.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "pin_risk_premium_sell"
    assert d["action"] == "SELL_IRON_CONDOR"
    assert d["context"]["max_pain"] == 678.0
    assert d["context"]["wing_offset"] >= 3.0
    assert d["max_hold_minutes"] > 60  # overnight hold
