"""Pattern test: skew_inversion_reversal — Tier 3 swing fear-spike fade."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.skew_inversion_reversal import PATTERN as SIR
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    skew_score: float = -65.0,
    tape_score: float = 5.0,
    vol_regime_score: float = 0.5,
    close: float = 678.0,
    closes: Optional[list[float]] = None,
    timestamp: Optional[datetime] = None,
    regime: str = "chop_range",
    vanna_score: float = 25.0,
    positioning_score: float = 30.0,
    ddp_score: float = -10.0,
) -> PlaybookContext:
    ts = timestamp or datetime(2026, 5, 1, 16, 0, tzinfo=timezone.utc)  # 12:00 ET
    if closes is None:
        # Mean ≈ 678 → close 678 within 0.5% of MA proxy.
        closes = [677.5 + 0.05 * (i % 10) for i in range(35)]
    closes = list(closes)
    closes[-1] = close

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=1.0e9,
        gamma_flip=678.0,
        put_call_ratio=1.5,
        max_pain=678.0,
        smart_call=100000.0,
        smart_put=200000.0,
        recent_closes=closes,
        iv_rank=None,
        vwap=678.0,
    )

    advanced: dict[str, SignalSnapshot] = {}
    basic: dict[str, SignalSnapshot] = {
        "skew_delta": SignalSnapshot(
            name="skew_delta",
            score=skew_score,
            clamped_score=skew_score / 100.0,
        ),
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_score,
            clamped_score=tape_score / 100.0,
        ),
        "vanna_charm_flow": SignalSnapshot(
            name="vanna_charm_flow",
            score=vanna_score,
            clamped_score=vanna_score / 100.0,
        ),
        "positioning_trap": SignalSnapshot(
            name="positioning_trap",
            score=positioning_score,
            clamped_score=positioning_score / 100.0,
        ),
        "dealer_delta_pressure": SignalSnapshot(
            name="dealer_delta_pressure",
            score=ddp_score,
            clamped_score=ddp_score / 100.0,
        ),
    }

    return PlaybookContext(
        market=market,
        msi_score=40.0,
        msi_regime=regime,
        msi_components={
            "volatility_regime": {"score": vol_regime_score},
        },
        advanced_signals=advanced,
        basic_signals=basic,
        levels={},
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# Happy path
# ----------------------------------------------------------------------


def test_matches_when_fear_extreme_but_tape_neutral():
    card = SIR.match(_ctx())
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert card.direction == "bullish"
    assert card.tier == "swing"
    assert card.pattern == "skew_inversion_reversal"
    assert len(card.legs) == 1
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    # OTM call → strike above close.
    assert card.legs[0].strike > 678.0
    # Entry at next session open.
    assert card.entry.trigger == "at_open_next"
    # Stop is signal-event (premium% or skew new low).
    assert card.stop.kind == "signal_event"


# ----------------------------------------------------------------------
# Gates
# ----------------------------------------------------------------------


def test_skew_not_extreme_enough_skips():
    assert SIR.match(_ctx(skew_score=-30.0)) is None


def test_bearish_tape_skips():
    assert SIR.match(_ctx(tape_score=-15.0)) is None


def test_low_vol_regime_skips():
    assert SIR.match(_ctx(vol_regime_score=0.1)) is None


def test_close_far_from_ma_skips():
    # Push close far above the MA proxy (~678) → exceeds 0.5%.
    far = _ctx(close=685.0)
    assert SIR.match(far) is None


def test_missing_volatility_regime_component_skips():
    ctx = _ctx()
    ctx.msi_components = {}
    assert SIR.match(ctx) is None


# ----------------------------------------------------------------------
# Target / OTM strike scaling
# ----------------------------------------------------------------------


def test_deeper_skew_pushes_target_further():
    """Greater fear (more negative skew) → larger expected upside revert."""
    shallow = SIR.match(_ctx(skew_score=-55.0))
    deep = SIR.match(_ctx(skew_score=-90.0))
    assert shallow is not None and deep is not None
    assert deep.target.ref_price > shallow.target.ref_price


def test_higher_realized_vol_pushes_otm_strike_further():
    calm = [678.0 + 0.05 * ((i % 4) - 1.5) for i in range(35)]
    wild = [678.0 + 1.0 * ((i % 4) - 1.5) for i in range(35)]
    calm_card = SIR.match(_ctx(closes=calm))
    wild_card = SIR.match(_ctx(closes=wild))
    assert calm_card is not None and wild_card is not None
    assert wild_card.legs[0].strike > calm_card.legs[0].strike


# ----------------------------------------------------------------------
# Confluence
# ----------------------------------------------------------------------


def test_opposing_dealer_delta_pressure_lowers_confidence():
    aligned = SIR.match(_ctx(ddp_score=20.0))  # bullish DDP — neutral on SIR's "against" axis
    opposed = SIR.match(_ctx(ddp_score=-40.0))  # bearish DDP opposes bullish bias
    assert aligned is not None and opposed is not None
    assert opposed.confidence < aligned.confidence


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = SIR.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "skew_inversion_reversal"
    assert d["context"]["skew_delta_score"] < 0
    assert d["context"]["volatility_regime_score"] >= 0.3
    assert d["max_hold_minutes"] >= 24 * 60  # multi-day
