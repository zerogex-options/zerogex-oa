"""Pattern test: positioning_trap_squeeze."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.positioning_trap_squeeze import PATTERN as PTS
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    ptrap_score: float = 60.0,
    tape_score: float = -25.0,
    vol_regime_score: float = 0.0,
    timestamp: Optional[datetime] = None,
    close: float = 678.0,
    closes: Optional[list[float]] = None,
    regime: str = "high_risk_reversal",
    skew_score: float = -25.0,
    ddp_score: float = -20.0,
    vcf_score: float = 0.0,
) -> PlaybookContext:
    ts = timestamp or datetime(2026, 5, 1, 16, 0, tzinfo=timezone.utc)
    if closes is None:
        # Range ~$3 across the recent window so 2*range gives a sensible target.
        base = 678.0
        closes = [base + 1.5 * (i / 30 - 0.5) + 0.3 * (i % 3) for i in range(35)]
    closes = list(closes)
    closes[-1] = close

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=0.5e9,
        gamma_flip=678.0,
        put_call_ratio=1.0,
        max_pain=678.0,
        smart_call=100000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        vwap=678.0,
    )

    advanced: dict[str, SignalSnapshot] = {}
    basic: dict[str, SignalSnapshot] = {
        "positioning_trap": SignalSnapshot(
            name="positioning_trap",
            score=ptrap_score,
            clamped_score=ptrap_score / 100.0,
        ),
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_score,
            clamped_score=tape_score / 100.0,
        ),
        "skew_delta": SignalSnapshot(
            name="skew_delta",
            score=skew_score,
            clamped_score=skew_score / 100.0,
        ),
        "dealer_delta_pressure": SignalSnapshot(
            name="dealer_delta_pressure",
            score=ddp_score,
            clamped_score=ddp_score / 100.0,
        ),
        "vanna_charm_flow": SignalSnapshot(
            name="vanna_charm_flow",
            score=vcf_score,
            clamped_score=vcf_score / 100.0,
        ),
    }

    return PlaybookContext(
        market=market,
        msi_score=15.0,
        msi_regime=regime,
        msi_components={"volatility_regime": {"score": vol_regime_score}},
        advanced_signals=advanced,
        basic_signals=basic,
        levels={},
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# Happy paths
# ----------------------------------------------------------------------


def test_long_crowd_squeezes_bearish():
    """Crowd long (ptrap +60) + tape turning bearish → BUY_PUT_SPREAD."""
    card = PTS.match(_ctx(ptrap_score=60.0, tape_score=-25.0))
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_SPREAD
    assert card.direction == "bearish"
    assert card.tier == "swing"
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"
    assert card.legs[1].right == "P" and card.legs[1].side == "SELL"
    # Put spread: short below long.
    assert card.legs[1].strike == card.legs[0].strike - 10.0


def test_short_crowd_squeezes_bullish():
    card = PTS.match(_ctx(ptrap_score=-60.0, tape_score=25.0, ddp_score=20.0))
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_SPREAD
    assert card.direction == "bullish"
    assert card.legs[1].strike == card.legs[0].strike + 10.0


# ----------------------------------------------------------------------
# Gates
# ----------------------------------------------------------------------


def test_crowd_not_extreme_skips():
    assert PTS.match(_ctx(ptrap_score=30.0)) is None


def test_tape_aligned_with_crowd_skips():
    # Long crowd + bullish tape → no squeeze.
    assert PTS.match(_ctx(ptrap_score=60.0, tape_score=25.0)) is None


def test_tape_too_weak_skips():
    # Long crowd, tape only -5 → not enough turn.
    assert PTS.match(_ctx(ptrap_score=60.0, tape_score=-5.0)) is None


def test_low_vol_regime_skips():
    assert PTS.match(_ctx(vol_regime_score=-0.5)) is None


# ----------------------------------------------------------------------
# Confidence: opposing vanna_charm_flow penalty
# ----------------------------------------------------------------------


def test_opposing_vanna_charm_flow_lowers_confidence():
    # Bearish squeeze + bullish vanna → opposes.
    base = PTS.match(_ctx(ptrap_score=60.0, tape_score=-25.0, vcf_score=0.0))
    opposed = PTS.match(_ctx(ptrap_score=60.0, tape_score=-25.0, vcf_score=40.0))
    assert base is not None and opposed is not None
    assert opposed.confidence < base.confidence


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = PTS.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "positioning_trap_squeeze"
    assert d["context"]["squeeze_direction"] in ("bullish", "bearish")
    assert d["context"]["positioning_trap_score"] != 0
    assert d["max_hold_minutes"] >= 24 * 60
