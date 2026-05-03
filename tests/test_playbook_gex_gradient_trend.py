"""Pattern test: gex_gradient_trend."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.gex_gradient_trend import PATTERN as GGT
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    gradient_score: float = 50.0,
    net_gex: float = 0.5e9,
    timestamp: Optional[datetime] = None,
    last_close_delta: float = 0.05,  # > 0 → bullish confirming bar
    closes: Optional[list[float]] = None,
    regime: str = "controlled_trend",
    rbi_label: str = "Weak Range",
    vol_regime_score: float = 0.0,
    vol_x_triggered: bool = False,
    ddp_score: float = 25.0,
    tape_score: float = 25.0,
) -> PlaybookContext:
    ts = timestamp or datetime(2026, 5, 1, 16, 0, tzinfo=timezone.utc)
    if closes is None:
        closes = [678.0 + 0.5 * (i % 4 - 1.5) for i in range(35)]
    closes = list(closes)
    # Force the last bar to confirm (or contradict) the drift direction.
    if len(closes) >= 2:
        closes[-1] = closes[-2] + last_close_delta
    close = closes[-1]

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=net_gex,
        gamma_flip=678.0,
        put_call_ratio=1.0,
        max_pain=678.0,
        smart_call=200000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        vwap=678.0,
    )

    advanced: dict[str, SignalSnapshot] = {
        "range_break_imminence": SignalSnapshot(
            name="range_break_imminence",
            score=30.0,
            clamped_score=0.30,
            context_values={"label": rbi_label},
        ),
        "vol_expansion": SignalSnapshot(
            name="vol_expansion",
            score=10.0,
            clamped_score=0.10,
            triggered=vol_x_triggered,
            context_values={},
        ),
    }
    basic: dict[str, SignalSnapshot] = {
        "gex_gradient": SignalSnapshot(
            name="gex_gradient",
            score=gradient_score,
            clamped_score=gradient_score / 100.0,
        ),
        "dealer_delta_pressure": SignalSnapshot(
            name="dealer_delta_pressure",
            score=ddp_score,
            clamped_score=ddp_score / 100.0,
        ),
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_score,
            clamped_score=tape_score / 100.0,
        ),
    }

    return PlaybookContext(
        market=market,
        msi_score=55.0,
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


def test_matches_bullish_drift():
    card = GGT.match(_ctx(gradient_score=50.0, net_gex=0.5e9))
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert card.direction == "bullish"
    assert card.tier == "swing"
    assert card.pattern == "gex_gradient_trend"
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    # OTM call → strike above close.
    assert card.legs[0].strike >= 678.0
    # Target above close.
    assert card.target.ref_price > 678.0


def test_matches_bearish_drift():
    card = GGT.match(
        _ctx(
            gradient_score=-50.0,
            net_gex=-0.5e9,
            last_close_delta=-0.05,
            ddp_score=-25.0,
            tape_score=-25.0,
        )
    )
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_DEBIT
    assert card.direction == "bearish"
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"
    assert card.target.ref_price < card.entry.ref_price


# ----------------------------------------------------------------------
# Gates
# ----------------------------------------------------------------------


def test_low_gradient_skips():
    assert GGT.match(_ctx(gradient_score=20.0)) is None


def test_net_gex_disagrees_with_gradient_skips():
    # Bullish gradient but net_gex negative → signs differ, skip.
    assert GGT.match(_ctx(gradient_score=50.0, net_gex=-0.5e9)) is None


def test_breakout_mode_skips():
    assert GGT.match(_ctx(rbi_label="Breakout Mode")) is None


def test_low_vol_regime_skips():
    assert GGT.match(_ctx(vol_regime_score=-0.7)) is None


def test_no_confirming_bar_skips():
    # Bullish gradient but last bar moved down.
    assert GGT.match(_ctx(gradient_score=50.0, last_close_delta=-0.10)) is None


# ----------------------------------------------------------------------
# Confidence: vol_expansion triggered
# ----------------------------------------------------------------------


def test_vol_expansion_triggered_lowers_confidence():
    base = GGT.match(_ctx(vol_x_triggered=False))
    breakout = GGT.match(_ctx(vol_x_triggered=True))
    assert base is not None and breakout is not None
    assert breakout.confidence < base.confidence


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = GGT.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "gex_gradient_trend"
    assert d["context"]["drift_direction"] in ("bullish", "bearish")
    assert d["context"]["gex_gradient_score"] != 0
    assert d["context"]["atr_dollars"] >= 0
