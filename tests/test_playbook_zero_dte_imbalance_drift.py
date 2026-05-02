"""Pattern test: zero_dte_imbalance_drift."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.zero_dte_imbalance_drift import (
    PATTERN as ZDID,
)
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    odpi_score: float = 35.0,
    odpi_triggered: bool = True,
    flow_source: str = "zero_dte",
    timestamp: Optional[datetime] = None,
    close: float = 678.50,
    call_wall: Optional[float] = 690.0,
    put_wall: Optional[float] = 670.0,
    regime: str = "controlled_trend",
    trap_signal: Optional[str] = None,
    trap_triggered: bool = False,
    rbi_label: str = "Weak Range",
    tape_score: float = 25.0,
    vanna_score: float = 25.0,
) -> PlaybookContext:
    # 12:30 ET = 16:30 UTC, dead-center of the midday window.
    ts = timestamp or datetime(2026, 5, 1, 16, 30, tzinfo=timezone.utc)
    # Synthetic recent_closes giving realistic SPY-magnitude sigma
    # (~0.10–0.15% per minute) so wall-vs-sigma target tests are meaningful.
    base = 678.0
    closes = [base + 0.5 * ((i % 4) - 1.5) for i in range(35)]
    closes[-1] = close

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=0.5e9,
        gamma_flip=677.0,
        put_call_ratio=1.0,
        max_pain=678.0,
        smart_call=200000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        vwap=678.0,
    )

    advanced: dict[str, SignalSnapshot] = {
        "0dte_position_imbalance": SignalSnapshot(
            name="0dte_position_imbalance",
            score=odpi_score,
            clamped_score=odpi_score / 100.0,
            triggered=odpi_triggered,
            context_values={"flow_source": flow_source},
        ),
        "range_break_imminence": SignalSnapshot(
            name="range_break_imminence",
            score=50.0,
            clamped_score=0.50,
            context_values={"label": rbi_label},
        ),
    }
    if trap_signal is not None:
        advanced["trap_detection"] = SignalSnapshot(
            name="trap_detection",
            score=-30.0 if trap_signal == "bearish_fade" else 30.0,
            clamped_score=-0.30 if trap_signal == "bearish_fade" else 0.30,
            triggered=trap_triggered,
            signal=trap_signal,
            context_values={"signal": trap_signal, "triggered": trap_triggered},
        )

    basic: dict[str, SignalSnapshot] = {
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
    }

    levels: dict[str, Optional[float]] = {}
    if call_wall is not None:
        levels["call_wall"] = call_wall
    if put_wall is not None:
        levels["put_wall"] = put_wall

    return PlaybookContext(
        market=market,
        msi_score=55.0,
        msi_regime=regime,
        msi_components={},
        advanced_signals=advanced,
        basic_signals=basic,
        levels=levels,
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# Happy paths
# ----------------------------------------------------------------------


def test_matches_bullish_drift():
    card = ZDID.match(_ctx(odpi_score=40.0))
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_SPREAD
    assert card.direction == "bullish"
    assert card.tier == "0DTE"
    assert card.pattern == "zero_dte_imbalance_drift"
    assert len(card.legs) == 2
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    assert card.legs[1].right == "C" and card.legs[1].side == "SELL"
    # Long leg below short leg by spread width.
    assert card.legs[1].strike == card.legs[0].strike + 5.0


def test_matches_bearish_drift():
    card = ZDID.match(_ctx(odpi_score=-40.0, tape_score=-25.0, vanna_score=-25.0))
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_SPREAD
    assert card.direction == "bearish"
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"
    assert card.legs[1].right == "P" and card.legs[1].side == "SELL"
    # Short leg below long by spread width (put spread).
    assert card.legs[1].strike == card.legs[0].strike - 5.0


# ----------------------------------------------------------------------
# Time-of-day gates
# ----------------------------------------------------------------------


def test_too_early_skips():
    # 10:30 ET = 14:30 UTC.
    early = datetime(2026, 5, 1, 14, 30, tzinfo=timezone.utc)
    assert ZDID.match(_ctx(timestamp=early)) is None


def test_too_late_skips():
    # 14:45 ET = 18:45 UTC — past 14:30 cutoff.
    late = datetime(2026, 5, 1, 18, 45, tzinfo=timezone.utc)
    assert ZDID.match(_ctx(timestamp=late)) is None


# ----------------------------------------------------------------------
# Signal gates
# ----------------------------------------------------------------------


def test_odpi_below_threshold_skips():
    assert ZDID.match(_ctx(odpi_score=20.0)) is None


def test_odpi_not_triggered_skips():
    assert ZDID.match(_ctx(odpi_triggered=False)) is None


def test_all_expiry_fallback_skips():
    assert ZDID.match(_ctx(flow_source="all_expiry_fallback")) is None


# ----------------------------------------------------------------------
# Trap conflict gate
# ----------------------------------------------------------------------


def test_trap_inactive_does_not_block():
    card = ZDID.match(_ctx(odpi_score=40.0, trap_signal=None))
    assert card is not None


def test_trap_aligned_with_drift_does_not_block():
    # Bullish drift + bullish_fade trap (also bullish) → fine.
    card = ZDID.match(_ctx(odpi_score=40.0, trap_signal="bullish_fade", trap_triggered=True))
    assert card is not None


def test_trap_opposing_drift_blocks():
    # Bullish drift + bearish_fade trap → conflict, skip.
    card = ZDID.match(_ctx(odpi_score=40.0, trap_signal="bearish_fade", trap_triggered=True))
    assert card is None


def test_trap_triggered_but_signal_none_blocks():
    card = ZDID.match(_ctx(odpi_score=40.0, trap_signal="none", trap_triggered=True))
    assert card is None


# ----------------------------------------------------------------------
# Range-fade confidence penalty
# ----------------------------------------------------------------------


def test_range_fade_label_lowers_confidence():
    base = ZDID.match(_ctx(odpi_score=40.0, rbi_label="Weak Range"))
    fade = ZDID.match(_ctx(odpi_score=40.0, rbi_label="Range Fade"))
    assert base is not None and fade is not None
    assert fade.confidence < base.confidence


# ----------------------------------------------------------------------
# Target picker
# ----------------------------------------------------------------------


def test_close_call_wall_caps_target_below_sigma_target():
    # Wall just $1 above close → must be closer than 2σ × close (~$1+ for SPY-magnitude vol).
    card = ZDID.match(_ctx(odpi_score=40.0, close=678.5, call_wall=679.5))
    assert card is not None
    assert card.target.level_name == "call_wall"


def test_far_call_wall_falls_through_to_atr_target():
    # Wall $20 above close → way past 2σ → use sigma target.
    card = ZDID.match(_ctx(odpi_score=40.0, close=678.5, call_wall=698.5))
    assert card is not None
    assert card.target.level_name == "atr_2x"


def test_no_wall_uses_atr_target():
    card = ZDID.match(_ctx(odpi_score=40.0, call_wall=None))
    assert card is not None
    assert card.target.level_name == "atr_2x"


# ----------------------------------------------------------------------
# Stop math
# ----------------------------------------------------------------------


def test_stop_below_close_for_bullish_drift():
    card = ZDID.match(_ctx(odpi_score=40.0, close=678.5))
    assert card is not None
    assert card.stop.ref_price < 678.5
    assert card.stop.level_name == "atr_stop"


def test_stop_above_close_for_bearish_drift():
    card = ZDID.match(_ctx(odpi_score=-40.0, close=678.5, tape_score=-25.0, vanna_score=-25.0))
    assert card is not None
    assert card.stop.ref_price > 678.5


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = ZDID.match(_ctx(odpi_score=40.0))
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "zero_dte_imbalance_drift"
    assert d["context"]["flow_source"] == "zero_dte"
    assert d["context"]["drift_direction"] in ("bullish", "bearish")
    assert d["context"]["atr_proxy_dollars"] >= 0
