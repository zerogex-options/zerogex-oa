"""Pattern test: eod_pressure_drift — last-hour drift trade."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.eod_pressure_drift import PATTERN as EOD_DRIFT
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    eod_score: float = 35.0,
    eod_triggered: bool = True,
    timestamp: Optional[datetime] = None,
    close: float = 678.50,
    vwap: float = 678.00,
    call_wall: Optional[float] = 681.0,
    put_wall: Optional[float] = 675.0,
    last_close_delta: float = 0.05,  # > 0 → bullish confirming bar
    regime: str = "controlled_trend",
    odpi_triggered: bool = True,
    ddp_score: float = 25.0,
    tape_score: float = 0.0,
) -> PlaybookContext:
    # 15:30 ET = 19:30 UTC, well inside the EOD window.
    ts = timestamp or datetime(2026, 5, 1, 19, 30, tzinfo=timezone.utc)
    closes = [678.0 + i * 0.01 for i in range(20)]
    # Override the last bar to control the confirming-bar gate.
    if len(closes) >= 2:
        closes[-1] = closes[-2] + last_close_delta

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
        vwap=vwap,
    )

    advanced: dict[str, SignalSnapshot] = {
        "eod_pressure": SignalSnapshot(
            name="eod_pressure",
            score=eod_score,
            clamped_score=eod_score / 100.0,
            triggered=eod_triggered,
            context_values={},
        ),
        "0dte_position_imbalance": SignalSnapshot(
            name="0dte_position_imbalance",
            score=20.0 if eod_score > 0 else -20.0,
            clamped_score=0.20 if eod_score > 0 else -0.20,
            triggered=odpi_triggered,
            context_values={},
        ),
    }

    basic: dict[str, SignalSnapshot] = {
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_score,
            clamped_score=tape_score / 100.0,
        ),
        "dealer_delta_pressure": SignalSnapshot(
            name="dealer_delta_pressure",
            score=ddp_score,
            clamped_score=ddp_score / 100.0,
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
    card = EOD_DRIFT.match(_ctx(eod_score=40.0))
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert card.direction == "bullish"
    assert card.tier == "0DTE"
    assert card.pattern == "eod_pressure_drift"
    assert len(card.legs) == 1
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    assert card.entry.trigger == "at_market"


def test_matches_bearish_drift():
    card = EOD_DRIFT.match(_ctx(eod_score=-40.0, close=677.5, vwap=678.0, last_close_delta=-0.05))
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_DEBIT
    assert card.direction == "bearish"
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"


# ----------------------------------------------------------------------
# Time-of-day gates
# ----------------------------------------------------------------------


def test_too_early_skips():
    early = datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)  # 14:30 ET
    card = EOD_DRIFT.match(_ctx(timestamp=early))
    assert card is None


def test_too_close_to_bell_skips():
    # 15:56 ET — minutes_to_close = 4, < 5-min buffer.
    very_late = datetime(2026, 5, 1, 19, 56, tzinfo=timezone.utc)
    card = EOD_DRIFT.match(_ctx(timestamp=very_late))
    assert card is None


def test_max_hold_minutes_decreases_near_close():
    # 15:30 ET → 25 min to close minus 5 buffer = 20 min hold.
    ts_early = datetime(2026, 5, 1, 19, 30, tzinfo=timezone.utc)
    ts_late = datetime(2026, 5, 1, 19, 50, tzinfo=timezone.utc)  # 5 min to close after buffer
    early = EOD_DRIFT.match(_ctx(timestamp=ts_early))
    late = EOD_DRIFT.match(_ctx(timestamp=ts_late))
    assert early is not None and late is not None
    assert early.max_hold_minutes > late.max_hold_minutes


# ----------------------------------------------------------------------
# Signal gates
# ----------------------------------------------------------------------


def test_eod_below_threshold_skips():
    card = EOD_DRIFT.match(_ctx(eod_score=20.0))
    assert card is None


def test_eod_not_triggered_skips():
    card = EOD_DRIFT.match(_ctx(eod_triggered=False))
    assert card is None


def test_no_confirming_bar_skips():
    # Bullish drift but last bar was negative.
    card = EOD_DRIFT.match(_ctx(eod_score=40.0, last_close_delta=-0.10))
    assert card is None


# ----------------------------------------------------------------------
# Wall blockers
# ----------------------------------------------------------------------


def test_call_wall_blocker_skips_bullish():
    # call_wall just 0.20% above close — too close.
    blocked = _ctx(eod_score=40.0, close=678.5, call_wall=678.5 * 1.0020)
    assert EOD_DRIFT.match(blocked) is None
    # But 0.50% away → fine.
    clear = _ctx(eod_score=40.0, close=678.5, call_wall=678.5 * 1.0050)
    assert EOD_DRIFT.match(clear) is not None


def test_put_wall_blocker_skips_bearish():
    blocked = _ctx(
        eod_score=-40.0,
        close=678.5,
        put_wall=678.5 * 0.9980,
        vwap=679.0,
        last_close_delta=-0.05,
    )
    assert EOD_DRIFT.match(blocked) is None


# ----------------------------------------------------------------------
# Target / stop math
# ----------------------------------------------------------------------


def test_target_uses_vwap_extension_formula():
    """Target = 1.5*close - 0.5*vwap (algebraic collapse of both directions)."""
    close = 680.0
    vwap = 678.0
    # Push call_wall well above close so the blocker gate doesn't trip.
    card = EOD_DRIFT.match(_ctx(eod_score=40.0, close=close, vwap=vwap, call_wall=685.0))
    assert card is not None
    expected = 1.5 * close - 0.5 * vwap
    assert abs(card.target.ref_price - expected) < 1e-6
    assert card.target.level_name == "vwap_extension"


def test_stop_at_vwap():
    card = EOD_DRIFT.match(_ctx(close=680.0, vwap=678.0, eod_score=40.0, call_wall=685.0))
    assert card is not None
    assert abs(card.stop.ref_price - 678.0) < 1e-6
    assert card.stop.level_name == "vwap_cross"


def test_vwap_unavailable_skips():
    ctx = _ctx(eod_score=40.0)
    ctx.market.vwap = None
    assert EOD_DRIFT.match(ctx) is None


# ----------------------------------------------------------------------
# Confluence
# ----------------------------------------------------------------------


def test_opposing_tape_flow_bias_lowers_confidence():
    aligned = EOD_DRIFT.match(_ctx(eod_score=40.0, tape_score=0.0))
    opposed = EOD_DRIFT.match(_ctx(eod_score=40.0, tape_score=-50.0))
    assert aligned is not None and opposed is not None
    assert opposed.confidence < aligned.confidence


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = EOD_DRIFT.match(_ctx(eod_score=40.0))
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "eod_pressure_drift"
    assert d["context"]["drift_direction"] in ("bullish", "bearish")
    assert d["context"]["eod_pressure_score"] == 40.0
    assert d["context"]["minutes_to_close"] > 0
