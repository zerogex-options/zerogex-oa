"""Pattern test: squeeze_breakout — Tier 3 swing pattern."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.squeeze_breakout import (
    PATTERN as SQB,
    _envelope,
)
from src.signals.playbook.types import ActionEnum


def _bullish_envelope_breakout_closes(envelope_high: float = 678.0) -> list[float]:
    """30 closes inside [677.5, 678.0], then most recent close above 678.0."""
    closes = [677.5 + 0.5 * (i % 2) * 0.5 for i in range(30)]  # 677.5–677.75
    closes.append(envelope_high - 0.05)  # still inside
    closes.append(envelope_high + 0.30)  # break out above
    return closes


def _bearish_envelope_breakout_closes(envelope_low: float = 678.0) -> list[float]:
    closes = [envelope_low + 0.5 * (i % 2) * 0.5 for i in range(30)]
    closes.append(envelope_low + 0.05)
    closes.append(envelope_low - 0.30)
    return closes


def _ctx(
    *,
    closes: Optional[list[float]] = None,
    close: Optional[float] = None,
    net_gex: float = 0.5e9,
    squeeze_triggered: bool = True,
    squeeze_signal: str = "bullish_squeeze",
    vol_x_score: float = 40.0,
    gradient_score: float = 40.0,
    timestamp: Optional[datetime] = None,
    regime: str = "controlled_trend",
    rbi_label: str = "Break Watch",
    call_wall: Optional[float] = 685.0,
    put_wall: Optional[float] = 671.0,
    positioning_trap_score: float = 25.0,
    tape_score: float = 25.0,
    ddp_score: float = 25.0,
) -> PlaybookContext:
    # 11:00 ET = 15:00 UTC.
    ts = timestamp or datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc)
    if closes is None:
        closes = _bullish_envelope_breakout_closes(envelope_high=678.0)
    if close is None:
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
        "squeeze_setup": SignalSnapshot(
            name="squeeze_setup",
            score=35.0,
            clamped_score=0.35,
            triggered=squeeze_triggered,
            signal=squeeze_signal,
            context_values={"signal": squeeze_signal},
        ),
        "vol_expansion": SignalSnapshot(
            name="vol_expansion",
            score=vol_x_score,
            clamped_score=vol_x_score / 100.0,
            triggered=vol_x_score >= 25.0,
            context_values={},
        ),
        "range_break_imminence": SignalSnapshot(
            name="range_break_imminence",
            score=70.0,
            clamped_score=0.70,
            context_values={"label": rbi_label},
        ),
    }

    basic: dict[str, SignalSnapshot] = {
        "gex_gradient": SignalSnapshot(
            name="gex_gradient",
            score=gradient_score,
            clamped_score=gradient_score / 100.0,
        ),
        "positioning_trap": SignalSnapshot(
            name="positioning_trap",
            score=positioning_trap_score,
            clamped_score=positioning_trap_score / 100.0,
        ),
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
# Envelope helper
# ----------------------------------------------------------------------


def test_envelope_returns_min_max_excluding_last_bar():
    closes = [100.0, 101.0, 102.0, 103.0, 104.0, 200.0]  # last bar is "now" (breakout)
    env = _envelope(closes)
    assert env is not None
    low, high = env
    # Last bar (200) excluded.
    assert low == 100.0
    assert high == 104.0


def test_envelope_returns_none_for_too_few_closes():
    assert _envelope([100.0, 101.0]) is None


# ----------------------------------------------------------------------
# Happy paths
# ----------------------------------------------------------------------


def test_matches_bullish_breakout():
    card = SQB.match(_ctx())
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_SPREAD
    assert card.direction == "bullish"
    assert card.tier == "swing"
    assert card.pattern == "squeeze_breakout"
    assert len(card.legs) == 2
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    assert card.legs[1].right == "C" and card.legs[1].side == "SELL"
    # +10 strike-width spread.
    assert card.legs[1].strike == card.legs[0].strike + 10.0
    # Stop at envelope high.
    assert abs(card.stop.ref_price - 678.0) < 0.5
    assert card.stop.level_name == "envelope_reentry"


def test_matches_bearish_breakout():
    closes = _bearish_envelope_breakout_closes(envelope_low=678.0)
    card = SQB.match(
        _ctx(
            closes=closes,
            squeeze_signal="bearish_squeeze",
            gradient_score=-40.0,
            tape_score=-25.0,
            ddp_score=-25.0,
            positioning_trap_score=-25.0,
        )
    )
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_SPREAD
    assert card.direction == "bearish"
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"
    assert card.legs[1].right == "P" and card.legs[1].side == "SELL"
    # Put spread: short below long.
    assert card.legs[1].strike == card.legs[0].strike - 10.0


# ----------------------------------------------------------------------
# Gates
# ----------------------------------------------------------------------


def test_squeeze_not_triggered_skips():
    assert SQB.match(_ctx(squeeze_triggered=False)) is None


def test_low_vol_expansion_score_skips():
    assert SQB.match(_ctx(vol_x_score=20.0)) is None


def test_low_gradient_score_skips():
    assert SQB.match(_ctx(gradient_score=15.0)) is None


def test_entrenched_long_gamma_skips():
    # |net_gex| > 1B → dealer regime entrenched, skip.
    assert SQB.match(_ctx(net_gex=2.5e9)) is None


def test_entrenched_short_gamma_skips():
    assert SQB.match(_ctx(net_gex=-2.5e9)) is None


def test_squeeze_disagrees_with_gradient_skips():
    # Squeeze says bullish but gradient says bearish.
    assert SQB.match(_ctx(squeeze_signal="bullish_squeeze", gradient_score=-40.0)) is None


def test_no_breakout_yet_skips():
    """Close still inside envelope → no confirming breakout."""
    closes = [677.5 + 0.1 * (i % 3) for i in range(32)]  # all inside
    assert SQB.match(_ctx(closes=closes, close=677.6)) is None


def test_too_early_skips():
    early = datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc)  # 9:30 ET
    assert SQB.match(_ctx(timestamp=early)) is None


# ----------------------------------------------------------------------
# Target picker
# ----------------------------------------------------------------------


def test_target_caps_at_call_wall_when_close():
    # Wide envelope ($4 range) → 2 × range = $8 target. With wall just $1
    # above breakout, wall + 0.5% buffer is the closer target.
    closes = [675.5 + 1.0 * (i % 5) for i in range(30)]  # range 675.5–679.5
    closes.append(679.5)  # last in-envelope bar
    closes.append(680.0)  # break-out close
    card = SQB.match(_ctx(closes=closes, call_wall=681.0))
    assert card is not None
    assert card.target.level_name == "call_wall_plus_buffer"


def test_target_falls_through_to_range_when_wall_far():
    closes = _bullish_envelope_breakout_closes(envelope_high=678.0)
    card = SQB.match(_ctx(closes=closes, call_wall=720.0))
    assert card is not None
    assert card.target.level_name == "range_2x"


def test_no_call_wall_uses_range_target():
    closes = _bullish_envelope_breakout_closes(envelope_high=678.0)
    card = SQB.match(_ctx(closes=closes, call_wall=None))
    assert card is not None
    assert card.target.level_name == "range_2x"


# ----------------------------------------------------------------------
# Confidence
# ----------------------------------------------------------------------


def test_range_fade_label_lowers_confidence():
    base = SQB.match(_ctx(rbi_label="Break Watch"))
    fade = SQB.match(_ctx(rbi_label="Range Fade"))
    assert base is not None and fade is not None
    assert fade.confidence < base.confidence


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = SQB.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "squeeze_breakout"
    assert d["tier"] == "swing"
    assert d["context"]["breakout_direction"] in ("bullish", "bearish")
    assert d["context"]["envelope_high"] >= d["context"]["envelope_low"]
    assert d["max_hold_minutes"] >= 24 * 60  # multi-day hold
