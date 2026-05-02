"""Pattern test: overnight_trap_continuation."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.overnight_trap_continuation import (
    PATTERN as OTC,
)
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    trap_signal: str = "bearish_fade",
    trap_triggered: bool = True,
    wall_migrated_up: bool = False,
    wall_migrated_down: bool = False,
    flip_distance_subscore: float = -0.3,
    timestamp: Optional[datetime] = None,
    close: float = 678.50,
    closes: Optional[list[float]] = None,
    odpi_score: float = 0.0,
    odpi_triggered: bool = False,
    positioning_trap_score: float = 25.0,
    skew_delta_score: float = 25.0,
) -> PlaybookContext:
    # 14:45 ET = 18:45 UTC.
    ts = timestamp or datetime(2026, 5, 1, 18, 45, tzinfo=timezone.utc)
    if closes is None:
        # Realistic daily range: spread of ~$3 across the recent window.
        base = 678.0
        closes = [base + 1.5 * (i / 30 - 0.5) + 0.3 * (i % 3) for i in range(35)]
    closes = list(closes)
    closes[-1] = close

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=0.5e9,
        gamma_flip=677.0,
        put_call_ratio=1.0,
        max_pain=678.0,
        smart_call=100000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        vwap=678.0,
    )

    advanced: dict[str, SignalSnapshot] = {
        "trap_detection": SignalSnapshot(
            name="trap_detection",
            score=-35.0 if trap_signal == "bearish_fade" else 35.0,
            clamped_score=-0.35 if trap_signal == "bearish_fade" else 0.35,
            triggered=trap_triggered,
            signal=trap_signal,
            context_values={
                "signal": trap_signal,
                "triggered": trap_triggered,
                "wall_migrated_up": wall_migrated_up,
                "wall_migrated_down": wall_migrated_down,
            },
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
        "positioning_trap": SignalSnapshot(
            name="positioning_trap",
            score=positioning_trap_score,
            clamped_score=positioning_trap_score / 100.0,
        ),
        "skew_delta": SignalSnapshot(
            name="skew_delta",
            score=skew_delta_score,
            clamped_score=skew_delta_score / 100.0,
        ),
    }

    return PlaybookContext(
        market=market,
        msi_score=20.0,
        msi_regime="high_risk_reversal",
        msi_components={
            "gamma_anchor": {"context": {"flip_distance_subscore": flip_distance_subscore}},
        },
        advanced_signals=advanced,
        basic_signals=basic,
        levels={},
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# Happy paths
# ----------------------------------------------------------------------


def test_matches_bearish_fade_trap():
    card = OTC.match(_ctx(trap_signal="bearish_fade"))
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_DEBIT
    assert card.direction == "bearish"
    assert card.tier == "1DTE"
    assert card.pattern == "overnight_trap_continuation"
    assert len(card.legs) == 1
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"
    # OTM put → strike below close.
    assert card.legs[0].strike < 678.5


def test_matches_bullish_fade_trap():
    card = OTC.match(
        _ctx(trap_signal="bullish_fade", positioning_trap_score=-25.0, skew_delta_score=-25.0)
    )
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert card.direction == "bullish"
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    # OTM call → strike above close.
    assert card.legs[0].strike > 678.5


# ----------------------------------------------------------------------
# Gates
# ----------------------------------------------------------------------


def test_too_early_skips():
    early = datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)  # 14:00 ET
    assert OTC.match(_ctx(timestamp=early)) is None


def test_trap_not_triggered_skips():
    assert OTC.match(_ctx(trap_triggered=False)) is None


def test_non_fade_trap_signal_skips():
    assert OTC.match(_ctx(trap_signal="none")) is None


def test_wall_migrated_up_invalidates_setup():
    assert OTC.match(_ctx(trap_signal="bearish_fade", wall_migrated_up=True)) is None


def test_wall_migrated_down_invalidates_setup():
    assert OTC.match(_ctx(trap_signal="bullish_fade", wall_migrated_down=True)) is None


def test_price_at_flip_skips():
    # flip_distance_subscore > 0 = price at flip → skip.
    assert OTC.match(_ctx(flip_distance_subscore=0.5)) is None


# ----------------------------------------------------------------------
# Confidence: opposing 0DTE flow
# ----------------------------------------------------------------------


def test_opposing_0dte_flow_lowers_confidence():
    base = OTC.match(_ctx(trap_signal="bearish_fade", odpi_score=0.0, odpi_triggered=False))
    # Bearish trap fights bullish 0DTE imbalance → opposes.
    opposed = OTC.match(_ctx(trap_signal="bearish_fade", odpi_score=40.0, odpi_triggered=True))
    assert base is not None and opposed is not None
    assert opposed.confidence < base.confidence


# ----------------------------------------------------------------------
# OTM strike scaling
# ----------------------------------------------------------------------


def test_higher_realized_vol_pushes_otm_offset_further():
    # Build two contexts with different sigma, expect strike further OTM
    # for the higher-vol case.
    calm = [678.0 + 0.05 * ((i % 4) - 1.5) for i in range(35)]
    wild = [678.0 + 1.0 * ((i % 4) - 1.5) for i in range(35)]
    calm_card = OTC.match(_ctx(trap_signal="bearish_fade", closes=calm))
    wild_card = OTC.match(_ctx(trap_signal="bearish_fade", closes=wild))
    assert calm_card is not None and wild_card is not None
    # Wild → strike further below close.
    assert wild_card.legs[0].strike < calm_card.legs[0].strike


# ----------------------------------------------------------------------
# Target = prior intraday range midpoint
# ----------------------------------------------------------------------


def test_target_is_prior_range_midpoint():
    closes = [677.0, 678.0, 679.0] * 10
    card = OTC.match(_ctx(trap_signal="bearish_fade", closes=closes, close=679.0))
    assert card is not None
    # Midpoint of [677, 679] = 678.
    assert abs(card.target.ref_price - 678.0) < 1e-6
    assert card.target.level_name == "prior_range_midpoint"


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = OTC.match(_ctx(trap_signal="bearish_fade"))
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "overnight_trap_continuation"
    assert d["context"]["trap_signal"] == "bearish_fade"
    assert d["context"]["direction"] == "bearish"
    assert d["context"]["intraday_range_high"] >= d["context"]["intraday_range_low"]
    assert d["max_hold_minutes"] > 60  # overnight hold
