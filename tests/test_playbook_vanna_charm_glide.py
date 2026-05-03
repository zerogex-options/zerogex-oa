"""Pattern test: vanna_charm_glide — Friday-targeted swing drift."""

from datetime import date, datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.vanna_charm_glide import (
    PATTERN as VCG,
    _next_friday,
)
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    vcf_score: float = 50.0,
    timestamp: Optional[datetime] = None,
    close: float = 678.0,
    closes: Optional[list[float]] = None,
    regime: str = "controlled_trend",
    positioning_score: float = 25.0,
    gex_grad_score: float = 25.0,
    tape_score: float = 25.0,
    rbi_label: str = "Weak Range",
) -> PlaybookContext:
    # Default to Wednesday 2026-04-29 12:00 ET = 16:00 UTC.
    ts = timestamp or datetime(2026, 4, 29, 16, 0, tzinfo=timezone.utc)
    if closes is None:
        closes = [677.5 + 0.05 * (i % 10) for i in range(35)]
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
        smart_call=200000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        vwap=678.0,
    )

    advanced: dict[str, SignalSnapshot] = {
        "range_break_imminence": SignalSnapshot(
            name="range_break_imminence",
            score=40.0,
            clamped_score=0.40,
            context_values={"label": rbi_label},
        ),
    }
    basic: dict[str, SignalSnapshot] = {
        "vanna_charm_flow": SignalSnapshot(
            name="vanna_charm_flow",
            score=vcf_score,
            clamped_score=vcf_score / 100.0,
        ),
        "positioning_trap": SignalSnapshot(
            name="positioning_trap",
            score=positioning_score,
            clamped_score=positioning_score / 100.0,
        ),
        "gex_gradient": SignalSnapshot(
            name="gex_gradient",
            score=gex_grad_score,
            clamped_score=gex_grad_score / 100.0,
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
        msi_components={},
        advanced_signals=advanced,
        basic_signals=basic,
        levels={},
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# _next_friday helper
# ----------------------------------------------------------------------


def test_next_friday_from_wednesday():
    # 2026-04-29 (Wed) → 2026-05-01 (Fri).
    assert _next_friday(date(2026, 4, 29)) == date(2026, 5, 1)


def test_next_friday_from_friday_returns_same_day():
    assert _next_friday(date(2026, 5, 1)) == date(2026, 5, 1)


def test_next_friday_from_monday():
    # 2026-04-27 (Mon) → 2026-05-01 (Fri).
    assert _next_friday(date(2026, 4, 27)) == date(2026, 5, 1)


# ----------------------------------------------------------------------
# Happy paths
# ----------------------------------------------------------------------


def test_matches_bullish_drift_on_wednesday():
    card = VCG.match(_ctx(vcf_score=55.0))
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert card.direction == "bullish"
    assert card.tier == "swing"
    assert card.pattern == "vanna_charm_glide"
    assert len(card.legs) == 1
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    # Friday expiry.
    assert card.legs[0].expiry == "2026-05-01"
    # Target above close (bullish).
    assert card.target.ref_price > 678.0
    # max_hold > 1 day, < 4 days (Wed→Fri).
    assert 24 * 60 < card.max_hold_minutes < 4 * 24 * 60


def test_matches_bearish_drift():
    card = VCG.match(
        _ctx(
            vcf_score=-55.0,
            positioning_score=-25.0,
            gex_grad_score=-25.0,
            tape_score=-25.0,
        )
    )
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_DEBIT
    assert card.direction == "bearish"
    assert card.target.ref_price < 678.0


# ----------------------------------------------------------------------
# Day-of-week gate
# ----------------------------------------------------------------------


def test_monday_skips():
    monday = datetime(2026, 4, 27, 16, 0, tzinfo=timezone.utc)
    assert VCG.match(_ctx(timestamp=monday)) is None


def test_friday_skips():
    friday = datetime(2026, 5, 1, 16, 0, tzinfo=timezone.utc)
    assert VCG.match(_ctx(timestamp=friday)) is None


def test_saturday_skips():
    sat = datetime(2026, 5, 2, 16, 0, tzinfo=timezone.utc)
    assert VCG.match(_ctx(timestamp=sat)) is None


# ----------------------------------------------------------------------
# Signal gates
# ----------------------------------------------------------------------


def test_vcf_below_threshold_skips():
    assert VCG.match(_ctx(vcf_score=20.0)) is None


def test_positioning_against_drift_skips():
    # Bullish drift but crowd heavily short → opposes.
    assert VCG.match(_ctx(vcf_score=55.0, positioning_score=-50.0)) is None


def test_neutral_positioning_does_not_block():
    """Small-magnitude positioning_trap shouldn't block the entry."""
    card = VCG.match(_ctx(vcf_score=55.0, positioning_score=-10.0))
    assert card is not None


# ----------------------------------------------------------------------
# Confidence
# ----------------------------------------------------------------------


def test_breakout_mode_label_lowers_confidence():
    base = VCG.match(_ctx(rbi_label="Weak Range"))
    bm = VCG.match(_ctx(rbi_label="Breakout Mode"))
    assert base is not None and bm is not None
    assert bm.confidence < base.confidence


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_emitted_card_serializes_to_full_dict():
    card = VCG.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "vanna_charm_glide"
    assert d["context"]["day_of_week"] in ("Tue", "Wed", "Thu")
    assert d["context"]["friday_expiry"].endswith("-05-01")
    assert d["context"]["atr_dollars"] >= 0
