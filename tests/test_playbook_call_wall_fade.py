"""Pattern test: call_wall_fade end-to-end behavior.

Builds a PlaybookContext that satisfies (or selectively breaks) each
trigger condition and asserts the pattern matches / abstains correctly,
and that the emitted Card has the expected instrument and references.
"""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.call_wall_fade import PATTERN as CALL_WALL_FADE
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    close: float = 678.4,
    call_wall: float = 678.0,
    net_gex: float = 7.1e9,
    max_pain: float = 675.0,
    gamma_flip: float = 676.5,
    timestamp: Optional[datetime] = None,
    tape_score: float = -50.0,
    trap_signal: Optional[str] = "bearish_fade",
    rbi_label: str = "Range Fade",
    regime: str = "high_risk_reversal",
    vix_level: float = 16.7,
    recent_closes: Optional[list[float]] = None,
) -> PlaybookContext:
    ts = timestamp or datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)  # 2:30 PM ET
    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=net_gex,
        gamma_flip=gamma_flip,
        put_call_ratio=0.36,
        max_pain=max_pain,
        smart_call=-765000.0,
        smart_put=-134000.0,
        recent_closes=recent_closes or [],
        iv_rank=None,
        extra={"vix_level": vix_level, "call_wall": call_wall},
    )
    advanced: dict[str, SignalSnapshot] = {}
    if trap_signal is not None:
        advanced["trap_detection"] = SignalSnapshot(
            name="trap_detection",
            score=-35.0,
            clamped_score=-0.35,
            triggered=True,
            signal=trap_signal,
            context_values={"signal": trap_signal, "triggered": True},
        )
    advanced["range_break_imminence"] = SignalSnapshot(
        name="range_break_imminence",
        score=20.0,
        clamped_score=0.20,
        triggered=False,
        context_values={"label": rbi_label},
    )
    basic: dict[str, SignalSnapshot] = {
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_score,
            clamped_score=tape_score / 100.0,
        ),
        "positioning_trap": SignalSnapshot(
            name="positioning_trap",
            score=-30.0,
            clamped_score=-0.30,
        ),
    }
    return PlaybookContext(
        market=market,
        msi_score=0.0,
        msi_regime=regime,
        msi_components={},
        advanced_signals=advanced,
        basic_signals=basic,
        levels={"call_wall": call_wall, "max_pain": max_pain},
        open_positions=[],
        recently_emitted={},
    )


def test_matches_when_all_triggers_satisfied():
    card = CALL_WALL_FADE.match(_ctx())
    assert card is not None
    # Default branch (low vol) → SELL_CALL_SPREAD with 2 legs.
    assert card.action == ActionEnum.SELL_CALL_SPREAD
    assert card.pattern == "call_wall_fade"
    assert card.tier == "0DTE"
    assert card.direction == "bearish"
    assert len(card.legs) == 2
    assert card.legs[0].right == "C" and card.legs[0].side == "SELL"
    assert card.legs[1].right == "C" and card.legs[1].side == "BUY"
    # Long leg above short by spread width (default $5).
    assert card.legs[1].strike == card.legs[0].strike + 5.0
    # Target should fall back to max_pain since it's below close.
    assert card.target.level_name == "max_pain"
    assert card.target.ref_price == 675.0
    # Stop sits above the wall.
    assert card.stop.ref_price > card.legs[0].strike
    # Confidence within spec range.
    assert 0.20 <= card.confidence <= 0.95


def test_high_vol_switches_to_put_debit():
    # Build recent_closes with elevated 30-min sigma > 0.0025.
    base = 678.0
    closes = [base + (i % 2) * (base * 0.005) - (base * 0.0025) for i in range(35)]
    card = CALL_WALL_FADE.match(_ctx(recent_closes=closes))
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_DEBIT
    assert len(card.legs) == 1
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"


def test_too_far_from_wall_does_not_match():
    # close 0.6% from call_wall — outside 0.2% trigger.
    card = CALL_WALL_FADE.match(_ctx(close=682.0, call_wall=678.0))
    assert card is None
    misses = CALL_WALL_FADE.explain_miss(_ctx(close=682.0, call_wall=678.0))
    assert any("from call_wall" in m for m in misses)


def test_no_corroborating_advanced_signal_skips():
    card = CALL_WALL_FADE.match(_ctx(trap_signal=None))
    assert card is None


def test_breakout_mode_blocks_pattern():
    card = CALL_WALL_FADE.match(_ctx(rbi_label="Breakout Mode"))
    assert card is None


def test_short_gamma_backdrop_skips():
    card = CALL_WALL_FADE.match(_ctx(net_gex=-1.0e9))
    assert card is None


def test_too_early_in_session_skips():
    # 9:45 AM ET = 13:45 UTC.
    early = datetime(2026, 5, 1, 13, 45, tzinfo=timezone.utc)
    card = CALL_WALL_FADE.match(_ctx(timestamp=early))
    assert card is None


def test_high_vix_reduces_confidence():
    base = CALL_WALL_FADE.match(_ctx(vix_level=15.0))
    high = CALL_WALL_FADE.match(_ctx(vix_level=28.0))
    assert base is not None and high is not None
    assert high.confidence < base.confidence


def test_target_falls_back_to_gamma_flip_when_max_pain_above_close():
    # max_pain above close → ineligible; fall through to gamma_flip below close.
    ctx = _ctx(close=677.5, max_pain=678.5, gamma_flip=676.0)
    card = CALL_WALL_FADE.match(ctx)
    assert card is not None
    assert card.target.level_name == "gamma_flip"
    assert card.target.ref_price == 676.0


def test_emitted_card_serializes_to_full_dict():
    card = CALL_WALL_FADE.match(_ctx())
    assert card is not None
    d = card.to_dict()
    # Spot-check the contract.
    assert d["action"] in ("SELL_CALL_SPREAD", "BUY_PUT_DEBIT")
    assert d["pattern"] == "call_wall_fade"
    assert d["context"]["call_wall"] == 678.0
    assert "trap_detection" in d["context"]["advanced_signals_aligned"]
