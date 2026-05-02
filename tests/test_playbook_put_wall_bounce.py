"""Pattern test: put_wall_bounce — symmetric mirror of call_wall_fade."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.put_wall_bounce import PATTERN as PUT_WALL_BOUNCE
from src.signals.playbook.types import ActionEnum


def _ctx(
    *,
    close: float = 674.6,
    put_wall: float = 674.0,
    net_gex: float = 7.1e9,
    max_pain: float = 678.0,
    gamma_flip: float = 676.5,
    timestamp: Optional[datetime] = None,
    tape_score: float = 50.0,
    trap_signal: Optional[str] = "bullish_fade",
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
        put_call_ratio=1.4,
        max_pain=max_pain,
        smart_call=765000.0,
        smart_put=134000.0,
        recent_closes=recent_closes or [],
        iv_rank=None,
        extra={"vix_level": vix_level, "put_wall": put_wall},
    )
    advanced: dict[str, SignalSnapshot] = {}
    if trap_signal is not None:
        advanced["trap_detection"] = SignalSnapshot(
            name="trap_detection",
            score=35.0,
            clamped_score=0.35,
            triggered=True,
            signal=trap_signal,
            context_values={"signal": trap_signal, "triggered": True},
        )
    advanced["range_break_imminence"] = SignalSnapshot(
        name="range_break_imminence",
        score=-20.0,
        clamped_score=-0.20,
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
            score=30.0,
            clamped_score=0.30,
        ),
    }
    return PlaybookContext(
        market=market,
        msi_score=0.0,
        msi_regime=regime,
        msi_components={},
        advanced_signals=advanced,
        basic_signals=basic,
        levels={"put_wall": put_wall, "max_pain": max_pain},
        open_positions=[],
        recently_emitted={},
    )


def test_matches_when_all_triggers_satisfied():
    card = PUT_WALL_BOUNCE.match(_ctx())
    assert card is not None
    assert card.action == ActionEnum.SELL_PUT_SPREAD
    assert card.pattern == "put_wall_bounce"
    assert card.tier == "0DTE"
    assert card.direction == "bullish"
    assert len(card.legs) == 2
    assert card.legs[0].right == "P" and card.legs[0].side == "SELL"
    assert card.legs[1].right == "P" and card.legs[1].side == "BUY"
    # Long leg below short by spread width.
    assert card.legs[1].strike == card.legs[0].strike - 5.0
    # Bullish target sits above close: max_pain (678) is above 674.6.
    assert card.target.level_name == "max_pain"
    assert card.target.ref_price == 678.0
    # Stop sits below the wall.
    assert card.stop.ref_price < card.legs[0].strike


def test_high_vol_switches_to_call_debit():
    base = 674.0
    closes = [base + (i % 2) * (base * 0.005) - (base * 0.0025) for i in range(35)]
    card = PUT_WALL_BOUNCE.match(_ctx(recent_closes=closes))
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert len(card.legs) == 1
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"


def test_too_far_from_wall_does_not_match():
    card = PUT_WALL_BOUNCE.match(_ctx(close=678.0, put_wall=674.0))
    assert card is None


def test_no_corroborating_advanced_signal_skips():
    card = PUT_WALL_BOUNCE.match(_ctx(trap_signal=None))
    assert card is None


def test_breakout_mode_blocks_pattern():
    card = PUT_WALL_BOUNCE.match(_ctx(rbi_label="Breakout Mode"))
    assert card is None


def test_short_gamma_backdrop_skips():
    card = PUT_WALL_BOUNCE.match(_ctx(net_gex=-1.0e9))
    assert card is None


def test_target_falls_back_to_gamma_flip_when_max_pain_below_close():
    # max_pain below close → ineligible; fall through to gamma_flip above close.
    # Keep close within 0.2% of put_wall to satisfy the proximity trigger.
    ctx = _ctx(close=674.5, put_wall=674.0, max_pain=673.0, gamma_flip=677.0)
    card = PUT_WALL_BOUNCE.match(ctx)
    assert card is not None
    assert card.target.level_name == "gamma_flip"
    assert card.target.ref_price == 677.0


def test_emitted_card_serializes_to_full_dict():
    card = PUT_WALL_BOUNCE.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["action"] in ("SELL_PUT_SPREAD", "BUY_CALL_DEBIT")
    assert d["pattern"] == "put_wall_bounce"
    assert d["context"]["put_wall"] == 674.0
    assert "trap_detection" in d["context"]["advanced_signals_aligned"]


def test_call_wall_fade_and_put_wall_bounce_dont_co_fire():
    """Sanity: bullish put_wall_bounce ctx must not also satisfy call_wall_fade,
    and vice versa.  Their flow gates have opposite signs so they should be
    mutually exclusive."""
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CALL_WALL_FADE

    bullish_ctx = _ctx()
    pwb_card = PUT_WALL_BOUNCE.match(bullish_ctx)
    cwf_card = CALL_WALL_FADE.match(bullish_ctx)
    assert pwb_card is not None
    assert cwf_card is None
