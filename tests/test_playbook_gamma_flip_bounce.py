"""Pattern test: gamma_flip_bounce — tag-and-reject at the gamma flip."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.gamma_flip_bounce import (
    PATTERN as GAMMA_FLIP_BOUNCE,
    _detect_bounce,
)
from src.signals.playbook.types import ActionEnum


def _bullish_bounce_closes(
    flip: float = 736.0, n_prior: int = 30, n_recent: int = 3
) -> list[float]:
    """Prior 30 bars trade well above the flip, then last 3 bars tag the flip
    (one bar closes just below) and the most recent close rejects back above."""
    prior = [flip + 7.0 + (i % 4) * 0.10 for i in range(n_prior)]  # all > flip
    recent = [flip + 0.30, flip - 0.05, flip + 1.00]  # touch then reject
    return prior + recent


def _bearish_bounce_closes(
    flip: float = 736.0, n_prior: int = 30, n_recent: int = 3
) -> list[float]:
    """Mirror: prior 30 bars below the flip, recent 3 bars tag and reject back down."""
    prior = [flip - 7.0 - (i % 4) * 0.10 for i in range(n_prior)]  # all < flip
    recent = [flip - 0.30, flip + 0.05, flip - 1.00]  # touch then reject
    return prior + recent


def _clean_cross_through_closes(flip: float = 736.0) -> list[float]:
    """Prior below, recent all above — that's a break, not a bounce."""
    prior = [flip - 2.0 - (i % 3) * 0.10 for i in range(30)]
    recent = [flip + 0.05, flip + 0.20, flip + 0.50]
    return prior + recent


def _no_test_closes(flip: float = 736.0) -> list[float]:
    """Price stays far above flip — no tag, no rejection to detect."""
    return [flip + 5.0 + (i % 3) * 0.10 for i in range(35)]


def _ctx(
    *,
    closes: Optional[list[float]] = None,
    flip: float = 736.0,
    close: Optional[float] = None,
    net_gex: float = 3.0e9,
    flip_distance_subscore: float = 0.80,
    rbi_label: str = "Range Fade",
    vol_x_triggered: bool = False,
    timestamp: Optional[datetime] = None,
    regime: str = "controlled_trend",
    call_wall: float = 745.0,
    put_wall: float = 730.0,
    max_gamma_strike: float = 740.0,
    max_pain: float = 738.0,
    tape_score: float = 45.0,
    ofi_score: float = 30.0,
    positioning_trap_score: float = 25.0,
) -> PlaybookContext:
    # Default: 1:45 PM UTC = 9:45 AM ET on a non-DST date (Jan 13, 2026).
    # Picked Jan to keep ET offset stable at -5h independent of pytest tz.
    ts = timestamp or datetime(2026, 1, 13, 14, 45, tzinfo=timezone.utc)
    if closes is None:
        closes = _bullish_bounce_closes(flip)
    if close is None:
        close = closes[-1]

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=net_gex,
        gamma_flip=flip,
        put_call_ratio=1.0,
        max_pain=max_pain,
        smart_call=200000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        extra={"call_wall": call_wall, "put_wall": put_wall},
    )

    advanced: dict[str, SignalSnapshot] = {
        "range_break_imminence": SignalSnapshot(
            name="range_break_imminence",
            score=-10.0,
            clamped_score=-0.10,
            triggered=False,
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
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_score,
            clamped_score=tape_score / 100.0,
        ),
        "order_flow_imbalance": SignalSnapshot(
            name="order_flow_imbalance",
            score=ofi_score,
            clamped_score=ofi_score / 100.0,
        ),
        "positioning_trap": SignalSnapshot(
            name="positioning_trap",
            score=positioning_trap_score,
            clamped_score=positioning_trap_score / 100.0,
        ),
    }
    return PlaybookContext(
        market=market,
        msi_score=50.0,
        msi_regime=regime,
        msi_components={
            "gamma_anchor": {
                "context": {"flip_distance_subscore": flip_distance_subscore}
            },
        },
        advanced_signals=advanced,
        basic_signals=basic,
        levels={
            "call_wall": call_wall,
            "put_wall": put_wall,
            "max_gamma_strike": max_gamma_strike,
            "max_pain": max_pain,
        },
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# Bounce detection
# ----------------------------------------------------------------------


def test_detect_bounce_bullish():
    closes = _bullish_bounce_closes(flip=100.0)
    assert _detect_bounce(closes, 100.0) == "bullish"


def test_detect_bounce_bearish():
    closes = _bearish_bounce_closes(flip=100.0)
    assert _detect_bounce(closes, 100.0) == "bearish"


def test_detect_bounce_returns_none_on_clean_cross_through():
    """A clean below->above sweep with no tag-and-reject must not register."""
    closes = _clean_cross_through_closes(flip=100.0)
    assert _detect_bounce(closes, 100.0) is None


def test_detect_bounce_returns_none_when_price_far_from_flip():
    closes = _no_test_closes(flip=100.0)
    assert _detect_bounce(closes, 100.0) is None


def test_detect_bounce_returns_none_with_too_few_closes():
    assert _detect_bounce([100.5] * 5, 100.0) is None


# ----------------------------------------------------------------------
# Pattern matching - bullish leg
# ----------------------------------------------------------------------


def test_matches_bullish_bounce_at_945_et():
    """The setup from the user's chart: SPY taps gamma flip at 9:45 ET and bounces."""
    card = GAMMA_FLIP_BOUNCE.match(_ctx())
    assert card is not None
    assert card.pattern == "gamma_flip_bounce"
    assert card.direction == "bullish"
    assert card.tier == "0DTE"
    # Entry above flip (rejection confirmed).
    assert card.entry.ref_price > 736.0
    # Stop below flip (failure invalidates setup).
    assert card.stop.ref_price < 736.0
    # Target sits above close in the bounce direction.
    assert card.target.level_name in ("call_wall", "max_gamma_strike")
    assert card.target.ref_price > card.legs[0].strike or card.target.ref_price > 736.0
    # Confidence should be elevated (pattern_base=0.65 + controlled_trend preferred).
    assert card.confidence >= 0.65


def test_matches_bearish_bounce():
    closes = _bearish_bounce_closes(flip=736.0)
    card = GAMMA_FLIP_BOUNCE.match(
        _ctx(closes=closes, close=closes[-1], tape_score=-45.0, ofi_score=-30.0)
    )
    assert card is not None
    assert card.direction == "bearish"
    # Entry below flip.
    assert card.entry.ref_price < 736.0
    # Stop above flip.
    assert card.stop.ref_price > 736.0
    assert card.target.level_name in ("put_wall", "max_gamma_strike")


def test_clean_cross_through_does_not_match():
    """gamma_flip_break territory — bounce pattern should stand down."""
    card = GAMMA_FLIP_BOUNCE.match(_ctx(closes=_clean_cross_through_closes()))
    assert card is None


def test_short_gamma_backdrop_does_not_match():
    """Without long-gamma, dealers don't defend the flip — pattern stands down."""
    card = GAMMA_FLIP_BOUNCE.match(_ctx(net_gex=-1.0e9))
    assert card is None


def test_low_flip_distance_subscore_does_not_match():
    """Price too far from flip — gamma_anchor proximity gates out."""
    card = GAMMA_FLIP_BOUNCE.match(_ctx(flip_distance_subscore=0.3))
    assert card is None


def test_breakout_mode_blocks_pattern():
    """Active range-break overrides bounce logic."""
    card = GAMMA_FLIP_BOUNCE.match(_ctx(rbi_label="Breakout Mode"))
    assert card is None


def test_vol_expansion_triggered_blocks_pattern():
    card = GAMMA_FLIP_BOUNCE.match(_ctx(vol_x_triggered=True))
    assert card is None


def test_no_bullish_flow_does_not_match():
    """Without flow confirmation, the bounce isn't real demand — stand down."""
    card = GAMMA_FLIP_BOUNCE.match(_ctx(tape_score=5.0, ofi_score=5.0))
    assert card is None


def test_allows_entry_at_935_et():
    """First-30min gate is 9:35 ET, not 10:00 ET (key difference from gamma_flip_break)."""
    # 14:35 UTC = 9:35 AM ET
    at_open_window = datetime(2026, 1, 13, 14, 35, tzinfo=timezone.utc)
    card = GAMMA_FLIP_BOUNCE.match(_ctx(timestamp=at_open_window))
    assert card is not None


def test_blocks_entry_before_935_et():
    too_early = datetime(2026, 1, 13, 14, 32, tzinfo=timezone.utc)  # 9:32 ET
    card = GAMMA_FLIP_BOUNCE.match(_ctx(timestamp=too_early))
    assert card is None


def test_target_falls_back_when_no_levels_above():
    """If no call_wall/max_gamma above close, target is None (premium_pct kind)."""
    ctx = _ctx(call_wall=730.0, max_gamma_strike=720.0)  # both below close
    card = GAMMA_FLIP_BOUNCE.match(ctx)
    assert card is not None
    assert card.target.ref_price is None


def test_instrument_switches_with_vol():
    """High realized sigma -> BUY_CALL_DEBIT; low sigma -> SELL_PUT_SPREAD."""
    # Build high-vol closes by oscillating widely.
    flip = 736.0
    high_vol_prior = [
        flip + 7.0 + ((-1) ** i) * (flip * 0.004) for i in range(30)
    ]
    high_vol_recent = [flip + 0.30, flip - 0.05, flip + 1.00]
    card = GAMMA_FLIP_BOUNCE.match(_ctx(closes=high_vol_prior + high_vol_recent))
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert len(card.legs) == 1
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"

    # Default ctx uses low-vol closes -> spread.
    card_low = GAMMA_FLIP_BOUNCE.match(_ctx())
    assert card_low is not None
    assert card_low.action == ActionEnum.SELL_PUT_SPREAD
    assert len(card_low.legs) == 2
    assert card_low.legs[1].strike == card_low.legs[0].strike - 5.0


def test_emitted_card_serializes_to_full_dict():
    card = GAMMA_FLIP_BOUNCE.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "gamma_flip_bounce"
    assert d["direction"] == "bullish"
    assert d["context"]["bounce_direction"] == "bullish"
    assert d["context"]["gamma_flip"] == 736.0


def test_does_not_co_fire_with_gamma_flip_break():
    """A bounce setup must not also satisfy gamma_flip_break — break needs a
    completed cross-through, bounce needs a tag-and-reject."""
    from src.signals.playbook.patterns.gamma_flip_break import (
        PATTERN as GAMMA_FLIP_BREAK,
    )

    ctx = _ctx()
    bounce_card = GAMMA_FLIP_BOUNCE.match(ctx)
    break_card = GAMMA_FLIP_BREAK.match(ctx)
    assert bounce_card is not None
    # gamma_flip_break needs vol_expansion triggered + rbi label in
    # break/breakout modes — our bounce ctx has neither, so it must stand down.
    assert break_card is None


def test_pattern_is_discovered_by_engine():
    """Auto-discovery: PATTERN export must be picked up by the engine."""
    from src.signals.playbook.engine import PlaybookEngine

    engine = PlaybookEngine()
    ids = [p.id for p in engine.patterns]
    assert "gamma_flip_bounce" in ids
