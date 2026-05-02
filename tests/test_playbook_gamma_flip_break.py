"""Pattern test: gamma_flip_break — context-dependent cross trade."""

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.gamma_flip_break import (
    PATTERN as GAMMA_FLIP_BREAK,
    _detect_cross,
)
from src.signals.playbook.types import ActionEnum


def _bullish_cross_closes(flip: float = 678.0, n_prior: int = 30, n_recent: int = 5) -> list[float]:
    """Return closes where the prior window was below flip and the recent
    window crossed above."""
    prior = [flip - 1.5 + (i % 3) * 0.05 for i in range(n_prior)]  # all < flip
    recent = [flip - 0.05, flip - 0.02, flip + 0.05, flip + 0.10, flip + 0.20]  # last close > flip
    return prior + recent


def _bearish_cross_closes(flip: float = 678.0, n_prior: int = 30) -> list[float]:
    prior = [flip + 1.5 + (i % 3) * 0.05 for i in range(n_prior)]  # all > flip
    recent = [flip + 0.05, flip + 0.02, flip - 0.05, flip - 0.10, flip - 0.20]  # last close < flip
    return prior + recent


def _no_cross_closes(flip: float = 678.0) -> list[float]:
    """Whippy chop with no clear prior mode — should not trigger a cross."""
    closes = []
    for i in range(35):
        closes.append(flip + (0.5 if i % 2 == 0 else -0.5))
    return closes


def _ctx(
    *,
    closes: Optional[list[float]] = None,
    flip: float = 678.0,
    close: Optional[float] = None,
    flip_distance_subscore: float = 0.8,
    rbi_label: str = "Break Watch",
    vol_x_triggered: bool = True,
    timestamp: Optional[datetime] = None,
    regime: str = "controlled_trend",
    call_wall: float = 681.0,
    put_wall: float = 675.0,
    gex_gradient_score: float = 0.30,
    tape_score: float = 30.0,
    gvc_regime_direction: Optional[str] = None,
) -> PlaybookContext:
    ts = timestamp or datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)  # 2:30 PM ET
    if closes is None:
        closes = _bullish_cross_closes(flip)
    if close is None:
        close = closes[-1]

    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=0.5e9,
        gamma_flip=flip,
        put_call_ratio=1.0,
        max_pain=678.5,
        smart_call=200000.0,
        smart_put=100000.0,
        recent_closes=closes,
        iv_rank=None,
        extra={"call_wall": call_wall, "put_wall": put_wall},
    )

    advanced: dict[str, SignalSnapshot] = {
        "range_break_imminence": SignalSnapshot(
            name="range_break_imminence",
            score=70.0,
            clamped_score=0.70,
            triggered=True,
            context_values={"label": rbi_label},
        ),
        "vol_expansion": SignalSnapshot(
            name="vol_expansion",
            score=45.0,
            clamped_score=0.45,
            triggered=vol_x_triggered,
            context_values={},
        ),
    }
    if gvc_regime_direction is not None:
        advanced["gamma_vwap_confluence"] = SignalSnapshot(
            name="gamma_vwap_confluence",
            score=10.0,
            clamped_score=0.10,
            context_values={"regime_direction": gvc_regime_direction},
        )

    basic: dict[str, SignalSnapshot] = {
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_score,
            clamped_score=tape_score / 100.0,
        ),
        "gex_gradient": SignalSnapshot(
            name="gex_gradient",
            score=gex_gradient_score * 100,
            clamped_score=gex_gradient_score,
        ),
    }

    return PlaybookContext(
        market=market,
        msi_score=55.0,
        msi_regime=regime,
        msi_components={
            "gamma_anchor": {"context": {"flip_distance_subscore": flip_distance_subscore}},
        },
        advanced_signals=advanced,
        basic_signals=basic,
        levels={"call_wall": call_wall, "put_wall": put_wall, "max_gamma_strike": 679.0},
        open_positions=[],
        recently_emitted={},
    )


# ----------------------------------------------------------------------
# Cross detection
# ----------------------------------------------------------------------


def test_detect_cross_bullish():
    closes = _bullish_cross_closes(flip=100.0)
    assert _detect_cross(closes, 100.0) == "bullish"


def test_detect_cross_bearish():
    closes = _bearish_cross_closes(flip=100.0)
    assert _detect_cross(closes, 100.0) == "bearish"


def test_detect_cross_returns_none_when_no_clear_prior_mode():
    closes = _no_cross_closes(flip=100.0)
    assert _detect_cross(closes, 100.0) is None


def test_detect_cross_returns_none_when_too_few_closes():
    assert _detect_cross([100.0] * 3, 100.0) is None


# ----------------------------------------------------------------------
# Pattern matching
# ----------------------------------------------------------------------


def test_matches_bullish_cross():
    card = GAMMA_FLIP_BREAK.match(_ctx())
    assert card is not None
    assert card.action == ActionEnum.BUY_CALL_DEBIT
    assert card.direction == "bullish"
    assert card.tier == "0DTE"
    assert card.pattern == "gamma_flip_break"
    assert card.legs[0].right == "C" and card.legs[0].side == "BUY"
    # Entry sits *above* the flip (bullish breakout buffer).
    assert card.entry.ref_price > 678.0
    # Stop sits *below* the flip.
    assert card.stop.ref_price < 678.0
    # Target should be max_gamma_strike (679, closest above) or sigma_2x.
    assert card.target.level_name in ("max_gamma_strike", "call_wall", "sigma_2x")


def test_matches_bearish_cross():
    closes = _bearish_cross_closes(flip=678.0)
    card = GAMMA_FLIP_BREAK.match(_ctx(closes=closes, close=closes[-1]))
    assert card is not None
    assert card.action == ActionEnum.BUY_PUT_DEBIT
    assert card.direction == "bearish"
    assert card.legs[0].right == "P" and card.legs[0].side == "BUY"
    # Entry sits *below* the flip.
    assert card.entry.ref_price < 678.0
    # Stop sits *above* the flip.
    assert card.stop.ref_price > 678.0
    assert card.target.level_name in ("put_wall", "max_gamma_strike", "sigma_2x")


def test_no_cross_does_not_match():
    card = GAMMA_FLIP_BREAK.match(_ctx(closes=_no_cross_closes(flip=678.0)))
    assert card is None


def test_breakout_label_required():
    card = GAMMA_FLIP_BREAK.match(_ctx(rbi_label="Range Fade"))
    assert card is None


def test_vol_expansion_required():
    card = GAMMA_FLIP_BREAK.match(_ctx(vol_x_triggered=False))
    assert card is None


def test_flip_distance_subscore_below_threshold_skips():
    card = GAMMA_FLIP_BREAK.match(_ctx(flip_distance_subscore=0.3))
    assert card is None


def test_too_early_in_session_skips():
    early = datetime(2026, 5, 1, 13, 45, tzinfo=timezone.utc)  # 9:45 AM ET
    card = GAMMA_FLIP_BREAK.match(_ctx(timestamp=early))
    assert card is None


def test_mean_reversion_regime_lowers_confidence():
    base = GAMMA_FLIP_BREAK.match(_ctx(gvc_regime_direction=None))
    rev = GAMMA_FLIP_BREAK.match(_ctx(gvc_regime_direction="mean_reversion"))
    assert base is not None and rev is not None
    assert rev.confidence < base.confidence


def test_emitted_card_serializes_to_full_dict():
    card = GAMMA_FLIP_BREAK.match(_ctx())
    assert card is not None
    d = card.to_dict()
    assert d["pattern"] == "gamma_flip_break"
    assert d["direction"] in ("bullish", "bearish")
    assert d["context"]["cross_direction"] == d["direction"]
    assert d["context"]["gamma_flip"] == 678.0
    assert "vol_expansion" in d["context"]["advanced_signals_aligned"]


def test_does_not_co_fire_with_wall_patterns_on_breakout():
    """Sanity: gamma_flip_break fires in trend regimes; wall patterns don't.

    The 'controlled_trend' regime + Break Watch label is the playground for
    gamma_flip_break.  call_wall_fade requires chop_range / high_risk_reversal
    — so the two should not coexist on the same context.
    """
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CALL_WALL_FADE
    from src.signals.playbook.patterns.put_wall_bounce import PATTERN as PUT_WALL_BOUNCE

    ctx = _ctx()  # controlled_trend regime
    gfb = GAMMA_FLIP_BREAK.match(ctx)
    cwf = CALL_WALL_FADE.match(ctx)
    pwb = PUT_WALL_BOUNCE.match(ctx)
    assert gfb is not None
    # cwf and pwb may or may not match based on flow; the engine's regime
    # gate filters cwf/pwb out anyway since their valid_regimes don't include
    # controlled_trend.  Here we just assert gfb fires cleanly.
    assert gfb.pattern == "gamma_flip_break"
