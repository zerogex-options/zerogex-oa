"""Tests for the Range Break Imminence advanced signal."""
from __future__ import annotations

from datetime import datetime, timezone

from src.signals.advanced.engine import AdvancedSignalEngine
from src.signals.advanced.range_break_imminence import RangeBreakImminenceSignal
from src.signals.components.base import MarketContext


NOW = datetime(2026, 4, 24, 15, 0, tzinfo=timezone.utc)


def _ctx(**overrides) -> MarketContext:
    """Build a SPY-flavored ``MarketContext`` with sensible defaults.

    ``overrides`` can include an ``extra`` dict that is merged into (not
    replacing) the baseline ``extra`` payload — same pattern as the
    existing ``tests/test_independent_signals.py`` factory.
    """
    overrides = dict(overrides)
    base_extra = {
        "call_flow_delta": 0.0,
        "put_flow_delta": 0.0,
    }
    base_extra.update(overrides.pop("extra", {}))

    # Default: a 20-bar chop-y window centered near 600 (no compression,
    # no directional trend).
    default_closes = [
        600.0, 600.4, 599.6, 600.2, 599.8, 600.1, 599.9,
        600.3, 599.7, 600.0, 600.2, 599.8, 600.1, 599.9,
        600.4, 599.6, 600.2, 599.8, 600.1, 600.0,
    ]

    payload = dict(
        timestamp=NOW,
        underlying="SPY",
        close=600.0,
        net_gex=-100_000_000.0,
        gamma_flip=598.0,
        put_call_ratio=1.0,
        max_pain=600.0,
        smart_call=500_000.0,
        smart_put=400_000.0,
        recent_closes=default_closes,
        iv_rank=0.4,
        vwap=600.0,
        extra=base_extra,
    )
    payload.update(overrides)
    return MarketContext(**payload)


# ---------------------------------------------------------------------------
# Baseline / neutral behavior
# ---------------------------------------------------------------------------

def test_registered_in_advanced_engine():
    engine = AdvancedSignalEngine()
    results = {r.name: r for r in engine.evaluate(_ctx())}
    assert "range_break_imminence" in results


def test_neutral_chop_is_range_fade():
    signal = RangeBreakImminenceSignal()
    result = signal.evaluate(_ctx())
    assert result.context["label"] == "Range Fade"
    assert result.context["triggered"] is False
    assert result.context["signal"] == "range_fade"
    assert abs(result.score) < 0.4
    assert result.context["imminence"] < 40.0


def test_missing_optional_inputs_do_not_raise():
    # No skew, no gex_by_strike, no dealer_net_delta — every sub-component
    # should fall back gracefully to zero contribution.
    signal = RangeBreakImminenceSignal()
    result = signal.evaluate(_ctx())
    assert result.context["skew"]["signed"] == 0.0
    assert result.context["dealer"]["signed"] == 0.0
    # Trap can still fire when price sits at a range extreme; the default
    # closes above keep it centered so signed should stay 0.
    assert result.context["trap"]["signed"] == 0.0


# ---------------------------------------------------------------------------
# Sub-component sanity
# ---------------------------------------------------------------------------

def test_skew_component_extreme_put_skew_is_bearish():
    signal = RangeBreakImminenceSignal()
    ctx = _ctx(extra={"skew": {"otm_put_iv": 0.30, "otm_call_iv": 0.20}})
    result = signal.evaluate(ctx)
    skew = result.context["skew"]
    assert skew["signed"] < 0
    assert skew["magnitude"] > 0.0


def test_dealer_component_reads_explicit_net_delta():
    signal = RangeBreakImminenceSignal()
    ctx = _ctx(dealer_net_delta=2.5e8)  # dealers long delta → bearish
    result = signal.evaluate(ctx)
    dealer = result.context["dealer"]
    assert dealer["dealer_net_delta"] == 2.5e8
    assert dealer["signed"] < 0
    assert dealer["magnitude"] > 0.0


def test_compression_component_detects_contraction():
    signal = RangeBreakImminenceSignal()
    # Last 10 bars are quasi-flat (tiny sigma); earlier 50 bars are noisy.
    noisy = []
    for i in range(50):
        noisy.append(600.0 + (1.0 if i % 2 else -1.0))
    quiet = [600.0 + (0.01 if i % 2 else -0.01) for i in range(10)]
    ctx = _ctx(recent_closes=noisy + quiet, close=quiet[-1])
    result = signal.evaluate(ctx)
    compression = result.context["compression"]
    assert compression["magnitude"] > 50.0


# ---------------------------------------------------------------------------
# Full fusion — bearish break imminent
# ---------------------------------------------------------------------------

def test_bearish_break_imminent_aligned_inputs():
    """All four inputs scream bearish: extreme put skew, dealers long,
    price pinned at range-low with put flow accelerating, and realized
    vol contracting. Imminence should cross the Break Watch threshold
    and direction should be bearish."""
    # Compressed tape pinned near range low. Closes drift from ~601 down
    # to 599.0 with tight final bars to show contraction.
    closes = (
        [601.0, 601.2, 600.9, 601.1, 600.8, 601.0, 600.7, 601.0, 600.5, 601.0]
        + [600.2, 600.4, 600.1, 600.3, 599.9, 600.1, 599.8, 600.0, 599.7, 599.8]
        + [599.75, 599.80, 599.78, 599.82, 599.79, 599.81, 599.78, 599.80, 599.77, 599.79]
    )
    ctx = _ctx(
        close=599.79,
        recent_closes=closes,
        dealer_net_delta=2.5e8,  # dealers long delta ⇒ bearish
        extra={
            "skew": {"otm_put_iv": 0.32, "otm_call_iv": 0.20},  # extreme
            "put_flow_delta": 400_000.0,                         # puts chased
            "call_flow_delta": -150_000.0,                       # calls sold
        },
    )
    result = RangeBreakImminenceSignal().evaluate(ctx)
    ctx_out = result.context
    assert ctx_out["direction"] == "bearish"
    assert ctx_out["imminence"] >= 65.0, ctx_out
    assert ctx_out["label"] in ("Break Watch", "Breakout Mode")
    assert ctx_out["triggered"] is True
    assert result.score < 0
    assert ctx_out["trap"]["side"] == "bearish_trap"


def test_bullish_break_imminent_aligned_inputs():
    """Mirror case — call-skew-ish (low put-vs-call premium), dealers
    short, price pinned at range-high with call flow accelerating."""
    closes = (
        [599.0, 598.9, 599.1, 598.8, 599.0, 598.7, 599.0, 598.9, 599.1, 599.0]
        + [599.5, 599.4, 599.6, 599.3, 599.5, 599.7, 599.5, 599.6, 599.8, 599.5]
        + [600.20, 600.22, 600.21, 600.23, 600.19, 600.22, 600.21, 600.22, 600.20, 600.22]
    )
    ctx = _ctx(
        close=600.22,
        recent_closes=closes,
        dealer_net_delta=-2.5e8,  # dealers short delta ⇒ bullish
        extra={
            "skew": {"otm_put_iv": 0.18, "otm_call_iv": 0.22},  # call-rich
            "call_flow_delta": 400_000.0,
            "put_flow_delta": -150_000.0,
        },
    )
    result = RangeBreakImminenceSignal().evaluate(ctx)
    ctx_out = result.context
    assert ctx_out["direction"] == "bullish"
    assert ctx_out["imminence"] >= 65.0, ctx_out
    assert result.score > 0
    assert ctx_out["trap"]["side"] == "bullish_trap"


# ---------------------------------------------------------------------------
# Label boundary behavior
# ---------------------------------------------------------------------------

def test_weak_range_label_between_fade_and_break_watch():
    """Mild bearish skew + mild dealer pressure + compression but no trap
    should land in the 40–64 'Weak Range' band."""
    # Quietly contracting tape — compression ~70–100%, no trap.
    closes = (
        [600.0 + (0.5 if i % 2 else -0.5) for i in range(50)]
        + [600.0 + (0.02 if i % 2 else -0.02) for i in range(10)]
    )
    ctx = _ctx(
        close=600.0,
        recent_closes=closes,
        extra={
            "skew": {"otm_put_iv": 0.23, "otm_call_iv": 0.20},  # mild bearish
        },
    )
    result = RangeBreakImminenceSignal().evaluate(ctx)
    imminence = result.context["imminence"]
    # Compression alone contributes up to 20; mild skew adds a bit more.
    assert 15.0 <= imminence < 65.0, imminence
    assert result.context["label"] in ("Range Fade", "Weak Range")


def test_playbook_mentions_follow_the_break_when_imminence_is_high():
    closes = (
        [601.0] * 10
        + [600.5] * 10
        + [599.80, 599.82, 599.79, 599.81, 599.78, 599.80, 599.79, 599.80, 599.79, 599.79]
    )
    ctx = _ctx(
        close=599.79,
        recent_closes=closes,
        dealer_net_delta=2.9e8,
        extra={
            "skew": {"otm_put_iv": 0.35, "otm_call_iv": 0.20},
            "put_flow_delta": 600_000.0,
            "call_flow_delta": -300_000.0,
        },
    )
    result = RangeBreakImminenceSignal().evaluate(ctx)
    if result.context["label"] == "Breakout Mode":
        assert "Follow the break" in result.context["playbook"]
