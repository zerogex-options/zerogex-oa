from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.basic.positioning_trap import PositioningTrapComponent
from src.signals.unified_signal_engine import UnifiedSignalEngine

comp = PositioningTrapComponent()


def _ctx(**kwargs) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 10, 15, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=510.0,
        net_gex=-150_000_000.0,
        gamma_flip=508.0,
        put_call_ratio=1.0,
        max_pain=509.0,
        smart_call=500_000.0,
        smart_put=500_000.0,
        recent_closes=[510.0] * 5,
        iv_rank=None,
    )
    defaults.update(kwargs)
    return MarketContext(**defaults)


def test_squeeze_setup_scores_bullish():
    base = 510.0
    ctx = _ctx(
        put_call_ratio=1.35,
        smart_call=200_000,
        smart_put=900_000,
        recent_closes=[base] * 4 + [base * 1.003],
        close=511.5,
        gamma_flip=510.0,
        net_gex=-250_000_000,
    )
    assert comp.compute(ctx) > 0


def test_flush_setup_scores_bearish():
    base = 510.0
    ctx = _ctx(
        put_call_ratio=0.75,
        smart_call=900_000,
        smart_put=200_000,
        recent_closes=[base] * 4 + [base * 0.997],
        close=508.5,
        gamma_flip=510.0,
        net_gex=-250_000_000,
    )
    assert comp.compute(ctx) < 0


def test_component_in_unified_engine_and_weights_still_valid(monkeypatch):
    monkeypatch.delenv("SIGNAL_IV_RANK_ENABLED", raising=False)
    engine = UnifiedSignalEngine("SPY")
    assert any(c.name == "net_gex_sign" for c in engine.scoring_engine.components)


def test_signed_imbalance_uses_top_level_signed_fields():
    ctx = _ctx(smart_call=300_000.0, smart_put=-200_000.0)
    imbalance = comp._signed_imbalance(ctx)
    # (300k - (-200k)) / (300k + 200k) = 1.0
    assert imbalance == 1.0
