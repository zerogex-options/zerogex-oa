from datetime import datetime, timezone

from src.analytics.signal_engine import SignalComponent, SignalSmsNotifier, TradeSignal


def _signal(**overrides):
    sig = TradeSignal(
        underlying="SPY",
        timestamp=datetime(2026, 4, 1, 14, 30, tzinfo=timezone.utc),
        timeframe="intraday",
        composite_score=12,
        max_possible_score=18,
        normalized_score=0.72,
        direction="bullish",
        strength="high",
        estimated_win_pct=0.67,
        trade_type="short_put_spread",
        trade_rationale="test",
        target_expiry="0DTE",
        suggested_strikes="ATM",
        current_price=500.0,
        net_gex=1.0,
        gamma_flip=499.0,
        price_vs_flip=0.2,
        vwap=499.5,
        vwap_deviation_pct=0.1,
        put_call_ratio=0.8,
        dealer_net_delta=1000.0,
        smart_money_direction="bullish",
        unusual_volume_detected=False,
        orb_breakout_direction="bullish",
        components=[
            SignalComponent(
                name="ZeroGEX Exhaustion Score",
                weight=2,
                score=-2,
                description="ZES=88",
                value=88.0,
                applicable=True,
            )
        ],
    )
    for k, v in overrides.items():
        setattr(sig, k, v)
    return sig


def test_sms_notifier_eligibility(monkeypatch):
    monkeypatch.setenv("SIGNAL_SMS_ENABLED", "true")
    monkeypatch.setenv("SIGNAL_SMS_MIN_NORMALIZED_SCORE", "0.67")
    monkeypatch.setenv("SIGNAL_SMS_MIN_STRENGTH", "high")
    monkeypatch.setenv("SIGNAL_SMS_TIMEFRAMES", "intraday")

    notifier = SignalSmsNotifier("SPY")
    assert notifier._is_eligible(_signal())
    assert not notifier._is_eligible(_signal(normalized_score=0.4))
    assert not notifier._is_eligible(_signal(direction="neutral"))


def test_sms_notifier_maybe_send_respects_disabled(monkeypatch):
    monkeypatch.setenv("SIGNAL_SMS_ENABLED", "false")
    notifier = SignalSmsNotifier("SPY")

    called = {"sent": False}

    def _fake_send(*_args, **_kwargs):
        called["sent"] = True

    notifier._send_twilio_sms = _fake_send
    notifier.maybe_send(_signal())
    assert called["sent"] is False
