"""Tests for the custom strategy builder (Phase 3, src/backtesting/strategy.py)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.backtesting import strategy as strat
from src.backtesting.models import BacktestSpec, SpecError, StrategySpec

ET = timezone.utc
T0 = datetime(2026, 6, 10, 14, 0, tzinfo=ET)


# ----------------------------------------------------------------------
# Spec validation
# ----------------------------------------------------------------------


def _spec(**over):
    body = {
        "underlying": "SPY", "start_date": "2026-06-10", "end_date": "2026-06-11",
        "strategy": {
            "direction": "bullish",
            "conditions": [{"field": "msi", "op": "<", "value": 40}],
            "target_offset_pct": 0.3, "stop_offset_pct": 0.2, "entry": {"dte": 0},
        },
    }
    body.update(over)
    return BacktestSpec.from_dict(body)


def test_strategy_parses_and_roundtrips():
    spec = _spec()
    assert spec.strategy.direction == "bullish"
    assert spec.strategy.conditions[0].field == "msi"
    again = BacktestSpec.from_dict(spec.to_dict())
    assert again.strategy.conditions[0].value == 40.0


def test_strategy_rejects_unknown_field():
    with pytest.raises(SpecError):
        _spec(strategy={"direction": "bullish",
                        "conditions": [{"field": "bogus", "op": "<", "value": 1}]})


def test_strategy_rejects_bad_categorical_value():
    with pytest.raises(SpecError):
        _spec(strategy={"direction": "bullish",
                        "conditions": [{"field": "net_gex_sign", "op": "==", "value": "up"}]})


def test_strategy_requires_an_exit():
    # No level offsets and no premium overlay ⇒ every card would be unresolved.
    with pytest.raises(SpecError):
        _spec(strategy={"direction": "bullish",
                        "conditions": [{"field": "msi", "op": "<", "value": 40}]})


def test_strategy_premium_overlay_satisfies_exit_requirement():
    spec = _spec(
        strategy={"direction": "bullish", "conditions": [{"field": "msi", "op": "<", "value": 40}]},
        exit={"profit_target_pct": 0.5, "stop_loss_pct": 0.5},
    )
    assert spec.strategy is not None  # accepted: premium overlay is the exit


def test_strategy_requires_nonempty_conditions():
    with pytest.raises(SpecError):
        _spec(strategy={"direction": "bullish", "conditions": []})


# ----------------------------------------------------------------------
# Condition evaluation
# ----------------------------------------------------------------------


def test_passes_numeric_and_categorical():
    conds = StrategySpec.from_dict({
        "direction": "bullish",
        "conditions": [
            {"field": "msi", "op": "<", "value": 40},
            {"field": "net_gex_sign", "op": "==", "value": "negative"},
        ],
        "target_offset_pct": 0.3,
    }).conditions
    assert strat._passes(conds, {"msi": 35.0, "net_gex_sign": "negative"}) is True
    assert strat._passes(conds, {"msi": 55.0, "net_gex_sign": "negative"}) is False
    # Missing field fails closed.
    assert strat._passes(conds, {"msi": 35.0}) is False


# ----------------------------------------------------------------------
# As-of indicator merge
# ----------------------------------------------------------------------


def test_asof_merge_carries_latest_indicator_forward():
    # Two price bars; one gex row at T0 should attach to both bars (the second
    # is within the staleness window) and a stale row should be dropped.
    prices = [(T0, 500.0), (T0 + timedelta(minutes=5), 501.0)]
    gex = [(T0, 1.0e9, -2.0e9, None, 499.0, 505.0, 495.0, 1.1, 498.0, 0.2)]
    scores = [(T0, 35.0, "chop_range")]

    class _Cur:
        def __init__(self, store):
            self._store = store
            self._r = []

        def execute(self, sql, params):
            if "underlying_quotes" in sql:
                self._r = self._store["prices"]
            elif "gex_summary" in sql:
                self._r = self._store["gex"]
            elif "signal_scores" in sql:
                self._r = self._store["scores"]
            else:
                self._r = []

        def fetchall(self):
            return list(self._r)

    class _Conn:
        def cursor(self):
            return _Cur({"prices": prices, "gex": gex, "scores": scores})

    bars = strat.load_indicator_bars(_Conn(), "SPY", T0, T0 + timedelta(hours=1))
    assert len(bars) == 2
    b0 = bars[0]
    assert b0["net_gex_sign"] == "negative"      # net_gex_at_spot = -2e9
    assert b0["msi"] == 35.0
    assert b0["msi_regime"] == "chop_range"
    # dist_to_call_wall_pct = (505 - 500)/500*100 = 1.0
    assert b0["dist_to_call_wall_pct"] == pytest.approx(1.0)
    # flip_distance_pct = |500 - 499|/500*100 = 0.2
    assert b0["flip_distance_pct"] == pytest.approx(0.2)


# ----------------------------------------------------------------------
# Synthetic card generation
# ----------------------------------------------------------------------


def test_synth_card_levels_and_leg_for_bullish():
    spec = _spec()
    card = strat._synth_card(spec.strategy, {"ts": T0, "price": 500.0},
                             underlying="SPY", max_hold=60)
    assert card.direction == "bullish"
    assert card.pattern == "custom_strategy"
    leg = card.payload["legs"][0]
    assert leg["right"] == "C" and leg["strike"] == 500 and leg["expiry"] == "2026-06-10"
    # bullish: target above, stop below.
    assert card.payload["target"]["ref_price"] == pytest.approx(500.0 * 1.3)
    assert card.payload["stop"]["ref_price"] == pytest.approx(500.0 * 0.8)


def test_synth_card_vertical_builds_two_legs():
    spec = _spec(strategy={
        "direction": "bullish",
        "conditions": [{"field": "msi", "op": "<", "value": 40}],
        "structure": "vertical", "width": 5,
        "target_offset_pct": 0.006,
    })
    card = strat._synth_card(spec.strategy, {"ts": T0, "price": 500.0},
                             underlying="SPY", max_hold=60)
    legs = card.payload["legs"]
    assert len(legs) == 2
    assert legs[0] == {"expiry": "2026-06-10", "strike": 500, "right": "C", "side": "BUY"}
    # Bullish call vertical: short the higher strike.
    assert legs[1]["strike"] == 505 and legs[1]["right"] == "C" and legs[1]["side"] == "SELL"


def test_strategy_neutral_structures_build_legs():
    # Straddle: long ATM call + put.
    spec = _spec(strategy={
        "structure": "straddle",
        "conditions": [{"field": "msi", "op": "<", "value": 40}],
    }, exit={"profit_target_pct": 0.5, "stop_loss_pct": 0.5})
    assert spec.strategy.direction == "neutral"
    legs = strat._build_legs(spec.strategy, 500.0, "2026-06-10")
    assert sorted((leg_["right"], leg_["side"]) for leg_ in legs) == [("C", "BUY"), ("P", "BUY")]
    assert all(leg_["strike"] == 500 for leg_ in legs)

    # Strangle: long OTM call/put offset by width.
    sp2 = _spec(strategy={
        "structure": "strangle", "width": 5,
        "conditions": [{"field": "msi", "op": "<", "value": 40}],
    }, exit={"profit_target_pct": 0.5})
    legs = strat._build_legs(sp2.strategy, 500.0, "2026-06-10")
    strikes = {leg_["right"]: leg_["strike"] for leg_ in legs}
    assert strikes == {"C": 505, "P": 495}

    # Iron condor: 4 legs — sell inner strangle, buy wings.
    sp3 = _spec(strategy={
        "structure": "condor", "width": 5, "wing": 5,
        "conditions": [{"field": "msi", "op": "<", "value": 40}],
    }, exit={"profit_target_pct": 0.5, "stop_loss_pct": 0.5})
    legs = strat._build_legs(sp3.strategy, 500.0, "2026-06-10")
    assert len(legs) == 4
    sides = {(leg_["right"], leg_["strike"]): leg_["side"] for leg_ in legs}
    assert sides[("C", 505)] == "SELL" and sides[("C", 510)] == "BUY"
    assert sides[("P", 495)] == "SELL" and sides[("P", 490)] == "BUY"


def test_strategy_neutral_rejects_directional_direction():
    with pytest.raises(SpecError):
        _spec(strategy={
            "structure": "straddle", "direction": "bullish",
            "conditions": [{"field": "msi", "op": "<", "value": 40}],
        }, exit={"profit_target_pct": 0.5})


def test_strategy_neutral_requires_premium_exit():
    # Level offsets don't apply to a neutral structure ⇒ must use premium overlay.
    with pytest.raises(SpecError):
        _spec(strategy={
            "structure": "strangle",
            "conditions": [{"field": "msi", "op": "<", "value": 40}],
            "target_offset_pct": 0.3,  # directional offset — not a valid neutral exit
        })


def test_strategy_directional_still_requires_direction():
    with pytest.raises(SpecError):
        _spec(strategy={
            "structure": "vertical",
            "conditions": [{"field": "msi", "op": "<", "value": 40}],
            "target_offset_pct": 0.3,
        })  # no direction


def test_strategy_vertical_requires_positive_width():
    with pytest.raises(SpecError):
        _spec(strategy={
            "direction": "bullish",
            "conditions": [{"field": "msi", "op": "<", "value": 40}],
            "structure": "vertical", "width": 0, "target_offset_pct": 0.006,
        })


def test_synth_card_bearish_uses_put_and_inverts_levels():
    spec = _spec(strategy={
        "direction": "bearish",
        "conditions": [{"field": "msi", "op": ">", "value": 70}],
        "target_offset_pct": 0.4, "stop_offset_pct": 0.3,
    })
    card = strat._synth_card(spec.strategy, {"ts": T0, "price": 500.0},
                             underlying="SPY", max_hold=60)
    assert card.payload["legs"][0]["right"] == "P"
    assert card.payload["target"]["ref_price"] == pytest.approx(500.0 * 0.6)  # below
    assert card.payload["stop"]["ref_price"] == pytest.approx(500.0 * 1.3)    # above


# ----------------------------------------------------------------------
# End-to-end: run_backtest in strategy mode
# ----------------------------------------------------------------------


class _StrategyConn:
    """Serves the indicator load, fetch_quotes (OHLC), and the leg quote."""

    def __init__(self, *, prices_close, prices_ohlc, gex, scores, leg_quote):
        self._s = {
            "close": prices_close, "ohlc": prices_ohlc,
            "gex": gex, "scores": scores, "leg_quote": leg_quote,
        }

    def cursor(self):
        return _StrategyConn._Cur(self._s)

    class _Cur:
        def __init__(self, s):
            self._s = s
            self._r = []

        def execute(self, sql, params=None):
            t = " ".join(sql.split())
            if "FROM underlying_quotes" in t and "open, high, low, close" in t:
                self._r = self._s["ohlc"]
            elif "FROM underlying_quotes" in t:
                self._r = self._s["close"]
            elif "FROM gex_summary" in t:
                self._r = self._s["gex"]
            elif "FROM signal_scores" in t:
                self._r = self._s["scores"]
            elif "option_symbol = %s" in t and "timestamp > %s" in t:
                self._r = []  # no premium series; level exits resolve on underlying
            elif "FROM option_chains" in t and "to_regclass" not in t:
                self._r = [self._s["leg_quote"]]
            else:
                self._r = []

        def fetchall(self):
            return list(self._r)

        def fetchone(self):
            return self._r[0] if self._r else None


def test_run_backtest_strategy_mode_end_to_end():
    from datetime import date

    from src.backtesting.engine import run_backtest

    # Indicator bars: msi 35 (<40) and net_gex_at_spot negative on all bars.
    close = [(T0, 500.0), (T0 + timedelta(minutes=1), 500.2), (T0 + timedelta(minutes=2), 503.5)]
    ohlc = [
        (T0, 500.0, 500.3, 499.8, 500.0),
        (T0 + timedelta(minutes=1), 500.0, 500.5, 499.9, 500.2),
        (T0 + timedelta(minutes=2), 500.2, 503.6, 500.0, 503.5),  # hits +0.6% target (503)
    ]
    gex = [(T0, 1.0e9, -2.0e9, None, 499.0, 505.0, 495.0, 1.1, 498.0, 0.2)]
    scores = [(T0, 35.0, "chop_range")]
    leg_quote = ("SPY 260610C500", 500.0, date(2026, 6, 10), "C", 2.00, 2.10, 2.05, 2.05, T0)

    conn = _StrategyConn(prices_close=close, prices_ohlc=ohlc, gex=gex, scores=scores,
                         leg_quote=leg_quote)
    spec = BacktestSpec.from_dict({
        "underlying": "SPY", "start_date": "2026-06-10", "end_date": "2026-06-10",
        "cooldown_minutes": 0,
        "fill_model": {"slippage_pct": 0.0, "commission_per_contract": 0.0},
        "sizing": {"capital": 10_000, "risk_per_trade_pct": 10, "max_concurrent": 3},
        "strategy": {
            "direction": "bullish",
            "conditions": [
                {"field": "msi", "op": "<", "value": 40},
                {"field": "net_gex_sign", "op": "==", "value": "negative"},
            ],
            "target_offset_pct": 0.006, "stop_offset_pct": 0.004, "entry": {"dte": 0},
        },
    })
    result = run_backtest(conn, spec)
    assert result.summary["n_trades"] >= 1
    tr = result.trades[0]
    assert tr.pattern == "custom_strategy"
    assert tr.outcome == "target_hit"          # underlying reached +0.6% on bar 3
    assert result.summary["diagnostics"]["cards_total"] == 3   # all 3 bars matched
