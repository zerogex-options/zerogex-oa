"""Tests for the realistic execution model (bid/ask by side + slippage)."""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.signals import execution
from src.signals.execution import leg_fill_price, leg_fill_price_from_row
from src.signals.portfolio_engine import PortfolioEngine
from src.signals.position_optimizer_engine import (
    PositionOptimizerContext,
    PositionOptimizerEngine,
)

NOW = datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# leg_fill_price
# ---------------------------------------------------------------------------


class TestLegFillPrice:
    def test_long_open_pays_ask(self):
        assert leg_fill_price(bid=1.00, ask=1.10, side="long", action="open") == pytest.approx(1.10)

    def test_short_open_receives_bid(self):
        assert leg_fill_price(bid=1.00, ask=1.10, side="short", action="open") == pytest.approx(
            1.00
        )

    def test_long_close_sells_at_bid(self):
        assert leg_fill_price(bid=1.00, ask=1.10, side="long", action="close") == pytest.approx(
            1.00
        )

    def test_short_close_buys_at_ask(self):
        assert leg_fill_price(bid=1.00, ask=1.10, side="short", action="close") == pytest.approx(
            1.10
        )

    def test_slippage_widens_buyer_and_seller(self):
        # 2% slippage: buyer pays 1.10 * 1.02; seller gets 1.00 * 0.98.
        buy = leg_fill_price(bid=1.00, ask=1.10, side="long", action="open", slippage_pct=0.02)
        sell = leg_fill_price(bid=1.00, ask=1.10, side="short", action="open", slippage_pct=0.02)
        assert buy == pytest.approx(1.122)
        assert sell == pytest.approx(0.98)

    def test_missing_ask_falls_back_to_mid(self):
        # No ask => fall back so the caller still gets a usable price.
        price = leg_fill_price(bid=1.00, ask=0.0, last=1.05, side="long", action="open")
        assert price == pytest.approx(1.05)

    def test_negative_slippage_clamped_to_zero(self):
        # Guard against a pathological negative config that would tighten the
        # spread in the trader's favor.
        buy = leg_fill_price(bid=1.00, ask=1.10, side="long", action="open", slippage_pct=-0.5)
        assert buy == pytest.approx(1.10)

    def test_invalid_side_or_action_raises(self):
        with pytest.raises(ValueError):
            leg_fill_price(bid=1.0, ask=1.1, side="middle", action="open")
        with pytest.raises(ValueError):
            leg_fill_price(bid=1.0, ask=1.1, side="long", action="hold")


# ---------------------------------------------------------------------------
# leg_fill_price_from_row
# ---------------------------------------------------------------------------


class TestLegFillPriceFromRow:
    def test_row_form_matches_explicit(self):
        row = {"bid": 2.00, "ask": 2.10, "last": 2.05}
        assert leg_fill_price_from_row(row, side="long", action="open") == pytest.approx(2.10)
        assert leg_fill_price_from_row(row, side="short", action="open") == pytest.approx(2.00)


# ---------------------------------------------------------------------------
# Position optimizer entry pricing
# ---------------------------------------------------------------------------


def _make_row(
    strike, opt, bid, ask, delta=0.30, gamma=0.01, theta=-0.02, iv=0.25, volume=500, oi=1000
):
    return {
        "expiration": date(2026, 4, 17),
        "strike": float(strike),
        "option_type": opt,
        "bid": bid,
        "ask": ask,
        "last": (bid + ask) / 2.0,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "iv": iv,
        "volume": volume,
        "open_interest": oi,
    }


def _make_context(direction="bullish") -> PositionOptimizerContext:
    return PositionOptimizerContext(
        timestamp=NOW,
        signal_timestamp=NOW,
        signal_timeframe="intraday",
        signal_direction=direction,
        signal_strength="high",
        trade_type="trend_follow",
        current_price=500.0,
        net_gex=-1e9,
        gamma_flip=499.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call_premium=0.0,
        smart_put_premium=0.0,
        dealer_net_delta=0.0,
        target_dte_min=0,
        target_dte_max=2,
        option_rows=[],
    )


class TestOptimizerEntryFills:
    def test_bull_call_debit_pays_ask_on_long_and_bids_on_short(self):
        engine = PositionOptimizerEngine(underlying="SPY")
        ctx = _make_context()
        # long 500C: bid 2.90, ask 3.10 -> buy at 3.10
        # short 505C: bid 0.40, ask 0.60 -> sell at 0.40
        # realistic debit (per share) = 3.10 - 0.40 = 2.70 -> $270 per contract
        long_call = _make_row(500, "C", 2.90, 3.10, delta=0.52)
        short_call = _make_row(505, "C", 0.40, 0.60, delta=0.18)
        cand = engine._score_candidate(
            ctx, "bull_call_debit", date(2026, 4, 17), "C", short_call, long_call
        )
        assert cand is not None
        assert cand.entry_debit == pytest.approx(270.0)

    def test_bull_put_credit_sells_at_bid_and_buys_at_ask(self):
        engine = PositionOptimizerEngine(underlying="SPY")
        ctx = _make_context()
        # short 495P: bid 1.80, ask 2.00 -> sell at 1.80
        # long 490P: bid 0.70, ask 0.90 -> buy at 0.90
        # realistic credit (per share) = 1.80 - 0.90 = 0.90 -> $90 per contract
        short_put = _make_row(495, "P", 1.80, 2.00, delta=-0.32)
        long_put = _make_row(490, "P", 0.70, 0.90, delta=-0.18)
        cand = engine._score_candidate(
            ctx, "bull_put_credit", date(2026, 4, 17), "P", short_put, long_put
        )
        assert cand is not None
        assert cand.entry_credit == pytest.approx(90.0)

    def test_slippage_increases_debit_and_shrinks_credit(self, monkeypatch):
        # Bump slippage to 5% and verify the engine picks it up via the config.
        monkeypatch.setattr(execution, "SIGNALS_EXECUTION_SLIPPAGE_PCT", 0.05)
        engine = PositionOptimizerEngine(underlying="SPY")
        ctx = _make_context()
        long_call = _make_row(500, "C", 2.90, 3.10, delta=0.52)
        short_call = _make_row(505, "C", 0.40, 0.60, delta=0.18)
        cand = engine._score_candidate(
            ctx, "bull_call_debit", date(2026, 4, 17), "C", short_call, long_call
        )
        # buy = 3.10 * 1.05 = 3.255; sell = 0.40 * 0.95 = 0.38
        # debit per share = 3.255 - 0.38 = 2.875 -> $287.50 per contract
        assert cand is not None
        assert cand.entry_debit == pytest.approx(287.5)


# ---------------------------------------------------------------------------
# Portfolio spread-mark realistic exit
# ---------------------------------------------------------------------------


def _debit_trade():
    return {
        "id": 1,
        "option_symbol": "SPY 260410C500",
        "entry_price": 2.70,
        "current_price": 2.70,
        "quantity_open": 10,
        "quantity_initial": 10,
        "status": "open",
        "direction": "bullish",
        "realized_pnl": 0.0,
        "components_at_entry": {
            "optimizer": {
                "pricing_mode": "debit",
                "legs": [
                    {"side": "long", "option_symbol": "SPY 260410C500"},
                    {"side": "short", "option_symbol": "SPY 260410C505"},
                ],
            }
        },
    }


def _credit_trade():
    return {
        "id": 2,
        "option_symbol": "SPY 260410P495",
        "entry_price": 0.90,
        "current_price": 0.90,
        "quantity_open": 5,
        "quantity_initial": 5,
        "status": "open",
        "direction": "bullish",
        "realized_pnl": 0.0,
        "components_at_entry": {
            "optimizer": {
                "pricing_mode": "credit",
                "legs": [
                    {"side": "short", "option_symbol": "SPY 260410P495"},
                    {"side": "long", "option_symbol": "SPY 260410P490"},
                ],
            }
        },
    }


def _engine():
    with patch("src.signals.portfolio_engine.get_canonical_symbol", return_value="SPY"):
        return PortfolioEngine("SPY")


class TestSpreadMarkRealisticExit:
    def test_debit_exit_uses_bid_for_long_and_ask_for_short(self):
        engine = _engine()
        # Debit spread exit: sell long at bid, buy short at ask.
        quotes = {
            "SPY 260410C500": (3.00, 3.20, 3.10),  # long -> sell at 3.00
            "SPY 260410C505": (0.50, 0.70, 0.60),  # short -> buy at 0.70
        }
        with patch.object(
            engine, "_latest_option_quote", side_effect=lambda sym, *a, **kw: quotes[sym]
        ):
            value, mode = engine._spread_mark(_debit_trade(), NOW, conn=MagicMock())
        assert mode == "debit"
        # liquidation = 3.00 - 0.70 = 2.30
        assert value == pytest.approx(2.30)

    def test_credit_exit_is_cost_to_close(self):
        engine = _engine()
        # Credit spread exit: buy short back at ask, sell long at bid.
        quotes = {
            "SPY 260410P495": (1.60, 1.80, 1.70),  # short -> buy back at 1.80
            "SPY 260410P490": (0.50, 0.70, 0.60),  # long -> sell at 0.50
        }
        with patch.object(
            engine, "_latest_option_quote", side_effect=lambda sym, *a, **kw: quotes[sym]
        ):
            value, mode = engine._spread_mark(_credit_trade(), NOW, conn=MagicMock())
        assert mode == "credit"
        # cost-to-close = 1.80 - 0.50 = 1.30
        assert value == pytest.approx(1.30)

    def test_slippage_widens_debit_exit_loss(self, monkeypatch):
        monkeypatch.setattr(execution, "SIGNALS_EXECUTION_SLIPPAGE_PCT", 0.05)
        engine = _engine()
        quotes = {
            "SPY 260410C500": (3.00, 3.20, 3.10),
            "SPY 260410C505": (0.50, 0.70, 0.60),
        }
        with patch.object(
            engine, "_latest_option_quote", side_effect=lambda sym, *a, **kw: quotes[sym]
        ):
            value, _ = engine._spread_mark(_debit_trade(), NOW, conn=MagicMock())
        # sell long = 3.00 * 0.95 = 2.85; buy short = 0.70 * 1.05 = 0.735
        # liquidation = 2.85 - 0.735 = 2.115
        assert value == pytest.approx(2.115)
