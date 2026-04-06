"""Tests for aggregate portfolio exposure gating in UnifiedSignalEngine."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from src.signals.unified_signal_engine import UnifiedSignalEngine


def _make_engine() -> UnifiedSignalEngine:
    """Create an engine instance with mocked symbol lookup."""
    with patch("src.signals.unified_signal_engine.get_canonical_symbol", return_value="SPY"):
        return UnifiedSignalEngine("SPY")


def _make_trade(
    *,
    entry_price: float = 2.0,
    quantity_open: int = 100,
    quantity_initial: int = 100,
    direction: str = "bullish",
    realized_pnl: float = 0.0,
) -> dict:
    return {
        "id": 1,
        "option_symbol": "SPY 260410C500",
        "entry_price": entry_price,
        "current_price": entry_price,
        "quantity_open": quantity_open,
        "quantity_initial": quantity_initial,
        "status": "open",
        "direction": direction,
        "realized_pnl": realized_pnl,
        "components_at_entry": {},
    }


NOW = datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _get_portfolio_exposure
# ---------------------------------------------------------------------------

class TestGetPortfolioExposure:
    def test_empty_trades_returns_zero(self):
        engine = _make_engine()
        exp = engine._get_portfolio_exposure([])
        assert exp["open_count"] == 0
        assert exp["total_notional"] == 0.0
        assert exp["heat_pct"] == 0.0
        assert exp["net_direction"] == "neutral"

    @patch("src.signals.unified_signal_engine.db_connection")
    def test_single_bullish_trade(self, mock_db):
        # Suppress the DB call for last_opened_at
        mock_db.side_effect = Exception("no db")
        engine = _make_engine()
        trades = [_make_trade(entry_price=2.0, quantity_open=50)]
        exp = engine._get_portfolio_exposure(trades)
        # notional = 2.0 * 50 * 100 = 10,000
        assert exp["open_count"] == 1
        assert exp["total_notional"] == 10_000.0
        assert exp["net_direction"] == "bullish"
        assert exp["bullish_count"] == 1
        assert exp["bearish_count"] == 0

    @patch("src.signals.unified_signal_engine.db_connection")
    def test_mixed_directions(self, mock_db):
        mock_db.side_effect = Exception("no db")
        engine = _make_engine()
        trades = [
            _make_trade(direction="bullish"),
            _make_trade(direction="bearish"),
        ]
        exp = engine._get_portfolio_exposure(trades)
        assert exp["net_direction"] == "mixed"
        assert exp["bullish_count"] == 1
        assert exp["bearish_count"] == 1


# ---------------------------------------------------------------------------
# _check_exposure_allows_entry
# ---------------------------------------------------------------------------

class TestCheckExposureAllowsEntry:
    def test_allows_when_no_open_trades(self):
        engine = _make_engine()
        exposure = {
            "open_count": 0,
            "total_notional": 0.0,
            "heat_pct": 0.0,
            "net_direction": "neutral",
            "bullish_count": 0,
            "bearish_count": 0,
            "last_opened_at": None,
        }
        allowed, reason = engine._check_exposure_allows_entry(exposure, "bullish", NOW)
        assert allowed is True

    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_OPEN_TRADES", 3)
    def test_blocks_at_max_open_trades(self):
        engine = _make_engine()
        exposure = {
            "open_count": 3,
            "total_notional": 5000.0,
            "heat_pct": 0.005,
            "net_direction": "bullish",
            "bullish_count": 3,
            "bearish_count": 0,
            "last_opened_at": NOW - timedelta(hours=1),
        }
        allowed, reason = engine._check_exposure_allows_entry(exposure, "bullish", NOW)
        assert allowed is False
        assert "max open trades" in reason

    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 0.06)
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_OPEN_TRADES", 100)
    def test_blocks_at_heat_cap(self):
        engine = _make_engine()
        exposure = {
            "open_count": 2,
            "total_notional": 70_000.0,
            "heat_pct": 0.07,  # 7% > 6% cap
            "net_direction": "bullish",
            "bullish_count": 2,
            "bearish_count": 0,
            "last_opened_at": NOW - timedelta(hours=1),
        }
        allowed, reason = engine._check_exposure_allows_entry(exposure, "bullish", NOW)
        assert allowed is False
        assert "portfolio heat" in reason

    @patch("src.signals.unified_signal_engine.SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES", 30)
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_OPEN_TRADES", 100)
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 1.0)
    def test_blocks_during_cooldown(self):
        engine = _make_engine()
        exposure = {
            "open_count": 1,
            "total_notional": 5000.0,
            "heat_pct": 0.005,
            "net_direction": "bullish",
            "bullish_count": 1,
            "bearish_count": 0,
            "last_opened_at": NOW - timedelta(minutes=10),  # 10 min ago, cooldown is 30
        }
        allowed, reason = engine._check_exposure_allows_entry(exposure, "bullish", NOW)
        assert allowed is False
        assert "cooldown" in reason

    @patch("src.signals.unified_signal_engine.SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES", 30)
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_OPEN_TRADES", 100)
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 1.0)
    def test_allows_after_cooldown_expires(self):
        engine = _make_engine()
        exposure = {
            "open_count": 1,
            "total_notional": 5000.0,
            "heat_pct": 0.005,
            "net_direction": "bullish",
            "bullish_count": 1,
            "bearish_count": 0,
            "last_opened_at": NOW - timedelta(minutes=45),  # 45 min > 30 cooldown
        }
        allowed, reason = engine._check_exposure_allows_entry(exposure, "bullish", NOW)
        assert allowed is True

    @patch("src.signals.unified_signal_engine.SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES", 30)
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_OPEN_TRADES", 100)
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 1.0)
    def test_cooldown_does_not_block_opposite_direction(self):
        engine = _make_engine()
        exposure = {
            "open_count": 1,
            "total_notional": 5000.0,
            "heat_pct": 0.005,
            "net_direction": "bullish",
            "bullish_count": 1,
            "bearish_count": 0,
            "last_opened_at": NOW - timedelta(minutes=5),
        }
        # Bearish entry when existing is bullish — cooldown should not block
        allowed, reason = engine._check_exposure_allows_entry(exposure, "bearish", NOW)
        assert allowed is True


# ---------------------------------------------------------------------------
# _scale_size_for_exposure
# ---------------------------------------------------------------------------

class TestScaleSizeForExposure:
    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 0.06)
    def test_full_size_at_zero_heat(self):
        engine = _make_engine()
        exposure = {"heat_pct": 0.0}
        assert engine._scale_size_for_exposure(100, exposure) == 100

    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 0.06)
    def test_half_size_at_half_heat(self):
        engine = _make_engine()
        exposure = {"heat_pct": 0.03}  # 50% of 6% cap
        scaled = engine._scale_size_for_exposure(100, exposure)
        assert scaled == 50

    @patch("src.signals.unified_signal_engine.SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 0.06)
    def test_minimum_one_contract(self):
        engine = _make_engine()
        exposure = {"heat_pct": 0.059}  # nearly at cap
        scaled = engine._scale_size_for_exposure(100, exposure)
        assert scaled >= 1

    def test_single_contract_unchanged(self):
        engine = _make_engine()
        exposure = {"heat_pct": 0.05}
        assert engine._scale_size_for_exposure(1, exposure) == 1
