"""Tests for PortfolioEngine portfolio reconciliation logic."""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from src.signals.portfolio_engine import PortfolioEngine, PortfolioTarget, TargetPosition


def _make_engine() -> PortfolioEngine:
    """Create an engine instance with mocked symbol lookup."""
    with patch("src.signals.portfolio_engine.get_canonical_symbol", return_value="SPY"):
        return PortfolioEngine("SPY")


def _make_trade(
    *,
    trade_id: int = 1,
    entry_price: float = 2.0,
    quantity_open: int = 100,
    quantity_initial: int = 100,
    direction: str = "bullish",
    realized_pnl: float = 0.0,
) -> dict:
    return {
        "id": trade_id,
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
# _majority_direction
# ---------------------------------------------------------------------------

class TestMajorityDirection:
    def test_empty_trades_returns_neutral(self):
        assert PortfolioEngine._majority_direction([]) == "neutral"

    def test_single_bullish(self):
        trades = [_make_trade(direction="bullish")]
        assert PortfolioEngine._majority_direction(trades) == "bullish"

    def test_mixed_directions_by_quantity(self):
        trades = [
            _make_trade(direction="bullish", quantity_open=50),
            _make_trade(direction="bearish", quantity_open=100),
        ]
        assert PortfolioEngine._majority_direction(trades) == "bearish"

    def test_equal_quantities_neutral(self):
        trades = [
            _make_trade(direction="bullish", quantity_open=50),
            _make_trade(direction="bearish", quantity_open=50),
        ]
        assert PortfolioEngine._majority_direction(trades) == "neutral"


# ---------------------------------------------------------------------------
# Cash target helper
# ---------------------------------------------------------------------------

class TestCashTarget:
    def test_cash_target_has_no_positions(self):
        engine = _make_engine()
        from src.signals.scoring_engine import ScoreSnapshot
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.3,
            normalized_score=0.3,
            direction="neutral",
            components={},
        )
        target = engine._cash_target(score, "test rationale")
        assert target.target_positions == []
        assert target.total_target_contracts == 0
        assert target.target_heat_pct == 0.0
        assert target.rationale == "test rationale"
