"""Tests for PortfolioEngine portfolio reconciliation logic."""

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.signals.portfolio_engine import PortfolioEngine, PortfolioTarget, TargetPosition
from src.signals.position_optimizer_engine import SpreadCandidate, SizingProfile
from src.signals.scoring_engine import ScoreSnapshot


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


class TestTargetPositionStrike:
    def test_compute_target_uses_candidate_leg_strike_not_underlying_spot(self):
        engine = _make_engine()
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.9,
            normalized_score=0.9,
            direction="bullish",
            components={"dealer_regime": {"score": 0.8, "weight": 0.12}},
        )
        candidate = SpreadCandidate(
            rank=1,
            strategy_type="bull_call_debit",
            expiry=date(2026, 4, 17),
            dte=1,
            strikes="Long 705C / Short 710C",
            option_type="C",
            entry_debit=250.0,
            entry_credit=0.0,
            width=5.0,
            max_profit=250.0,
            max_loss=250.0,
            risk_reward_ratio=1.0,
            probability_of_profit=0.55,
            expected_value=20.0,
            sharpe_like_ratio=0.08,
            liquidity_score=0.8,
            net_delta=20.0,
            net_gamma=1.0,
            net_theta=-3.0,
            premium_efficiency=1.0,
            market_structure_fit=0.8,
            greek_alignment_score=0.8,
            edge_score=0.7,
            kelly_fraction=0.05,
            sizing_profiles=[
                SizingProfile(
                    profile="optimal",
                    contracts=3,
                    max_risk_dollars=750.0,
                    expected_value_dollars=60.0,
                    constrained_by="kelly",
                )
            ],
        )
        market_ctx = {
            "close": 700.79,
            "net_gex": -1.0e9,
            "gamma_flip": 699.0,
            "put_call_ratio": 1.0,
            "max_pain": 700.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [699.5, 700.2, 700.79],
            "iv_rank": 0.3,
        }

        with patch.object(engine, "_select_optimizer_candidate", return_value={
                 "candidate": candidate,
                 "signal_timeframe": "intraday",
                 "signal_strength": "high",
             }), \
             patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C705"):
            target = engine.compute_target(score, market_ctx, conn=MagicMock())

        assert target.target_positions
        assert target.target_positions[0].strike == 705.0


class TestIndependentSignalTriggering:
    def test_independent_signal_can_trigger_when_composite_is_cash(self):
        engine = _make_engine()
        base_score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.1,
            normalized_score=0.1,
            direction="neutral",
            components={},
        )
        market_ctx = {
            "close": 500.0,
            "net_gex": -1.0e9,
            "gamma_flip": 499.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [498.5, 499.2, 500.0],
            "iv_rank": 0.4,
        }

        independent_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=0.72,
            normalized_score=0.72,
            direction="bullish",
            target_positions=[],
            total_target_contracts=0,
            target_heat_pct=0.0,
            rationale="independent candidate",
            source="advanced:squeeze_setup",
        )
        independent_target.target_positions = [
            TargetPosition(
                direction="bullish",
                strategy_type="long_straddle",
                contracts=1,
                option_symbol="SPY 260417C500",
                option_type="C",
                expiration=date(2026, 4, 17),
                strike=500.0,
                entry_mark=2.5,
                probability_of_profit=0.55,
                expected_value=20.0,
                kelly_fraction=0.05,
                optimizer_payload={},
            )
        ]
        independent_target.total_target_contracts = 1

        with patch.object(
            engine,
            "compute_target",
            return_value=PortfolioTarget(
                underlying="SPY",
                timestamp=NOW,
                composite_score=0.1,
                normalized_score=0.1,
                direction="neutral",
                target_positions=[],
                total_target_contracts=0,
                target_heat_pct=0.0,
                rationale="composite cash",
            ),
        ), patch.object(
            engine,
            "_build_advanced_target",
            return_value=independent_target,
        ):
            out = engine.compute_target_with_independents(
                base_score,
                market_ctx,
                independent_results=[],
            )
        assert out.source.startswith("advanced:")
        assert out.target_positions

    def test_stronger_independent_overrides_composite_position(self):
        engine = _make_engine()
        base_score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.6,
            normalized_score=0.6,
            direction="bullish",
            components={},
        )
        market_ctx = {
            "close": 500.0,
            "net_gex": -1.0e9,
            "gamma_flip": 499.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [498.5, 499.2, 500.0],
            "iv_rank": 0.4,
        }
        composite_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=0.6,
            normalized_score=0.6,
            direction="bullish",
            target_positions=[
                TargetPosition(
                    direction="bullish",
                    strategy_type="bull_call_debit",
                    contracts=1,
                    option_symbol="SPY 260417C500",
                    option_type="C",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.0,
                    probability_of_profit=0.54,
                    expected_value=18.0,
                    kelly_fraction=0.04,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=1,
            target_heat_pct=0.01,
            rationale="composite entry",
        )
        independent_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=-0.8,
            normalized_score=0.8,
            direction="bearish",
            target_positions=[
                TargetPosition(
                    direction="bearish",
                    strategy_type="bear_put_debit",
                    contracts=1,
                    option_symbol="SPY 260417P500",
                    option_type="P",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.3,
                    probability_of_profit=0.56,
                    expected_value=24.0,
                    kelly_fraction=0.05,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=1,
            target_heat_pct=0.01,
            rationale="independent trap",
            source="advanced:trap_detection",
        )
        with patch.object(engine, "compute_target", return_value=composite_target), patch.object(
            engine,
            "_build_advanced_target",
            return_value=independent_target,
        ):
            out = engine.compute_target_with_independents(
                base_score,
                market_ctx,
                independent_results=[],
            )
        assert out.source == "advanced:trap_detection"
        assert out.direction == "bearish"

    def test_do_not_fade_blocks_countertrend_advanced_setup(self):
        engine = _make_engine()
        base_score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=85.0,
            normalized_score=0.85,
            direction="trend_expansion",
            components={},
            aggregation={},
        )
        market_ctx = {
            "close": 500.0,
            "net_gex": -1.5e9,
            "gamma_flip": 495.0,  # far (>= 0.6%)
            "max_gamma_strike": 493.0,  # far (>= 1.2%)
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [497.5, 499.0, 500.0],  # bullish trend dir
            "iv_rank": 0.4,
        }
        advanced_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=85.0,
            normalized_score=0.5,
            direction="bearish",  # counter-trend fade
            target_positions=[
                TargetPosition(
                    direction="bearish",
                    strategy_type="bear_put_debit",
                    contracts=1,
                    option_symbol="SPY 260417P500",
                    option_type="P",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.4,
                    probability_of_profit=0.55,
                    expected_value=12.0,
                    kelly_fraction=0.03,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=1,
            target_heat_pct=0.01,
            rationale="advanced fade",
            source="advanced:trap_detection",
        )
        with patch.object(engine, "_build_advanced_target", return_value=advanced_target):
            out = engine.compute_target_with_independents(
                base_score,
                market_ctx,
                independent_results=[],
            )
        assert out.target_positions == []
        assert "Do-not-fade policy active" in out.rationale

    def test_without_advanced_setup_engine_stays_cash(self):
        engine = _make_engine()
        base_score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=75.0,
            normalized_score=0.75,
            direction="trend_expansion",
            components={},
            aggregation={},
        )
        market_ctx = {
            "close": 500.0,
            "net_gex": -1.0e9,
            "gamma_flip": 499.5,
            "max_gamma_strike": 500.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [498.0, 499.0, 500.0],
            "iv_rank": 0.4,
        }
        with patch.object(
            engine,
            "_build_advanced_target",
            return_value=None,
        ):
            out = engine.compute_target_with_independents(
                base_score,
                market_ctx,
                independent_results=[],
            )
        assert out.target_positions == []
        assert "No advanced signal setup confirmed" in out.rationale


class TestFreshCrossSizingBoost:
    """A fresh gamma-flip cross in the signaled direction should increase
    the contract count. Same setup as TestTargetPositionStrike but we vary
    the recent_closes fixture to flip the fresh-cross flag on or off."""

    @staticmethod
    def _candidate() -> SpreadCandidate:
        return SpreadCandidate(
            rank=1,
            strategy_type="bull_call_debit",
            expiry=date(2026, 4, 17),
            dte=1,
            strikes="Long 500C / Short 505C",
            option_type="C",
            entry_debit=250.0,
            entry_credit=0.0,
            width=5.0,
            max_profit=250.0,
            max_loss=250.0,
            risk_reward_ratio=1.0,
            probability_of_profit=0.55,
            expected_value=20.0,
            sharpe_like_ratio=0.08,
            liquidity_score=0.8,
            net_delta=20.0,
            net_gamma=1.0,
            net_theta=-3.0,
            premium_efficiency=1.0,
            market_structure_fit=0.8,
            greek_alignment_score=0.8,
            edge_score=0.7,
            kelly_fraction=0.05,
            sizing_profiles=[
                SizingProfile(
                    profile="optimal",
                    contracts=10,
                    max_risk_dollars=2500.0,
                    expected_value_dollars=200.0,
                    constrained_by="kelly",
                )
            ],
        )

    @staticmethod
    def _score() -> ScoreSnapshot:
        return ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.9,
            normalized_score=0.9,
            direction="bullish",
            components={"dealer_regime": {"score": 0.8, "weight": 0.12}},
        )

    def _compute(self, recent_closes: list[float]) -> int:
        engine = _make_engine()
        candidate = self._candidate()
        market_ctx = {
            "close": 501.0,
            "net_gex": 1.0e9,
            "gamma_flip": 500.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": recent_closes,
            "iv_rank": 0.3,
        }
        with patch.object(engine, "_select_optimizer_candidate", return_value={
                 "candidate": candidate,
                 "signal_timeframe": "intraday",
                 "signal_strength": "high",
             }), \
             patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C500"):
            target = engine.compute_target(self._score(), market_ctx, conn=MagicMock())
        assert target.target_positions
        return target.total_target_contracts

    def test_no_fresh_cross_uses_base_contracts(self):
        # Prev close already above flip -- no fresh cross.
        contracts = self._compute([499.0, 500.5, 501.0])
        # base = 10 * 0.9 = 9
        assert contracts == 9

    def test_fresh_bullish_cross_boosts_contracts(self):
        # Prev close at/below flip, current above -- fresh cross.
        contracts = self._compute([498.0, 499.5, 501.0])
        # base = 10 * 0.9 = 9; with 20% boost -> 10
        assert contracts == 10

    def test_fresh_cross_reflected_in_rationale(self):
        engine = _make_engine()
        candidate = self._candidate()
        market_ctx = {
            "close": 501.0,
            "net_gex": 1.0e9,
            "gamma_flip": 500.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [498.0, 499.5, 501.0],
            "iv_rank": 0.3,
        }
        with patch.object(engine, "_select_optimizer_candidate", return_value={
                 "candidate": candidate,
                 "signal_timeframe": "intraday",
                 "signal_strength": "high",
             }), \
             patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C500"):
            target = engine.compute_target(self._score(), market_ctx, conn=MagicMock())
        assert "fresh-cross boost" in target.rationale


class TestMarketStatusGate:
    def test_market_status_open_within_regular_options_window(self):
        status = PortfolioEngine._market_status(datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc))
        assert status == "OPEN"

    def test_market_status_closed_after_1615_et(self):
        status = PortfolioEngine._market_status(datetime(2026, 4, 6, 20, 16, tzinfo=timezone.utc))
        assert status == "CLOSED"

    def test_reconcile_holds_when_market_closed(self):
        engine = _make_engine()
        target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=0.8,
            normalized_score=0.8,
            direction="bullish",
            target_positions=[],
            total_target_contracts=0,
            target_heat_pct=0.0,
            rationale="test target",
        )
        open_trade = _make_trade(quantity_open=1)

        with patch.object(engine, "_market_status", return_value="CLOSED"), \
             patch.object(engine, "_fetch_open_trades", return_value=[open_trade]), \
             patch.object(engine, "_update_trade_mark") as update_mark, \
             patch.object(engine, "snapshot") as snapshot, \
             patch.object(engine, "_close_trade") as close_trade, \
             patch.object(engine, "_open_position") as open_position:
            action = engine.reconcile(target, conn=MagicMock())

        assert action == "held_market_closed"
        update_mark.assert_called_once()
        close_trade.assert_not_called()
        open_position.assert_not_called()
        snapshot.assert_called_once()


class TestIndependentSignalTriggers:
    def test_composite_primary_when_active(self):
        engine = _make_engine()
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.72,
            normalized_score=0.72,
            direction="bullish",
            components={"dealer_regime": {"score": 0.6, "weight": 0.12}},
        )
        market_ctx = {
            "close": 501.0,
            "net_gex": -2.0e8,
            "gamma_flip": 499.0,
            "put_call_ratio": 0.95,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [499.0, 500.0, 501.0],
            "iv_rank": 0.4,
        }
        independent_results = [
            SimpleNamespace(
                name="gamma_vwap_confluence",
                score=0.35,
                context={"signal": "bullish_confluence", "triggered": True},
            )
        ]
        composite_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=score.composite_score,
            normalized_score=score.normalized_score,
            direction="bullish",
            target_positions=[
                TargetPosition(
                    direction="bullish",
                    strategy_type="bull_call_debit",
                    contracts=2,
                    option_symbol="SPY 260417C500",
                    option_type="C",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.0,
                    probability_of_profit=0.55,
                    expected_value=12.0,
                    kelly_fraction=0.04,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=2,
            target_heat_pct=0.02,
            rationale="composite rationale",
        )
        independent_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=0.35,
            normalized_score=0.35,
            direction="bullish",
            target_positions=[
                TargetPosition(
                    direction="bullish",
                    strategy_type="long_straddle",
                    contracts=1,
                    option_symbol="SPY 260417C500",
                    option_type="C",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.8,
                    probability_of_profit=0.53,
                    expected_value=8.0,
                    kelly_fraction=0.02,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=1,
            target_heat_pct=0.01,
            rationale="independent candidate",
            source="advanced:gamma_vwap_confluence",
        )
        with patch.object(engine, "compute_target", return_value=composite_target), patch.object(
            engine,
            "_build_advanced_target",
            return_value=independent_target,
        ):
            target = engine.compute_target_with_independents(
                score,
                market_ctx,
                independent_results=independent_results,
                conn=MagicMock(),
            )
        assert target.source == "advanced:gamma_vwap_confluence"
        assert target.rationale.startswith("Advanced gamma_vwap_confluence")

    def test_independent_signal_can_trigger_when_composite_is_cash(self):
        engine = _make_engine()
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.0,
            normalized_score=0.15,
            direction="neutral",
            components={},
        )
        market_ctx = {
            "close": 500.0,
            "net_gex": -3.0e8,
            "gamma_flip": 499.0,
            "put_call_ratio": 0.9,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [498.0, 499.0, 500.0],
            "iv_rank": 0.4,
        }
        independent_results = [
            SimpleNamespace(
                name="vol_expansion",
                score=0.62,
                context={"signal": "bullish_expansion", "triggered": True},
            )
        ]
        cash_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=0.0,
            normalized_score=0.0,
            direction="neutral",
            target_positions=[],
            total_target_contracts=0,
            target_heat_pct=0.0,
            rationale="cash",
        )
        independent_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=0.62,
            normalized_score=0.62,
            direction="bullish",
            target_positions=[
                TargetPosition(
                    direction="bullish",
                    strategy_type="long_straddle",
                    contracts=1,
                    option_symbol="SPY 260417C500",
                    option_type="C",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=3.1,
                    probability_of_profit=0.52,
                    expected_value=18.0,
                    kelly_fraction=0.03,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=1,
            target_heat_pct=0.01,
            rationale="independent",
            source="advanced:vol_expansion",
        )
        with patch.object(engine, "compute_target", return_value=cash_target), patch.object(
            engine, "_build_advanced_target", return_value=independent_target
        ):
            target = engine.compute_target_with_independents(
                score,
                market_ctx,
                independent_results=independent_results,
                conn=MagicMock(),
            )
        assert target.source == "advanced:vol_expansion"
        assert target.direction == "bullish"

    def test_compute_target_with_independents_stays_cash_without_advanced_setup(self):
        engine = _make_engine()
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=62.0,
            normalized_score=0.62,
            direction="controlled_trend",
            components={},
            aggregation={},
        )
        market_ctx = {
            "close": 500.0,
            "net_gex": -1.0e8,
            "gamma_flip": 499.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [499.0, 499.5, 500.0],
            "iv_rank": 0.4,
            "max_gamma_strike": 500.0,
        }
        with patch.object(engine, "_build_advanced_target", return_value=None):
            target = engine.compute_target_with_independents(
                score,
                market_ctx,
                independent_results=[],
            )
        assert target.target_positions == []
        assert "No advanced signal setup confirmed" in target.rationale


class TestTradeSlotsAndContractSizing:
    def test_compute_target_contracts_not_clamped_by_max_open_trades(self):
        engine = _make_engine()
        engine.max_open_trades = 1
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.9,
            normalized_score=0.9,
            direction="bullish",
            components={"dealer_regime": {"score": 0.8, "weight": 0.12}},
        )
        candidate = SpreadCandidate(
            rank=1,
            strategy_type="bull_call_debit",
            expiry=date(2026, 4, 17),
            dte=1,
            strikes="Long 705C / Short 710C",
            option_type="C",
            entry_debit=250.0,
            entry_credit=0.0,
            width=5.0,
            max_profit=250.0,
            max_loss=250.0,
            risk_reward_ratio=1.0,
            probability_of_profit=0.55,
            expected_value=20.0,
            sharpe_like_ratio=0.08,
            liquidity_score=0.8,
            net_delta=20.0,
            net_gamma=1.0,
            net_theta=-3.0,
            premium_efficiency=1.0,
            market_structure_fit=0.8,
            greek_alignment_score=0.8,
            edge_score=0.7,
            kelly_fraction=0.05,
            sizing_profiles=[
                SizingProfile(
                    profile="optimal",
                    contracts=10,
                    max_risk_dollars=2500.0,
                    expected_value_dollars=200.0,
                    constrained_by="kelly",
                )
            ],
        )
        market_ctx = {
            "close": 700.79,
            "net_gex": -1.0e9,
            "gamma_flip": 699.0,
            "put_call_ratio": 1.0,
            "max_pain": 700.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [699.5, 700.2, 700.79],
            "iv_rank": 0.3,
        }

        with patch.object(engine, "_select_optimizer_candidate", return_value={
                 "candidate": candidate,
                 "signal_timeframe": "intraday",
                 "signal_strength": "high",
             }), \
             patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C705"):
            target = engine.compute_target(score, market_ctx, conn=MagicMock())

        assert target.target_positions
        assert target.target_positions[0].contracts == 9

    def test_reconcile_blocks_new_trade_when_trade_slots_full(self):
        engine = _make_engine()
        engine.max_open_trades = 1
        target_position = TargetPosition(
            direction="bullish",
            strategy_type="bull_call_debit",
            contracts=3,
            option_symbol="SPY 260417C705",
            option_type="C",
            expiration=date(2026, 4, 17),
            strike=705.0,
            entry_mark=2.5,
            probability_of_profit=0.55,
            expected_value=20.0,
            kelly_fraction=0.05,
            optimizer_payload={},
        )
        target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=0.8,
            normalized_score=0.8,
            direction="bullish",
            target_positions=[target_position],
            total_target_contracts=5,
            target_heat_pct=0.01,
            rationale="test target",
        )
        open_trade = _make_trade(quantity_open=2, direction="bullish")

        with patch.object(engine, "_market_status", return_value="OPEN"), \
             patch.object(engine, "_fetch_open_trades", return_value=[open_trade]), \
             patch.object(engine, "snapshot") as snapshot, \
             patch.object(engine, "_open_position") as open_position:
            action = engine.reconcile(target, conn=MagicMock())

        assert action == "held_max_open_trades"
        open_position.assert_not_called()
        snapshot.assert_called_once()


# ---------------------------------------------------------------------------
# _spread_mark / _spread_pnl
# ---------------------------------------------------------------------------


class TestSpreadPricing:
    """Spread P&L must price every leg, not just the primary long leg.

    Regression for the bug where a bull_call_debit trade marked at the long
    call's absolute mid ($5) was compared against the net debit entry ($2.50),
    producing phantom profits on every update even when the underlying dropped.
    """

    def _debit_trade(self) -> dict:
        # Bull call debit: long 500C @ $3.00, short 505C @ $0.50, net debit $2.50
        trade = _make_trade(entry_price=2.5)
        trade["components_at_entry"] = {
            "optimizer": {
                "pricing_mode": "debit",
                "legs": [
                    {"side": "long", "option_symbol": "SPY 260410C500"},
                    {"side": "short", "option_symbol": "SPY 260410C505"},
                ],
            }
        }
        return trade

    def test_debit_spread_marks_with_both_legs(self):
        engine = _make_engine()
        # Long leg rises to 3.40, short leg rises to 0.70 => net value 2.70 (+$0.20).
        # Zero-width quotes exercise the happy path: bid == ask, so the
        # realistic exit fill (sell long at bid, buy short at ask) equals mid.
        quotes = {
            "SPY 260410C500": (3.40, 3.40, 3.40),
            "SPY 260410C505": (0.70, 0.70, 0.70),
        }
        with patch.object(engine, "_latest_option_quote", side_effect=lambda sym, *a, **kw: quotes[sym]):
            value, mode = engine._spread_mark(self._debit_trade(), NOW, conn=MagicMock())
        assert mode == "debit"
        assert value == pytest.approx(2.70)

    def test_debit_spread_exit_fill_uses_bid_for_long_and_ask_for_short(self):
        engine = _make_engine()
        # Realistic exit of a debit spread: sell long at bid, buy short at ask.
        # long 500C bid=3.30/ask=3.50; short 505C bid=0.60/ask=0.80
        # liquidation = 3.30 - 0.80 = 2.50 (tighter than the 2.70 mid).
        quotes = {
            "SPY 260410C500": (3.30, 3.50, 3.40),
            "SPY 260410C505": (0.60, 0.80, 0.70),
        }
        with patch.object(engine, "_latest_option_quote", side_effect=lambda sym, *a, **kw: quotes[sym]):
            value, mode = engine._spread_mark(self._debit_trade(), NOW, conn=MagicMock())
        assert mode == "debit"
        assert value == pytest.approx(2.50)

    def test_debit_spread_pnl_matches_both_legs(self):
        # Entry $2.50, current $2.70, 2 contracts => ($2.70 - $2.50) * 2 * 100 = $40.
        assert PortfolioEngine._spread_pnl(
            entry=2.5, mark=2.7, qty=2, pricing_mode="debit"
        ) == pytest.approx(40.0)

    def test_debit_spread_pnl_goes_negative_when_underlying_drops(self):
        # If the long leg's mid drops faster than the short leg's on a -0.3%
        # move, the net debit value falls below entry and P&L must be negative.
        assert PortfolioEngine._spread_pnl(
            entry=2.5, mark=2.3, qty=10, pricing_mode="debit"
        ) == pytest.approx(-200.0)

    def test_credit_spread_pnl_inverts_sign(self):
        # Sold the spread for $1.20 credit; cost to close has fallen to $0.40.
        # Closing profit = ($1.20 - $0.40) * 5 * 100 = $400.
        assert PortfolioEngine._spread_pnl(
            entry=1.2, mark=0.4, qty=5, pricing_mode="credit"
        ) == pytest.approx(400.0)

    def test_legacy_trade_without_legs_falls_back_to_single_leg(self):
        engine = _make_engine()
        trade = _make_trade(entry_price=2.0)
        trade["components_at_entry"] = {}  # no optimizer payload
        with patch.object(engine, "_latest_option_mark", return_value=1.75):
            value, mode = engine._spread_mark(trade, NOW, conn=MagicMock())
        assert value == 1.75
        assert mode == "debit"

    def test_missing_leg_mark_returns_none(self):
        engine = _make_engine()
        with patch.object(engine, "_latest_option_quote", return_value=None):
            value, mode = engine._spread_mark(self._debit_trade(), NOW, conn=MagicMock())
        assert value is None
        assert mode == "debit"
