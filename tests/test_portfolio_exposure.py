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
            composite_score=10.0,
            normalized_score=0.1,
            direction="high_risk_reversal",
            components={},
            aggregation={},
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
            composite_score=90.0,
            normalized_score=0.9,
            direction="trend_expansion",
            components={"dealer_regime": {"score": 0.8, "weight": 0.12}},
            aggregation={},
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
            kelly_fraction=0.10,
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

        with (
            patch.object(
                engine,
                "_select_optimizer_candidate",
                return_value={
                    "candidate": candidate,
                    "signal_timeframe": "intraday",
                    "signal_strength": "high",
                },
            ),
            patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C705"),
        ):
            target = engine.compute_target(score, market_ctx, conn=MagicMock())

        assert target.target_positions
        assert target.target_positions[0].strike == 705.0


class TestIndependentSignalTriggering:
    def test_independent_signal_can_trigger_when_composite_is_cash(self):
        engine = _make_engine()
        base_score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=10.0,
            normalized_score=0.1,
            direction="high_risk_reversal",
            components={},
            aggregation={},
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
            composite_score=72.0,
            normalized_score=0.72,
            direction="trend_expansion",
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

        with (
            patch.object(
                engine,
                "compute_target",
                return_value=PortfolioTarget(
                    underlying="SPY",
                    timestamp=NOW,
                    composite_score=10.0,
                    normalized_score=0.1,
                    direction="high_risk_reversal",
                    target_positions=[],
                    total_target_contracts=0,
                    target_heat_pct=0.0,
                    rationale="composite cash",
                ),
            ),
            patch.object(
                engine,
                "_build_advanced_target",
                return_value=independent_target,
            ),
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
            composite_score=60.0,
            normalized_score=0.6,
            direction="controlled_trend",
            components={},
            aggregation={},
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
            composite_score=60.0,
            normalized_score=0.6,
            direction="controlled_trend",
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
            composite_score=80.0,
            normalized_score=0.8,
            direction="trend_expansion",
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
        with (
            patch.object(engine, "compute_target", return_value=composite_target),
            patch.object(
                engine,
                "_build_advanced_target",
                return_value=independent_target,
            ),
        ):
            out = engine.compute_target_with_independents(
                base_score,
                market_ctx,
                independent_results=[],
            )
        assert out.source == "advanced:trap_detection"
        assert out.direction == "trend_expansion"

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
        assert "No advanced signal setup or confluence confirmed" in out.rationale


class TestFreshCrossSizingBoost:
    """Fresh-cross boost is disabled under MSI-first targeting."""

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
            kelly_fraction=0.10,
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
            composite_score=90.0,
            normalized_score=0.9,
            direction="trend_expansion",
            components={"dealer_regime": {"score": 0.8, "weight": 0.12}},
            aggregation={},
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
        with (
            patch.object(
                engine,
                "_select_optimizer_candidate",
                return_value={
                    "candidate": candidate,
                    "signal_timeframe": "intraday",
                    "signal_strength": "high",
                },
            ),
            patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C500"),
        ):
            target = engine.compute_target(self._score(), market_ctx, conn=MagicMock())
        assert target.target_positions
        return target.total_target_contracts

    def test_no_fresh_cross_uses_base_contracts(self):
        contracts = self._compute([499.0, 500.5, 501.0])
        assert contracts == 9

    def test_fresh_bullish_cross_no_longer_boosts_contracts(self):
        contracts = self._compute([498.0, 499.5, 501.0])
        assert contracts == 9

    def test_fresh_cross_not_reflected_in_rationale(self):
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
        with (
            patch.object(
                engine,
                "_select_optimizer_candidate",
                return_value={
                    "candidate": candidate,
                    "signal_timeframe": "intraday",
                    "signal_strength": "high",
                },
            ),
            patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C500"),
        ):
            target = engine.compute_target(self._score(), market_ctx, conn=MagicMock())
        assert "fresh-cross boost" not in target.rationale


class TestMarketStatusGate:
    def test_market_status_open_within_regular_options_window(self):
        status = PortfolioEngine._market_status(datetime(2026, 4, 6, 14, 0, tzinfo=timezone.utc))
        assert status == "OPEN"

    def test_market_status_closed_after_1600_et(self):
        # 20:01 UTC = 16:01 ET, one minute past the cash-equity close.
        # Trades opened in this window were the smoking gun in the trade
        # history — the underlying has stopped, but option chains still report
        # stale quotes that the engine was happy to fill against.
        status = PortfolioEngine._market_status(datetime(2026, 4, 6, 20, 1, tzinfo=timezone.utc))
        assert status == "CLOSED"

    def test_market_status_closed_at_exact_1600_et(self):
        # 20:00 UTC = 16:00 ET; treat the bell tick itself as CLOSED.
        status = PortfolioEngine._market_status(datetime(2026, 4, 6, 20, 0, tzinfo=timezone.utc))
        assert status == "CLOSED"

    def test_market_status_closed_after_1615_et(self):
        status = PortfolioEngine._market_status(datetime(2026, 4, 6, 20, 16, tzinfo=timezone.utc))
        assert status == "CLOSED"

    def test_reconcile_holds_when_market_closed(self):
        engine = _make_engine()
        target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=80.0,
            normalized_score=0.8,
            direction="trend_expansion",
            target_positions=[],
            total_target_contracts=0,
            target_heat_pct=0.0,
            rationale="test target",
        )
        open_trade = _make_trade(quantity_open=1)

        with (
            patch.object(engine, "_market_status", return_value="CLOSED"),
            patch.object(engine, "_fetch_open_trades", return_value=[open_trade]),
            patch.object(engine, "_update_trade_mark") as update_mark,
            patch.object(engine, "snapshot") as snapshot,
            patch.object(engine, "_close_trade") as close_trade,
            patch.object(engine, "_open_position") as open_position,
        ):
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
            composite_score=72.0,
            normalized_score=0.72,
            direction="trend_expansion",
            components={"dealer_regime": {"score": 0.6, "weight": 0.12}},
            aggregation={},
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
            composite_score=35.0,
            normalized_score=0.35,
            direction="chop_range",
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
        with (
            patch.object(engine, "compute_target", return_value=composite_target),
            patch.object(
                engine,
                "_build_advanced_target",
                return_value=independent_target,
            ),
        ):
            target = engine.compute_target_with_independents(
                score,
                market_ctx,
                independent_results=independent_results,
                conn=MagicMock(),
            )
        assert target.source == "advanced:gamma_vwap_confluence"
        assert target.rationale == "independent candidate"

    def test_independent_signal_can_trigger_when_composite_is_cash(self):
        engine = _make_engine()
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=15.0,
            normalized_score=0.15,
            direction="high_risk_reversal",
            components={},
            aggregation={},
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
            composite_score=10.0,
            normalized_score=0.1,
            direction="high_risk_reversal",
            target_positions=[],
            total_target_contracts=0,
            target_heat_pct=0.0,
            rationale="cash",
        )
        independent_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=62.0,
            normalized_score=0.62,
            direction="controlled_trend",
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
        with (
            patch.object(engine, "compute_target", return_value=cash_target),
            patch.object(engine, "_build_advanced_target", return_value=independent_target),
        ):
            target = engine.compute_target_with_independents(
                score,
                market_ctx,
                independent_results=independent_results,
                conn=MagicMock(),
            )
        assert target.source == "advanced:vol_expansion"
        assert target.direction == "controlled_trend"

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
            "recent_closes": [500.0, 499.5, 499.0],
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
        assert "No advanced signal setup or confluence confirmed" in target.rationale


class TestSignalConfluenceTriggers:
    """Confluence-based entry path: Basic + Advanced signals agreeing on
    direction should trigger a trade even when no single advanced signal
    individually crosses its trigger threshold."""

    @staticmethod
    def _basic(name: str, score: float) -> SimpleNamespace:
        return SimpleNamespace(name=name, score=score, context={})

    @staticmethod
    def _advanced(name: str, signal: str, score: float, triggered: bool = False) -> SimpleNamespace:
        return SimpleNamespace(
            name=name,
            score=score,
            context={"signal": signal, "triggered": triggered},
        )

    @staticmethod
    def _score() -> ScoreSnapshot:
        return ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=30.0,
            normalized_score=0.3,
            direction="chop_range",
            components={},
            aggregation={},
        )

    @staticmethod
    def _market_ctx() -> dict:
        return {
            "close": 500.0,
            "net_gex": -2.0e8,
            "gamma_flip": 499.0,
            "put_call_ratio": 0.95,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [499.0, 499.5, 500.0],
            "iv_rank": 0.4,
        }

    def test_confluence_returns_none_when_no_signals_opinionated(self):
        basic = [self._basic("tape_flow_bias", 0.05), self._basic("skew_delta", -0.03)]
        advanced = [self._advanced("squeeze_setup", "neutral", 0.1)]
        assert PortfolioEngine._signal_confluence(advanced, basic) is None

    def test_confluence_fires_on_multi_signal_bullish_agreement(self):
        basic = [
            self._basic("tape_flow_bias", 0.40),
            self._basic("vanna_charm_flow", 0.35),
            self._basic("dealer_delta_pressure", 0.30),
        ]
        advanced = [
            self._advanced("gamma_vwap_confluence", "bullish_confluence", 0.18),
        ]
        result = PortfolioEngine._signal_confluence(advanced, basic)
        assert result is not None
        assert result["direction"] == "bullish"
        assert result["agree"] == 4
        assert result["disagree"] == 0
        assert "tape_flow_bias" in result["contributors"]
        assert "gamma_vwap_confluence" in result["contributors"]

    def test_confluence_rejects_near_split(self):
        basic = [
            self._basic("tape_flow_bias", 0.40),
            self._basic("skew_delta", 0.30),
            self._basic("vanna_charm_flow", -0.35),
            self._basic("dealer_delta_pressure", -0.30),
        ]
        advanced = [
            self._advanced("squeeze_setup", "bullish_squeeze", 0.22),
            self._advanced("trap_detection", "bearish_fade", 0.20),
        ]
        result = PortfolioEngine._signal_confluence(advanced, basic)
        assert result is None

    def test_confluence_ignores_weak_opinions(self):
        basic = [
            self._basic("tape_flow_bias", 0.05),
            self._basic("skew_delta", 0.08),
            self._basic("vanna_charm_flow", 0.10),
        ]
        advanced = [self._advanced("squeeze_setup", "bullish_squeeze", 0.10)]
        assert PortfolioEngine._signal_confluence(advanced, basic) is None

    def test_confluence_triggers_trade_when_advanced_path_misses(self):
        engine = _make_engine()
        score = self._score()
        market_ctx = self._market_ctx()
        basic_results = [
            self._basic("tape_flow_bias", 0.45),
            self._basic("vanna_charm_flow", 0.40),
            self._basic("dealer_delta_pressure", 0.35),
            self._basic("gex_gradient", 0.30),
        ]
        # Advanced signal below the 0.25 per-signal trigger: strongest-signal
        # path returns None, so only confluence can fire.
        advanced_results = [
            self._advanced(
                "gamma_vwap_confluence",
                "bullish_confluence",
                0.20,
                triggered=False,
            )
        ]
        confluence_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=score.composite_score,
            normalized_score=0.4,
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
                    entry_mark=2.1,
                    probability_of_profit=0.54,
                    expected_value=10.0,
                    kelly_fraction=0.03,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=1,
            target_heat_pct=0.01,
            rationale="optimizer filled",
        )
        with patch.object(engine, "compute_target", return_value=confluence_target):
            target = engine.compute_target_with_advanced_signals(
                score,
                market_ctx,
                advanced_results=advanced_results,
                basic_results=basic_results,
                conn=MagicMock(),
            )
        assert target.source == "confluence"
        assert target.direction == "bullish"
        assert target.target_positions
        assert "Confluence bullish" in target.rationale

    def test_confluence_skipped_when_advanced_path_already_fires(self):
        engine = _make_engine()
        score = self._score()
        market_ctx = self._market_ctx()
        advanced_results = [
            self._advanced(
                "squeeze_setup",
                "bullish_squeeze",
                0.50,
                triggered=True,
            )
        ]
        basic_results = [
            self._basic("tape_flow_bias", 0.35),
            self._basic("vanna_charm_flow", 0.30),
        ]
        advanced_target = PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=score.composite_score,
            normalized_score=0.5,
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
                    entry_mark=2.5,
                    probability_of_profit=0.55,
                    expected_value=20.0,
                    kelly_fraction=0.04,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=1,
            target_heat_pct=0.01,
            rationale="advanced fired",
            source="advanced:squeeze_setup",
        )
        with (
            patch.object(engine, "_build_advanced_target", return_value=advanced_target),
            patch.object(engine, "_build_confluence_target") as conf_mock,
        ):
            target = engine.compute_target_with_advanced_signals(
                score,
                market_ctx,
                advanced_results=advanced_results,
                basic_results=basic_results,
                conn=MagicMock(),
            )
        conf_mock.assert_not_called()
        assert target.source == "advanced:squeeze_setup"


class TestTradeSlotsAndContractSizing:
    def test_compute_target_contracts_not_clamped_by_max_open_trades(self):
        engine = _make_engine()
        engine.max_open_trades = 1
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=90.0,
            normalized_score=0.9,
            direction="trend_expansion",
            components={"dealer_regime": {"score": 0.8, "weight": 0.12}},
            aggregation={},
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
            kelly_fraction=0.10,
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

        with (
            patch.object(
                engine,
                "_select_optimizer_candidate",
                return_value={
                    "candidate": candidate,
                    "signal_timeframe": "intraday",
                    "signal_strength": "high",
                },
            ),
            patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C705"),
        ):
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
            composite_score=80.0,
            normalized_score=0.8,
            direction="bullish",
            target_positions=[target_position],
            total_target_contracts=5,
            target_heat_pct=0.01,
            rationale="test target",
        )
        open_trade = _make_trade(quantity_open=2, direction="bullish")

        with (
            patch.object(engine, "_market_status", return_value="OPEN"),
            patch.object(engine, "_fetch_open_trades", return_value=[open_trade]),
            patch.object(engine, "_close_trade", return_value=0.0),
            patch.object(engine, "snapshot") as snapshot,
            patch.object(engine, "_open_position") as open_position,
        ):
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
        with patch.object(
            engine, "_latest_option_quote", side_effect=lambda sym, *a, **kw: quotes[sym]
        ):
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
        with patch.object(
            engine, "_latest_option_quote", side_effect=lambda sym, *a, **kw: quotes[sym]
        ):
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


# ---------------------------------------------------------------------------
# Phase 2.3: multi-source confirmation gate for advanced-signal entries
# ---------------------------------------------------------------------------


class TestAdvancedConfirmationGate:
    """A single advanced trigger isn't enough — at least one independent
    confirmation (basic same-direction, MSI same-direction, or another
    triggered advanced) is required before the entry path can size a trade."""

    @staticmethod
    def _adv(name: str, signal: str, score: float, triggered: bool = True):
        return SimpleNamespace(
            name=name,
            score=score,
            context={"signal": signal, "triggered": triggered},
        )

    @staticmethod
    def _basic(name: str, score: float):
        return SimpleNamespace(name=name, score=score, context={})

    @staticmethod
    def _score(composite: float = 30.0):
        normalized = composite / 100.0
        return ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=composite,
            normalized_score=normalized,
            direction="chop_range",
            components={},
            aggregation={},
        )

    @staticmethod
    def _market_ctx(closes=None):
        return {
            "close": 500.0,
            "net_gex": -2.0e8,
            "gamma_flip": 499.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": closes or [499.0, 499.5, 500.0],
            "iv_rank": 0.4,
        }

    def test_lone_advanced_signal_rejected_when_no_confirmation(self):
        primary = self._adv("squeeze_setup", "bullish_squeeze", 0.45)
        result = PortfolioEngine._evaluate_advanced_confirmation(
            primary=primary,
            primary_direction="bullish",
            advanced_results=[primary],
            basic_results=[],
            score=self._score(composite=30.0),  # MSI conviction 0.30 < 0.50 cutoff
            market_ctx=self._market_ctx(closes=[500.0, 500.0, 500.0]),  # neutral trend
        )
        assert result["passed"] is False
        assert result["label"] == "no-confirmation"

    def test_basic_signal_provides_confirmation(self):
        primary = self._adv("squeeze_setup", "bullish_squeeze", 0.45)
        result = PortfolioEngine._evaluate_advanced_confirmation(
            primary=primary,
            primary_direction="bullish",
            advanced_results=[primary],
            basic_results=[
                self._basic("tape_flow_bias", 0.40),  # > 0.30 cutoff, bullish
            ],
            score=self._score(composite=30.0),
            market_ctx=self._market_ctx(closes=[500.0, 500.0, 500.0]),
        )
        assert result["passed"] is True
        assert "basic:tape_flow_bias" in result["label"]

    def test_basic_signal_in_wrong_direction_does_not_confirm(self):
        primary = self._adv("squeeze_setup", "bullish_squeeze", 0.45)
        result = PortfolioEngine._evaluate_advanced_confirmation(
            primary=primary,
            primary_direction="bullish",
            advanced_results=[primary],
            basic_results=[
                self._basic("tape_flow_bias", -0.40),  # bearish, opposes primary
            ],
            score=self._score(composite=30.0),
            market_ctx=self._market_ctx(closes=[500.0, 500.0, 500.0]),
        )
        assert result["passed"] is False

    def test_msi_trend_confirms_when_aligned_and_above_cutoff(self):
        primary = self._adv("squeeze_setup", "bullish_squeeze", 0.45)
        result = PortfolioEngine._evaluate_advanced_confirmation(
            primary=primary,
            primary_direction="bullish",
            advanced_results=[primary],
            basic_results=[],
            score=self._score(composite=60.0),  # 0.60 conviction > 0.50 cutoff
            market_ctx=self._market_ctx(closes=[498.0, 499.0, 500.0]),  # bullish trend
        )
        assert result["passed"] is True
        assert "msi:" in result["label"]

    def test_second_advanced_signal_provides_confirmation(self):
        primary = self._adv("squeeze_setup", "bullish_squeeze", 0.45)
        secondary = self._adv("vol_expansion", "bullish_expansion", 0.32)
        result = PortfolioEngine._evaluate_advanced_confirmation(
            primary=primary,
            primary_direction="bullish",
            advanced_results=[primary, secondary],
            basic_results=[],
            score=self._score(composite=30.0),
            market_ctx=self._market_ctx(closes=[500.0, 500.0, 500.0]),
        )
        assert result["passed"] is True
        assert "adv:vol_expansion" in result["label"]


class TestAdaptiveExpirySelection:
    """Phase 3.3: 0DTE is suppressed in the first N minutes after the open."""

    def test_dte_min_bumped_to_one_in_morning_window(self):
        # 09:50 ET (= 13:50 UTC), 20 minutes into the session — inside default
        # 90-minute no-0DTE window.
        ts = datetime(2026, 4, 28, 13, 50, tzinfo=timezone.utc)
        dte_min, dte_max = PortfolioEngine._resolve_dte_window("intraday", ts)
        assert dte_min == 1
        assert dte_max >= 1

    def test_dte_window_unchanged_after_morning_cutoff(self):
        # 11:30 ET — past the default 90-minute window.
        ts = datetime(2026, 4, 28, 15, 30, tzinfo=timezone.utc)
        dte_min, dte_max = PortfolioEngine._resolve_dte_window("intraday", ts)
        # intraday window is (0, 2) per TARGET_DTE_WINDOWS — 0DTE re-enabled.
        assert dte_min == 0

    def test_dte_min_bumped_to_one_in_afternoon_window(self):
        # 14:35 ET (= 18:35 UTC), 5 minutes into the last-90 lockout.
        # Phase 4.4: late-day 0DTE pricing is dominated by pin compression
        # and charm decay — bump dte_min to keep theta on our side.
        ts = datetime(2026, 4, 28, 18, 35, tzinfo=timezone.utc)
        dte_min, dte_max = PortfolioEngine._resolve_dte_window("intraday", ts)
        assert dte_min == 1
        assert dte_max >= 1

    def test_dte_window_open_in_midday(self):
        # 13:00 ET (= 17:00 UTC) — outside both the morning block and the
        # afternoon lockout.  0DTE is allowed.
        ts = datetime(2026, 4, 28, 17, 0, tzinfo=timezone.utc)
        dte_min, _ = PortfolioEngine._resolve_dte_window("intraday", ts)
        assert dte_min == 0

    def test_swing_window_never_includes_0dte(self):
        # Swing fetches 1-7 DTE regardless of time-of-day; the no-0DTE filter
        # should be a no-op since base_min already > 0.
        ts = datetime(2026, 4, 28, 13, 35, tzinfo=timezone.utc)
        dte_min, dte_max = PortfolioEngine._resolve_dte_window("swing", ts)
        assert dte_min == 1
        assert dte_max == 7


class TestBreakoutBoost:
    """Phase 3.2: breakout-eligible advanced signals get bigger size and a
    wider take-profit target via per-trade overrides."""

    @staticmethod
    def _target(direction: str = "bullish", contracts: int = 4) -> PortfolioTarget:
        return PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=60.0,
            normalized_score=0.6,
            direction=direction,
            target_positions=[
                TargetPosition(
                    direction=direction,
                    strategy_type="bull_call_debit",
                    contracts=contracts,
                    option_symbol="SPY 260417C500",
                    option_type="C",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.50,
                    probability_of_profit=0.55,
                    expected_value=20.0,
                    kelly_fraction=0.10,
                    optimizer_payload={"pricing_mode": "debit"},
                )
            ],
            total_target_contracts=contracts,
            target_heat_pct=0.005,
            rationale="optimizer base",
        )

    def test_breakout_signal_scales_contracts_and_stamps_override(self):
        target = self._target(contracts=4)
        label = PortfolioEngine._apply_breakout_boost(target, "range_break_imminence")
        assert label is not None and "breakout-boost" in label
        # 4 * 1.50 = 6 contracts.
        assert target.target_positions[0].contracts == 6
        assert target.total_target_contracts == 6
        # Per-trade target_pct override now lives on the optimizer payload.
        risk_overrides = target.target_positions[0].optimizer_payload["risk_overrides"]
        assert risk_overrides["target_pct"] == 1.00
        assert risk_overrides["breakout_boost"]["signal"] == "range_break_imminence"

    def test_non_breakout_signal_leaves_target_unchanged(self):
        target = self._target(contracts=4)
        label = PortfolioEngine._apply_breakout_boost(target, "vol_expansion")
        assert label is None
        assert target.target_positions[0].contracts == 4
        assert "risk_overrides" not in target.target_positions[0].optimizer_payload

    def test_risk_plan_uses_overridden_target_pct(self):
        engine = _make_engine()
        target = self._target(contracts=4)
        PortfolioEngine._apply_breakout_boost(target, "squeeze_setup")
        tp = target.target_positions[0]
        risk = engine._build_risk_plan(tp, NOW)
        # Default target_pct=0.50 would put target at 2.50 * 1.50 = 3.75.
        # Boost target_pct=1.00 widens to 2.50 * 2.00 = 5.00.
        assert risk["target_price"] == pytest.approx(5.00)
        assert risk["target_pct"] == 1.00


class TestAttributionMetadata:
    """Phase 4.1: every opened trade carries source/regime/composite/time
    bucket metadata in components_at_entry["attribution"]."""

    def _target(
        self,
        *,
        composite: float = 65.0,
        direction: str = "bullish",
        source: str = "advanced:squeeze_setup",
        rationale: str = "test rationale",
    ) -> PortfolioTarget:
        return PortfolioTarget(
            underlying="SPY",
            timestamp=datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc),  # 10:00 ET
            composite_score=composite,
            normalized_score=composite / 100.0,
            direction=direction,
            target_positions=[
                TargetPosition(
                    direction=direction,
                    strategy_type="bull_call_debit",
                    contracts=4,
                    option_symbol="SPY 260417C500",
                    option_type="C",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.5,
                    probability_of_profit=0.55,
                    expected_value=20.0,
                    kelly_fraction=0.10,
                    optimizer_payload={
                        "pricing_mode": "debit",
                        "drawdown_multiplier": 1.0,
                    },
                )
            ],
            total_target_contracts=4,
            target_heat_pct=0.005,
            rationale=rationale,
            source=source,
        )

    def test_attribution_captures_source_and_regime(self):
        engine = _make_engine()
        target = self._target(composite=65.0)
        attribution = engine._build_attribution(target, target.target_positions[0])
        assert attribution["source"] == "advanced:squeeze_setup"
        assert attribution["regime"] == "controlled_trend"  # 40 <= 65 < 70
        assert attribution["composite_score"] == 65.0
        assert attribution["direction"] == "bullish"

    def test_attribution_classifies_open_morning_lunch_afternoon_close(self):
        engine = _make_engine()
        cases = [
            (datetime(2026, 4, 28, 13, 45, tzinfo=timezone.utc), "open"),  # 09:45 ET
            (datetime(2026, 4, 28, 15, 0, tzinfo=timezone.utc), "morning"),  # 11:00 ET
            (datetime(2026, 4, 28, 16, 30, tzinfo=timezone.utc), "lunch"),  # 12:30 ET
            (datetime(2026, 4, 28, 18, 30, tzinfo=timezone.utc), "afternoon"),  # 14:30 ET
            (datetime(2026, 4, 28, 19, 45, tzinfo=timezone.utc), "close"),  # 15:45 ET
        ]
        for ts, expected in cases:
            target = self._target()
            target.timestamp = ts
            attribution = engine._build_attribution(target, target.target_positions[0])
            assert attribution["time_bucket"] == expected, f"{ts} -> {attribution['time_bucket']}"

    def test_attribution_records_breakout_boost_when_present(self):
        engine = _make_engine()
        target = self._target(source="advanced:squeeze_setup")
        # Simulate that _apply_breakout_boost has stamped overrides.
        target.target_positions[0].optimizer_payload["risk_overrides"] = {
            "target_pct": 1.0,
            "breakout_boost": {
                "signal": "squeeze_setup",
                "size_multiplier": 1.5,
                "target_pct": 1.0,
            },
        }
        attribution = engine._build_attribution(target, target.target_positions[0])
        assert attribution["breakout_boost_applied"] is True
        assert attribution["boost_size_multiplier"] == 1.5
        assert attribution["boost_target_pct"] == 1.0

    def test_attribution_persisted_when_opening_position(self):
        """End-to-end: _open_position writes the attribution sub-block."""
        engine = _make_engine()
        target = self._target()
        captured = {}

        class _RecordingCursor:
            def execute(self_, sql, params):
                captured["params"] = params

        class _RecordingConn:
            def cursor(self_):
                cur = _RecordingCursor()
                cur.rowcount = 1
                return cur

        engine._open_position(target.target_positions[0], target, _RecordingConn())
        # components_at_entry is the last positional parameter (json-encoded).
        components_json = captured["params"][-1]
        assert "attribution" in components_json
        assert "source" in components_json
        assert "regime" in components_json
        assert "time_bucket" in components_json


class TestDrawdownAwareSizing:
    """Phase 4.3: rolling-PnL circuit breaker pulls size back after losses."""

    def _conn_with_pnl(self, pnl_rows: list[float]):
        cur = MagicMock()
        cur.fetchall.return_value = [(p,) for p in pnl_rows]
        conn = MagicMock()
        conn.cursor.return_value = cur
        return conn

    def test_no_drawdown_engaged_when_rolling_pnl_positive(self):
        engine = _make_engine()
        # 10 winners summing to +$5,000 — no breaker.
        conn = self._conn_with_pnl([500.0] * 10)
        state = engine._drawdown_sizing_state(conn=conn)
        assert state["engaged"] is False
        assert state["multiplier"] == 1.0

    def test_drawdown_engaged_when_rolling_pnl_below_trigger(self):
        engine = _make_engine()
        # Default trigger: -2% of $1M = -$20,000.  Sum of -$25,000 trips it.
        conn = self._conn_with_pnl([-2500.0] * 10)
        state = engine._drawdown_sizing_state(conn=conn)
        assert state["engaged"] is True
        assert state["multiplier"] == 0.50  # default SIGNALS_DRAWDOWN_SIZE_MULTIPLIER
        assert state["rolling_pnl"] <= -20000.0

    def test_drawdown_state_safe_under_db_failure(self):
        engine = _make_engine()
        conn = MagicMock()
        conn.cursor.side_effect = RuntimeError("db down")
        state = engine._drawdown_sizing_state(conn=conn)
        assert state["multiplier"] == 1.0
        assert state["engaged"] is False

    def test_drawdown_disabled_when_env_off(self, monkeypatch):
        monkeypatch.setenv("SIGNALS_DRAWDOWN_AWARE_SIZING_ENABLED", "false")
        # Reload config + module so the disabled flag takes effect.
        import importlib
        import src.config as config
        import src.signals.portfolio_engine as pe

        importlib.reload(config)
        importlib.reload(pe)
        with patch("src.signals.portfolio_engine.get_canonical_symbol", return_value="SPY"):
            engine = pe.PortfolioEngine("SPY")
        conn = self._conn_with_pnl([-9_999_999.0])  # extreme loss
        state = engine._drawdown_sizing_state(conn=conn)
        assert state["engaged"] is False
        assert state["multiplier"] == 1.0
        # Restore default state for downstream tests.
        monkeypatch.delenv("SIGNALS_DRAWDOWN_AWARE_SIZING_ENABLED", raising=False)
        importlib.reload(config)
        importlib.reload(pe)


class TestRegimeFilterIntegration:
    """compute_target_with_advanced_signals should ask the regime filter
    whether to suppress *new* entries (no held position OR opposite direction).
    """

    @staticmethod
    def _score(composite: float = 50.0):
        return ScoreSnapshot(
            timestamp=NOW,  # 14:00 UTC = 10:00 ET, normal window by default
            underlying="SPY",
            composite_score=composite,
            normalized_score=composite / 100.0,
            direction="controlled_trend",
            components={},
            aggregation={},
        )

    @staticmethod
    def _build_target(direction: str = "bullish") -> PortfolioTarget:
        return PortfolioTarget(
            underlying="SPY",
            timestamp=NOW,
            composite_score=50.0,
            normalized_score=0.5,
            direction=direction,
            target_positions=[
                TargetPosition(
                    direction=direction,
                    strategy_type="bull_call_debit",
                    contracts=2,
                    option_symbol="SPY 260417C500",
                    option_type="C",
                    expiration=date(2026, 4, 17),
                    strike=500.0,
                    entry_mark=2.5,
                    probability_of_profit=0.55,
                    expected_value=20.0,
                    kelly_fraction=0.10,
                    optimizer_payload={},
                )
            ],
            total_target_contracts=2,
            target_heat_pct=0.01,
            rationale="advanced fired",
            source="advanced:squeeze_setup",
        )

    def test_filter_skip_returns_cash_when_no_held_position(self):
        engine = _make_engine()
        target = self._build_target()
        from src.signals import regime_filter as rf

        with (
            patch.object(
                engine,
                "compute_target",
                return_value=PortfolioTarget(
                    underlying="SPY",
                    timestamp=NOW,
                    composite_score=50.0,
                    normalized_score=0.5,
                    direction="controlled_trend",
                    target_positions=[],
                    total_target_contracts=0,
                    target_heat_pct=0.0,
                    rationale="composite cash",
                ),
            ),
            patch.object(engine, "_build_advanced_target", return_value=target),
            patch.object(engine, "_current_position_direction", return_value="neutral"),
            patch.object(
                rf, "evaluate", return_value=rf.FilterDecision(skip=True, reason="Lunch chop test")
            ),
        ):
            out = engine.compute_target_with_advanced_signals(
                self._score(),
                {
                    "close": 500.0,
                    "net_gex": -1e8,
                    "gamma_flip": 499.0,
                    "max_gamma_strike": 500.0,
                    "put_call_ratio": 1.0,
                    "max_pain": 500.0,
                    "smart_call": 0.0,
                    "smart_put": 0.0,
                    "recent_closes": [499.0, 499.5, 500.0],
                    "iv_rank": 0.4,
                },
                advanced_results=[],
                basic_results=[],
                conn=MagicMock(),
            )
        assert out.target_positions == []
        assert "Regime filter" in out.rationale
        assert "Lunch chop test" in out.rationale

    def test_filter_does_not_block_same_direction_hold(self):
        engine = _make_engine()
        target = self._build_target(direction="bullish")
        from src.signals import regime_filter as rf

        with (
            patch.object(
                engine,
                "compute_target",
                return_value=PortfolioTarget(
                    underlying="SPY",
                    timestamp=NOW,
                    composite_score=50.0,
                    normalized_score=0.5,
                    direction="controlled_trend",
                    target_positions=[],
                    total_target_contracts=0,
                    target_heat_pct=0.0,
                    rationale="composite cash",
                ),
            ),
            patch.object(engine, "_build_advanced_target", return_value=target),
            patch.object(engine, "_current_position_direction", return_value="bullish"),
            patch.object(rf, "evaluate") as eval_mock,
        ):
            out = engine.compute_target_with_advanced_signals(
                self._score(),
                {
                    "close": 500.0,
                    "net_gex": -1e8,
                    "gamma_flip": 499.0,
                    "max_gamma_strike": 500.0,
                    "put_call_ratio": 1.0,
                    "max_pain": 500.0,
                    "smart_call": 0.0,
                    "smart_put": 0.0,
                    "recent_closes": [499.0, 499.5, 500.0],
                    "iv_rank": 0.4,
                },
                advanced_results=[],
                basic_results=[],
                conn=MagicMock(),
            )
        eval_mock.assert_not_called()
        assert out.target_positions  # held position retained


class TestTrendDirectionThreshold:
    """Phase 4.4: a 0.05% threshold flipped on single-tick noise; require a
    real multi-bar move (15bps default) before _msi_trend_direction commits."""

    def test_micro_noise_returns_neutral(self):
        # 0.10% over 5 bars — under the 15bps default threshold.  The legacy
        # 5bps trigger fired bullish here and produced the bull→bear→bull
        # whipsaws seen in QQQ trade history.
        closes = [500.0, 500.1, 500.2, 500.3, 500.5]
        assert PortfolioEngine._msi_trend_direction({"recent_closes": closes}) == "neutral"

    def test_real_uptrend_returns_bullish(self):
        # 0.40% over 5 bars — clears the 15bps floor.
        closes = [500.0, 500.4, 500.8, 501.5, 502.0]
        assert PortfolioEngine._msi_trend_direction({"recent_closes": closes}) == "bullish"

    def test_real_downtrend_returns_bearish(self):
        closes = [500.0, 499.5, 499.0, 498.5, 498.0]
        assert PortfolioEngine._msi_trend_direction({"recent_closes": closes}) == "bearish"

    def test_short_history_falls_back_to_3_bar(self):
        # Only 3 bars available — use full history rather than returning
        # neutral. 0.30% over 3 bars > 15bps → bullish.
        closes = [498.5, 499.2, 500.0]
        assert PortfolioEngine._msi_trend_direction({"recent_closes": closes}) == "bullish"


class TestChopRangeConvictionGate:
    """Phase 4.4: directional debits in chop_range with weak conviction are
    blocked even when an advanced/confluence trigger fires."""

    @staticmethod
    def _score(composite: float) -> ScoreSnapshot:
        return ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=composite,
            normalized_score=composite / 100.0,
            direction="chop_range",
            components={},
            aggregation={"advanced_trigger": True},  # signal-driven path
        )

    @staticmethod
    def _market_ctx() -> dict:
        return {
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

    def test_low_conviction_chop_returns_cash_even_with_advanced_trigger(self):
        engine = _make_engine()
        # MSI 25 → conviction 0.25 < 0.30 default chop floor.
        out = engine.compute_target(
            self._score(composite=25.0), self._market_ctx(), conn=MagicMock()
        )
        assert out.target_positions == []
        assert "Chop regime conviction" in out.rationale

    def test_chop_with_conviction_at_floor_passes_gate(self):
        """At conviction == floor the gate uses strict <, so this proceeds.

        We patch the optimizer so the test exercises the gate, not the full
        sizing pipeline.
        """
        engine = _make_engine()
        score = self._score(composite=30.0)  # conviction 0.30 == floor (not <)
        with (patch.object(engine, "_select_optimizer_candidate", return_value=None),):
            out = engine.compute_target(score, self._market_ctx(), conn=MagicMock())
        # Optimizer returned None, so we still end in cash — but for a
        # different reason (no positive-EV structure), not the chop gate.
        assert "Chop regime conviction" not in out.rationale


class TestDailyLossKillSwitch:
    """Phase 4.4: once today's realized losses exceed the kill threshold,
    every subsequent compute_target returns a cash target."""

    @staticmethod
    def _conn_with_today_pnl(realized: float):
        cur = MagicMock()
        cur.fetchone.return_value = (realized,)
        conn = MagicMock()
        conn.cursor.return_value = cur
        return conn

    @staticmethod
    def _score() -> ScoreSnapshot:
        return ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=80.0,
            normalized_score=0.8,
            direction="trend_expansion",
            components={},
            aggregation={},
        )

    @staticmethod
    def _market_ctx() -> dict:
        return {
            "close": 500.0,
            "net_gex": -1.0e9,
            "gamma_flip": 499.0,
            "put_call_ratio": 1.0,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [498.0, 499.0, 500.0],
            "iv_rank": 0.4,
        }

    def test_kill_engaged_when_today_realized_breaches_threshold(self):
        engine = _make_engine()
        # Default kill: -1% of $1M = -$10,000.  -$15K trips it.
        conn = self._conn_with_today_pnl(-15_000.0)
        out = engine.compute_target(self._score(), self._market_ctx(), conn=conn)
        assert out.target_positions == []
        assert "Daily loss kill" in out.rationale

    def test_kill_disengaged_when_today_pnl_within_limit(self):
        engine = _make_engine()
        # -$5K well above the -$10K floor — engine continues normally.
        conn = self._conn_with_today_pnl(-5_000.0)
        with (patch.object(engine, "_select_optimizer_candidate", return_value=None),):
            out = engine.compute_target(self._score(), self._market_ctx(), conn=conn)
        assert "Daily loss kill" not in out.rationale

    def test_daily_loss_state_safe_under_db_failure(self):
        engine = _make_engine()
        conn = MagicMock()
        conn.cursor.side_effect = RuntimeError("db down")
        state = engine._daily_loss_state(NOW, conn=conn)
        assert state["kill"] is False
        assert state["realized_today"] == 0.0
