"""Tests for PortfolioEngine portfolio reconciliation logic."""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

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


class TestDealerRegimeHardGates:
    def test_bullish_gate_passes_when_holding_above_flip_and_drs_strong(self):
        engine = _make_engine()
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=0.65,
            normalized_score=0.65,
            direction="bullish",
            components={"dealer_regime": {"score": 0.55, "weight": 0.12}},
        )
        ok, _reason = engine._passes_dealer_regime_gates(
            score,
            {"close": 502.0, "gamma_flip": 500.0, "recent_closes": [499.0, 501.0, 502.0]},
        )
        assert ok is True

    def test_bearish_gate_requires_fresh_cross_below_flip(self):
        engine = _make_engine()
        score = ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=-0.7,
            normalized_score=0.7,
            direction="bearish",
            components={"dealer_regime": {"score": -0.35, "weight": 0.12}},
        )
        ok, reason = engine._passes_dealer_regime_gates(
            score,
            {"close": 498.0, "gamma_flip": 500.0, "recent_closes": [497.0, 498.5, 498.0]},
        )
        assert ok is False
        assert "fresh cross below gamma flip" in reason


# ---------------------------------------------------------------------------
# Scalp tier + DRS override (compute_target threshold classification)
# ---------------------------------------------------------------------------

class TestScalpTierAndDrsOverride:
    """These tests exercise the pre-optimizer gating logic of compute_target
    by forcing the optimizer lookup to return None -- the interesting behavior
    is what ``reason`` ends up on the cash target."""

    @staticmethod
    def _ctx() -> dict:
        # Bearish day -- price already below gamma flip, no fresh cross.
        return {
            "close": 498.0,
            "net_gex": -1.0e9,
            "gamma_flip": 500.0,
            "put_call_ratio": 1.1,
            "max_pain": 500.0,
            "smart_call": 0.0,
            "smart_put": 0.0,
            "recent_closes": [497.5, 498.2, 498.0],
            "iv_rank": 0.5,
        }

    @staticmethod
    def _score(normalized: float, direction: str = "bearish") -> ScoreSnapshot:
        return ScoreSnapshot(
            timestamp=NOW,
            underlying="SPY",
            composite_score=-normalized if direction == "bearish" else normalized,
            normalized_score=normalized,
            direction=direction,
            components={"dealer_regime": {"score": -0.3, "weight": 0.12}},
        )

    def test_below_scalp_threshold_goes_to_cash(self):
        engine = _make_engine()
        score = self._score(0.25)
        with patch.object(engine, "_select_optimizer_candidate", return_value=None):
            target = engine.compute_target(score, self._ctx())
        assert target.target_positions == []
        assert "below scalp threshold" in target.rationale

    def test_scalp_band_bypasses_drs_and_tries_optimizer(self):
        """Score in the scalp band (0.36 <= n < effective) should skip the DRS
        gate and attempt an optimizer lookup -- we prove this by verifying the
        cash rationale is "no positive-EV structure" rather than "DRS blocked"."""
        engine = _make_engine()
        score = self._score(0.40)  # above 0.36 scalp, below 0.52 full
        with patch.object(engine, "_select_optimizer_candidate", return_value=None):
            target = engine.compute_target(score, self._ctx())
        assert target.target_positions == []
        # DRS gate would have blocked a bearish entry on a no-fresh-cross day,
        # but scalp tier bypasses it -- we should fail at the optimizer step.
        assert "positive-EV" in target.rationale
        assert "DRS" not in target.rationale

    def test_full_tier_below_override_still_hits_drs_gate(self):
        """A bearish signal at 0.55 (full tier, below override threshold 0.70)
        should still be blocked by the DRS gate on a no-fresh-cross day."""
        engine = _make_engine()
        score = self._score(0.55)
        with patch.object(engine, "_score_trend_confirmation", return_value=True), \
             patch.object(engine, "_select_optimizer_candidate", return_value=None):
            target = engine.compute_target(score, self._ctx())
        assert target.target_positions == []
        assert "DRS hard gate" in target.rationale

    def test_strong_conviction_overrides_drs_gate(self):
        """Score at/above the 0.70 override threshold should bypass DRS."""
        engine = _make_engine()
        score = self._score(0.75)
        with patch.object(engine, "_score_trend_confirmation", return_value=True), \
             patch.object(engine, "_select_optimizer_candidate", return_value=None):
            target = engine.compute_target(score, self._ctx())
        # DRS override should fire; cash rationale should be optimizer-based.
        assert "positive-EV" in target.rationale
        assert "DRS hard gate" not in target.rationale


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

        with patch.object(engine, "_passes_dealer_regime_gates", return_value=(True, "ok")), \
             patch.object(engine, "_score_trend_confirmation", return_value=True), \
             patch.object(engine, "_select_optimizer_candidate", return_value={
                 "candidate": candidate,
                 "signal_timeframe": "intraday",
                 "signal_strength": "high",
             }), \
             patch.object(engine, "_resolve_option_symbol_for_leg", return_value="SPY 260417C705"):
            target = engine.compute_target(score, market_ctx, conn=MagicMock())

        assert target.target_positions
        assert target.target_positions[0].strike == 705.0


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

        with patch.object(engine, "_passes_dealer_regime_gates", return_value=(True, "ok")), \
             patch.object(engine, "_score_trend_confirmation", return_value=True), \
             patch.object(engine, "_select_optimizer_candidate", return_value={
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
