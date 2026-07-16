"""Tests for the PutWallMagnetReversal thesis backtest's pure logic.

The DB-loading path needs a live database and is exercised by running the
tool; here we lock the analysis math that decides win/loss/expectancy and the
gate reconstruction that must stay faithful to the live bot.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.tools.put_wall_magnet_backtest import (
    Bar,
    GateParams,
    TradeResult,
    distribution_knots,
    dollar_wall_strength,
    enrich,
    forward_outcome,
    qualifies,
    run,
    summarize,
    target_for,
)
from src.tradeworkz.context import MarketSnapshot

TS = datetime(2026, 7, 16, 15, 30, tzinfo=timezone.utc)


# ----------------------------------------------------------------------
# dollar_wall_strength / distribution_knots
# ----------------------------------------------------------------------


def test_dollar_wall_strength_matches_walls_convention():
    assert dollar_wall_strength(1000.0, 750.0) == pytest.approx(1000.0 * 750.0 * 750.0)


def test_distribution_knots_none_when_thin():
    assert distribution_knots([1.0, 2.0, 3.0]) is None
    knots = distribution_knots(list(range(100)))
    assert knots is not None and len(knots) == 5
    assert knots[2] == pytest.approx(49.5, abs=1.0)  # p50 of 0..99


# ----------------------------------------------------------------------
# forward_outcome — the win/loss/timeout classifier
# ----------------------------------------------------------------------


def test_forward_outcome_win_first_touch():
    outcome, r = forward_outcome(749.0, 751.75, 746.878, [749.5, 750.0, 751.8])
    assert outcome == "win"
    assert r == pytest.approx((751.75 - 749.0) / (749.0 - 746.878))


def test_forward_outcome_loss_is_minus_one_r():
    outcome, r = forward_outcome(749.0, 751.75, 746.878, [748.5, 746.8])
    assert outcome == "loss"
    assert r == pytest.approx(-1.0)


def test_forward_outcome_stop_takes_priority_when_hit_first():
    # Stop touched before target in sequence -> loss, even though a later
    # bar would have hit target.
    outcome, _ = forward_outcome(749.0, 751.75, 746.878, [746.0, 752.0])
    assert outcome == "loss"


def test_forward_outcome_timeout_marks_to_final():
    outcome, r = forward_outcome(749.0, 751.75, 746.878, [749.2, 749.5])
    assert outcome == "timeout"
    assert r == pytest.approx((749.5 - 749.0) / (749.0 - 746.878))


def test_forward_outcome_invalid_when_stop_above_entry():
    outcome, r = forward_outcome(749.0, 751.0, 750.0, [749.0])
    assert outcome == "invalid"


# ----------------------------------------------------------------------
# qualifies — reuses the live snapshot gates
# ----------------------------------------------------------------------


def _snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=TS,
        spot=749.0,
        net_gex=-5.0e8,  # negative_weak via absolute fallback
        gamma_flip=751.75,
        put_wall=748.0,
        prior_put_wall=748.0,
        put_wall_strength_pctile=95.0,
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_qualifies_on_the_intended_setup():
    assert qualifies(_snap(), GateParams()) is True


def test_qualifies_rejects_positive_gamma():
    assert qualifies(_snap(net_gex=1.0e9), GateParams()) is False


def test_qualifies_rejects_ordinary_wall():
    assert qualifies(_snap(put_wall_strength_pctile=70.0), GateParams()) is False


def test_qualifies_rejects_missing_pctile():
    assert qualifies(_snap(put_wall_strength_pctile=None), GateParams()) is False


def test_qualifies_rejects_far_from_wall_and_collapsing_wall():
    assert qualifies(_snap(spot=752.0), GateParams()) is False
    assert qualifies(_snap(prior_put_wall=752.0), GateParams()) is False


def test_target_prefers_gamma_flip():
    assert target_for(_snap()) == 751.75
    assert target_for(_snap(gamma_flip=700.0, max_pain=752.0)) == 752.0  # flip below spot


# ----------------------------------------------------------------------
# summarize — the by-percentile bucket view (the thesis test)
# ----------------------------------------------------------------------


def test_summarize_buckets_and_expectancy():
    trades = [
        TradeResult(TS, 749, 751, 747, "win", 1.0, 92.0),
        TradeResult(TS, 749, 751, 747, "loss", -1.0, 93.0),
        TradeResult(TS, 749, 751, 747, "win", 2.0, 99.5),
    ]
    stats = summarize(trades)
    assert stats["overall"]["n"] == 3
    assert stats["overall"]["expectancy_r"] == pytest.approx((1.0 - 1.0 + 2.0) / 3)
    assert stats["by_pctile"]["90-95"]["n"] == 2
    assert stats["by_pctile"]["99-100"]["n"] == 1
    assert stats["by_pctile"]["99-100"]["win_rate"] == 1.0


# ----------------------------------------------------------------------
# enrich + run — end-to-end over a synthetic series
# ----------------------------------------------------------------------


def _bar(i: int, put_gamma: float, spot: float = 749.0) -> Bar:
    return Bar(
        ts=TS + timedelta(minutes=i),
        spot=spot,
        net_gex=-5.0e8,
        gamma_flip=751.75,
        max_pain=751.0,
        put_wall=748.0,
        put_gamma_at_wall=put_gamma,
    )


def test_enrich_lags_wall_and_ranks_strength():
    # Spread the ordinary strengths (500..5300) so the distribution is not
    # degenerate, then drop in one mega wall.
    bars = [_bar(i, 500.0 + i * 200.0) for i in range(25)]
    bars[12].put_gamma_at_wall = 50_000.0
    enrich(bars)
    assert bars[0].prior_put_wall is None
    assert bars[1].prior_put_wall == 748.0
    # The mega wall ranks at the top; the smallest ordinary one sits low.
    assert bars[12].put_wall_pctile is not None and bars[12].put_wall_pctile >= 90.0
    assert bars[0].put_wall_pctile is not None and bars[0].put_wall_pctile < 90.0


def test_run_finds_the_mega_wall_setup_and_scores_a_bounce():
    bars = [_bar(i, 500.0 + i * 200.0) for i in range(25)]
    bars[12] = _bar(12, 50_000.0)
    # run() sources spot from the closes series (nearest-preceding close, as
    # the live snapshot does). Ordinary bars align to 755 (far from the 748
    # wall -> proximity fails); only the mega bar at minute 12 aligns to 749,
    # pressed against the wall -> exactly one qualifying entry, which bounces.
    closes = [(TS + timedelta(minutes=i), 755.0) for i in range(12)]
    closes.append((TS + timedelta(minutes=12), 749.0))  # mega entry spot
    closes += [
        (TS + timedelta(minutes=13), 750.0),
        (TS + timedelta(minutes=14), 752.0),  # >= flip 751.75 -> win
    ]
    trades = run("SPY", bars, closes, GateParams())
    assert len(trades) == 1
    assert trades[0].outcome == "win"
    assert trades[0].entry_spot == pytest.approx(749.0)
    assert trades[0].put_wall_pctile >= 90.0
