"""Risk sizing must key on a structure's TRUE max loss, not the net premium.

An iron condor enters at a near-zero net (a ~$0.06 debit in the live incident),
but its max loss is the wing width (~$2/share). Dividing the heat budget by the
$0.06 net over-levered the position ~34x (309 contracts) and turned a $0.22 move
into -$6.8K. structure_max_loss_per_share computes the real per-share max loss
from the expiry payoff so compute_contracts sizes on it.
"""

from __future__ import annotations

import pytest

from src.tradeworkz.models import BotCapital
from src.tradeworkz.sizing import compute_contracts, structure_max_loss_per_share


def _cap() -> BotCapital:
    return BotCapital(
        bot_id="b", starting_capital=140_000, current_capital=140_000,
        peak_capital=140_000, max_heat_pct=0.06, kelly_fraction=0.5, daily_kill_pct=0.02,
    )


_CONDOR = [
    {"strike": 742, "option_type": "put", "side": "long"},
    {"strike": 744, "option_type": "put", "side": "short"},
    {"strike": 747, "option_type": "call", "side": "short"},
    {"strike": 749, "option_type": "call", "side": "long"},
]
_VERTICAL = [
    {"strike": 745, "option_type": "call", "side": "long"},
    {"strike": 746, "option_type": "call", "side": "short"},
]
_STRADDLE = [
    {"strike": 745, "option_type": "call", "side": "long"},
    {"strike": 745, "option_type": "put", "side": "long"},
]


# ── structure_max_loss_per_share ──────────────────────────────────────


def test_condor_max_loss_is_wing_width_plus_debit():
    # 2-wide wings + $0.06 net debit -> $2.06/share max loss (not $0.06).
    assert structure_max_loss_per_share(_CONDOR, 0.06) == pytest.approx(2.06)


def test_long_single_max_loss_is_the_premium():
    legs = [{"strike": 745, "option_type": "call", "side": "long"}]
    assert structure_max_loss_per_share(legs, 2.52) == pytest.approx(2.52)


def test_debit_vertical_max_loss_is_the_debit():
    assert structure_max_loss_per_share(_VERTICAL, 0.62) == pytest.approx(0.62)


def test_straddle_max_loss_is_the_debit():
    assert structure_max_loss_per_share(_STRADDLE, 3.0) == pytest.approx(3.0)


def test_empty_or_strikeless_legs_fall_back_to_entry():
    assert structure_max_loss_per_share([], 1.25) == pytest.approx(1.25)


# ── compute_contracts sizes on max loss ───────────────────────────────


def test_condor_sizes_far_smaller_on_max_loss():
    cap = _cap()
    naive = compute_contracts(capital=cap, entry_price=0.06, conviction=0.6, size_multiplier=1.0)
    fixed = compute_contracts(
        capital=cap, entry_price=0.06, conviction=0.6, size_multiplier=1.0,
        max_loss_per_share=structure_max_loss_per_share(_CONDOR, 0.06),
    )
    # ~34x fewer contracts (206/6), and the real risk now ~ the heat budget.
    assert fixed < naive
    assert naive / fixed == pytest.approx(206 / 6, rel=0.1)
    assert fixed * 206 <= cap.current_capital * cap.max_heat_pct * 1.5  # bounded by heat


def test_debit_structure_sizing_unchanged():
    cap = _cap()
    without = compute_contracts(capital=cap, entry_price=0.62, conviction=0.6, size_multiplier=1.0)
    with_ml = compute_contracts(
        capital=cap, entry_price=0.62, conviction=0.6, size_multiplier=1.0,
        max_loss_per_share=structure_max_loss_per_share(_VERTICAL, 0.62),
    )
    assert without == with_ml  # max_loss == entry_price for debits


def test_none_max_loss_falls_back_to_entry_price():
    cap = _cap()
    a = compute_contracts(capital=cap, entry_price=1.20, conviction=0.6, size_multiplier=1.0)
    b = compute_contracts(
        capital=cap, entry_price=1.20, conviction=0.6, size_multiplier=1.0, max_loss_per_share=None)
    assert a == b
