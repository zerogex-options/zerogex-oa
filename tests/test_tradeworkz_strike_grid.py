"""Symbol-aware strike grid: helpers + the fleet-wide round_to_strike.

The fleet used to pick strikes with a bare ``round(snap.spot)``, which
lands on a non-existent strike for SPX (5-point grid) — the leg then never
matched an ``option_chains`` row and the trade silently failed to fill.
These tests lock in:

* ``default_strike_increment`` — $1 for ETFs, 5 for cash indices, env-
  overridable per symbol;
* ``nearest_strike`` — exactly ``round(x)`` on a $1 grid (so the fleet-wide
  swap is behaviour-preserving for SPY/QQQ/IWM) and a real 5-wide strike
  for SPX;
* ``MarketSnapshot.round_to_strike`` and that the wall-bouncer emits an
  on-grid SPX strike.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec
from src.tradeworkz.strikes import default_strike_increment, nearest_strike

MIDDAY = datetime(2026, 7, 14, 15, 30, tzinfo=timezone.utc)  # 11:30 ET


def test_default_increment_by_instrument_type():
    for etf in ("SPY", "QQQ", "IWM", "DIA"):
        assert default_strike_increment(etf) == 1.0
    for index in ("SPX", "NDX", "RUT", "DJX"):
        assert default_strike_increment(index) == 5.0


def test_default_increment_env_override(monkeypatch):
    monkeypatch.setenv("TRADEWORKZ_STRIKE_INCREMENTS", "SPX=25, SPY=0.5")
    assert default_strike_increment("SPX") == 25.0
    assert default_strike_increment("SPY") == 0.5
    assert default_strike_increment("QQQ") == 1.0  # untouched -> type default


def test_nearest_strike_is_round_on_dollar_grid():
    """The fleet-wide round(spot) -> round_to_strike swap must be a no-op on ETFs."""
    for x in (500.0, 500.4, 500.5, 501.5, 754.49, 754.5, 755.0, 219.7):
        assert nearest_strike(x, 1.0) == float(round(x))


def test_nearest_strike_snaps_to_five_wide_grid():
    assert nearest_strike(7529.4, 5.0) == 7530.0
    assert nearest_strike(7527.0, 5.0) == 7525.0
    assert nearest_strike(7532.5, 5.0) == 7530.0  # round-half-to-even -> 7530
    # A non-positive increment can never divide-by-zero; falls back to $1.
    assert nearest_strike(7529.4, 0.0) == 7529.0


def test_snapshot_round_to_strike_uses_symbol_grid():
    spy = MarketSnapshot(underlying="SPY", timestamp=MIDDAY, spot=754.6)
    spx = MarketSnapshot(underlying="SPX", timestamp=MIDDAY, spot=7529.4)
    assert spy.effective_strike_increment() == 1.0
    assert spx.effective_strike_increment() == 5.0
    assert spy.round_to_strike(spy.spot) == 755.0
    assert spx.round_to_strike(spx.spot) == 7530.0


def test_snapshot_explicit_increment_wins():
    snap = MarketSnapshot(
        underlying="SPX", timestamp=MIDDAY, spot=7529.4, strike_increment=10.0
    )
    assert snap.effective_strike_increment() == 10.0
    assert snap.round_to_strike(snap.spot) == 7530.0


def _bouncer() -> PutCallWallBouncer:
    spec = BotSpec(
        id="put_call_wall_bouncer",
        display_name="Put/Call Wall Bouncer",
        strategy_class="PutCallWallBouncer",
        tier="0DTE",
        direction_mode="context",
        universe="SPX",
        tagline="",
        description="",
        params={"wall_proximity_pct": 0.002, "wall_break_pct": 0.003},
    )
    return PutCallWallBouncer(spec, ml_state=None)


def test_bouncer_emits_on_grid_spx_strike():
    """The exact video setup, on SPX: the built put lands on a real 5-wide strike."""
    snap = MarketSnapshot(
        underlying="SPX",
        timestamp=MIDDAY,
        spot=7529.0,
        call_wall=7530.0,
        put_wall=7500.0,
        gamma_flip=7495.0,
        max_pain=7520.0,
        net_gex=2.0e10,       # SPX-scale dollar-gamma
        net_gex_pctile=90.0,  # strong for THIS symbol
    )
    sig = _bouncer().open_criteria(snap)
    assert sig is not None
    assert sig.direction == "bearish"
    strike = sig.legs[0].strike
    assert strike % 5 == 0, f"{strike} is not on SPX's 5-point grid"
    # The old round(spot) would have produced 7529 — a non-existent contract.
    assert strike == 7530.0
    assert strike != round(7529.0)


# ----------------------------------------------------------------------
# Option-symbol root resolution (SPX 0DTE must build the SPXW contract)
# ----------------------------------------------------------------------


def test_spx_leg_builds_spxw_symbol_via_symbol_aliases(monkeypatch):
    """With the documented SPX aliases, the leg matches the ingested SPXW quote."""
    from src.tradeworkz.bots.base import _synthetic_option_symbol

    monkeypatch.setenv("SYMBOL_ALIASES", "SPX=$SPXW.X")
    monkeypatch.setenv("OPTION_ROOT_ALIASES", "$SPXW.X=SPXW,$SPX.X=SPX")
    assert _synthetic_option_symbol("SPX", "put", 7530, "2026-07-14") == "SPXW 260714P7530"
    # ETFs have no alias -> unchanged.
    assert _synthetic_option_symbol("SPY", "call", 755, "2026-07-14") == "SPY 260714C755"


def test_spx_leg_builds_spxw_symbol_via_direct_option_root_alias(monkeypatch):
    """Alternative config: a direct SPX=SPXW option-root alias also resolves."""
    from src.tradeworkz.bots.base import _synthetic_option_symbol

    monkeypatch.delenv("SYMBOL_ALIASES", raising=False)
    monkeypatch.setenv("OPTION_ROOT_ALIASES", "$SPXW.X=SPXW,$SPX.X=SPX,SPX=SPXW")
    assert _synthetic_option_symbol("SPX", "put", 7530, "2026-07-14") == "SPXW 260714P7530"


def test_bouncer_spx_leg_is_fillable_spxw_contract(monkeypatch):
    """End-to-end: the bouncer's SPX put is a quotable SPXW contract, not bare SPX."""
    monkeypatch.setenv("SYMBOL_ALIASES", "SPX=$SPXW.X")
    monkeypatch.setenv("OPTION_ROOT_ALIASES", "$SPXW.X=SPXW,$SPX.X=SPX")
    snap = MarketSnapshot(
        underlying="SPX",
        timestamp=MIDDAY,
        spot=7529.0,
        call_wall=7530.0,
        put_wall=7500.0,
        gamma_flip=7495.0,
        max_pain=7520.0,
        net_gex=2.0e10,
        net_gex_pctile=90.0,
    )
    sig = _bouncer().open_criteria(snap)
    assert sig is not None
    assert sig.legs[0].option_symbol == "SPXW 260714P7530"
