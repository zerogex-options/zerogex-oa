"""Scale-out ladder: T1/T2 tranche takes, runner stops, and the one
consolidated size-weighted close.

Covers the exit-decision geometry in ``BaseBot._scale_exit_decision``, the
pure ``compute_ladder_levels`` helper, ``partial_close_position`` (books a
tranche onto the position without a trade row or capital move), and
``close_position``'s consolidation (one immutable tw_trades row whose
size-weighted exit reproduces the summed tranche realized exactly, so
Invariant A holds by construction).

See docs/design/tradeworkz-exit-strategy.md.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.tradeworkz.bots.base import BaseBot, compute_ladder_levels
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, ExitDecision, OpenPosition
from src.tradeworkz.reconciler import close_position, partial_close_position

# Running example from the design doc: entry_spot 749, wall 750, R = 1.00.
ENTRY_SPOT = 749.0
WALL = 750.0
T1 = 749.90   # 0.90 R
S2 = 749.75   # 0.75 R
T2 = 750.50   # 1.50 R


def _snap(spot: float) -> MarketSnapshot:
    ts = datetime(2026, 7, 8, 14, 30, tzinfo=timezone.utc)  # ~10:30 ET
    return MarketSnapshot(underlying="SPY", timestamp=ts, spot=spot)


def _bot(params: Optional[dict] = None) -> BaseBot:
    spec = BotSpec(
        id="unit_test_bot",
        display_name="Unit Test Bot",
        strategy_class="BaseBot",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params=params or {},
    )
    return BaseBot(spec)


def _armed_pos(
    direction: str = "bullish",
    *,
    entry_spot: float = ENTRY_SPOT,
    target_price: float = WALL,
    t1: Optional[float] = T1,
    s2: Optional[float] = S2,
    t2: Optional[float] = T2,
    scale_stage: int = 0,
    quantity_open: int = 8,
    quantity_initial: int = 8,
    entry_price: float = 1.00,
    current_price: float = 1.50,
    high_water_mark: Optional[float] = 1.50,
    stop_price: Optional[float] = None,
    realized_pnl_booked: float = 0.0,
    exit_tranches: Optional[List[dict]] = None,
    min_hold_seconds_ago: float = 120.0,
    time_stop_seconds_ahead: float = 3600.0,
) -> OpenPosition:
    now = datetime.now(timezone.utc)
    return OpenPosition(
        id=1,
        bot_id="unit_test_bot",
        underlying="SPY",
        opened_at=now - timedelta(minutes=5),
        direction=direction,
        strategy_type="BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT",
        legs=[{"option_symbol": "SPY 260101C750", "side": "long",
               "option_type": "call", "strike": 750, "expiration": "2026-01-01"}],
        entry_price=entry_price,
        current_price=current_price,
        quantity_open=quantity_open,
        unrealized_pnl=0.0,
        stop_price=stop_price,
        target_price=target_price,
        time_stop_at=now + timedelta(seconds=time_stop_seconds_ahead),
        min_hold_until=now - timedelta(seconds=min_hold_seconds_ago),
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.7,
        components_at_entry={},
        entry_spot=entry_spot,
        quantity_initial=quantity_initial,
        t1_trigger_price=t1,
        s2_stop_price=s2,
        target2_price=t2,
        scale_stage=scale_stage,
        high_water_mark=high_water_mark,
        realized_pnl_booked=realized_pnl_booked,
        exit_tranches=exit_tranches or [],
    )


# ── compute_ladder_levels (pure) ──────────────────────────────────────


def test_ladder_levels_bullish():
    lv = compute_ladder_levels(749.0, 750.0, "bullish", 0.90, 0.75, 1.5)
    assert lv == {"t1_trigger_price": pytest.approx(749.90),
                  "s2_stop_price": pytest.approx(749.75),
                  "target2_price": pytest.approx(750.50)}


def test_ladder_levels_bearish_mirror():
    lv = compute_ladder_levels(749.0, 748.0, "bearish", 0.90, 0.75, 1.5)
    # R = -1: every level is BELOW entry, and S2 sits above T1 (closer to entry).
    assert lv["t1_trigger_price"] == pytest.approx(748.10)
    assert lv["s2_stop_price"] == pytest.approx(748.25)
    assert lv["target2_price"] == pytest.approx(747.50)


@pytest.mark.parametrize(
    "entry_spot, target, direction",
    [
        (749.0, None, "bullish"),      # no target
        (None, 750.0, "bullish"),      # no entry spot
        (749.0, 748.0, "bullish"),     # bullish but target below entry (wrong sign)
        (749.0, 750.0, "bearish"),     # bearish but target above entry (wrong sign)
        (749.0, 750.0, "neutral"),     # non-directional
    ],
)
def test_ladder_levels_unarmable_returns_none(entry_spot, target, direction):
    assert compute_ladder_levels(entry_spot, target, direction, 0.90, 0.75, 1.5) is None


# ── T1 take (Stage 0) ─────────────────────────────────────────────────


def test_t1_fires_a_cut_before_the_wall():
    # Spot reaches 0.90 R (749.90) — BEFORE the structural wall (750).
    d = _bot().exit_criteria(_snap(749.90), _armed_pos())
    assert d.should_cut is True
    assert d.should_close is False
    assert d.reason == "scale_t1"
    assert d.cut_fraction == pytest.approx(0.5)


def test_t1_does_not_fire_below_trigger():
    d = _bot().exit_criteria(_snap(749.85), _armed_pos())
    assert d.should_cut is False
    assert d.should_close is False


def test_wall_does_not_full_close_when_armed():
    # At the wall (750) the armed position is already past T1 territory but must
    # NOT do the old single-target FULL close — it takes the T1 tranche instead.
    d = _bot().exit_criteria(_snap(750.00), _armed_pos())
    assert d.should_close is False
    assert d.should_cut is True
    assert d.reason == "scale_t1"


def test_t1_gated_by_min_hold():
    pos = _armed_pos(min_hold_seconds_ago=-30.0)  # min_hold still in the future
    d = _bot().exit_criteria(_snap(749.95), pos)
    assert d.should_cut is False
    assert d.should_close is False


def test_bearish_t1_fires_on_the_way_down():
    pos = _armed_pos("bearish", entry_spot=749.0, target_price=748.0,
                     t1=748.10, s2=748.25, t2=747.50)
    d = _bot().exit_criteria(_snap(748.10), pos)
    assert d.should_cut is True
    assert d.reason == "scale_t1"


# ── T2 take (Stage 1) ─────────────────────────────────────────────────


def test_t2_fires_a_cut_in_stage1():
    d = _bot().exit_criteria(_snap(750.50), _armed_pos(scale_stage=1, quantity_open=4))
    assert d.should_cut is True
    assert d.reason == "scale_t2"
    assert d.cut_fraction == pytest.approx(0.5)


def test_no_t2_in_stage0():
    # Below T1, in Stage 0, spot at T2 shouldn't happen — but even if it does,
    # Stage 0 only knows T1. (Spot 750.50 >= T1 so this is a T1 cut.)
    d = _bot().exit_criteria(_snap(750.50), _armed_pos(scale_stage=0))
    assert d.reason == "scale_t1"


# ── Runner stops (Stage 1 / 2) ────────────────────────────────────────


def test_runner_s2_floor_closes_remaining():
    # Spot falls back to the S2 floor; give-back not triggered (mark healthy).
    pos = _armed_pos(scale_stage=1, quantity_open=4, current_price=1.45,
                     high_water_mark=1.50)
    d = _bot().exit_criteria(_snap(749.75), pos)
    assert d.should_close is True
    assert d.reason == "runner_s2_stop"


def test_runner_trailing_giveback_closes_remaining():
    # Mark gives back >= 30% from the 2.00 high-water mark → exit at/under 1.40.
    pos = _armed_pos(scale_stage=1, quantity_open=4, current_price=1.39,
                     high_water_mark=2.00)
    d = _bot().exit_criteria(_snap(749.80), pos)  # spot above S2, so trail wins
    assert d.should_close is True
    assert d.reason == "runner_trail_stop"


def test_runner_holds_when_neither_stop_hit():
    pos = _armed_pos(scale_stage=1, quantity_open=4, current_price=1.60,
                     high_water_mark=1.70)
    d = _bot().exit_criteria(_snap(750.10), pos)  # above S2, below T2, mark healthy
    assert d.should_close is False
    assert d.should_cut is False


def test_stage2_rides_to_time_stop():
    pos = _armed_pos(scale_stage=2, quantity_open=2, current_price=1.90,
                     high_water_mark=2.00, time_stop_seconds_ahead=-5.0)
    d = _bot().exit_criteria(_snap(750.40), pos)
    assert d.should_close is True
    assert d.reason == "time_stop"


# ── Interactions with the existing stop stack ─────────────────────────


def test_premium_stop_still_fires_when_armed():
    # Catastrophic floor beats the ladder: 25% premium loss closes everything.
    pos = _armed_pos(scale_stage=1, quantity_open=4, current_price=0.70)
    d = _bot().exit_criteria(_snap(750.20), pos)
    assert d.should_close is True
    assert d.reason == "premium_stop"


def test_disabled_ladder_falls_back_to_single_target():
    # scale_out_enabled=False: even with geometry present, the ladder no-ops and
    # the classic single-target full close governs (reason 'target' at the wall).
    pos = _armed_pos()
    d = _bot(params={"scale_out_enabled": False}).exit_criteria(_snap(750.10), pos)
    assert d.should_close is True
    assert d.reason == "target"


def test_unarmed_position_uses_single_target():
    # No frozen geometry (t1/s2/t2 None) => today's behavior, full close at target.
    pos = _armed_pos(t1=None, s2=None, t2=None)
    d = _bot().exit_criteria(_snap(750.10), pos)
    assert d.should_close is True
    assert d.reason == "target"


# ── partial_close_position: books a tranche, no trade row / capital move ──


class _FakeCursor:
    def __init__(self):
        self.captured = []

    def execute(self, sql, params=None):
        self.captured.append((sql, params))

    def fetchone(self):
        return (42,)


class _FakeConn:
    def __init__(self):
        self._cursor = _FakeCursor()

    def cursor(self):
        return self._cursor


def _sql_blob(conn) -> str:
    return "\n".join(sql for sql, _ in conn.cursor().captured)


def test_partial_close_books_tranche_without_trade_or_capital():
    pos = _armed_pos(scale_stage=0, quantity_open=8, quantity_initial=8, entry_price=1.00)
    conn = _FakeConn()
    with patch("src.tradeworkz.reconciler.spread_price", return_value=1.50), patch(
        "src.tradeworkz.reconciler.notifications.fanout_event", return_value=0
    ):
        partial_close_position(conn, pos, ExitDecision(
            should_close=False, should_cut=True, cut_fraction=0.5, reason="scale_t1"))

    # Books onto the position: 50% of 8 = 4 contracts sold at 1.50.
    assert pos.quantity_open == 4
    assert pos.scale_stage == 1
    assert pos.realized_pnl_booked == pytest.approx((1.50 - 1.00) * 4 * 100)  # 200
    assert len(pos.exit_tranches) == 1
    # Critically: NO trade row and NO capital move on a partial.
    blob = _sql_blob(conn)
    assert "INSERT INTO tw_trades" not in blob
    assert "tw_bot_capital" not in blob
    assert "UPDATE tw_positions" in blob


def test_partial_close_t2_takes_half_of_remainder():
    pos = _armed_pos(scale_stage=1, quantity_open=4, quantity_initial=8,
                     entry_price=1.00, realized_pnl_booked=200.0,
                     exit_tranches=[{"stage": 1, "qty": 4}])
    conn = _FakeConn()
    with patch("src.tradeworkz.reconciler.spread_price", return_value=2.00), patch(
        "src.tradeworkz.reconciler.notifications.fanout_event", return_value=0
    ):
        partial_close_position(conn, pos, ExitDecision(
            should_close=False, should_cut=True, cut_fraction=0.5, reason="scale_t2"))
    assert pos.quantity_open == 2          # floor(0.5 * 4)
    assert pos.scale_stage == 2
    assert pos.realized_pnl_booked == pytest.approx(200.0 + (2.00 - 1.00) * 2 * 100)  # 400


def test_partial_close_always_leaves_a_runner():
    # cut_fraction 1.0 on 4 open must still leave >= 1 contract.
    pos = _armed_pos(scale_stage=0, quantity_open=4, quantity_initial=4)
    conn = _FakeConn()
    with patch("src.tradeworkz.reconciler.spread_price", return_value=1.50), patch(
        "src.tradeworkz.reconciler.notifications.fanout_event", return_value=0
    ):
        partial_close_position(conn, pos, ExitDecision(
            should_close=False, should_cut=True, cut_fraction=1.0, reason="scale_t1"))
    assert pos.quantity_open == 1


# ── close_position: one consolidated, arithmetically-honest trade row ──


def _patched_close(conn, pos, reason):
    with patch("src.tradeworkz.reconciler.spread_price", return_value=1.90), patch(
        "src.tradeworkz.reconciler.notifications.fanout_event", return_value=0
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.load_state", return_value=MagicMock(),
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.online_update", side_effect=lambda s, *a, **k: s
    ) as online, patch(
        "src.tradeworkz.reconciler.ml_mod.save_state", return_value=None
    ):
        close_position(conn, pos, reason=reason)
    return online


def _trade_insert(conn):
    return next(c for c in conn.cursor().captured if "INSERT INTO tw_trades" in c[0])


def test_consolidated_close_is_size_weighted_and_invariant_a_holds():
    # Doc scenario A: T1 4@1.50 (+200), T2 2@2.00 (+200) already booked (400),
    # final runner 2@1.90 (+180). Total realized 580 on the ORIGINAL 8.
    pos = _armed_pos(
        scale_stage=2, quantity_open=2, quantity_initial=8, entry_price=1.00,
        realized_pnl_booked=400.0,
        exit_tranches=[{"stage": 1, "qty": 4, "price": 1.50, "realized": 200.0},
                       {"stage": 2, "qty": 2, "price": 2.00, "realized": 200.0}],
    )
    conn = _FakeConn()
    _patched_close(conn, pos, "runner_trail_stop")

    _, params = _trade_insert(conn)
    entry_price, exit_price, quantity, realized = params[7], params[8], params[9], params[10]
    assert quantity == 8                                   # ORIGINAL size, not the runner's 2
    assert realized == pytest.approx(580.0)               # 400 booked + 180 final
    assert exit_price == pytest.approx(1.725)             # size-weighted VWAP
    # Invariant A: realized == (exit - entry) * quantity * 100, by construction.
    assert (exit_price - entry_price) * quantity * 100 == pytest.approx(realized)


def test_consolidated_close_credits_capital_once_with_total():
    pos = _armed_pos(scale_stage=2, quantity_open=2, quantity_initial=8,
                     entry_price=1.00, realized_pnl_booked=400.0,
                     exit_tranches=[{"qty": 4}, {"qty": 2}])
    conn = _FakeConn()
    _patched_close(conn, pos, "runner_trail_stop")
    cap = next(c for c in conn.cursor().captured if "tw_bot_capital" in c[0])
    # UPDATE ... current_capital + %s, peak GREATEST(..., current + %s) -> total realized.
    assert cap[1][0] == pytest.approx(580.0)


def test_consolidated_close_is_one_ml_sample():
    pos = _armed_pos(scale_stage=2, quantity_open=2, quantity_initial=8,
                     entry_price=1.00, realized_pnl_booked=400.0,
                     exit_tranches=[{"qty": 4}, {"qty": 2}])
    conn = _FakeConn()
    online = _patched_close(conn, pos, "runner_trail_stop")
    assert online.call_count == 1                          # scaling != 3 samples
    # won == (total realized > 0)
    assert online.call_args.kwargs.get("won") is True


def test_unarmed_close_matches_legacy_single_exit():
    # quantity_initial None, no booked realized, no tranches -> exit_price is the
    # raw fill and quantity is the open size (byte-for-byte legacy behavior).
    pos = _armed_pos(t1=None, s2=None, t2=None, quantity_initial=None,
                     quantity_open=10, entry_price=1.00)
    conn = _FakeConn()
    _patched_close(conn, pos, "target")
    _, params = _trade_insert(conn)
    exit_price, quantity, realized = params[8], params[9], params[10]
    assert quantity == 10
    assert exit_price == pytest.approx(1.90)              # the raw final fill
    assert realized == pytest.approx((1.90 - 1.00) * 10 * 100)  # 900
