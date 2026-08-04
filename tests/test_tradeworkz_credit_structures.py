"""Defined-risk credit structures (the iron condor) size and book correctly.

Before: the condor entered at a near-zero net, sized on that (~34x over), and
its P&L was inverted (a decaying condor - a WIN - booked as a loss because it
entered at a debit). Now: it must collect a real net credit, size on the wing
width, and book a decay to 0 as a profit. entry_price carries the sign, so the
(exit - entry) * qty * 100 formula is correct for a credit too.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.tradeworkz.models import BotCapital, OpenPosition
from src.tradeworkz.reconciler import close_position
from src.tradeworkz.sizing import compute_contracts, structure_max_loss_per_share

from datetime import datetime, timezone

# Short iron condor: near-OTM shorts, far-OTM long wings ($2 wings).
_CONDOR = [
    {"option_symbol": "SPY 260101P742", "side": "long", "option_type": "put", "strike": 742},
    {"option_symbol": "SPY 260101P744", "side": "short", "option_type": "put", "strike": 744},
    {"option_symbol": "SPY 260101C747", "side": "short", "option_type": "call", "strike": 747},
    {"option_symbol": "SPY 260101C749", "side": "long", "option_type": "call", "strike": 749},
]


def _cap() -> BotCapital:
    return BotCapital(
        bot_id="rc", starting_capital=140_000, current_capital=140_000,
        peak_capital=140_000, max_heat_pct=0.06, kelly_fraction=0.5, daily_kill_pct=0.02,
    )


# ── sizing ────────────────────────────────────────────────────────────


def test_credit_condor_max_loss_is_wing_minus_credit():
    # entered for a 0.50 credit -> entry_price = -0.50; max loss = 2 - 0.50.
    assert structure_max_loss_per_share(_CONDOR, -0.50) == pytest.approx(1.50)


def test_credit_condor_sizes_on_max_loss_bounded_by_heat():
    cap = _cap()
    n = compute_contracts(
        capital=cap, entry_price=-0.50, conviction=0.6, size_multiplier=1.0,
        max_loss_per_share=structure_max_loss_per_share(_CONDOR, -0.50),
    )
    assert n > 0
    # Real risk (contracts * max_loss * 100) is bounded by the heat budget, not
    # 34x it as when sizing keyed on the ~$0 net.
    assert n * 1.50 * 100 <= cap.current_capital * cap.max_heat_pct * 1.3


def test_bad_long_debit_quote_still_returns_zero():
    # No short legs + non-positive entry with no max_loss => bad quote => 0.
    assert compute_contracts(
        capital=_cap(), entry_price=0.0, conviction=0.6, size_multiplier=1.0) == 0


# ── close_position P&L (credit) ───────────────────────────────────────


class _FakeCursor:
    def __init__(self):
        self.captured = []

    def execute(self, sql, params=None):
        self.captured.append((sql, params))

    def fetchone(self):
        return (7,)


class _FakeConn:
    def __init__(self):
        self._cursor = _FakeCursor()

    def cursor(self):
        return self._cursor


def _credit_pos(entry: float = -0.50, qty: int = 48) -> OpenPosition:
    now = datetime.now(timezone.utc)
    return OpenPosition(
        id=1, bot_id="rc", underlying="SPY", opened_at=now, direction="neutral",
        strategy_type="SHORT_IRON_CONDOR", legs=_CONDOR, entry_price=entry,
        current_price=entry, quantity_open=qty, unrealized_pnl=0.0, stop_price=744.0,
        target_price=743.0, time_stop_at=now, min_hold_until=now,
        wall_ref_price=None, wall_ref_side=None, entry_conviction=0.6, components_at_entry={},
    )


def _close(conn, pos, close_mark, reason="time_stop"):
    with patch("src.tradeworkz.reconciler.spread_price", return_value=close_mark), patch(
        "src.tradeworkz.reconciler.notifications.fanout_event", return_value=0
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.load_state", return_value=MagicMock()
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.online_update", side_effect=lambda s, *a, **k: s
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.save_state", return_value=None
    ):
        close_position(conn, pos, reason=reason)


def _trade_params(conn):
    return next(c for c in conn.cursor().captured if "INSERT INTO tw_trades" in c[0])[1]


def test_decaying_credit_condor_books_a_profit():
    # Held in-range: the structure decays to a 0.00 close mark. Keeping the
    # 0.50 credit on 48 contracts is a +$2,400 WIN (was booked as a loss before).
    conn = _FakeConn()
    _close(conn, _credit_pos(entry=-0.50, qty=48), close_mark=0.0)
    p = _trade_params(conn)
    entry, exit_price, quantity, realized, pnl_pct, outcome = p[7], p[8], p[9], p[10], p[11], p[12]
    assert realized == pytest.approx((0.0 - (-0.50)) * 48 * 100)  # +2400
    assert realized > 0 and outcome == "win"
    assert pnl_pct > 0  # sign-general % (was 0.0/inverted for credits)
    # Invariant A holds for the credit structure.
    assert (exit_price - entry) * quantity * 100 == pytest.approx(realized)


def test_breached_credit_condor_capped_at_max_loss():
    # Spot through a short strike: close mark -1.50 => max loss (wing - credit).
    conn = _FakeConn()
    _close(conn, _credit_pos(entry=-0.50, qty=48), close_mark=-1.50)
    p = _trade_params(conn)
    realized, outcome = p[10], p[12]
    assert realized == pytest.approx((-1.50 - (-0.50)) * 48 * 100)  # -4800
    assert realized < 0 and outcome == "loss"
