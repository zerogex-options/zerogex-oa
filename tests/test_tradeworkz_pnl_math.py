"""Regression: long-debit P&L must not be sign-flipped by direction.

Every TradeWorkz bot as of this test's authoring trades LONG option debits
(``side='long'`` on every leg, ``strategy_type in {BUY_CALL_DEBIT,
BUY_PUT_DEBIT}``). For a long option, the per-share P&L is
``exit_price − entry_price``, regardless of whether the bot's directional
thesis was ``'bullish'`` or ``'bearish'``: if you buy a put at 1.80 and
sell it at 2.20, you made $0.40/share — the fact that a "put" is
directionally bearish is a property of the trade thesis, not of the
P&L math.

An earlier version of ``reconciler.close_position`` / ``mark_position``
negated the P&L when ``direction == 'bearish'``. That flipped every real
winner into a loss and every real loser into a win: the live sleeve on
``dealer_delta_pressure_rider`` ballooned from $111k to $4.4M in a bull
market because its bearish put buys were expiring worthless (real
losses) and being booked as wins. This test locks the fix in.

If a future strategy adds true SHORT legs, the correct thing to gate on
is the per-leg ``side`` field, not ``direction``. A test covering that
should live alongside this one, not replace it.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.tradeworkz.models import OpenPosition
from src.tradeworkz.reconciler import close_position, mark_position


def _pos(direction: str, entry: float = 1.80, qty: int = 10) -> OpenPosition:
    return OpenPosition(
        id=1,
        bot_id="test_bot",
        underlying="SPY",
        opened_at=datetime.now(timezone.utc),
        direction=direction,
        strategy_type=(
            "BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT"
        ),
        legs=[
            {
                "option_symbol": "SPY 260101P600",
                "side": "long",
                "option_type": "put" if direction == "bearish" else "call",
                "strike": 600,
                "expiration": "2026-01-01",
            }
        ],
        entry_price=entry,
        current_price=entry,
        quantity_open=qty,
        unrealized_pnl=0.0,
        stop_price=None,
        target_price=None,
        time_stop_at=None,
        min_hold_until=None,
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=0.7,
        components_at_entry={},
    )


class _FakeCursor:
    def __init__(self):
        self.captured = []

    def execute(self, sql, params=None):
        self.captured.append((sql, params))
        return None

    def fetchone(self):
        # close_position expects RETURNING id from tw_trades insert
        return (42,)


class _FakeConn:
    def __init__(self):
        self._cursor = _FakeCursor()

    def cursor(self):
        return self._cursor


@pytest.mark.parametrize(
    "direction, entry, exit_price, qty, expected_pnl",
    [
        # Bullish long call that appreciates 40c → $0.40 × 10 × 100 = +$400
        ("bullish", 1.80, 2.20, 10, 400.0),
        # Bullish long call that decays 80c → -$800
        ("bullish", 1.80, 1.00, 10, -800.0),
        # BEARISH long put that appreciates 40c → +$400 (was -$400 pre-fix)
        ("bearish", 1.80, 2.20, 10, 400.0),
        # BEARISH long put that decays 80c → -$800 (was +$800 pre-fix)
        # This is the case that let the fleet balloon in a bull market.
        ("bearish", 1.80, 1.00, 10, -800.0),
    ],
)
def test_close_position_pnl_no_sign_flip(direction, entry, exit_price, qty, expected_pnl):
    pos = _pos(direction, entry=entry, qty=qty)
    conn = _FakeConn()

    with patch(
        "src.tradeworkz.reconciler.spread_price", return_value=exit_price
    ), patch(
        "src.tradeworkz.reconciler.notifications.fanout_event", return_value=0
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.load_state", return_value=MagicMock(
            n_samples=0, n_wins=0, hit_rate=None, confidence_base=0.55,
            confidence_threshold=0.55, size_multiplier=1.0, weights={},
            feature_mean={}, feature_var={}, last_win_rate_7d=None,
            last_win_rate_30d=None, last_profit_factor=None,
        )
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.online_update", side_effect=lambda s, *_a, **_k: s
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.save_state", return_value=None
    ):
        close_position(conn, pos, reason="target")

    # Pull the tw_trades INSERT out of the captured cursor calls.
    trades_insert = next(
        c for c in conn.cursor().captured if "INSERT INTO tw_trades" in c[0]
    )
    params = trades_insert[1]
    # positional index of realized_pnl in the INSERT is 10
    realized = params[10]
    assert realized == pytest.approx(expected_pnl), (
        f"{direction} entry={entry} exit={exit_price} qty={qty}: "
        f"expected {expected_pnl}, got {realized}"
    )


def test_mark_position_pnl_no_sign_flip():
    """Unrealized P&L must not flip sign for bearish long positions either."""
    pos = _pos("bearish", entry=1.80, qty=10)
    conn = _FakeConn()

    with patch("src.tradeworkz.reconciler.spread_price", return_value=2.20):
        mark_position(conn, pos)

    # (2.20 - 1.80) × 10 × 100 = 400. Not −400.
    assert pos.unrealized_pnl == pytest.approx(400.0)
