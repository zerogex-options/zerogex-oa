"""Position lifecycle — open, mark, close.

The reconciler wraps the DB writes so bots stay pure. Every write happens
inside a single transaction with a per-bot advisory lock to prevent double
opens if two engine ticks race.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.tradeworkz import config as tw_config
from src.tradeworkz import ml as ml_mod
from src.tradeworkz import notifications
from src.tradeworkz.models import BotCapital, OpenPosition, TradeSignal
from src.tradeworkz.pricing import spread_price
from src.tradeworkz.sizing import compute_contracts

logger = logging.getLogger(__name__)


def _bot_lock_id(bot_id: str) -> int:
    """Stable 32-bit advisory-lock id for a bot."""
    return int(hashlib.blake2b(bot_id.encode(), digest_size=4).hexdigest(), 16)


def _acquire_lock(conn: Any, bot_id: str) -> bool:
    if not tw_config.RECONCILE_LOCK_ENABLED:
        return True
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (_bot_lock_id(bot_id),))
    row = cur.fetchone()
    return bool(row[0]) if row else False


def load_capital(conn: Any, bot_id: str) -> Optional[BotCapital]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bot_id, starting_capital, current_capital, peak_capital,
               max_heat_pct, kelly_fraction, daily_kill_pct
        FROM tw_bot_capital WHERE bot_id = %s
        """,
        (bot_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return BotCapital(
        bot_id=row[0],
        starting_capital=float(row[1]),
        current_capital=float(row[2]),
        peak_capital=float(row[3]),
        max_heat_pct=float(row[4]),
        kelly_fraction=float(row[5]),
        daily_kill_pct=float(row[6]),
    )


def load_open_positions(conn: Any, bot_id: str) -> List[OpenPosition]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, bot_id, underlying, opened_at, direction, strategy_type,
               legs, entry_price, current_price, quantity_open,
               unrealized_pnl, stop_price, target_price, time_stop_at,
               min_hold_until, wall_ref_price, wall_ref_side,
               entry_conviction, components_at_entry
        FROM tw_positions WHERE bot_id = %s
        """,
        (bot_id,),
    )
    result: List[OpenPosition] = []
    for r in cur.fetchall():
        legs = r[6] or []
        if isinstance(legs, str):
            legs = json.loads(legs)
        comp = r[18] or {}
        if isinstance(comp, str):
            comp = json.loads(comp)
        result.append(
            OpenPosition(
                id=r[0],
                bot_id=r[1],
                underlying=r[2],
                opened_at=r[3],
                direction=r[4],
                strategy_type=r[5],
                legs=list(legs),
                entry_price=float(r[7]),
                current_price=float(r[8] or 0.0),
                quantity_open=int(r[9]),
                unrealized_pnl=float(r[10] or 0.0),
                stop_price=float(r[11]) if r[11] is not None else None,
                target_price=float(r[12]) if r[12] is not None else None,
                time_stop_at=r[13],
                min_hold_until=r[14],
                wall_ref_price=float(r[15]) if r[15] is not None else None,
                wall_ref_side=r[16],
                entry_conviction=float(r[17]) if r[17] is not None else None,
                components_at_entry=dict(comp),
            )
        )
    return result


def open_position(
    conn: Any,
    signal: TradeSignal,
    capital: BotCapital,
    size_multiplier: float,
    wall_strength: Optional[float] = None,
    daily_realized_pnl: float = 0.0,
) -> Optional[int]:
    """Insert one row into ``tw_positions``. Returns the new position id.

    Skips if there is already an open position for the same bot / underlying
    within :data:`tw_config.ENTRY_DEDUPE_WINDOW_SECONDS`.
    """
    if not _acquire_lock(conn, signal.bot_id):
        logger.debug("skipping open: could not acquire lock for %s", signal.bot_id)
        return None

    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(1) FROM tw_positions
        WHERE bot_id = %s AND underlying = %s
          AND opened_at >= NOW() - (%s || ' seconds')::interval
        """,
        (signal.bot_id, signal.underlying, str(tw_config.ENTRY_DEDUPE_WINDOW_SECONDS)),
    )
    dupe = cur.fetchone()
    if dupe and dupe[0]:
        return None
    cur.execute(
        "SELECT COUNT(1) FROM tw_positions WHERE bot_id = %s AND underlying = %s",
        (signal.bot_id, signal.underlying),
    )
    open_count = cur.fetchone()
    if open_count and open_count[0]:
        # Only one open position per bot / underlying pair at a time.
        return None

    legs_dicts = [asdict(l) for l in signal.legs]
    entry_price = spread_price(conn, legs_dicts, action="open")
    if entry_price is None or entry_price <= 0:
        logger.debug("no fillable quote for bot=%s legs=%s", signal.bot_id, legs_dicts)
        return None

    contracts = compute_contracts(
        capital=capital,
        entry_price=entry_price,
        conviction=signal.conviction,
        size_multiplier=size_multiplier,
        wall_strength=wall_strength,
        daily_realized_pnl=daily_realized_pnl,
    )
    if contracts <= 0:
        return None

    now = datetime.now(timezone.utc)
    min_hold_until = now + timedelta(seconds=tw_config.MIN_HOLD_SECONDS)
    payload = dict(signal.components_at_entry)
    payload.update(
        {
            "conviction": signal.conviction,
            "rationale": signal.rationale,
            "size_multiplier": size_multiplier,
        }
    )
    cur.execute(
        """
        INSERT INTO tw_positions (
            bot_id, underlying, opened_at, updated_at, direction,
            strategy_type, legs, entry_price, current_price, quantity_open,
            unrealized_pnl, stop_price, target_price, time_stop_at,
            min_hold_until, wall_ref_price, wall_ref_side, entry_conviction,
            components_at_entry
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, 0,
            %s, %s, %s, %s, %s, %s, %s, %s::jsonb
        ) RETURNING id
        """,
        (
            signal.bot_id,
            signal.underlying,
            now,
            now,
            signal.direction,
            signal.strategy_type,
            json.dumps(legs_dicts),
            entry_price,
            entry_price,
            contracts,
            signal.stop_price,
            signal.target_price,
            signal.time_stop_at,
            min_hold_until,
            signal.wall_ref_price,
            signal.wall_ref_side,
            signal.conviction,
            json.dumps(payload),
        ),
    )
    row = cur.fetchone()
    position_id = int(row[0]) if row else None
    if position_id is not None:
        notifications.fanout_event(
            conn,
            bot_id=signal.bot_id,
            event_type="entry",
            payload={
                "underlying": signal.underlying,
                "direction": signal.direction,
                "strategy_type": signal.strategy_type,
                "conviction": signal.conviction,
                "entry_price": entry_price,
                "contracts": contracts,
                "target_price": signal.target_price,
                "stop_price": signal.stop_price,
                "rationale": signal.rationale,
            },
            position_id=position_id,
        )
    return position_id


def mark_position(conn: Any, pos: OpenPosition) -> Optional[float]:
    """Mark-to-market ``pos``. Returns updated per-share liquidation value.

    Every strategy in this engine trades LONG debits (BUY_CALL_DEBIT /
    BUY_PUT_DEBIT — see the bot classes; every leg is ``side='long'``).
    For a long option position the P&L is simply
    ``(exit − entry) × contracts × 100``, regardless of whether the bot's
    thesis is bullish or bearish: a put you bought at 1.80 and sold at
    2.20 gained $0.40/share whether the intended market view was up or
    down. The earlier code negated the P&L when ``direction == 'bearish'``,
    which flipped every real winner into a "loss" and every real loser
    into a "win" — the reason the leaderboard showed bearish bots
    running the sleeve up from $111k to $4.4M in a bull market: their
    real losses were being booked as wins. The negation is gone.

    If a strategy ever adds true SHORT legs, this needs to key off the
    per-leg ``side`` field, not ``direction``.
    """
    mark = spread_price(conn, pos.legs, action="close")
    if mark is None:
        return None
    upnl = (mark - pos.entry_price) * pos.quantity_open * 100.0
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE tw_positions
        SET current_price = %s, unrealized_pnl = %s, updated_at = NOW()
        WHERE id = %s
        """,
        (mark, upnl, pos.id),
    )
    pos.current_price = mark
    pos.unrealized_pnl = upnl
    return mark


def close_position(
    conn: Any, pos: OpenPosition, reason: str
) -> Optional[int]:
    """Close ``pos``, write an immutable tw_trades row, update capital,
    fan out notifications, and update the bot's ML state.

    Returns the ``tw_trades`` row id.
    """
    exit_price = spread_price(conn, pos.legs, action="close")
    if exit_price is None:
        return None
    # Long debit position — P&L is (exit − entry) × qty × 100 regardless
    # of directional thesis. See mark_position for the full rationale on
    # why the bearish negation was wrong.
    signed_pnl_per_contract = (exit_price - pos.entry_price) * 100.0
    realized = signed_pnl_per_contract * pos.quantity_open
    pnl_pct = (
        (exit_price / pos.entry_price - 1.0) if pos.entry_price > 0 else 0.0
    )

    if realized > 0:
        outcome = "win"
    elif realized < 0:
        outcome = "loss"
    else:
        outcome = "scratch"
    now = datetime.now(timezone.utc)

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tw_trades (
            bot_id, underlying, opened_at, closed_at, direction,
            strategy_type, legs, entry_price, exit_price, quantity,
            realized_pnl, pnl_percent, outcome, close_reason,
            entry_conviction, components_at_entry, components_at_exit
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s,
            %s, %s::jsonb, %s::jsonb
        ) RETURNING id
        """,
        (
            pos.bot_id,
            pos.underlying,
            pos.opened_at,
            now,
            pos.direction,
            pos.strategy_type,
            json.dumps(pos.legs),
            pos.entry_price,
            exit_price,
            pos.quantity_open,
            realized,
            pnl_pct,
            outcome,
            reason,
            pos.entry_conviction,
            json.dumps(pos.components_at_entry),
            json.dumps({"exit_reason": reason, "current_price": exit_price}),
        ),
    )
    row = cur.fetchone()
    trade_id = int(row[0]) if row else None

    cur.execute("DELETE FROM tw_positions WHERE id = %s", (pos.id,))
    cur.execute(
        """
        UPDATE tw_bot_capital
        SET current_capital = current_capital + %s,
            peak_capital = GREATEST(peak_capital, current_capital + %s),
            updated_at = NOW()
        WHERE bot_id = %s
        """,
        (realized, realized, pos.bot_id),
    )

    state = ml_mod.load_state(conn, pos.bot_id)
    components = dict(pos.components_at_entry)
    components["conviction"] = pos.entry_conviction or components.get("conviction")
    state = ml_mod.online_update(state, components, won=(outcome == "win"))
    ml_mod.save_state(conn, state)

    notifications.fanout_event(
        conn,
        bot_id=pos.bot_id,
        event_type="exit",
        payload={
            "underlying": pos.underlying,
            "direction": pos.direction,
            "outcome": outcome,
            "realized_pnl": realized,
            "pnl_percent": pnl_pct,
            "reason": reason,
            "contracts": pos.quantity_open,
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
        },
        trade_id=trade_id,
        position_id=pos.id,
    )
    return trade_id


def daily_realized_pnl(conn: Any, bot_id: str) -> float:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(realized_pnl), 0)
        FROM tw_trades
        WHERE bot_id = %s AND closed_at::date = CURRENT_DATE
        """,
        (bot_id,),
    )
    row = cur.fetchone()
    return float(row[0]) if row else 0.0
