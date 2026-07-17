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
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.tradeworkz import config as tw_config
from src.tradeworkz import ml as ml_mod
from src.tradeworkz import notifications
from src.tradeworkz.models import BotCapital, OpenPosition, TradeSignal
from src.tradeworkz.pricing import spread_price
from src.tradeworkz.sizing import compute_contracts

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")

# 15:55 ET — 5 minutes before the 4 pm regular-session equity close, and
# the last moment we can reliably get a filling option quote on the day
# a 0DTE expires. Every open-position time_stop_at is hard-capped at
# this instant on the earliest leg expiration so a 0DTE can never sit
# open past the point where the options market closes and the quote
# cache goes stale — see _cap_time_stop_at_expiration.
_EXPIRATION_CLOSE_HHMM = time(15, 55)


def _earliest_leg_expiration(legs: List[Dict[str, Any]]) -> Optional[date]:
    """Earliest leg expiration date across ``legs``, or ``None``.

    Structures currently trade single-leg debits, so the earliest is
    also the only expiration; the loop handles multi-leg spreads for
    completeness. Returns ``None`` if no leg carries a parseable ISO
    date so callers can no-op instead of guessing.
    """
    earliest: Optional[date] = None
    for leg in legs:
        exp_str = leg.get("expiration") or ""
        if not isinstance(exp_str, str) or len(exp_str) < 10:
            continue
        try:
            exp = date.fromisoformat(exp_str[:10])
        except ValueError:
            continue
        if earliest is None or exp < earliest:
            earliest = exp
    return earliest


def _expiration_close_cap_utc(expiration: date) -> datetime:
    """Return ``expiration`` at 15:55 ET converted to UTC.

    ``zoneinfo`` handles DST automatically so 15:55 ET is always the
    right wall-clock time in Eastern regardless of whether we're
    inside EDT or EST.
    """
    et_dt = datetime.combine(expiration, _EXPIRATION_CLOSE_HHMM, tzinfo=_ET)
    return et_dt.astimezone(timezone.utc)


def _cap_time_stop_at_expiration(
    time_stop_at: Optional[datetime], legs: List[Dict[str, Any]]
) -> Optional[datetime]:
    """Hard-cap ``time_stop_at`` at 15:55 ET on the earliest leg expiration.

    The bots each set their own max_hold_minutes (60-90 default), which
    is fine for a swing bot but nonsense for a 0DTE: an entry at 15:20 ET
    with max_hold_minutes=60 would put time_stop_at at 16:20 ET, 20
    minutes AFTER the options market closes for that expiration. The
    reconciler would then try to close the position on an option that
    no longer trades, spread_price would return None (quote too stale),
    and the position would sit "open" for hours with a stale mark. This
    cap makes sure time_stop_at cannot fire after the last moment we
    can plausibly fill.

    A ``None`` earliest-expiration (no parseable leg dates) is a no-op:
    we can't cap what we can't see, and refusing the signal would be
    worse than trusting the bot's own time_stop_at.
    """
    exp = _earliest_leg_expiration(legs)
    if exp is None:
        return time_stop_at
    cap = _expiration_close_cap_utc(exp)
    if time_stop_at is None or time_stop_at > cap:
        return cap
    return time_stop_at


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

    legs_dicts = [asdict(leg) for leg in signal.legs]
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
    # Cap the bot's requested time_stop_at at 15:55 ET on the earliest
    # leg expiration so a 0DTE can never time-stop after the options
    # market shuts. See _cap_time_stop_at_expiration for rationale.
    capped_time_stop_at = _cap_time_stop_at_expiration(signal.time_stop_at, legs_dicts)
    payload = dict(signal.components_at_entry)
    payload.update(
        {
            "conviction": signal.conviction,
            "rationale": signal.rationale,
            "size_multiplier": size_multiplier,
            # Explicit provenance marker so the audit surface can
            # distinguish real engine-opened positions from seeder
            # rows without inferring from field absence. simulate.py
            # stamps "simulate" on its rows. Any row that predates this
            # commit will show as legacy-live (absent field → treated
            # as live) in the audit filter.
            "origin": "live",
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
            capped_time_stop_at,
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


def close_position(conn: Any, pos: OpenPosition, reason: str) -> Optional[int]:
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
    pnl_pct = (exit_price / pos.entry_price - 1.0) if pos.entry_price > 0 else 0.0

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
    # Defensive recompute of the rolling aggregates (hit rate -> confidence,
    # profit factor -> size multiplier, adaptive threshold) + the auto-disable
    # circuit breaker, right after each close. This is what keeps the ML
    # surface live between sweeps — without it recalibrate_bot never runs and
    # size_multiplier / profit_factor sit at their defaults forever.
    if tw_config.CALIBRATION_ENABLED:
        # Isolate the recompute in a SAVEPOINT: a calibration SQL failure
        # aborts the transaction, and without this the close's own commit
        # (and the fanout below) would roll back with it — losing the trade
        # record. The savepoint confines a failure to the recompute alone.
        gcur = conn.cursor()
        try:
            gcur.execute("SAVEPOINT tw_recalibrate")
            ml_mod.recalibrate_bot(conn, pos.bot_id)
            gcur.execute("RELEASE SAVEPOINT tw_recalibrate")
        except Exception:  # pragma: no cover - calibration must not break a close
            try:
                gcur.execute("ROLLBACK TO SAVEPOINT tw_recalibrate")
            except Exception:
                pass
            logger.warning("recalibrate_bot failed for %s after close", pos.bot_id, exc_info=True)

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
    # Sum realized P&L over the ET trading day (the daily-kill-switch basis),
    # NOT the UTC calendar day: a UTC boundary would reset the kill switch at
    # 20:00 ET mid-session and mis-attribute an after-hours close to the next
    # day. Bucket the close and "now" both in America/New_York so they agree.
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(realized_pnl), 0)
        FROM tw_trades
        WHERE bot_id = %s
          AND (closed_at AT TIME ZONE 'America/New_York')::date
              = (NOW() AT TIME ZONE 'America/New_York')::date
        """,
        (bot_id,),
    )
    row = cur.fetchone()
    return float(row[0]) if row else 0.0
