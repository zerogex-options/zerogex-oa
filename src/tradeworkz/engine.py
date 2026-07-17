"""TradeWorkz engine tick — one reconcile pass across the fleet.

Called on a schedule by whatever supervises the API (either a background
FastAPI task or an external systemd unit). Each tick:

1. Builds one :class:`MarketSnapshot` per underlying in play.
2. For every enabled bot: loads capital + open positions + ML state.
3. Runs the bot's ``exit_criteria`` against each open position first
   (never open a new one while an eligible close is on the table).
4. Runs the bot's ``open_criteria`` against the fresh snapshot.
5. Rolls up daily equity / metrics if the session date has advanced.

Everything happens inside a single :func:`db_connection` transaction per
bot so a mid-tick crash rolls back cleanly.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from src.database import db_connection
from src.tradeworkz import config as tw_config
from src.tradeworkz import ml as ml_mod
from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.context import MarketSnapshot, build_snapshot
from src.tradeworkz.models import BotSpec
from src.tradeworkz.registry import get_bot_class, DEFAULT_ROSTER, RETIRED_BOT_IDS
from src.tradeworkz.reconciler import (
    close_position,
    daily_realized_pnl,
    load_capital,
    load_open_positions,
    mark_position,
    open_position,
)

logger = logging.getLogger(__name__)


def tick() -> Dict[str, Any]:
    """Run one reconcile cycle across every enabled bot. Returns a summary."""
    if not tw_config.ENGINE_ENABLED:
        return {"status": "disabled"}

    summary: Dict[str, Any] = {
        "ticks": 0,
        "opened": 0,
        "closed": 0,
        "marked": 0,
        "errors": [],
    }
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, display_name, strategy_class, tier, direction_mode,
                   universe, tagline, description, is_public, enabled, params
            FROM tw_bots WHERE enabled = TRUE
            """)
        bots = cur.fetchall()

    fleet_universes = tw_config.fleet_universes()
    # A bot pinned to a single ticker keeps that pin; a bot with the
    # wildcard '*' explodes into the full fleet universe on every tick.
    # Build the union of every underlying we need a snapshot for.
    underlyings_needed: set[str] = set()
    for row in bots:
        underlyings_needed.update(_bot_underlyings(row[5], fleet_universes))
    if not underlyings_needed:
        underlyings_needed = set(fleet_universes) or {"SPY"}

    snapshots: Dict[str, Optional[MarketSnapshot]] = {}
    with db_connection() as conn:
        for u in sorted(underlyings_needed):
            snapshots[u] = build_snapshot(conn, u)

    for row in bots:
        summary["ticks"] += 1
        bot_id = row[0]
        try:
            per_bot = _run_bot(row, snapshots, fleet_universes)
            summary["opened"] += per_bot.get("opened", 0)
            summary["closed"] += per_bot.get("closed", 0)
            summary["marked"] += per_bot.get("marked", 0)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("TradeWorkz bot %s tick failed", bot_id)
            summary["errors"].append({"bot_id": bot_id, "error": str(exc)})

    with db_connection() as conn:
        rollup_daily(conn)
    return summary


def _bot_underlyings(universe: str, fleet: tuple[str, ...]) -> list[str]:
    """Resolve a bot's ``universe`` field to a concrete list of tickers.

    * ``'*'`` — the fleet universe (every ticker in ``TRADEWORKZ_UNIVERSE``).
    * comma-separated (e.g. ``'SPY,QQQ'``) — that explicit subset.
    * bare ticker (e.g. ``'SPY'``) — pinned to that ticker only.
    """
    if not universe:
        return list(fleet)
    if universe.strip() == "*":
        return list(fleet)
    return [sym.strip().upper() for sym in universe.split(",") if sym.strip()]


def _run_bot(
    row: tuple,
    snapshots: Dict[str, Optional[MarketSnapshot]],
    fleet_universes: tuple[str, ...],
) -> Dict[str, int]:
    (
        bot_id,
        display_name,
        strategy_class,
        tier,
        direction_mode,
        universe,
        tagline,
        description,
        is_public,
        enabled,
        params,
    ) = row
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            params = {}

    stats = {"opened": 0, "closed": 0, "marked": 0}
    bot_universes = _bot_underlyings(universe, fleet_universes)
    if not bot_universes:
        return stats

    for u in bot_universes:
        snap = snapshots.get(u)
        if snap is None:
            continue
        # Rebuild the spec per-iteration so the bot instance sees the
        # underlying it's actively evaluating (some bots key sizing off
        # spec.universe or the snapshot's underlying label).
        spec = BotSpec(
            id=bot_id,
            display_name=display_name,
            strategy_class=strategy_class,
            tier=tier,
            direction_mode=direction_mode,
            universe=u,
            tagline=tagline or "",
            description=description or "",
            params=dict(params or {}),
            is_public=bool(is_public),
            enabled=bool(enabled),
        )
        with db_connection() as conn:
            capital = load_capital(conn, bot_id)
            if capital is None:
                continue
            ml_state = ml_mod.load_state(conn, bot_id)
            bot_cls = get_bot_class(strategy_class)
            bot: BaseBot = bot_cls(spec, ml_state=asdict(ml_state))

            # 1. Mark + evaluate exits on any open positions in THIS
            # underlying only. Positions in other underlyings for the
            # same bot are handled on their own iteration.
            for pos in load_open_positions(conn, bot_id):
                if pos.underlying != u:
                    continue
                mark = mark_position(conn, pos)
                if mark is None:
                    continue
                stats["marked"] += 1
                decision = bot.exit_criteria(snap, pos)
                if decision.should_close:
                    close_position(conn, pos, reason=decision.reason or "signal")
                    stats["closed"] += 1

            # 2. Look for a new entry in this underlying.
            signal = bot.open_criteria(snap)
            if signal is not None:
                wall_strength = (
                    signal.components_at_entry.get("wall_strength")
                    if signal.components_at_entry
                    else None
                )
                realized_today = daily_realized_pnl(conn, bot_id)
                pos_id = open_position(
                    conn,
                    signal,
                    capital,
                    size_multiplier=bot.size_multiplier(),
                    wall_strength=wall_strength,
                    daily_realized_pnl=realized_today,
                )
                if pos_id is not None:
                    stats["opened"] += 1
    return stats


def rollup_daily(conn: Any) -> None:
    """Materialize today's tw_equity_curve_daily / tw_bot_metrics_daily rows.

    Idempotent — safe to call every tick. Uses UPSERT so the row is always
    up-to-date and no cron job is required.
    """
    today = date.today()
    cur = conn.cursor()
    cur.execute(
        """
        WITH open_pnl AS (
            SELECT bot_id, COALESCE(SUM(unrealized_pnl), 0) AS upnl
            FROM tw_positions GROUP BY bot_id
        ),
        realized AS (
            SELECT bot_id,
                   COALESCE(SUM(realized_pnl), 0) AS rpnl,
                   COUNT(1)                            AS n_trades
            FROM tw_trades
            WHERE closed_at::date = %s
            GROUP BY bot_id
        )
        INSERT INTO tw_equity_curve_daily (
            bot_id, session_date, starting_nav, ending_nav,
            realized_pnl, unrealized_pnl, heat_pct, n_trades
        )
        SELECT c.bot_id, %s::date,
               c.starting_capital,
               c.current_capital + COALESCE(o.upnl, 0),
               COALESCE(r.rpnl, 0),
               COALESCE(o.upnl, 0),
               CASE WHEN c.current_capital > 0
                    THEN COALESCE(o.upnl, 0) / c.current_capital
                    ELSE 0 END,
               COALESCE(r.n_trades, 0)
        FROM tw_bot_capital c
        LEFT JOIN open_pnl o ON o.bot_id = c.bot_id
        LEFT JOIN realized r ON r.bot_id = c.bot_id
        ON CONFLICT (bot_id, session_date) DO UPDATE SET
            ending_nav = EXCLUDED.ending_nav,
            realized_pnl = EXCLUDED.realized_pnl,
            unrealized_pnl = EXCLUDED.unrealized_pnl,
            heat_pct = EXCLUDED.heat_pct,
            n_trades = EXCLUDED.n_trades
        """,
        (today, today),
    )

    cur.execute(
        """
        WITH bot_trades AS (
            SELECT bot_id,
                   COUNT(1)                                          AS trades_count,
                   SUM(CASE WHEN outcome = 'win'  THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) AS losses,
                   AVG(CASE WHEN outcome = 'win'  THEN realized_pnl END) AS avg_win_pnl,
                   AVG(CASE WHEN outcome = 'loss' THEN realized_pnl END) AS avg_loss_pnl,
                   SUM(CASE WHEN outcome = 'win'  THEN realized_pnl ELSE 0 END) AS gross_win,
                   -SUM(CASE WHEN outcome = 'loss' THEN realized_pnl ELSE 0 END) AS gross_loss
            FROM tw_trades
            WHERE closed_at::date = %s
            GROUP BY bot_id
        )
        INSERT INTO tw_bot_metrics_daily (
            bot_id, session_date, trades_count, wins, losses,
            win_rate, avg_win_pnl, avg_loss_pnl, profit_factor
        )
        SELECT bot_id, %s, trades_count, wins, losses,
               CASE WHEN trades_count > 0 THEN wins::float / trades_count END,
               avg_win_pnl,
               avg_loss_pnl,
               CASE WHEN gross_loss > 0 THEN gross_win / gross_loss END
        FROM bot_trades
        ON CONFLICT (bot_id, session_date) DO UPDATE SET
            trades_count = EXCLUDED.trades_count,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            win_rate = EXCLUDED.win_rate,
            avg_win_pnl = EXCLUDED.avg_win_pnl,
            avg_loss_pnl = EXCLUDED.avg_loss_pnl,
            profit_factor = EXCLUDED.profit_factor
        """,
        (today, today),
    )


def enabled_bot_ids(conn: Any) -> List[str]:
    """Ids of every currently-enabled bot."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM tw_bots WHERE enabled = TRUE ORDER BY id")
    return [r[0] for r in cur.fetchall()]


def run_calibration_sweep(conn: Any, bot_ids: List[str]) -> int:
    """Recalibrate each of ``bot_ids`` on ``conn``; return the count done.

    Per-bot ``try`` so one bot's failure can't abort the rest. This is the
    unit the scheduler's :func:`calibration_sweep` wraps with a connection.
    """
    if not tw_config.CALIBRATION_ENABLED:
        return 0
    done = 0
    for bot_id in bot_ids:
        try:
            ml_mod.recalibrate_bot(conn, bot_id)
            done += 1
        except Exception:  # pragma: no cover - defensive per-bot isolation
            logger.warning("calibration sweep: %s failed", bot_id, exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
    return done


def calibration_sweep() -> int:
    """Recalibrate every enabled bot — the in-process 'nightly' pass.

    Runs each bot in its OWN transaction so a mid-sweep failure commits the
    work already done and can't roll back healthy bots. Also runs once on
    scheduler start, so a fresh deploy calibrates immediately instead of
    waiting a full interval (the fix for size_multiplier / profit_factor
    sitting at their defaults). Returns the number of bots recalibrated.
    """
    if not tw_config.CALIBRATION_ENABLED:
        return 0
    with db_connection() as conn:
        bot_ids = enabled_bot_ids(conn)
    done = 0
    for bot_id in bot_ids:
        try:
            with db_connection() as conn:
                ml_mod.recalibrate_bot(conn, bot_id)
            done += 1
        except Exception:  # pragma: no cover - defensive per-bot isolation
            logger.warning("calibration sweep: %s failed", bot_id, exc_info=True)
    if done:
        logger.info("TradeWorkz calibration sweep recalibrated %d bots", done)
    return done


def provision_defaults(conn: Any, fleet_capital: Optional[float] = None) -> int:
    """Insert the default bot roster + slice fleet capital evenly across them.

    Idempotent. Behavior on subsequent runs:

    * Missing bot ids are inserted with a fresh capital sleeve (and
      ``enabled`` per the roster spec — normally true).
    * Existing bot rows have their ``universe``, ``display_name``,
      ``tagline``, ``description``, and ``params`` re-synced from the
      current roster — the operator's source of truth is the roster
      module, not stale DB rows from a prior release. ``enabled`` is
      deliberately NOT re-synced, so a runtime auto-disable (the
      governance circuit breaker) or a manual disable survives a
      redeploy; re-enable a bot explicitly to bring it back.
    * Bot ids listed in :data:`RETIRED_BOT_IDS` are flipped to
      ``enabled=false`` and their sleeve is zeroed so the freed capital
      shows up in the next admin-triggered rebalance.
    * ``starting_capital`` is re-synced to ``FLEET_CAPITAL / N`` for
      every active roster bot so shrinking or growing the roster keeps
      the total sleeve pot at ``FLEET_CAPITAL``. ``current_capital`` /
      ``peak_capital`` are left untouched — running P&L history is not
      erased; hit ``/admin/reset-fleet`` to snap those back to the
      fresh baseline.
    """
    fleet = tw_config.FLEET_CAPITAL if fleet_capital is None else float(fleet_capital)
    roster = list(DEFAULT_ROSTER)
    if not roster:
        return 0

    slice_amount = round(fleet / len(roster), 2)
    now = datetime.now(timezone.utc)

    inserted = 0
    cur = conn.cursor()
    for spec in roster:
        cur.execute(
            """
            INSERT INTO tw_bots (
                id, display_name, strategy_class, tier, direction_mode,
                universe, tagline, description, is_public, enabled, params,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                display_name  = EXCLUDED.display_name,
                tier          = EXCLUDED.tier,
                direction_mode = EXCLUDED.direction_mode,
                universe      = EXCLUDED.universe,
                tagline       = EXCLUDED.tagline,
                description   = EXCLUDED.description,
                params        = EXCLUDED.params,
                updated_at    = EXCLUDED.updated_at
            """,
            (
                spec.id,
                spec.display_name,
                spec.strategy_class,
                spec.tier,
                spec.direction_mode,
                spec.universe,
                spec.tagline,
                spec.description,
                spec.is_public,
                spec.enabled,
                json.dumps(spec.params),
                now,
                now,
            ),
        )
        if cur.rowcount:
            inserted += 1
        cur.execute(
            """
            INSERT INTO tw_bot_capital (
                bot_id, starting_capital, current_capital, peak_capital,
                max_heat_pct, kelly_fraction, daily_kill_pct, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (bot_id) DO NOTHING
            """,
            (
                spec.id,
                slice_amount,
                slice_amount,
                slice_amount,
                tw_config.DEFAULT_MAX_HEAT_PCT,
                tw_config.DEFAULT_KELLY_FRACTION,
                tw_config.DEFAULT_DAILY_KILL_PCT,
                now,
            ),
        )

    # Re-sync starting_capital for every active roster bot to the fresh
    # even split. Keeps the sleeve pot equal to FLEET_CAPITAL when the
    # roster changes size between releases. current_capital / peak
    # are left alone so running P&L history is not lost.
    active_ids = [spec.id for spec in roster]
    cur.execute(
        """
        UPDATE tw_bot_capital
           SET starting_capital = %s, updated_at = %s
         WHERE bot_id = ANY(%s) AND starting_capital <> %s
        """,
        (slice_amount, now, active_ids, slice_amount),
    )

    # Retire bots that used to be in the roster but no longer belong.
    # Flip enabled=false + zero the sleeve so their capital is no longer
    # counted against the fleet total. Historical rows stay put.
    if RETIRED_BOT_IDS:
        cur.execute(
            """
            UPDATE tw_bots
               SET enabled = FALSE, updated_at = %s
             WHERE id = ANY(%s) AND enabled IS TRUE
            """,
            (now, list(RETIRED_BOT_IDS)),
        )
        cur.execute(
            """
            UPDATE tw_bot_capital
               SET starting_capital = 0, current_capital = 0,
                   peak_capital = 0, updated_at = %s
             WHERE bot_id = ANY(%s)
               AND (starting_capital <> 0 OR current_capital <> 0)
            """,
            (now, list(RETIRED_BOT_IDS)),
        )
    return inserted
