"""TradeWorkz™ HTTP API.

Every endpoint here reads/writes the ``tw_*`` tables. The router is mounted
at ``/api/tradeworkz`` in :mod:`src.api.main`. Read endpoints are open at
the router-level; write endpoints (follow/unfollow) require a resolved
end user; admin endpoints require the ``tradeworkz:admin`` scope.

The frontend at ``/trading-signals`` is admin-tier gated (the route rule
lives in ``frontend/core/auth.ts``), so ordinary customers cannot reach any
of these endpoints via the BFF even when the endpoints themselves are open.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

from src.api.database import DatabaseManager
from src.api.identity import ANONYMOUS, resolve_end_user
from src.api.scopes import SIGNALS
from src.api.security import require_scopes
from src.tradeworkz import config as tw_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tradeworkz", tags=["TradeWorkz"])


def get_db() -> DatabaseManager:
    from src.api.main import db_manager

    if db_manager is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return db_manager


def _resolve_user(request: Request) -> Optional[str]:
    user_id, _source = resolve_end_user(request)
    if user_id and user_id != ANONYMOUS.end_user_id:
        return user_id
    return None


def _rows_affected(command_tag: Any) -> int:
    """Parse the ``N`` out of an asyncpg command tag like ``"UPDATE 3"``.

    asyncpg's ``Connection.execute`` returns the SQL command tag as a
    string; the trailing integer is the number of affected rows. Returns
    0 for any tag we can't parse.
    """
    if isinstance(command_tag, str):
        parts = command_tag.split()
        if parts:
            try:
                return int(parts[-1])
            except ValueError:
                return 0
    return 0


async def _cancel_queued_for_disabled_channels(
    conn: Any, user: str, bot_id: str, channels: Dict[str, Any]
) -> int:
    """Cancel pending queued notifications for channels no longer opted-in.

    Flips ``status`` to ``'cancelled'`` for any queued ``tw_notifications_log``
    row belonging to ``(user, bot_id)`` whose ``channel`` is not enabled in
    the supplied ``channels`` preference set. Returns the number of rows
    cancelled. ``in_app`` rows are written as ``'sent'`` (never ``'queued'``)
    so this only ever touches undelivered email / webhook rows.
    """
    result = await conn.execute(
        """
        UPDATE tw_notifications_log
        SET status = 'cancelled', error = 'channel disabled by follower'
        WHERE end_user = $1 AND bot_id = $2 AND status = 'queued'
          AND COALESCE(($3::jsonb ->> channel)::boolean, false) = false
        """,
        user, bot_id, json.dumps(channels),
    )
    return _rows_affected(result)


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------


@router.get("/summary")
async def fleet_summary(db: DatabaseManager = Depends(get_db)) -> Dict[str, Any]:
    """Fleet-wide snapshot used by the /trading-signals hero row.

    Returns total fleet capital, total realized P&L today, count of live
    positions, best-performing bot 24h, and total number of bots.
    """
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            WITH cap AS (
                SELECT SUM(starting_capital) AS start, SUM(current_capital) AS cur,
                       SUM(peak_capital) AS peak
                FROM tw_bot_capital
            ),
            live AS (
                SELECT COUNT(*) AS n_positions,
                       COALESCE(SUM(unrealized_pnl), 0) AS unrealized
                FROM tw_positions
            ),
            today AS (
                SELECT COALESCE(SUM(realized_pnl), 0) AS realized,
                       COUNT(*) AS n_closes,
                       SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS wins
                FROM tw_trades WHERE closed_at::date = CURRENT_DATE
            ),
            best AS (
                SELECT bot_id, SUM(realized_pnl) AS pnl
                FROM tw_trades
                WHERE closed_at >= NOW() - INTERVAL '24 hours'
                GROUP BY bot_id
                ORDER BY pnl DESC LIMIT 1
            ),
            worst AS (
                SELECT bot_id, SUM(realized_pnl) AS pnl
                FROM tw_trades
                WHERE closed_at >= NOW() - INTERVAL '24 hours'
                GROUP BY bot_id
                ORDER BY pnl ASC LIMIT 1
            ),
            n_bots AS (SELECT COUNT(*) AS n FROM tw_bots WHERE enabled = TRUE)
            SELECT
                (SELECT start FROM cap) AS starting_capital,
                (SELECT cur FROM cap) AS current_capital,
                (SELECT peak FROM cap) AS peak_capital,
                (SELECT n_positions FROM live) AS live_positions,
                (SELECT unrealized FROM live) AS unrealized_pnl,
                (SELECT realized FROM today) AS realized_pnl_today,
                (SELECT n_closes FROM today) AS trades_today,
                (SELECT wins FROM today) AS wins_today,
                (SELECT n FROM n_bots) AS n_bots,
                (SELECT bot_id FROM best) AS best_bot_id,
                (SELECT pnl FROM best) AS best_bot_pnl,
                (SELECT bot_id FROM worst) AS worst_bot_id,
                (SELECT pnl FROM worst) AS worst_bot_pnl
            """
        )
    starting = float(row["starting_capital"] or 0.0)
    current = float(row["current_capital"] or 0.0)
    return {
        "fleet_capital_starting": starting,
        "fleet_capital_current": current,
        "fleet_capital_peak": float(row["peak_capital"] or 0.0),
        "fleet_return_pct": (current / starting - 1.0) if starting > 0 else None,
        "live_positions": int(row["live_positions"] or 0),
        "unrealized_pnl": float(row["unrealized_pnl"] or 0.0),
        "realized_pnl_today": float(row["realized_pnl_today"] or 0.0),
        "trades_today": int(row["trades_today"] or 0),
        "wins_today": int(row["wins_today"] or 0),
        "n_bots": int(row["n_bots"] or 0),
        "best_bot_id": row["best_bot_id"],
        "best_bot_pnl": float(row["best_bot_pnl"] or 0.0) if row["best_bot_pnl"] is not None else None,
        "worst_bot_id": row["worst_bot_id"],
        "worst_bot_pnl": float(row["worst_bot_pnl"] or 0.0) if row["worst_bot_pnl"] is not None else None,
        "fleet_capital_config": tw_config.FLEET_CAPITAL,
    }


@router.get("/bots")
async def list_bots(
    db: DatabaseManager = Depends(get_db),
    tier: Optional[str] = Query(default=None, description="Filter by tier"),
) -> Dict[str, Any]:
    """Fleet roster + per-bot lifetime performance snapshot."""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH lifetime AS (
                SELECT bot_id,
                       COUNT(1) AS trades,
                       SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS wins,
                       SUM(realized_pnl) AS realized_pnl_lifetime
                FROM tw_trades GROUP BY bot_id
            ),
            live AS (
                SELECT bot_id, COUNT(1) AS live_positions,
                       COALESCE(SUM(unrealized_pnl),0) AS live_unrealized
                FROM tw_positions GROUP BY bot_id
            ),
            windowed AS (
                SELECT bot_id,
                       SUM(CASE WHEN closed_at >= NOW() - INTERVAL '1 day'   THEN realized_pnl ELSE 0 END) AS pnl_1d,
                       SUM(CASE WHEN closed_at >= NOW() - INTERVAL '7 days'  THEN realized_pnl ELSE 0 END) AS pnl_7d,
                       SUM(CASE WHEN closed_at >= NOW() - INTERVAL '30 days' THEN realized_pnl ELSE 0 END) AS pnl_30d,
                       SUM(CASE WHEN closed_at >= NOW() - INTERVAL '365 days' THEN realized_pnl ELSE 0 END) AS pnl_365d
                FROM tw_trades GROUP BY bot_id
            )
            SELECT b.id, b.display_name, b.strategy_class, b.tier,
                   b.direction_mode, b.universe, b.tagline, b.description,
                   b.enabled, b.is_public,
                   c.starting_capital, c.current_capital, c.peak_capital,
                   COALESCE(lt.trades, 0)    AS lifetime_trades,
                   COALESCE(lt.wins, 0)      AS lifetime_wins,
                   COALESCE(lt.realized_pnl_lifetime, 0) AS lifetime_pnl,
                   COALESCE(lv.live_positions, 0) AS live_positions,
                   COALESCE(lv.live_unrealized, 0) AS live_unrealized,
                   COALESCE(w.pnl_1d, 0)   AS pnl_1d,
                   COALESCE(w.pnl_7d, 0)   AS pnl_7d,
                   COALESCE(w.pnl_30d, 0)  AS pnl_30d,
                   COALESCE(w.pnl_365d, 0) AS pnl_365d,
                   ml.hit_rate, ml.confidence_base, ml.confidence_threshold,
                   ml.size_multiplier, ml.last_win_rate_30d,
                   ml.last_profit_factor
            FROM tw_bots b
            LEFT JOIN tw_bot_capital c ON c.bot_id = b.id
            LEFT JOIN lifetime lt ON lt.bot_id = b.id
            LEFT JOIN live lv ON lv.bot_id = b.id
            LEFT JOIN windowed w ON w.bot_id = b.id
            LEFT JOIN tw_ml_state ml ON ml.bot_id = b.id
            WHERE ($1::text IS NULL OR b.tier = $1)
            ORDER BY COALESCE(lt.realized_pnl_lifetime, 0) DESC, b.id
            """,
            tier,
        )
    bots = []
    for r in rows:
        starting = float(r["starting_capital"] or 0.0)
        current = float(r["current_capital"] or 0.0)
        lifetime_return = (current / starting - 1.0) if starting > 0 else None
        trades = int(r["lifetime_trades"] or 0)
        wins = int(r["lifetime_wins"] or 0)
        win_rate = wins / trades if trades else None
        bots.append(
            {
                "id": r["id"],
                "display_name": r["display_name"],
                "strategy_class": r["strategy_class"],
                "tier": r["tier"],
                "direction_mode": r["direction_mode"],
                "universe": r["universe"],
                "tagline": r["tagline"],
                "description": r["description"],
                "enabled": r["enabled"],
                "is_public": r["is_public"],
                "starting_capital": starting,
                "current_capital": current,
                "peak_capital": float(r["peak_capital"] or 0.0),
                "lifetime_trades": trades,
                "lifetime_wins": wins,
                "lifetime_pnl": float(r["lifetime_pnl"] or 0.0),
                "lifetime_return_pct": lifetime_return,
                "lifetime_win_rate": win_rate,
                "live_positions": int(r["live_positions"] or 0),
                "live_unrealized_pnl": float(r["live_unrealized"] or 0.0),
                "pnl_1d": float(r["pnl_1d"] or 0.0),
                "pnl_7d": float(r["pnl_7d"] or 0.0),
                "pnl_30d": float(r["pnl_30d"] or 0.0),
                "pnl_365d": float(r["pnl_365d"] or 0.0),
                "hit_rate": r["hit_rate"],
                "confidence_base": r["confidence_base"],
                "confidence_threshold": r["confidence_threshold"],
                "size_multiplier": r["size_multiplier"],
                "recent_win_rate_30d": r["last_win_rate_30d"],
                "recent_profit_factor": r["last_profit_factor"],
            }
        )
    return {"bots": bots}


@router.get("/leaderboard")
async def leaderboard(
    period: str = Query(default="all", pattern="^(1d|7d|30d|365d|all)$"),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Ranked list of bots by return over ``period``."""
    intervals = {
        "1d": "INTERVAL '1 day'",
        "7d": "INTERVAL '7 days'",
        "30d": "INTERVAL '30 days'",
        "365d": "INTERVAL '365 days'",
    }
    where_clause = ""
    if period in intervals:
        where_clause = f"WHERE closed_at >= NOW() - {intervals[period]}"
    sql = f"""
        WITH agg AS (
            SELECT bot_id,
                   COUNT(1) AS trades,
                   SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                   SUM(realized_pnl) AS pnl,
                   AVG(pnl_percent) AS avg_pnl_pct
            FROM tw_trades
            {where_clause}
            GROUP BY bot_id
        )
        SELECT b.id, b.display_name, b.tier, b.tagline, b.enabled,
               COALESCE(a.trades, 0) AS trades,
               COALESCE(a.wins, 0) AS wins,
               COALESCE(a.pnl, 0) AS pnl,
               c.starting_capital, c.current_capital,
               COALESCE(a.avg_pnl_pct, 0) AS avg_pnl_pct
        FROM tw_bots b
        LEFT JOIN agg a ON a.bot_id = b.id
        LEFT JOIN tw_bot_capital c ON c.bot_id = b.id
        WHERE b.enabled = TRUE AND b.is_public = TRUE
        ORDER BY COALESCE(a.pnl, 0) DESC, b.id
    """
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(sql)
    board = []
    for rank, r in enumerate(rows, start=1):
        starting = float(r["starting_capital"] or 0.0)
        current = float(r["current_capital"] or 0.0)
        trades = int(r["trades"] or 0)
        wins = int(r["wins"] or 0)
        board.append(
            {
                "rank": rank,
                "bot_id": r["id"],
                "display_name": r["display_name"],
                "tier": r["tier"],
                "tagline": r["tagline"],
                "trades": trades,
                "wins": wins,
                "win_rate": (wins / trades) if trades else None,
                "pnl": float(r["pnl"] or 0.0),
                "avg_pnl_pct": float(r["avg_pnl_pct"] or 0.0),
                "starting_capital": starting,
                "current_capital": current,
                "return_pct": (current / starting - 1.0) if starting > 0 else None,
            }
        )
    return {"period": period, "leaderboard": board}


@router.get("/bots/{bot_id}")
async def bot_detail(bot_id: str, db: DatabaseManager = Depends(get_db)) -> Dict[str, Any]:
    async with db.pool.acquire() as conn:
        b = await conn.fetchrow(
            """
            SELECT b.id, b.display_name, b.strategy_class, b.tier,
                   b.direction_mode, b.universe, b.tagline, b.description,
                   b.enabled, b.is_public, b.params,
                   c.starting_capital, c.current_capital, c.peak_capital,
                   c.max_heat_pct, c.kelly_fraction, c.daily_kill_pct
            FROM tw_bots b
            LEFT JOIN tw_bot_capital c ON c.bot_id = b.id
            WHERE b.id = $1
            """,
            bot_id,
        )
        if not b:
            raise HTTPException(status_code=404, detail="Bot not found")
        ml = await conn.fetchrow(
            "SELECT * FROM tw_ml_state WHERE bot_id = $1", bot_id
        )
        positions = await conn.fetch(
            """
            SELECT id, underlying, opened_at, direction, strategy_type,
                   entry_price, current_price, quantity_open, unrealized_pnl,
                   stop_price, target_price, time_stop_at, entry_conviction
            FROM tw_positions WHERE bot_id = $1
            ORDER BY opened_at DESC
            """,
            bot_id,
        )
    params = b["params"] or {}
    if isinstance(params, str):
        params = json.loads(params)
    return {
        "id": b["id"],
        "display_name": b["display_name"],
        "strategy_class": b["strategy_class"],
        "tier": b["tier"],
        "direction_mode": b["direction_mode"],
        "universe": b["universe"],
        "tagline": b["tagline"],
        "description": b["description"],
        "enabled": b["enabled"],
        "is_public": b["is_public"],
        "params": params,
        "capital": {
            "starting": float(b["starting_capital"] or 0.0),
            "current": float(b["current_capital"] or 0.0),
            "peak": float(b["peak_capital"] or 0.0),
            "max_heat_pct": b["max_heat_pct"],
            "kelly_fraction": b["kelly_fraction"],
            "daily_kill_pct": b["daily_kill_pct"],
        },
        "ml_state": dict(ml) if ml else None,
        "open_positions": [dict(p) for p in positions],
    }


@router.get("/bots/{bot_id}/trades")
async def bot_trades(
    bot_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, underlying, opened_at, closed_at, direction,
                   strategy_type, entry_price, exit_price, quantity,
                   realized_pnl, pnl_percent, outcome, close_reason,
                   entry_conviction
            FROM tw_trades WHERE bot_id = $1
            ORDER BY closed_at DESC LIMIT $2
            """,
            bot_id, limit,
        )
    return {"bot_id": bot_id, "trades": [dict(r) for r in rows]}


@router.get("/bots/{bot_id}/equity-curve")
async def bot_equity_curve(
    bot_id: str,
    days: int = Query(default=90, ge=1, le=730),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT session_date, starting_nav, ending_nav, realized_pnl,
                   unrealized_pnl, heat_pct, n_trades
            FROM tw_equity_curve_daily
            WHERE bot_id = $1 AND session_date >= CURRENT_DATE - ($2 || ' days')::interval
            ORDER BY session_date
            """,
            bot_id, str(days),
        )
    return {"bot_id": bot_id, "points": [dict(r) for r in rows]}


@router.get("/equity-curves")
async def all_equity_curves(
    days: int = Query(default=90, ge=1, le=730),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Bundled equity curves for every enabled bot.

    The dashboard's fleet-overview chart overlays every bot on one axis, so
    one round-trip returning all curves beats N parallel requests — both for
    latency (single DB scan on the bot_id-indexed table) and for the React
    hook rules (no dynamic hook-in-loop on the client)."""
    async with db.pool.acquire() as conn:
        bots = await conn.fetch(
            "SELECT id, display_name FROM tw_bots WHERE enabled = TRUE ORDER BY id",
        )
        points = await conn.fetch(
            """
            SELECT bot_id, session_date, starting_nav, ending_nav,
                   realized_pnl, unrealized_pnl, heat_pct, n_trades
            FROM tw_equity_curve_daily
            WHERE session_date >= CURRENT_DATE - ($1 || ' days')::interval
            ORDER BY bot_id, session_date
            """,
            str(days),
        )
    by_bot: Dict[str, list[Dict[str, Any]]] = {b["id"]: [] for b in bots}
    for p in points:
        row = dict(p)
        bot_id = row.pop("bot_id")
        if bot_id in by_bot:
            by_bot[bot_id].append(row)
    bundles = [
        {
            "bot_id": b["id"],
            "display_name": b["display_name"],
            "points": by_bot.get(b["id"], []),
        }
        for b in bots
    ]
    return {"days": days, "bundles": bundles}


@router.get("/bots/{bot_id}/metrics")
async def bot_metrics(
    bot_id: str,
    days: int = Query(default=90, ge=1, le=730),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT session_date, trades_count, wins, losses, win_rate,
                   avg_win_pnl, avg_loss_pnl, profit_factor, sharpe_20d,
                   max_drawdown_pct
            FROM tw_bot_metrics_daily
            WHERE bot_id = $1 AND session_date >= CURRENT_DATE - ($2 || ' days')::interval
            ORDER BY session_date
            """,
            bot_id, str(days),
        )
    return {"bot_id": bot_id, "metrics": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# Follower endpoints
# ---------------------------------------------------------------------------


@router.get("/me/follows")
async def my_follows(request: Request, db: DatabaseManager = Depends(get_db)) -> Dict[str, Any]:
    user = _resolve_user(request)
    if user is None:
        return {"follows": []}
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT f.bot_id, f.followed_at, f.channels, f.min_confidence,
                   b.display_name, b.tagline, b.tier
            FROM tw_bot_followers f
            LEFT JOIN tw_bots b ON b.id = f.bot_id
            WHERE f.end_user = $1
            ORDER BY f.followed_at DESC
            """,
            user,
        )
    return {"follows": [dict(r) for r in rows]}


@router.post("/bots/{bot_id}/follow")
async def follow_bot(
    bot_id: str,
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    user = _resolve_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Sign in to follow a bot")
    channels = body.get("channels") or {"in_app": True}
    min_confidence = float(body.get("min_confidence") or 0.0)
    async with db.pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM tw_bots WHERE id = $1", bot_id)
        if not exists:
            raise HTTPException(status_code=404, detail="Bot not found")
        await conn.execute(
            """
            INSERT INTO tw_bot_followers (end_user, bot_id, followed_at, channels, min_confidence)
            VALUES ($1, $2, NOW(), $3::jsonb, $4)
            ON CONFLICT (end_user, bot_id) DO UPDATE
              SET channels = EXCLUDED.channels,
                  min_confidence = EXCLUDED.min_confidence
            """,
            user, bot_id, json.dumps(channels), min_confidence,
        )
        # Turning a channel OFF must take effect immediately — including for
        # events that already fanned out to a queued (not-yet-delivered) row.
        # Cancel any pending queued rows for this (user, bot) whose channel is
        # not enabled in the new preference set, so e.g. disabling "email"
        # stops the backlog the delivery worker would otherwise still ship.
        cancelled = await _cancel_queued_for_disabled_channels(conn, user, bot_id, channels)
    return {
        "status": "ok",
        "bot_id": bot_id,
        "channels": channels,
        "min_confidence": min_confidence,
        "cancelled_queued": cancelled,
    }


@router.delete("/bots/{bot_id}/follow")
async def unfollow_bot(
    bot_id: str, request: Request, db: DatabaseManager = Depends(get_db)
) -> Dict[str, Any]:
    user = _resolve_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Sign in required")
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM tw_bot_followers WHERE end_user = $1 AND bot_id = $2",
            user, bot_id,
        )
        # Unfollowing means "stop notifying me about this bot" — including
        # events that already fanned out to a queued (email / webhook) row
        # the delivery worker hasn't shipped yet. Cancel those pending rows
        # so an unfollow reliably stops the email backlog rather than only
        # preventing *future* events from queuing.
        cancel_result = await conn.execute(
            """
            UPDATE tw_notifications_log
            SET status = 'cancelled', error = 'follower unfollowed'
            WHERE end_user = $1 AND bot_id = $2 AND status = 'queued'
            """,
            user, bot_id,
        )
    return {
        "status": "ok",
        "bot_id": bot_id,
        "cancelled_queued": _rows_affected(cancel_result),
    }


@router.delete("/me/feed")
async def clear_my_feed(
    request: Request,
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Delete every ``in_app`` notification row belonging to the caller.

    Only touches the ``in_app`` channel — pending ``email`` / ``webhook``
    rows the delivery worker hasn't shipped yet are left in place so a
    "clear my inbox" gesture on the dashboard doesn't accidentally
    cancel a queued email. Returns the number of rows deleted.
    """
    user = _resolve_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Sign in required")
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM tw_notifications_log
            WHERE end_user = $1 AND channel = 'in_app'
            """,
            user,
        )
    deleted = 0
    if isinstance(result, str) and result.startswith("DELETE "):
        try:
            deleted = int(result.split()[1])
        except (IndexError, ValueError):
            deleted = 0
    return {"status": "ok", "deleted": deleted}


@router.get("/me/feed")
async def my_feed(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    user = _resolve_user(request)
    if user is None:
        return {"feed": []}
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT n.id, n.bot_id, b.display_name, n.event_type, n.trade_id,
                   n.position_id, n.status, n.payload, n.sent_at
            FROM tw_notifications_log n
            LEFT JOIN tw_bots b ON b.id = n.bot_id
            WHERE n.end_user = $1 AND n.channel = 'in_app'
            ORDER BY n.sent_at DESC LIMIT $2
            """,
            user, limit,
        )
    feed = []
    for r in rows:
        payload = r["payload"] or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        feed.append(
            {
                "id": r["id"],
                "bot_id": r["bot_id"],
                "bot_display_name": r["display_name"],
                "event_type": r["event_type"],
                "trade_id": r["trade_id"],
                "position_id": r["position_id"],
                "status": r["status"],
                "payload": payload,
                "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None,
            }
        )
    return {"feed": feed}


# ---------------------------------------------------------------------------
# Admin endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/admin/provision",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_provision(db: DatabaseManager = Depends(get_db)) -> Dict[str, Any]:
    """Seed the default bot roster + capital sleeves (idempotent).

    Guarded on the ``signals`` scope — the same scope the admin monitoring
    page is behind. TradeWorkz remains admin-only until we expose it to
    customers, at which point this stays admin-only regardless.
    """
    from src.tradeworkz.engine import provision_defaults
    from src.database import db_connection

    with db_connection() as conn:
        inserted = provision_defaults(conn)
    return {"status": "ok", "inserted": inserted}


@router.patch(
    "/admin/bots/{bot_id}",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_update_bot(
    bot_id: str,
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    fields = {}
    if "enabled" in body:
        fields["enabled"] = bool(body["enabled"])
    if "is_public" in body:
        fields["is_public"] = bool(body["is_public"])
    if "tagline" in body:
        fields["tagline"] = body["tagline"]
    if "description" in body:
        fields["description"] = body["description"]
    if not fields:
        raise HTTPException(status_code=400, detail="No updatable fields provided")
    sets = ", ".join(f"{k} = ${i + 2}" for i, k in enumerate(fields))
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            f"UPDATE tw_bots SET {sets}, updated_at = NOW() WHERE id = $1",
            bot_id, *fields.values(),
        )
    return {"status": "ok", "bot_id": bot_id, "updated": fields, "result": result}


@router.patch(
    "/admin/bots/{bot_id}/capital",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_update_capital(
    bot_id: str,
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    fields = {}
    for k in ("current_capital", "max_heat_pct", "kelly_fraction", "daily_kill_pct"):
        if k in body:
            fields[k] = float(body[k])
    if not fields:
        raise HTTPException(status_code=400, detail="No updatable fields provided")
    sets = ", ".join(f"{k} = ${i + 2}" for i, k in enumerate(fields))
    async with db.pool.acquire() as conn:
        await conn.execute(
            f"UPDATE tw_bot_capital SET {sets}, updated_at = NOW() WHERE bot_id = $1",
            bot_id, *fields.values(),
        )
    return {"status": "ok", "bot_id": bot_id, "updated": fields}


@router.post(
    "/admin/simulate",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_simulate(
    body: Dict[str, Any] = Body(default_factory=dict),
) -> Dict[str, Any]:
    """Seed the tw_ tables with deterministic synthetic trade history.

    Fully wipes any existing trades / equity / metrics / ml_state per bot
    before inserting, so the endpoint is idempotent — re-running with the
    same body converges to the same fleet state. Uses the synchronous
    psycopg2 pool via ``src.database.db_connection`` because the simulator
    does many small inserts and pipelining them through asyncpg would add
    complexity for no throughput gain here.

    Body (all optional):
        days: how many trading days of history to synthesize (default 60,
              min 1, max 365)
        seed: master RNG seed for reproducibility (default 42)
        scale: PnL magnitude multiplier (default 1.0)
        bot_ids: subset to simulate; default = every enabled bot
    """
    from src.database import db_connection
    from src.tradeworkz.simulate import simulate

    days = int(body.get("days") or 60)
    days = max(1, min(365, days))
    master_seed = int(body.get("seed") or 42)
    scale = float(body.get("scale") or 1.0)
    bot_ids = body.get("bot_ids")
    if bot_ids is not None and not isinstance(bot_ids, list):
        raise HTTPException(status_code=400, detail="bot_ids must be a list of strings")

    with db_connection() as conn:
        summary = simulate(
            conn, days=days, master_seed=master_seed, scale=scale, bot_ids=bot_ids,
        )
    return summary


@router.post(
    "/admin/simulate/clear",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_simulate_clear(
    body: Dict[str, Any] = Body(default_factory=dict),
) -> Dict[str, Any]:
    """DEPRECATED alias for /admin/reset-fleet — clears sim AND live rows.

    Kept for backward-compat with the existing "Clear" admin button.
    Prefer POST /admin/reset-fleet for new call-sites; the semantics are
    identical.
    """
    from src.database import db_connection
    from src.tradeworkz.simulate import clear

    bot_ids = body.get("bot_ids")
    if bot_ids is not None and not isinstance(bot_ids, list):
        raise HTTPException(status_code=400, detail="bot_ids must be a list of strings")
    with db_connection() as conn:
        return clear(conn, bot_ids=bot_ids)


@router.post(
    "/admin/reset-fleet",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_reset_fleet(
    body: Dict[str, Any] = Body(default_factory=dict),
) -> Dict[str, Any]:
    """Full fleet reset: wipe every trade / position / notification / equity
    row and reset every sleeve to its starting_capital.

    Use this after a P&L math bug, an accidental seeding run, or any time
    the leaderboard is showing state you don't trust. The response
    includes per-table deletion counts so you can eyeball that something
    was actually cleared (useful when the leaderboard doesn't visibly
    change because everything summed to zero anyway).

    Body (optional):
        bot_ids: list of bot ids to reset. Omit to reset the full fleet.
    """
    from src.database import db_connection
    from src.tradeworkz.simulate import clear

    bot_ids = body.get("bot_ids")
    if bot_ids is not None and not isinstance(bot_ids, list):
        raise HTTPException(status_code=400, detail="bot_ids must be a list of strings")
    with db_connection() as conn:
        return clear(conn, bot_ids=bot_ids)


@router.get(
    "/admin/positions",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_positions(
    db: DatabaseManager = Depends(get_db),
):
    """Fleet-wide currently-OPEN positions with mark-to-market.

    The /admin/trades feed is a history of CLOSED trades — one row per
    close. This endpoint fills the complementary need: what's still on
    the book right now, per bot, with unrealized P&L, target/stop
    levels, and time_stop_at so the operator can see at a glance how
    long a hold has been running and where the exits would fire.

    Read-only, no filters, always returns all open rows across the
    fleet — a fleet has at most tens of concurrent positions in
    practice, so paging isn't needed.
    """
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.bot_id, b.display_name AS bot_display_name,
                   p.underlying, p.opened_at, p.updated_at, p.direction,
                   p.strategy_type, p.legs, p.entry_price, p.current_price,
                   p.quantity_open, p.unrealized_pnl, p.stop_price,
                   p.target_price, p.time_stop_at, p.min_hold_until,
                   p.wall_ref_price, p.wall_ref_side, p.entry_conviction,
                   COALESCE(p.components_at_entry ->> 'origin', 'live') AS origin,
                   p.components_at_entry
            FROM tw_positions p
            LEFT JOIN tw_bots b ON b.id = p.bot_id
            ORDER BY p.opened_at DESC
            """
        )

    def _parse_json(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v

    entries: List[Dict[str, Any]] = []
    for r in rows:
        entries.append(
            {
                "id": r["id"],
                "bot_id": r["bot_id"],
                "bot_display_name": r["bot_display_name"],
                "underlying": r["underlying"],
                "opened_at": r["opened_at"].isoformat() if r["opened_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                "direction": r["direction"],
                "strategy_type": r["strategy_type"],
                "legs": _parse_json(r["legs"]),
                "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                "current_price": (
                    float(r["current_price"]) if r["current_price"] is not None else None
                ),
                "quantity_open": (
                    int(r["quantity_open"]) if r["quantity_open"] is not None else None
                ),
                "unrealized_pnl": (
                    float(r["unrealized_pnl"]) if r["unrealized_pnl"] is not None else None
                ),
                "stop_price": (
                    float(r["stop_price"]) if r["stop_price"] is not None else None
                ),
                "target_price": (
                    float(r["target_price"]) if r["target_price"] is not None else None
                ),
                "time_stop_at": (
                    r["time_stop_at"].isoformat() if r["time_stop_at"] else None
                ),
                "min_hold_until": (
                    r["min_hold_until"].isoformat() if r["min_hold_until"] else None
                ),
                "wall_ref_price": (
                    float(r["wall_ref_price"]) if r["wall_ref_price"] is not None else None
                ),
                "wall_ref_side": r["wall_ref_side"],
                "entry_conviction": (
                    float(r["entry_conviction"])
                    if r["entry_conviction"] is not None else None
                ),
                "origin": r["origin"],
                "components_at_entry": _parse_json(r["components_at_entry"]),
            }
        )
    total_unrealized = sum(
        (e["unrealized_pnl"] or 0.0) for e in entries
    )
    return {
        "entries": entries,
        "summary": {
            "n_open": len(entries),
            "total_unrealized_pnl": total_unrealized,
        },
    }


@router.get(
    "/admin/trades",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_trades(
    origin: str = Query(default="all", pattern="^(all|live|simulate)$"),
    bot_id: Optional[str] = Query(default=None),
    since: Optional[str] = Query(
        default=None,
        description="ISO-8601 timestamp; only trades closed on/after this.",
    ),
    until: Optional[str] = Query(
        default=None,
        description="ISO-8601 timestamp; only trades closed before this.",
    ),
    limit: int = Query(default=200, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    format: str = Query(default="json", pattern="^(json|csv)$"),
    db: DatabaseManager = Depends(get_db),
):
    """Audit-trail feed of every closed trade with an explicit sim/live tag.

    Params:
        origin: 'all' (default), 'live', or 'simulate'. Rows whose
                ``components_at_entry`` has no ``origin`` field predate
                the stamping change and are treated as **live** (legacy
                real trades).
        bot_id: filter to one bot.
        since / until: ISO-8601 (or bare ``YYYY-MM-DD``) filter on
                       ``closed_at``. ``until`` is exclusive.
        limit / offset: standard paging. Max page size 2000.
        format: ``json`` (default) or ``csv``. CSV is returned with a
                Content-Type of text/csv so a curl > file.csv works.

    The response includes a summary row (n_trades, sum_realized_pnl,
    n_wins, n_losses, n_scratches) computed across the FILTERED set —
    useful for "does the total P&L in the audit view match the
    leaderboard NAV?" sanity checks.
    """
    from fastapi.responses import PlainTextResponse

    where: List[str] = []
    params: List[Any] = []

    if origin == "live":
        where.append(
            "(components_at_entry ->> 'origin') IS DISTINCT FROM 'simulate'"
        )
    elif origin == "simulate":
        where.append("(components_at_entry ->> 'origin') = 'simulate'")

    if bot_id:
        params.append(bot_id)
        where.append(f"bot_id = ${len(params)}")
    if since:
        params.append(since)
        where.append(f"closed_at >= ${len(params)}::timestamptz")
    if until:
        params.append(until)
        where.append(f"closed_at < ${len(params)}::timestamptz")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    params.append(limit)
    limit_idx = len(params)
    params.append(offset)
    offset_idx = len(params)

    # LEFT JOIN tw_bots so the response carries the human-readable
    # display_name alongside the bot_id — the audit UI prefers the
    # display_name for readability, but keeps bot_id in the payload
    # for stability across renames. Column names in the WHERE clauses
    # above (bot_id, closed_at, components_at_entry) are unambiguous
    # because none of those columns exist on tw_bots.
    row_sql = f"""
        SELECT tr.id, tr.bot_id, b.display_name AS bot_display_name,
               tr.underlying, tr.opened_at, tr.closed_at, tr.direction,
               tr.strategy_type, tr.entry_price, tr.exit_price, tr.quantity,
               tr.realized_pnl, tr.pnl_percent, tr.outcome, tr.close_reason,
               tr.entry_conviction,
               COALESCE(tr.components_at_entry ->> 'origin', 'live') AS origin,
               tr.legs, tr.components_at_entry, tr.components_at_exit
        FROM tw_trades tr
        LEFT JOIN tw_bots b ON b.id = tr.bot_id
        {where_sql}
        ORDER BY tr.closed_at DESC
        LIMIT ${limit_idx} OFFSET ${offset_idx}
    """
    summary_sql = f"""
        SELECT COUNT(*)                                          AS n_trades,
               COALESCE(SUM(realized_pnl), 0)                    AS sum_realized_pnl,
               SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END)  AS n_wins,
               SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) AS n_losses,
               SUM(CASE WHEN outcome = 'scratch' THEN 1 ELSE 0 END)
                                                                 AS n_scratches
        FROM tw_trades
        {where_sql}
    """

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(row_sql, *params)
        summary_params = params[:-2]  # drop limit/offset
        summary_row = await conn.fetchrow(summary_sql, *summary_params)

    def _parse_json(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v

    entries: List[Dict[str, Any]] = []
    for r in rows:
        entries.append(
            {
                "id": r["id"],
                "bot_id": r["bot_id"],
                "bot_display_name": r["bot_display_name"],
                "underlying": r["underlying"],
                "opened_at": r["opened_at"].isoformat() if r["opened_at"] else None,
                "closed_at": r["closed_at"].isoformat() if r["closed_at"] else None,
                "direction": r["direction"],
                "strategy_type": r["strategy_type"],
                "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                "exit_price": float(r["exit_price"]) if r["exit_price"] is not None else None,
                "quantity": int(r["quantity"]) if r["quantity"] is not None else None,
                "realized_pnl": float(r["realized_pnl"]) if r["realized_pnl"] is not None else None,
                "pnl_percent": float(r["pnl_percent"]) if r["pnl_percent"] is not None else None,
                "outcome": r["outcome"],
                "close_reason": r["close_reason"],
                "entry_conviction": (
                    float(r["entry_conviction"])
                    if r["entry_conviction"] is not None else None
                ),
                "origin": r["origin"],
                "legs": _parse_json(r["legs"]),
                "components_at_entry": _parse_json(r["components_at_entry"]),
                "components_at_exit": _parse_json(r["components_at_exit"]),
            }
        )

    summary = {
        "n_trades": int(summary_row["n_trades"] or 0),
        "sum_realized_pnl": float(summary_row["sum_realized_pnl"] or 0.0),
        "n_wins": int(summary_row["n_wins"] or 0),
        "n_losses": int(summary_row["n_losses"] or 0),
        "n_scratches": int(summary_row["n_scratches"] or 0),
    }

    if format == "csv":
        import io
        import csv

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(
            [
                "id", "bot_id", "bot_display_name", "underlying", "origin",
                "opened_at", "closed_at", "direction", "strategy_type",
                "contract", "entry_price", "exit_price", "quantity",
                "cost_basis", "proceeds",
                "realized_pnl", "pnl_percent", "outcome",
                "close_reason", "entry_conviction",
            ]
        )
        for e in entries:
            # Compact "SPY 7/8 P746" contract label from the first leg;
            # multi-leg spreads collapse to "leg1 / leg2".
            legs = e.get("legs") or []
            contract_parts: List[str] = []
            for leg in legs:
                exp = str(leg.get("expiration") or "")
                exp_short = exp[5:].replace("-", "/") if len(exp) >= 10 else exp
                strike = leg.get("strike")
                right = (leg.get("option_type") or "")[:1].upper()
                sym = leg.get("option_symbol") or f"{e['underlying']} {exp_short}{right}{strike}"
                contract_parts.append(str(sym))
            contract = " / ".join(contract_parts) if contract_parts else ""
            qty = e["quantity"] or 0
            entry_px = e["entry_price"] or 0
            exit_px = e["exit_price"] or 0
            cost_basis = round(entry_px * qty * 100, 2) if qty else None
            proceeds = round(exit_px * qty * 100, 2) if qty else None
            w.writerow(
                [
                    e["id"], e["bot_id"], e["bot_display_name"],
                    e["underlying"], e["origin"],
                    e["opened_at"], e["closed_at"], e["direction"],
                    e["strategy_type"], contract,
                    e["entry_price"], e["exit_price"], e["quantity"],
                    cost_basis, proceeds,
                    e["realized_pnl"], e["pnl_percent"],
                    e["outcome"], e["close_reason"], e["entry_conviction"],
                ]
            )
        return PlainTextResponse(
            content=buf.getvalue(),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="tradeworkz-audit-'
                    f'{origin}.csv"'
                ),
            },
        )

    return {
        "entries": entries,
        "summary": summary,
        "filters": {
            "origin": origin,
            "bot_id": bot_id,
            "since": since,
            "until": until,
            "limit": limit,
            "offset": offset,
        },
    }


@router.post(
    "/admin/inject-test-event",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_inject_test_event(
    body: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    """Synthesize a fake entry / exit / lifecycle notification for a bot.

    Drives the same ``notifications.fanout_event`` helper the reconciler
    uses on real ticks, so every follower of the bot whose channel
    preferences include the enabled channel gets a row in
    ``tw_notifications_log``:
      * in_app  — status='sent' (visible in the bell immediately)
      * email   — status='queued' (delivery worker picks it up ≤60s)
      * webhook — status='queued' (delivery worker not yet wired)

    Body:
        bot_id (required): the bot to attribute the fake event to.
        event_type: 'entry' | 'exit' | 'both'. Default 'exit'.
        payload: optional dict of overrides merged over the synthesized
                 default payload for the event type.
    """
    from src.database import db_connection
    from src.tradeworkz.notifications import fanout_event

    bot_id = body.get("bot_id")
    if not isinstance(bot_id, str) or not bot_id:
        raise HTTPException(status_code=400, detail="bot_id (string) is required")
    event_type = body.get("event_type") or "exit"
    if event_type not in ("entry", "exit", "both"):
        raise HTTPException(
            status_code=400,
            detail="event_type must be one of 'entry', 'exit', 'both'",
        )
    payload_overrides = body.get("payload") or {}
    if not isinstance(payload_overrides, dict):
        raise HTTPException(status_code=400, detail="payload must be an object")

    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, display_name FROM tw_bots WHERE id = %s",
            (bot_id,),
        )
        bot_row = cur.fetchone()
        if not bot_row:
            raise HTTPException(status_code=404, detail=f"Bot {bot_id!r} not found")

        entry_default: Dict[str, Any] = {
            "underlying": "SPY",
            "direction": "bullish",
            "strategy_type": "BUY_CALL_DEBIT",
            "conviction": 0.72,
            "contracts": 4,
            "entry_price": 3.15,
            "target_price": 4.20,
            "stop_price": 2.80,
            "rationale": "Injected test entry: put-wall rejection at 578 in positive-γ regime.",
            "test": True,
        }
        exit_default: Dict[str, Any] = {
            "underlying": "SPY",
            "direction": "bullish",
            "strategy_type": "BUY_CALL_DEBIT",
            "outcome": "win",
            "realized_pnl": 348.0,
            "pnl_percent": 0.276,
            "reason": "target",
            "contracts": 4,
            "entry_price": 3.15,
            "exit_price": 4.02,
            "conviction": 0.72,
            "test": True,
        }

        written = {"entry": 0, "exit": 0}
        if event_type in ("entry", "both"):
            entry_payload = {**entry_default, **payload_overrides}
            written["entry"] = fanout_event(
                conn,
                bot_id=bot_id,
                event_type="entry",
                payload=entry_payload,
            )
        if event_type in ("exit", "both"):
            exit_payload = {**exit_default, **payload_overrides}
            written["exit"] = fanout_event(
                conn,
                bot_id=bot_id,
                event_type="exit",
                payload=exit_payload,
            )

    return {
        "status": "ok",
        "bot_id": bot_id,
        "bot_display_name": bot_row[1],
        "event_type": event_type,
        "notifications_written": written,
        "note": (
            "in_app rows are visible in the bell immediately. email rows "
            "will be picked up by zerogex-web-tradeworkz-notify.timer within a minute."
        ),
    }


# ---------------------------------------------------------------------------
# Internal delivery-worker endpoints
# ---------------------------------------------------------------------------
#
# The frontend Node worker at ``scripts/tradeworkz-notify-deliver.mts``
# polls queued email / webhook notification rows, ships them through
# Resend / a webhook, and reports success or failure back. Both endpoints
# require the SIGNALS scope — the same scope the worker's shared
# ``ZEROGEX_API_TOKEN`` already carries for the BFF proxy. That way we
# don't invent a new scope just for a one-caller machine credential.


@router.get(
    "/admin/diagnose",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def admin_diagnose() -> Dict[str, Any]:
    """Explain per-bot why an entry did or did not fire on the current tick.

    Rebuilds one MarketSnapshot per universe, then runs every enabled
    bot's ``open_criteria`` inline and captures:

      * The raw snapshot (spot, walls, GEX, regime, session progress).
      * Whether the bot returned a signal — and if not, which gate
        rejected it, walked in the same order the bot uses at tick time.
      * For any bot that DID emit a signal: whether ``spread_price``
        would return a fillable per-share debit, which is the single
        biggest reason "signal fires but no trade is inserted." A
        missing or stale ``option_chains`` row for the ATM strike is
        the usual culprit.
      * Scheduler status + when it last ticked (best-effort log-derived).

    This endpoint is read-only — it never inserts a position or
    persists any state — so it is safe to hit repeatedly during
    diagnosis. Admin-scoped for consistency with the other
    ``/admin/*`` endpoints.
    """
    from dataclasses import asdict
    from src.database import db_connection
    from src.tradeworkz import scheduler as scheduler_mod
    from src.tradeworkz.bots.base import BaseBot
    from src.tradeworkz.context import build_snapshot, MarketSnapshot
    from src.tradeworkz.models import BotSpec
    from src.tradeworkz.pricing import spread_price
    from src.tradeworkz.registry import get_bot_class
    from src.tradeworkz import ml as ml_mod
    from src.tradeworkz.reconciler import load_capital

    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, display_name, strategy_class, tier, direction_mode,
                   universe, tagline, description, is_public, enabled, params
            FROM tw_bots WHERE enabled = TRUE
            ORDER BY id
            """
        )
        bots = cur.fetchall()

        underlyings = sorted({row[5] for row in bots}) or [tw_config.UNIVERSE]
        snapshots: Dict[str, Optional[MarketSnapshot]] = {}
        for u in underlyings:
            snapshots[u] = build_snapshot(conn, u)

        bot_reports: List[Dict[str, Any]] = []
        for row in bots:
            (
                bot_id, display_name, strategy_class, tier, direction_mode,
                universe, tagline, description, is_public, enabled, params,
            ) = row
            if isinstance(params, str):
                try:
                    params = json.loads(params)
                except Exception:
                    params = {}
            snap = snapshots.get(universe)
            report: Dict[str, Any] = {
                "bot_id": bot_id,
                "display_name": display_name,
                "strategy_class": strategy_class,
                "universe": universe,
            }
            if snap is None:
                report["decision"] = "no_snapshot"
                report["reason"] = (
                    f"build_snapshot({universe}) returned None — no "
                    "underlying_quotes tick or no gex_summary row."
                )
                bot_reports.append(report)
                continue

            spec = BotSpec(
                id=bot_id, display_name=display_name,
                strategy_class=strategy_class, tier=tier,
                direction_mode=direction_mode, universe=universe,
                tagline=tagline or "", description=description or "",
                params=dict(params or {}),
                is_public=bool(is_public), enabled=bool(enabled),
            )
            ml_state = ml_mod.load_state(conn, bot_id)
            capital = load_capital(conn, bot_id)
            bot_cls = get_bot_class(strategy_class)
            bot: BaseBot = bot_cls(spec, ml_state=asdict(ml_state))

            open_pos_count = 0
            cur.execute(
                "SELECT COUNT(1) FROM tw_positions WHERE bot_id = %s",
                (bot_id,),
            )
            row_ct = cur.fetchone()
            open_pos_count = int(row_ct[0]) if row_ct else 0
            report["open_positions"] = open_pos_count
            report["current_capital"] = (
                capital.current_capital if capital else None
            )
            report["confidence_base"] = bot.confidence_base()
            report["confidence_threshold"] = bot.confidence_threshold()
            report["size_multiplier"] = bot.size_multiplier()

            try:
                signal = bot.open_criteria(snap)
            except Exception as exc:  # noqa: BLE001
                report["decision"] = "error"
                report["error"] = f"{type(exc).__name__}: {exc}"
                bot_reports.append(report)
                continue

            if signal is None:
                report["decision"] = "no_signal"
                report["reason"] = _explain_no_signal(bot, snap)
                bot_reports.append(report)
                continue

            report["decision"] = "signal"
            report["direction"] = signal.direction
            report["strategy_type"] = signal.strategy_type
            report["conviction"] = signal.conviction
            report["legs"] = [
                {
                    "option_symbol": l.option_symbol,
                    "side": l.side,
                    "option_type": l.option_type,
                    "strike": l.strike,
                    "expiration": l.expiration,
                }
                for l in signal.legs
            ]
            report["rationale"] = signal.rationale

            legs_dicts = [
                {
                    "option_symbol": l.option_symbol,
                    "side": l.side,
                    "option_type": l.option_type,
                    "strike": l.strike,
                    "expiration": l.expiration,
                }
                for l in signal.legs
            ]
            fill_probe: Dict[str, Any] = {}
            for l in legs_dicts:
                sym = l["option_symbol"]
                cur.execute(
                    """
                    SELECT bid, ask, last, timestamp,
                           EXTRACT(EPOCH FROM (NOW() - timestamp)) AS age_s
                    FROM option_chains
                    WHERE option_symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (sym,),
                )
                q = cur.fetchone()
                fill_probe[sym] = (
                    {
                        "bid": float(q[0]) if q[0] is not None else None,
                        "ask": float(q[1]) if q[1] is not None else None,
                        "last": float(q[2]) if q[2] is not None else None,
                        "quote_age_s": float(q[4]) if q[4] is not None else None,
                    }
                    if q
                    else {"status": "no_row"}
                )
            report["fill_probe"] = fill_probe

            price = spread_price(conn, legs_dicts, action="open")
            report["fill_price"] = price
            report["fillable"] = price is not None and price > 0
            if not report["fillable"]:
                report["fill_block_reason"] = (
                    "spread_price returned None or <= 0. Either no "
                    "option_chains row exists for the ATM symbol, or "
                    "the quote is older than OPTION_QUOTE_MAX_AGE_SECONDS."
                )
            bot_reports.append(report)

    scheduler = scheduler_mod.scheduler
    scheduler_status = {
        "enabled": tw_config.ENGINE_ENABLED,
        "running": scheduler.is_running(),
        "interval_seconds": tw_config.ENGINE_INTERVAL_SECONDS,
    }

    snapshot_summaries: Dict[str, Any] = {}
    for u, snap in snapshots.items():
        if snap is None:
            snapshot_summaries[u] = None
            continue
        snapshot_summaries[u] = {
            "timestamp": (
                snap.timestamp.isoformat()
                if hasattr(snap.timestamp, "isoformat")
                else str(snap.timestamp)
            ),
            "spot": snap.spot,
            "call_wall": snap.call_wall,
            "put_wall": snap.put_wall,
            "gamma_flip": snap.gamma_flip,
            "max_pain": snap.max_pain,
            "net_gex": snap.net_gex,
            "gex_regime": snap.gex_regime(),
            "dealer_net_delta": snap.dealer_net_delta,
            "vix": snap.vix,
            "vwap": snap.vwap,
            "vwap_deviation_pct": snap.vwap_deviation_pct,
            "session_high": snap.session_high,
            "session_low": snap.session_low,
            "minutes_since_open": snap.minutes_since_open,
            "minutes_to_close": snap.minutes_to_close,
            "distance_to_call_wall_pct": snap.distance_to_call_wall_pct(),
            "distance_to_put_wall_pct": snap.distance_to_put_wall_pct(),
        }

    return {
        "scheduler": scheduler_status,
        "snapshots": snapshot_summaries,
        "bots": bot_reports,
    }


def _explain_no_signal(bot: "BaseBot", snap: "MarketSnapshot") -> str:  # noqa: F821
    """Walk the shared gates every bot uses and name the first one that fails.

    Each bot's ``open_criteria`` implements its own gate order, so this is
    a best-effort common-case explanation covering:
      * gamma regime (only relevant for regime-gated bots)
      * minutes_since_open floor (early-session noise reject)
      * wall_proximity_pct (for the wall bouncer)
      * confidence threshold vs. base

    Anything more specific requires reading the individual bot's source.
    """
    reasons: List[str] = []
    regime = snap.gex_regime()
    strategy = bot.spec.strategy_class

    if strategy == "PutCallWallBouncer":
        if regime not in {"positive_strong", "positive_weak"}:
            return (
                f"regime_gate: gex_regime={regime!r}; wall bouncer only fires "
                "in positive-γ regimes."
            )
        mins = snap.minutes_since_open
        if mins is None or mins < 30:
            return (
                f"session_gate: minutes_since_open={mins}; wall bouncer waits "
                "≥30 min after the open."
            )
        prox_pct = float(bot.params.get("wall_proximity_pct", 0.002))
        call_d = snap.distance_to_call_wall_pct()
        put_d = snap.distance_to_put_wall_pct()
        call_touch = (
            call_d is not None
            and abs(call_d) <= prox_pct
            and snap.spot <= (snap.call_wall or 0)
        )
        put_touch = (
            put_d is not None
            and abs(put_d) <= prox_pct
            and snap.spot >= (snap.put_wall or 0)
        )
        if not (call_touch or put_touch):
            return (
                f"proximity_gate: spot {snap.spot} not within "
                f"{prox_pct * 100:.2f}% of call_wall={snap.call_wall} "
                f"(dist={call_d}) or put_wall={snap.put_wall} "
                f"(dist={put_d})."
            )
        return (
            "conviction_gate: wall was touched but blended conviction < "
            f"threshold ({bot.confidence_threshold():.2f})."
        )
    if snap.gex_regime() == "unknown":
        reasons.append("net_gex is None — gex_summary may be empty.")
    if snap.minutes_since_open is None:
        reasons.append("session hasn't started (pre-open).")
    return "; ".join(reasons) or (
        f"bot-specific gate; open_criteria returned None with regime={regime}."
    )


@router.get(
    "/internal/queued-notifications",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def internal_queued_notifications(
    limit: int = Query(default=100, ge=1, le=1000),
    channel: Optional[str] = Query(default=None, pattern="^(email|webhook)$"),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Return queued email / webhook notification rows for the worker.

    Rows are ordered by ``sent_at`` (which for a queued row is the
    ``NOW()`` at insert time — effectively FIFO). The worker pulls
    ``limit`` at a time so a burst of hundreds of exits doesn't stall
    delivery of newer ones.

    A queued row records that the follower *wanted* this channel at the
    moment the event fanned out. Opt-in is re-checked here at delivery
    time: a row is only handed to the worker if the recipient still
    follows the bot with that channel enabled. This is the authoritative
    guard behind unfollow / channel-off — it suppresses even backlog that
    queued before the follower opted out (including rows orphaned by an
    unfollow that removed the follower entirely), so "I unfollowed but the
    emails keep coming" cannot happen regardless of how a row was queued.
    """
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT n.id, n.end_user, n.bot_id, b.display_name AS bot_display_name,
                   n.event_type, n.trade_id, n.position_id, n.channel,
                   n.status, n.payload, n.sent_at
            FROM tw_notifications_log n
            LEFT JOIN tw_bots b ON b.id = n.bot_id
            WHERE n.status = 'queued'
              AND ($1::text IS NULL OR n.channel = $1)
              AND n.channel IN ('email', 'webhook')
              AND EXISTS (
                  SELECT 1 FROM tw_bot_followers f
                  WHERE f.end_user = n.end_user
                    AND f.bot_id = n.bot_id
                    AND COALESCE((f.channels ->> n.channel)::boolean, false) = true
              )
            ORDER BY n.sent_at ASC
            LIMIT $2
            """,
            channel, limit,
        )
    entries = []
    for r in rows:
        payload = r["payload"] or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        entries.append(
            {
                "id": r["id"],
                "end_user": r["end_user"],
                "bot_id": r["bot_id"],
                "bot_display_name": r["bot_display_name"],
                "event_type": r["event_type"],
                "trade_id": r["trade_id"],
                "position_id": r["position_id"],
                "channel": r["channel"],
                "status": r["status"],
                "payload": payload,
                "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None,
            }
        )
    return {"entries": entries}


@router.post(
    "/internal/mark-notification",
    dependencies=[Depends(require_scopes(SIGNALS))],
)
async def internal_mark_notification(
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Mark a queued notification row as sent or failed.

    Body:
        id: BIGINT, the tw_notifications_log row id.
        status: 'sent' | 'failed'.
        error: optional string; recorded on failure so an operator can
               inspect why (bad recipient, Resend rate-limit, transient
               5xx, etc.).
    """
    row_id = body.get("id")
    status = body.get("status")
    error = body.get("error")
    if not isinstance(row_id, int):
        raise HTTPException(status_code=400, detail="id must be an integer")
    if status not in ("sent", "failed"):
        raise HTTPException(status_code=400, detail="status must be 'sent' or 'failed'")
    async with db.pool.acquire() as conn:
        # ``$1`` must be cast to text in BOTH uses. Postgres infers the
        # parameter's type from every occurrence: ``status = $1`` deduces
        # ``character varying`` (the column type) while ``$1 = 'sent'``
        # deduces ``text`` (the untyped literal), and the two are reported
        # as inconsistent (AmbiguousParameterError). This 500'd every
        # mark-notification call, so the delivery worker could never flip a
        # row out of ``queued`` — it re-sent the same email every cycle.
        # Pinning both uses to ``$1::text`` gives the parameter one type.
        result = await conn.execute(
            """
            UPDATE tw_notifications_log
            SET status = $1::text,
                error = $2,
                sent_at = CASE WHEN $1::text = 'sent' THEN NOW() ELSE sent_at END
            WHERE id = $3
            """,
            status, error, row_id,
        )
    return {"status": "ok", "id": row_id, "new_status": status, "result": result}
