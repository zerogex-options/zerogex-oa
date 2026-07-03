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
    return {"status": "ok", "bot_id": bot_id, "channels": channels, "min_confidence": min_confidence}


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
    return {"status": "ok", "bot_id": bot_id}


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
    """Wipe all synthesized (and real) trade history back to a clean fleet."""
    from src.database import db_connection
    from src.tradeworkz.simulate import clear

    bot_ids = body.get("bot_ids")
    if bot_ids is not None and not isinstance(bot_ids, list):
        raise HTTPException(status_code=400, detail="bot_ids must be a list of strings")
    with db_connection() as conn:
        return clear(conn, bot_ids=bot_ids)
