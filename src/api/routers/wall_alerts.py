"""Wall alerts HTTP API — members' rules, their in-app feed, and the outbox.

Mounted at ``/api/alerts/walls``. Three audiences:

* **Members**, through the website BFF with an ``X-End-User-Token``: manage
  their rules (``/rules``), read and clear their in-app alerts (``/feed``),
  and preview what a filter would match against today's walls (``/live``).
  Every rule query is scoped to the token's end user; a rule id belonging to
  someone else is a 404, never a 403, so ids cannot be probed. The website
  gates the whole prefix at the Basic tier (``frontend/core/api/apiTierGate.ts``).
* **Anyone**: ``/config`` — the symbols, sensitivity profiles, channels and
  limits the form is built from, so the website never hard-codes a number the
  detector owns.
* **Delivery workers** (``/internal/*``): drain the email / SMS outbox and
  apply one-click unsubscribes. These carry other members' alerts, so they
  require the admin token on top of the API key (:func:`require_admin`) —
  the API key alone is held by every external integrator, and the website BFF
  forwards browser requests with a key of its own. ``internal`` is also a
  control-plane segment, so ``/api/v2`` does not mirror these routes.

The detector that produces the alerts is ``src/alerts/wall_alert_engine.py``;
the design is ``docs/design/wall-alerts.md``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request

from src.alerts import config as alerts_config
from src.alerts.wall_detector import DEFAULT_PROFILE, EVENT_TYPES, PROFILES
from src.alerts.wall_payload import STRENGTH_TIERS, regime_for, strength_tier
from src.api.database import DatabaseManager
from src.api.identity import ANONYMOUS, resolve_end_user
from src.api.security import require_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/alerts/walls", tags=["Wall Alerts"])

_INTERNAL_GATE = [Depends(require_admin)]

SIDES = ("call", "put", "both")
#: Strength-floor slider bounds. 100 is excluded: "stronger than every wall in
#: 30 days" is a filter that would almost never fire, which reads as broken.
MAX_STRENGTH_FLOOR = 95
#: Statuses a worker may report for a delivery row.
_MARK_STATUSES = ("sent", "failed", "retry", "cancelled")
#: Attempts after which a 'retry' becomes 'failed'.
MAX_DELIVERY_ATTEMPTS = 3
#: Queued alerts older than this are expired instead of sent: a "the wall was
#: hit" email that arrives an hour late is noise. Workers may ask for less.
DEFAULT_QUEUE_MAX_AGE_SECONDS = 1200
#: Unread count window — an account nobody has opened in months should not
#: show a four-digit badge.
_UNREAD_WINDOW_DAYS = 7


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


def _require_user(request: Request) -> str:
    user = _resolve_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Sign in to manage wall alerts")
    return user


def _json(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return {}
    return raw if isinstance(raw, dict) else {}


def _f(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def channel_available(channel: str) -> bool:
    """Whether members may switch ``channel`` on today."""
    if channel not in alerts_config.CHANNELS:
        return False
    if channel == "sms":
        return alerts_config.SMS_ENABLED
    return True


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _bad(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def _validate_events(raw: Any) -> Dict[str, bool]:
    if not isinstance(raw, dict):
        raise _bad('events must be an object like {"hit": true, "break": true, "hold": false}')
    unknown = sorted(set(raw) - set(EVENT_TYPES))
    if unknown:
        raise _bad(f"unknown event(s): {', '.join(unknown)}")
    events = {kind: bool(raw.get(kind, False)) for kind in EVENT_TYPES}
    if not any(events.values()):
        raise _bad("choose at least one event to be alerted about")
    return events


def _validate_channels(raw: Any) -> Dict[str, bool]:
    if not isinstance(raw, dict):
        raise _bad('channels must be an object like {"in_app": true, "email": false}')
    unknown = sorted(set(raw) - set(alerts_config.CHANNELS))
    if unknown:
        raise _bad(f"unknown channel(s): {', '.join(unknown)}")
    channels = {name: bool(raw.get(name, False)) for name in alerts_config.CHANNELS}
    for name, on in channels.items():
        if on and not channel_available(name):
            raise _bad(f"{name} alerts are not available yet")
    if not any(channels.values()):
        raise _bad("choose at least one way to be alerted")
    return channels


def validate_rule(body: Dict[str, Any], *, partial: bool = False) -> Dict[str, Any]:
    """Normalize a rule body; raises 422 with a member-readable reason.

    ``partial`` (PATCH) validates only the fields present.
    """
    if not isinstance(body, dict):
        raise _bad("body must be a JSON object")
    out: Dict[str, Any] = {}

    if "symbol" in body or not partial:
        symbol = str(body.get("symbol") or "").strip().upper()
        if symbol not in alerts_config.symbols():
            raise _bad(
                f"symbol must be one of {', '.join(alerts_config.symbols())}"
                + (" (for ES / NQ, set the alert on SPX / NDX)" if symbol in ("ES", "NQ") else "")
            )
        out["symbol"] = symbol
    if "side" in body or not partial:
        side = str(body.get("side") or "").strip().lower()
        if side not in SIDES:
            raise _bad("side must be call, put or both")
        out["side"] = side
    if "events" in body or not partial:
        out["events"] = _validate_events(
            body.get("events", {"hit": True, "break": True, "hold": False})
        )
    if "min_strength_pctile" in body or not partial:
        raw = body.get("min_strength_pctile", 0)
        try:
            floor = int(round(float(raw if raw is not None else 0)))
        except (TypeError, ValueError):
            raise _bad("min_strength_pctile must be a number from 0 to 95")
        if floor < 0 or floor > MAX_STRENGTH_FLOOR:
            raise _bad("min_strength_pctile must be from 0 to 95")
        out["min_strength_pctile"] = floor
    if "profile" in body or not partial:
        profile = str(body.get("profile") or DEFAULT_PROFILE).strip().lower()
        if profile not in PROFILES:
            raise _bad(f"profile must be one of {', '.join(PROFILES)}")
        out["profile"] = profile
    if "channels" in body or not partial:
        out["channels"] = _validate_channels(body.get("channels", {"in_app": True}))
    if "enabled" in body or not partial:
        enabled = body.get("enabled", True)
        if not isinstance(enabled, bool):
            raise _bad("enabled must be true or false")
        out["enabled"] = enabled
    if partial and not out:
        raise _bad("nothing to update")
    return out


def _rule_out(row: Any) -> Dict[str, Any]:
    channels = _json(row["channels"])
    return {
        "id": int(row["id"]),
        "symbol": row["underlying"],
        "side": row["side"],
        "events": {k: bool(_json(row["events"]).get(k, False)) for k in EVENT_TYPES},
        "min_strength_pctile": int(row["min_strength_pctile"]),
        "profile": row["profile"],
        "channels": {k: bool(channels.get(k, False)) for k in alerts_config.CHANNELS},
        "enabled": bool(row["enabled"]),
        "created_at": _iso(row["created_at"]),
        "updated_at": _iso(row["updated_at"]),
        "last_triggered_at": _iso(row["last_triggered_at"]),
    }


_RULE_COLUMNS = (
    "id, underlying, side, events, min_strength_pctile, profile, channels, "
    "enabled, created_at, updated_at, last_triggered_at"
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@router.get("/config")
async def wall_alert_config() -> Dict[str, Any]:
    """Everything the alert form is built from."""
    return {
        "symbols": list(alerts_config.symbols()),
        "sides": list(SIDES),
        "events": list(EVENT_TYPES),
        "profiles": [PROFILES[name].as_dict() for name in PROFILES],
        "default_profile": DEFAULT_PROFILE,
        "channels": [
            {
                "id": name,
                "available": channel_available(name),
                "delivery": alerts_config.CHANNELS[name],
            }
            for name in alerts_config.CHANNELS
        ],
        "strength_tiers": [{"min_pctile": floor, "id": key} for floor, key in STRENGTH_TIERS],
        "max_strength_floor": MAX_STRENGTH_FLOOR,
        "max_rules": alerts_config.MAX_RULES_PER_USER,
        "daily_cap": alerts_config.DAILY_CAP_PER_USER,
        "session": "regular",
    }


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


@router.get("/rules")
async def list_rules(request: Request, db: DatabaseManager = Depends(get_db)) -> Dict[str, Any]:
    user = _resolve_user(request)
    if user is None:
        return {"rules": [], "signed_in": False}
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {_RULE_COLUMNS} FROM wall_alert_rules
            WHERE end_user = $1 ORDER BY created_at, id
            """,
            user,
        )
    return {"rules": [_rule_out(r) for r in rows], "signed_in": True}


@router.post("/rules", status_code=201)
async def create_rule(
    request: Request,
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    user = _require_user(request)
    rule = validate_rule(body)
    async with db.pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM wall_alert_rules WHERE end_user = $1", user
        )
        if int(count or 0) >= alerts_config.MAX_RULES_PER_USER:
            raise HTTPException(
                status_code=409,
                detail=f"You can have up to {alerts_config.MAX_RULES_PER_USER} wall alerts. "
                "Delete one to add another.",
            )
        row = await conn.fetchrow(
            f"""
            INSERT INTO wall_alert_rules
                (end_user, underlying, side, events, min_strength_pctile, profile,
                 channels, enabled)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::jsonb, $8)
            RETURNING {_RULE_COLUMNS}
            """,
            user,
            rule["symbol"],
            rule["side"],
            json.dumps(rule["events"]),
            rule["min_strength_pctile"],
            rule["profile"],
            json.dumps(rule["channels"]),
            rule["enabled"],
        )
    return {"rule": _rule_out(row)}


@router.patch("/rules/{rule_id}")
async def update_rule(
    request: Request,
    rule_id: int = Path(..., ge=1),
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    user = _require_user(request)
    patch = validate_rule(body, partial=True)
    sets: List[str] = []
    args: List[Any] = [rule_id, user]
    columns = {
        "symbol": ("underlying", None),
        "side": ("side", None),
        "events": ("events", "jsonb"),
        "min_strength_pctile": ("min_strength_pctile", None),
        "profile": ("profile", None),
        "channels": ("channels", "jsonb"),
        "enabled": ("enabled", None),
    }
    for key, value in patch.items():
        column, cast = columns[key]
        args.append(json.dumps(value) if cast == "jsonb" else value)
        sets.append(f"{column} = ${len(args)}{'::jsonb' if cast else ''}")
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"""
                UPDATE wall_alert_rules
                SET {', '.join(sets)}, updated_at = NOW()
                WHERE id = $1 AND end_user = $2
                RETURNING {_RULE_COLUMNS}
                """,
                *args,
            )
            if row is None:
                raise HTTPException(status_code=404, detail="Alert not found")
            # Switching a channel off — or the whole rule — must stop mail that
            # already queued from it, not only future events.
            cancelled = await _cancel_queued_for_rule(conn, row)
    return {"rule": _rule_out(row), "cancelled_queued": cancelled}


async def _cancel_queued_for_rule(conn: Any, row: Any) -> int:
    channels = _json(row["channels"])
    off = [c for c in alerts_config.CHANNELS if not (row["enabled"] and channels.get(c))]
    if not off:
        return 0
    result = await conn.execute(
        """
        UPDATE wall_alert_deliveries
        SET status = 'cancelled', error = 'turned off by member'
        WHERE rule_id = $1 AND status = 'queued' AND channel = ANY($2::text[])
        """,
        int(row["id"]),
        off,
    )
    return _rows_affected(result)


@router.delete("/rules/{rule_id}")
async def delete_rule(
    request: Request,
    rule_id: int = Path(..., ge=1),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    user = _require_user(request)
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            # Cancel first: deleting the rule nulls rule_id on its deliveries
            # (ON DELETE SET NULL), after which they can no longer be found.
            cancelled = await conn.execute(
                """
                UPDATE wall_alert_deliveries d
                SET status = 'cancelled', error = 'alert deleted by member'
                FROM wall_alert_rules r
                WHERE d.rule_id = r.id AND r.id = $1 AND r.end_user = $2
                  AND d.status = 'queued'
                """,
                rule_id,
                user,
            )
            deleted = await conn.execute(
                "DELETE FROM wall_alert_rules WHERE id = $1 AND end_user = $2",
                rule_id,
                user,
            )
    if _rows_affected(deleted) == 0:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"status": "ok", "id": rule_id, "cancelled_queued": _rows_affected(cancelled)}


def _rows_affected(command_tag: Any) -> int:
    if isinstance(command_tag, str):
        parts = command_tag.split()
        if parts:
            try:
                return int(parts[-1])
            except ValueError:
                return 0
    return 0


# ---------------------------------------------------------------------------
# In-app feed
# ---------------------------------------------------------------------------


def _feed_entry(row: Any) -> Dict[str, Any]:
    payload = _json(row["payload"])
    return {
        "id": int(row["id"]),
        "event_id": int(row["event_id"]),
        "rule_id": None if row["rule_id"] is None else int(row["rule_id"]),
        "created_at": _iso(row["created_at"]),
        "read": row["read_at"] is not None,
        "symbol": row["underlying"],
        "side": row["side"],
        "kind": row["event_type"],
        "wall": _f(row["wall_strike"]),
        "price": _f(row["price"]),
        "payload": payload,
    }


@router.get("/feed")
async def my_feed(
    request: Request,
    limit: int = Query(default=30, ge=1, le=100),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    user = _resolve_user(request)
    if user is None:
        return {"entries": [], "unread": 0, "signed_in": False}
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT d.id, d.event_id, d.rule_id, d.created_at, d.read_at,
                   e.underlying, e.side, e.event_type, e.wall_strike, e.price, e.payload
            FROM wall_alert_deliveries d
            JOIN wall_alert_events e ON e.id = d.event_id
            WHERE d.end_user = $1 AND d.channel = 'in_app'
            ORDER BY d.created_at DESC, d.id DESC
            LIMIT $2
            """,
            user,
            limit,
        )
        unread = await conn.fetchval(
            """
            SELECT COUNT(*) FROM wall_alert_deliveries
            WHERE end_user = $1 AND channel = 'in_app' AND read_at IS NULL
              AND created_at > NOW() - make_interval(days => $2)
            """,
            user,
            _UNREAD_WINDOW_DAYS,
        )
    return {
        "entries": [_feed_entry(r) for r in rows],
        "unread": int(unread or 0),
        "signed_in": True,
    }


@router.post("/feed/read")
async def mark_feed_read(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Mark the caller's in-app alerts read: ``{"ids": [...]}`` or everything."""
    user = _require_user(request)
    ids = body.get("ids") if isinstance(body, dict) else None
    async with db.pool.acquire() as conn:
        if ids is None:
            result = await conn.execute(
                """
                UPDATE wall_alert_deliveries SET read_at = NOW()
                WHERE end_user = $1 AND channel = 'in_app' AND read_at IS NULL
                """,
                user,
            )
        else:
            if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
                raise _bad("ids must be a list of integers")
            result = await conn.execute(
                """
                UPDATE wall_alert_deliveries SET read_at = NOW()
                WHERE end_user = $1 AND channel = 'in_app' AND read_at IS NULL
                  AND id = ANY($2::bigint[])
                """,
                user,
                ids[:500],
            )
    return {"status": "ok", "marked": _rows_affected(result)}


@router.delete("/feed")
async def clear_feed(request: Request, db: DatabaseManager = Depends(get_db)) -> Dict[str, Any]:
    """Delete the caller's in-app alerts. Queued email is left alone."""
    user = _require_user(request)
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM wall_alert_deliveries WHERE end_user = $1 AND channel = 'in_app'",
            user,
        )
    return {"status": "ok", "deleted": _rows_affected(result)}


# ---------------------------------------------------------------------------
# Live preview
# ---------------------------------------------------------------------------


def _side_view(
    strike: Optional[float],
    strength: Optional[float],
    pctile: Optional[float],
    price: Optional[float],
    side: str,
) -> Optional[Dict[str, Any]]:
    if strike is None:
        return None
    distance = None
    distance_pct = None
    if price is not None and price > 0:
        distance = strike - price if side == "call" else price - strike
        distance_pct = distance / price * 100.0
    return {
        "strike": strike,
        "strength": strength,
        "strength_pctile": None if pctile is None else round(pctile, 1),
        "strength_tier": strength_tier(pctile),
        # Signed toward the approach side: >0 still ahead of price.
        "distance": distance,
        "distance_pct": distance_pct,
    }


@router.get("/live")
async def live_walls(
    symbols: Optional[str] = Query(default=None, description="Comma-separated; default all"),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Today's walls with their strength percentile — what a filter would see now."""
    from src.tradeworkz.gex_stats import percentile_rank, tod_bucket_for

    allowed = alerts_config.symbols()
    wanted = (
        [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else list(allowed)
    )
    wanted = [s for s in wanted if s in allowed]
    if not wanted:
        return {"as_of": _iso(datetime.now(timezone.utc)), "symbols": []}

    async with db.pool.acquire() as conn:
        walls = await conn.fetch(
            """
            SELECT s.sym, g.timestamp, g.call_wall, g.put_wall, g.call_wall_strength,
                   g.put_wall_strength, g.net_gex_at_spot
            FROM unnest($1::text[]) AS s(sym)
            CROSS JOIN LATERAL (
                SELECT timestamp, call_wall, put_wall, call_wall_strength,
                       put_wall_strength, net_gex_at_spot
                FROM gex_summary
                WHERE underlying = s.sym
                ORDER BY timestamp DESC
                LIMIT 1
            ) g
            """,
            wanted,
        )
        quotes = await conn.fetch(
            """
            SELECT s.sym, q.timestamp, q.close
            FROM unnest($1::text[]) AS s(sym)
            CROSS JOIN LATERAL (
                SELECT timestamp, close FROM underlying_quotes
                WHERE symbol = s.sym
                ORDER BY timestamp DESC
                LIMIT 1
            ) q
            """,
            wanted,
        )
        buckets: Dict[str, int] = {}
        for row in walls:
            buckets[row["sym"]] = tod_bucket_for(row["timestamp"]) if row["timestamp"] else -1
        knots = await conn.fetch(
            """
            SELECT underlying, metric, tod_bucket, p05, p25, p50, p75, p95
            FROM gex_historical_stats
            WHERE underlying = ANY($1::text[])
              AND metric IN ('call_wall_strength', 'put_wall_strength')
              AND window_label = '30d'
              AND tod_bucket = ANY($2::smallint[])
            """,
            wanted,
            sorted(set(list(buckets.values()) + [-1])),
        )

    dist: Dict[Tuple[str, str], Tuple[int, Tuple[Any, ...]]] = {}
    for k in knots:
        sym = k["underlying"]
        side = "call" if k["metric"] == "call_wall_strength" else "put"
        bucket = int(k["tod_bucket"])
        want = buckets.get(sym, -1)
        if bucket not in (want, -1):
            continue
        rank = 0 if bucket == want else 1
        current = dist.get((sym, side))
        if current is None or rank < current[0]:
            dist[(sym, side)] = (rank, (k["p05"], k["p25"], k["p50"], k["p75"], k["p95"]))

    def pctile(sym: str, side: str, strength: Optional[float]) -> Optional[float]:
        entry = dist.get((sym, side))
        if strength is None or entry is None:
            return None
        return percentile_rank(strength, *entry[1])

    price_by = {q["sym"]: (q["timestamp"], _f(q["close"])) for q in quotes}
    now = datetime.now(timezone.utc)
    out = []
    for row in walls:
        sym = row["sym"]
        price_ts, price = price_by.get(sym, (None, None))
        cs, ps = _f(row["call_wall_strength"]), _f(row["put_wall_strength"])
        walls_ts = row["timestamp"]
        stale = walls_ts is None or (now - walls_ts) > timedelta(
            seconds=alerts_config.MAX_WALLS_AGE_SECONDS
        )
        out.append(
            {
                "symbol": sym,
                "walls_ts": _iso(walls_ts),
                "price": price,
                "price_ts": _iso(price_ts),
                "stale": stale,
                "regime": regime_for(_f(row["net_gex_at_spot"])),
                "call": _side_view(
                    _f(row["call_wall"]), cs, pctile(sym, "call", cs), price, "call"
                ),
                "put": _side_view(_f(row["put_wall"]), ps, pctile(sym, "put", ps), price, "put"),
            }
        )
    out.sort(key=lambda r: wanted.index(r["symbol"]))
    return {"as_of": _iso(now), "symbols": out}


# ---------------------------------------------------------------------------
# Internal — delivery workers
# ---------------------------------------------------------------------------


@router.get("/internal/queued", dependencies=_INTERNAL_GATE)
async def internal_queued(
    channel: str = Query(..., pattern="^(email|sms)$"),
    limit: int = Query(default=200, ge=1, le=1000),
    max_age_seconds: int = Query(default=DEFAULT_QUEUE_MAX_AGE_SECONDS, ge=60, le=86400),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Queued deliveries for one channel, oldest first, still wanted.

    Two guards run before anything is handed out:

    * rows older than ``max_age_seconds`` are expired ('cancelled'), because a
      wall alert is only worth sending while it is still news;
    * a row is only returned while its rule still exists, is enabled and has
      this channel on — the delivery-time re-check behind "I turned email off
      but it still came". Rows that fail it are cancelled, not left queued.
    """
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            expired = await conn.execute(
                """
                UPDATE wall_alert_deliveries
                SET status = 'cancelled', error = 'expired before delivery'
                WHERE status = 'queued' AND channel = $1
                  AND created_at < NOW() - make_interval(secs => $2)
                """,
                channel,
                max_age_seconds,
            )
            orphaned = await conn.execute(
                """
                UPDATE wall_alert_deliveries d
                SET status = 'cancelled', error = 'alert turned off or deleted'
                WHERE d.status = 'queued' AND d.channel = $1
                  AND NOT EXISTS (
                      SELECT 1 FROM wall_alert_rules r
                      WHERE r.id = d.rule_id AND r.enabled
                        AND COALESCE((r.channels ->> d.channel)::boolean, false)
                  )
                """,
                channel,
            )
            rows = await conn.fetch(
                """
                SELECT d.id, d.end_user, d.channel, d.attempts, d.created_at, d.rule_id,
                       e.id AS event_id, e.underlying, e.side, e.event_type,
                       e.wall_strike, e.price, e.payload
                FROM wall_alert_deliveries d
                JOIN wall_alert_events e ON e.id = d.event_id
                WHERE d.status = 'queued' AND d.channel = $1
                ORDER BY d.created_at ASC, d.id ASC
                LIMIT $2
                """,
                channel,
                limit,
            )
    entries = []
    for r in rows:
        entries.append(
            {
                "id": int(r["id"]),
                "end_user": r["end_user"],
                "channel": r["channel"],
                "attempts": int(r["attempts"]),
                "created_at": _iso(r["created_at"]),
                "rule_id": None if r["rule_id"] is None else int(r["rule_id"]),
                "event_id": int(r["event_id"]),
                "symbol": r["underlying"],
                "side": r["side"],
                "kind": r["event_type"],
                "wall": _f(r["wall_strike"]),
                "price": _f(r["price"]),
                "payload": _json(r["payload"]),
            }
        )
    return {
        "entries": entries,
        "expired": _rows_affected(expired),
        "cancelled": _rows_affected(orphaned),
    }


@router.post("/internal/mark", dependencies=_INTERNAL_GATE)
async def internal_mark(
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Record a worker's outcome for one or more queued rows.

    ``status``: ``sent`` | ``failed`` | ``cancelled`` | ``retry``. ``retry``
    keeps the rows queued with one more attempt counted, and turns into
    ``failed`` once a row has used :data:`MAX_DELIVERY_ATTEMPTS`. Only rows
    still ``queued`` move, so a late or repeated call cannot flip a row a
    member's unsubscribe already cancelled back to ``sent``.
    """
    ids = body.get("ids")
    status = body.get("status")
    error = body.get("error")
    if not isinstance(ids, list) or not ids or not all(isinstance(i, int) for i in ids):
        raise _bad("ids must be a non-empty list of integers")
    if status not in _MARK_STATUSES:
        raise _bad(f"status must be one of {', '.join(_MARK_STATUSES)}")
    if error is not None:
        error = str(error)[:500]
    async with db.pool.acquire() as conn:
        if status == "retry":
            result = await conn.execute(
                """
                UPDATE wall_alert_deliveries
                SET attempts = attempts + 1,
                    error = $2,
                    status = CASE WHEN attempts + 1 >= $3 THEN 'failed' ELSE 'queued' END
                WHERE id = ANY($1::bigint[]) AND status = 'queued'
                """,
                ids,
                error,
                MAX_DELIVERY_ATTEMPTS,
            )
        else:
            result = await conn.execute(
                """
                UPDATE wall_alert_deliveries
                SET status = $2::text,
                    error = $3,
                    attempts = attempts + 1,
                    sent_at = CASE WHEN $2::text = 'sent' THEN NOW() ELSE sent_at END
                WHERE id = ANY($1::bigint[]) AND status = 'queued'
                """,
                ids,
                status,
                error,
            )
    return {"status": "ok", "updated": _rows_affected(result)}


@router.post("/internal/disable-channel", dependencies=_INTERNAL_GATE)
async def internal_disable_channel(
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Turn one channel off on every rule a member has (one-click unsubscribe)."""
    end_user = body.get("end_user")
    channel = body.get("channel")
    if not isinstance(end_user, str) or not end_user.strip():
        raise _bad("end_user is required")
    if channel not in alerts_config.CHANNELS or channel == "in_app":
        raise _bad("channel must be email or sms")
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            rules = await conn.execute(
                """
                UPDATE wall_alert_rules
                SET channels = channels || jsonb_build_object($2::text, false),
                    updated_at = NOW()
                WHERE end_user = $1 AND COALESCE((channels ->> $2)::boolean, false)
                """,
                end_user,
                channel,
            )
            cancelled = await conn.execute(
                """
                UPDATE wall_alert_deliveries
                SET status = 'cancelled', error = 'unsubscribed'
                WHERE end_user = $1 AND channel = $2 AND status = 'queued'
                """,
                end_user,
                channel,
            )
    return {
        "status": "ok",
        "rules_updated": _rows_affected(rules),
        "cancelled_queued": _rows_affected(cancelled),
    }


@router.post("/internal/purge-user", dependencies=_INTERNAL_GATE)
async def internal_purge_user(
    body: Dict[str, Any] = Body(...),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    """Remove a deleted account's rules and alerts.

    Called when the website deletes an account, and by the email worker when
    a queued row's member no longer exists. Events stay (they are not
    personal); every row tying them to this member goes.
    """
    end_user = body.get("end_user")
    if not isinstance(end_user, str) or not end_user.strip():
        raise _bad("end_user is required")
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            deliveries = await conn.execute(
                "DELETE FROM wall_alert_deliveries WHERE end_user = $1", end_user
            )
            rules = await conn.execute("DELETE FROM wall_alert_rules WHERE end_user = $1", end_user)
    return {
        "status": "ok",
        "rules_deleted": _rows_affected(rules),
        "deliveries_deleted": _rows_affected(deliveries),
    }
