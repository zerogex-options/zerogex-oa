"""Follower notification fan-out.

When a bot opens, closes, adds to, or cuts a position, this module writes
one row into ``tw_notifications_log`` for every follower of that bot whose
opt-in channels include ``in_app``. The in-app channel is polled by the
frontend at ``GET /api/tradeworkz/me/feed``; email / SMS / webhook channels
are stubbed as ``queued`` rows for a future delivery worker to consume.

The design deliberately keeps notification writes inside the same DB
transaction as the trade lifecycle event so a partial fan-out cannot end
up with a trade recorded but no notifications, or vice versa.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from src.tradeworkz import config as tw_config

logger = logging.getLogger(__name__)


def fanout_event(
    conn: Any,
    *,
    bot_id: str,
    event_type: str,
    payload: Dict[str, Any],
    trade_id: Optional[int] = None,
    position_id: Optional[int] = None,
    min_confidence: float = 0.0,
) -> int:
    """Write one notification row per opted-in follower.

    Returns the number of rows written. Silently short-circuits when
    :data:`tw_config.NOTIFY_ENABLED` is off.
    """
    if not tw_config.NOTIFY_ENABLED:
        return 0

    cur = conn.cursor()
    cur.execute(
        """
        SELECT end_user, channels, COALESCE(min_confidence, 0)
        FROM tw_bot_followers
        WHERE bot_id = %s
        """,
        (bot_id,),
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    written = 0
    now = datetime.now(timezone.utc)
    # Email dust suppression: for exit events where |realized_pnl| is
    # below the operator-tunable threshold AND the exit wasn't a
    # risk-off stop/wall_break, we skip inserting the email row entirely
    # (in_app / webhook are unaffected). This kills the "you got emailed
    # about a $1.20 close" complaint without ever hiding a real
    # stop-out. Threshold source: tw_config.EMAIL_DUST_THRESHOLD.
    dust_threshold = float(tw_config.EMAIL_DUST_THRESHOLD)
    payload_reason = str(payload.get("reason") or "").lower()
    is_riskoff = payload_reason in {"stop", "wall_break"}
    realized_pnl_val = payload.get("realized_pnl")
    try:
        realized_abs = abs(float(realized_pnl_val)) if realized_pnl_val is not None else None
    except (TypeError, ValueError):
        realized_abs = None
    email_dust_suppressed = (
        event_type == "exit"
        and dust_threshold > 0
        and realized_abs is not None
        and realized_abs < dust_threshold
        and not is_riskoff
    )

    for end_user, channels, follower_min in rows:
        if isinstance(channels, str):
            try:
                channels = json.loads(channels)
            except Exception:
                channels = {}
        channels = channels or {"in_app": True}
        effective_min = max(min_confidence, float(follower_min or 0.0))
        conf = float(payload.get("conviction") or 1.0)
        if conf < effective_min:
            continue
        for channel, enabled in channels.items():
            if not enabled:
                continue
            if channel == "email" and email_dust_suppressed:
                # Silently skip email; in_app row on this fanout still
                # writes so the user's bell / audit trail is complete.
                continue
            status = "sent" if channel == "in_app" else "queued"
            cur.execute(
                """
                INSERT INTO tw_notifications_log
                    (end_user, bot_id, event_type, trade_id, position_id,
                     channel, status, payload, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    end_user,
                    bot_id,
                    event_type,
                    trade_id,
                    position_id,
                    channel,
                    status,
                    json.dumps(payload),
                    now,
                ),
            )
            written += 1
    return written


def recent_feed(conn: Any, end_user: str, limit: int = 50) -> Iterable[Dict[str, Any]]:
    """Yield the caller's recent notification rows for the in-app feed."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT n.id, n.bot_id, b.display_name, n.event_type, n.trade_id,
               n.position_id, n.status, n.payload, n.sent_at
        FROM tw_notifications_log n
        LEFT JOIN tw_bots b ON b.id = n.bot_id
        WHERE n.end_user = %s AND n.channel = 'in_app'
        ORDER BY n.sent_at DESC
        LIMIT %s
        """,
        (end_user, limit),
    )
    for row in cur.fetchall():
        payload = row[7] or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        yield {
            "id": row[0],
            "bot_id": row[1],
            "bot_display_name": row[2],
            "event_type": row[3],
            "trade_id": row[4],
            "position_id": row[5],
            "status": row[6],
            "payload": payload,
            "sent_at": row[8].isoformat() if row[8] else None,
        }
