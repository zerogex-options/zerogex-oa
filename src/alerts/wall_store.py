"""Database side of wall alerts: read the tape and the walls, record events,
fan them out to members, and keep the detector's state.

Every function takes an open psycopg2 connection and owns no transaction
boundary, except where noted: the caller decides what commits together. The
one boundary that matters is :func:`record_and_fan_out` — an event and its
deliveries are written in the same transaction, so a crash can never leave an
alert recorded but undelivered, or delivered twice (the event's unique key
makes a retried insert a no-op).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from psycopg2.extras import execute_values

from src.alerts import config as alerts_config
from src.alerts.wall_detector import (
    ET,
    PROFILES,
    Bar,
    StreamKey,
    WallEvent,
    WallsInForce,
)
from src.alerts.wall_payload import build_payload, reportable_odds

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WallsRow:
    """One ``gex_summary`` row: the walls in force plus context for payloads."""

    walls: WallsInForce
    net_gex_at_spot: Optional[float]


@dataclass(frozen=True)
class TapeBar:
    """A ``underlying_quotes`` row: the bar plus when ingestion last wrote it."""

    bar: Bar
    updated_at: Optional[datetime]


@dataclass
class FanOutResult:
    event_id: Optional[int]
    duplicate: bool = False
    rules_matched: int = 0
    deliveries: int = 0
    filtered_by_strength: int = 0
    capped_users: List[str] = field(default_factory=list)


def _f(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def session_start_utc(session: date) -> datetime:
    """Midnight ET of ``session`` — the boundary daily counters reset at."""
    return datetime.combine(session, time(0, 0), tzinfo=ET)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


def load_walls(conn: Any, symbol: str, since: datetime, until: datetime) -> List[WallsRow]:
    """Published walls for ``symbol`` in ``[since, until]``, oldest first.

    Includes the last row BEFORE ``since`` too, so the wall in force at the
    start of the window is known (walls are a step function of their publish
    time, never interpolated).
    """
    cur = conn.cursor()
    cur.execute(
        """
        (
            SELECT timestamp, call_wall, put_wall, call_wall_strength,
                   put_wall_strength, wall_break_buffer, net_gex_at_spot
            FROM gex_summary
            WHERE underlying = %s AND timestamp < %s
            ORDER BY timestamp DESC
            LIMIT 1
        )
        UNION ALL
        (
            SELECT timestamp, call_wall, put_wall, call_wall_strength,
                   put_wall_strength, wall_break_buffer, net_gex_at_spot
            FROM gex_summary
            WHERE underlying = %s AND timestamp >= %s AND timestamp <= %s
        )
        ORDER BY timestamp ASC
        """,
        (symbol, since, symbol, since, until),
    )
    rows = []
    for ts, cw, pw, cws, pws, buf, net in cur.fetchall():
        rows.append(
            WallsRow(
                walls=WallsInForce(
                    ts=ts,
                    call_wall=_f(cw),
                    put_wall=_f(pw),
                    call_strength=_f(cws),
                    put_strength=_f(pws),
                    break_buffer=_f(buf),
                ),
                net_gex_at_spot=_f(net),
            )
        )
    return rows


def load_bars(conn: Any, symbol: str, since: datetime, until: datetime) -> List[TapeBar]:
    """1-minute bars for ``symbol`` stamped in ``[since, until)``, oldest first."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, high, low, close, updated_at
        FROM underlying_quotes
        WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
        ORDER BY timestamp ASC
        """,
        (symbol, since, until),
    )
    return [
        TapeBar(bar=Bar(ts=ts, high=float(h), low=float(lo), close=float(c)), updated_at=upd)
        for ts, h, lo, c, upd in cur.fetchall()
    ]


def load_odds(conn: Any, symbol: str, side: str) -> List[Dict[str, Any]]:
    """Reportable published break odds for one symbol and side (may be empty)."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT horizon_minutes, break_prob, ci_low, ci_high, n_tests, reportable
            FROM wall_break_stats
            WHERE underlying = %s AND side = %s
            """,
            (symbol, side),
        )
        rows = [
            {
                "horizon_minutes": h,
                "break_prob": p,
                "ci_low": lo,
                "ci_high": hi,
                "n_tests": n,
                "reportable": rep,
            }
            for h, p, lo, hi, n, rep in cur.fetchall()
        ]
    except Exception:  # pragma: no cover - table absent on a fresh box
        logger.debug("wall_break_stats lookup failed for %s/%s", symbol, side, exc_info=True)
        conn.rollback()
        return []
    return reportable_odds(rows, alerts_config.ODDS_HORIZONS)


# ---------------------------------------------------------------------------
# Detector state
# ---------------------------------------------------------------------------


def load_states(conn: Any, symbol: str, session: date) -> Dict[StreamKey, Dict[str, Any]]:
    """Saved detector state per (profile, touch tolerance) stream, today's only."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT profile, touch_bp, state
        FROM wall_alert_detector_state
        WHERE underlying = %s AND session_date = %s
        """,
        (symbol, session),
    )
    out: Dict[StreamKey, Dict[str, Any]] = {}
    for profile, touch_bp, state in cur.fetchall():
        if isinstance(state, str):
            try:
                state = json.loads(state)
            except ValueError:
                continue
        if isinstance(state, dict):
            out[(str(profile), int(touch_bp))] = state
    return out


def save_states(
    conn: Any, symbol: str, session: date, states: Mapping[StreamKey, Dict[str, Any]]
) -> None:
    if not states:
        return
    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO wall_alert_detector_state
            (underlying, profile, touch_bp, session_date, state, updated_at)
        VALUES %s
        ON CONFLICT (underlying, profile, touch_bp) DO UPDATE SET
            session_date = EXCLUDED.session_date,
            state = EXCLUDED.state,
            updated_at = EXCLUDED.updated_at
        """,
        [
            (symbol, profile, int(touch_bp), session, json.dumps(state), datetime.now(tz=ET))
            for (profile, touch_bp), state in states.items()
        ],
        template="(%s, %s, %s, %s, %s::jsonb, %s)",
    )


# ---------------------------------------------------------------------------
# Events and fan-out
# ---------------------------------------------------------------------------


def deliverable_channels() -> Tuple[str, ...]:
    """Channels a delivery row may be written for right now.

    A rule can carry a channel whose worker is switched off (SMS saved while
    the flag was on); queuing rows nobody drains would only build a backlog
    that fires the day the worker starts, so those are skipped at fan-out.
    """
    out = []
    for channel in alerts_config.CHANNELS:
        if channel == "sms" and not alerts_config.SMS_ENABLED:
            continue
        out.append(channel)
    return tuple(out)


def _channels(raw: Any) -> Dict[str, bool]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return {}
    if not isinstance(raw, dict):
        return {}
    return {str(k): bool(v) for k, v in raw.items()}


def select_targets(
    rules: Sequence[Tuple[int, str, Any, int]],
    *,
    strength_pctile: Optional[float],
    used_today: Mapping[str, int],
    daily_cap: int,
    channels: Sequence[str],
) -> Tuple[List[Tuple[int, str, str]], int, List[str]]:
    """Decide who gets an event, on which channels. Pure.

    :param rules: ``(rule_id, end_user, channels_json, min_strength_pctile)``
        for every enabled rule that wants this symbol, side, profile and kind.
    :returns: ``(targets, filtered_by_strength, capped_users)`` where targets
        are ``(rule_id, end_user, channel)``, at most one per (user, channel)
        however many of the member's rules matched — the first matching rule
        is credited.

    A rule with a strength floor only matches an event whose strength is
    KNOWN and at or above the floor: "only strong walls" must never be
    satisfied by "we could not tell". A rule with no floor matches either way.
    """
    targets: List[Tuple[int, str, str]] = []
    seen: set = set()
    filtered = 0
    capped: List[str] = []
    for rule_id, end_user, raw_channels, floor in rules:
        floor = int(floor or 0)
        if floor > 0 and (strength_pctile is None or strength_pctile < floor):
            filtered += 1
            continue
        if used_today.get(end_user, 0) >= daily_cap:
            if end_user not in capped:
                capped.append(end_user)
            continue
        wanted = _channels(raw_channels)
        for channel in channels:
            if not wanted.get(channel):
                continue
            key = (end_user, channel)
            if key in seen:
                continue
            seen.add(key)
            targets.append((int(rule_id), str(end_user), channel))
    return targets, filtered, capped


def record_and_fan_out(
    conn: Any,
    event: WallEvent,
    *,
    strength_pctile: Optional[float],
    net_gex_at_spot: Optional[float],
    current_wall: Optional[float],
    odds: Sequence[Dict[str, Any]],
    now: datetime,
) -> FanOutResult:
    """Insert one event and its deliveries. Commits nothing — see module doc."""
    payload = build_payload(
        event,
        strength_pctile=strength_pctile,
        net_gex_at_spot=net_gex_at_spot,
        current_wall=current_wall,
        odds=odds,
    )
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO wall_alert_events
            (underlying, session_date, profile, touch_bp, side, event_type, wall_strike,
             test_ordinal, bar_ts, price, strength, strength_pctile, payload, detected_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT ON CONSTRAINT wall_alert_events_uq DO NOTHING
        RETURNING id
        """,
        (
            event.symbol,
            event.session_date,
            event.profile,
            event.touch_bp,
            event.side,
            event.kind,
            event.wall,
            event.ordinal,
            event.bar_ts,
            event.price,
            event.strength,
            strength_pctile,
            json.dumps(payload),
            now,
        ),
    )
    row = cur.fetchone()
    if row is None:
        return FanOutResult(event_id=None, duplicate=True)
    event_id = int(row[0])
    result = FanOutResult(event_id=event_id)

    # The rules listening to this event's stream: same profile, and a touch
    # tolerance that resolves to the stream's (Auto = the profile's own band).
    cur.execute(
        """
        SELECT id, end_user, channels, min_strength_pctile
        FROM wall_alert_rules
        WHERE enabled
          AND underlying = %s
          AND profile = %s
          AND COALESCE(touch_bp, %s) = %s
          AND side IN (%s, 'both')
          AND COALESCE((events ->> %s)::boolean, false)
        ORDER BY id
        """,
        (
            event.symbol,
            event.profile,
            PROFILES[event.profile].touch_bp,
            event.touch_bp,
            event.side,
            event.kind,
        ),
    )
    rules = cur.fetchall()
    result.rules_matched = len(rules)
    if not rules:
        return result

    users = sorted({r[1] for r in rules})
    cur.execute(
        """
        SELECT end_user, COUNT(DISTINCT event_id)
        FROM wall_alert_deliveries
        WHERE end_user = ANY(%s) AND created_at >= %s
        GROUP BY end_user
        """,
        (users, session_start_utc(event.session_date)),
    )
    used_today = {u: int(n) for u, n in cur.fetchall()}

    targets, filtered, capped = select_targets(
        rules,
        strength_pctile=strength_pctile,
        used_today=used_today,
        daily_cap=alerts_config.DAILY_CAP_PER_USER,
        channels=deliverable_channels(),
    )
    result.filtered_by_strength = filtered
    result.capped_users = capped
    if capped:
        logger.warning(
            "wall alerts: daily cap (%d) reached for %d member(s); event %s not delivered to them",
            alerts_config.DAILY_CAP_PER_USER,
            len(capped),
            event_id,
        )
    if not targets:
        return result

    rows = []
    for rule_id, end_user, channel in targets:
        instant = alerts_config.CHANNELS.get(channel) == "instant"
        rows.append(
            (
                event_id,
                rule_id,
                end_user,
                channel,
                "sent" if instant else "queued",
                now if instant else None,
                now,
            )
        )
    execute_values(
        cur,
        """
        INSERT INTO wall_alert_deliveries
            (event_id, rule_id, end_user, channel, status, sent_at, created_at)
        VALUES %s
        ON CONFLICT ON CONSTRAINT wall_alert_deliveries_uq DO NOTHING
        """,
        rows,
    )
    result.deliveries = len(rows)
    cur.execute(
        "UPDATE wall_alert_rules SET last_triggered_at = %s WHERE id = ANY(%s)",
        (now, sorted({t[0] for t in targets})),
    )
    return result


def prune(conn: Any, retention_days: int) -> int:
    """Delete events (and, by cascade, deliveries) past the retention window."""
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM wall_alert_events WHERE detected_at < NOW() - make_interval(days => %s)",
        (int(retention_days),),
    )
    return cur.rowcount or 0


def walls_at(rows: Sequence[WallsRow], ts: datetime) -> Optional[WallsRow]:
    """The walls row in force at ``ts``: the last one stamped at or before it."""
    found: Optional[WallsRow] = None
    for row in rows:
        stamp = row.walls.ts
        if stamp is None or stamp > ts:
            break
        found = row
    return found


def session_bounds(session: date) -> Tuple[datetime, datetime]:
    """``[open, close)`` of the regular session on ``session``, ET-aware."""
    from src.market_calendar import regular_session_close

    open_ts = datetime.combine(session, time(9, 30), tzinfo=ET)
    close_ts = datetime.combine(session, regular_session_close(session), tzinfo=ET)
    return open_ts, close_ts
