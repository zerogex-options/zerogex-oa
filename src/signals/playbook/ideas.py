"""A pattern's current idea on a symbol: the input to "one Card per idea".

Most patterns trigger on a condition that stays true for the whole move
("price is above the opening range", "dealer-delta pressure above 30"). The
engine re-checks every second, and the only brake used to be a short dwell
(5 minutes for 0DTE), so a pattern re-issued the same idea every few minutes
for as long as the move lasted, each Card at a worse price than the one
before. The later Cards were the ones that arrived after the move and just
before it turned.

``PlaybookEngine`` already had the rule to stop that (no new entry while the
pattern's last position is inside its hold window) but was never told about
any position. This module supplies them: the most recent idea per pattern on
the symbol, taken from published Cards (``signal_action_cards``) and from
ideas the adaptive gate held back (``playbook_card_outcomes`` rows with no
``card_id``), with the grader's verdict so far.

Everything here is best-effort. If ``playbook_card_outcomes`` is missing
(schema not applied yet) the loader falls back to published Cards alone, and
the writer skips, so the signal cycle never breaks.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Iterable, Optional

import pytz

from src import config
from src.signals.playbook.context import OpenPosition

logger = logging.getLogger(__name__)

# Longest hold any pattern uses is 3 days; 5 calendar days covers a weekend.
LOOKBACK_DAYS = 5

# Seconds to stop touching playbook_card_outcomes after finding it missing, so a
# service restarted before `make schema-apply` doesn't error every second.
_MISSING_TABLE_BACKOFF_SECONDS = 600
_table_missing_until = 0.0
_last_write_warning = 0.0


def _placeholders(style: str) -> tuple[str, str]:
    return ("$1", "$2") if style == "asyncpg" else ("%s", "%s")


def open_ideas_sql(style: str = "psycopg2") -> str:
    """Latest idea per pattern: published Cards (with their grade so far) plus
    ideas the gate held back. Two parameters, both the symbol."""
    p1, p2 = _placeholders(style)
    return f"""
        SELECT DISTINCT ON (pattern)
               pattern, direction, action, issued_at, max_hold, outcome
        FROM (
            SELECT c.pattern, c.direction, c.action, c.timestamp AS issued_at,
                   c.payload->>'max_hold_minutes' AS max_hold, o.outcome
            FROM signal_action_cards c
            LEFT JOIN playbook_card_outcomes o ON o.card_id = c.id
            WHERE c.underlying = {p1}
              AND c.timestamp > NOW() - INTERVAL '{LOOKBACK_DAYS} days'
              AND c.action <> 'STAND_DOWN'
            UNION ALL
            SELECT o.pattern, o.direction, o.action, o.issued_at,
                   o.max_hold_minutes::text AS max_hold, o.outcome
            FROM playbook_card_outcomes o
            WHERE o.underlying = {p2}
              AND o.card_id IS NULL
              AND o.issued_at > NOW() - INTERVAL '{LOOKBACK_DAYS} days'
        ) ideas
        ORDER BY pattern, issued_at DESC
    """


def published_only_sql(style: str = "psycopg2") -> str:
    """Fallback when playbook_card_outcomes doesn't exist yet. One parameter."""
    p1, _ = _placeholders(style)
    return f"""
        SELECT DISTINCT ON (pattern)
               pattern, direction, action, timestamp AS issued_at,
               payload->>'max_hold_minutes' AS max_hold, NULL AS outcome
        FROM signal_action_cards
        WHERE underlying = {p1}
          AND timestamp > NOW() - INTERVAL '{LOOKBACK_DAYS} days'
          AND action <> 'STAND_DOWN'
        ORDER BY pattern, timestamp DESC
    """


def outcomes_table_available() -> bool:
    return time.time() >= _table_missing_until


def note_table_missing() -> None:
    global _table_missing_until
    _table_missing_until = time.time() + _MISSING_TABLE_BACKOFF_SECONDS


def is_missing_table_error(exc: BaseException) -> bool:
    """True for "relation does not exist" from psycopg2 (pgcode) or asyncpg
    (sqlstate), without importing either driver."""
    code = getattr(exc, "pgcode", None) or getattr(exc, "sqlstate", None)
    return code == "42P01" or type(exc).__name__ == "UndefinedTableError"


def _parse_hold(raw: Any) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _aware(ts: Any) -> Optional[datetime]:
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(ts, datetime):
        return None
    if ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    return ts


def open_positions_from_rows(rows: Iterable, underlying: str) -> list[OpenPosition]:
    """Rows of (pattern, direction, action, issued_at, max_hold, outcome),
    as tuples or mappings, to the engine's OpenPosition list."""
    out: list[OpenPosition] = []
    for row in rows or ():
        if isinstance(row, dict) or hasattr(row, "keys"):
            pattern, direction, action = row["pattern"], row["direction"], row["action"]
            issued_at, max_hold, outcome = row["issued_at"], row["max_hold"], row["outcome"]
        else:
            pattern, direction, action, issued_at, max_hold, outcome = row
        opened_at = _aware(issued_at)
        if not pattern or opened_at is None:
            continue
        out.append(
            OpenPosition(
                pattern_id=str(pattern),
                direction=str(direction or ""),
                instrument=str(action or ""),
                opened_at=opened_at,
                underlying=underlying,
                max_hold_minutes=_parse_hold(max_hold),
                status=str(outcome or "pending"),
            )
        )
    return out


def _rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001
        pass


def load_open_ideas_sync(conn, underlying: str) -> list[OpenPosition]:
    """psycopg2: the latest idea per pattern on ``underlying``. Never raises."""
    if not config.PLAYBOOK_ONE_CARD_PER_IDEA or conn is None or not underlying:
        return []
    if outcomes_table_available():
        try:
            cur = conn.cursor()
            cur.execute(open_ideas_sql(), (underlying, underlying))
            return open_positions_from_rows(cur.fetchall(), underlying)
        except Exception as exc:  # noqa: BLE001
            _rollback(conn)
            if is_missing_table_error(exc):
                note_table_missing()
            logger.debug("load_open_ideas_sync failed (%s): %s", underlying, exc)
    try:
        cur = conn.cursor()
        cur.execute(published_only_sql(), (underlying,))
        return open_positions_from_rows(cur.fetchall(), underlying)
    except Exception as exc:  # noqa: BLE001
        _rollback(conn)
        logger.debug("load_open_ideas_sync fallback failed (%s): %s", underlying, exc)
        return []


# A target/stop ``ref_price`` more than this fraction away from the entry is
# not read as an underlying price (it could only be an option premium).
_MAX_LEVEL_DISTANCE = 0.20


def _level(section: Any, entry_price: Optional[float]) -> Optional[float]:
    """A target/stop as an underlying price, or None.

    ``kind == "level"`` is a price by definition. Any other kind still counts
    when it carries a price near the entry: call_wall_fade and put_wall_bounce
    label their stop ``premium_pct`` but set it to the wall price the catalog
    names ("close above call_wall x 1.003"), and the Card page shows it as the
    stop price. Grading them as if they had no stop meant they could never
    lose. A value far from the entry can only be a premium and is ignored.
    """
    if not isinstance(section, dict):
        return None
    value = section.get("ref_price")
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    if section.get("kind") == "level":
        return float(value)
    if entry_price and abs(float(value) / entry_price - 1.0) <= _MAX_LEVEL_DISTANCE:
        return float(value)
    return None


def idea_levels(card: dict[str, Any]) -> dict[str, Any]:
    """Entry / target / stop / hold from a Card dict, for playbook_card_outcomes."""
    entry = card.get("entry") or {}
    raw_entry = entry.get("ref_price") if isinstance(entry, dict) else None
    entry_price = float(raw_entry) if isinstance(raw_entry, (int, float)) else None
    return {
        "entry_price": entry_price,
        "entry_trigger": (str(entry.get("trigger") or "")[:16] if isinstance(entry, dict) else ""),
        "target_price": _level(card.get("target"), entry_price),
        "stop_price": _level(card.get("stop"), entry_price),
        "max_hold_minutes": _parse_hold(card.get("max_hold_minutes")),
    }


INSERT_HELD_BACK_SQL = """
    INSERT INTO playbook_card_outcomes
        (card_id, underlying, pattern, action, tier, direction, confidence,
         issued_at, held_back_reason, entry_price, entry_trigger,
         target_price, stop_price, max_hold_minutes, outcome)
    VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
    ON CONFLICT DO NOTHING
"""


def insert_held_back_idea_sync(conn, card: dict[str, Any], reason: str) -> None:
    """Record an idea the adaptive gate held back, so the grader grades it.

    This is what lets a paused pattern earn its way back. Best-effort; never
    raises.
    """
    global _last_write_warning
    if conn is None or not card or not outcomes_table_available():
        return
    ts = _aware(card.get("timestamp"))
    underlying = card.get("underlying")
    if ts is None or not underlying or not card.get("pattern"):
        return
    levels = idea_levels(card)
    try:
        cur = conn.cursor()
        cur.execute(
            INSERT_HELD_BACK_SQL,
            (
                underlying,
                card.get("pattern"),
                card.get("action"),
                card.get("tier") or "n/a",
                card.get("direction") or "non_directional",
                float(card.get("confidence") or 0.0),
                ts,
                (reason or "")[:500],
                levels["entry_price"],
                levels["entry_trigger"],
                levels["target_price"],
                levels["stop_price"],
                levels["max_hold_minutes"],
            ),
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        _rollback(conn)
        if is_missing_table_error(exc):
            note_table_missing()
        # One line per ten minutes at most: this runs every cycle.
        now = time.time()
        if now - _last_write_warning > _MISSING_TABLE_BACKOFF_SECONDS:
            _last_write_warning = now
            logger.warning(
                "insert_held_back_idea_sync failed (%s %s): %s",
                underlying,
                card.get("pattern"),
                exc,
            )
