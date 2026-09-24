"""Grade every Playbook idea against what the underlying did after it.

This is the check-back the engine never had. Each published Action Card, and
each idea the adaptive gate held back, becomes a row in
``playbook_card_outcomes``. Once the underlying reaches the Card's target or
stop, or its hold window runs out, the row is graded:

* ``outcome``: ``target_hit`` / ``stop_hit`` / ``time_exit`` (graded), or
  ``no_fill`` (a touch/break entry never triggered), ``no_data``,
  ``unresolved`` (no price-level exits, or a non-directional Card),
  ``off_session`` (a Card issued outside the regular session by the engine
  before it had a session gate), and ``mispriced`` (an at-market Card whose
  quoted price was nowhere near the market that minute, as every Card built by
  the API before 2026-09-24 was: it quoted VWAP). None of these count.
* ``r_multiple``: the result in units of the Card's own risk, entry to stop.
  A target hit scores its reward-to-risk (capped at 5), a stop hit -1, a time
  exit wherever price ended, clipped to [-1, reward-to-risk]. A Card with no
  price stop is scored against its target distance instead.
* ``prior_move_pct``: how far price had already moved the Card's way in the
  30 minutes before it, the lateness read.
* ``is_repeat``: a published Card issued while an earlier Card from the same
  pattern, same direction, was still inside its hold window. That was the
  engine re-issuing one idea every few minutes; the record counts the idea
  once.

Resolution reuses the backtest harness's ``compute_outcome`` (intrabar
high/low, entry-trigger fills, stop wins a same-bar tie), so live grades and
the nightly stats read a Card the same way. A 0DTE Card's hold is capped at
that day's close, when its options expire.

The adaptive gate (``adaptive_gate.py``) reads these grades. The signals
service grades its own symbol every ``PLAYBOOK_GRADING_INTERVAL_SECONDS`` via
:func:`maybe_grade`; ``make playbook-grade`` backfills the lookback, and the
nightly calibration job re-runs it as a backstop.

CLI:
    python -m src.signals.playbook.grading                    # all symbols, lookback
    python -m src.signals.playbook.grading --days 30 --underlyings SPY QQQ
    python -m src.signals.playbook.grading --rebuild          # regrade from scratch
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence

import pytz

from src import config
from src.market_calendar import in_regular_session, regular_session_close
from src.signals.playbook import ideas
from src.signals.playbook.backtest import (
    _IMMEDIATE_TRIGGERS,
    CardRow,
    compute_outcome,
    fetch_quotes,
)

logger = logging.getLogger(__name__)

_ET = pytz.timezone("America/New_York")

PENDING = "pending"
GRADED = ("target_hit", "stop_hit", "time_exit")

# The lateness read looks this far back from the Card.
PRIOR_MOVE_MINUTES = 30
# A time exit is final this long after the hold ends, so the last bars are in.
_SETTLE_MINUTES = 5
# Cap on reward-to-risk, so a stop a hair from entry can't post a 40R win.
_MAX_REWARD_R = 5.0
# How much history the signals service re-syncs on each pass. Backfills and
# the nightly backstop use the full lookback.
_IN_PROCESS_DAYS = 5
# An at-market Card's quoted price must sit inside the range traded within two
# minutes of it, give or take this fraction of price.
_QUOTE_WINDOW_MINUTES = 2
_QUOTE_TOLERANCE = 0.0005


@dataclass
class Idea:
    """A ``playbook_card_outcomes`` row, as the grader needs it."""

    id: int
    underlying: str
    pattern: str
    action: str
    tier: str
    direction: str
    issued_at: datetime
    entry_price: Optional[float]
    entry_trigger: str
    target_price: Optional[float]
    stop_price: Optional[float]
    max_hold_minutes: Optional[int]


@dataclass
class Grade:
    outcome: str
    resolved_at: Optional[datetime] = None
    exit_price: Optional[float] = None
    mfe_pct: Optional[float] = None
    mae_pct: Optional[float] = None
    r_multiple: Optional[float] = None
    prior_move_pct: Optional[float] = None


# ---------------------------------------------------------------------------
# Pure grading
# ---------------------------------------------------------------------------


def effective_hold_minutes(tier: str, issued_at: datetime, max_hold: Optional[int]) -> int:
    """The Card's hold, capped at the session close for a 0DTE Card."""
    if not max_hold or max_hold <= 0:
        return 0
    if tier != "0DTE":
        return int(max_hold)
    et = issued_at.astimezone(_ET)
    close = _ET.localize(datetime.combine(et.date(), regular_session_close(et.date())))
    to_close = int((close - et).total_seconds() // 60)
    if to_close <= 0:
        return 0
    return min(int(max_hold), to_close)


def _level_payload(price: Optional[float]) -> dict:
    """A price level in the Card payload shape ``compute_outcome`` reads."""
    return {"kind": "level", "ref_price": price} if price is not None else {}


def valid_geometry(
    direction: str, entry: float, target: Optional[float], stop: Optional[float]
) -> bool:
    """Target beyond the entry and stop behind it, in the Card's direction."""
    sign = 1.0 if direction == "bullish" else -1.0
    if target is not None and sign * (target - entry) <= 0:
        return False
    if stop is not None and sign * (entry - stop) <= 0:
        return False
    return True


def r_multiple(
    direction: str,
    entry: float,
    target: Optional[float],
    stop: Optional[float],
    outcome: str,
    exit_price: Optional[float],
) -> Optional[float]:
    """The result in units of the Card's risk (see the module docstring)."""
    sign = 1.0 if direction == "bullish" else -1.0
    reward = abs(target - entry) if target is not None else None
    risk = abs(entry - stop) if stop is not None else None
    if not risk:
        risk = reward  # no price stop: score against the target distance
    if not risk:
        return None
    reward_r = min(reward / risk, _MAX_REWARD_R) if reward else 1.0
    if outcome == "target_hit":
        return reward_r
    if outcome == "stop_hit":
        return -1.0
    if outcome == "time_exit" and exit_price is not None:
        return max(-1.0, min(reward_r, sign * (exit_price - entry) / risk))
    return None


def prior_move_pct(
    bars: Sequence[tuple],
    times: Sequence[datetime],
    issued_at: datetime,
    direction: str,
    minutes: int = PRIOR_MOVE_MINUTES,
) -> Optional[float]:
    """Move in the Card's direction over the ``minutes`` before it, as a
    fraction of price. None when the bars around either end are missing."""
    i_now = bisect_right(times, issued_at) - 1
    if i_now < 0 or issued_at - times[i_now] > timedelta(minutes=5):
        return None
    cutoff = issued_at - timedelta(minutes=minutes)
    i_ref = bisect_right(times, cutoff) - 1
    if i_ref < 0 or cutoff - times[i_ref] > timedelta(minutes=15):
        return None
    ref = float(bars[i_ref][4])
    if ref <= 0:
        return None
    move = (float(bars[i_now][4]) - ref) / ref
    return move if direction == "bullish" else -move


def quoted_off_market(idea: Idea, bars: Sequence[tuple], times: Sequence[datetime]) -> bool:
    """True when an at-market Card quoted a price the market wasn't trading.

    Such a Card can't be graded from its own levels: whoever followed it got
    the real price, not the quoted one. Touch/break entries are exempt, since
    their entry is a level price has yet to reach. With no bars near the Card
    this can't tell, and says False.
    """
    trigger = (idea.entry_trigger or "").strip().lower()
    if trigger not in _IMMEDIATE_TRIGGERS or not idea.entry_price:
        return False
    window = timedelta(minutes=_QUOTE_WINDOW_MINUTES)
    near = bars[
        bisect_left(times, idea.issued_at - window) : bisect_right(times, idea.issued_at + window)
    ]
    if not near:
        return False
    tolerance = idea.entry_price * _QUOTE_TOLERANCE
    low = min(float(b[3]) for b in near) - tolerance
    high = max(float(b[2]) for b in near) + tolerance
    return not (low <= idea.entry_price <= high)


def grade_idea(
    idea: Idea,
    bars: Sequence[tuple],
    times: Sequence[datetime],
    now: datetime,
) -> Optional[Grade]:
    """A final grade, or None while the idea can still resolve.

    ``bars`` are ``(ts, open, high, low, close)`` oldest first, ``times``
    their timestamps, together covering the idea's hold window and the
    lateness lookback before it.
    """
    if idea.direction not in ("bullish", "bearish"):
        return Grade(outcome="unresolved")
    entry = idea.entry_price
    hold = effective_hold_minutes(idea.tier, idea.issued_at, idea.max_hold_minutes)
    if entry is None or entry <= 0 or hold <= 0:
        return Grade(outcome="no_data")
    # Mispriced first: a Card priced at VWAP often also has its levels on the
    # wrong side of that price, and the price is the cause.
    if quoted_off_market(idea, bars, times):
        return Grade(outcome="mispriced")
    if idea.target_price is None and idea.stop_price is None:
        return Grade(outcome="unresolved")
    if not valid_geometry(idea.direction, entry, idea.target_price, idea.stop_price):
        return Grade(outcome="unresolved")

    deadline = idea.issued_at + timedelta(minutes=hold)
    window = bars[bisect_left(times, idea.issued_at) : bisect_right(times, min(deadline, now))]
    card = CardRow(
        underlying=idea.underlying,
        timestamp=idea.issued_at,
        pattern=idea.pattern,
        action=idea.action,
        tier=idea.tier,
        direction=idea.direction,
        confidence=0.0,
        payload={
            "entry": {"ref_price": entry, "trigger": idea.entry_trigger or "at_market"},
            "target": _level_payload(idea.target_price),
            "stop": _level_payload(idea.stop_price),
            "max_hold_minutes": hold,
            "direction": idea.direction,
        },
    )
    oc = compute_outcome(card, window)
    prior = prior_move_pct(bars, times, idea.issued_at, idea.direction)

    if oc.outcome in ("target_hit", "stop_hit"):
        exit_price = idea.target_price if oc.outcome == "target_hit" else idea.stop_price
        return Grade(
            outcome=oc.outcome,
            resolved_at=oc.target_hit_at or oc.stop_hit_at,
            exit_price=exit_price,
            mfe_pct=oc.mfe_pct,
            mae_pct=oc.mae_pct,
            r_multiple=r_multiple(
                idea.direction, entry, idea.target_price, idea.stop_price, oc.outcome, exit_price
            ),
            prior_move_pct=prior,
        )
    if now < deadline + timedelta(minutes=_SETTLE_MINUTES):
        return None  # still live
    if oc.outcome == "time_exit":
        exit_price = float(window[-1][4]) if window else None
        return Grade(
            outcome="time_exit",
            resolved_at=deadline,
            exit_price=exit_price,
            mfe_pct=oc.mfe_pct,
            mae_pct=oc.mae_pct,
            r_multiple=r_multiple(
                idea.direction, entry, idea.target_price, idea.stop_price, "time_exit", exit_price
            ),
            prior_move_pct=prior,
        )
    return Grade(outcome=oc.outcome, prior_move_pct=prior)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


_INSERT_PUBLISHED_SQL = """
    INSERT INTO playbook_card_outcomes
        (card_id, underlying, pattern, action, tier, direction, confidence,
         issued_at, is_repeat, entry_price, entry_trigger, target_price,
         stop_price, max_hold_minutes, outcome)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (card_id) DO NOTHING
"""


def _payload_dict(raw: Any) -> dict:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return dict(raw) if isinstance(raw, dict) else {}


def sync_published_cards(conn, underlying: str, since: datetime) -> int:
    """Add published Cards since ``since`` that aren't graded rows yet.

    Marks repeats as it goes: a Card from the same pattern and direction as
    the pattern's last non-repeat Card, issued inside that Card's hold
    window. Returns the number of rows added.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.id, c.timestamp, c.pattern, c.action, c.tier, c.direction,
               c.confidence, c.payload
        FROM signal_action_cards c
        LEFT JOIN playbook_card_outcomes o ON o.card_id = c.id
        WHERE c.underlying = %s
          AND c.timestamp >= %s
          AND c.action <> 'STAND_DOWN'
          AND o.id IS NULL
        ORDER BY c.timestamp ASC, c.id ASC
        """,
        (underlying, since),
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    # Each pattern's last first-of-its-idea Card before this batch.
    first_ts = rows[0][1]
    cur.execute(
        """
        SELECT DISTINCT ON (pattern) pattern, direction, issued_at, max_hold_minutes
        FROM playbook_card_outcomes
        WHERE underlying = %s
          AND card_id IS NOT NULL
          AND NOT is_repeat
          AND outcome <> 'off_session'
          AND issued_at < %s
          AND issued_at > %s
        ORDER BY pattern, issued_at DESC
        """,
        (underlying, first_ts, first_ts - timedelta(days=ideas.LOOKBACK_DAYS)),
    )
    last_idea: dict[str, tuple[str, datetime, Optional[int]]] = {
        r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()
    }

    added = 0
    for card_id, ts, pattern, action, tier, direction, confidence, raw in rows:
        payload = _payload_dict(raw)
        levels = ideas.idea_levels(payload)
        in_session = in_regular_session(ts)
        repeat = False
        prev = last_idea.get(pattern)
        if in_session and prev is not None:
            prev_direction, prev_ts, prev_hold = prev
            if (
                prev_direction == direction
                and prev_hold
                and ts < prev_ts + timedelta(minutes=prev_hold)
            ):
                repeat = True
        if in_session and not repeat:
            last_idea[pattern] = (direction, ts, levels["max_hold_minutes"])
        cur.execute(
            _INSERT_PUBLISHED_SQL,
            (
                card_id,
                underlying,
                pattern,
                action,
                tier or "n/a",
                direction or "non_directional",
                float(confidence or 0.0),
                ts,
                repeat,
                levels["entry_price"],
                levels["entry_trigger"],
                levels["target_price"],
                levels["stop_price"],
                levels["max_hold_minutes"],
                PENDING if in_session else "off_session",
            ),
        )
        added += 1
    conn.commit()
    return added


def _load_pending(conn, underlying: str) -> list[Idea]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, pattern, action, tier, direction, issued_at, entry_price,
               entry_trigger, target_price, stop_price, max_hold_minutes
        FROM playbook_card_outcomes
        WHERE underlying = %s AND outcome = 'pending'
        ORDER BY issued_at ASC
        """,
        (underlying,),
    )
    return [
        Idea(
            id=r[0],
            underlying=underlying,
            pattern=r[1],
            action=r[2],
            tier=r[3],
            direction=r[4],
            issued_at=r[5],
            entry_price=float(r[6]) if r[6] is not None else None,
            entry_trigger=r[7] or "",
            target_price=float(r[8]) if r[8] is not None else None,
            stop_price=float(r[9]) if r[9] is not None else None,
            max_hold_minutes=int(r[10]) if r[10] is not None else None,
        )
        for r in cur.fetchall()
    ]


def grade_pending(conn, underlying: str, now: Optional[datetime] = None) -> dict[str, int]:
    """Grade every pending idea on ``underlying`` that can be graded now.

    Returns counts per outcome written, plus ``still_live``.
    """
    now = now or datetime.now(timezone.utc)
    pending = _load_pending(conn, underlying)
    counts: dict[str, int] = {}
    if not pending:
        return counts
    start = pending[0].issued_at - timedelta(minutes=PRIOR_MOVE_MINUTES + 20)
    bars = fetch_quotes(conn, underlying, start, now)
    times = [b[0] for b in bars]
    cur = conn.cursor()
    for idea in pending:
        grade = grade_idea(idea, bars, times, now)
        if grade is None:
            counts["still_live"] = counts.get("still_live", 0) + 1
            continue
        cur.execute(
            """
            UPDATE playbook_card_outcomes
            SET outcome = %s, resolved_at = %s, exit_price = %s, mfe_pct = %s,
                mae_pct = %s, r_multiple = %s, prior_move_pct = %s,
                graded_at = NOW()
            WHERE id = %s AND outcome = 'pending'
            """,
            (
                grade.outcome,
                grade.resolved_at,
                grade.exit_price,
                grade.mfe_pct,
                grade.mae_pct,
                grade.r_multiple,
                grade.prior_move_pct,
                idea.id,
            ),
        )
        counts[grade.outcome] = counts.get(grade.outcome, 0) + 1
    conn.commit()
    return counts


def clear_published_grades(conn, underlying: str, since: datetime) -> int:
    """Delete the graded rows of published Cards since ``since``, so the next
    sync rebuilds them with the current grading rules.

    Only published Cards: they are rebuilt from ``signal_action_cards``. Ideas
    the gate held back exist only in this table and are kept.
    """
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM playbook_card_outcomes
        WHERE underlying = %s AND card_id IS NOT NULL AND issued_at >= %s
        """,
        (underlying, since),
    )
    removed = cur.rowcount or 0
    conn.commit()
    return removed


def run(
    conn,
    underlying: str,
    *,
    days: int,
    now: Optional[datetime] = None,
    rebuild: bool = False,
) -> dict[str, int]:
    """Sync the last ``days`` of published Cards, then grade what's pending.

    ``rebuild`` first clears those Cards' existing grades, for when the
    grading rules themselves have changed.
    """
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=days)
    out: dict[str, int] = {}
    if rebuild:
        out["cleared"] = clear_published_grades(conn, underlying, since)
    out["added"] = sync_published_cards(conn, underlying, since)
    out.update(grade_pending(conn, underlying, now))
    return out


_last_pass: dict[str, float] = {}


def maybe_grade(underlying: str) -> None:
    """The signals service's in-process pass: at most once per
    ``PLAYBOOK_GRADING_INTERVAL_SECONDS`` per symbol. Never raises."""
    if not config.PLAYBOOK_GRADING_ENABLED or not underlying:
        return
    key = underlying.upper()
    now = time.time()
    if now - _last_pass.get(key, 0.0) < config.PLAYBOOK_GRADING_INTERVAL_SECONDS:
        return
    _last_pass[key] = now
    if not ideas.outcomes_table_available():
        return
    try:
        from src.database.connection import db_connection

        with db_connection() as conn:
            result = run(conn, key, days=_IN_PROCESS_DAYS)
        graded = sum(v for k, v in result.items() if k not in ("added", "still_live"))
        if graded:
            logger.info("playbook grading [%s]: %s", key, result)
    except Exception as exc:  # noqa: BLE001 - grading must never break a signal cycle
        if ideas.is_missing_table_error(exc):
            ideas.note_table_missing()
            logger.warning(
                "playbook grading [%s]: playbook_card_outcomes is missing; "
                "run `make schema-apply`",
                key,
            )
        else:
            logger.warning("playbook grading [%s] failed: %s", key, exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def graded_underlyings(conn, days: int) -> list[str]:
    """Every symbol with a Card in the window, plus the configured ones."""
    configured = [
        s.strip().upper() for s in (config.SIGNALS_UNDERLYINGS or "").split(",") if s.strip()
    ]
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT underlying FROM signal_action_cards
        WHERE timestamp > NOW() - (%s || ' days')::interval
        """,
        (str(int(days)),),
    )
    found = [r[0] for r in cur.fetchall() if r[0]]
    return sorted(set(configured) | set(found))


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Grade Playbook Cards and held-back ideas")
    parser.add_argument("--underlyings", nargs="*", default=None)
    parser.add_argument("--days", type=int, default=config.PLAYBOOK_ADAPTIVE_LOOKBACK_DAYS)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="regrade published Cards from scratch (after a change to the grading rules)",
    )
    args = parser.parse_args(argv)

    from src.database.connection import db_connection

    with db_connection() as conn:
        symbols = args.underlyings or graded_underlyings(conn, args.days)
        for symbol in symbols:
            symbol = symbol.upper()
            try:
                result = run(conn, symbol, days=args.days, rebuild=args.rebuild)
            except Exception:  # noqa: BLE001 - one symbol must not stop the rest
                logger.exception("grading failed for %s; continuing", symbol)
                conn.rollback()
                continue
            print(f"{symbol}: {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
