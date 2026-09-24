"""Adaptive entry bar: each pattern earns its bar, per symbol, from its record.

The engine used to drop Cards below one flat confidence floor (0.25). That
floor sat under nearly every Card the patterns produce, and nothing ever
checked whether a pattern's Cards worked, so a pattern that kept losing on a
symbol kept publishing there.

This module replaces the flat floor with a bar learned from the graded record
the grader (``grading.py``) writes to ``playbook_card_outcomes``:

* Each graded idea is scored in R, the move in units of the Card's own risk
  (entry to stop). A target hit scores its reward-to-risk, a stop hit scores
  -1, and a Card that ran out its hold scores wherever price ended up. A fixed
  friction charge (``PLAYBOOK_ADAPTIVE_COST_R``) comes off every idea.
* Records are kept per (pattern, symbol, direction) and recency-weighted
  (``PLAYBOOK_ADAPTIVE_HALF_LIFE_DAYS``). Each is blended with how the same
  pattern did everywhere else (other symbols, the other direction), which is
  itself blended with "no edge", each side weighted as
  ``PLAYBOOK_ADAPTIVE_PRIOR_WEIGHT`` ideas. A thin record therefore leans on
  the pattern's wider record instead of swinging the bar on a few trades.
* The shrunk expectancy sets the bar. At ``PROVEN_R`` or better the bar drops
  to ``MIN_BAR`` (every Card the pattern produces goes out); between zero and
  ``PAUSE_R`` it climbs to ``MAX_BAR``; at ``PAUSE_R`` or worse the pattern is
  paused on that symbol and direction.
* A pattern with fewer than ``MIN_IDEAS`` graded ideas keeps the neutral bar,
  which is the old flat floor.

Ideas the bar holds back are not thrown away: the cycle records them as
unpublished ideas and the grader grades them like any Card, so a paused
pattern whose ideas start working again earns its way back.

The active store is a process-global loaded from the database on a TTL
(:func:`maybe_refresh` for the psycopg2 signals service,
:func:`refresh_async` for the asyncpg API). The hot-path consult
(:func:`assess`) is a pure in-memory lookup.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional

from src import config

logger = logging.getLogger(__name__)

# Status labels, worst to best.
PAUSED = "paused"
LAGGING = "lagging"
LEARNING = "learning"
POSITIVE = "positive"
PROVEN = "proven"

# A bar no confidence can reach (confidence is clamped to [0.20, 0.95]).
_PAUSED_BAR = 2.0

# Outcomes that count toward a record. no_fill / no_data / unresolved never
# had a price-resolvable result; pending ones don't have one yet.
GRADED_OUTCOMES = ("target_hit", "stop_hit", "time_exit")


@dataclass(frozen=True)
class Record:
    """A graded record: raw counts plus the recency-weighted mean R."""

    n: int = 0  # graded ideas
    weight: float = 0.0  # recency-weighted count
    wins: int = 0  # target reached before the stop
    losses: int = 0  # stop reached before the target
    mean_r: float = 0.0  # recency-weighted mean R per idea, before friction

    def plus(self, other: "Record") -> "Record":
        weight = self.weight + other.weight
        mean = (
            (self.mean_r * self.weight + other.mean_r * other.weight) / weight
            if weight > 0
            else 0.0
        )
        return Record(
            n=self.n + other.n,
            weight=weight,
            wins=self.wins + other.wins,
            losses=self.losses + other.losses,
            mean_r=mean,
        )

    def minus(self, part: "Record") -> "Record":
        """This record without ``part`` (which must be contained in it)."""
        weight = self.weight - part.weight
        if self.n - part.n <= 0 or weight <= 1e-9:
            return Record()
        return Record(
            n=self.n - part.n,
            weight=weight,
            wins=max(0, self.wins - part.wins),
            losses=max(0, self.losses - part.losses),
            mean_r=(self.mean_r * self.weight - part.mean_r * part.weight) / weight,
        )


@dataclass
class GateStore:
    """Records per (pattern, symbol, direction), plus their sums per
    (pattern, symbol) and per pattern."""

    by_leaf: dict[tuple[str, str, str], Record] = field(default_factory=dict)
    by_pair: dict[tuple[str, str], Record] = field(default_factory=dict)
    by_pattern: dict[str, Record] = field(default_factory=dict)
    loaded_at: float = 0.0


@dataclass(frozen=True)
class Assessment:
    """What the record says about one candidate Card."""

    bar: float  # minimum confidence to publish; above 1 means paused
    status: str  # PAUSED | LAGGING | LEARNING | POSITIVE | PROVEN
    expected_r: Optional[float]  # shrunk expectancy net of friction; None while learning
    record: Record  # the most specific record that has graded ideas
    scope: str = ""  # where that record comes from, e.g. "on SPY bullish"

    @property
    def paused(self) -> bool:
        return self.status == PAUSED

    @property
    def is_adaptive(self) -> bool:
        """True when the record, not the neutral floor, set the bar."""
        return self.status != LEARNING

    def summary(self) -> dict:
        """JSON-safe snapshot stored on each Card's ``context``."""
        return {
            "status": self.status,
            "bar": None if self.paused else round(self.bar, 3),
            "expected_r": None if self.expected_r is None else round(self.expected_r, 3),
            "scope": self.scope,
            "ideas": self.record.n,
            "wins": self.record.wins,
            "losses": self.record.losses,
        }

    def miss_reason(self, confidence: float) -> str:
        """Near-miss text. Customers read it on the Stand Down card, so it is
        plain English: no R units, no internal names."""
        if self.status == LEARNING:
            return f"confidence {confidence:.2f} below the {self.bar:.2f} floor"
        rec = self.record
        record_txt = f"{rec.wins} won, {rec.losses} lost of {rec.n} graded ideas {self.scope}"
        if self.paused:
            return f"paused: {record_txt}; still tracked until its ideas start working"
        return f"track record {record_txt}; needs confidence {self.bar:.2f}, has {confidence:.2f}"


# Process-global active store + the lock guarding (re)assignment.
_active: Optional[GateStore] = None
_lock = threading.Lock()


def set_active_store(store: Optional[GateStore]) -> None:
    """Install (or clear) the active store. Used by the refresh paths + tests."""
    global _active
    with _lock:
        _active = store


def active_store() -> Optional[GateStore]:
    return _active


# ---------------------------------------------------------------------------
# The bar
# ---------------------------------------------------------------------------


def _shrink(rec: Optional[Record], parent: float) -> float:
    """Blend a record's mean with its parent's estimate.

    The parent counts as ``PLAYBOOK_ADAPTIVE_PRIOR_WEIGHT`` ideas, so a record
    with that many (recency-weighted) ideas sits halfway between the two.
    """
    k = config.PLAYBOOK_ADAPTIVE_PRIOR_WEIGHT
    if rec is None or rec.weight <= 0:
        return parent
    if k <= 0:
        return rec.mean_r
    return (rec.weight * rec.mean_r + k * parent) / (rec.weight + k)


def bar_for(expected_r: float) -> tuple[float, str]:
    """Map a shrunk expectancy (R per idea, net of friction) to (bar, status)."""
    neutral = config.PLAYBOOK_ADAPTIVE_NEUTRAL_BAR
    low = min(config.PLAYBOOK_ADAPTIVE_MIN_BAR, neutral)
    high = max(config.PLAYBOOK_ADAPTIVE_MAX_BAR, neutral)
    proven = config.PLAYBOOK_ADAPTIVE_PROVEN_R
    pause = config.PLAYBOOK_ADAPTIVE_PAUSE_R
    if expected_r >= proven:
        return low, PROVEN
    if expected_r >= 0.0:
        return neutral - (neutral - low) * (expected_r / proven), POSITIVE
    if expected_r > pause:
        return neutral + (high - neutral) * (expected_r / pause), LAGGING
    return _PAUSED_BAR, PAUSED


def _neutral(record: Record, scope: str = "") -> Assessment:
    return Assessment(
        bar=config.PLAYBOOK_ADAPTIVE_NEUTRAL_BAR,
        status=LEARNING,
        expected_r=None,
        record=record,
        scope=scope,
    )


def assess(
    pattern_id: str,
    underlying: str,
    direction: str,
    store: Optional[GateStore] = None,
) -> Assessment:
    """The bar a Card from ``pattern_id`` on ``underlying`` must clear.

    Falls back to the neutral bar (the old flat floor) when the gate is off,
    no store is loaded, or the pattern has too few graded ideas. Pure
    in-memory; safe to call every cycle.
    """
    store = store if store is not None else _active
    undl = (underlying or "").upper()
    if store is None or not config.PLAYBOOK_ADAPTIVE_GATE_ENABLED:
        return _neutral(Record())
    pattern_rec = store.by_pattern.get(pattern_id)
    leaf = store.by_leaf.get((pattern_id, undl, direction))
    pair = store.by_pair.get((pattern_id, undl))
    # Quote the most specific record that has anything in it.
    side = f" {direction}" if direction in ("bullish", "bearish") else ""
    if leaf is not None:
        shown, scope = leaf, f"on {undl}{side}"
    elif pair is not None:
        shown, scope = pair, f"on {undl}"
    else:
        shown, scope = pattern_rec or Record(), "across all symbols"
    if pattern_rec is None or pattern_rec.n < config.PLAYBOOK_ADAPTIVE_MIN_IDEAS:
        return _neutral(shown, scope)
    # How the pattern did everywhere else, pulled toward "no edge"; then this
    # symbol and direction, pulled toward that. The two records are disjoint,
    # so no trade counts twice. Friction comes off once, at the end.
    rest = pattern_rec.minus(leaf) if leaf is not None else pattern_rec
    expected = _shrink(leaf, _shrink(rest, 0.0)) - config.PLAYBOOK_ADAPTIVE_COST_R
    bar, status = bar_for(expected)
    return Assessment(bar=bar, status=status, expected_r=expected, record=shown, scope=scope)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def records_sql() -> str:
    """Per (pattern, symbol, direction) aggregate over the lookback.

    Parameter-free so the same text runs under psycopg2 and asyncpg; the only
    interpolated values are the two numeric settings, forced through
    ``int``/``float``. Repeat Cards (the same idea re-issued while its first
    Card was still live) are left out: they are one idea, not several.
    """
    lookback = int(config.PLAYBOOK_ADAPTIVE_LOOKBACK_DAYS)
    half_life = float(config.PLAYBOOK_ADAPTIVE_HALF_LIFE_DAYS)
    outcomes = ", ".join(f"'{o}'" for o in GRADED_OUTCOMES)
    return f"""
        SELECT pattern, underlying, direction,
               COUNT(*)::int AS n,
               SUM(w)::float8 AS weight,
               SUM(CASE WHEN outcome = 'target_hit' THEN 1 ELSE 0 END)::int AS wins,
               SUM(CASE WHEN outcome = 'stop_hit' THEN 1 ELSE 0 END)::int AS losses,
               SUM(w * r_multiple)::float8 AS sum_wr
        FROM (
            SELECT pattern, underlying, direction, outcome, r_multiple,
                   POWER(
                       0.5::float8,
                       (EXTRACT(EPOCH FROM (NOW() - issued_at)) / 86400.0
                        / {half_life:.6f})::float8
                   ) AS w
            FROM playbook_card_outcomes
            WHERE issued_at > NOW() - INTERVAL '{lookback} days'
              AND outcome IN ({outcomes})
              AND r_multiple IS NOT NULL
              AND NOT is_repeat
        ) graded
        GROUP BY pattern, underlying, direction
    """


def build_store(rows: Iterable) -> GateStore:
    """Build a store from ``records_sql`` rows (tuples or mappings)."""
    by_leaf: dict[tuple[str, str, str], Record] = {}
    for row in rows:
        if isinstance(row, dict) or hasattr(row, "keys"):
            pattern, underlying, direction = row["pattern"], row["underlying"], row["direction"]
            n, weight, wins, losses, sum_wr = (
                row["n"],
                row["weight"],
                row["wins"],
                row["losses"],
                row["sum_wr"],
            )
        else:
            pattern, underlying, direction, n, weight, wins, losses, sum_wr = row
        if not pattern or not n or weight is None or float(weight) <= 0:
            continue
        weight = float(weight)
        key = (str(pattern), str(underlying or "").upper(), str(direction or ""))
        by_leaf[key] = Record(
            n=int(n),
            weight=weight,
            wins=int(wins or 0),
            losses=int(losses or 0),
            mean_r=float(sum_wr or 0.0) / weight,
        )
    by_pair: dict[tuple[str, str], Record] = {}
    by_pattern: dict[str, Record] = {}
    for (pattern, underlying, _direction), rec in by_leaf.items():
        by_pair[(pattern, underlying)] = by_pair.get((pattern, underlying), Record()).plus(rec)
        by_pattern[pattern] = by_pattern.get(pattern, Record()).plus(rec)
    return GateStore(by_leaf=by_leaf, by_pair=by_pair, by_pattern=by_pattern, loaded_at=time.time())


def load_store(conn) -> GateStore:
    """psycopg2 load of the active store."""
    cur = conn.cursor()
    cur.execute(records_sql())
    return build_store(cur.fetchall())


def _due(ttl_seconds: Optional[int]) -> bool:
    ttl = config.PLAYBOOK_ADAPTIVE_REFRESH_SECONDS if ttl_seconds is None else ttl_seconds
    store = _active
    return store is None or (time.time() - store.loaded_at) >= ttl


def _log_refresh(store: GateStore) -> None:
    counts: dict[str, int] = {}
    for pattern, underlying, direction in store.by_leaf:
        status = assess(pattern, underlying, direction, store).status
        counts[status] = counts.get(status, 0) + 1
    logger.info(
        "playbook adaptive gate: loaded %d pattern/symbol/direction records (%s)",
        len(store.by_leaf),
        ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none graded yet",
    )


def _keep_previous_for_a_while() -> None:
    """After a failed load, stamp the store so the retry waits a TTL.

    Without this a missing table (schema not yet applied) would retry the
    query every cycle. The previous records, if any, stay in force.
    """
    store = _active or GateStore()
    set_active_store(
        GateStore(
            by_leaf=store.by_leaf,
            by_pair=store.by_pair,
            by_pattern=store.by_pattern,
            loaded_at=time.time(),
        )
    )


def maybe_refresh(ttl_seconds: Optional[int] = None) -> None:
    """Reload the store (psycopg2) once the TTL has elapsed.

    Called once per cycle by the signals service; a cheap no-op in between.
    Never raises: a failure keeps the previous store (or none, which means the
    neutral bar) and is logged.
    """
    if not config.PLAYBOOK_ADAPTIVE_GATE_ENABLED or not _due(ttl_seconds):
        return
    try:
        from src.database.connection import close_db_connection, get_db_connection

        conn = get_db_connection()
        try:
            store = load_store(conn)
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            raise
        finally:
            close_db_connection(conn)
        set_active_store(store)
        _log_refresh(store)
    except Exception:  # noqa: BLE001 - the gate must never break a signal cycle
        logger.warning("playbook adaptive gate: refresh failed; keeping prior store", exc_info=True)
        _keep_previous_for_a_while()


_async_lock: Optional[asyncio.Lock] = None
_async_lock_loop = None


def _lock_for_running_loop() -> asyncio.Lock:
    """One lock per event loop: a lock that has waited on one loop can't be
    awaited on another (tests spin up a fresh loop per client)."""
    global _async_lock, _async_lock_loop
    loop = asyncio.get_running_loop()
    if _async_lock is None or _async_lock_loop is not loop:
        _async_lock = asyncio.Lock()
        _async_lock_loop = loop
    return _async_lock


async def refresh_async(db, ttl_seconds: Optional[int] = None) -> None:
    """Reload the store through the API's asyncpg pool once the TTL has elapsed.

    ``db`` is the API ``DatabaseManager``; its ``get_playbook_track_records``
    returns ``None`` on failure. Requests that arrive while a load is in
    flight wait for it rather than evaluating against the old store (or, at
    startup, none). Never raises.
    """
    if not config.PLAYBOOK_ADAPTIVE_GATE_ENABLED or not _due(ttl_seconds):
        return
    async with _lock_for_running_loop():
        if not _due(ttl_seconds):
            return  # another request loaded it while this one waited
        try:
            rows = await db.get_playbook_track_records()
        except Exception:  # noqa: BLE001
            logger.warning("playbook adaptive gate: async refresh failed", exc_info=True)
            rows = None
        if rows is None:
            _keep_previous_for_a_while()
            return
        store = build_store(rows)
        set_active_store(store)
        _log_refresh(store)
