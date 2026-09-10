"""Session-by-session assembly of the labelled dataset.

Order of operations per session, and the reason for it:

1. Read the day's ``gex_summary`` frames and minute bars.
2. Extract and label the wall tests (:mod:`events`).
3. Read flow **only for the strikes those tests landed on** — one query per
   session instead of the whole chain, which is the difference between a
   study that finishes overnight and one that does not.
4. Build each event's feature vector at its own test timestamp
   (:mod:`features`).

The wall-strength percentile is maintained here rather than in the feature
layer because it is the one feature that needs memory of *other* sessions, and
that memory must be strictly backward-looking.  :class:`TrailingStrength`
holds a rolling window of previous sessions only; the current session is
appended after its events are built, never before.  Ranking today's wall
against a distribution that includes today is the classic way this kind of
study accidentally reports a result.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Iterator, Optional, Sequence

from research.wall_break_odds.events import ET, EventConfig, extract_wall_tests
from research.wall_break_odds.features import FlowWindow, SummarySeries, build_features

logger = logging.getLogger(__name__)

__all__ = [
    "TRAILING_SESSIONS",
    "TrailingStrength",
    "SessionResult",
    "build_session",
    "build_dataset",
    "write_jsonl",
    "read_jsonl",
]

#: How many previous sessions the strength percentile ranks against. Matches
#: the spirit of production's 30-day ``gex_historical_stats`` window.
TRAILING_SESSIONS = 30

#: Time-of-day bucket width in minutes. Production buckets at 5 minutes
#: because it aggregates over far more history than a research window holds;
#: at 30 minutes a 30-session window still puts ~900 observations behind every
#: percentile instead of ~30, which is the difference between a percentile and
#: a rounding artefact.
TOD_BUCKET_MINUTES = 30


def _tod_bucket(ts: datetime) -> int:
    et = ts.astimezone(ET)
    minutes = (et.hour - 9) * 60 + (et.minute - 30)
    if minutes < 0:
        return 0
    return int(minutes // TOD_BUCKET_MINUTES)


class TrailingStrength:
    """Backward-looking wall-strength distribution, bucketed by time of day."""

    def __init__(self, max_sessions: int = TRAILING_SESSIONS) -> None:
        self._sessions: deque[dict[tuple[str, int], list[float]]] = deque(maxlen=max_sessions)

    def percentile(self, side: str, ts: datetime, value: Optional[float]) -> Optional[float]:
        """Empirical percentile (0-100) of ``value`` in the trailing window.

        Falls back to the all-day distribution when the matching time-of-day
        bucket is thin, and returns None when there is no history at all — a
        cold start reports "unknown", never a fabricated 50.
        """
        if value is None or not self._sessions:
            return None
        bucket = _tod_bucket(ts)
        pool: list[float] = []
        for session in self._sessions:
            pool.extend(session.get((side, bucket), ()))
        if len(pool) < 50:
            pool = []
            for session in self._sessions:
                for (s, _b), vals in session.items():
                    if s == side:
                        pool.extend(vals)
        if len(pool) < 50:
            return None
        below = sum(1 for p in pool if p < value)
        return 100.0 * below / len(pool)

    def add_session(self, rows: Sequence[dict[str, Any]]) -> None:
        """Fold one completed session into the window."""
        acc: dict[tuple[str, int], list[float]] = {}
        for r in rows:
            ts = r.get("timestamp")
            if ts is None:
                continue
            bucket = _tod_bucket(ts)
            for side, key in (("call", "call_wall_strength"), ("put", "put_wall_strength")):
                v = r.get(key)
                if v is not None:
                    acc.setdefault((side, bucket), []).append(float(v))
        self._sessions.append(acc)


@dataclass
class SessionResult:
    """What one session contributed, including why it contributed nothing."""

    session: date
    events: list[dict[str, Any]]
    skipped_reason: Optional[str] = None
    n_frames: int = 0
    n_bars: int = 0
    #: Flow rows fetched, and how many distinct contracts were usable after
    #: canonicalising the option type. Rows > 0 with contracts == 0 is the
    #: encoding-mismatch signature.
    n_flow_rows: int = 0
    n_flow_contracts: int = 0


def build_session(
    conn: Any,
    symbol: str,
    session: date,
    trailing: TrailingStrength,
    cfg: Optional[EventConfig] = None,
    *,
    strike_step: float = 5.0,
    with_flow: bool = True,
) -> SessionResult:
    """Label and featurise one session. Never raises on a thin or odd day."""
    from research.wall_break_odds import sources

    cfg = cfg or EventConfig()
    rows = sources.fetch_summary_frames(conn, symbol, session)
    bars = sources.fetch_bars(conn, symbol, session)
    if len(rows) < cfg.min_session_frames:
        trailing.add_session(rows)
        return SessionResult(session, [], "too_few_frames", len(rows), len(bars))
    if not bars:
        trailing.add_session(rows)
        return SessionResult(session, [], "no_bars", len(rows), 0)

    tests = extract_wall_tests(symbol, session, sources.to_wall_frames(rows), bars, cfg)
    if not tests:
        trailing.add_session(rows)
        return SessionResult(session, [], "no_tests", len(rows), len(bars))

    flow: Optional[FlowWindow] = None
    flow_rows: list[dict[str, Any]] = []
    if with_flow:
        walls = {t.wall for t in tests}
        strikes = sorted(
            walls | {w + strike_step for w in walls} | {w - strike_step for w in walls}
        )
        flow_rows = sources.fetch_flow_at_strikes(conn, symbol, session, strikes)
        flow = FlowWindow.from_rows(flow_rows) if flow_rows else None
        # A populated table that yields no usable contracts is an encoding
        # mismatch, not a quiet tape, and it is invisible downstream: every
        # flow feature just reads None and the column drops out of the screen
        # looking like missing data. Say so at WARNING, once per session.
        if flow is not None and not flow.coverage():
            logger.warning(
                "%s %s: %d flow rows fetched but none usable "
                "(unrecognised option_type values: %s) — flow features will be empty",
                symbol,
                session,
                len(flow_rows),
                flow.unrecognised or "none",
            )

    summary = SummarySeries.from_rows(rows)
    vix = sources.fetch_vix_series(conn, session)
    strength_series = summary.call_wall_strength, summary.put_wall_strength

    out: list[dict[str, Any]] = []
    for event in tests:
        strength_at_test = (
            strength_series[0].at(event.tested_at)
            if event.side == "call"
            else strength_series[1].at(event.tested_at)
        )
        pctile = trailing.percentile(event.side, event.tested_at, strength_at_test)
        feats = build_features(
            event,
            summary,
            bars,
            flow=flow,
            vix=vix if len(vix) else None,
            strength_percentile=pctile,
            strike_step=strike_step,
        )
        record = event.to_dict()
        record["features"] = feats
        out.append(record)

    # Only now does this session become history for the ones that follow.
    trailing.add_session(rows)
    return SessionResult(
        session,
        out,
        None,
        len(rows),
        len(bars),
        n_flow_rows=len(flow_rows),
        n_flow_contracts=sum((flow.coverage() if flow else {}).values()),
    )


def build_dataset(
    conn: Any,
    symbol: str,
    start: date,
    end: date,
    cfg: Optional[EventConfig] = None,
    *,
    strike_step: float = 5.0,
    with_flow: bool = True,
    progress: Optional[Any] = None,
) -> Iterator[SessionResult]:
    """Yield one :class:`SessionResult` per session in ``[start, end]``."""
    from research.wall_break_odds import sources

    cfg = cfg or EventConfig()
    trailing = TrailingStrength()
    for session in sources.fetch_sessions(conn, symbol, start, end):
        try:
            result = build_session(
                conn, symbol, session, trailing, cfg, strike_step=strike_step, with_flow=with_flow
            )
        except Exception as exc:  # pragma: no cover - one bad day must not end the run
            logger.warning("session %s failed: %s", session, exc, exc_info=True)
            result = SessionResult(session, [], f"error: {exc}")
        if progress is not None:
            progress(result)
        yield result


def write_jsonl(path: str, records: Iterable[dict[str, Any]]) -> int:
    """Write records one per line; returns the count written."""
    n = 0
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, default=str) + "\n")
            n += 1
    return n


def read_jsonl(path: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out
