"""Read-only access to the production database.

Every statement in this module is a ``SELECT``.  The study writes nothing to
any production table; its outputs go to files.  That is what makes it safe to
point at the live analytics database.

The connection helper itself is reused from
``research.mm_attributed_gex.sources`` rather than copied: it sets the session
``READ ONLY`` and raises (then restores) the per-statement timeout, and that
logic is generic research plumbing, not specific to that experiment.  Copying
it would leave two versions of a safety guarantee to keep in sync.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Optional, Sequence
from zoneinfo import ZoneInfo

from research.mm_attributed_gex.sources import DatabaseUnavailable, research_connection
from research.wall_break_odds.events import ET, PriceBar, StepSeries, WallFrame

logger = logging.getLogger(__name__)

__all__ = [
    "DatabaseUnavailable",
    "research_connection",
    "session_bounds",
    "fetch_sessions",
    "fetch_summary_frames",
    "fetch_bars",
    "fetch_flow_at_strikes",
    "fetch_vix_series",
]

#: The gex_summary columns the study reads. Kept as one string so the SELECT
#: and the row shaping cannot drift apart.
_SUMMARY_COLUMNS = (
    "timestamp, call_wall, put_wall, call_wall_strength, put_wall_strength, "
    "total_net_gex, gamma_flip_point, flip_distance, local_gex, convexity_risk"
)


def session_bounds(session: date) -> tuple[datetime, datetime]:
    """The cash session as UTC instants, inclusive of the 16:00 bar."""
    start = datetime.combine(session, time(9, 30), tzinfo=ET)
    end = datetime.combine(session, time(16, 0), tzinfo=ET)
    return start.astimezone(ZoneInfo("UTC")), end.astimezone(ZoneInfo("UTC"))


def fetch_sessions(conn: Any, symbol: str, start: date, end: date) -> list[date]:
    """ET session dates in ``[start, end]`` that have any GEX frames.

    Driven off ``gex_summary`` rather than a market calendar so the study only
    ever asks for days the platform actually published — a holiday or an
    outage simply does not appear, instead of entering the sample as a session
    with no walls.
    """
    sql = """
        SELECT DISTINCT (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date AS d
          FROM gex_summary
         WHERE underlying = %(symbol)s
           AND timestamp >= %(start)s
           AND timestamp < %(end)s
         ORDER BY d
    """
    lo = datetime.combine(start, time(0, 0), tzinfo=ET)
    hi = datetime.combine(end + timedelta(days=1), time(0, 0), tzinfo=ET)
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "start": lo, "end": hi})
        return [r[0] for r in cur.fetchall() if r[0] is not None]


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    return out if out == out else None  # NaN -> None


def fetch_summary_frames(conn: Any, symbol: str, session: date) -> list[dict[str, Any]]:
    """Every ``gex_summary`` row published during one cash session."""
    start, end = session_bounds(session)
    sql = f"""
        SELECT {_SUMMARY_COLUMNS}
          FROM gex_summary
         WHERE underlying = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "start": start, "end": end})
        rows = cur.fetchall()
    return [
        {
            "timestamp": r[0],
            "call_wall": _f(r[1]),
            "put_wall": _f(r[2]),
            "call_wall_strength": _f(r[3]),
            "put_wall_strength": _f(r[4]),
            "total_net_gex": _f(r[5]),
            "gamma_flip_point": _f(r[6]),
            "flip_distance": _f(r[7]),
            "local_gex": _f(r[8]),
            "convexity_risk": _f(r[9]),
        }
        for r in rows
    ]


def to_wall_frames(rows: Sequence[dict[str, Any]]) -> list[WallFrame]:
    """Summary rows reduced to what the labeller needs."""
    return [
        WallFrame(ts=r["timestamp"], call_wall=r.get("call_wall"), put_wall=r.get("put_wall"))
        for r in rows
        if r.get("timestamp") is not None
    ]


def fetch_bars(conn: Any, symbol: str, session: date) -> list[PriceBar]:
    """Minute OHLC for the underlying over one cash session.

    Rows with a missing high or low (pre-backfill rows carry a close only) fall
    back to the close, matching ``level_history._normalize_bars`` so the two
    views of a session agree on what price did.
    """
    start, end = session_bounds(session)
    sql = """
        SELECT timestamp, high, low, close
          FROM underlying_quotes
         WHERE symbol = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "start": start, "end": end})
        rows = cur.fetchall()
    bars: list[PriceBar] = []
    for ts, high, low, close in rows:
        c = _f(close)
        if ts is None or c is None:
            continue
        h, lo = _f(high), _f(low)
        bars.append(
            PriceBar(
                ts=ts,
                high=h if h is not None else c,
                low=lo if lo is not None else c,
                close=c,
            )
        )
    return bars


def fetch_flow_at_strikes(
    conn: Any, symbol: str, session: date, strikes: Sequence[float]
) -> list[dict[str, Any]]:
    """Per-contract aggressor flow at the given strikes, one session.

    ``net_premium`` here is **day-to-date cumulative per contract** through the
    end of each 5-minute bucket (it resets at 09:30 ET). The feature layer
    differences it; nothing downstream may sum it. See
    ``features.FlowWindow`` for why that distinction matters.

    Returns ``[]`` when the table is absent or the query fails, so a deployment
    without flow history yields a study with the flow columns empty rather than
    no study at all.
    """
    if not strikes:
        return []
    start, end = session_bounds(session)
    sql = """
        SELECT timestamp, option_type, strike, expiration, net_premium, net_volume
          FROM flow_by_contract
         WHERE symbol = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
           AND strike = ANY(%(strikes)s)
         ORDER BY timestamp
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "symbol": symbol,
                    "start": start,
                    "end": end,
                    "strikes": [float(s) for s in strikes],
                },
            )
            rows = cur.fetchall()
    except Exception:
        logger.debug("flow_by_contract unavailable for %s %s", symbol, session, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    return [
        {
            "timestamp": r[0],
            "option_type": r[1],
            "strike": _f(r[2]),
            "expiration": r[3],
            "net_premium": _f(r[4]),
            "net_volume": _f(r[5]),
        }
        for r in rows
    ]


def fetch_vix_series(conn: Any, session: date) -> StepSeries:
    """VIX bars over one session as a step function.

    Empty (rather than absent) when the table is missing, so the vanna-proxy
    feature reads as unavailable instead of taking the whole session down.
    """
    start, end = session_bounds(session)
    sql = """
        SELECT timestamp, close
          FROM vix_bars
         WHERE timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"start": start, "end": end})
            rows = cur.fetchall()
    except Exception:
        logger.debug("vix_bars unavailable for %s", session, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return StepSeries([])
    return StepSeries((r[0], _f(r[1])) for r in rows)
