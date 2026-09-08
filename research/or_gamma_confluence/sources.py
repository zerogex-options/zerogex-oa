"""Read-only access to the production database.

Every statement in this module is a ``SELECT``.  The study writes nothing to
any production table; its outputs go to files.  That is what makes it safe to
point at the live analytics database.

The connection helper is reused from
:mod:`research.mm_attributed_gex.sources` rather than copied — it sets the
session ``READ ONLY`` and raises (then restores) the per-statement timeout,
which is generic research plumbing and should not exist in two versions.
Minute bars are read by :func:`research.msi_regime_excursion.sources.load_bars`
for the same reason: it already routes ``underlying_quotes`` vs
``futures_quotes`` correctly, and duplicating that routing is how ES quietly
ends up measured on the SPX tape.

One read here is not like the others.  :func:`coverage` exists because the
history depth of this study is not knowable from the code: ``gex_summary`` and
``underlying_quotes`` are retention-exempt while ``gex_by_strike`` is pruned at
``DATA_RETENTION_DAYS``, so the wall/flip arm and the ranked-GEX arm have
different — and deployment-specific — windows.  It also measures the
``created_at`` distribution, which is what decides whether the publish clock
(:data:`~research.or_gamma_confluence.config.CLOCK_PUBLISHED`) is usable at all
or whether backfilled rows have made it fiction.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from research.mm_attributed_gex.sources import DatabaseUnavailable, research_connection
from research.msi_regime_excursion.excursion import ET, Bar
from research.msi_regime_excursion.sources import load_bars
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.instruments import InstrumentSpec, spec
from src.symbols import resolve_volume_proxy

logger = logging.getLogger(__name__)

UTC = ZoneInfo("UTC")

__all__ = [
    "DatabaseUnavailable",
    "research_connection",
    "session_bounds",
    "fetch_sessions",
    "fetch_summary_frames",
    "fetch_bars",
    "fetch_index_bars",
    "fetch_volumes",
    "fetch_strike_frames",
    "fetch_trade_bias",
    "coverage",
    "SUMMARY_COLUMNS",
]

#: The ``gex_summary`` columns the study reads, as one string so the SELECT and
#: the row shaping cannot drift apart.  ``created_at`` is not optional here —
#: it is the publish clock, and the whole anti-look-ahead argument depends on
#: it (see :mod:`.levels`).
SUMMARY_COLUMNS = (
    "timestamp, created_at, call_wall, put_wall, call_wall_strength, "
    "put_wall_strength, gamma_flip_point, gamma_flip_raw, flip_distance, "
    "max_pain, pin_strike, pin_score, pin_confidence, max_gamma_strike, "
    "total_net_gex, net_gex_at_spot, local_gex, convexity_risk"
)

_SUMMARY_KEYS = tuple(c.strip() for c in SUMMARY_COLUMNS.split(","))


def session_bounds(session: date, cfg: ResearchConfig) -> tuple[datetime, datetime]:
    """The cash session as UTC instants, inclusive of the closing bar."""
    start = datetime.combine(session, cfg.session_start, tzinfo=ET)
    end = datetime.combine(session, cfg.session_end, tzinfo=ET)
    return start.astimezone(UTC), end.astimezone(UTC)


def fetch_sessions(conn: Any, gamma_symbol: str, start: date, end: date) -> list[date]:
    """ET session dates in ``[start, end]`` that have gamma frames.

    Driven off ``gex_summary`` rather than a market calendar, so the study only
    asks for days the platform actually published: a holiday or an outage
    simply does not appear, instead of entering the sample as a session with no
    levels.
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
        cur.execute(sql, {"symbol": gamma_symbol.upper(), "start": lo, "end": hi})
        return [r[0] for r in cur.fetchall() if r[0] is not None]


def fetch_summary_frames(
    conn: Any, gamma_symbol: str, session: date, cfg: ResearchConfig
) -> list[dict[str, Any]]:
    """Every ``gex_summary`` row published during one cash session.

    Bounded by the frame's own ``timestamp``.  A frame whose data instant is in
    the session but which was WRITTEN after the bell is still returned — it is
    the availability clock in :mod:`.levels`, not this query, that decides
    whether such a frame was knowable in time, and hiding it here would make
    that decision silently.
    """
    start, end = session_bounds(session, cfg)
    sql = f"""
        SELECT {SUMMARY_COLUMNS}
          FROM gex_summary
         WHERE underlying = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": gamma_symbol.upper(), "start": start, "end": end})
        rows = cur.fetchall()
    return [dict(zip(_SUMMARY_KEYS, r)) for r in rows]


def fetch_bars(conn: Any, inst: InstrumentSpec, session: date, cfg: ResearchConfig) -> list[Bar]:
    """Minute OHLC for one cash session, on the instrument's own price axis.

    Delegates to ``msi_regime_excursion.sources.load_bars``: ES/NQ come from
    ``futures_quotes`` keyed by the backing index, everything else from
    ``underlying_quotes``.  :class:`~.instruments.InstrumentSpec` carries the
    same ``bar_source`` / ``bar_symbol`` fields that reader expects.
    """
    start, end = session_bounds(session, cfg)
    return load_bars(conn, inst, start, end)


def fetch_strike_frames(
    conn: Any, gamma_symbol: str, session: date, cfg: ResearchConfig
) -> dict[datetime, list[dict[str, Any]]]:
    """``gex_by_strike`` rows for one session, grouped by frame timestamp.

    One query per session rather than one per frame — a session is ~390 frames
    and a per-frame read is the difference between a study that finishes and
    one that does not.

    Returns ``{}`` when the table has no rows in range.  That is the normal
    case outside the retention window (``gex_by_strike`` IS pruned, unlike
    ``gex_summary``), and it must degrade to "no ranked levels" rather than to
    "no session": every wall / flip / max-pain / pin cohort is still available.
    """
    start, end = session_bounds(session, cfg)
    sql = """
        SELECT timestamp, strike, call_gamma, put_gamma, net_gex, expiration
          FROM gex_by_strike
         WHERE underlying = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp, strike
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"symbol": gamma_symbol.upper(), "start": start, "end": end})
            rows = cur.fetchall()
    except Exception:
        logger.debug("gex_by_strike unavailable for %s %s", gamma_symbol, session, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return {}
    grouped: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for ts, strike, call_gamma, put_gamma, net_gex, expiration in rows:
        if ts is None or strike is None:
            continue
        grouped[ts].append(
            {
                "strike": float(strike),
                "call_gamma": float(call_gamma or 0.0),
                "put_gamma": float(put_gamma or 0.0),
                "net_gex": float(net_gex or 0.0),
                "expiration": expiration,
            }
        )
    return dict(grouped)


def fetch_trade_bias(
    conn: Any, gamma_symbol: str, session: date, cfg: ResearchConfig
) -> list[dict[str, Any]]:
    """``trade_bias_scores`` rows for one session — the existing trend read.

    Empty when the table is absent or the symbol has no rows, so the trade-bias
    trend filter reads as unavailable instead of taking the session down.
    """
    start, end = session_bounds(session, cfg)
    sql = """
        SELECT timestamp, bias_code, direction, market_state, confidence, bias_score
          FROM trade_bias_scores
         WHERE underlying = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
           AND tenor = 'swing'
         ORDER BY timestamp
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"symbol": gamma_symbol.upper(), "start": start, "end": end})
            rows = cur.fetchall()
    except Exception:
        logger.debug(
            "trade_bias_scores unavailable for %s %s", gamma_symbol, session, exc_info=True
        )
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    keys = ("timestamp", "bias_code", "direction", "market_state", "confidence", "bias_score")
    return [dict(zip(keys, r)) for r in rows]


# ── Coverage: how much history is actually there ─────────────────────


def _span(conn: Any, sql: str, params: Mapping[str, Any]) -> dict[str, Any]:
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"available": False, "error": str(exc)[:200]}
    if not row or row[0] is None:
        return {"available": True, "rows": 0, "sessions": 0, "earliest": None, "latest": None}
    earliest, latest, rows, sessions = row
    return {
        "available": True,
        "rows": int(rows or 0),
        "sessions": int(sessions or 0),
        "earliest": earliest.isoformat() if earliest else None,
        "latest": latest.isoformat() if latest else None,
    }


_SPAN_SQL = """
    SELECT MIN(timestamp), MAX(timestamp), COUNT(*),
           COUNT(DISTINCT (timestamp AT TIME ZONE 'UTC'
                           AT TIME ZONE 'America/New_York')::date)
      FROM {table}
     WHERE {column} = %(symbol)s
"""

#: Every table this study reads, with the column its symbol lives under.
#: An explicit allowlist, because these names are interpolated into SQL.
_COVERAGE_TABLES: tuple[tuple[str, str, str], ...] = (
    ("gex_summary", "underlying", "gamma"),
    ("gex_by_strike", "underlying", "gamma"),
    ("underlying_quotes", "symbol", "bars_cash"),
    ("futures_quotes", "index_symbol", "bars_futures"),
    ("trade_bias_scores", "underlying", "gamma"),
)

_PUBLISH_LAG_SQL = """
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE created_at IS NULL),
           PERCENTILE_CONT(0.5) WITHIN GROUP (
               ORDER BY EXTRACT(EPOCH FROM (created_at - timestamp))),
           PERCENTILE_CONT(0.95) WITHIN GROUP (
               ORDER BY EXTRACT(EPOCH FROM (created_at - timestamp))),
           MAX(EXTRACT(EPOCH FROM (created_at - timestamp))),
           COUNT(*) FILTER (WHERE created_at < timestamp),
           COUNT(*) FILTER (
               WHERE EXTRACT(EPOCH FROM (created_at - timestamp)) > %(max_lag)s)
      FROM gex_summary
     WHERE underlying = %(symbol)s
       AND timestamp >= %(since)s
"""


def _publish_lag(
    conn: Any, gamma_symbol: str, cfg: ResearchConfig, since: datetime
) -> dict[str, Any]:
    """Is ``created_at`` usable as a publish clock for this symbol?

    Three failure shapes are counted separately because they mean different
    things: NULLs are a deployment that never populated the column, negative
    lags are a clock or a write-order problem, and lags beyond
    ``max_publish_lag_seconds`` are backfilled rows whose ``created_at`` is the
    backfill time.  Only the last of those is recoverable by narrowing the
    window.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                _PUBLISH_LAG_SQL,
                {
                    "symbol": gamma_symbol.upper(),
                    "max_lag": float(cfg.max_publish_lag_seconds),
                    "since": since,
                },
            )
            row = cur.fetchone()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"available": False, "error": str(exc)[:200]}
    if not row or not row[0]:
        return {"available": True, "rows": 0}
    total, nulls, p50, p95, worst, negative, backfilled = row
    total = int(total or 0)
    return {
        "available": True,
        "rows": total,
        "created_at_null": int(nulls or 0),
        "created_at_null_pct": round(100.0 * (nulls or 0) / total, 2) if total else None,
        "lag_p50_s": round(float(p50), 1) if p50 is not None else None,
        "lag_p95_s": round(float(p95), 1) if p95 is not None else None,
        "lag_max_s": round(float(worst), 1) if worst is not None else None,
        "negative_lag_rows": int(negative or 0),
        "backfilled_rows": int(backfilled or 0),
        "backfilled_pct": round(100.0 * (backfilled or 0) / total, 2) if total else None,
    }


def coverage(
    conn: Any,
    symbols: Sequence[str],
    cfg: ResearchConfig,
    *,
    lag_lookback_days: int = 120,
) -> dict[str, Any]:
    """How much usable history exists, per symbol and per table.

    This is the read that bounds the whole study, and it answers three
    questions the code cannot:

    * How far back does ``gex_summary`` go?  (retention-exempt, so possibly
      years)
    * How far back does ``gex_by_strike`` go?  (pruned, so the ranked-GEX arm
      is capped here and only here)
    * Is ``created_at`` a usable publish clock, or has backfilling made it
      fiction?
    """
    out: dict[str, Any] = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "availability_clock": cfg.availability_clock,
        "max_publish_lag_seconds": cfg.max_publish_lag_seconds,
        "symbols": {},
    }
    since = datetime.now(tz=UTC) - timedelta(days=lag_lookback_days)
    lag_cache: dict[str, dict[str, Any]] = {}

    for symbol in symbols:
        inst = spec(symbol)
        tables: dict[str, Any] = {}
        for table, column, role in _COVERAGE_TABLES:
            if role == "bars_cash" and inst.is_futures:
                continue
            if role == "bars_futures" and not inst.is_futures:
                continue
            target = inst.gamma_symbol if role == "gamma" else inst.bar_symbol
            tables[table] = {
                "symbol_used": target,
                **_span(
                    conn,
                    _SPAN_SQL.format(table=table, column=column),
                    {"symbol": target.upper()},
                ),
            }
        if inst.gamma_symbol not in lag_cache:
            lag_cache[inst.gamma_symbol] = _publish_lag(conn, inst.gamma_symbol, cfg, since)
        out["symbols"][inst.key] = {
            "spec": inst.to_dict(),
            "tables": tables,
            "publish_clock": lag_cache[inst.gamma_symbol],
        }
    return out


def fetch_index_bars(conn: Any, gamma_symbol: str, session: date, cfg: ResearchConfig) -> list[Bar]:
    """Minute bars for the GAMMA symbol, always from ``underlying_quotes``.

    Distinct from :func:`fetch_bars`, and the distinction is load-bearing for
    ES/NQ.  The ranked GEX ladder is computed from ``gex_by_strike``, whose
    strikes are INDEX strikes, so it must be ranked against the INDEX spot —
    ranking NDX strikes against an NQ price would put every strike on the wrong
    side of spot by roughly the basis and silently invert the whole ladder.

    For a cash symbol this returns the same rows as :func:`fetch_bars`; the two
    calls are kept separate anyway so the futures path cannot regress into
    sharing one series.
    """
    start, end = session_bounds(session, cfg)
    sql = """
        SELECT timestamp, open, high, low, close
          FROM underlying_quotes
         WHERE symbol = %(symbol)s
           AND timestamp >= %(start)s
           AND timestamp <= %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": gamma_symbol.upper(), "start": start, "end": end})
        rows = cur.fetchall()
    out: list[Bar] = []
    for ts, o, h, l, c in rows:
        if ts is None or None in (o, h, l, c):
            continue
        out.append(Bar(ts=ts, open=float(o), high=float(h), low=float(l), close=float(c)))
    return out


def fetch_volumes(
    conn: Any, inst: InstrumentSpec, session: date, cfg: ResearchConfig
) -> tuple[dict[datetime, float], Optional[str]]:
    """Per-bar total volume for VWAP, and the proxy symbol if one was used.

    A cash index has no transactional volume of its own, so production
    substitutes its tracking ETF's profile (``src.symbols.resolve_volume_proxy``
    — SPX→SPY, NDX→QQQ) and this does the same, reporting which symbol was
    actually read so a VWAP is never quoted without saying whose volume shaped
    it.  Futures carry their own ``up_volume``/``down_volume``.

    Returns ``({}, proxy)`` when no usable volume exists; the feature layer then
    reports VWAP as unavailable rather than fabricating one.
    """
    start, end = session_bounds(session, cfg)
    proxy: Optional[str] = None
    if inst.is_futures:
        sql = """
            SELECT timestamp, COALESCE(up_volume, 0) + COALESCE(down_volume, 0)
              FROM futures_quotes
             WHERE index_symbol = %(symbol)s
               AND timestamp BETWEEN %(start)s AND %(end)s
             ORDER BY timestamp
        """
        target = inst.bar_symbol
    else:
        proxy = resolve_volume_proxy(inst.bar_symbol)
        target = proxy or inst.bar_symbol
        sql = """
            SELECT timestamp, COALESCE(up_volume, 0) + COALESCE(down_volume, 0)
              FROM underlying_quotes
             WHERE symbol = %(symbol)s
               AND timestamp BETWEEN %(start)s AND %(end)s
             ORDER BY timestamp
        """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"symbol": target.upper(), "start": start, "end": end})
            rows = cur.fetchall()
    except Exception:
        logger.debug("volume read failed for %s %s", target, session, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return {}, proxy
    return {ts: float(v or 0.0) for ts, v in rows if ts is not None}, proxy
