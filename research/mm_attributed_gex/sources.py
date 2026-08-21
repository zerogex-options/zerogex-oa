"""Read-only access to the production database.

Strictly read-only.  Every statement here is a ``SELECT``; the experiment
writes nothing to any production table, and its own outputs go to files.  That
constraint is what makes it safe to run this against the live analytics
database.

Three things are read:

* the SPX option chain at a timestamp (``option_chains`` /
  ``option_chains_latest`` / ``option_chains_archive``) — the IVs, gammas and
  open interest the MM positions are priced against, so both methodologies see
  exactly the same market data;
* the persisted production readings (``gex_summary``) — the *actual* values the
  live system published, which is a stronger comparand than a recomputation;
* underlying bars (``underlying_quotes``) and VIX (``vix_bars``) for forward
  outcomes and volatility-regime controls.

The archive table (``option_chains_archive``) is retention-exempt, so it is the
only place chain history older than ``DATA_RETENTION_DAYS`` survives; the chain
reader unions it in automatically when the requested window is old.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any, Iterator, Optional, Sequence

from research.mm_attributed_gex.gex import ChainQuote
from research.mm_attributed_gex.schema import SeriesKey

logger = logging.getLogger(__name__)

__all__ = [
    "DatabaseUnavailable",
    "research_connection",
    "fetch_chain_snapshot",
    "fetch_snapshot_timestamps",
    "fetch_production_summary",
    "fetch_production_summaries",
    "fetch_underlying_bars",
    "fetch_vix_closes",
    "fetch_series_listing_dates",
    "fetch_open_interest_series",
]


class DatabaseUnavailable(RuntimeError):
    """Raised when the production database cannot be reached.

    Surfaced rather than swallowed: an experiment that silently produces an
    empty dataset because the DSN was wrong is worse than one that stops.
    """


@contextmanager
def research_connection() -> Iterator[Any]:
    """Yield a read-only production connection, or raise :class:`DatabaseUnavailable`.

    Wraps ``src.database.db_connection`` (the platform's pooled connection) and
    sets the session to READ ONLY, so a mistake in this package cannot write to
    a production table even in principle.
    """
    try:
        from src.database import db_connection
    except Exception as exc:  # pragma: no cover - import guard
        raise DatabaseUnavailable(f"cannot import src.database: {exc}") from exc

    try:
        with db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION READ ONLY")
            except Exception:  # pragma: no cover - some drivers/pools disallow
                logger.debug("could not set READ ONLY on the research connection")
            yield conn
    except DatabaseUnavailable:
        raise
    except Exception as exc:
        raise DatabaseUnavailable(f"database unavailable: {exc}") from exc


# ---------------------------------------------------------------------------
# Option chain
# ---------------------------------------------------------------------------

_CHAIN_COLUMNS = (
    "option_symbol, strike, expiration, option_type, "
    "implied_volatility, gamma, open_interest, volume"
)

_CHAIN_SQL = f"""
    SELECT DISTINCT ON (option_symbol) {_CHAIN_COLUMNS}
      FROM option_chains
     WHERE underlying = %(symbol)s
       AND timestamp <= %(ts)s
       AND timestamp >= %(floor_ts)s
       AND expiration >= %(as_of)s
       AND gamma IS NOT NULL
     ORDER BY option_symbol, timestamp DESC
"""

# The archive keeps no volume/open_interest columns, so those come back as 0;
# MM-attributed gamma does not use open interest, and the production comparand
# for an archived window is read from gex_summary rather than recomputed.
_ARCHIVE_SQL = """
    SELECT DISTINCT ON (option_symbol)
           option_symbol, strike, expiration, option_type,
           implied_volatility, gamma, 0 AS open_interest, 0 AS volume
      FROM option_chains_archive
     WHERE underlying = %(symbol)s
       AND timestamp <= %(ts)s
       AND timestamp >= %(floor_ts)s
       AND expiration >= %(as_of)s
       AND gamma IS NOT NULL
     ORDER BY option_symbol, timestamp DESC
"""


def fetch_chain_snapshot(
    conn: Any,
    symbol: str,
    ts: datetime,
    *,
    lookback_hours: float = 2.0,
    include_archive: bool = True,
) -> list[ChainQuote]:
    """Latest quote per contract at or before ``ts``.

    Mirrors the production snapshot read (``AnalyticsEngine._get_snapshot``):
    ``DISTINCT ON (option_symbol) … ORDER BY option_symbol, timestamp DESC``
    within a bounded lookback, filtered to contracts that still have a gamma.
    Using the same read shape matters — a different contract universe would
    contaminate the comparison with a coverage difference.
    """
    params = {
        "symbol": symbol,
        "ts": ts,
        "floor_ts": ts - timedelta(hours=lookback_hours),
        "as_of": ts.date(),
    }
    rows: list[tuple] = []
    with conn.cursor() as cur:
        cur.execute(_CHAIN_SQL, params)
        rows = list(cur.fetchall())
        if not rows and include_archive:
            cur.execute(_ARCHIVE_SQL, params)
            rows = list(cur.fetchall())

    out: list[ChainQuote] = []
    for r in rows:
        iv = float(r[4]) if r[4] is not None else None
        out.append(
            ChainQuote(
                option_symbol=r[0],
                strike=float(r[1]),
                expiration=r[2],
                option_type=r[3],
                implied_volatility=iv,
                gamma=float(r[5]) if r[5] is not None else None,
                open_interest=int(r[6] or 0),
                volume=int(r[7] or 0),
            )
        )
    return out


def fetch_snapshot_timestamps(
    conn: Any,
    symbol: str,
    start: datetime,
    end: datetime,
    *,
    step_minutes: int = 5,
) -> list[datetime]:
    """Production analytics timestamps in ``[start, end]``, thinned to ``step_minutes``.

    Anchoring the research grid to timestamps that actually exist in
    ``gex_summary`` guarantees every research row has a production comparand;
    thinning keeps a multi-month study tractable without changing what is
    measured.
    """
    sql = """
        SELECT timestamp
          FROM gex_summary
         WHERE underlying = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "start": start, "end": end})
        stamps = [r[0] for r in cur.fetchall()]
    if step_minutes <= 1 or not stamps:
        return stamps
    kept: list[datetime] = []
    last: Optional[datetime] = None
    gap = timedelta(minutes=step_minutes)
    for ts in stamps:
        if last is None or (ts - last) >= gap:
            kept.append(ts)
            last = ts
    return kept


# ---------------------------------------------------------------------------
# Production readings
# ---------------------------------------------------------------------------

_SUMMARY_COLUMNS = (
    "timestamp, gamma_flip_point, gamma_flip_raw, net_gex_at_spot, total_net_gex, "
    "call_wall, put_wall, call_wall_strength, put_wall_strength, max_gamma_strike, "
    "max_pain, put_call_ratio, flip_distance, local_gex, convexity_risk, "
    "total_call_oi, total_put_oi, pin_strike"
)


def _summary_row(r: Sequence[Any]) -> dict[str, Any]:
    def _f(v: Any) -> Optional[float]:
        return None if v is None else float(v)

    return {
        "timestamp": r[0],
        "existing_gamma_flip": _f(r[1]),
        "existing_gamma_flip_raw": _f(r[2]),
        "existing_dealer_gamma_at_spot": _f(r[3]),
        "existing_net_gex": _f(r[4]),
        "existing_call_wall": _f(r[5]),
        "existing_put_wall": _f(r[6]),
        "existing_call_wall_strength": _f(r[7]),
        "existing_put_wall_strength": _f(r[8]),
        "existing_max_gamma_strike": _f(r[9]),
        "existing_max_pain": _f(r[10]),
        "existing_put_call_ratio": _f(r[11]),
        "existing_flip_distance": _f(r[12]),
        "existing_local_gex": _f(r[13]),
        "existing_convexity_risk": _f(r[14]),
        "existing_total_call_oi": _f(r[15]),
        "existing_total_put_oi": _f(r[16]),
        "existing_pin_strike": _f(r[17]),
    }


def fetch_production_summary(conn: Any, symbol: str, ts: datetime) -> Optional[dict[str, Any]]:
    """The production ``gex_summary`` row at ``ts`` (exact match)."""
    sql = f"""
        SELECT {_SUMMARY_COLUMNS}
          FROM gex_summary
         WHERE underlying = %(symbol)s AND timestamp = %(ts)s
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "ts": ts})
        row = cur.fetchone()
    return _summary_row(row) if row else None


def fetch_production_summaries(
    conn: Any, symbol: str, start: datetime, end: datetime
) -> dict[datetime, dict[str, Any]]:
    """Every production reading in a window, keyed by timestamp.

    One round trip for a whole study window instead of one per snapshot — the
    difference between a research run that finishes and one that does not.
    """
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
    out: dict[datetime, dict[str, Any]] = {}
    for r in rows:
        shaped = _summary_row(r)
        out[shaped["timestamp"]] = shaped
    return out


def fetch_composite_scores(
    conn: Any, symbol: str, start: datetime, end: datetime
) -> dict[datetime, float]:
    """Persisted Market State Index (composite score) by timestamp."""
    sql = """
        SELECT timestamp, composite_score
          FROM signal_scores
         WHERE underlying = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "start": start, "end": end})
        return {r[0]: float(r[1]) for r in cur.fetchall() if r[1] is not None}


# ---------------------------------------------------------------------------
# Market data for outcomes and controls
# ---------------------------------------------------------------------------


def fetch_underlying_bars(
    conn: Any, symbol: str, start: datetime, end: datetime
) -> list[tuple[datetime, float, float, float, float]]:
    """``(timestamp, open, high, low, close)`` minute bars, ascending."""
    sql = """
        SELECT timestamp, open, high, low, close
          FROM underlying_quotes
         WHERE symbol = %(symbol)s
           AND timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "start": start, "end": end})
        return [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in cur.fetchall()]


def fetch_vix_closes(conn: Any, start: datetime, end: datetime) -> list[tuple[datetime, float]]:
    """VIX 5-minute closes — the volatility-regime control."""
    sql = """
        SELECT timestamp, close
          FROM vix_bars
         WHERE timestamp BETWEEN %(start)s AND %(end)s
         ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"start": start, "end": end})
        return [(r[0], float(r[1])) for r in cur.fetchall() if r[1] is not None]


# ---------------------------------------------------------------------------
# Inventory support
# ---------------------------------------------------------------------------


def fetch_series_listing_dates(
    conn: Any, symbol: str, expirations: Sequence[date]
) -> dict[SeriesKey, date]:
    """First ET date each series appears in ZeroGEX's own chain history.

    This is the independent evidence that upgrades a series' censoring verdict
    from "first trade fell inside the data window" (a heuristic) to "observed
    from listing" (a fact about when the contract started existing in our
    data).  It is a lower bound on the true listing date — the contract cannot
    have been quoted here before ZeroGEX first saw it — which is the
    conservative direction: it can only ever mark a series as censored that was
    in fact clean, never the reverse.
    """
    if not expirations:
        return {}
    sql = """
        SELECT strike, expiration, option_type, MIN(timestamp) AS first_seen
          FROM option_chains
         WHERE underlying = %(symbol)s
           AND expiration = ANY(%(expirations)s)
         GROUP BY strike, expiration, option_type
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol, "expirations": list(expirations)})
        rows = cur.fetchall()
    out: dict[SeriesKey, date] = {}
    for strike, expiration, option_type, first_seen in rows:
        if first_seen is None:
            continue
        out[(symbol, expiration, float(strike), option_type)] = first_seen.date()
    return out


def fetch_open_interest_series(
    conn: Any,
    symbol: str,
    expirations: Sequence[date],
    start: date,
    end: date,
) -> dict[tuple[SeriesKey, date], int]:
    """End-of-session open interest per series per date, for reconciliation.

    Open interest is the independent yardstick the MM reconstruction is checked
    against (see :mod:`~.reconcile`): every contract opened by one participant
    is opened against another, so aggregate opening flow must move OI by the
    amount the feed says it did.
    """
    if not expirations:
        return {}
    sql = """
        SELECT DISTINCT ON ((timestamp AT TIME ZONE 'America/New_York')::date,
                            strike, expiration, option_type)
               (timestamp AT TIME ZONE 'America/New_York')::date AS d,
               strike, expiration, option_type, open_interest
          FROM option_chains
         WHERE underlying = %(symbol)s
           AND expiration = ANY(%(expirations)s)
           AND (timestamp AT TIME ZONE 'America/New_York')::date
               BETWEEN %(start)s AND %(end)s
         ORDER BY (timestamp AT TIME ZONE 'America/New_York')::date,
                  strike, expiration, option_type, timestamp DESC
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "symbol": symbol,
                "expirations": list(expirations),
                "start": start,
                "end": end,
            },
        )
        rows = cur.fetchall()
    out: dict[tuple[SeriesKey, date], int] = {}
    for d, strike, expiration, option_type, oi in rows:
        key: SeriesKey = (symbol, expiration, float(strike), option_type)
        out[(key, d)] = int(oi or 0)
    return out
