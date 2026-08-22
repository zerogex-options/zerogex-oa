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
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterator, Optional, Sequence

from research.mm_attributed_gex.gex import ChainQuote
from research.mm_attributed_gex.schema import SeriesKey

logger = logging.getLogger(__name__)

__all__ = [
    "DatabaseUnavailable",
    "RESEARCH_STATEMENT_TIMEOUT_MS",
    "research_connection",
    "fetch_chain_snapshot",
    "fetch_snapshot_timestamps",
    "fetch_production_summary",
    "fetch_production_summaries",
    "fetch_underlying_bars",
    "fetch_vix_closes",
    "fetch_series_listing_dates",
    "fetch_open_interest_series",
    "SampleAnchor",
    "fetch_sample_anchor",
]


class DatabaseUnavailable(RuntimeError):
    """Raised when the production database cannot be reached.

    Surfaced rather than swallowed: an experiment that silently produces an
    empty dataset because the DSN was wrong is worse than one that stops.
    """


#: Per-statement ceiling for research reads, in milliseconds.  The pool-wide
#: ``DB_STATEMENT_TIMEOUT_MS`` is sized for sub-second API queries; a research
#: sweep legitimately runs longer and is not on any latency path.  Raising it
#: here is scoped to this connection and restored on exit, so nothing else that
#: borrows the pooled connection inherits it.
RESEARCH_STATEMENT_TIMEOUT_MS = int(os.getenv("MMGEX_STATEMENT_TIMEOUT_MS", "300000"))


@contextmanager
def research_connection(*, statement_timeout_ms: Optional[int] = None) -> Iterator[Any]:
    """Yield a read-only production connection, or raise :class:`DatabaseUnavailable`.

    Wraps ``src.database.db_connection`` (the platform's pooled connection) and
    sets the session to READ ONLY, so a mistake in this package cannot write to
    a production table even in principle.  Also raises the per-statement
    timeout for the duration (see :data:`RESEARCH_STATEMENT_TIMEOUT_MS`) and
    restores the previous value afterwards.
    """
    try:
        from src.database import db_connection
    except Exception as exc:  # pragma: no cover - import guard
        raise DatabaseUnavailable(f"cannot import src.database: {exc}") from exc

    timeout = (
        RESEARCH_STATEMENT_TIMEOUT_MS if statement_timeout_ms is None else int(statement_timeout_ms)
    )
    try:
        with db_connection() as conn:
            previous_timeout: Optional[str] = None
            try:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION READ ONLY")
                    if timeout > 0:
                        cur.execute("SHOW statement_timeout")
                        row = cur.fetchone()
                        previous_timeout = row[0] if row else None
                        cur.execute(f"SET statement_timeout = {int(timeout)}")
            except Exception:  # pragma: no cover - some drivers/pools disallow
                logger.debug("could not configure the research connection")
            try:
                yield conn
            finally:
                if previous_timeout is not None:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"SET statement_timeout = '{previous_timeout}'")
                    except Exception:  # pragma: no cover
                        logger.debug("could not restore statement_timeout")
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
    lookback_hours: float = 0.5,
    include_archive: bool = True,
) -> list[ChainQuote]:
    """Latest quote per contract at or before ``ts``.

    Mirrors the production snapshot read (``AnalyticsEngine._get_snapshot``):
    ``DISTINCT ON (option_symbol) … ORDER BY option_symbol, timestamp DESC``
    within a bounded lookback, filtered to contracts that still have a gamma.
    Using the same read shape matters — a different contract universe would
    contaminate the comparison with a coverage difference.

    The lookback defaults NARROWER than production's 2 hours, because the two
    reads face different problems.  Production must survive a cold start where
    the newest row may be days old; this read targets a specific historical
    RTH minute where every active contract was quoted within a few minutes.
    The rows scanned scale linearly with the window, and a research sweep
    issues one of these per snapshot — hundreds of them — so a 4x wider
    window than the data needs is 4x the work, repeated hundreds of times.
    Widen it for a window that spans a market closure.
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
    conn: Any,
    symbol: str,
    expirations: Sequence[date],
    *,
    since: Optional[datetime] = None,
) -> dict[SeriesKey, date]:
    """First ET date each series appears in ZeroGEX's own chain history.

    This is the independent evidence that upgrades a censoring verdict from
    "first trade fell inside the data window" (a heuristic) to "observed from
    listing" (a fact about when the contract started existing in our data).

    ``since`` bounds the scan.  Without it this aggregates over the entire
    retained history of ``option_chains`` for those expirations — tens of
    millions of rows for a real SPX chain — and reliably exceeds the pool's
    statement timeout.

    **The bound cannot be applied naively.**  If the scan starts at ``since``,
    then ``MIN(timestamp)`` for a contract that was already trading before it
    equals ``since`` — and the series would look "listed at the window start"
    and be marked cleanly reconstructable when it is in fact left-censored.
    That is the one direction this experiment must never fail in, because it
    manufactures confidence in an inventory that does not have it.

    So a ``first_seen`` at the scan floor is treated as UNKNOWN and omitted:
    "the contract was listed then" and "our scan started then" are
    indistinguishable, and the caller falls back to the window heuristic, which
    is the conservative reading.  Only a first appearance strictly inside the
    scanned window is evidence of anything.
    """
    if not expirations:
        return {}
    params: dict[str, Any] = {"symbol": symbol, "expirations": list(expirations)}
    floor_clause = ""
    if since is not None:
        floor_clause = "AND timestamp >= %(since)s"
        params["since"] = since
    sql = f"""
        SELECT strike, expiration, option_type, MIN(timestamp) AS first_seen
          FROM option_chains
         WHERE underlying = %(symbol)s
           AND expiration = ANY(%(expirations)s)
           {floor_clause}
         GROUP BY strike, expiration, option_type
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    # A first appearance within this margin of the scan floor is not
    # distinguishable from the floor itself (the chain is quoted per minute, so
    # the very first bucket of the window is exactly where a pre-existing
    # contract shows up).
    floor_margin = timedelta(hours=12)
    out: dict[SeriesKey, date] = {}
    for strike, expiration, option_type, first_seen in rows:
        if first_seen is None:
            continue
        if since is not None and first_seen <= since + floor_margin:
            continue  # unknown, not "listed here"
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


@dataclass(frozen=True)
class SampleAnchor:
    """Real series and sessions drawn from ZeroGEX's own chain history.

    Used to anchor a synthetic Open-Close file to contracts that genuinely
    exist in the database, so the dataset and backtest steps can be rehearsed
    end to end before any Cboe data is purchased.  The MM *flow* over those
    series is still invented — only the series identities, the sessions and
    the market data around them are real.
    """

    symbol: str
    sessions: tuple[date, ...]
    expirations: tuple[date, ...]
    strikes: tuple[float, ...]
    start: datetime
    end: datetime
    spot: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "sessions": [d.isoformat() for d in self.sessions],
            "expirations": [d.isoformat() for d in self.expirations],
            "n_strikes": len(self.strikes),
            "strike_range": [min(self.strikes), max(self.strikes)] if self.strikes else [],
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "spot": self.spot,
        }


def fetch_sample_anchor(
    conn: Any,
    symbol: str = "SPX",
    *,
    days: int = 5,
    strike_band_pct: float = 0.03,
    max_expirations: int = 3,
    max_strikes: int = 15,
) -> Optional[SampleAnchor]:
    """Find real sessions, expirations and strikes to anchor a synthetic file to.

    Deliberately bounded — a narrow strike band around spot, a handful of
    expirations, a few sessions — because this runs against the production
    database and a rehearsal is not worth an expensive scan.  Returns ``None``
    when the window has no analytics rows to anchor to, which is itself useful:
    it means the study window you were planning has no production comparand.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(timestamp) FROM gex_summary WHERE underlying = %(symbol)s
            """,
            {"symbol": symbol},
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        end: datetime = row[0]
        start = end - timedelta(days=days * 2)

        # Sessions that actually have analytics rows — the snapshots the
        # dataset builder would iterate.
        cur.execute(
            """
            SELECT DISTINCT (timestamp AT TIME ZONE 'America/New_York')::date AS d
              FROM gex_summary
             WHERE underlying = %(symbol)s
               AND timestamp BETWEEN %(start)s AND %(end)s
             ORDER BY d DESC
             LIMIT %(days)s
            """,
            {"symbol": symbol, "start": start, "end": end, "days": days},
        )
        sessions = sorted(r[0] for r in cur.fetchall())
        if not sessions:
            return None

        cur.execute(
            """
            SELECT close FROM underlying_quotes
             WHERE symbol = %(symbol)s AND timestamp <= %(end)s
             ORDER BY timestamp DESC LIMIT 1
            """,
            {"symbol": symbol, "end": end},
        )
        spot_row = cur.fetchone()
        if not spot_row or not spot_row[0]:
            return None
        spot = float(spot_row[0])

        lo, hi = spot * (1 - strike_band_pct), spot * (1 + strike_band_pct)
        cur.execute(
            """
            SELECT DISTINCT expiration, strike
              FROM option_chains
             WHERE underlying = %(symbol)s
               AND timestamp BETWEEN %(from_ts)s AND %(end)s
               AND gamma IS NOT NULL
               AND expiration >= %(first_session)s
               AND strike BETWEEN %(lo)s AND %(hi)s
             ORDER BY expiration, strike
             LIMIT 2000
            """,
            {
                "symbol": symbol,
                "from_ts": end - timedelta(hours=6),
                "end": end,
                "first_session": sessions[0],
                "lo": lo,
                "hi": hi,
            },
        )
        pairs = cur.fetchall()

    if not pairs:
        return None
    expirations = sorted({p[0] for p in pairs})[:max_expirations]
    strikes = sorted({float(p[1]) for p in pairs if p[0] in expirations})
    # Thin evenly across the band rather than taking one end of it.
    if len(strikes) > max_strikes:
        step = len(strikes) / max_strikes
        strikes = [strikes[int(i * step)] for i in range(max_strikes)]

    return SampleAnchor(
        symbol=symbol,
        sessions=tuple(sessions),
        expirations=tuple(expirations),
        strikes=tuple(strikes),
        start=start,
        end=end,
        spot=spot,
    )
