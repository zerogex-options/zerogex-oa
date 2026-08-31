"""Per-session Gamma Regime read — the stored history behind the live card.

:mod:`src.analytics.regime_shift` is pure maths over two lists of rows.  This
module is the thin DB-touching orchestration on top of it, and it exists so
exactly one implementation produces both halves of the Gamma Shift page:

* the **live** read the card renders (via ``/api/gex/regime-shift``), and
* the **stored** read one row per session (via
  ``src.tools.regime_session_refresh``) that the history strip renders and
  that supplies the z-score denominator for the live read.

Keeping them in one place is not tidiness — it is the only way the strip and
the card can agree.  If the refresh job computed a session's read even
slightly differently from the endpoint, today's bar in the history strip
would disagree with the headline directly above it, every day, and neither
number would be trustworthy.

``db`` is duck-typed throughout: anything exposing the three coroutines used
here works, which is what lets the tests drive this with an in-memory stub
instead of a live Postgres.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Protocol, Tuple

from zoneinfo import ZoneInfo

from src.analytics import regime_shift as rs

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

#: Trailing stored sessions used as the z-score denominator.  60 spans about
#: a quarter — long enough for a stable sigma, short enough that a change in
#: the underlying's own volatility regime eventually washes through it.
SIGMA_WINDOW_SESSIONS = 60

#: How late after the cash close the session-end snapshot is looked for.  The
#: analytics engine keeps writing for a short while past 16:00, and the last
#: row it writes is the one that should freeze into the session's read.
SESSION_END_ET = time(16, 5)


class SupportsRegimeReads(Protocol):
    """The slice of DatabaseManager this module needs."""

    async def get_gex_chain_at_ts(
        self, symbol: str, at_ts: datetime, max_rows: int = ...
    ) -> Dict[str, Any]:
        ...

    async def get_gex_summary_at_ts(
        self, symbol: str, at_ts: datetime
    ) -> Optional[Dict[str, Any]]:
        ...

    async def get_regime_sessions(
        self, symbol: str, limit: int = ..., before_date: Optional[date] = ...
    ) -> List[Dict[str, Any]]:
        ...


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and abs(out) != float("inf") else None


def session_open_utc(session: date) -> datetime:
    """09:30 ET on ``session``, in UTC."""
    return datetime.combine(session, time(9, 30), tzinfo=ET).astimezone(UTC)


def session_end_utc(session: date) -> datetime:
    """The instant the session's read is anchored to at its end."""
    return datetime.combine(session, SESSION_END_ET, tzinfo=ET).astimezone(UTC)


async def trailing_sigmas(
    db: SupportsRegimeReads,
    symbol: str,
    before: Optional[date],
    *,
    window: int = SIGMA_WINDOW_SESSIONS,
    scores: Optional[rs.ShiftScores] = None,
    hours: Optional[float] = None,
) -> Tuple[Optional[float], Optional[float], str, int]:
    """Standard deviations of the two axes over stored sessions.

    Returns ``(lean_sigma, stability_sigma, normalization, n_sessions)`` where
    ``normalization`` is one of:

    ``trailing``
        Enough stored sessions to compute the denominator from this symbol's
        own shift history.  The strong form of the claim.

    ``proxy``
        Bootstrapping off ``scores`` — a fraction of the near-spot gamma stock
        the change was measured against (:func:`regime_shift.proxy_sigma`).
        Surfaced on the payload so the card can say the magnitude is
        provisional rather than quietly printing "dramatically" off a
        three-day sample.

    ``none``
        Neither available.  Callers render the read without a magnitude claim.

    ``before`` excludes the in-progress session: a session must never
    normalize against itself, which would drag its own z toward zero exactly
    when the move is large.

    ``hours`` is the trading time the live window actually spans.  Every
    stored read is a full since-open session, so a shorter or longer window
    needs its yardstick rescaled before the comparison means anything — see
    :func:`regime_shift.horizon_factor`.  Both branches are scaled, because
    the proxy is a one-session scale too.
    """
    sessions = await db.get_regime_sessions(symbol, limit=window, before_date=before)
    leans = [v for v in (_f(s.get("lean_raw")) for s in sessions) if v is not None]
    stabs = [v for v in (_f(s.get("stability_raw")) for s in sessions) if v is not None]

    scale = rs.horizon_factor(hours) if hours is not None else 1.0

    if len(leans) >= rs.MIN_SESSIONS_FOR_SIGMA:
        lean_sigma = rs.robust_scale(leans)
        stab_sigma = rs.robust_scale(stabs)
        if lean_sigma or stab_sigma:
            # A degenerate axis (constant across the whole window) borrows the
            # other's sigma rather than falling to zero, which would make its
            # z infinite on the next non-constant session.
            return (
                (lean_sigma or stab_sigma) * scale,
                (stab_sigma or lean_sigma) * scale,
                "trailing",
                len(leans),
            )

    proxy = rs.proxy_sigma(scores) if scores is not None else None
    if proxy:
        scaled = proxy * scale
        return scaled, scaled, "proxy", len(leans)
    return None, None, "none", len(leans)


def rth_hours_between(start: Optional[datetime], end: Optional[datetime]) -> float:
    """Regular-trading hours between two instants, weekends removed.

    The horizon a live read actually spans, which is rarely the horizon its
    yardstick was built from: "since open" at 09:45 is a quarter of an hour,
    "vs last week" is five sessions, and both are normalized against a
    full-session sigma unless someone measures the difference.

    Overnight and weekend time is dropped because dealer gamma does not
    reposition while the chain is closed — counting it would make a
    day-over-day read look like a 24-hour move and understate it accordingly.

    A weekday filter, not a holiday calendar, matching :func:`recent_sessions`
    in this module: a holiday inside the window overstates the horizon by one
    session at most, which nudges the magnitude down rather than up, and the
    lookbacks this serves span at most a week.
    """
    if start is None or end is None:
        return 0.0
    if end < start:
        start, end = end, start
    lo = start.astimezone(ET)
    hi = end.astimezone(ET)

    total = 0.0
    day = lo.date()
    while day <= hi.date():
        if day.weekday() < 5:
            open_dt = datetime.combine(day, time(9, 30), tzinfo=ET)
            close_dt = datetime.combine(day, time(16, 0), tzinfo=ET)
            first = max(open_dt, lo)
            last = min(close_dt, hi)
            if last > first:
                total += (last - first).total_seconds() / 3600.0
        day += timedelta(days=1)
    return total


async def compute_session_read(
    db: SupportsRegimeReads,
    symbol: str,
    session_date: date,
    *,
    end_ts: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Build one session's stored read, ready for ``upsert_regime_session``.

    The read is always SINCE THE OPEN: A is the 09:30 ET snapshot, B is the
    freshest snapshot at or before ``end_ts`` (defaulting to just after the
    cash close).  Run intraday it answers "how has today gone so far"; run
    after the close — or during a backfill — it answers "what was that
    session", because B stops advancing once the engine stops writing.  Same
    function, same columns, no separate end-of-day path to drift.

    Returns ``None`` when the session has no usable pair of snapshots, so the
    caller skips the row rather than storing a read built on one frame.
    """
    open_ts = session_open_utc(session_date)
    close_ts = end_ts or session_end_utc(session_date)

    chain_b = await db.get_gex_chain_at_ts(symbol, close_ts)
    if chain_b.get("timestamp") is None:
        return None
    # Guard against an anchor that fell through to a PRIOR session: asking for
    # 16:05 on a day the engine never ran would otherwise silently return
    # yesterday's final frame and store it as today's read.
    if chain_b["timestamp"].astimezone(ET).date() != session_date:
        return None

    chain_a = await db.get_gex_chain_at_ts(symbol, open_ts)
    if chain_a.get("timestamp") is None or chain_a["timestamp"] == chain_b["timestamp"]:
        return None

    summary_a = await db.get_gex_summary_at_ts(symbol, chain_a["timestamp"])
    summary_b = await db.get_gex_summary_at_ts(symbol, chain_b["timestamp"])

    diff = rs.shift_rows(chain_a["rows"], chain_b["rows"])
    spot = chain_b.get("spot") or 0.0
    scores = rs.weighted_scores(diff.rows, spot)
    band = rs.concentration_band(diff.rows, spot=spot)
    rolloff = rs.rolloff_tranches(chain_b["rows"], as_of=session_date)

    # No ``hours`` here: the stored read IS the since-open session the
    # trailing sigma is built from, so its horizon factor is 1 by
    # construction.  Passing the measured elapsed time instead would rescale
    # every row against its own length and make the stored history
    # incomparable with itself.
    lean_sigma, stab_sigma, normalization, _n = await trailing_sigmas(
        db, symbol, before=session_date, scores=scores
    )
    read = rs.classify(
        rs.zscore(scores.lean, lean_sigma), rs.zscore(scores.stability, stab_sigma)
    )

    def level(summary: Optional[Dict[str, Any]], *names: str) -> Optional[float]:
        if not summary:
            return None
        for name in names:
            if summary.get(name) is not None:
                return _f(summary.get(name))
        return None

    next_tranche = rolloff.next_tranche
    return {
        "underlying": symbol.upper(),
        "session_date": session_date,
        "open_ts": chain_a["timestamp"],
        "close_ts": chain_b["timestamp"],
        "spot_open": chain_a.get("spot"),
        "spot_close": chain_b.get("spot"),
        # Raw, never clamped: these are the columns a future backfill's sigma
        # is computed from, so a normalized value here would freeze each row
        # against whatever history existed the day it was written.
        "lean_raw": scores.lean,
        "stability_raw": scores.stability,
        "net_shift": scores.net_shift,
        "gross_shift": scores.gross_shift,
        "sigma_price": scores.sigma_price,
        "lean_z": read.lean_z,
        "stability_z": read.stability_z,
        "magnitude": read.magnitude,
        "state": read.state,
        "normalization": normalization,
        "band_low": band.low if band.resolved else None,
        "band_high": band.high if band.resolved else None,
        "band_share": band.share,
        "band_resolved": band.resolved,
        "rolloff_expiration": next_tranche.expiration if next_tranche else None,
        "rolloff_net_gex": next_tranche.net_gex if next_tranche else None,
        "rolloff_abs_gex": next_tranche.abs_gex if next_tranche else None,
        "rolloff_share": next_tranche.share if next_tranche else None,
        "chain_abs_gex": rolloff.total_abs_gex,
        "gamma_flip_open": level(summary_a, "gamma_flip", "gamma_flip_point"),
        "gamma_flip_close": level(summary_b, "gamma_flip", "gamma_flip_point"),
        "call_wall_open": level(summary_a, "call_wall"),
        "call_wall_close": level(summary_b, "call_wall"),
        "put_wall_open": level(summary_a, "put_wall"),
        "put_wall_close": level(summary_b, "put_wall"),
        "common_expiration_count": len(diff.common_expirations),
        "expired_expiration_count": len(diff.expired_expirations),
        "expired_net_gex": diff.expired_net_gex,
    }


def recent_sessions(as_of: date, count: int) -> List[date]:
    """The last ``count`` weekdays at or before ``as_of``, newest first.

    A weekday filter, not a holiday calendar: a market holiday simply yields
    no snapshots and :func:`compute_session_read` returns ``None`` for it, so
    the backfill skips it without needing the calendar to be right.
    """
    out: List[date] = []
    cursor = as_of
    while len(out) < count:
        if cursor.weekday() < 5:
            out.append(cursor)
        cursor -= timedelta(days=1)
    return out
