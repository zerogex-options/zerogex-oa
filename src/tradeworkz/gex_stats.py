"""Per-symbol GEX normalization for the TradeWorkz regime gates.

The bots used to classify the dealer-gamma regime off absolute dollar
constants (``net_gex >= 1.5e9`` == "positive_strong", ``gex / 3.0e9`` for
the wall-bouncer's quality score). Net dollar-gamma scales with the
underlying's price level squared times open interest, so those constants
are only meaningful at the scale they were tuned for (SPY). SPX trades
~10x SPY's level and runs an order of magnitude larger in dollar-gamma; a
``1.5e9`` "strong" line is background noise there, and the reverse for a
smaller symbol.

This module normalizes ``net_gex_at_spot`` against *the symbol's own*
history. The analytics platform already pre-aggregates that distribution
per symbol in ``gex_historical_stats`` (see
``src/tools/gex_historical_stats_refresh.py``): rolling 30-day percentile
breakpoints, bucketed by 5-minute ET time-of-day (0DTE gamma has strong
intraday seasonality — the 15:55 pin dwarfs the 09:35 read). We read the
matching row and interpolate where the live value falls, yielding a 0..100
percentile that means the same thing for every symbol.

Every lookup is best-effort: any failure (table absent, no history for a
new symbol, thin bucket) returns ``None`` so the caller falls back to the
old absolute thresholds. Regime classification therefore never breaks —
it just loses its symbol-relative precision until the stats exist.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_RTH_BUCKETS = 78  # 09:30-16:00 ET in 5-min buckets (0..77)

# Metric + window we normalize against — matches the rows the refresh tool
# writes and the /gamma-pulse endpoint reads.
_METRIC = "net_gex_at_spot"
_WINDOW = "30d"

# Wall-strength metrics, one per side — the refresh tool aggregates these
# from the gex_summary.{call,put}_wall_strength columns. Mirrors _METRIC.
_WALL_METRIC = {"call": "call_wall_strength", "put": "put_wall_strength"}


def tod_bucket_for(ts: datetime) -> int:
    """5-minute ET RTH bucket index for ``ts`` (0..77), or -1 outside RTH.

    Mirrors ``gex_historical_stats_refresh._tod_bucket_for`` so a live
    lookup lands in the same bucket the distribution was aggregated into.
    A naive timestamp is treated as UTC, consistent with the rest of the
    snapshot layer.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    et = ts.astimezone(_ET)
    minutes_since_open = (et.hour - 9) * 60 + (et.minute - 30)
    if minutes_since_open < 0 or minutes_since_open >= _RTH_BUCKETS * 5:
        return -1
    return minutes_since_open // 5


def _clean_knots(knots: List[Tuple[float, Any]]) -> List[Tuple[float, float]]:
    """Keep finite (pct, value) knots, enforcing a non-decreasing value axis."""
    clean: List[Tuple[float, float]] = []
    for pct, raw in knots:
        if raw is None:
            continue
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        if v != v or v in (float("inf"), float("-inf")):  # NaN / inf guard
            continue
        if clean and v < clean[-1][1]:
            v = clean[-1][1]  # clamp inversions so interpolation stays monotone
        clean.append((pct, v))
    return clean


def percentile_rank(
    value: float,
    p05: Any,
    p25: Any,
    p50: Any,
    p75: Any,
    p95: Any,
) -> Optional[float]:
    """Interpolate ``value``'s percentile (0..100) from 5 breakpoints.

    Piecewise-linear between the known percentile knots, extrapolating with
    the end-segment slope beyond p05 / p95 and clamped to [0, 100]. Returns
    ``None`` when there aren't at least two usable knots, and 50.0 for a
    degenerate (flat) distribution where every knot collapses to one value.
    """
    knots = _clean_knots([(5.0, p05), (25.0, p25), (50.0, p50), (75.0, p75), (95.0, p95)])
    if len(knots) < 2:
        return None
    if knots[0][1] == knots[-1][1]:
        return 50.0

    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    def _interp(a: Tuple[float, float], b: Tuple[float, float]) -> float:
        (p_a, v_a), (p_b, v_b) = a, b
        if v_b == v_a:
            return p_a
        return p_a + (v - v_a) * (p_b - p_a) / (v_b - v_a)

    if v <= knots[0][1]:
        return max(0.0, min(100.0, _interp(knots[0], knots[1])))
    if v >= knots[-1][1]:
        return max(0.0, min(100.0, _interp(knots[-2], knots[-1])))
    for a, b in zip(knots, knots[1:]):
        if a[1] <= v <= b[1]:
            return max(0.0, min(100.0, _interp(a, b)))
    return knots[-1][0]


def fetch_net_gex_pctile(
    conn: Any,
    underlying: str,
    net_gex_at_spot: Optional[float],
    ts: datetime,
) -> Optional[float]:
    """Percentile of ``net_gex_at_spot`` in the symbol's 30d distribution.

    Looks up the ``gex_historical_stats`` row for the current 5-minute ET
    bucket, falling back to the flat (``tod_bucket = -1``) distribution
    when the specific bucket has no row. Returns ``None`` — and clears any
    aborted transaction state so the caller's next query still works — on
    any failure or missing data.
    """
    if net_gex_at_spot is None:
        return None
    bucket = tod_bucket_for(ts)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p05, p25, p50, p75, p95
            FROM gex_historical_stats
            WHERE underlying = %s AND metric = %s AND window_label = %s
              AND tod_bucket = ANY(%s)
            ORDER BY (tod_bucket = %s) DESC
            LIMIT 1
            """,
            (underlying, _METRIC, _WINDOW, [bucket, -1], bucket),
        )
        row = cur.fetchone()
    except Exception:  # pragma: no cover - defensive: missing table etc.
        logger.debug("gex_historical_stats lookup failed for %s", underlying, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    if not row:
        return None
    return percentile_rank(net_gex_at_spot, *row)


def fetch_wall_strength_pctile(
    conn: Any,
    underlying: str,
    side: str,
    wall_strength: Optional[float],
    ts: datetime,
) -> Optional[float]:
    """Percentile (0..100) of a wall's dollar-gamma strength in the symbol's
    30d, time-of-day-bucketed distribution — the wall analog of
    :func:`fetch_net_gex_pctile`.

    ``side`` selects the metric: ``'call'`` -> ``call_wall_strength``,
    ``'put'`` -> ``put_wall_strength``. Best-effort like its sibling:
    returns ``None`` — and clears any aborted transaction state so the
    caller's next query still works — on any failure, unknown side,
    missing value, or absent history (a brand-new column has no history
    until the nightly refresh accumulates it), so the caller falls back to
    neutral, wall-agnostic sizing.
    """
    if wall_strength is None:
        return None
    metric = _WALL_METRIC.get((side or "").lower())
    if metric is None:
        return None
    bucket = tod_bucket_for(ts)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p05, p25, p50, p75, p95
            FROM gex_historical_stats
            WHERE underlying = %s AND metric = %s AND window_label = %s
              AND tod_bucket = ANY(%s)
            ORDER BY (tod_bucket = %s) DESC
            LIMIT 1
            """,
            (underlying, metric, _WINDOW, [bucket, -1], bucket),
        )
        row = cur.fetchone()
    except Exception:  # pragma: no cover - defensive: missing table etc.
        logger.debug(
            "wall_strength pctile lookup failed for %s/%s",
            underlying,
            side,
            exc_info=True,
        )
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    if not row:
        return None
    return percentile_rank(wall_strength, *row)
