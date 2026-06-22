"""Populate gex_historical_stats from rolling gex_summary history.

The /api/gex/historical-context endpoint and /gamma-pulse page need to be
able to tell the user "the current Net GEX at spot is in the 92nd
percentile of the last 30 days" or "this is an all-time record high".  We
pre-aggregate the per-symbol distributions of the two headline metrics
(``total_net_gex`` and ``net_gex_at_spot``) into ``gex_historical_stats``
so the endpoint becomes a single indexed lookup instead of a heavy
PERCENTILE_CONT pass over thousands of rows per request.

Granularity
    * window_label = '30d'      — rolling 30 calendar days of history
    * window_label = 'all_time' — everything in gex_summary
    * tod_bucket   = 0..77      — 5-minute RTH bucket index, 09:30 ET = 0
    * tod_bucket   = -1         — flat distribution (all RTH samples,
                                   no bucketing), used as fallback when a
                                   specific TOD bucket has too few samples

Time-of-day bucketing matters because intraday gamma has strong seasonality
(0DTE pinning is structurally larger at 15:55 ET than at 09:35 ET); a flat
"vs last 30 days" comparison would mis-flag the normal end-of-day spike as
"record-setting" every single afternoon.

Usage:
    python -m src.tools.gex_historical_stats_refresh
    python -m src.tools.gex_historical_stats_refresh --symbols SPY QQQ
    python -m src.tools.gex_historical_stats_refresh --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence

import numpy as np
import pytz

from src.database.connection import db_connection

logger = logging.getLogger(__name__)

# Per-metric refresh: each maps a logical name to the gex_summary column
# the distribution is computed over.  These two are the headline figures
# the live MetricCards expose, so they are the "is this unusual?" questions
# users most often want answered.  Add more by appending here — no schema
# change required.
METRICS: tuple[tuple[str, str], ...] = (
    ("net_gex_at_spot", "net_gex_at_spot"),
    ("total_net_gex", "total_net_gex"),
)

# Window definitions.  ``None`` for ``rolling_days`` means all_time (no
# trailing-window filter).
@dataclass(frozen=True)
class WindowSpec:
    label: str
    rolling_days: int | None


WINDOWS: tuple[WindowSpec, ...] = (
    WindowSpec(label="30d", rolling_days=30),
    WindowSpec(label="all_time", rolling_days=None),
)

# Below this we skip the per-bucket upsert — the percentile estimate is
# unreliable.  The flat (tod_bucket=-1) fallback still gets written so a
# thin bucket can lean on the all-RTH distribution for its regime label.
MIN_BUCKET_SAMPLES = 10
MIN_FLAT_SAMPLES = 100

# RTH window in ET — 09:30 to 16:00 = 78 five-minute buckets (0..77).
ET = pytz.timezone("US/Eastern")
RTH_BUCKETS = 78  # 6.5h / 5min


@dataclass(frozen=True)
class Distribution:
    p05: float
    p25: float
    p50: float
    p75: float
    p95: float
    mean: float
    std: float
    min_value: float
    max_value: float
    sample_size: int


def _summarize(samples: Sequence[float]) -> Distribution | None:
    arr = np.asarray(
        [float(v) for v in samples if v is not None and np.isfinite(v)],
        dtype=float,
    )
    if arr.size == 0:
        return None
    return Distribution(
        p05=float(np.percentile(arr, 5)),
        p25=float(np.percentile(arr, 25)),
        p50=float(np.percentile(arr, 50)),
        p75=float(np.percentile(arr, 75)),
        p95=float(np.percentile(arr, 95)),
        mean=float(arr.mean()),
        std=float(arr.std(ddof=1)) if arr.size > 1 else 0.0,
        min_value=float(arr.min()),
        max_value=float(arr.max()),
        sample_size=int(arr.size),
    )


def _active_symbols(cur) -> list[str]:
    cur.execute(
        "SELECT symbol FROM symbols WHERE COALESCE(is_active, TRUE) = TRUE ORDER BY symbol"
    )
    return [r[0] for r in cur.fetchall()]


def _fetch_rows(
    cur,
    symbol: str,
    column: str,
    rolling_days: int | None,
) -> list[tuple[datetime, float]]:
    """Fetch (timestamp, value) pairs for one (symbol, metric, window)."""
    # Inline the column name (it is a closed enum from METRICS, never user
    # input).  The where-clause params are bound normally.
    if rolling_days is None:
        query = f"""
            SELECT timestamp, {column}::double precision
            FROM gex_summary
            WHERE underlying = %s
              AND {column} IS NOT NULL
        """
        cur.execute(query, (symbol,))
    else:
        query = f"""
            SELECT timestamp, {column}::double precision
            FROM gex_summary
            WHERE underlying = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
              AND {column} IS NOT NULL
        """
        cur.execute(query, (symbol, str(rolling_days)))
    return [(r[0], r[1]) for r in cur.fetchall()]


def _tod_bucket_for(ts: datetime) -> int:
    """Return the 5-min ET RTH bucket index for ``ts``, or -1 if outside RTH."""
    et = ts.astimezone(ET)
    minutes_since_open = (et.hour - 9) * 60 + (et.minute - 30)
    if minutes_since_open < 0 or minutes_since_open >= RTH_BUCKETS * 5:
        return -1
    return minutes_since_open // 5


def _upsert(
    cur,
    symbol: str,
    metric: str,
    window_label: str,
    tod_bucket: int,
    dist: Distribution,
) -> None:
    cur.execute(
        """
        INSERT INTO gex_historical_stats (
            underlying, metric, window_label, tod_bucket,
            p05, p25, p50, p75, p95,
            mean, std, min_value, max_value,
            sample_size, refreshed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (underlying, metric, window_label, tod_bucket) DO UPDATE SET
            p05 = EXCLUDED.p05,
            p25 = EXCLUDED.p25,
            p50 = EXCLUDED.p50,
            p75 = EXCLUDED.p75,
            p95 = EXCLUDED.p95,
            mean = EXCLUDED.mean,
            std = EXCLUDED.std,
            min_value = EXCLUDED.min_value,
            max_value = EXCLUDED.max_value,
            sample_size = EXCLUDED.sample_size,
            refreshed_at = NOW()
        """,
        (
            symbol,
            metric,
            window_label,
            tod_bucket,
            dist.p05,
            dist.p25,
            dist.p50,
            dist.p75,
            dist.p95,
            dist.mean,
            dist.std,
            dist.min_value,
            dist.max_value,
            dist.sample_size,
        ),
    )


def refresh(
    conn,
    symbols: Iterable[str],
    dry_run: bool = False,
) -> dict[str, dict[str, dict[str, int]]]:
    """Refresh distributions for the given symbols.

    Returns a nested {symbol: {metric: {window: rows_written}}} map so
    callers (and the smoke test) can verify coverage.
    """
    results: dict[str, dict[str, dict[str, int]]] = {}
    with conn.cursor() as cur:
        for symbol in symbols:
            sym_upper = symbol.upper()
            results[sym_upper] = {}
            for metric_name, column in METRICS:
                results[sym_upper][metric_name] = {}
                for window in WINDOWS:
                    rows = _fetch_rows(cur, sym_upper, column, window.rolling_days)
                    rows_written = 0
                    if not rows:
                        logger.warning(
                            "%s/%s/%s: 0 samples — skipping",
                            sym_upper,
                            metric_name,
                            window.label,
                        )
                        results[sym_upper][metric_name][window.label] = 0
                        continue

                    # Bucket samples by 5-min ET RTH index; -1 = outside RTH.
                    by_bucket: dict[int, list[float]] = {}
                    rth_samples: list[float] = []
                    for ts, value in rows:
                        bucket = _tod_bucket_for(ts)
                        if bucket < 0:
                            continue
                        by_bucket.setdefault(bucket, []).append(value)
                        rth_samples.append(value)

                    # Flat distribution row (tod_bucket = -1).  Anchors the
                    # endpoint's fallback when a specific bucket is thin.
                    flat_dist = _summarize(rth_samples)
                    if flat_dist and flat_dist.sample_size >= MIN_FLAT_SAMPLES:
                        if not dry_run:
                            _upsert(cur, sym_upper, metric_name, window.label, -1, flat_dist)
                        rows_written += 1
                        logger.info(
                            "%s/%s/%s/flat: n=%d p50=%.3g p05=%.3g p95=%.3g min=%.3g max=%.3g",
                            sym_upper,
                            metric_name,
                            window.label,
                            flat_dist.sample_size,
                            flat_dist.p50,
                            flat_dist.p05,
                            flat_dist.p95,
                            flat_dist.min_value,
                            flat_dist.max_value,
                        )
                    elif flat_dist:
                        logger.warning(
                            "%s/%s/%s/flat: only %d samples (need >= %d) — skipping",
                            sym_upper,
                            metric_name,
                            window.label,
                            flat_dist.sample_size,
                            MIN_FLAT_SAMPLES,
                        )

                    # Per-bucket rows.
                    for bucket_idx in range(RTH_BUCKETS):
                        bucket_samples = by_bucket.get(bucket_idx, [])
                        dist = _summarize(bucket_samples)
                        if dist is None or dist.sample_size < MIN_BUCKET_SAMPLES:
                            continue
                        if not dry_run:
                            _upsert(
                                cur,
                                sym_upper,
                                metric_name,
                                window.label,
                                bucket_idx,
                                dist,
                            )
                        rows_written += 1
                    logger.info(
                        "%s/%s/%s: wrote %d rows (flat + %d buckets)",
                        sym_upper,
                        metric_name,
                        window.label,
                        rows_written,
                        rows_written - (1 if flat_dist and flat_dist.sample_size >= MIN_FLAT_SAMPLES else 0),
                    )
                    results[sym_upper][metric_name][window.label] = rows_written
    return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Symbols to refresh (default: all rows in symbols where is_active=TRUE).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute distributions but do not upsert to the table.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Log level (DEBUG, INFO, WARNING, ERROR).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    with db_connection() as conn:
        with conn.cursor() as cur:
            symbols = [s.upper() for s in args.symbols] if args.symbols else _active_symbols(cur)
        if not symbols:
            logger.warning("No symbols to refresh")
            return 0
        logger.info(
            "Refreshing gex_historical_stats for %d symbols (dry_run=%s)",
            len(symbols),
            args.dry_run,
        )
        refresh(conn, symbols, dry_run=args.dry_run)
        if args.dry_run:
            conn.rollback()
    return 0


if __name__ == "__main__":
    sys.exit(main())
