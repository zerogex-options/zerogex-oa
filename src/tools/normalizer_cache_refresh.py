"""Populate component_normalizer_cache from rolling historical magnitudes.

The signal engine reads per-symbol saturation levels (p95 of |value|) for
several scoring fields out of ``component_normalizer_cache`` so that the
score's [-1, +1] clip lines up with each underlying's actual magnitude
distribution.  Without these rows the engine falls back to env-var
defaults, which were calibrated for SPY-magnitude underlyings and tend to
saturate at non-SPY scales (or even on SPY when the default pre-dated a
units change — the original ``_VC_NORM=5e7`` is the canonical example).

This script computes the rolling distribution (p05, p50, p95, std) of the
fields the engine consumes and upserts one row per (underlying, field)
into ``component_normalizer_cache``.  Run nightly (or after a market-
structure change) so the cache stays current.

Usage:
    python -m src.tools.normalizer_cache_refresh
    python -m src.tools.normalizer_cache_refresh --symbols SPY QQQ
    python -m src.tools.normalizer_cache_refresh --window-days 30 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np

from src.database.connection import db_connection

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 20
MIN_SAMPLES = 30  # below this we skip the upsert — distribution unreliable


@dataclass(frozen=True)
class FieldSpec:
    """How to sample one normalizer field's magnitude distribution.

    ``query`` must select a single column of sample values (one row per
    sample) and accept two parameters: the underlying symbol and the
    look-back window in days (as an int, interpolated into an INTERVAL
    expression by psycopg2).
    """

    name: str
    query: str
    notes: str = ""


# Each spec returns one sample per row.  We fetch them, drop NULLs, then
# compute |sample| percentiles + signed std in Python.
FIELD_SPECS: tuple[FieldSpec, ...] = (
    FieldSpec(
        name="dealer_vanna_exposure",
        query="""
            SELECT SUM(dealer_vanna_exposure)::double precision
            FROM gex_by_strike
            WHERE underlying = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
              AND dealer_vanna_exposure IS NOT NULL
            GROUP BY timestamp
        """,
        notes="Sum of dealer vanna across strikes/expirations per timestamp.",
    ),
    FieldSpec(
        name="dealer_charm_exposure",
        query="""
            SELECT SUM(dealer_charm_exposure)::double precision
            FROM gex_by_strike
            WHERE underlying = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
              AND dealer_charm_exposure IS NOT NULL
            GROUP BY timestamp
        """,
        notes="Sum of dealer charm across strikes/expirations per timestamp.",
    ),
    FieldSpec(
        name="local_gex",
        query="""
            SELECT local_gex::double precision
            FROM gex_summary
            WHERE underlying = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
              AND local_gex IS NOT NULL
        """,
        notes="Per-timestamp local_gex from gex_summary (consumed by local_gamma).",
    ),
    FieldSpec(
        name="net_gex_delta",
        query="""
            SELECT (total_net_gex - LAG(total_net_gex)
                     OVER (ORDER BY timestamp))::double precision
            FROM gex_summary
            WHERE underlying = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
        """,
        notes="Cycle-over-cycle change in total_net_gex.",
    ),
    FieldSpec(
        name="call_flow_delta",
        query="""
            WITH windows AS (
                SELECT
                    DATE_TRUNC('hour', timestamp)
                      + (FLOOR(EXTRACT(MINUTE FROM timestamp) / 15)::int
                         * INTERVAL '15 minutes') AS bucket,
                    SUM(COALESCE(buy_premium, 0)
                        - COALESCE(sell_premium, 0))::double precision AS net_premium
                FROM flow_contract_facts
                WHERE symbol = %s
                  AND option_type = 'C'
                  AND timestamp >= NOW() - (%s || ' days')::interval
                GROUP BY bucket
            )
            SELECT (net_premium - LAG(net_premium) OVER (ORDER BY bucket))::double precision
            FROM windows
        """,
        notes="15-min-window-over-window change in call net premium (Lee-Ready signed).",
    ),
    FieldSpec(
        name="put_flow_delta",
        query="""
            WITH windows AS (
                SELECT
                    DATE_TRUNC('hour', timestamp)
                      + (FLOOR(EXTRACT(MINUTE FROM timestamp) / 15)::int
                         * INTERVAL '15 minutes') AS bucket,
                    SUM(COALESCE(buy_premium, 0)
                        - COALESCE(sell_premium, 0))::double precision AS net_premium
                FROM flow_contract_facts
                WHERE symbol = %s
                  AND option_type = 'P'
                  AND timestamp >= NOW() - (%s || ' days')::interval
                GROUP BY bucket
            )
            SELECT (net_premium - LAG(net_premium) OVER (ORDER BY bucket))::double precision
            FROM windows
        """,
        notes="15-min-window-over-window change in put net premium.",
    ),
    # Smart-money calibration (D6 follow-up).  These two are NOT consumed
    # via ctx.extra['normalizers'] like the specs above — they are read
    # directly by AnalyticsEngine._refresh_flow_caches to replace the
    # static smart-money tier thresholds with the per-symbol upper
    # percentile of recent per-contract flow ("unusual = upper pct").
    # Sampled from the canonical flow_contract_facts so the distribution
    # matches what the smart-money SQL scores (volume_delta and the
    # volume_delta*price*100 premium).
    FieldSpec(
        name="smart_money_volume_delta",
        query="""
            SELECT volume_delta::double precision
            FROM flow_contract_facts
            WHERE symbol = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
              AND volume_delta > 0
        """,
        notes="Per-contract per-cycle volume_delta; p95 calibrates smart-money volume tiers.",
    ),
    FieldSpec(
        name="smart_money_premium",
        query="""
            SELECT premium_delta::double precision
            FROM flow_contract_facts
            WHERE symbol = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
              AND volume_delta > 0
        """,
        notes="Per-contract per-cycle premium (volume_delta*price*100); p95 calibrates smart-money premium tiers.",
    ),
)


@dataclass(frozen=True)
class Distribution:
    p05: float
    p50: float
    p95: float
    std: float
    sample_size: int


def _summarize(samples: Sequence[float]) -> Distribution | None:
    """Return percentile/std summary of |samples|, or None if too small."""
    arr = np.asarray(
        [float(v) for v in samples if v is not None and np.isfinite(v)],
        dtype=float,
    )
    if arr.size < MIN_SAMPLES:
        return None
    abs_arr = np.abs(arr)
    return Distribution(
        p05=float(np.percentile(abs_arr, 5)),
        p50=float(np.percentile(abs_arr, 50)),
        p95=float(np.percentile(abs_arr, 95)),
        std=float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0,
        sample_size=int(arr.size),
    )


def _active_symbols(cur) -> list[str]:
    cur.execute("SELECT symbol FROM symbols WHERE COALESCE(is_active, TRUE) = TRUE ORDER BY symbol")
    return [r[0] for r in cur.fetchall()]


def _fetch_samples(cur, spec: FieldSpec, symbol: str, window_days: int) -> list[float]:
    cur.execute(spec.query, (symbol, str(window_days)))
    return [r[0] for r in cur.fetchall() if r[0] is not None]


def _upsert(cur, symbol: str, field: str, window_days: int, dist: Distribution) -> None:
    cur.execute(
        """
        INSERT INTO component_normalizer_cache (
            underlying, field_name, window_days,
            p05, p50, p95, std, sample_size, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (underlying, field_name) DO UPDATE SET
            window_days = EXCLUDED.window_days,
            p05 = EXCLUDED.p05,
            p50 = EXCLUDED.p50,
            p95 = EXCLUDED.p95,
            std = EXCLUDED.std,
            sample_size = EXCLUDED.sample_size,
            updated_at = NOW()
        """,
        (
            symbol,
            field,
            window_days,
            dist.p05,
            dist.p50,
            dist.p95,
            dist.std,
            dist.sample_size,
        ),
    )


def refresh(
    conn,
    symbols: Iterable[str],
    window_days: int = DEFAULT_WINDOW_DAYS,
    dry_run: bool = False,
) -> dict[str, dict[str, Distribution | None]]:
    """Refresh normalizer rows for the given symbols.  Returns a per-symbol
    map of {field: Distribution|None} so callers can introspect results."""
    results: dict[str, dict[str, Distribution | None]] = {}
    with conn.cursor() as cur:
        for symbol in symbols:
            sym_upper = symbol.upper()
            results[sym_upper] = {}
            for spec in FIELD_SPECS:
                samples = _fetch_samples(cur, spec, sym_upper, window_days)
                dist = _summarize(samples)
                results[sym_upper][spec.name] = dist
                if dist is None:
                    logger.warning(
                        "%s/%s: only %d samples (need >= %d) — skipping",
                        sym_upper,
                        spec.name,
                        len(samples),
                        MIN_SAMPLES,
                    )
                    continue
                logger.info(
                    "%s/%s: n=%d p05=%.3g p50=%.3g p95=%.3g std=%.3g",
                    sym_upper,
                    spec.name,
                    dist.sample_size,
                    dist.p05,
                    dist.p50,
                    dist.p95,
                    dist.std,
                )
                if not dry_run:
                    _upsert(cur, sym_upper, spec.name, window_days, dist)
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
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help=f"Look-back window in days (default: {DEFAULT_WINDOW_DAYS}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and log distributions but do not write to the cache.",
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

    if args.window_days <= 0:
        parser.error("--window-days must be positive")

    with db_connection() as conn:
        with conn.cursor() as cur:
            symbols = [s.upper() for s in args.symbols] if args.symbols else _active_symbols(cur)
        if not symbols:
            logger.warning("No symbols to refresh")
            return 0
        logger.info(
            "Refreshing normalizer cache for %d symbols (window=%d days, dry_run=%s)",
            len(symbols),
            args.window_days,
            args.dry_run,
        )
        refresh(conn, symbols, window_days=args.window_days, dry_run=args.dry_run)
        # ``with db_connection()`` commits on successful exit; explicit
        # rollback for dry-run keeps any incidental statements (none here)
        # from leaking.
        if args.dry_run:
            conn.rollback()
    return 0


if __name__ == "__main__":
    sys.exit(main())
