"""Persistence for the intraday spread surface rollup.

One module so the UPSERT, the time-of-day bucketing rule and the
minimum-contract floors exist exactly once, shared by the live analytics
writer and the historical backfill.  Those two must produce interchangeable
rows: the API ranks today's reading inside a population made of both, and a
percentile across two subtly different definitions ranks the definitions.

The reduction itself is not here — it is
:func:`src.analytics.spread_stats.surface_scopes`, the same module the daily
rollup and the live API use.  This file only decides *where a row goes* and
*whether it is worth storing*.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, List, Sequence, Tuple

from src.analytics import spread_stats as spread_stats_mod
from src.market_calendar import trading_dte_map
from src.config import (
    SPREAD_SURFACE_BUCKET_MINUTES,
    SPREAD_SURFACE_MIN_BUCKET_CONTRACTS,
    SPREAD_SURFACE_MIN_CONTRACTS,
)

logger = logging.getLogger(__name__)

__all__ = [
    "SESSION_START_MIN",
    "SESSION_END_MIN",
    "bucket_start_minutes",
    "bucket_label",
    "is_publishable",
    "SURFACE_UPSERT_SQL",
    "surface_param_rows",
    "store_surface_scopes",
]

#: Cash-session bounds in minutes past ET midnight — 09:30 and 16:00.
#: Half-open [start, end): the 15:30 bucket is the last one written, so the
#: closing rotation (which prints wide and then settles) never becomes a
#: session of its own.  The live writer gates on these and the read path
#: clamps to them, so a quote timestamped outside the session is ranked
#: against the nearest bucket that has history rather than against nothing.
SESSION_START_MIN: int = 9 * 60 + 30
SESSION_END_MIN: int = 16 * 60


def bucket_start_minutes(when_et: dt.datetime) -> int:
    """Floor an ET wall-clock time to the surface's time-of-day grid.

    Returns minutes past ET midnight.  The caller is responsible for handing
    over an ET-localised datetime — this module holds no timezone, so the
    same function serves the live writer (which has one) and the backfill
    (which derives one per historical session).
    """
    width = max(1, int(SPREAD_SURFACE_BUCKET_MINUTES))
    return (when_et.hour * 60 + when_et.minute) // width * width


def bucket_label(bucket_min: int) -> str:
    """``930`` -> ``'09:30-10:00 ET'``. What the page tells the reader it matched."""
    width = max(1, int(SPREAD_SURFACE_BUCKET_MINUTES))
    end = bucket_min + width
    return (
        f"{bucket_min // 60:02d}:{bucket_min % 60:02d}"
        f"-{(end // 60) % 24:02d}:{end % 60:02d} ET"
    )


def is_publishable(scope: spread_stats_mod.SurfaceScope) -> bool:
    """Does this cell carry enough contracts to be a measurement?

    Two floors, because a whole band and one slice of it are different
    questions — see ``SPREAD_SURFACE_MIN_CONTRACTS`` in config for why the
    slice floor is so much lower and why neither is the daily rollup's 100.

    A cell with no usable median is dropped whatever its count: every
    contract in it was no-bid, locked or crossed, and there is no width to
    record.  The coverage percentages for that population survive on the
    band-wide row, which is where the page reads them from.
    """
    agg = scope.aggregate
    if agg.median_relative_spread_pct is None:
        return False
    floor = (
        SPREAD_SURFACE_MIN_CONTRACTS
        if scope.money_bucket == spread_stats_mod.BAND_WIDE
        else SPREAD_SURFACE_MIN_BUCKET_CONTRACTS
    )
    return agg.contract_count >= floor


SURFACE_UPSERT_SQL = """
    INSERT INTO spread_surface_stats (
        underlying, trading_date, bucket_start_min, option_type,
        dte_scope, band_pct, money_bucket, spot_price,
        contract_count, tradable_count, two_sided_pct, zero_bid_pct,
        crossed_or_locked_pct, median_relative_spread_pct,
        p90_relative_spread_pct, median_spread, source_timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (underlying, trading_date, bucket_start_min, option_type,
                 dte_scope, band_pct, money_bucket) DO UPDATE
    SET spot_price                 = EXCLUDED.spot_price,
        contract_count             = EXCLUDED.contract_count,
        tradable_count             = EXCLUDED.tradable_count,
        two_sided_pct              = EXCLUDED.two_sided_pct,
        zero_bid_pct               = EXCLUDED.zero_bid_pct,
        crossed_or_locked_pct      = EXCLUDED.crossed_or_locked_pct,
        median_relative_spread_pct = EXCLUDED.median_relative_spread_pct,
        p90_relative_spread_pct    = EXCLUDED.p90_relative_spread_pct,
        median_spread              = EXCLUDED.median_spread,
        source_timestamp           = EXCLUDED.source_timestamp,
        updated_at                 = NOW()
"""


def surface_param_rows(
    underlying: str,
    trading_date: dt.date,
    bucket_min: int,
    option_type: str,
    spot: float,
    source_ts: dt.datetime,
    scopes: Sequence[spread_stats_mod.SurfaceScope],
) -> List[Tuple[Any, ...]]:
    """Parameter tuples for :data:`SURFACE_UPSERT_SQL`, floors applied."""
    rows: List[Tuple[Any, ...]] = []
    for scope in scopes:
        if not is_publishable(scope):
            continue
        agg = scope.aggregate
        rows.append(
            (
                underlying,
                trading_date,
                int(bucket_min),
                option_type,
                scope.dte_scope,
                float(scope.band_pct),
                scope.money_bucket,
                spot,
                agg.contract_count,
                agg.tradable_count,
                agg.two_sided_pct,
                agg.zero_bid_pct,
                agg.crossed_or_locked_pct,
                agg.median_relative_spread_pct,
                agg.p90_relative_spread_pct,
                agg.median_spread,
                source_ts,
            )
        )
    return rows


def store_surface_scopes(
    cursor,
    underlying: str,
    trading_date: dt.date,
    bucket_min: int,
    spot: float,
    source_ts: dt.datetime,
    spreads_by_type: Dict[str, Sequence[spread_stats_mod.ContractSpread]],
    dte_of: Dict[Any, int],
) -> int:
    """Compute and UPSERT every publishable surface cell. Returns rows written.

    ``spreads_by_type`` maps ``'C'``/``'P'`` to that side's contract readings.
    Sides are kept apart all the way down: the page never blends them, because
    the question it exists for is whether the PUTS specifically have gone wide.

    The trading-session distances are derived here rather than asked of the
    caller. Every writer already hands over ``trading_date`` and the
    expirations, so deriving it once at the funnel is the only way the live
    engine and both backfills cannot drift into measuring different buckets
    — which is the same reason the reduction itself lives in one module.
    """
    trading_dte_of = trading_dte_map(dte_of.keys(), trading_date)
    written = 0
    for option_type, spreads in spreads_by_type.items():
        if not spreads:
            continue
        scopes = spread_stats_mod.surface_scopes(spreads, dte_of, trading_dte_of)
        rows = surface_param_rows(
            underlying, trading_date, bucket_min, option_type, spot, source_ts, scopes
        )
        for row in rows:
            cursor.execute(SURFACE_UPSERT_SQL, row)
        written += len(rows)
    return written
