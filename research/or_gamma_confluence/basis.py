"""Carrying gamma levels onto the ES / NQ price axis, using production's ratio.

ZeroGEX computes gamma from INDEX option chains and never from options on
futures, so NQ's dealer book *is* NDX's book — what differs is the price axis.
:mod:`src.jobs.futures_projection` owns that mapping in production, including
the parts that are easy to get wrong and expensive to get wrong quietly: the
median over several print pairs, the 3% sanity bound that rejects a
back-adjusted series, the ``measured`` / ``measured_stale`` / ``carry``
labelling, and the cost-of-carry fallback.  None of that is restated here.

**Why an adapter is needed at all.**  ``resolve_basis`` is ``async`` and reads
through ``DatabaseManager`` (asyncpg).  Research runs synchronously on psycopg2.
So this module supplies a minimal object exposing the one coroutine
``resolve_basis`` calls, backed by a psycopg2 cursor, and drives it with
``asyncio.run``.  The SQL is a psycopg2 transcription of
``src/api/database.py:get_futures_basis_samples`` — same joins, same ordering,
same anchor semantics — because the placeholder styles are incompatible.  It is
transport only: every decision about what the ratio IS stays in production.

**Why the anchor matters.**  ``resolve_basis(at=...)`` exists precisely for
historical reads, and its own docstring says why: *"projecting a frame from
three months ago with today's ratio offsets every level by however much it has
moved since.  That silent offset is invisible on a chart and corrupts a
backtest."*  Every call here passes an explicit ``at`` — the instant the gamma
frame became available — and never ``None``.

**What is projected, and what is not.**  Prices only.  ``net_gex``, wall
strengths, pin scores and every other dollar or ratio quantity pass through
untouched, matching production's ``PRICE_FIELDS`` allowlist: dealer exposure is
a property of the option book, not of the axis you plot it on.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import datetime
from typing import Any, Optional

from research.or_gamma_confluence.instruments import InstrumentSpec
from research.or_gamma_confluence.levels import GammaSnapshot
from src.jobs.futures_projection import FuturesBasis, resolve_basis

logger = logging.getLogger(__name__)

__all__ = ["BasisReader", "BasisUnavailable", "resolve_basis_at", "project_snapshot"]


class BasisUnavailable(RuntimeError):
    """No basis could be resolved for a symbol that requires one.

    Raised rather than silently falling back to a ratio of 1.0, which would
    publish NDX levels on an NQ chart — an error of roughly 200 points, far
    larger than every confluence threshold under test.
    """


#: psycopg2 transcription of ``DatabaseManager.get_futures_basis_samples``.
#: The LIMIT is applied AFTER the join, and the lateral takes the newest index
#: print at or BEFORE each futures bar — both deliberate in the original; see
#: that method's docstring for why either change breaks the read.
_SAMPLES_SQL = """
    SELECT
        f.timestamp        AS observed_at,
        f.future_symbol    AS future_symbol,
        f.close::float8    AS future_close,
        u.close::float8    AS index_close
    FROM futures_quotes f
    JOIN LATERAL (
        SELECT close
        FROM underlying_quotes
        WHERE symbol = %(symbol)s
          AND timestamp <= f.timestamp
          AND timestamp >= f.timestamp - INTERVAL '120 seconds'
        ORDER BY timestamp DESC
        LIMIT 1
    ) u ON TRUE
    WHERE f.index_symbol = %(symbol)s
      AND f.timestamp <= %(at)s
      AND f.timestamp >= %(at)s - (%(lookback)s * INTERVAL '1 minute')
    ORDER BY f.timestamp DESC
    LIMIT %(limit)s
"""


class BasisReader:
    """Sync-backed stand-in for the object ``resolve_basis`` expects.

    Exposes exactly one coroutine — the method ``resolve_basis`` awaits — and
    memoises by ``(index_symbol, minute)``.  Basis is a carry ratio that walks
    toward expiry over months; within a minute it is constant, so the cache is
    a performance choice with no effect on any value.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

    async def get_futures_basis_samples(
        self,
        index_symbol: str,
        *,
        lookback_minutes: int = 5760,
        limit: int = 15,
        at: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        if at is None:  # pragma: no cover - research always anchors
            raise BasisUnavailable(
                "historical basis read requires an explicit anchor; "
                "at=None would apply today's ratio to a past frame"
            )
        key = (index_symbol.upper(), at.replace(second=0, microsecond=0).isoformat())
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    _SAMPLES_SQL,
                    {
                        "symbol": index_symbol.upper(),
                        "at": at,
                        "lookback": int(lookback_minutes),
                        "limit": int(limit),
                    },
                )
                rows = [
                    {
                        "observed_at": r[0],
                        "future_symbol": r[1],
                        "future_close": r[2],
                        "index_close": r[3],
                    }
                    for r in cur.fetchall()
                ]
        except Exception:
            logger.debug("basis sample read failed for %s", index_symbol, exc_info=True)
            try:
                self._conn.rollback()
            except Exception:
                pass
            rows = []
        self._cache[key] = rows
        return rows


def resolve_basis_at(
    reader: BasisReader, inst: InstrumentSpec, at: datetime
) -> Optional[FuturesBasis]:
    """Production's basis for ``inst`` at ``at``; ``None`` when not projectable.

    ``None`` is the honest answer for a cash symbol — there is nothing to
    project.  For a symbol whose spec says ``needs_basis``, ``None`` back from
    production means the pair is not configured, which is a misconfiguration
    rather than a data gap, so the caller raises.
    """
    basis = asyncio.run(resolve_basis(reader, inst.key, at=at))
    if basis is None and inst.needs_basis:
        raise BasisUnavailable(
            f"{inst.key} requires a futures basis but production reports no "
            f"projectable pair; check FUTURES_UNDERLYINGS_MAP"
        )
    return basis


def project_snapshot(
    snapshot: GammaSnapshot, basis: Optional[FuturesBasis], tick: Optional[float]
) -> GammaSnapshot:
    """Carry a snapshot's LEVEL PRICES onto the futures axis.

    Dollar and ratio quantities (``total_net_gex``, wall strengths, pin score,
    ``convexity_risk``) are returned unchanged — projecting them would invent
    exposure nobody holds.  ``spot`` is also left alone: it is the INDEX spot
    the ladder was ranked against, and it stays on the index axis so it remains
    comparable with the strikes it ranked.

    ``basis is None`` returns the snapshot untouched, so a cash symbol needs no
    branch at the call site.
    """
    if basis is None:
        return snapshot
    projected = tuple(
        replace(level, price=basis.project(level.price, tick=tick)) for level in snapshot.levels
    )
    return replace(snapshot, levels=projected)
