"""Durable per-key API usage metering.

Every authenticated request increments an in-process counter keyed by
``(UTC day, caller_user_id, caller_key_id, end_user_id)``. A background
task flushes the accumulated counts to the ``api_usage_daily`` table on a
fixed interval with an increment-UPSERT, so the per-customer request
totals:

* **survive restarts** — counts are durable once flushed and a final
  flush runs on shutdown;
* **aggregate across workers and hosts** — each worker keeps its own
  in-memory map and the ``ON CONFLICT ... DO UPDATE SET request_count =
  request_count + EXCLUDED.request_count`` merge sums them server-side, so
  running N uvicorn workers needs no shared in-memory state.

This is the metering substrate for usage-based API billing and per-tier
quotas: a B2B caller is ``caller_user_id``; a B2B2C end-user (attributed
via the website's ``X-End-User-Token``) is ``end_user_id``, so the same
table supports both per-account and per-seat billing.

**Off by default.** With ``API_USAGE_METERING_ENABLED`` unset,
:meth:`UsageMeter.record` is a cheap no-op and :meth:`UsageMeter.start`
launches no task, so wiring the middleware in cannot change behavior
until the flag is flipped.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or default).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_ENABLED = _env_flag("API_USAGE_METERING_ENABLED")
_FLUSH_INTERVAL = float(os.getenv("API_USAGE_FLUSH_SECONDS", "60") or "60")
_MAX_KEYS = int(os.getenv("API_USAGE_MAX_KEYS", "200000") or "200000")

# Sentinels for the NOT NULL primary-key columns when a dimension is
# absent (anonymous caller, no per-user key id, no end-user token). They
# mirror the column DEFAULTs in ``api_usage_daily`` so a row aggregated
# in memory and a row inserted directly land on the same PK.
_NO_USER = "-"
_NO_END_USER = "-"
_NO_KEY_ID = 0

# (day, caller_user_id, caller_key_id, end_user_id)
_Bucket = Tuple[date, str, int, str]

_UPSERT_SQL = """
    INSERT INTO api_usage_daily
        (day, caller_user_id, caller_key_id, end_user_id,
         request_count, error_count, first_seen_at, last_seen_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
    ON CONFLICT (day, caller_user_id, caller_key_id, end_user_id)
    DO UPDATE SET
        request_count = api_usage_daily.request_count + EXCLUDED.request_count,
        error_count   = api_usage_daily.error_count + EXCLUDED.error_count,
        last_seen_at  = NOW()
"""


class UsageMeter:
    """In-process request aggregation with periodic durable flush.

    Mirrors the ``key_store`` lifecycle: a lazy pool *getter* is registered
    at startup so a ``DatabaseManager`` reconnect (which swaps ``self.pool``)
    is picked up transparently on the next flush.
    """

    def __init__(self) -> None:
        self._get_pool: Optional[Callable[[], Any]] = None
        # bucket -> [request_count, error_count]
        self._counts: Dict[_Bucket, List[int]] = {}
        self._task: Optional[asyncio.Task] = None
        self._dropped_buckets = 0

    # -- lifecycle ---------------------------------------------------------

    def configure(self, get_pool: Optional[Callable[[], Any]]) -> None:
        """Register (or clear, with ``None``) the DB-pool getter."""
        self._get_pool = get_pool

    def start(self) -> None:
        """Launch the background flush loop (no-op when disabled).

        Must be called from within a running event loop (the lifespan
        startup hook). Idempotent: a second call while a task is live is
        ignored.
        """
        if not _ENABLED:
            return
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Cancel the flush loop and flush whatever remains."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.warning("usage flush loop exited with error", exc_info=True)
        # Final drain so a clean shutdown never loses the last window.
        await self.flush()

    # -- hot path ----------------------------------------------------------

    def record(self, identity: Any, status_code: int) -> None:
        """Increment the counter for ``identity`` (no-op when disabled).

        Synchronous and allocation-light: called once per request from the
        metering middleware. ``identity`` is the ``RequestIdentity`` the
        auth dependency resolved (or ``None`` / anonymous).
        """
        if not _ENABLED:
            return
        day = datetime.now(timezone.utc).date()
        caller_user_id = getattr(identity, "caller_user_id", None) or _NO_USER
        caller_key_id = getattr(identity, "caller_key_id", None) or _NO_KEY_ID
        end_user_id = getattr(identity, "end_user_id", None) or _NO_END_USER
        bucket: _Bucket = (day, caller_user_id, int(caller_key_id), end_user_id)

        entry = self._counts.get(bucket)
        if entry is None:
            if len(self._counts) >= _MAX_KEYS:
                # Cardinality guard: drop rather than grow unbounded. The
                # next flush empties the map, so this is self-correcting.
                self._dropped_buckets += 1
                return
            entry = [0, 0]
            self._counts[bucket] = entry
        entry[0] += 1
        if status_code >= 500:
            entry[1] += 1

    # -- flush -------------------------------------------------------------

    async def flush(self) -> int:
        """Persist and clear accumulated counts. Returns rows written.

        On DB failure the snapshot is merged back so counts are retried on
        the next flush rather than lost (additive: re-adding sums with any
        requests recorded since the swap).
        """
        if not self._counts:
            return 0
        pool = self._get_pool() if self._get_pool is not None else None
        if pool is None:
            return 0

        # Atomic swap — no await between read and clear, so no request is
        # lost to a concurrent record() on this single-threaded loop.
        snapshot = self._counts
        self._counts = {}

        rows = [
            (day, user, key_id, end_user, counts[0], counts[1])
            for (day, user, key_id, end_user), counts in snapshot.items()
        ]
        try:
            async with pool.acquire() as conn:
                await conn.executemany(_UPSERT_SQL, rows)
        except Exception:
            logger.warning(
                "usage flush failed for %d buckets; will retry next flush",
                len(rows),
                exc_info=True,
            )
            self._merge_back(snapshot)
            return 0

        if self._dropped_buckets:
            logger.warning(
                "usage meter dropped %d new buckets since last flush "
                "(cardinality cap %d reached)",
                self._dropped_buckets,
                _MAX_KEYS,
            )
            self._dropped_buckets = 0
        return len(rows)

    def _merge_back(self, snapshot: Dict[_Bucket, List[int]]) -> None:
        for bucket, counts in snapshot.items():
            entry = self._counts.get(bucket)
            if entry is None:
                self._counts[bucket] = counts
            else:
                entry[0] += counts[0]
                entry[1] += counts[1]

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(_FLUSH_INTERVAL)
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("periodic usage flush errored", exc_info=True)


#: Process-wide singleton, wired to the DB pool in the app lifespan.
usage_meter = UsageMeter()
