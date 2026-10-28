"""In-process TradeWorkz tick scheduler.

Started from the FastAPI lifespan hook. Runs one engine tick every
``TRADEWORKZ_ENGINE_INTERVAL_SECONDS`` on a background asyncio task.

The tick itself (:func:`src.tradeworkz.engine.tick`) uses the synchronous
psycopg2 pool, so it is dispatched to a thread via :func:`asyncio.to_thread`
to avoid blocking the FastAPI event loop that serves API requests. One tick
in flight at a time — a slow tick delays the next one instead of stacking.

Every tick is wrapped in a try/except so a transient DB blip logs a warning
and the scheduler keeps going; a hard `CancelledError` is the only way the
loop exits.

Under ``uvicorn --workers N`` the lifespan hook runs once per worker, so N
schedulers start, but only the one holding the runner lock ticks (see
``_RUNNER_LOCK_PATH``).
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Optional

from src.tradeworkz import config as tw_config
from src.tradeworkz.engine import calibration_sweep as _calibration_sweep
from src.tradeworkz.engine import tick as _engine_tick

logger = logging.getLogger(__name__)

# One worker runs the ticks. Nothing in a tick stops two workers from opening
# the same position in the same second, and the API runs several workers. The
# runner is whoever holds a non-blocking flock on this file, which sits in the
# unit's private /tmp (PrivateTmp=true): every worker of the unit shares it and
# nothing outside the unit sees it. The kernel drops the lock when the holder
# exits, however it exits, so a surviving worker takes over on its next tick.
# One host only; a second API host would need a database lock instead.
_RUNNER_LOCK_PATH = os.path.join(tempfile.gettempdir(), "zerogex-tradeworkz-scheduler.lock")


def _sweep_due(last_run: Optional[datetime], now: datetime, interval_seconds: float) -> bool:
    """Whether the calibration sweep should run now.

    ``interval_seconds <= 0`` disables the periodic sweep entirely. A
    ``None`` ``last_run`` is due immediately, so the sweep fires once on
    scheduler start (calibrates a fresh deploy without waiting a full
    interval).
    """
    if interval_seconds <= 0:
        return False
    if last_run is None:
        return True
    return (now - last_run).total_seconds() >= interval_seconds


class TradeWorkzScheduler:
    """Owns the background task that drives :func:`engine.tick`."""

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._last_sweep_at: Optional[datetime] = None
        self._lock_fd: Optional[int] = None
        self._lock_warned = False

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        """Launch the background loop. No-op when the engine is disabled or
        the scheduler is already running.
        """
        if not tw_config.ENGINE_ENABLED:
            logger.info("TradeWorkz scheduler disabled (TRADEWORKZ_ENGINE_ENABLED=false)")
            return
        if self.is_running():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="tradeworkz-scheduler")
        logger.info(
            "TradeWorkz scheduler started (interval=%ss)",
            tw_config.ENGINE_INTERVAL_SECONDS,
        )

    async def stop(self) -> None:
        """Cancel the background loop cleanly. Safe to call more than once."""
        if self._stop_event is not None:
            self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        self._stop_event = None
        self._release_runner_lock()
        logger.info("TradeWorkz scheduler stopped")

    def _holds_runner_lock(self) -> bool:
        """Whether this worker runs the ticks, taking the lock if it is free.

        Fails open: if the lock cannot be taken for any reason other than
        another worker holding it, this worker ticks, which is how every
        worker behaved before the lock existed. Duplicate ticks are the
        lesser failure; no worker ticking would stop TradeWorkz silently.
        """
        if self._lock_fd is not None:
            return True
        try:
            fd = os.open(_RUNNER_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o600)
        except OSError:
            self._warn_lock_unavailable()
            return True
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        except OSError:
            os.close(fd)
            self._warn_lock_unavailable()
            return True
        self._lock_fd = fd
        logger.info("TradeWorkz scheduler: this worker (pid %d) runs the ticks", os.getpid())
        return True

    def _warn_lock_unavailable(self) -> None:
        if not self._lock_warned:
            self._lock_warned = True
            logger.warning(
                "TradeWorkz scheduler: cannot lock %s; this worker ticks regardless",
                _RUNNER_LOCK_PATH,
                exc_info=True,
            )

    def _release_runner_lock(self) -> None:
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)  # closing the descriptor drops the flock
            except OSError:
                pass
            self._lock_fd = None

    async def _loop(self) -> None:
        interval = tw_config.ENGINE_INTERVAL_SECONDS
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            # Re-checked every interval, so a standby worker takes over within
            # one interval of the runner exiting.
            if self._holds_runner_lock():
                await self._tick_and_sweep()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                # If wait() returned (rather than timing out), stop was requested.
                return
            except asyncio.TimeoutError:
                pass

    async def _tick_and_sweep(self) -> None:
        try:
            summary = await asyncio.to_thread(_engine_tick)
            # Only log a real summary line — the DEBUG cadence would drown
            # journalctl at 1s ticks. Warn if the tick reported errors.
            errors = summary.get("errors") or []
            opened = summary.get("opened", 0)
            closed = summary.get("closed", 0)
            if errors:
                logger.warning(
                    "TradeWorkz tick errors=%d opened=%d closed=%d %s",
                    len(errors),
                    opened,
                    closed,
                    errors[:3],
                )
            elif opened or closed:
                logger.info(
                    "TradeWorkz tick opened=%d closed=%d marked=%d",
                    opened,
                    closed,
                    summary.get("marked", 0),
                )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — one tick failing must not kill the loop
            logger.exception("TradeWorkz tick failed; continuing")
        await self._maybe_run_sweep()

    async def _maybe_run_sweep(self) -> None:
        """Run the calibration sweep when its interval is due.

        Dispatched to a thread (the sweep uses the sync psycopg2 pool). A
        failure is logged and swallowed so it never kills the tick loop.
        """
        now = datetime.now(timezone.utc)
        interval_s = tw_config.CALIBRATION_SWEEP_INTERVAL_HOURS * 3600.0
        if not _sweep_due(self._last_sweep_at, now, interval_s):
            return
        self._last_sweep_at = now
        try:
            await asyncio.to_thread(_calibration_sweep)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — sweep failure must not kill the loop
            logger.exception("TradeWorkz calibration sweep failed; continuing")


scheduler = TradeWorkzScheduler()
