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
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from src.tradeworkz import config as tw_config
from src.tradeworkz.engine import calibration_sweep as _calibration_sweep
from src.tradeworkz.engine import tick as _engine_tick

logger = logging.getLogger(__name__)


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
        logger.info("TradeWorkz scheduler stopped")

    async def _loop(self) -> None:
        interval = tw_config.ENGINE_INTERVAL_SECONDS
        assert self._stop_event is not None
        while not self._stop_event.is_set():
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
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                # If wait() returned (rather than timing out), stop was requested.
                return
            except asyncio.TimeoutError:
                pass

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
