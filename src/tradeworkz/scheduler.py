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
from typing import Optional

from src.tradeworkz import config as tw_config
from src.tradeworkz.engine import tick as _engine_tick

logger = logging.getLogger(__name__)


class TradeWorkzScheduler:
    """Owns the background task that drives :func:`engine.tick`."""

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

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
                        len(errors), opened, closed, errors[:3],
                    )
                elif opened or closed:
                    logger.info(
                        "TradeWorkz tick opened=%d closed=%d marked=%d",
                        opened, closed, summary.get("marked", 0),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — one tick failing must not kill the loop
                logger.exception("TradeWorkz tick failed; continuing")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                # If wait() returned (rather than timing out), stop was requested.
                return
            except asyncio.TimeoutError:
                pass


scheduler = TradeWorkzScheduler()
