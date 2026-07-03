"""
Quote broker — fans out per-symbol underlying-quote NOTIFY events to
WebSocket subscribers.

Ingestion (``src.ingestion.main_engine._upsert_underlying_quote``) fires
``NOTIFY underlying_quote_events, '{"symbol":"SPY","ts":"..."}'`` inside
the same transaction as the upsert. This module holds a dedicated
asyncpg connection running ``LISTEN`` on that channel, and for each
event refetches the enriched row via ``DatabaseManager.get_latest_quote``
(applying the same session / soft-close / volume-join logic the REST
endpoint uses) so the WS payload matches ``UnderlyingQuote`` exactly.

One fetch per event is fanned out to every subscriber for the affected
symbol, so a hundred connected browsers watching SPY still only cost
one DB round-trip per tick.
"""

from __future__ import annotations

import asyncio
import json
import time as time_module
from typing import Any, Callable, Dict, Optional, Set

import asyncpg

from src.utils import get_logger

logger = get_logger(__name__)


NOTIFY_CHANNEL = "underlying_quote_events"

# Bound the per-subscriber queue so a slow client can't grow it unboundedly.
# When full, the oldest tick is dropped in favor of the newest — a stale tick
# is worthless once a fresher one has arrived, so this is the correct
# back-pressure policy for live-quote streaming.
_QUEUE_MAXSIZE = 8

# Max wait between reconnection attempts on LISTEN loop failures.
_RECONNECT_BACKOFF_INITIAL_SECONDS = 1.0
_RECONNECT_BACKOFF_MAX_SECONDS = 30.0


class _Subscriber:
    """One connected WS client for one symbol."""

    __slots__ = ("queue",)

    def __init__(self) -> None:
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)

    def offer(self, payload: Dict[str, Any]) -> None:
        """Push a payload with drop-oldest back-pressure."""
        try:
            self.queue.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self.queue.put_nowait(payload)
            except asyncio.QueueFull:
                # Two racing offers filled the queue between get and put;
                # dropping this one is still correct — the next tick will
                # supersede it anyway.
                pass


class QuoteBroker:
    """LISTEN-driven fanout for /api/market/quote pushes."""

    def __init__(
        self,
        *,
        connect_factory: Callable[[], "asyncio.Future[asyncpg.Connection]"],
        enrich: Callable[[str], "asyncio.Future[Optional[Dict[str, Any]]]"],
    ) -> None:
        self._connect_factory = connect_factory
        self._enrich = enrich
        self._subscribers: Dict[str, Set[_Subscriber]] = {}
        self._latest: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    def start(self) -> None:
        if self._task is not None:
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run(), name="quote-broker-listen")

    async def stop(self) -> None:
        self._stopped.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def subscribe(self, symbol: str) -> "tuple[_Subscriber, Optional[Dict[str, Any]]]":
        """Register a subscriber; return it plus the cached latest snapshot (if any)."""
        symbol = symbol.upper()
        sub = _Subscriber()
        async with self._lock:
            self._subscribers.setdefault(symbol, set()).add(sub)
            snapshot = self._latest.get(symbol)
        return sub, snapshot

    async def unsubscribe(self, symbol: str, sub: _Subscriber) -> None:
        symbol = symbol.upper()
        async with self._lock:
            subs = self._subscribers.get(symbol)
            if subs:
                subs.discard(sub)
                if not subs:
                    self._subscribers.pop(symbol, None)

    async def _fanout(self, symbol: str, payload: Dict[str, Any]) -> None:
        async with self._lock:
            self._latest[symbol] = payload
            subs = list(self._subscribers.get(symbol, ()))
        for sub in subs:
            sub.offer(payload)

    async def _handle_notify(self, raw_payload: str) -> None:
        try:
            event = json.loads(raw_payload)
        except json.JSONDecodeError:
            logger.warning("[quote-broker] bad NOTIFY payload: %r", raw_payload[:200])
            return
        symbol = str(event.get("symbol") or "").upper()
        if not symbol:
            return
        # Only enrich when at least one subscriber is watching this symbol.
        # Idle channels stay cheap even though ingestion fires NOTIFY on
        # every upsert for every symbol.
        async with self._lock:
            has_subs = bool(self._subscribers.get(symbol))
        if not has_subs:
            return
        try:
            enriched = await self._enrich(symbol)
        except Exception:
            logger.exception("[quote-broker] enrich failed for %s", symbol)
            return
        if enriched is None:
            return
        await self._fanout(symbol, enriched)

    async def _run(self) -> None:
        backoff = _RECONNECT_BACKOFF_INITIAL_SECONDS
        while not self._stopped.is_set():
            conn: Optional[asyncpg.Connection] = None
            try:
                conn = await self._connect_factory()

                def _on_notify(_conn, _pid, _channel, payload):  # noqa: ANN001
                    # Callback runs in asyncpg's reader task; hand off to the
                    # event loop with create_task so a slow enrich() can't
                    # block the reader.
                    asyncio.create_task(self._handle_notify(payload))

                await conn.add_listener(NOTIFY_CHANNEL, _on_notify)
                logger.info("[quote-broker] LISTEN %s established", NOTIFY_CHANNEL)
                backoff = _RECONNECT_BACKOFF_INITIAL_SECONDS
                # Idle until stopped or the connection dies. asyncpg fires
                # add_listener callbacks off its own reader task, so we just
                # need to keep this task alive.
                while not self._stopped.is_set():
                    if conn.is_closed():
                        raise ConnectionError("LISTEN connection closed")
                    await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "[quote-broker] LISTEN loop failure (%s); reconnecting in %.1fs",
                    exc,
                    backoff,
                )
            finally:
                if conn is not None:
                    try:
                        await conn.close()
                    except Exception:
                        pass
            if self._stopped.is_set():
                break
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=backoff)
                break
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SECONDS)
