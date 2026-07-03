"""Real-time quote broadcast — asyncpg LISTEN → per-symbol WebSocket fan-out.

This module owns the API-side half of the streaming quote path. The
ingestion process is a *separate* systemd service with its own DB pool,
so the only shared surface between it and each uvicorn API worker is
Postgres itself. We use ``NOTIFY zgx_quote_updates`` as the inter-process
bus:

    ingestion._upsert_underlying_quote()
      └── same-transaction ``NOTIFY zgx_quote_updates`` with JSON payload
                                    │
                                    ▼  (delivered on commit)
    QuoteBroadcaster.start()
      └── one asyncpg connection per API worker, held for LISTEN
      └── on_notify() parses payload, computes ``session`` label,
          fans out to ``set[WebSocket]`` per subscribed symbol
                                    │
                                    ▼
                              browser tick

Why not Redis? The deployment has zero brokers today; LISTEN/NOTIFY adds
one long-lived pg connection per worker and nothing else.

Why not in-process asyncio.Queue? Ingestion is a different OS process
under a different systemd unit — there is no shared memory. NOTIFY is
the ONE mechanism that already exists in the stack.

Why publish before / with persist? The publish is co-transactional with
the upsert on the same connection — Postgres only delivers NOTIFY on
COMMIT, so subscribers see a tick iff it landed durably. During DB
circuit-breaker backoff on the ingestion side, both the write and the
notify skip together — matching the existing behavior; the *next*
successful tick immediately supersedes the missed one because the
frontend cache is last-tick-wins.

Multi-worker note: uvicorn runs `--workers 2`. Both workers LISTEN on
the same channel, both receive every NOTIFY, and each fans out only to
its own WebSockets. This is correct fan-out — a WebSocket connection is
pinned to one worker for its lifetime; no cross-worker routing needed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time as _time
from typing import Any, Dict, Optional, Set

import asyncpg
from fastapi import WebSocket

logger = logging.getLogger(__name__)

# Postgres channel name. Kept short (channel names have a 63-byte limit)
# and prefixed so any future analytics/replay/backtest streams can add
# their own zgx_ channels without collisions.
NOTIFY_CHANNEL = "zgx_quote_updates"

# Payload size ceiling (Postgres NOTIFY caps at 8000 bytes). Our quote
# rows serialize to well under 300 bytes; the guard just refuses to try
# publishing anything malformed on the ingestion side.
MAX_PAYLOAD_BYTES = 7500

# Maximum concurrent WebSocket clients per API worker. A soft ceiling —
# rejecting new connections above this returns policy-violation close
# code 1008. Set via env so ops can raise it without a code change.
_MAX_CONNECTIONS = int(os.getenv("WS_MAX_CONNECTIONS_PER_WORKER", "2000"))

# Per-authenticated-user connection cap. Without this, one logged-in
# caller scripting reconnects can fill the entire worker's slot pool
# (the workflow-level DoS finding). Set intentionally low — a browser
# with SPY, QQQ, and SPX tabs each open a socket, so 8 leaves plenty
# of headroom for legitimate multi-tab use while blocking runaway
# scripts.
_MAX_CONNECTIONS_PER_USER = int(os.getenv("WS_MAX_CONNECTIONS_PER_USER", "8"))

# Bounded keepalive — an asyncpg ``SELECT 1`` on a silently-dead peer
# (VPC gateway drop, iptables blackhole, RDS failover mid-flight) can
# block for the OS TCP keepalive timeout (~2h on Linux). Wrapping the
# ping in a hard wait_for lets the LISTEN reconnect loop notice within
# seconds instead of hours.
_KEEPALIVE_TIMEOUT_SECONDS = float(os.getenv("WS_LISTEN_KEEPALIVE_TIMEOUT", "10"))


class QuoteBroadcaster:
    """Owns the LISTEN connection and the per-symbol subscriber sets.

    Lifecycle mirrors ``usage_meter``:

        broadcaster = QuoteBroadcaster(
            connect_kwargs_getter=lambda: db.asyncpg_kwargs(),
            session_computer=get_market_session,
        )
        await broadcaster.start()
        ...
        await broadcaster.stop()

    ``connect_kwargs_getter`` is a callable returning the asyncpg
    ``connect(**kwargs)`` args for the current DB. Called each time we
    (re)open the LISTEN connection so a DatabaseManager reconnect that
    changed credentials is transparent — mirrors the key-store pattern.
    """

    def __init__(
        self,
        connect_kwargs_getter,
        session_computer,
    ) -> None:
        self._get_connect_kwargs = connect_kwargs_getter
        # ``session_computer(asset_type, price_is_stable, close_data_available)
        # -> str`` — imported from main.py so we don't duplicate the market
        # session state machine. See main.get_market_session.
        self._compute_session = session_computer

        # ``symbol -> set[WebSocket]``. Guarded by ``_lock`` only for
        # membership churn (subscribe/unsubscribe). Iteration for fan-out
        # copies the snapshot so a slow client's disconnect during a send
        # doesn't mutate the set mid-iteration.
        self._subscribers: Dict[str, Set[WebSocket]] = {}

        # Last snapshot per symbol — the "hello" payload sent to a new
        # subscriber on subscribe(). Prevents the two-frame flicker where
        # a new consumer would mount empty and wait for the next tick.
        self._latest: Dict[str, Dict[str, Any]] = {}

        # asset_type cache keyed by symbol; used when computing the
        # session label at fan-out time. Ingestion embeds asset_type in
        # each NOTIFY payload so the API never has to hit the symbols
        # table on the hot path.
        self._asset_type: Dict[str, str] = {}

        # Per-symbol soft-close trackers — 3 consecutive identical closes
        # count as "stable" for the closed transition (mirrors the
        # ``_SoftCloseTracker`` in main.py; kept separate here so the
        # broadcaster can be started without touching main's module
        # state).
        self._soft_close: Dict[str, list] = {}

        self._lock = asyncio.Lock()
        self._listen_conn: Optional[asyncpg.Connection] = None
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        # Total sockets registered on this worker (bounded by
        # ``_MAX_CONNECTIONS``). Owned by the reservation lock — read
        # and mutated only under it — so ``try_reserve``'s check-and-
        # bump is atomic and the TOCTOU between check and accept()
        # can't drive it past the ceiling.
        self._connection_count = 0
        self._per_user_count: Dict[str, int] = {}
        self._ws_owner: Dict[int, str] = {}
        self._reservation_lock = asyncio.Lock()
        # Long-lived, strong-referenced set of in-flight fan-out tasks
        # so ``asyncio.create_task`` in ``_on_notify`` isn't garbage-
        # collected mid-flight. Python's event loop only weak-refs
        # tasks; the docs explicitly recommend this pattern.
        self._fanout_tasks: Set[asyncio.Task] = set()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn the LISTEN loop as a background task.

        Idempotent — repeated calls are a no-op. Never raises; a failure
        to acquire the initial LISTEN connection is logged and the loop
        keeps trying on backoff, so the API can boot even if the DB is
        temporarily unavailable.
        """
        if self._task is not None:
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="quote-broadcaster")
        logger.info("QuoteBroadcaster started (LISTEN %s)", NOTIFY_CHANNEL)

    async def stop(self) -> None:
        """Cancel the LISTEN task and close all subscriber sockets."""
        self._stopping.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        # Close the LISTEN connection outside the task so we don't race
        # asyncpg's own cleanup path.
        if self._listen_conn is not None:
            try:
                await self._listen_conn.close()
            except Exception:
                pass
            self._listen_conn = None
        # Close every subscriber so the browser side sees a clean close
        # and starts its reconnect backoff instead of hanging on a stale
        # socket during graceful shutdown.
        async with self._lock:
            all_sockets: Set[WebSocket] = set()
            for sockets in self._subscribers.values():
                all_sockets.update(sockets)
            self._subscribers.clear()
        for ws in all_sockets:
            try:
                await ws.close(code=1001, reason="server shutting down")
            except Exception:
                pass
        logger.info("QuoteBroadcaster stopped")

    # ------------------------------------------------------------------
    # LISTEN loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        """Hold one dedicated asyncpg connection open for LISTEN.

        The connection must NOT go back into the pool — asyncpg's
        listener callback fires on the connection that issued LISTEN,
        and pool ``acquire()``/release cycles would drop the
        subscription. We therefore open a raw connection directly
        against the same DSN the pool uses.
        """
        backoff = 1.0
        while not self._stopping.is_set():
            try:
                conn = await self._open_listen_connection()
                if conn is None:
                    await asyncio.sleep(min(backoff, 30.0))
                    backoff = min(backoff * 2, 30.0)
                    continue
                self._listen_conn = conn
                backoff = 1.0
                await conn.add_listener(NOTIFY_CHANNEL, self._on_notify)
                logger.info("LISTEN %s connected", NOTIFY_CHANNEL)
                # Block until stop() is called or the connection drops.
                # A dead connection surfaces via a background exception
                # in asyncpg that we detect with a periodic ``execute``.
                while not self._stopping.is_set():
                    try:
                        await asyncio.wait_for(self._stopping.wait(), timeout=30.0)
                    except asyncio.TimeoutError:
                        # Keepalive ping — bounded so a silently-dead
                        # peer (VPC gateway drop, RDS failover) can't
                        # park us on a stuck socket read until the
                        # 2h-plus OS TCP keepalive fires. On timeout
                        # we raise, break out of the inner loop, and
                        # let the outer reconnect path take over.
                        try:
                            await asyncio.wait_for(
                                conn.execute("SELECT 1"),
                                timeout=_KEEPALIVE_TIMEOUT_SECONDS,
                            )
                        except asyncio.TimeoutError:
                            logger.warning(
                                "LISTEN keepalive timed out (>%.1fs); "
                                "reconnecting",
                                _KEEPALIVE_TIMEOUT_SECONDS,
                            )
                            raise
                if self._stopping.is_set():
                    break
            except (asyncpg.PostgresError, OSError, asyncio.TimeoutError) as exc:
                logger.warning("LISTEN connection lost: %s; reconnecting", exc)
            except Exception:
                logger.exception("LISTEN loop crashed; reconnecting")
            finally:
                if self._listen_conn is not None:
                    try:
                        await self._listen_conn.close()
                    except Exception:
                        pass
                    self._listen_conn = None
            if self._stopping.is_set():
                break
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2, 30.0)

    async def _open_listen_connection(self) -> Optional[asyncpg.Connection]:
        """Open a bare asyncpg connection using the DatabaseManager's creds.

        We can't ``pool.acquire()`` because LISTEN is per-connection and
        a released connection would drop the subscription; and we don't
        want to burn a pool slot indefinitely either. Using the same
        host/port/database/user/password kwargs the pool was created
        with keeps us aligned with whatever creds the app uses without
        depending on asyncpg-internal DSN fields (which don't exist on
        every version — earlier code that inspected `_params.dsn` was
        a no-op on asyncpg 0.31+).
        """
        try:
            kwargs = self._get_connect_kwargs() if self._get_connect_kwargs else None
        except Exception:
            logger.exception("QuoteBroadcaster: connect_kwargs_getter raised")
            return None
        if not kwargs or not isinstance(kwargs, dict):
            return None
        try:
            return await asyncpg.connect(**kwargs)
        except Exception:
            logger.exception("QuoteBroadcaster: failed to open LISTEN connection")
            return None

    def _on_notify(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """asyncpg listener callback — synchronous, fan out via a task.

        asyncpg calls listeners synchronously in the event loop; we
        immediately schedule the async fan-out so a slow/backlogged
        subscriber never blocks the connection or delays the next tick.
        We hold a strong reference to the task in ``_fanout_tasks`` for
        its lifetime — the asyncio event loop only weak-refs tasks and
        low-frequency notifications could otherwise be GC'd mid-flight.
        """
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            logger.warning("Malformed NOTIFY payload dropped (%d bytes)", len(payload))
            return
        task = asyncio.create_task(self._fanout(data))
        self._fanout_tasks.add(task)
        task.add_done_callback(self._fanout_tasks.discard)

    async def _fanout(self, raw: Dict[str, Any]) -> None:
        """Enrich the raw ingestion payload and send to all subscribers."""
        symbol = raw.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            return
        asset_type = raw.get("asset_type")
        if asset_type:
            # Cache asset_type so late payloads without it (unlikely, but
            # future-proof) still compute a correct session label.
            self._asset_type[symbol] = str(asset_type)
        else:
            asset_type = self._asset_type.get(symbol)

        # Compute session at broadcast time (not on the ingestion side)
        # because the wall-clock-driven state machine belongs to the API
        # process, and doing it here means every worker's answer stays
        # consistent with GET /api/market/quote — which uses the same
        # helper.
        prices = self._soft_close.setdefault(symbol, [])
        close_val = raw.get("close")
        if close_val is not None:
            prices.append(close_val)
            del prices[:-3]
        stable = len(prices) >= 3 and len(set(prices)) == 1
        # ``close_data_available`` on the fast path is approximated as
        # True: ingestion by definition has landed at least one bar for
        # today. Full parity with the HTTP handler would require another
        # DB round-trip per notify — not worth it; the header multiplexes
        # this against session-closes anyway.
        try:
            session = self._compute_session(asset_type, stable, True)
        except Exception:
            session = None

        message = {
            "type": "quote",
            "symbol": symbol,
            "timestamp": raw.get("timestamp"),
            "open": raw.get("open"),
            "high": raw.get("high"),
            "low": raw.get("low"),
            "close": raw.get("close"),
            "volume": raw.get("volume"),
            "up_volume": raw.get("up_volume"),
            "down_volume": raw.get("down_volume"),
            "session": session,
            # Server-side receive time. Frontend can use this to detect
            # clock skew or dead streams (last_recv older than 30s → poll
            # fallback re-engages even if the socket is still "open").
            "server_ts": _time.time(),
        }
        self._latest[symbol] = message

        async with self._lock:
            subscribers = list(self._subscribers.get(symbol, ()))
        if not subscribers:
            return
        text = json.dumps(message, default=str)
        # Fire all sends concurrently — a single slow client shouldn't
        # slow the fan-out. ``send_text`` throws on a closed socket; we
        # catch per-send and drop the failing subscriber outside the
        # gather so cleanup doesn't race the send loop.
        results = await asyncio.gather(
            *(self._safe_send(ws, text) for ws in subscribers),
            return_exceptions=True,
        )
        dead = [ws for ws, ok in zip(subscribers, results) if ok is not True]
        if dead:
            await self._drop_dead(symbol, dead)

    async def _safe_send(self, ws: WebSocket, text: str) -> bool:
        try:
            await ws.send_text(text)
            return True
        except Exception:
            return False

    async def _drop_dead(self, symbol: str, dead) -> None:
        """Remove sockets whose send just failed from this symbol's set.

        The connection is not released here — the receive loop in the
        WS handler catches the disconnect and calls ``unregister()``,
        which is the single authoritative release path. Doing it in
        both places would double-decrement the counter.
        """
        async with self._lock:
            symbol_set = self._subscribers.get(symbol)
            if symbol_set is None:
                return
            for ws in dead:
                symbol_set.discard(ws)
            if not symbol_set:
                self._subscribers.pop(symbol, None)

    # ------------------------------------------------------------------
    # Subscriber API — called from the WebSocket route handler
    # ------------------------------------------------------------------

    async def try_reserve(self, ws: WebSocket, end_user_id: str) -> bool:
        """Atomically check the caps and reserve a slot for ``ws``.

        Returns ``True`` when the caller may proceed with
        ``websocket.accept()``. Returns ``False`` (and reserves
        nothing) when either the per-worker or per-user cap is
        exhausted. The check-and-bump runs under a single lock so two
        concurrent handshakes at the ceiling can't both succeed — the
        TOCTOU that the pre-fix ``can_accept()`` + ``register()`` pair
        allowed.

        Call ``unregister(ws)`` on the socket regardless of whether
        the accept succeeds or the session later dies, and it
        releases the slot exactly once.
        """
        async with self._reservation_lock:
            if self._connection_count >= _MAX_CONNECTIONS:
                return False
            user_count = self._per_user_count.get(end_user_id, 0)
            if user_count >= _MAX_CONNECTIONS_PER_USER:
                return False
            self._connection_count += 1
            self._per_user_count[end_user_id] = user_count + 1
            self._ws_owner[id(ws)] = end_user_id
        return True

    async def unregister(self, ws: WebSocket) -> None:
        """Drop ``ws`` from every symbol set and release its reservation.

        Safe to call twice (the finally + explicit-close paths both
        call it); the ``pop`` on ``_ws_owner`` is the idempotency
        marker.
        """
        async with self._lock:
            empty = []
            for symbol, sockets in self._subscribers.items():
                if ws in sockets:
                    sockets.discard(ws)
                    if not sockets:
                        empty.append(symbol)
            for symbol in empty:
                self._subscribers.pop(symbol, None)
        async with self._reservation_lock:
            owner = self._ws_owner.pop(id(ws), None)
            if owner is None:
                return  # already released or never reserved
            self._connection_count = max(0, self._connection_count - 1)
            user_count = self._per_user_count.get(owner, 0) - 1
            if user_count <= 0:
                self._per_user_count.pop(owner, None)
            else:
                self._per_user_count[owner] = user_count

    async def subscribe(self, ws: WebSocket, symbol: str) -> Optional[Dict[str, Any]]:
        """Add ``ws`` to the ``symbol`` subscriber set; return snapshot if any."""
        symbol = symbol.strip().upper()
        if not symbol:
            return None
        async with self._lock:
            self._subscribers.setdefault(symbol, set()).add(ws)
        return self._latest.get(symbol)

    async def unsubscribe(self, ws: WebSocket, symbol: str) -> None:
        symbol = symbol.strip().upper()
        if not symbol:
            return
        async with self._lock:
            sockets = self._subscribers.get(symbol)
            if sockets is None:
                return
            sockets.discard(ws)
            if not sockets:
                self._subscribers.pop(symbol, None)


# Module-level singleton — mirrors ``usage_meter`` / ``key_store``. Wired
# up in the API lifespan (see main.py). Import as
# ``from .quote_broadcaster import broadcaster``.
broadcaster: Optional[QuoteBroadcaster] = None


def get_broadcaster() -> Optional[QuoteBroadcaster]:
    """Accessor for the module singleton (readable at import time)."""
    return broadcaster


def set_broadcaster(instance: Optional[QuoteBroadcaster]) -> None:
    """Wire the broadcaster from ``lifespan()``. Passing ``None`` clears it."""
    global broadcaster
    broadcaster = instance
