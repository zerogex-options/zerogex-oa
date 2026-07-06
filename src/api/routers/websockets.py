"""Real-time WebSocket endpoints — currently the underlying quote stream.

Wire contract (browser side):

    ws = new WebSocket(`wss://api.zerogex.io/ws?ticket=${ticket}`)

    ws.onopen = () => {
      ws.send(JSON.stringify({op: 'subscribe', symbols: ['SPY']}))
    }

    ws.onmessage = (e) => {
      // e.data → JSON string, either:
      //   {type:'quote', symbol, timestamp, open, high, low, close,
      //    volume, up_volume, down_volume, session, server_ts}
      //   {type:'welcome', symbols_subscribed:[...]}
      //   {type:'ack', op:'subscribe'|'unsubscribe', symbols:[...]}
      //   {type:'error', code, detail}
      //   {type:'pong', server_ts}
    }

Message ops (client → server):

    {op:'subscribe',   symbols: string[]}   // add these to your subscription set
    {op:'unsubscribe', symbols: string[]}   // remove these
    {op:'ping'}                              // keepalive; server replies with pong

Design notes:

* Auth is ticket-based (see identity.verify_ws_ticket). Browsers cannot
  attach Bearer/X-API-Key on WS handshake so the Next.js BFF mints a
  short-lived HS256 JWT with ``aud='ws'`` via /api/ws/ticket and the
  browser passes it as ?ticket=. The API verifies it with the shared
  END_USER_TOKEN_SECRET (pure crypto, no DB).

* Origin allowlist enforced at handshake — WS bypasses Starlette CORS,
  so we duplicate the origin check here from CORS_ALLOW_ORIGINS.

* The route is registered directly on the FastAPI app (not a mounted
  APIRouter) via ``register(app, ...)`` so the lifespan-scoped
  broadcaster singleton is captured in the closure. Follows the pattern
  in main.py of decorating routes directly on ``@app`` rather than
  threading DI through routers for lifespan-owned services.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status
from starlette.websockets import WebSocketState

from ..identity import verify_ws_ticket
from ..quote_broadcaster import QuoteBroadcaster

logger = logging.getLogger(__name__)

# Symbols the browser is allowed to subscribe to. Ingestion currently
# writes SPY only by default; the picker is capped to SPY/QQQ/SPX. Any
# other symbol quietly no-ops so a stale client can't blow up a worker
# by requesting a wildcard.
_ALLOWED_SYMBOLS: Set[str] = {
    s.strip().upper()
    for s in os.getenv("WS_ALLOWED_SYMBOLS", "SPY,QQQ,SPX,$VIX.X,$VXN.X").split(",")
    if s.strip()
}

# Per-connection soft ceilings — a client that goes rogue can't drag
# down the worker. Values chosen for headroom vs the current UI which
# needs at most 1-3 symbols at a time.
_MAX_SUBS_PER_CONN = int(os.getenv("WS_MAX_SUBS_PER_CONN", "16"))
_IDLE_TIMEOUT_SECONDS = float(os.getenv("WS_IDLE_TIMEOUT_SECONDS", "120"))


def _parse_origins(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    return {o.strip() for o in raw.split(",") if o.strip()}


def _origin_allowed(origin: Optional[str], allowed: Set[str]) -> bool:
    # No origin header (e.g. a native client) is accepted only in dev,
    # where the same relaxed rule applies to CORS. In production the
    # frontend always sets Origin.
    if not allowed or "*" in allowed:
        return True
    if not origin:
        return False
    return origin in allowed


def register(
    app: FastAPI,
    *,
    get_broadcaster: Callable[[], Optional[QuoteBroadcaster]],
) -> None:
    """Attach the /ws route to ``app``.

    ``get_broadcaster`` is a callable so the route captures the lookup,
    not the instance — the lifespan may wire the broadcaster after
    ``register`` is called, and a hot-reload could re-create it.
    """
    allowed_origins = _parse_origins(os.getenv("CORS_ALLOW_ORIGINS"))

    @app.websocket("/ws")
    async def quote_stream(websocket: WebSocket) -> None:
        # ------------------------------------------------------------------
        # Handshake
        # ------------------------------------------------------------------
        origin = websocket.headers.get("origin")
        if not _origin_allowed(origin, allowed_origins):
            logger.info("WS handshake refused: origin=%r not allowed", origin)
            # 1008 = Policy Violation. Browser JS gets a clean close.
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        ticket = websocket.query_params.get("ticket")
        end_user_id = verify_ws_ticket(ticket)
        if not end_user_id:
            logger.info("WS handshake refused: invalid or missing ticket")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        broadcaster = get_broadcaster()
        if broadcaster is None:
            logger.warning("WS handshake refused: broadcaster not initialized")
            await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
            return

        # Atomic per-user reservation. The pre-fix pattern of
        # ``can_accept()`` then ``accept()`` then ``register()`` had two
        # bugs: (a) two concurrent handshakes at the ceiling could both
        # pass the check, and (b) the counter was per-worker only, so
        # one authenticated user scripting reconnects could fill every
        # slot and lock out other users. ``try_reserve`` holds a lock
        # across both checks and the bump.
        if not await broadcaster.try_reserve(websocket, end_user_id):
            logger.warning(
                "WS handshake refused: connection cap reached (user=%r)",
                end_user_id,
            )
            await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
            return

        try:
            await websocket.accept()
        except Exception:
            # accept() itself failed (client disconnected during
            # handshake). Release the slot we reserved above so a
            # flaky client can't leak reservations.
            await broadcaster.unregister(websocket)
            raise

        try:
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "welcome",
                        "symbols_allowed": sorted(_ALLOWED_SYMBOLS),
                        "max_subs_per_conn": _MAX_SUBS_PER_CONN,
                        "idle_timeout_seconds": _IDLE_TIMEOUT_SECONDS,
                    }
                )
            )
        except Exception:
            await broadcaster.unregister(websocket)
            return

        # ------------------------------------------------------------------
        # Message loop
        # ------------------------------------------------------------------
        subscribed: Set[str] = set()
        try:
            while True:
                # Idle timeout — an abandoned tab holding a WS burns a
                # slot forever. Any inbound message (subscribe/ping)
                # resets the clock; nothing arriving for _IDLE_TIMEOUT
                # closes the socket cleanly.
                try:
                    raw = await asyncio.wait_for(
                        websocket.receive_text(), timeout=_IDLE_TIMEOUT_SECONDS
                    )
                except asyncio.TimeoutError:
                    await _safe_send_error(websocket, "idle_timeout", "no message received")
                    break

                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    await _safe_send_error(websocket, "bad_json", "message is not valid JSON")
                    continue
                if not isinstance(msg, dict):
                    await _safe_send_error(websocket, "bad_shape", "message must be a JSON object")
                    continue

                op = msg.get("op")
                if op == "subscribe":
                    symbols = _normalize_symbols(msg.get("symbols"))
                    if not symbols:
                        await _safe_send_error(websocket, "bad_symbols", "symbols[] required")
                        continue
                    added: List[str] = []
                    snapshots: List[Dict[str, Any]] = []
                    for symbol in symbols:
                        if symbol not in _ALLOWED_SYMBOLS:
                            continue
                        if symbol in subscribed:
                            continue
                        if len(subscribed) >= _MAX_SUBS_PER_CONN:
                            await _safe_send_error(
                                websocket,
                                "subscription_cap",
                                f"max {_MAX_SUBS_PER_CONN} symbols per connection",
                            )
                            break
                        snapshot = await broadcaster.subscribe(websocket, symbol)
                        subscribed.add(symbol)
                        added.append(symbol)
                        if snapshot is not None:
                            snapshots.append(snapshot)
                    await _safe_send(
                        websocket,
                        {"type": "ack", "op": "subscribe", "symbols": added},
                    )
                    for snap in snapshots:
                        await _safe_send(websocket, snap)

                elif op == "unsubscribe":
                    symbols = _normalize_symbols(msg.get("symbols"))
                    removed: List[str] = []
                    for symbol in symbols:
                        if symbol in subscribed:
                            await broadcaster.unsubscribe(websocket, symbol)
                            subscribed.discard(symbol)
                            removed.append(symbol)
                    await _safe_send(
                        websocket,
                        {"type": "ack", "op": "unsubscribe", "symbols": removed},
                    )

                elif op == "ping":
                    await _safe_send(
                        websocket,
                        {"type": "pong", "client_ts": msg.get("client_ts")},
                    )

                else:
                    await _safe_send_error(
                        websocket,
                        "unknown_op",
                        f"unknown op: {op!r}",
                    )

        except WebSocketDisconnect:
            pass
        except Exception:
            logger.exception("WS session crashed; closing")
        finally:
            await broadcaster.unregister(websocket)
            if websocket.client_state != WebSocketState.DISCONNECTED:
                try:
                    await websocket.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_symbols(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        sym = item.strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
        if len(out) >= _MAX_SUBS_PER_CONN:
            break
    return out


async def _safe_send(ws: WebSocket, obj: Dict[str, Any]) -> None:
    try:
        await ws.send_text(json.dumps(obj, default=str))
    except Exception:
        # Send failures are surfaced by the next receive_text raising;
        # we don't want a transient send error to spam the log or crash
        # the receive loop.
        pass


async def _safe_send_error(ws: WebSocket, code: str, detail: str) -> None:
    await _safe_send(ws, {"type": "error", "code": code, "detail": detail})
