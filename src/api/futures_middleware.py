"""Serve ES / NQ by projecting the SPX / NDX surfaces onto the futures axis.

A request for ``symbol=ES`` is answered by running the ordinary ``SPX``
handler and carrying its price-space fields across on the way out.  Doing
this once, in middleware, is what makes ES / NQ work across the *whole* API
— including the routers (``/api/v1/levels``, forced flow, trade bias) —
instead of requiring each of seventy-odd symbol-taking endpoints to grow its
own futures branch and drift out of step with the others.

Every request falls into exactly one of four buckets:

1. **Not a future** — passed straight through, untouched.  This is almost
   every request, and it must cost nothing: hence pure ASGI (below).
2. **Served natively** (:data:`_NATIVE_PATHS`) — the handler already reads
   the future's own bars out of ``futures_quotes``.  The symbol is NOT
   rewritten and the response is NOT projected: these endpoints carry
   OBSERVED prices, and projecting a frozen 16:00 SPX print would report
   where ES stood at the bell rather than where it is now.
3. **Unsupported** (:data:`_UNSUPPORTED_PREFIXES`) — per-contract option
   endpoints.  There is no ES option chain in ZeroGEX, so an SPX contract
   relabelled ES would be a fabrication; these answer 400 instead.
4. **Projected** — everything else: rewrite the symbol inbound, project the
   allowlisted price fields outbound, relabel, attach a ``projection`` block
   recording the ratio used, and substitute the observed futures print for
   any spot-like field.

Why pure ASGI rather than ``BaseHTTPMiddleware``: the latter wraps every
request in an anyio task group and buffers through a stream even when the
middleware does nothing, which changes exception propagation and streaming
behaviour for the entire application.  Every other middleware in this app is
pure ASGI (see :mod:`src.api.middleware`); a futures feature has no business
altering how SPX requests are served.  Here, a non-futures request costs one
dict lookup and one string compare before ``self.app`` is called directly.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode

from src.jobs.futures_projection import (
    SPOT_FIELDS,
    project_payload,
    projection_metadata,
    projection_tick,
    resolve_basis,
)
from src.symbols import is_futures_symbol, resolve_futures_index

logger = logging.getLogger("zerogex.futures_middleware")

# Query parameters that name the underlying across the API surface.
_SYMBOL_PARAMS = ("symbol", "underlying")

# Endpoints that resolve futures natively from ``futures_quotes``.  Their
# prices are OBSERVED, so they must not be rewritten or projected.
_NATIVE_PATHS = frozenset(
    {
        "/api/market/quote",
        "/api/market/historical",
        "/api/market/session-closes",
        "/api/market/session-levels",
    }
)

# Per-contract option surfaces.  ZeroGEX ingests no options on futures, so
# there is nothing truthful to return for ES / NQ here — an SPX contract with
# its strike multiplied by the basis is not a contract anyone can trade, and
# the strike would no longer round-trip back to the chain it came from.
_UNSUPPORTED_PREFIXES = (
    "/api/option/",
    "/api/tools/option-calculator",
)

# Responses larger than this are projected anyway but logged: the walk is
# O(payload) on the event loop, and a very large one is worth knowing about.
_LARGE_BODY_BYTES = 4 * 1024 * 1024


def _scope_query(scope: dict) -> list[tuple[str, str]]:
    return parse_qsl(scope.get("query_string", b"").decode("latin-1"), keep_blank_values=True)


def _futures_target(scope: dict) -> Optional[tuple[str, str]]:
    """Return ``(futures_symbol, index_symbol)`` this request asks for.

    ``None`` when the request names no first-class future — the overwhelmingly
    common case, and the one that must stay free.
    """
    for key, value in _scope_query(scope):
        if key in _SYMBOL_PARAMS and value and is_futures_symbol(value):
            index = resolve_futures_index(value)
            if index:
                return value.strip().upper(), index

    # Path-addressed symbols, e.g. /api/v1/levels/ES. Only the final segment
    # is considered, so an unrelated path can never be rewritten by accident.
    tail = scope.get("path", "").rstrip("/").rsplit("/", 1)[-1]
    if tail and is_futures_symbol(tail):
        index = resolve_futures_index(tail)
        if index:
            return tail.strip().upper(), index
    return None


def _rewrite_scope(scope: dict, futures_symbol: str, index_symbol: str) -> None:
    """Point the request at the backing index, in place.

    The pre-rewrite path and the symbol the caller actually asked for are
    stashed on the scope so the audit log can report what was requested rather
    than what was routed — an ES request attributed to SPX in the audit trail
    is a small lie that would be very annoying to debug later.
    """
    scope["zerogex_requested_symbol"] = futures_symbol
    scope["zerogex_original_path"] = scope.get("path", "")

    query = _scope_query(scope)
    if query:
        scope["query_string"] = urlencode(
            [
                (key, index_symbol if key in _SYMBOL_PARAMS and is_futures_symbol(value) else value)
                for key, value in query
            ]
        ).encode("latin-1")

    path = scope.get("path", "")
    stripped = path.rstrip("/")
    if stripped.rsplit("/", 1)[-1].upper() == futures_symbol:
        head = stripped.rsplit("/", 1)[0]
        scope["path"] = f"{head}/{index_symbol}" + ("/" if path.endswith("/") else "")
        scope["raw_path"] = scope["path"].encode("latin-1")


def _header(headers: list, name: bytes) -> bytes:
    for key, value in headers:
        if key.lower() == name:
            return value
    return b""


async def _live_futures_spot(index_symbol: str) -> Optional[float]:
    """Latest OBSERVED futures close for ``index_symbol``, if the feed has one.

    Passes the same cash-close anchor the quote endpoint uses so both callers
    hit the same cache entry with the same meaning.
    """
    try:
        from src.api.main import db_manager
        from src.market_calendar import current_cash_close_reference

        if db_manager is None:
            return None
        quote = await db_manager.get_latest_future_quote(
            index_symbol, current_cash_close_reference()
        )
        if quote and quote.get("close") is not None:
            return float(quote["close"])
    except Exception as e:
        logger.debug("live futures spot unavailable for %s: %s", index_symbol, e)
    return None


def _db_manager():
    """The live DatabaseManager, or None before startup."""
    from src.api.main import db_manager

    return db_manager


class FuturesProjectionMiddleware:
    """Answer ES / NQ requests from the SPX / NDX surfaces (pure ASGI)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        target = _futures_target(scope)
        if target is None:
            # The hot path: no future named, nothing to do, no wrapping.
            await self.app(scope, receive, send)
            return

        futures_symbol, index_symbol = target
        path = scope.get("path", "")

        if path in _NATIVE_PATHS:
            await self.app(scope, receive, send)
            return

        if path.startswith(_UNSUPPORTED_PREFIXES):
            await self._reject(send, futures_symbol, index_symbol)
            return

        _rewrite_scope(scope, futures_symbol, index_symbol)
        await self._project_response(scope, receive, send, futures_symbol, index_symbol)

    async def _reject(self, send, futures_symbol: str, index_symbol: str) -> None:
        body = json.dumps(
            {
                "detail": (
                    f"{futures_symbol} has no option chain of its own — its levels are "
                    f"{index_symbol} option-derived. Per-contract option endpoints are "
                    f"only available for {index_symbol}."
                )
            }
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("latin-1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def _project_response(
        self, scope, receive, send, futures_symbol: str, index_symbol: str
    ) -> None:
        status: Optional[int] = None
        headers: list = []
        chunks: list[bytes] = []
        started = False

        async def capture(message):
            nonlocal status, headers, started
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = list(message.get("headers", []))
                return
            if message["type"] != "http.response.body":
                await send(message)
                return

            chunks.append(message.get("body", b"") or b"")
            if message.get("more_body"):
                return

            body = b"".join(chunks)
            content_type = _header(headers, b"content-type").decode("latin-1", "ignore")
            if status != 200 or "application/json" not in content_type:
                await self._passthrough(send, status, headers, body)
                started = True
                return

            try:
                payload = json.loads(body)
            except (ValueError, UnicodeDecodeError):
                await self._passthrough(send, status, headers, body)
                started = True
                return

            if len(body) > _LARGE_BODY_BYTES:
                logger.info(
                    "projecting a %d-byte %s response for %s",
                    len(body),
                    scope.get("zerogex_original_path", ""),
                    futures_symbol,
                )

            try:
                projected = await self._transform(payload, futures_symbol, index_symbol)
                out = json.dumps(projected).encode("utf-8")
            except Exception as e:
                # Never ship un-projected cash levels under an ES label: that
                # would be silently wrong on a chart. Fail visibly instead,
                # keeping the upstream headers so CORS still applies.
                logger.error(
                    "futures projection failed for %s: %s", futures_symbol, e, exc_info=True
                )
                detail = json.dumps(
                    {"detail": (f"Could not project {index_symbol} levels onto {futures_symbol}.")}
                ).encode("utf-8")
                await self._passthrough(send, 503, headers, detail)
                started = True
                return

            out_headers = [
                (key, value)
                for key, value in headers
                if key.lower() not in (b"content-length", b"content-encoding")
            ]
            out_headers.append((b"content-length", str(len(out)).encode("latin-1")))
            out_headers.append((b"x-zerogex-projection", b"1"))
            await send({"type": "http.response.start", "status": 200, "headers": out_headers})
            await send({"type": "http.response.body", "body": out})
            started = True

        await self.app(scope, receive, capture)

        if not started and status is not None:
            # Body never arrived (empty response); relay what we have.
            await self._passthrough(send, status, headers, b"")

    async def _passthrough(self, send, status, headers, body: bytes) -> None:
        out_headers = [
            (key, value) for key, value in (headers or []) if key.lower() != b"content-length"
        ]
        out_headers.append((b"content-length", str(len(body)).encode("latin-1")))
        await send({"type": "http.response.start", "status": status or 500, "headers": out_headers})
        await send({"type": "http.response.body", "body": body})

    async def _transform(self, payload: Any, futures_symbol: str, index_symbol: str) -> Any:
        basis = await resolve_basis(_db_manager(), futures_symbol)
        if basis is None:
            return payload

        projected = project_payload(
            payload,
            basis,
            tick=projection_tick(futures_symbol),
            relabel=(index_symbol, futures_symbol),
        )

        # Prefer the OBSERVED futures print over a projected one for spot.
        # Only the top level is substituted: a nested per-row "spot" is that
        # row's historical spot, not the live one, and must stay projected.
        if isinstance(projected, dict) and any(k in projected for k in SPOT_FIELDS):
            live = await _live_futures_spot(index_symbol)
            if live is not None:
                for spot_key in SPOT_FIELDS:
                    if spot_key in projected and projected[spot_key] is not None:
                        projected[spot_key] = live

        if isinstance(projected, dict):
            projected["projection"] = projection_metadata(basis)
        return projected
