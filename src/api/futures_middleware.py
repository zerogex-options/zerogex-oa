"""Serve ES / NQ by projecting the SPX / NDX surfaces onto the futures axis.

A request for ``symbol=ES`` is answered by running the ordinary ``SPX``
handler and carrying its price-space fields across on the way out.  Doing
this once, in middleware, is what makes ES / NQ work across the *whole* API
— including the routers (``/api/v1/levels``, forced flow, trade bias) —
instead of requiring each of thirty-odd endpoints to grow its own futures
branch and drift out of step with the others.

Flow per request:

1. **Inbound** — if ``symbol`` / ``underlying`` (query) or the final path
   segment names a first-class future, rewrite it to the backing cash index
   so the handler runs completely unaware that futures exist.
2. **Outbound** — resolve the basis, project the allowlisted price fields
   (:mod:`src.jobs.futures_projection`), relabel the symbol back to the
   future, and attach a ``projection`` block recording the ratio used.

Two deliberate carve-outs:

* ``/api/market/quote`` and ``/api/market/historical`` are bypassed.  They
  serve the future's OWN bars out of ``futures_quotes``, because projecting
  a frozen 16:00 SPX print would report where ES was at the bell rather
  than where it is now.  Levels may be projected; prices must be observed.
* ``spot_price`` on a projected payload is likewise overridden with the live
  futures print when one is available, and only falls back to the projected
  value if the futures feed is empty.

Requests that name no future are passed straight through untouched — no
buffering, no parse, no added latency on the SPX/SPY/QQQ/NDX hot path.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from src.jobs.futures_projection import (
    project_payload,
    projection_metadata,
    projection_tick,
    resolve_basis,
)
from src.symbols import is_futures_symbol, resolve_futures_index

logger = logging.getLogger("zerogex.futures_middleware")

# Query parameters that name the underlying across the API surface.
_SYMBOL_PARAMS = ("symbol", "underlying")

# Endpoints that resolve futures natively from ``futures_quotes`` and must
# NOT have their observed prices overwritten by a projection.
_BYPASS_PATHS = frozenset(
    {
        "/api/market/quote",
        "/api/market/historical",
    }
)

# Keys whose *value* is a symbol label and should read as the future.
_LABEL_KEYS = ("symbol", "underlying", "data_symbol")


def _futures_target(request: Request) -> Optional[tuple[str, str]]:
    """Return ``(futures_symbol, index_symbol)`` this request is asking for.

    ``None`` when the request names no first-class future, which is the
    overwhelmingly common case and the one that must stay free.
    """
    for param in _SYMBOL_PARAMS:
        raw = request.query_params.get(param)
        if raw and is_futures_symbol(raw):
            index = resolve_futures_index(raw)
            if index:
                return raw.strip().upper(), index

    # Path-addressed symbols, e.g. /api/v1/levels/ES. Only the final segment
    # is considered, so an unrelated path can never be rewritten by accident.
    tail = request.url.path.rstrip("/").rsplit("/", 1)[-1]
    if tail and is_futures_symbol(tail):
        index = resolve_futures_index(tail)
        if index:
            return tail.strip().upper(), index
    return None


def _rewrite_scope(request: Request, futures_symbol: str, index_symbol: str) -> None:
    """Point the request at the backing index, in place."""
    scope = request.scope

    query = parse_qsl(scope.get("query_string", b"").decode("latin-1"), keep_blank_values=True)
    if query:
        rewritten = [
            (key, index_symbol if key in _SYMBOL_PARAMS and is_futures_symbol(value) else value)
            for key, value in query
        ]
        scope["query_string"] = urlencode(rewritten).encode("latin-1")

    path = scope.get("path", "")
    stripped = path.rstrip("/")
    if stripped.rsplit("/", 1)[-1].upper() == futures_symbol:
        head = stripped.rsplit("/", 1)[0]
        scope["path"] = f"{head}/{index_symbol}" + ("/" if path.endswith("/") else "")
        scope["raw_path"] = scope["path"].encode("latin-1")


def _relabel(payload: Any, index_symbol: str, futures_symbol: str) -> Any:
    """Swap symbol labels from the backing index back to the future.

    Only exact matches under known label keys are touched, so a strike list
    or a free-text note that happens to mention the index is left alone.
    """
    if isinstance(payload, dict):
        out = {}
        for key, value in payload.items():
            if key in _LABEL_KEYS and isinstance(value, str) and value.upper() == index_symbol:
                out[key] = futures_symbol
            else:
                out[key] = _relabel(value, index_symbol, futures_symbol)
        return out
    if isinstance(payload, list):
        return [_relabel(item, index_symbol, futures_symbol) for item in payload]
    return payload


def _db_manager():
    """The live DatabaseManager, or None before startup.

    Imported late (``src.api.main`` imports this module) and via a single
    seam so tests can drive the middleware without standing up the app.
    """
    from src.api.main import db_manager

    return db_manager


async def _live_futures_spot(index_symbol: str) -> Optional[float]:
    """Latest observed futures close for ``index_symbol``, if the feed has one."""
    try:
        db_manager = _db_manager()
        if db_manager is None:
            return None
        quote = await db_manager.get_latest_future_quote(index_symbol)
        if quote and quote.get("close") is not None:
            return float(quote["close"])
    except Exception as e:
        logger.debug("live futures spot unavailable for %s: %s", index_symbol, e)
    return None


class FuturesProjectionMiddleware(BaseHTTPMiddleware):
    """Answer ES / NQ requests from the SPX / NDX surfaces."""

    async def dispatch(self, request: Request, call_next):
        target = _futures_target(request)
        if target is None or request.url.path in _BYPASS_PATHS:
            return await call_next(request)

        futures_symbol, index_symbol = target
        _rewrite_scope(request, futures_symbol, index_symbol)

        response = await call_next(request)

        content_type = response.headers.get("content-type", "")
        if response.status_code != 200 or "application/json" not in content_type:
            return response

        body = b"".join([chunk async for chunk in response.body_iterator])
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            return Response(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=content_type,
            )

        try:
            payload = await self._project(payload, futures_symbol, index_symbol)
            body = json.dumps(payload).encode("utf-8")
            projected = "1"
        except Exception as e:
            # A projection failure must not turn a good SPX answer into a
            # 500 — but it must never ship un-projected cash levels under an
            # ES label either, so the response is failed explicitly.
            logger.error("futures projection failed for %s: %s", futures_symbol, e, exc_info=True)
            return Response(
                content=json.dumps(
                    {"detail": f"Could not project {index_symbol} levels onto {futures_symbol}."}
                ).encode("utf-8"),
                status_code=503,
                media_type="application/json",
            )

        headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in ("content-length", "content-encoding")
        }
        headers["X-ZeroGEX-Projection"] = projected
        return Response(
            content=body,
            status_code=response.status_code,
            headers=headers,
            media_type="application/json",
        )

    async def _project(self, payload: Any, futures_symbol: str, index_symbol: str) -> Any:
        basis = await resolve_basis(_db_manager(), futures_symbol)
        if basis is None:
            return payload

        projected = project_payload(payload, basis, tick=projection_tick(futures_symbol))
        projected = _relabel(projected, index_symbol, futures_symbol)

        # Prefer the observed futures print over a projected one for spot.
        if isinstance(projected, dict) and "spot_price" in projected:
            live = await _live_futures_spot(index_symbol)
            if live is not None:
                projected["spot_price"] = live

        if isinstance(projected, dict):
            projected["projection"] = projection_metadata(basis)
        return projected
