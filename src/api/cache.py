"""Identity-aware response cache for hot derived-analytics endpoints.

This is the read side of the Redis tier: a small decorator that wraps a
FastAPI endpoint, computes a deterministic cache key from the request, and
serves the cached JSON if present. The motivation is the same one that
justifies caching at every level of the Postgres stack — the derived
analytics endpoints (GEX summary, max-pain current, flow by-contract) are
the same shape for everyone calling them in the same minute, and Postgres is
the eventual bottleneck as B2B traffic grows.

Two non-obvious design decisions:

* **Identity-bucketed keys.** The cache key includes a coarse identity
  classifier — wildcard / per-tier hash / anonymous — so a key that
  authorized differently (e.g. an end-user-attributed call vs an anonymous
  one) can never receive a response that was computed for the wrong
  authorization class. The end-user id itself is *not* part of the key, only
  whether one is present, because the endpoint's output doesn't depend on
  who is asking — only on whether they're allowed to ask.
* **Stale-on-error.** A Redis hiccup is logged at debug and treated as a
  MISS that bypasses the cache for that request; it never raises into the
  request path. The point of caching is to remove load, not to add a SPOF.

Off by default: with ``REDIS_URL`` unset (no pool) the decorator returns the
endpoint's value unchanged and never touches the wire, so wiring it onto a
route cannot change behavior until Redis is configured.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
from typing import Any, Awaitable, Callable, Optional

from fastapi import Request

from . import redis_client

logger = logging.getLogger(__name__)

# Cache keys carry a short version tag so a backwards-incompatible change to
# the response shape can be rolled out by bumping the version — every old key
# becomes a MISS until it falls out of Redis on its own TTL, no flush needed.
_CACHE_VERSION = "v1"
_KEY_PREFIX = f"zgx:cache:{_CACHE_VERSION}:"


def _identity_bucket(request: Request) -> str:
    """Return a coarse identity classifier for cache-key segmentation.

    The classifier is intentionally coarse: ``anon``, ``end_user``, ``caller``
    or ``static``. Two requests in the same bucket receive the same cached
    body for the same path+query, so this must not leak per-user data.
    """
    identity = getattr(request.state, "identity", None)
    if identity is None:
        return "anon"
    if getattr(identity, "end_user_id", None):
        return "end_user"
    kind = getattr(identity, "caller_kind", None)
    if kind == "db":
        return "caller"
    if kind == "static":
        return "static"
    return "anon"


def _build_key(request: Request, namespace: str) -> str:
    bucket = _identity_bucket(request)
    # Sort query params so order doesn't fragment the cache.
    query_items = sorted(request.query_params.multi_items())
    payload = json.dumps(
        {
            "path": request.url.path,
            "query": query_items,
            "bucket": bucket,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"{_KEY_PREFIX}{namespace}:{bucket}:{digest}"


async def _read(key: str) -> Optional[bytes]:
    client = redis_client.get_client()
    if client is None:
        return None
    try:
        return await client.get(key)
    except Exception:
        logger.debug("cache GET failed; treating as MISS", exc_info=True)
        return None


async def _write(key: str, value: bytes, ttl_seconds: int) -> None:
    client = redis_client.get_client()
    if client is None:
        return
    try:
        await client.set(key, value, ex=max(1, ttl_seconds))
    except Exception:
        logger.debug("cache SET failed; ignoring", exc_info=True)


def _to_bytes(value: Any) -> Optional[bytes]:
    """Serialize a JSON-able endpoint return value to bytes, or ``None``.

    Pydantic models are dumped via ``model_dump`` (v2) or ``dict`` (v1) so
    the cached representation matches what FastAPI's JSON encoder would
    have produced. If the value isn't trivially serialisable we skip
    caching rather than blow up the request.
    """
    try:
        if hasattr(value, "model_dump"):
            payload = value.model_dump()  # pydantic v2
        elif hasattr(value, "dict"):
            payload = value.dict()  # pydantic v1
        else:
            payload = value
        return json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
    except Exception:
        return None


def cache_response(
    namespace: str,
    ttl_seconds: int = 30,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Wrap an async endpoint with a Redis-backed response cache.

    The wrapped function must take its ``Request`` either as an explicit
    parameter (named ``request``) or via FastAPI's standard dependency
    injection; either way we resolve it at runtime via the call kwargs.

    With no Redis configured the decorator is a transparent passthrough.

    ``namespace`` segments the keyspace so two endpoints that happen to
    share path+query (e.g. an alias) don't collide. ``ttl_seconds`` should
    track how stale the upstream data can be — derived analytics that
    refresh once a minute should sit at 30s; cached snapshots updated
    daily can sit at hours.

    Caching is opt-in per endpoint. The MISS path returns the endpoint's
    response object unchanged, so this works with both raw dicts and
    pydantic models. The cached HIT path returns a ``Response`` directly so
    FastAPI skips the re-serialisation step.
    """

    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not redis_client.is_configured():
                return await fn(*args, **kwargs)
            request: Optional[Request] = kwargs.get("request")
            if request is None:
                # Look for a Request positional arg (rare in this codebase
                # but cheap to handle for symmetry).
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
            if request is None:
                # No request handle — can't compute a key. Bypass.
                return await fn(*args, **kwargs)

            key = _build_key(request, namespace)
            cached = await _read(key)
            if cached is not None:
                # We return a Starlette Response directly — bypassing
                # FastAPI's response_model serialisation — because the
                # cached bytes are already the on-the-wire payload.
                from starlette.responses import Response as StarletteResponse

                return StarletteResponse(
                    content=cached,
                    media_type="application/json",
                    headers={"X-Cache": "HIT"},
                )

            value = await fn(*args, **kwargs)
            payload = _to_bytes(value)
            if payload is not None:
                # Write-back fire-and-forget: a slow write must never delay
                # the response. The task is registered so the loop doesn't
                # weak-ref it away mid-flight.
                task = asyncio.create_task(_write(key, payload, ttl_seconds))
                _BG_TASKS.add(task)
                task.add_done_callback(_BG_TASKS.discard)
            return value

        return wrapper

    return decorator


_BG_TASKS: "set[asyncio.Task[Any]]" = set()


async def invalidate_namespace(namespace: str) -> int:
    """Delete every cached key in ``namespace``. Returns deleted count.

    Uses SCAN + UNLINK so a large keyspace doesn't block the Redis loop on
    KEYS. Intended for admin/operational use; not called from the hot path.
    """
    client = redis_client.get_client()
    if client is None:
        return 0
    match = f"{_KEY_PREFIX}{namespace}:*"
    deleted = 0
    try:
        async for key in client.scan_iter(match=match, count=500):
            try:
                deleted += int(await client.unlink(key))
            except Exception:
                # Older Redis without UNLINK falls back to DEL.
                deleted += int(await client.delete(key))
    except Exception:
        logger.warning("cache invalidation scan failed", exc_info=True)
    return deleted
