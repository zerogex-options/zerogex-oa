"""Lazy, optional Redis pool for multi-worker rate limiting and response cache.

Both consumers — the distributed rate limiter and the response cache — need
the *same* Redis instance to coordinate state across uvicorn workers, so they
share one connection pool managed here. The pool is:

* **Optional**: nothing is created unless ``REDIS_URL`` is set. Modules that
  need Redis call :func:`get_pool` and treat ``None`` as "fall back to the
  in-memory path." This keeps every dependency on Redis a graceful degradation
  rather than a startup gate.
* **Lazy**: created on first request after :func:`configure` is called. Tests
  can monkey-patch ``REDIS_URL`` and re-import without touching the wire.
* **Resilient**: a connection failure is logged once and the pool is dropped
  so the next caller retries; it never raises into the request path. Each
  consumer is responsible for its own per-call failure handling (the rate
  limiter fails open + logs; the cache treats it as a MISS).

Pool sizing: a single uvicorn worker handling a steady 1k rps with a 200µs
hot-path Redis hit needs roughly ``rps × p99_latency = ~10`` concurrent
connections. The default ``REDIS_MAX_CONNECTIONS=32`` leaves comfortable
headroom; bump it if the limiter or cache logs ``ConnectionPoolFullError``.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Read once at import time. A reconfiguration after startup is intentionally
# not supported: changing the Redis backend under a running app would mean
# every in-flight rate-limit / cache decision was made against a different
# coordinator than the one that committed it.
_REDIS_URL: Optional[str] = (os.getenv("REDIS_URL") or "").strip() or None
_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "32") or "32")
_SOCKET_TIMEOUT: float = float(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "0.5") or "0.5")

# We import lazily inside _build_pool so the rest of the API can boot when
# the `redis` package is absent (tests, minimal deployments). The package is
# listed in the `api` extra so a normal install pulls it in.
_pool: Optional[Any] = None
_disabled_reason_logged = False


def is_configured() -> bool:
    """``True`` when ``REDIS_URL`` is set in the environment."""
    return _REDIS_URL is not None


def _build_pool() -> Optional[Any]:
    """Create the connection pool, or log + return ``None`` on failure.

    Import errors are demoted to a single info log so a deployment without
    ``redis`` installed degrades to the in-memory path instead of refusing
    to start.
    """
    global _disabled_reason_logged
    if _REDIS_URL is None:
        return None
    try:
        from redis import asyncio as redis_asyncio  # type: ignore[import-not-found]
    except Exception:
        if not _disabled_reason_logged:
            logger.info(
                "Redis disabled: the `redis` package is not installed; "
                "rate limiting and response cache will fall back to the "
                "per-worker in-memory path."
            )
            _disabled_reason_logged = True
        return None
    try:
        return redis_asyncio.ConnectionPool.from_url(
            _REDIS_URL,
            max_connections=_MAX_CONNECTIONS,
            socket_timeout=_SOCKET_TIMEOUT,
            socket_connect_timeout=_SOCKET_TIMEOUT,
            decode_responses=False,
        )
    except Exception:
        logger.warning(
            "Failed to build Redis connection pool; falling back to in-memory",
            exc_info=True,
        )
        return None


def get_pool() -> Optional[Any]:
    """Return the shared pool, creating it on first call. ``None`` if unset."""
    global _pool
    if _pool is None:
        _pool = _build_pool()
    return _pool


def get_client() -> Optional[Any]:
    """Return a Redis client bound to the shared pool, or ``None``.

    Each call returns a fresh client object that draws connections from the
    shared pool — the recommended usage pattern with ``redis-py``'s asyncio
    layer because ``Redis(connection_pool=...)`` is cheap and clients do not
    hold a connection between commands.
    """
    pool = get_pool()
    if pool is None:
        return None
    try:
        from redis import asyncio as redis_asyncio  # type: ignore[import-not-found]
    except Exception:
        return None
    try:
        return redis_asyncio.Redis(connection_pool=pool)
    except Exception:
        logger.warning("Failed to acquire Redis client from pool", exc_info=True)
        return None


async def close() -> None:
    """Disconnect the pool; safe to call when no pool exists."""
    global _pool
    pool, _pool = _pool, None
    if pool is None:
        return
    try:
        await pool.disconnect(inuse_connections=True)
    except Exception:
        logger.warning("Error closing Redis pool", exc_info=True)


async def ping() -> bool:
    """Best-effort health probe: ``True`` iff Redis answers a PING."""
    client = get_client()
    if client is None:
        return False
    try:
        return bool(await client.ping())
    except Exception:
        return False
