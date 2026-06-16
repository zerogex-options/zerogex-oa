"""Identity-keyed request rate limiting — log-only, off by default.

Layered on top of the end-user attribution work so a noisy end-user (or
caller) can be throttled independently of the shared website API key.
Three states, selected by env:

* **off** (default) — :func:`rate_limit` is a no-op.
* **log-only** (``END_USER_RATE_LIMIT_ENABLED=1``) — counts requests and
  logs a ``WOULD-BLOCK`` warning when a key exceeds the limit, but never
  rejects. Use this to size limits against real traffic before enforcing.
* **enforce** (``+ END_USER_RATE_LIMIT_ENFORCE=1``) — over-limit
  requests get ``429`` with a ``Retry-After`` header.

Two backing stores, selected at import time:

* **In-memory fixed-window map** — the default. Per-worker, which is fine
  for a single-uvicorn-worker deployment and adequate for a smoke /
  observability rollout. Memory is bounded by ``_MAX_KEYS`` with an LRU-ish
  eviction.
* **Redis fixed-window counter** — activated when ``REDIS_URL`` is set (see
  ``redis_client.py``). One ``INCR`` per request against a key whose name
  is ``rl:{window_start}:{bucket}``, with ``EXPIRE`` set on the first hit
  so old windows reap themselves. Sums correctly across N workers, which
  is what the operator gets when they scale beyond a single uvicorn
  process. The two backends share the same :func:`rate_limit_key`
  derivation so an env-only switch between them keeps the bucketing
  semantics identical.

A Redis failure transparently falls back to the in-memory counter for that
request and logs at debug — the rate limiter must never be a SPOF.

Wired as a global dependency that runs *after* the auth dependency, so
``request.state.identity`` is already populated.
"""

from __future__ import annotations

import logging
import os
from src.config import _getenv_int
import time
from typing import Dict, Tuple

from fastapi import HTTPException, Request, status

from . import redis_client
from .identity import ANONYMOUS

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or default).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_ENABLED = _env_flag("END_USER_RATE_LIMIT_ENABLED")
_ENFORCE = _env_flag("END_USER_RATE_LIMIT_ENFORCE")
_LIMIT = _getenv_int("END_USER_RATE_LIMIT_REQUESTS", 600)
_WINDOW = _getenv_int("END_USER_RATE_LIMIT_WINDOW_SECONDS", 60)
_MAX_KEYS = _getenv_int("END_USER_RATE_LIMIT_MAX_KEYS", 100000)


def _parse_trusted_proxies() -> frozenset[str]:
    """Trusted reverse-proxy IPs from ``RATE_LIMIT_TRUSTED_PROXIES``.

    Comma-separated. When empty (default), X-Forwarded-For is never
    consulted and the rate-limit bucket falls back to the direct peer
    IP — the pre-existing behavior.
    """
    raw = os.getenv("RATE_LIMIT_TRUSTED_PROXIES", "")
    return frozenset(p.strip() for p in raw.split(",") if p.strip())


_TRUSTED_PROXIES = _parse_trusted_proxies()


def _client_ip(request: Request) -> str:
    """Resolve the real client IP for rate-limit bucketing.

    Behind a reverse proxy the direct peer (``request.client.host``) is
    the proxy itself, so every anonymous request collides into one
    bucket — turning the per-IP limit into a single global counter (or,
    if X-Forwarded-For were trusted blindly, into a trivially spoofable
    one). We only consult X-Forwarded-For when the *direct peer* is in
    the operator-configured ``RATE_LIMIT_TRUSTED_PROXIES`` set, then walk
    the chain right-to-left past any further trusted hops to the first
    untrusted address (the real client). Otherwise we use the peer IP
    directly — XFF from an untrusted peer is attacker-controlled and
    ignored.
    """
    host = request.client.host if request.client else "unknown"
    if not _TRUSTED_PROXIES or host not in _TRUSTED_PROXIES:
        return host
    xff = request.headers.get("X-Forwarded-For", "")
    chain = [ip.strip() for ip in xff.split(",") if ip.strip()]
    for ip in reversed(chain):
        if ip not in _TRUSTED_PROXIES:
            return ip
    # Whole chain was trusted proxies (or XFF absent) — fall back to peer.
    return host


def rate_limit_key(identity, request: Request) -> str:
    """Most-specific stable bucket key: end-user > caller > client IP."""
    if getattr(identity, "end_user_id", None):
        return f"eu:{identity.end_user_id}"
    if getattr(identity, "caller_user_id", None):
        return f"cu:{identity.caller_user_id}"
    return f"ip:{_client_ip(request)}"


# key -> (window_start_epoch, count). Per-worker; see module docstring.
_counters: Dict[str, Tuple[int, int]] = {}


def _evict(window_start: int) -> None:
    """Bound memory: drop rolled-over windows, else one arbitrary entry."""
    rolled = [k for k, (ws, _) in _counters.items() if ws != window_start]
    if rolled:
        for k in rolled:
            del _counters[k]
        return
    try:
        _counters.pop(next(iter(_counters)))
    except StopIteration:
        pass


async def _count_in_memory(key: str, window_start: int) -> int:
    """Single-worker fixed-window counter. Returns the new count."""
    entry = _counters.get(key)
    if entry is not None and entry[0] == window_start:
        count = entry[1] + 1
        _counters[key] = (window_start, count)
        return count
    if key not in _counters and len(_counters) >= _MAX_KEYS:
        _evict(window_start)
    _counters[key] = (window_start, 1)
    return 1


async def _count_redis(key: str, window_start: int) -> int:
    """Multi-worker fixed-window counter backed by Redis INCR + EXPIRE.

    Returns the new count, or ``-1`` to mean "Redis unavailable; caller
    should fall back to the in-memory path." The redis-key namespace is
    versioned so an incompatible change in the future can be rolled out
    without flushing the running window.
    """
    client = redis_client.get_client()
    if client is None:
        return -1
    redis_key = f"zgx:rl:v1:{window_start}:{key}"
    try:
        pipe = client.pipeline(transaction=False)
        pipe.incr(redis_key)
        # Set EXPIRE every call: cheap, idempotent, and guarantees the
        # TTL is in place even if the very first INCR's EXPIRE call was
        # lost to a transient error. Two-window expiry so an entry can't
        # vanish under a window-boundary read.
        pipe.expire(redis_key, _WINDOW * 2)
        results = await pipe.execute()
        # results == [new_count, expire_ok]
        return int(results[0])
    except Exception:
        logger.debug("RATE_LIMIT redis INCR failed; in-memory fallback", exc_info=True)
        return -1


async def rate_limit(request: Request) -> None:
    """Global dependency: count this request; optionally 429 over-limit.

    No-op unless ``END_USER_RATE_LIMIT_ENABLED``. Must run after the auth
    dependency so ``request.state.identity`` exists. When Redis is
    configured the count comes from a shared INCR so the limit holds
    across workers and hosts; otherwise (or on a Redis hiccup) it falls
    back to the per-worker in-memory counter.
    """
    if not _ENABLED:
        return

    identity = getattr(request.state, "identity", ANONYMOUS)
    key = rate_limit_key(identity, request)

    now = int(time.time())
    window_start = now - (now % _WINDOW)

    # Redis-first; transparent fallback on miss.
    count = await _count_redis(key, window_start)
    if count < 0:
        count = await _count_in_memory(key, window_start)

    if count <= _LIMIT:
        return

    if _ENFORCE:
        logger.warning(
            "RATE_LIMIT exceeded key=%s count=%d limit=%d window=%ds; rejecting",
            key,
            count,
            _LIMIT,
            _WINDOW,
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(_WINDOW)},
        )

    logger.warning(
        "RATE_LIMIT WOULD-BLOCK key=%s count=%d limit=%d window=%ds (log-only)",
        key,
        count,
        _LIMIT,
        _WINDOW,
    )
