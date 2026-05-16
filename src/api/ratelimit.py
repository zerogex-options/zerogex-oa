"""Per-end-user rate limiting — log-only by default.

Keyed on the resolved :class:`~src.api.identity.RequestIdentity`:
the end-user when known, else the API caller, else the client IP.  This
ships **disabled**, and even when enabled defaults to **log-only**, so it
can be wired into production safely: it will never ``429`` a real user
until ``END_USER_RATE_LIMIT_ENFORCE`` is explicitly set, by which point
the "would-block" log lines have given real per-user traffic data to size
the limit against.

Backend is an in-memory fixed-window counter, **per worker**.  That is
deliberately the simplest thing that satisfies "log-only first": for
multi-worker *enforcement* with a shared view, swap this for ``slowapi``
backed by Redis (key function below ports over unchanged) — see the
"Rate limiting" note in ``API_Guide.md``.

Run as a global FastAPI dependency *after* ``api_key_auth`` so
``request.state.identity`` is populated before the key is computed.
"""

from __future__ import annotations

import os
import time
from typing import Dict, Tuple

from fastapi import HTTPException, Request, status

from src.utils import get_logger

from .identity import ANONYMOUS, RequestIdentity

logger = get_logger("src.api.ratelimit")


def _flag(name: str) -> bool:
    return (os.getenv(name, "false") or "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_ENABLED: bool = _flag("END_USER_RATE_LIMIT_ENABLED")
_ENFORCE: bool = _flag("END_USER_RATE_LIMIT_ENFORCE")
_LIMIT: int = int(os.getenv("END_USER_RATE_LIMIT_REQUESTS", "600") or "600")
_WINDOW: int = int(os.getenv("END_USER_RATE_LIMIT_WINDOW_SECONDS", "60") or "60")
# Cap distinct tracked keys so a flood of unique principals (or spoofed
# end-user ids) can't grow the counter map without bound.
_MAX_KEYS: int = int(os.getenv("END_USER_RATE_LIMIT_MAX_KEYS", "100000") or "100000")

# key -> (window_start_epoch, count)
_buckets: Dict[str, Tuple[int, int]] = {}


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def rate_limit_key(identity: RequestIdentity, request: Request) -> str:
    """Most specific principal available: end-user > caller > client IP."""
    if identity.end_user_id:
        return f"eu:{identity.end_user_id}"
    if identity.caller_user_id:
        return f"cu:{identity.caller_user_id}"
    return f"ip:{_client_ip(request)}"


def _hit(key: str) -> Tuple[int, bool]:
    """Register one request against ``key``; return (count, over_limit)."""
    now = int(time.time())
    window_start, count = _buckets.get(key, (now, 0))
    if now - window_start >= _WINDOW:
        window_start, count = now, 0
    count += 1
    if key not in _buckets and len(_buckets) >= _MAX_KEYS:
        # Evict the entries whose window has already rolled over; if none
        # have, drop an arbitrary one rather than grow unbounded.
        stale = [k for k, (ws, _) in _buckets.items() if now - ws >= _WINDOW]
        for k in stale or [next(iter(_buckets))]:
            _buckets.pop(k, None)
    _buckets[key] = (window_start, count)
    return count, count > _LIMIT


async def rate_limit(request: Request) -> None:
    """Global dependency.  No-op unless explicitly enabled."""
    if not _ENABLED:
        return
    identity = getattr(request.state, "identity", ANONYMOUS)
    key = rate_limit_key(identity, request)
    count, over = _hit(key)
    if not over:
        return
    if _ENFORCE:
        logger.warning(
            "rate limit EXCEEDED key=%s count=%d limit=%d/%ds — rejecting",
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
        "rate limit WOULD-BLOCK key=%s count=%d limit=%d/%ds "
        "(log-only; set END_USER_RATE_LIMIT_ENFORCE=1 to enforce)",
        key,
        count,
        _LIMIT,
        _WINDOW,
    )
