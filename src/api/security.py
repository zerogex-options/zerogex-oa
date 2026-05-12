"""API authentication — static or per-user API keys.

Two authentication mechanisms are supported and may be enabled together:

1. **Static shared key** — set ``API_KEY`` in the environment.  Accepted in
   either an ``X-API-Key`` header or an ``Authorization: Bearer <key>``
   header.  Comparison uses :func:`hmac.compare_digest`.  Useful for
   service-to-service calls and the legacy single-credential deployment.

2. **Per-user DB-backed keys** — long-lived keys stored in the ``api_keys``
   table (see ``setup/database/schema.sql``).  Provision with the
   ``src.api.admin_keys`` CLI; it returns a ``secrets.token_urlsafe`` key
   shown exactly once.  At validation time the presented key is SHA-256
   hashed and looked up against the table; non-revoked rows authorize the
   request and ``last_used_at`` is touched (throttled).

When neither mechanism is configured (no ``API_KEY`` set and no DB pool
registered with :data:`key_store`), the dependency is a no-op so local
development and CI continue to work without credentials.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import time as _time
from typing import Any, Callable, Dict, Optional, Set, Tuple

import asyncpg

from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# HTTPBearer is the only declared security scheme so Swagger UI surfaces a
# single "Authorize" entry. X-API-Key is still accepted (read directly from
# request headers in api_key_auth below) for the colocated BFF and any
# legacy caller that hasn't moved to Authorization: Bearer yet, but it's
# no longer advertised in the OpenAPI spec to keep the Authorize modal
# unambiguous. ``auto_error=False`` lets the dependency raise its own 401.
_bearer_scheme = HTTPBearer(auto_error=False, bearerFormat="API-Key")

_API_KEY: Optional[str] = (os.getenv("API_KEY") or "").strip() or None
_ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development").strip().lower()


def _hash_key(raw: str) -> str:
    """SHA-256 hex digest of the raw key — what the DB stores."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class _KeyStore:
    """In-process cache + DB pool getter for per-user API keys.

    The DB pool is registered at app startup via :meth:`configure` with a
    callable that returns the current pool.  Looking it up lazily on every
    request means a reconnect inside ``DatabaseManager`` (which replaces
    ``DatabaseManager.pool`` with a fresh pool and closes the old one) is
    transparent to the key store — the next ``lookup`` picks up the new
    pool instead of holding a stale reference to the closed one.

    If no getter is registered, or the getter returns ``None``, the store
    reports ``is_enabled() == False`` and the auth dependency falls back
    to static-key-only behavior.
    """

    def __init__(self) -> None:
        self._get_pool: Optional[Callable[[], Any]] = None
        self._cache: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}
        self._cache_ttl: float = float(os.getenv("API_KEY_CACHE_TTL_SECONDS", "60"))
        self._touch_throttle_seconds: float = 60.0
        self._last_touch: Dict[str, float] = {}
        self._touch_tasks: Set[asyncio.Task] = set()

    def configure(self, get_pool: Optional[Callable[[], Any]]) -> None:
        """Register (or clear) a callable that returns the current DB pool.

        Pass ``lambda: db_manager.pool`` so that any future reconnect in
        ``DatabaseManager`` is picked up on the next lookup.  Pass ``None``
        to disable DB-backed auth (e.g. during shutdown).
        """
        self._get_pool = get_pool
        self._cache.clear()
        self._last_touch.clear()

    def is_enabled(self) -> bool:
        return self._get_pool is not None

    def invalidate(self) -> None:
        """Drop the lookup cache — call after CLI mutations to api_keys."""
        self._cache.clear()

    async def lookup(self, raw_key: str) -> Optional[Dict[str, Any]]:
        """Return user info dict for an active key, or ``None``."""
        if self._get_pool is None:
            return None
        pool = self._get_pool()
        if pool is None:
            return None
        key_hash = _hash_key(raw_key)
        now = _time.monotonic()
        cached = self._cache.get(key_hash)
        if cached and cached[0] > now:
            return cached[1]
        try:
            async with pool.acquire() as conn:
                # `scopes` is selected for the audit/info dict but not
                # enforced by the auth dependency. To enforce, callers
                # would need a per-endpoint required-scope declaration
                # (e.g. `Depends(require_scope("signals:read"))`) that
                # inspects the info dict this lookup returns.
                row = await conn.fetchrow(
                    """
                    SELECT id, user_id, name, scopes
                    FROM api_keys
                    WHERE key_hash = $1
                      AND revoked_at IS NULL
                    """,
                    key_hash,
                )
        except asyncpg.exceptions.InterfaceError as e:
            if "pool is closed" in str(e).lower():
                logger.error(
                    "POOL_CLOSED: key_store lookup hit asyncpg "
                    "InterfaceError('pool is closed') despite using "
                    "a lazy pool getter. The getter returned a closed "
                    "pool reference — investigate the DatabaseManager "
                    "lifecycle.",
                    exc_info=True,
                )
            else:
                logger.warning(
                    "API-key DB lookup failed; treating as miss",
                    exc_info=True,
                )
            return None
        except Exception:
            # DB errors must not 500 the request: log + cache miss briefly.
            logger.warning("API-key DB lookup failed; treating as miss", exc_info=True)
            return None
        info: Optional[Dict[str, Any]] = dict(row) if row else None
        self._cache[key_hash] = (now + self._cache_ttl, info)
        if info is not None:
            self._schedule_touch(key_hash)
        return info

    def _schedule_touch(self, key_hash: str) -> None:
        now = _time.monotonic()
        last = self._last_touch.get(key_hash, 0.0)
        if now - last < self._touch_throttle_seconds:
            return
        self._last_touch[key_hash] = now
        try:
            task = asyncio.create_task(self._touch(key_hash))
        except RuntimeError:
            # No running loop — nothing to schedule (e.g. during a sync test).
            return
        # Strong-ref: asyncio loop only weak-refs tasks; without this, low-traffic
        # touches can be GC'd mid-flight before the UPDATE lands.
        self._touch_tasks.add(task)
        task.add_done_callback(self._touch_tasks.discard)

    async def _touch(self, key_hash: str) -> None:
        if self._get_pool is None:
            return
        pool = self._get_pool()
        if pool is None:
            return
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = $1",
                    key_hash,
                )
        except Exception:
            logger.warning("last_used_at touch failed", exc_info=True)


key_store = _KeyStore()


if _API_KEY is None:
    if _ENVIRONMENT == "production":
        # In production this is almost certainly a misconfiguration.  Make
        # it impossible to miss in the logs.
        logger.error(
            "API_KEY is not set but ENVIRONMENT=production; static-key auth "
            "is disabled.  Per-user DB-backed keys may still be configured."
        )
    else:
        logger.info(
            "Static API_KEY auth disabled (API_KEY not set).  Per-user "
            "DB-backed keys will be checked when configured."
        )


def _matches_static(provided: Optional[str]) -> bool:
    if not provided or _API_KEY is None:
        return False
    return hmac.compare_digest(provided, _API_KEY)


def _extract_candidate(
    x_api_key: Optional[str],
    bearer: Optional[HTTPAuthorizationCredentials],
) -> Optional[str]:
    # Bearer wins over X-API-Key when both are present: nginx may still
    # `proxy_set_header X-API-Key "<static>"` during the migration window
    # (legacy include in /etc/nginx/conf.d/), which overwrites the caller's
    # X-API-Key but leaves Authorization untouched. Preferring Bearer lets a
    # caller-supplied per-user key authenticate through that path.
    if bearer and bearer.credentials:
        return bearer.credentials
    if x_api_key:
        return x_api_key
    return None


async def api_key_auth(
    request: Request,
    bearer: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
) -> Optional[Dict[str, Any]]:
    """FastAPI dependency that enforces the configured API-key scheme."""
    static_enabled = _API_KEY is not None
    db_enabled = key_store.is_enabled()
    if not static_enabled and not db_enabled:
        return None  # auth fully disabled (dev/CI)

    # Read X-API-Key from raw request headers rather than declaring it via
    # Security() so it doesn't surface as a separate Swagger Authorize entry
    # or a per-endpoint parameter.
    x_api_key = request.headers.get("X-API-Key")
    candidate = _extract_candidate(x_api_key, bearer)
    if candidate:
        if static_enabled and _matches_static(candidate):
            # Log WARNING so we can identify callers still using the static
            # break-glass credential ahead of removing API_KEY from .env. The
            # static path has no per-user attribution, so the source IP is the
            # only signal we get here. Lives at WARNING (visible at the default
            # journal level) so it surfaces in any periodic log review.
            client_host = request.client.host if request.client else "unknown"
            logger.warning(
                "STATIC_KEY auth used from %s on %s %s",
                client_host,
                request.method,
                request.url.path,
            )
            return None
        if db_enabled:
            info = await key_store.lookup(candidate)
            if info is not None:
                return info

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "Bearer"},
    )
