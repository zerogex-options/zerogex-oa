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
from typing import Any, Dict, Optional, Tuple

from fastapi import Header, HTTPException, Security, status
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# Declared as security schemes so Swagger UI / ReDoc surface an
# "Authorize" button.  ``auto_error=False`` lets us combine the two
# schemes inside one dependency function without FastAPI raising
# 403 on its own when a header is absent.
_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)
_bearer_scheme = HTTPBearer(auto_error=False, bearerFormat="API-Key")

_API_KEY: Optional[str] = (os.getenv("API_KEY") or "").strip() or None
_ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development").strip().lower()


def _hash_key(raw: str) -> str:
    """SHA-256 hex digest of the raw key — what the DB stores."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class _KeyStore:
    """In-process cache + DB pool reference for per-user API keys.

    The DB pool is registered at app startup via :meth:`configure`; if no
    pool is registered the store reports ``is_enabled() == False`` and the
    auth dependency falls back to static-key-only behavior.
    """

    def __init__(self) -> None:
        self._pool: Any = None  # asyncpg.Pool, untyped to avoid hard import
        self._cache: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}
        self._cache_ttl: float = float(os.getenv("API_KEY_CACHE_TTL_SECONDS", "60"))
        self._touch_throttle_seconds: float = 60.0
        self._last_touch: Dict[str, float] = {}

    def configure(self, pool: Any) -> None:
        """Register (or clear) the DB pool used for key lookups."""
        self._pool = pool
        self._cache.clear()
        self._last_touch.clear()

    def is_enabled(self) -> bool:
        return self._pool is not None

    def invalidate(self) -> None:
        """Drop the lookup cache — call after CLI mutations to api_keys."""
        self._cache.clear()

    async def lookup(self, raw_key: str) -> Optional[Dict[str, Any]]:
        """Return user info dict for an active key, or ``None``."""
        if self._pool is None:
            return None
        key_hash = _hash_key(raw_key)
        now = _time.monotonic()
        cached = self._cache.get(key_hash)
        if cached and cached[0] > now:
            return cached[1]
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, user_id, name, scopes
                    FROM api_keys
                    WHERE key_hash = $1
                      AND revoked_at IS NULL
                    """,
                    key_hash,
                )
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
            asyncio.create_task(self._touch(key_hash))
        except RuntimeError:
            # No running loop — nothing to schedule (e.g. during a sync test).
            pass

    async def _touch(self, key_hash: str) -> None:
        if self._pool is None:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = $1",
                    key_hash,
                )
        except Exception:
            logger.debug("last_used_at touch failed", exc_info=True)


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
    authorization: Optional[str],
) -> Optional[str]:
    if x_api_key:
        return x_api_key
    if bearer and bearer.credentials:
        return bearer.credentials
    # Final fallback for callers that send a non-"Bearer" Authorization
    # header that HTTPBearer ignores (rare, but preserves the original
    # behavior of api_key_auth from before the security-scheme migration).
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        return token or None
    return None


async def api_key_auth(
    x_api_key: Optional[str] = Security(_api_key_scheme),
    bearer: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
    authorization: Optional[str] = Header(default=None),
) -> Optional[Dict[str, Any]]:
    """FastAPI dependency that enforces the configured API-key scheme."""
    static_enabled = _API_KEY is not None
    db_enabled = key_store.is_enabled()
    if not static_enabled and not db_enabled:
        return None  # auth fully disabled (dev/CI)

    candidate = _extract_candidate(x_api_key, bearer, authorization)
    if candidate:
        if static_enabled and _matches_static(candidate):
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
