"""API authentication — optional API-key scheme.

When the ``API_KEY`` environment variable is set, every endpoint that
depends on :func:`api_key_auth` requires the client to present that key
in either an ``X-API-Key`` header or an ``Authorization: Bearer <key>``
header.  When ``API_KEY`` is unset the dependency is a no-op so local
development and CI continue to work without credentials.

Secrets comparison uses ``hmac.compare_digest`` to resist timing attacks.
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Optional

from fastapi import Header, HTTPException, status

logger = logging.getLogger(__name__)

_API_KEY: Optional[str] = (os.getenv("API_KEY") or "").strip() or None
_ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development").strip().lower()

if _API_KEY is None:
    if _ENVIRONMENT == "production":
        # In production this is almost certainly a misconfiguration.  Make
        # it impossible to miss in the logs.
        logger.error(
            "API_KEY is not set but ENVIRONMENT=production; API endpoints "
            "are exposed without authentication.  Set API_KEY to enable auth."
        )
    else:
        # Dev/CI: a single INFO line at startup is plenty.  Previously this
        # was a WARNING and fired on every process start, which was noisy
        # in non-prod logs where auth is intentionally disabled.
        logger.info(
            "API authentication is disabled (API_KEY not set).  Set API_KEY "
            "and ENVIRONMENT=production to enable."
        )


def _matches(provided: Optional[str]) -> bool:
    if not provided or _API_KEY is None:
        return False
    return hmac.compare_digest(provided, _API_KEY)


async def api_key_auth(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None),
) -> None:
    """FastAPI dependency that enforces the configured API key."""
    if _API_KEY is None:
        return  # auth disabled
    if _matches(x_api_key):
        return
    if authorization and authorization.lower().startswith("bearer "):
        if _matches(authorization[7:].strip()):
            return
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "Bearer"},
    )
