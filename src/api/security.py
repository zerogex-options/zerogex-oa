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

if _API_KEY is None:
    logger.warning(
        "API_KEY is not set — API endpoints are unauthenticated. "
        "Set API_KEY in production to enable auth."
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
