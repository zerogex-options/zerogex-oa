"""Shared error-handling helpers for API routes.

``@handle_api_errors("operation name")`` wraps an async route handler to:

* Let :class:`fastapi.HTTPException` propagate unchanged (so ``404`` etc.
  are not swallowed).
* Log every other exception with a full traceback (``exc_info=True``) but
  *without* embedding user-provided inputs into the log message — the
  ``operation`` argument is a static string chosen by the developer.
* Re-raise a generic ``500 Internal server error`` to the client, so the
  exception body is never leaked over the wire.

This replaces ~25 copies of the same try/except/raise pattern across the
API layer.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Awaitable, Callable, TypeVar

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def handle_api_errors(operation: str) -> Callable[[F], F]:
    """Return a decorator that standardizes error handling for one route.

    Parameters
    ----------
    operation:
        Human-readable label for logs.  Must NOT contain request data.
    """

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except HTTPException:
                raise
            except Exception:
                logger.exception("%s failed", operation)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Internal server error",
                )

        return wrapped  # type: ignore[return-value]

    return decorator


def not_found(detail: str) -> HTTPException:
    """Sugar for ``HTTPException(404)``."""
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
