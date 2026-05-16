"""ASGI middleware for the API.

RequestIdMiddleware
    Reads ``X-Request-Id`` from the incoming request (or generates a fresh
    UUID4 hex), stashes it in ``src.utils.logging.request_id_var`` so any
    log line emitted while the request is being handled carries the id,
    and echoes it back as ``X-Request-Id`` on the response. Pure-ASGI
    rather than ``BaseHTTPMiddleware`` so we don't break streaming
    responses or swallow exception context.

AuditLogMiddleware
    Emits exactly one structured log line per request — method, path,
    status, resolved caller + end-user identity, and latency — reading the
    :class:`~src.api.identity.RequestIdentity` that ``api_key_auth``
    stashed on ``request.state``.  Middleware (not per-route) so a new
    endpoint can never silently skip auditing.  Best-effort: it never
    raises and never blocks the response, and it writes a log line (not a
    DB row) so a database outage can't turn into a 500.
"""

from __future__ import annotations

import time

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.utils import get_logger
from src.utils.logging import new_request_id, request_id_var

_audit_logger = get_logger("src.api.audit")


class RequestIdMiddleware:
    """Pure-ASGI request-id propagation."""

    def __init__(self, app: ASGIApp, header_name: str = "X-Request-Id") -> None:
        self.app = app
        self.header_name = header_name
        self._header_name_lower = header_name.lower().encode("latin-1")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = MutableHeaders(scope=scope)
        incoming = headers.get(self.header_name)
        request_id = incoming if incoming else new_request_id()
        token = request_id_var.set(request_id)

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                response_headers = MutableHeaders(scope=message)
                response_headers[self.header_name] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            request_id_var.reset(token)


class AuditLogMiddleware:
    """Pure-ASGI per-request audit line.

    Reads ``scope["state"]["identity"]`` after the app has run (the auth
    dependency populates it deep inside routing, mutating the shared scope
    state dict this middleware can see on the way back out).  Logged via
    the standard logger so the request-id filter and structured formatter
    apply automatically.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        status_holder = {"code": 0}

        async def send_with_status(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["code"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_with_status)
        finally:
            try:
                duration_ms = (time.perf_counter() - start) * 1000.0
                identity = (scope.get("state") or {}).get("identity")
                caller_kind = getattr(identity, "caller_kind", "anonymous")
                caller_user_id = getattr(identity, "caller_user_id", None)
                end_user_id = getattr(identity, "end_user_id", None)
                _audit_logger.info(
                    "api_request method=%s path=%s status=%s caller_kind=%s "
                    "caller_user_id=%s end_user_id=%s duration_ms=%.1f",
                    scope.get("method", "-"),
                    scope.get("path", "-"),
                    status_holder["code"],
                    caller_kind,
                    caller_user_id or "-",
                    end_user_id or "-",
                    duration_ms,
                )
            except Exception:
                # Auditing must never break or slow a response.
                pass
