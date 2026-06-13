"""ASGI middleware for the API.

RequestIdMiddleware
    Reads ``X-Request-Id`` from the incoming request (or generates a fresh
    UUID4 hex), stashes it in ``src.utils.logging.request_id_var`` so any
    log line emitted while the request is being handled carries the id,
    and echoes it back as ``X-Request-Id`` on the response. Pure-ASGI
    rather than ``BaseHTTPMiddleware`` so we don't break streaming
    responses or swallow exception context.

AuditLogMiddleware
    Emits exactly one structured ``src.api.audit`` line per HTTP request
    with method, path, status, the resolved caller/end-user identity
    (set on ``request.state.identity`` by the auth dependency), and the
    wall-clock duration. Pure-ASGI, same as above. Registered so that
    ``RequestIdMiddleware`` stays *outermost* — the request-id contextvar
    is still set when the audit line is emitted — while this middleware
    still wraps routing, so it observes the identity resolved during
    dependency injection. All audit work runs in a guarded ``finally`` so
    it can never break or slow a response.

UsageMeterMiddleware
    Records exactly one usage increment per HTTP request against the
    resolved identity into the process-wide :data:`src.api.usage.usage_meter`.
    Pure-ASGI and, like the audit middleware, wraps routing so it sees the
    identity the auth dependency set. The meter is a no-op unless usage
    metering is enabled, and the increment runs in a guarded ``finally`` so
    it can never break or slow a response.
"""

from __future__ import annotations

import time

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.utils import get_logger
from src.utils.logging import new_request_id, request_id_var

from .usage import usage_meter

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
    """Pure-ASGI per-request audit logging.

    Captures the response status, reads the identity the auth dependency
    stashed on ``request.state`` (mirrored into ``scope["state"]``), and
    emits one ``src.api.audit`` line. Every observation runs inside a
    guarded ``finally`` — auditing must never break or slow a response.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        captured = {"status": 0}

        async def send_with_audit(message: Message) -> None:
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_with_audit)
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
                    captured["status"],
                    caller_kind,
                    caller_user_id or "-",
                    end_user_id or "-",
                    duration_ms,
                )
            except Exception:
                # Auditing must never break or slow a response.
                pass


class UsageMeterMiddleware:
    """Pure-ASGI per-request usage metering.

    Captures the response status, reads the identity the auth dependency
    stashed on ``request.state`` (mirrored into ``scope["state"]``), and
    records one increment against :data:`src.api.usage.usage_meter`. The
    meter guards the disabled fast path; the call here is additionally
    wrapped in a guarded ``finally`` — metering must never break or slow a
    response.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        captured = {"status": 0}

        async def send_with_meter(message: Message) -> None:
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_with_meter)
        finally:
            try:
                identity = (scope.get("state") or {}).get("identity")
                usage_meter.record(identity, captured["status"])
            except Exception:
                # Metering must never break or slow a response.
                pass
