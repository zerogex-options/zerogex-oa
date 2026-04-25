"""ASGI middleware for the API.

RequestIdMiddleware
    Reads ``X-Request-Id`` from the incoming request (or generates a fresh
    UUID4 hex), stashes it in ``src.utils.logging.request_id_var`` so any
    log line emitted while the request is being handled carries the id,
    and echoes it back as ``X-Request-Id`` on the response. Pure-ASGI
    rather than ``BaseHTTPMiddleware`` so we don't break streaming
    responses or swallow exception context.
"""

from __future__ import annotations

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.utils.logging import new_request_id, request_id_var


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
