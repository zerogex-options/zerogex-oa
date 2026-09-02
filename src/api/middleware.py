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
    with method, path, status, the client IP, the resolved caller/end-user
    identity (set on ``request.state.identity`` by the auth dependency),
    and the wall-clock duration. Pure-ASGI, same as above. Registered so that
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


def _audit_token(value: object) -> str:
    """Render one audit field as a single whitespace-free token.

    The audit line is space-separated ``key=value`` pairs precisely so it
    stays greppable/awk-able straight out of ``journalctl`` with no real
    parser. Two of the fields are free text — ``caller_name`` is an
    operator-chosen label (``make api-keys-create NAME=...``) and
    ``end_user_id`` is a ``sub`` from the BFF's token — so either could
    carry a space and silently shift every field after it for a positional
    reader. Collapsing internal whitespace to ``_`` keeps every field
    exactly one token. Empty and ``None`` both render as ``-`` for the same
    reason: a blank value would leave ``key=`` followed by a space.

    Lengths are already bounded upstream by the schema (``api_keys.name``
    is VARCHAR(128), ``user_id`` VARCHAR(128)) and by the signed token, so
    values are not truncated here — a truncated identifier is worse than a
    long one when the whole point of the line is attribution.
    """
    if value is None:
        return "-"
    text = str(value).strip()
    if not text:
        return "-"
    return "_".join(text.split())


class AuditLogMiddleware:
    """Pure-ASGI per-request audit logging.

    Captures the response status, reads the identity the auth dependency
    stashed on ``request.state`` (mirrored into ``scope["state"]``), and
    emits one ``src.api.audit`` line. Every observation runs inside a
    guarded ``finally`` — auditing must never break or slow a response.

    The line records ``client_ip`` alongside the key identity so "which key
    is this IP using" is a grep rather than a correlation exercise. It used
    to be the latter: nginx's access log has the IP but no credential (the
    ``zerogex_scrubbed`` format in ``deploy/steps/120.nginx_api`` logs no
    key and rewrites any ``?api_key=`` to REDACTED), while this line had the
    credential but no IP — so attributing a caller meant joining the two
    logs on (second, method, path, status), which is ambiguous whenever two
    clients hit the same path in the same second.

    ``client_ip`` is read from ``scope["client"]``, which is the real client
    address only because uvicorn's ProxyHeadersMiddleware rewrites it from
    ``X-Forwarded-For`` (``proxy_headers`` defaults on, trusting
    ``127.0.0.1``, and nginx sets the header and proxies from localhost —
    see ``deploy/steps/120.nginx_api``). Were the API ever fronted directly,
    or ``--no-proxy-headers`` passed, this would degrade to the proxy's own
    address rather than lie about a different client.

    ``caller_key_id`` and ``caller_name`` distinguish *which* of an owner's
    keys is in use, which is what a rotation or a revocation needs to
    target — ``caller_user_id`` alone only identifies the account.

    COST: the three added fields are ~61 bytes on a representative line, or
    about 40% more per request (154 -> 215 bytes), and this line is emitted
    once per request. The journal is capped at 300M shared across all four
    zerogex units and vacuumed nightly
    (``setup/systemd/zerogex-oa-journald.conf``), and API log volume has
    already had to be cut once to keep useful history — see the header of
    ``tests/test_api_log_volume.py``. At 50k requests/hour this costs ~3
    MB/h. If that retention ever matters more than self-describing lines,
    ``caller_name`` is the field to drop first: it is recoverable from
    ``caller_key_id`` via the ``api_keys`` table, which is exactly what
    ``src/tools/api_caller_report.py`` does.
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
                # scope["client"] is a (host, port) tuple, or None for a
                # transport that has no peer (ASGI test harnesses).
                client = scope.get("client")
                client_ip = client[0] if client else None
                _audit_logger.info(
                    "api_request method=%s path=%s status=%s client_ip=%s "
                    "caller_kind=%s caller_user_id=%s caller_key_id=%s "
                    "caller_name=%s end_user_id=%s duration_ms=%.1f",
                    scope.get("method", "-"),
                    # An ES/NQ request is routed to its backing index, but the
                    # audit trail must record what the caller actually asked
                    # for (FuturesProjectionMiddleware stashes it).
                    scope.get("zerogex_original_path") or scope.get("path", "-"),
                    captured["status"],
                    _audit_token(client_ip),
                    _audit_token(getattr(identity, "caller_kind", "anonymous")),
                    _audit_token(getattr(identity, "caller_user_id", None)),
                    _audit_token(getattr(identity, "caller_key_id", None)),
                    _audit_token(getattr(identity, "caller_name", None)),
                    _audit_token(getattr(identity, "end_user_id", None)),
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
