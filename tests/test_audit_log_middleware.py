"""Tests for src.api.middleware.AuditLogMiddleware.

The audit line is the only record that ties an API key to the request it
authenticated — nginx's access log is deliberately credential-free (see the
``zerogex_scrubbed`` format in ``deploy/steps/120.nginx_api``). So these
verify the fields an investigation actually depends on:

- Every field is emitted, including ``client_ip`` and the ``caller_key_id`` /
  ``caller_name`` that identify *which* of an owner's keys was used.
- Free-text values stay single whitespace-free tokens, so the line survives
  ``awk``-style parsing straight out of journalctl.
- Absent values render as ``-`` rather than blank, which would shift every
  following field for a positional reader.
- Auditing never breaks a response, however broken the identity object is.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from src.api.identity import RequestIdentity
from src.api.middleware import AuditLogMiddleware, _audit_token


def _build_app(identity=None) -> FastAPI:
    """Minimal app that stashes ``identity`` the way the auth dependency does.

    Avoids importing src.api.main, which pulls in the full DB surface.
    """
    app = FastAPI()
    app.add_middleware(AuditLogMiddleware)

    @app.get("/api/probe")
    async def probe(request: Request):
        if identity is not None:
            request.state.identity = identity
        return {"ok": True}

    @app.get("/api/boom")
    async def boom(request: Request):
        if identity is not None:
            request.state.identity = identity
        raise RuntimeError("handler exploded")

    return app


def _audit_line(caplog) -> str:
    lines = [r.getMessage() for r in caplog.records if r.name == "src.api.audit"]
    assert len(lines) == 1, f"expected exactly one audit line, got {lines}"
    return lines[0]


def _fields(line: str) -> dict:
    assert line.startswith("api_request "), line
    return dict(part.split("=", 1) for part in line.split()[1:])


def test_audit_line_carries_full_key_identity(caplog):
    identity = RequestIdentity(
        caller_kind="db",
        caller_user_id="alice@example.com",
        caller_key_id=7,
        caller_name="alice-laptop",
        caller_scopes=("gex:read",),
    )
    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        with TestClient(_build_app(identity)) as client:
            assert client.get("/api/probe").status_code == 200

    fields = _fields(_audit_line(caplog))
    assert fields["method"] == "GET"
    assert fields["path"] == "/api/probe"
    assert fields["status"] == "200"
    assert fields["caller_kind"] == "db"
    assert fields["caller_user_id"] == "alice@example.com"
    # The two fields that say WHICH key, not just whose account.
    assert fields["caller_key_id"] == "7"
    assert fields["caller_name"] == "alice-laptop"
    assert fields["end_user_id"] == "-"
    # TestClient's transport sets scope["client"]; in production this is the
    # real peer because uvicorn's ProxyHeadersMiddleware rewrites it from
    # X-Forwarded-For.
    assert fields["client_ip"] == "testclient"


def test_anonymous_request_renders_placeholders(caplog):
    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        with TestClient(_build_app()) as client:
            assert client.get("/api/probe").status_code == 200

    fields = _fields(_audit_line(caplog))
    assert fields["caller_kind"] == "anonymous"
    # Never blank: "caller_user_id= caller_key_id=..." would silently shift
    # every following field for anything parsing positionally.
    assert fields["caller_user_id"] == "-"
    assert fields["caller_key_id"] == "-"
    assert fields["caller_name"] == "-"


def test_key_name_with_spaces_stays_one_token(caplog):
    """``make api-keys-create NAME="alice laptop"`` must not break the line."""
    identity = RequestIdentity(
        caller_kind="db",
        caller_user_id="alice@example.com",
        caller_key_id=7,
        caller_name="alice laptop  spare",
    )
    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        with TestClient(_build_app(identity)) as client:
            client.get("/api/probe")

    line = _audit_line(caplog)
    # Every whitespace-separated chunk after the marker is a key=value pair.
    for part in line.split()[1:]:
        assert "=" in part, f"{part!r} is not a key=value token in {line!r}"
    assert _fields(line)["caller_name"] == "alice_laptop_spare"


def test_end_user_id_is_recorded_when_present(caplog):
    identity = RequestIdentity(
        caller_kind="db",
        caller_user_id="bff@zerogex",
        caller_key_id=1,
        caller_name="website",
        end_user_id="user_42",
        end_user_source="web-token",
    )
    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        with TestClient(_build_app(identity)) as client:
            client.get("/api/probe")

    assert _fields(_audit_line(caplog))["end_user_id"] == "user_42"


def test_audit_never_breaks_the_response(caplog):
    """A hostile identity object must not turn a 200 into a 500."""

    class Exploding:
        def __getattr__(self, name):
            raise RuntimeError(f"no attribute {name}")

    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        with TestClient(_build_app(Exploding())) as client:
            assert client.get("/api/probe").status_code == 200


def test_failed_request_is_still_audited(caplog):
    """An exception in the handler must not swallow the audit line.

    The status is recorded as ``0``, not ``500``. When the handler raises,
    the exception propagates through this middleware before any
    ``http.response.start`` is sent, so nothing is captured; the 500 the
    client eventually receives is synthesized by Starlette's
    ``ServerErrorMiddleware``, which sits *outside* this one. Longstanding
    behavior, asserted here so it is a documented property rather than a
    surprise to whoever next counts error rates out of the audit trail —
    ``status=0`` means "handler raised", and ordinary 4xx/5xx responses
    (including ``HTTPException``) still record their real status.
    """
    identity = RequestIdentity(caller_kind="db", caller_user_id="alice@example.com")
    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        client = TestClient(_build_app(identity), raise_server_exceptions=False)
        with client:
            assert client.get("/api/boom").status_code == 500

    fields = _fields(_audit_line(caplog))
    assert fields["status"] == "0"
    # The identity is what matters here: an erroring request must still be
    # attributable to the key that made it.
    assert fields["caller_user_id"] == "alice@example.com"


class TestAuditToken:
    def test_none_and_empty_become_dash(self):
        assert _audit_token(None) == "-"
        assert _audit_token("") == "-"
        assert _audit_token("   ") == "-"

    def test_integers_render_plainly(self):
        assert _audit_token(7) == "7"
        assert _audit_token(0) == "0"

    def test_internal_whitespace_collapses(self):
        assert _audit_token("alice laptop") == "alice_laptop"
        assert _audit_token("  padded  key \t name ") == "padded_key_name"

    def test_ordinary_values_pass_through(self):
        assert _audit_token("alice@example.com") == "alice@example.com"
        assert _audit_token("2601:286:c181::1") == "2601:286:c181::1"
