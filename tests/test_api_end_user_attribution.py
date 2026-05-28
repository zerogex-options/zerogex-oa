"""End-user attribution: token verification, identity, audit, rate-limit key.

Mirrors the module-reload pattern used by ``test_api_auth_and_cors.py`` /
``test_api_db_auth.py``: ``END_USER_TOKEN_SECRET`` (and the leeway/max-age
ceilings) are read at import time in ``src.api.identity``, so every test
that needs a different config flushes ``src.api.*`` from ``sys.modules``
and re-imports.

The JWT minter below is deliberately hand-rolled (base64url, no padding,
HS256) so the tests pin the exact wire contract the website must produce,
independent of any JWT library.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import importlib
import json
import logging
import sys
import time
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient

# --------------------------------------------------------------------------
# Local JWT minter (base64url-unpadded, HS256) — pins the wire contract.
# --------------------------------------------------------------------------


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _mint(
    secret: str,
    *,
    sub: str = "acct-12345",
    alg: str = "HS256",
    include_exp: bool = True,
    include_iat: bool = True,
    exp_delta: int = 300,
    iat_delta: int = 0,
    signature: str | None = None,
) -> str:
    """Produce a JWT exactly as the website would (or deliberately broken)."""
    header = {"alg": alg, "typ": "JWT"}
    payload: dict = {"sub": sub}
    now = int(time.time())
    if include_iat:
        payload["iat"] = now + iat_delta
    if include_exp:
        payload["exp"] = now + exp_delta
    h_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode())
    p_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")
    if signature is not None:
        s_b64 = signature
    else:
        s_b64 = _b64url(hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest())
    return f"{h_b64}.{p_b64}.{s_b64}"


# --------------------------------------------------------------------------
# Module reload helpers
# --------------------------------------------------------------------------

_EU_ENV = (
    "END_USER_TOKEN_SECRET",
    "END_USER_TOKEN_LEEWAY_SECONDS",
    "END_USER_TOKEN_MAX_AGE_SECONDS",
)


def _reload_identity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    secret: str | None = "unit-test-secret",
    max_age: int | None = None,
    leeway: int | None = None,
):
    for name in _EU_ENV:
        monkeypatch.delenv(name, raising=False)
    if secret is not None:
        monkeypatch.setenv("END_USER_TOKEN_SECRET", secret)
    if max_age is not None:
        monkeypatch.setenv("END_USER_TOKEN_MAX_AGE_SECONDS", str(max_age))
    if leeway is not None:
        monkeypatch.setenv("END_USER_TOKEN_LEEWAY_SECONDS", str(leeway))

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    return importlib.import_module("src.api.identity")


# --------------------------------------------------------------------------
# verify_end_user_token — unit
# --------------------------------------------------------------------------

_SECRET = "unit-test-secret"


def test_valid_token_returns_sub(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.is_enabled() is True
    token = _mint(_SECRET, sub="acct-777")
    assert identity.verify_end_user_token(token) == "acct-777"


def test_no_secret_disables_and_ignores_token(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=None)
    assert identity.is_enabled() is False
    # A perfectly valid-looking token is still ignored when disabled.
    assert identity.verify_end_user_token(_mint("any-secret")) is None


def test_wrong_signature_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    forged = _mint("a-different-secret", sub="attacker")
    assert identity.verify_end_user_token(forged) is None


def test_expired_token_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    # exp well past the 60s leeway.
    token = _mint(_SECRET, exp_delta=-3600, iat_delta=-3700)
    assert identity.verify_end_user_token(token) is None


def test_future_iat_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    token = _mint(_SECRET, iat_delta=3600)  # issued an hour in the future
    assert identity.verify_end_user_token(token) is None


def test_alg_none_with_empty_sig_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    token = _mint(_SECRET, alg="none", signature="")
    assert identity.verify_end_user_token(token) is None


def test_alg_hs512_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    token = _mint(_SECRET, alg="HS512")
    assert identity.verify_end_user_token(token) is None


def test_missing_exp_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    token = _mint(_SECRET, include_exp=False)
    assert identity.verify_end_user_token(token) is None


def test_lifetime_ceiling_exceeded_rejected(monkeypatch: pytest.MonkeyPatch):
    """Old iat + far-future exp: honored lifetime is capped at max-age."""
    identity = _reload_identity(monkeypatch, secret=_SECRET, max_age=900)
    token = _mint(_SECRET, iat_delta=-5000, exp_delta=86400)
    assert identity.verify_end_user_token(token) is None


@pytest.mark.parametrize(
    "bad",
    ["", "garbage", "a.b", "a.b.c.d", "..", "x.y.z"],
)
def test_malformed_tokens_rejected(monkeypatch: pytest.MonkeyPatch, bad: str):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(bad) is None


def test_oversized_sub_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    token = _mint(_SECRET, sub="x" * 257)
    assert identity.verify_end_user_token(token) is None


def test_sub_at_max_len_accepted(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    token = _mint(_SECRET, sub="y" * 256)
    assert identity.verify_end_user_token(token) == "y" * 256


# --------------------------------------------------------------------------
# rate_limit_key precedence: end-user > caller > IP
# --------------------------------------------------------------------------


class _FakeClient:
    def __init__(self, host: str) -> None:
        self.host = host


class _FakeRequest:
    def __init__(self, host: str | None = "203.0.113.7") -> None:
        self.client = _FakeClient(host) if host is not None else None


class _FakeRequestWithHeaders(_FakeRequest):
    def __init__(self, host: str | None = "203.0.113.7", headers: dict | None = None) -> None:
        super().__init__(host=host)
        self.headers = headers or {}


def _reload_ratelimit(monkeypatch: pytest.MonkeyPatch, *, trusted_proxies: str | None = None):
    """Reimport ratelimit so module-level _TRUSTED_PROXIES re-reads env."""
    monkeypatch.delenv("RATE_LIMIT_TRUSTED_PROXIES", raising=False)
    if trusted_proxies is not None:
        monkeypatch.setenv("RATE_LIMIT_TRUSTED_PROXIES", trusted_proxies)
    sys.modules.pop("src.api.ratelimit", None)
    import importlib

    import src.api.ratelimit as rl

    return importlib.reload(rl)


def test_rate_limit_key_precedence(monkeypatch: pytest.MonkeyPatch):
    _reload_identity(monkeypatch, secret=_SECRET)
    from src.api.identity import RequestIdentity
    from src.api.ratelimit import rate_limit_key

    req = _FakeRequest()

    eu = RequestIdentity(caller_kind="db", caller_user_id="zerogex-web", end_user_id="acct-9")
    assert rate_limit_key(eu, req) == "eu:acct-9"

    cu = RequestIdentity(caller_kind="db", caller_user_id="zerogex-web")
    assert rate_limit_key(cu, req) == "cu:zerogex-web"

    anon = RequestIdentity(caller_kind="anonymous")
    assert rate_limit_key(anon, req) == "ip:203.0.113.7"

    assert rate_limit_key(anon, _FakeRequest(host=None)) == "ip:unknown"


def test_rate_limit_ignores_xff_from_untrusted_peer(monkeypatch: pytest.MonkeyPatch):
    """With no trusted-proxy config, X-Forwarded-For is attacker-controlled
    and must be ignored — the bucket keys off the direct peer."""
    rl = _reload_ratelimit(monkeypatch, trusted_proxies=None)
    from src.api.identity import RequestIdentity

    anon = RequestIdentity(caller_kind="anonymous")
    req = _FakeRequestWithHeaders(host="203.0.113.7", headers={"X-Forwarded-For": "1.2.3.4"})
    # Spoofed XFF ignored → keyed on the real peer.
    assert rl.rate_limit_key(anon, req) == "ip:203.0.113.7"


def test_rate_limit_uses_xff_when_peer_is_trusted_proxy(monkeypatch: pytest.MonkeyPatch):
    """When the direct peer is a configured trusted proxy, the bucket keys
    off the real client from X-Forwarded-For, not the shared proxy IP."""
    rl = _reload_ratelimit(monkeypatch, trusted_proxies="10.0.0.1")
    from src.api.identity import RequestIdentity

    anon = RequestIdentity(caller_kind="anonymous")
    # nginx (10.0.0.1) forwards for real client 198.51.100.23.
    req = _FakeRequestWithHeaders(host="10.0.0.1", headers={"X-Forwarded-For": "198.51.100.23"})
    assert rl.rate_limit_key(anon, req) == "ip:198.51.100.23"


def test_rate_limit_walks_past_chained_trusted_proxies(monkeypatch: pytest.MonkeyPatch):
    """Multiple trusted hops: walk right-to-left to the first untrusted IP."""
    rl = _reload_ratelimit(monkeypatch, trusted_proxies="10.0.0.1,10.0.0.2")
    from src.api.identity import RequestIdentity

    anon = RequestIdentity(caller_kind="anonymous")
    # client -> edge(10.0.0.2) -> app-proxy(10.0.0.1) -> us.
    req = _FakeRequestWithHeaders(
        host="10.0.0.1",
        headers={"X-Forwarded-For": "198.51.100.23, 10.0.0.2"},
    )
    assert rl.rate_limit_key(anon, req) == "ip:198.51.100.23"


def test_rate_limit_falls_back_to_peer_when_chain_all_trusted(monkeypatch: pytest.MonkeyPatch):
    rl = _reload_ratelimit(monkeypatch, trusted_proxies="10.0.0.1,10.0.0.2")
    from src.api.identity import RequestIdentity

    anon = RequestIdentity(caller_kind="anonymous")
    req = _FakeRequestWithHeaders(host="10.0.0.1", headers={"X-Forwarded-For": "10.0.0.2"})
    # No untrusted IP in the chain → fall back to the direct peer.
    assert rl.rate_limit_key(anon, req) == "ip:10.0.0.1"


# --------------------------------------------------------------------------
# Integration: full request path through the auth dependency + middleware
# --------------------------------------------------------------------------

_STATIC_KEY = "static-integration-key"
_INT_SECRET = "integration-secret"


def _build_app(monkeypatch: pytest.MonkeyPatch, *, secret: str | None = _INT_SECRET):
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS", *_EU_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("API_KEY", _STATIC_KEY)
    monkeypatch.setenv("ENVIRONMENT", "development")
    if secret is not None:
        monkeypatch.setenv("END_USER_TOKEN_SECRET", secret)

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)

    from src.api.main import app  # noqa: E402
    from src.api import security  # noqa: E402
    from src.api.identity import RequestIdentity, current_identity  # noqa: E402

    # Force every request through api_key_auth (see sibling auth tests).
    monkeypatch.setattr(security, "_PUBLIC_PATHS", set())

    async def _whoami(identity: RequestIdentity = Depends(current_identity)):
        return {
            "caller_kind": identity.caller_kind,
            "caller_user_id": identity.caller_user_id,
            "end_user_id": identity.end_user_id,
            "end_user_source": identity.end_user_source,
            "subject": identity.subject,
        }

    app.add_api_route("/api/_whoami", _whoami, methods=["GET"])
    return app


def test_no_token_authenticates_as_caller_only(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.get("/api/_whoami", headers={"X-API-Key": _STATIC_KEY})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["caller_kind"] == "static"
    assert body["end_user_id"] is None
    assert body["subject"] == "caller:static"


def test_valid_token_attaches_end_user(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch)
    token = _mint(_INT_SECRET, sub="acct-abc")
    with TestClient(app) as client:
        r = client.get(
            "/api/_whoami",
            headers={"X-API-Key": _STATIC_KEY, "X-End-User-Token": token},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["caller_kind"] == "static"
    assert body["end_user_id"] == "acct-abc"
    assert body["end_user_source"] == "web-token"
    assert body["subject"] == "end_user:acct-abc"


def test_forged_token_is_ignored_not_rejected(monkeypatch: pytest.MonkeyPatch):
    """Additive contract: a bad token must never turn a 200 into a 4xx."""
    app = _build_app(monkeypatch)
    forged = _mint("not-the-real-secret", sub="attacker")
    with TestClient(app) as client:
        r = client.get(
            "/api/_whoami",
            headers={"X-API-Key": _STATIC_KEY, "X-End-User-Token": forged},
        )
    assert r.status_code == 200, r.text
    assert r.json()["end_user_id"] is None


def test_secret_unset_ignores_token(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, secret=None)
    token = _mint("whatever", sub="acct-xyz")
    with TestClient(app) as client:
        r = client.get(
            "/api/_whoami",
            headers={"X-API-Key": _STATIC_KEY, "X-End-User-Token": token},
        )
    assert r.status_code == 200, r.text
    assert r.json()["end_user_id"] is None


def test_audit_line_emitted_with_end_user_and_status(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    app = _build_app(monkeypatch)
    token = _mint(_INT_SECRET, sub="acct-audit")
    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        with TestClient(app) as client:
            r = client.get(
                "/api/_whoami",
                headers={"X-API-Key": _STATIC_KEY, "X-End-User-Token": token},
            )
    assert r.status_code == 200, r.text
    audit_lines = [rec.getMessage() for rec in caplog.records if rec.name == "src.api.audit"]
    assert any("acct-audit" in line and "status=200" in line for line in audit_lines), audit_lines
