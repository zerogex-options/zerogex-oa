"""Tests for per-end-user identity attribution.

Two layers are exercised:

* ``src.api.identity.verify_end_user_token`` — the pure-crypto token
  verifier (unit), including the spoofing/expiry/alg-confusion paths.
* The end-to-end request path — a valid caller key plus an optional
  ``X-End-User-Token`` resolves to a ``RequestIdentity`` on
  ``request.state``, surfaces via the ``current_identity`` dependency,
  and lands in the audit log line.

The token verifier never touches the DB, so these need no Postgres.
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
from typing import Optional
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient

_SECRET = "test-end-user-signing-secret"


def _b64(obj) -> str:
    raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _sign(signing_input: str, secret: str) -> str:
    sig = hmac.new(secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")


def _mint(
    sub: str = "web-user-42",
    *,
    secret: str = _SECRET,
    alg: str = "HS256",
    exp_delta: int = 300,
    iat_delta: int = 0,
    omit_exp: bool = False,
    include_iat: bool = True,
) -> str:
    now = int(time.time())
    header = {"alg": alg, "typ": "JWT"}
    payload: dict = {"sub": sub}
    if include_iat:
        payload["iat"] = now + iat_delta
    if not omit_exp:
        payload["exp"] = now + exp_delta
    signing_input = f"{_b64(header)}.{_b64(payload)}"
    return f"{signing_input}.{_sign(signing_input, secret)}"


def _reload_identity(monkeypatch: pytest.MonkeyPatch, *, secret: Optional[str]):
    """Reimport src.api.identity with END_USER_TOKEN_SECRET in place.

    The secret is read at module import time, so the module subtree has
    to be flushed and re-imported per configuration.
    """
    for name in (
        "END_USER_TOKEN_SECRET",
        "END_USER_TOKEN_LEEWAY_SECONDS",
        "END_USER_TOKEN_MAX_AGE_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
    if secret is not None:
        monkeypatch.setenv("END_USER_TOKEN_SECRET", secret)
    sys.modules.pop("src.api.identity", None)
    import src.api.identity as identity  # noqa: WPS433

    return importlib.reload(identity)


# --------------------------------------------------------------------------
# Token verifier (unit) — the security-critical surface
# --------------------------------------------------------------------------


def test_valid_token_resolves_subject(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(_mint("web-user-42")) == "web-user-42"


def test_disabled_when_no_secret(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=None)
    assert identity.is_enabled() is False
    # A perfectly well-formed token must still be ignored when the API
    # has no secret configured — attribution is simply off.
    assert identity.verify_end_user_token(_mint("web-user-42")) is None


def test_wrong_signature_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    forged = _mint("attacker", secret="not-the-real-secret")
    assert identity.verify_end_user_token(forged) is None


def test_expired_token_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(_mint("u", exp_delta=-3600)) is None


def test_future_iat_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(_mint("u", iat_delta=3600)) is None


def test_alg_none_rejected(monkeypatch: pytest.MonkeyPatch):
    """The classic JWT downgrade: alg=none with an empty signature."""
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    header = _b64({"alg": "none", "typ": "JWT"})
    payload = _b64({"sub": "admin", "exp": int(time.time()) + 300})
    assert identity.verify_end_user_token(f"{header}.{payload}.") is None


def test_wrong_alg_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(_mint("u", alg="HS512")) is None


def test_missing_exp_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(_mint("u", omit_exp=True)) is None


def test_lifetime_ceiling_enforced(monkeypatch: pytest.MonkeyPatch):
    """Even a valid signature is refused once honored lifetime is blown,
    regardless of a generous exp the website might have chosen."""
    monkeypatch.setenv("END_USER_TOKEN_MAX_AGE_SECONDS", "900")
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    # Issued 2h ago but exp still in the future → past the 15-min ceiling.
    token = _mint("u", iat_delta=-7200, exp_delta=7200)
    assert identity.verify_end_user_token(token) is None


@pytest.mark.parametrize("bad", ["", "garbage", "a.b", "a.b.c.d", "..", "x.y.z"])
def test_malformed_tokens_rejected(monkeypatch: pytest.MonkeyPatch, bad: str):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(bad) is None


def test_oversized_subject_rejected(monkeypatch: pytest.MonkeyPatch):
    identity = _reload_identity(monkeypatch, secret=_SECRET)
    assert identity.verify_end_user_token(_mint("x" * 257)) is None


# --------------------------------------------------------------------------
# Rate-limit key precedence (unit)
# --------------------------------------------------------------------------


def test_rate_limit_key_precedence(monkeypatch: pytest.MonkeyPatch):
    _reload_identity(monkeypatch, secret=_SECRET)
    sys.modules.pop("src.api.ratelimit", None)
    import src.api.ratelimit as rl  # noqa: WPS433

    rl = importlib.reload(rl)
    from src.api.identity import RequestIdentity

    class _Req:
        client = type("C", (), {"host": "9.9.9.9"})()

    req = _Req()
    eu = RequestIdentity(caller_kind="db", caller_user_id="zerogex-web", end_user_id="u-1")
    cu = RequestIdentity(caller_kind="db", caller_user_id="zerogex-web")
    anon = RequestIdentity(caller_kind="anonymous")

    assert rl.rate_limit_key(eu, req) == "eu:u-1"
    assert rl.rate_limit_key(cu, req) == "cu:zerogex-web"
    assert rl.rate_limit_key(anon, req) == "ip:9.9.9.9"


# --------------------------------------------------------------------------
# End-to-end: identity exposure + audit line
# --------------------------------------------------------------------------


def _build_app(monkeypatch: pytest.MonkeyPatch, *, secret: Optional[str]):
    """Reload the API with a static key (so the caller authenticates) and
    the given end-user signing secret, and mount a probe route that
    returns whatever ``current_identity`` resolved."""
    for name in (
        "API_KEY",
        "ENVIRONMENT",
        "CORS_ALLOW_ORIGINS",
        "END_USER_TOKEN_SECRET",
        "END_USER_RATE_LIMIT_ENABLED",
        "END_USER_RATE_LIMIT_ENFORCE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("API_KEY", "caller-key")
    monkeypatch.setenv("ENVIRONMENT", "development")
    if secret is not None:
        monkeypatch.setenv("END_USER_TOKEN_SECRET", secret)

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod  # noqa: WPS433

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)

    from src.api import security  # noqa: WPS433
    from src.api.identity import RequestIdentity, current_identity
    from src.api.main import app  # noqa: WPS433

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


def test_caller_only_when_no_token(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, secret=_SECRET)
    with TestClient(app) as client:
        r = client.get("/api/_whoami", headers={"X-API-Key": "caller-key"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["caller_kind"] == "static"
    assert body["end_user_id"] is None
    assert body["subject"] == "caller:static"


def test_valid_token_attaches_end_user(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, secret=_SECRET)
    with TestClient(app) as client:
        r = client.get(
            "/api/_whoami",
            headers={"X-API-Key": "caller-key", "X-End-User-Token": _mint("web-user-99")},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["end_user_id"] == "web-user-99"
    assert body["end_user_source"] == "web-token"
    assert body["subject"] == "end_user:web-user-99"


def test_bad_token_does_not_reject_request(monkeypatch: pytest.MonkeyPatch):
    """A forged/expired end-user token must NOT 401 the request — it just
    means 'no end-user'. The caller is still authenticated."""
    app = _build_app(monkeypatch, secret=_SECRET)
    with TestClient(app) as client:
        r = client.get(
            "/api/_whoami",
            headers={
                "X-API-Key": "caller-key",
                "X-End-User-Token": _mint("attacker", secret="wrong"),
            },
        )
    assert r.status_code == 200, r.text
    assert r.json()["end_user_id"] is None


def test_token_ignored_when_attribution_disabled(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, secret=None)
    with TestClient(app) as client:
        r = client.get(
            "/api/_whoami",
            headers={"X-API-Key": "caller-key", "X-End-User-Token": _mint("web-user-1")},
        )
    assert r.status_code == 200, r.text
    assert r.json()["end_user_id"] is None


def test_audit_line_carries_identity(monkeypatch: pytest.MonkeyPatch, caplog):
    app = _build_app(monkeypatch, secret=_SECRET)
    with caplog.at_level(logging.INFO, logger="src.api.audit"):
        with TestClient(app) as client:
            r = client.get(
                "/api/_whoami",
                headers={"X-API-Key": "caller-key", "X-End-User-Token": _mint("audit-me")},
            )
    assert r.status_code == 200, r.text
    audit_lines = [rec.getMessage() for rec in caplog.records if rec.name == "src.api.audit"]
    assert any("end_user_id=audit-me" in m and "status=200" in m for m in audit_lines), audit_lines
