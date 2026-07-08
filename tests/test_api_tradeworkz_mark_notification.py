"""POST /internal/mark-notification must issue a type-unambiguous UPDATE.

Regression for an asyncpg AmbiguousParameterError. The mark query used ``$1``
in two contexts — ``SET status = $1`` (deduces ``character varying`` from the
column) and ``CASE WHEN $1 = 'sent'`` (deduces ``text`` from the untyped
literal). Postgres reports the two as inconsistent and refuses to prepare the
statement, so *every* call 500'd. Because the delivery worker calls this
endpoint to flip a row out of ``queued`` after sending, the failure meant no
row was ever marked sent — the worker re-sent the same email every cycle.

The fix pins both uses to ``$1::text`` so the parameter has a single type.
This test is hermetic (fake asyncpg connection); the type resolution itself
was verified against a real Postgres. Here we lock in the SQL shape (both
uses cast, no bare ``$1 = 'sent'``) and the endpoint contract so the
ambiguous form can't silently return.
"""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

_STATIC_KEY = "static-integration-key"


class _FakeConn:
    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        return "UPDATE 1"


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _FakeAcquire(self._conn)


def _build_app(monkeypatch: pytest.MonkeyPatch):
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS", "END_USER_TOKEN_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("API_KEY", _STATIC_KEY)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)

    from src.api import main as main_mod
    from src.api import security

    monkeypatch.setattr(security, "_PUBLIC_PATHS", set())
    return main_mod


def _headers() -> dict:
    return {"X-API-Key": _STATIC_KEY}


def _mark(main_mod, conn, body):
    with TestClient(main_mod.app) as client:
        main_mod.db_manager.pool = _FakePool(conn)
        return client.post(
            "/api/tradeworkz/internal/mark-notification",
            headers=_headers(),
            json=body,
        )


def test_mark_notification_sql_is_type_unambiguous(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn()
    r = _mark(main_mod, conn, {"id": 42, "status": "sent"})
    assert r.status_code == 200, r.text
    assert r.json()["new_status"] == "sent"

    assert len(conn.calls) == 1, "expected exactly one UPDATE"
    sql, args = conn.calls[0]
    # Both $1 uses must be cast to text so Postgres deduces a single type.
    assert "SET status = $1::text" in sql
    assert "$1::text = 'sent'" in sql
    # The ambiguous form must not come back.
    assert "CASE WHEN $1 = 'sent'" not in sql
    # Params are (status, error, id); error omitted -> None.
    assert args == ("sent", None, 42)


def test_mark_notification_failed_passes_error(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn()
    r = _mark(main_mod, conn, {"id": 7, "status": "failed", "error": "resend 5xx"})
    assert r.status_code == 200, r.text
    _sql, args = conn.calls[0]
    assert args == ("failed", "resend 5xx", 7)


def test_mark_notification_rejects_bad_status(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn()
    r = _mark(main_mod, conn, {"id": 1, "status": "bogus"})
    assert r.status_code == 400, r.text
    assert conn.calls == []


def test_mark_notification_rejects_non_int_id(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn()
    r = _mark(main_mod, conn, {"id": "42", "status": "sent"})
    assert r.status_code == 400, r.text
    assert conn.calls == []
