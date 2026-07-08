"""TradeWorkz unfollow / channel-off must stop queued email delivery.

Regression coverage for the "I unfollowed everything but the emails keep
coming" report. The email path is fire-and-forget: a trade event fans out a
``queued`` row into ``tw_notifications_log`` and the every-minute delivery
worker drains it later. Nothing used to re-check the follower's opt-in
between queue and send, so unfollowing (or turning a channel off) only
stopped *future* events — the backlog still shipped.

These tests pin the three guards that close that gap, driven through the
real FastAPI routing / identity / body-parsing stack against a fake asyncpg
pool that records the SQL each endpoint runs:

  1. DELETE /bots/{id}/follow cancels the caller's pending queued rows for
     that bot (not just the follower row).
  2. POST /bots/{id}/follow cancels pending queued rows for any channel the
     new preference set turns off.
  3. GET /internal/queued-notifications only hands the worker rows whose
     follower still opts into that channel — the authoritative delivery-time
     guard that also suppresses orphaned backlog.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
import time
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

_STATIC_KEY = "static-integration-key"
_SECRET = "unit-test-secret"


# --------------------------------------------------------------------------
# JWT minter (base64url-unpadded HS256) — mirrors the website wire contract.
# --------------------------------------------------------------------------
def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _mint(sub: str, secret: str = _SECRET) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    payload = {"sub": sub, "iat": now, "exp": now + 300}
    h = _b64url(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    s = _b64url(hmac.new(secret.encode(), f"{h}.{p}".encode("ascii"), hashlib.sha256).digest())
    return f"{h}.{p}.{s}"


# --------------------------------------------------------------------------
# Fake asyncpg pool / connection that records every statement it runs.
# --------------------------------------------------------------------------
class _FakeConn:
    def __init__(self, *, fetch_rows=None, fetchval_result=1):
        self._fetch_rows = fetch_rows if fetch_rows is not None else []
        self._fetchval_result = fetchval_result
        self.calls: list[tuple[str, str, tuple]] = []

    def _tag(self, sql: str) -> str:
        s = sql.strip().upper()
        if s.startswith("UPDATE"):
            return "UPDATE 2"
        if s.startswith("DELETE"):
            return "DELETE 1"
        if s.startswith("INSERT"):
            return "INSERT 0 1"
        return "OK"

    async def fetch(self, sql, *args):
        self.calls.append(("fetch", sql, args))
        return list(self._fetch_rows)

    async def fetchval(self, sql, *args):
        self.calls.append(("fetchval", sql, args))
        return self._fetchval_result

    async def fetchrow(self, sql, *args):
        self.calls.append(("fetchrow", sql, args))
        return None

    async def execute(self, sql, *args):
        self.calls.append(("execute", sql, args))
        return self._tag(sql)


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
    monkeypatch.setenv("END_USER_TOKEN_SECRET", _SECRET)

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

    # Force auth on every path so the request exercises identity resolution
    # (we hand it the static break-glass key), matching the sibling
    # attribution/auth tests.
    monkeypatch.setattr(security, "_PUBLIC_PATHS", set())
    return main_mod


def _inject_pool(main_mod, conn: _FakeConn) -> None:
    """After lifespan set the module-global db_manager, give it a fake pool."""
    assert main_mod.db_manager is not None
    main_mod.db_manager.pool = _FakePool(conn)


def _headers(token: str | None = None) -> dict:
    h = {"X-API-Key": _STATIC_KEY}
    if token:
        h["X-End-User-Token"] = token
    return h


def _updates_to_notifications_log(conn: _FakeConn) -> list[tuple[str, tuple]]:
    return [
        (sql, args)
        for (_m, sql, args) in conn.calls
        if _m == "execute"
        and sql.strip().upper().startswith("UPDATE")
        and "tw_notifications_log" in sql
    ]


# --------------------------------------------------------------------------
# unfollow: cancels the caller's pending queued rows for the bot.
# --------------------------------------------------------------------------
def test_unfollow_cancels_pending_queued_rows(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn()
    with TestClient(main_mod.app) as client:
        _inject_pool(main_mod, conn)
        r = client.delete(
            "/api/tradeworkz/bots/gamma_flip_defender/follow",
            headers=_headers(_mint("acct-1")),
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    # The command tag "UPDATE 2" is parsed into the affected-row count.
    assert body["cancelled_queued"] == 2

    # A DELETE of the follower row AND a cancel-UPDATE of queued rows ran,
    # both scoped to (end_user, bot_id).
    deletes = [
        c for c in conn.calls if c[0] == "execute" and c[1].strip().upper().startswith("DELETE")
    ]
    assert deletes, "follower row was not deleted"
    assert deletes[0][2] == ("acct-1", "gamma_flip_defender")

    cancels = _updates_to_notifications_log(conn)
    assert len(cancels) == 1, "queued rows were not cancelled on unfollow"
    sql, args = cancels[0]
    assert "status = 'cancelled'" in sql
    assert "status = 'queued'" in sql
    assert args == ("acct-1", "gamma_flip_defender")


def test_unfollow_requires_end_user(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn()
    with TestClient(main_mod.app) as client:
        _inject_pool(main_mod, conn)
        r = client.delete(
            "/api/tradeworkz/bots/gamma_flip_defender/follow",
            headers=_headers(),  # caller key only, no end-user token
        )
    assert r.status_code == 401, r.text
    # Nothing was touched without a resolved end-user.
    assert conn.calls == []


# --------------------------------------------------------------------------
# follow upsert: turning a channel off cancels its pending queued rows.
# --------------------------------------------------------------------------
def test_follow_upsert_with_email_off_cancels_email_backlog(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn(fetchval_result=1)  # bot exists
    with TestClient(main_mod.app) as client:
        _inject_pool(main_mod, conn)
        r = client.post(
            "/api/tradeworkz/bots/gamma_flip_defender/follow",
            headers=_headers(_mint("acct-1")),
            json={"channels": {"in_app": True}, "min_confidence": 0.0},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cancelled_queued"] == 2

    cancels = _updates_to_notifications_log(conn)
    assert len(cancels) == 1, "channel-off did not reconcile queued rows"
    sql, args = cancels[0]
    # The reconcile keys the per-row decision off the new channel set: a row
    # whose channel is not enabled in the JSON is cancelled.
    assert "->> channel" in sql
    user, bot_id, channels_json = args
    assert (user, bot_id) == ("acct-1", "gamma_flip_defender")
    assert json.loads(channels_json) == {"in_app": True}


# --------------------------------------------------------------------------
# delivery worker query: only rows the follower still opts into are handed out.
# --------------------------------------------------------------------------
def test_queued_notifications_filters_on_current_optin(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn(fetch_rows=[])
    with TestClient(main_mod.app) as client:
        _inject_pool(main_mod, conn)
        r = client.get(
            "/api/tradeworkz/internal/queued-notifications?channel=email&limit=10",
            headers=_headers(),  # scope dep is a no-op for the static key
        )
    assert r.status_code == 200, r.text
    fetches = [c for c in conn.calls if c[0] == "fetch"]
    assert fetches, "worker query did not run"
    sql = fetches[0][1]
    # The delivery-time opt-in guard must be present: a queued row is only
    # returned if the follower still exists with that channel enabled.
    assert "EXISTS" in sql
    assert "tw_bot_followers" in sql
    assert "->> n.channel" in sql
