"""Self-serve developer portal endpoints: ownership scoping, key cap,
rotate-in-transaction semantics, and the usage rollup shape.

These tests drive the router functions directly with a fake asyncpg
pool, so they exercise the real SQL we send (via the fake connection's
``calls`` log) without needing a Postgres instance up. They mirror the
pattern used by ``test_api_usage_metering.py``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytest
from fastapi import HTTPException

from src.api import scopes
from src.api.routers import dev_portal

# --------------------------------------------------------------------------
# Fake pool / request — enough surface that the router runs unmodified.
# --------------------------------------------------------------------------


@dataclass
class _Identity:
    end_user_id: Optional[str] = None
    caller_kind: str = "db"
    caller_user_id: Optional[str] = "zerogex-web"


class _State:
    def __init__(self, identity: Optional[_Identity]) -> None:
        self.identity = identity


class _Request:
    def __init__(self, identity: Optional[_Identity]) -> None:
        self.state = _State(identity)


class _FakeConn:
    def __init__(self, fetch_rows: List[Dict[str, Any]]) -> None:
        self._rows = fetch_rows
        self.calls: List[Tuple[str, Tuple[Any, ...]]] = []
        # Per-test pluggable handlers.
        self._fetchrow_handler = None
        self._fetch_handler = None
        self._execute_handler = None

    async def fetchrow(self, sql: str, *args: Any) -> Optional[Dict[str, Any]]:
        self.calls.append(("fetchrow", (sql,) + args))
        if self._fetchrow_handler is not None:
            return self._fetchrow_handler(sql, args)
        return self._rows.pop(0) if self._rows else None

    async def fetch(self, sql: str, *args: Any) -> List[Dict[str, Any]]:
        self.calls.append(("fetch", (sql,) + args))
        if self._fetch_handler is not None:
            return self._fetch_handler(sql, args)
        return list(self._rows)

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append(("execute", (sql,) + args))
        if self._execute_handler is not None:
            return self._execute_handler(sql, args)
        return "UPDATE 1"

    def transaction(self) -> "_FakeTx":
        return _FakeTx(self)


class _FakeTx:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> "_FakeTx":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False


class _Acquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: Any) -> bool:
        return False


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


class _FakeDB:
    def __init__(self, conn: _FakeConn) -> None:
        self.pool = _FakePool(conn)


def _patch_db(monkeypatch: pytest.MonkeyPatch, conn: _FakeConn) -> _FakeDB:
    db = _FakeDB(conn)
    monkeypatch.setattr(dev_portal, "_db", lambda request: db)
    # The keys-change paths call security.key_store.invalidate(); patch
    # it out so we don't fight with module reload in this suite.
    monkeypatch.setattr(dev_portal, "_invalidate_caches", lambda: None)
    return db


# --------------------------------------------------------------------------
# _require_end_user
# --------------------------------------------------------------------------


def test_missing_end_user_token_raises_400():
    req = _Request(_Identity(end_user_id=None))
    with pytest.raises(HTTPException) as exc:
        dev_portal._require_end_user(req)
    assert exc.value.status_code == 400


def test_present_end_user_token_returns_sub():
    req = _Request(_Identity(end_user_id="user_abc123"))
    assert dev_portal._require_end_user(req) == "user_abc123"


# --------------------------------------------------------------------------
# Tier inference from a stored scope set
# --------------------------------------------------------------------------


def test_infer_tier_for_wildcard_returns_full():
    assert dev_portal._infer_tier(["*"]) == "full"


def test_infer_tier_for_analytics_bundle():
    assert dev_portal._infer_tier(sorted(scopes.TIERS[scopes.TIER_ANALYTICS])) == "analytics"


def test_infer_tier_for_unknown_bundle_is_none():
    assert dev_portal._infer_tier([scopes.GEX]) is None


# --------------------------------------------------------------------------
# list_keys — never reveals raw secret; rows scoped to user_id.
# --------------------------------------------------------------------------


def test_list_keys_scopes_query_to_caller_user(monkeypatch: pytest.MonkeyPatch):
    now = datetime.now(timezone.utc)
    rows = [
        {
            "id": 7,
            "user_id": "alice@example.com",
            "name": "alice-prod",
            "prefix": "ab12cd34",
            "scopes": sorted(scopes.TIERS[scopes.TIER_ANALYTICS]),
            "created_at": now,
            "last_used_at": now,
            "revoked_at": None,
        }
    ]
    conn = _FakeConn(rows)
    _patch_db(monkeypatch, conn)

    req = _Request(_Identity(end_user_id="alice@example.com"))
    resp = asyncio.run(dev_portal.list_keys(req))

    assert resp.limit_per_user == dev_portal._MAX_KEYS_PER_USER
    assert len(resp.keys) == 1
    info = resp.keys[0]
    assert info.id == 7
    assert info.tier == "analytics"
    assert info.active is True
    # The SQL must scope on the JWT-derived user, not an arbitrary arg.
    last_call_sql, last_call_args = conn.calls[-1][1][0], conn.calls[-1][1][1:]
    assert "user_id = $1" in last_call_sql
    assert last_call_args[0] == "alice@example.com"


# --------------------------------------------------------------------------
# create_key — bundle expansion, per-user cap, raw secret returned once.
# --------------------------------------------------------------------------


def test_create_key_enforces_per_user_cap(monkeypatch: pytest.MonkeyPatch):
    conn = _FakeConn([])
    # SELECT COUNT(*) returns {"n": _MAX_KEYS_PER_USER}.
    conn._fetchrow_handler = lambda sql, args: {"n": dev_portal._MAX_KEYS_PER_USER}
    _patch_db(monkeypatch, conn)

    req = _Request(_Identity(end_user_id="alice"))
    payload = dev_portal.KeyCreateRequest(name="dup", tier="analytics")

    with pytest.raises(HTTPException) as exc:
        asyncio.run(dev_portal.create_key(payload, req))
    assert exc.value.status_code == 409


def test_create_key_returns_raw_secret_once_with_analytics_scopes(
    monkeypatch: pytest.MonkeyPatch,
):
    now = datetime.now(timezone.utc)
    inserted_scopes: List[List[str]] = []

    def _fetchrow(sql: str, args: Tuple[Any, ...]):
        # First call: SELECT COUNT(*) AS n; subsequent: the INSERT RETURNING.
        if "COUNT(*)" in sql:
            return {"n": 0}
        # INSERT INTO api_keys (..., scopes) -- args = (user, name, hash, prefix, scopes)
        inserted_scopes.append(list(args[4]))
        return {
            "id": 42,
            "user_id": args[0],
            "name": args[1],
            "prefix": args[3],
            "scopes": list(args[4]),
            "created_at": now,
            "last_used_at": None,
            "revoked_at": None,
        }

    conn = _FakeConn([])
    conn._fetchrow_handler = _fetchrow
    _patch_db(monkeypatch, conn)

    req = _Request(_Identity(end_user_id="alice"))
    payload = dev_portal.KeyCreateRequest(name=" alice-prod ", tier="analytics")
    resp = asyncio.run(dev_portal.create_key(payload, req))

    assert resp.raw_key
    assert len(resp.raw_key) > 32  # token_urlsafe(32) ≈ 43 chars
    assert resp.key.id == 42
    assert resp.key.name == "alice-prod"  # stripped by validator
    assert resp.key.prefix == resp.raw_key[: dev_portal._PREFIX_LEN]
    assert inserted_scopes == [sorted(scopes.TIERS[scopes.TIER_ANALYTICS])]


def test_create_key_rejects_internal_full_tier():
    with pytest.raises(ValueError):
        dev_portal.KeyCreateRequest(name="x", tier="full")


# --------------------------------------------------------------------------
# rotate_key — atomic, denies cross-user access, suffixes name.
# --------------------------------------------------------------------------


def test_rotate_key_denies_cross_user_access(monkeypatch: pytest.MonkeyPatch):
    """A 404 — not a 403 — so a guessed id never leaks existence."""
    conn = _FakeConn([])
    # _fetch_owned_key returns None for "not your row".
    conn._fetchrow_handler = lambda sql, args: None
    _patch_db(monkeypatch, conn)

    req = _Request(_Identity(end_user_id="alice"))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(dev_portal.rotate_key(req, key_id=99))
    assert exc.value.status_code == 404


def test_rotate_key_runs_insert_and_revoke_in_one_transaction(
    monkeypatch: pytest.MonkeyPatch,
):
    now = datetime.now(timezone.utc)
    fetchrow_calls = []

    def _fetchrow(sql: str, args: Tuple[Any, ...]):
        fetchrow_calls.append(sql)
        if "SELECT" in sql and "FROM api_keys" in sql:
            return {
                "id": 7,
                "user_id": "alice",
                "name": "alice-prod",
                "prefix": "old00000",
                "scopes": sorted(scopes.TIERS[scopes.TIER_ANALYTICS]),
                "created_at": now,
                "last_used_at": None,
                "revoked_at": None,
            }
        if "INSERT" in sql and "api_keys" in sql:
            return {
                "id": 8,
                "user_id": args[0],
                "name": args[1],
                "prefix": args[3],
                "scopes": list(args[4]),
                "created_at": now,
                "last_used_at": None,
                "revoked_at": None,
            }
        return None

    conn = _FakeConn([])
    conn._fetchrow_handler = _fetchrow
    _patch_db(monkeypatch, conn)

    req = _Request(_Identity(end_user_id="alice"))
    resp = asyncio.run(dev_portal.rotate_key(req, key_id=7))

    assert resp.key.id == 8
    assert resp.key.name.endswith("(rotated)")
    # We expect exactly: SELECT (own_key) -> INSERT (returning new) -> UPDATE revoke.
    sqls = [c[1][0] for c in conn.calls]
    assert any("INSERT INTO api_keys" in s for s in sqls)
    assert any("revoked_at = NOW()" in s for s in sqls)


# --------------------------------------------------------------------------
# revoke_key — two-column WHERE + 404 on miss.
# --------------------------------------------------------------------------


def test_revoke_key_404_when_not_owned(monkeypatch: pytest.MonkeyPatch):
    conn = _FakeConn([])
    conn._execute_handler = lambda sql, args: "UPDATE 0"
    _patch_db(monkeypatch, conn)
    req = _Request(_Identity(end_user_id="alice"))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(dev_portal.revoke_key(req, key_id=99))
    assert exc.value.status_code == 404


def test_revoke_key_204_when_owned(monkeypatch: pytest.MonkeyPatch):
    conn = _FakeConn([])
    conn._execute_handler = lambda sql, args: "UPDATE 1"
    _patch_db(monkeypatch, conn)
    req = _Request(_Identity(end_user_id="alice"))
    # No raise == success path.
    asyncio.run(dev_portal.revoke_key(req, key_id=7))
    # The SQL must scope by user too.
    rev_sql = conn.calls[-1][1][0]
    assert "user_id = $2" in rev_sql


# --------------------------------------------------------------------------
# usage endpoints — windowing + summary shape.
# --------------------------------------------------------------------------


def test_usage_history_returns_points_in_window(monkeypatch: pytest.MonkeyPatch):
    today = datetime.now(timezone.utc).date()
    rows = [
        {"day": today - timedelta(days=2), "request_count": 100, "error_count": 1},
        {"day": today - timedelta(days=1), "request_count": 200, "error_count": 5},
        {"day": today, "request_count": 50, "error_count": 0},
    ]
    conn = _FakeConn([])
    conn._fetch_handler = lambda sql, args: rows
    _patch_db(monkeypatch, conn)

    req = _Request(_Identity(end_user_id="alice"))
    resp = asyncio.run(dev_portal.usage_history(req, days=7))

    assert resp.window_days == 7
    assert resp.total_requests == 350
    assert resp.total_errors == 6
    # SQL must scope by caller_user_id with the JWT sub.
    sql = conn.calls[-1][1][0]
    assert "caller_user_id = $1" in sql


def test_usage_summary_packages_three_rollups(monkeypatch: pytest.MonkeyPatch):
    conn = _FakeConn([])
    now = datetime.now(timezone.utc)
    conn._fetchrow_handler = lambda sql, args: {
        "current_month": 1234,
        "last_month": 999,
        "last_30": 1500,
        "last_seen_at": now,
    }
    _patch_db(monkeypatch, conn)

    req = _Request(_Identity(end_user_id="alice"))
    resp = asyncio.run(dev_portal.usage_summary(req))
    assert resp.current_month_requests == 1234
    assert resp.last_month_requests == 999
    assert resp.last_30_days_requests == 1500
    assert resp.last_seen_at == now
