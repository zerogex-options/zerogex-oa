"""The members' Trade audit routes answer without the admin token, live rows only.

The /trading-signals "Trade audit" panel read ``/admin/trades`` and
``/admin/positions``. Those need ``X-Admin-Token``, which the website never
sends, so the panel showed "HTTP 403" to every member and to the admin
(reported by a Pro member on 2026-11-12). It now reads ``/trades`` and
``/positions``, which these tests pin:

  1. Both answer a plain API key, no admin token.
  2. Both filter out seeder rows (``origin = 'simulate'``) in every query they
     run, whatever ``origin`` the caller asks for.
  3. The admin routes are unchanged: still 403 without the token, and with it
     ``/admin/trades?origin=all`` and ``/admin/positions`` still read every row.

Driven through the real FastAPI app against a fake asyncpg pool that records
the SQL each request runs, as in ``test_api_tradeworkz_unfollow.py``.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

_STATIC_KEY = "static-integration-key"
_SECRET = "unit-test-secret"
_ADMIN_TOKEN = "unit-test-admin-token"

_LIVE_FILTER = "IS DISTINCT FROM 'simulate'"
_SIM_ONLY_FILTER = "= 'simulate'"

_TRADE_ROW = {
    "id": 7,
    "bot_id": "gamma_flip_defender",
    "bot_display_name": "Gamma Flip Defender",
    "underlying": "SPX",
    "opened_at": datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc),
    "closed_at": datetime(2026, 11, 11, 16, 0, tzinfo=timezone.utc),
    "direction": "bullish",
    "strategy_type": "long_call",
    "entry_price": 4.2,
    "exit_price": 5.1,
    "quantity": 2,
    "realized_pnl": 180.0,
    "pnl_percent": 21.4,
    "outcome": "win",
    "close_reason": "target",
    "entry_conviction": 0.71,
    "origin": "live",
    "legs": '[{"option_type": "call", "strike": 6850, "expiration": "2026-11-11"}]',
    "components_at_entry": None,
    "components_at_exit": None,
}

_SUMMARY_ROW = {
    "n_trades": 1,
    "sum_realized_pnl": 180.0,
    "n_wins": 1,
    "n_losses": 0,
    "n_scratches": 0,
}


class _FakeConn:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.calls: list[tuple[str, str, tuple]] = []

    async def fetch(self, sql, *args):
        self.calls.append(("fetch", sql, args))
        return list(self._rows)

    async def fetchrow(self, sql, *args):
        self.calls.append(("fetchrow", sql, args))
        return dict(_SUMMARY_ROW)


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
    monkeypatch.setenv("KEY_ADMIN_TOKEN", _ADMIN_TOKEN)

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


def _get(monkeypatch, path, *, rows=None, admin=False):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn(rows)
    headers = {"X-API-Key": _STATIC_KEY}
    if admin:
        headers["X-Admin-Token"] = _ADMIN_TOKEN
    with TestClient(main_mod.app) as client:
        main_mod.db_manager.pool = _FakePool(conn)
        r = client.get(path, headers=headers)
    return r, [sql for (_m, sql, _a) in conn.calls]


# --------------------------------------------------------------------------
# The members' routes: no admin token, live rows only.
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "query",
    ["", "?origin=all", "?origin=simulate", "?origin=live&bot_id=gamma_flip_defender"],
)
def test_trades_answers_a_plain_key_with_live_rows_only(monkeypatch, query):
    r, sqls = _get(monkeypatch, f"/api/tradeworkz/trades{query}", rows=[_TRADE_ROW])
    assert r.status_code == 200, r.text
    assert len(sqls) == 2, "expected the page query and the summary query"
    for sql in sqls:
        assert _LIVE_FILTER in sql, f"a query ran without the live filter:\n{sql}"
        assert "->> 'origin') = 'simulate'" not in sql
    body = r.json()
    assert body["filters"]["origin"] == "live"
    assert body["summary"]["n_trades"] == 1
    assert body["entries"][0]["origin"] == "live"
    assert body["entries"][0]["closed_at"] == "2026-11-11T16:00:00+00:00"


def test_trades_csv_is_the_live_audit(monkeypatch):
    r, sqls = _get(monkeypatch, "/api/tradeworkz/trades?format=csv", rows=[_TRADE_ROW])
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/csv")
    assert 'filename="tradeworkz-audit-live.csv"' in r.headers["content-disposition"]
    assert all(_LIVE_FILTER in sql for sql in sqls)
    assert "Gamma Flip Defender" in r.text


@pytest.mark.parametrize("query", ["", "?origin=all", "?origin=simulate"])
def test_positions_answers_a_plain_key_with_live_rows_only(monkeypatch, query):
    r, sqls = _get(monkeypatch, f"/api/tradeworkz/positions{query}")
    assert r.status_code == 200, r.text
    assert len(sqls) == 1
    assert _LIVE_FILTER in sqls[0]
    assert r.json() == {"entries": [], "summary": {"n_open": 0, "total_unrealized_pnl": 0}}


# --------------------------------------------------------------------------
# The admin routes are unchanged.
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "path", ["/api/tradeworkz/admin/trades", "/api/tradeworkz/admin/positions"]
)
def test_admin_routes_still_need_the_admin_token(monkeypatch, path):
    r, sqls = _get(monkeypatch, path)
    assert r.status_code == 403, r.text
    assert sqls == [], "the admin route reached the database without the token"


def test_admin_trades_still_reads_every_origin(monkeypatch):
    r, sqls = _get(monkeypatch, "/api/tradeworkz/admin/trades?origin=all", admin=True)
    assert r.status_code == 200, r.text
    assert sqls and not any(_LIVE_FILTER in sql for sql in sqls)
    assert r.json()["filters"]["origin"] == "all"


def test_admin_trades_can_still_read_seeder_rows(monkeypatch):
    r, sqls = _get(monkeypatch, "/api/tradeworkz/admin/trades?origin=simulate", admin=True)
    assert r.status_code == 200, r.text
    assert all(_SIM_ONLY_FILTER in sql for sql in sqls)


def test_admin_positions_still_reads_every_row(monkeypatch):
    r, sqls = _get(monkeypatch, "/api/tradeworkz/admin/positions", admin=True)
    assert r.status_code == 200, r.text
    assert len(sqls) == 1 and "WHERE" not in sqls[0]
