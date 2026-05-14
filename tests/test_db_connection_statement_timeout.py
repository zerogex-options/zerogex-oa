"""Regression: psycopg2 pool must apply a server-side statement_timeout.

Locks the wiring added after the 2026-05-13 incident where an analytics
DISTINCT ON query ran for 23+ minutes server-side because the psycopg2
pool had no statement_timeout and no client-side budget.  Without this
test, a future refactor of _initialize_connection_pool could silently
drop the ``options=-c statement_timeout=...`` parameter and re-introduce
the unbounded-query failure mode.
"""

import importlib

import pytest


@pytest.fixture
def reload_connection_module(monkeypatch):
    """Reload the connection module fresh after env vars are patched."""

    def _reload():
        # The module caches a pool global; reloading is safer than poking.
        return importlib.reload(importlib.import_module("src.database.connection"))

    return _reload


def _capture_pool_kwargs(monkeypatch, module):
    """Patch SimpleConnectionPool to capture kwargs instead of connecting."""
    captured: dict = {}

    class _FakePool:
        def __init__(self, *args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs

        def getconn(self):
            class _FakeConn:
                def cursor(self):
                    class _FakeCursor:
                        def execute(self, _sql):
                            return None

                        def fetchone(self):
                            return ("PostgreSQL 17.0 (mock)",)

                        def close(self):
                            return None

                    return _FakeCursor()

            return _FakeConn()

        def putconn(self, _conn):
            return None

        def closeall(self):
            return None

    monkeypatch.setattr(module.pool, "SimpleConnectionPool", _FakePool)
    monkeypatch.setattr(module, "get_db_password", lambda: "fake-password", raising=True)
    return captured


def test_default_statement_timeout_applied(monkeypatch, reload_connection_module):
    """Default 90 s timeout is forwarded to libpq via the options token."""
    for env_key in (
        "DB_STATEMENT_TIMEOUT_MS",
        "DB_POOL_MIN",
        "DB_POOL_MAX",
        "DB_SSLMODE",
    ):
        monkeypatch.delenv(env_key, raising=False)

    module = reload_connection_module()
    captured = _capture_pool_kwargs(monkeypatch, module)
    module._initialize_connection_pool()

    assert captured["kwargs"].get("options") == "-c statement_timeout=90000"


def test_statement_timeout_env_override(monkeypatch, reload_connection_module):
    """DB_STATEMENT_TIMEOUT_MS overrides the default value."""
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "5000")
    monkeypatch.delenv("DB_POOL_MIN", raising=False)
    monkeypatch.delenv("DB_POOL_MAX", raising=False)

    module = reload_connection_module()
    captured = _capture_pool_kwargs(monkeypatch, module)
    module._initialize_connection_pool()

    assert captured["kwargs"].get("options") == "-c statement_timeout=5000"


def test_statement_timeout_disabled_when_zero(monkeypatch, reload_connection_module):
    """Setting DB_STATEMENT_TIMEOUT_MS=0 leaves the options parameter unset.

    Provides an escape hatch for operators running ad-hoc long migrations
    through the same pool, but the production default keeps the cap on.
    """
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "0")
    monkeypatch.delenv("DB_POOL_MIN", raising=False)
    monkeypatch.delenv("DB_POOL_MAX", raising=False)

    module = reload_connection_module()
    captured = _capture_pool_kwargs(monkeypatch, module)
    module._initialize_connection_pool()

    assert "options" not in captured["kwargs"]
