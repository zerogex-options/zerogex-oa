"""Redis-backed rate limiting + graceful in-memory fallback.

The ``ratelimit`` module imports ``REDIS_URL`` and the limiter env at
import time, so each test that needs a different mode flushes the module
from ``sys.modules`` and re-imports — the same pattern used by
``test_api_scopes`` and ``test_api_usage_metering``.

The Redis client is replaced with an in-process fake so the suite doesn't
require a running Redis (or even the ``redis`` package).
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from typing import Any, Dict, List

import pytest

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _reload_ratelimit(
    monkeypatch: pytest.MonkeyPatch,
    *,
    enabled: bool = False,
    enforce: bool = False,
    redis_url: str = "",
    limit: int = 5,
    window: int = 60,
):
    for var in [
        "END_USER_RATE_LIMIT_ENABLED",
        "END_USER_RATE_LIMIT_ENFORCE",
        "END_USER_RATE_LIMIT_REQUESTS",
        "END_USER_RATE_LIMIT_WINDOW_SECONDS",
        "REDIS_URL",
    ]:
        monkeypatch.delenv(var, raising=False)
    if enabled:
        monkeypatch.setenv("END_USER_RATE_LIMIT_ENABLED", "1")
    if enforce:
        monkeypatch.setenv("END_USER_RATE_LIMIT_ENFORCE", "1")
    monkeypatch.setenv("END_USER_RATE_LIMIT_REQUESTS", str(limit))
    monkeypatch.setenv("END_USER_RATE_LIMIT_WINDOW_SECONDS", str(window))
    if redis_url:
        monkeypatch.setenv("REDIS_URL", redis_url)
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    return importlib.import_module("src.api.ratelimit")


class _FakePipeline:
    def __init__(self, client: "_FakeRedis") -> None:
        self.client = client
        self.ops: List[tuple] = []

    def incr(self, key: str) -> "_FakePipeline":
        self.ops.append(("incr", key))
        return self

    def expire(self, key: str, ttl: int) -> "_FakePipeline":
        self.ops.append(("expire", key, ttl))
        return self

    async def execute(self) -> List[Any]:
        results: List[Any] = []
        for op in self.ops:
            if op[0] == "incr":
                self.client.store[op[1]] = self.client.store.get(op[1], 0) + 1
                results.append(self.client.store[op[1]])
            elif op[0] == "expire":
                results.append(True)
        return results


class _FakeRedis:
    """Just enough surface for ratelimit + cache; backing dict is shared
    so tests can simulate "two workers, one Redis"."""

    def __init__(self, store: Dict[str, Any], *, fail: bool = False) -> None:
        self.store = store
        self.fail = fail

    def pipeline(self, transaction: bool = True) -> _FakePipeline:
        if self.fail:
            raise RuntimeError("simulated redis outage")
        return _FakePipeline(self)


class _DummyIdentity:
    def __init__(self, end_user_id=None, caller_user_id=None):
        self.end_user_id = end_user_id
        self.caller_user_id = caller_user_id


class _DummyState:
    def __init__(self, identity):
        self.identity = identity


class _DummyClient:
    host = "127.0.0.1"


class _DummyRequest:
    def __init__(self, identity):
        self.state = _DummyState(identity)
        self.headers = {}
        self.client = _DummyClient()


# --------------------------------------------------------------------------
# Disabled and in-memory baseline (no Redis).
# --------------------------------------------------------------------------


def test_disabled_is_noop(monkeypatch: pytest.MonkeyPatch):
    rl = _reload_ratelimit(monkeypatch, enabled=False)
    req = _DummyRequest(_DummyIdentity(caller_user_id="alice"))
    # 10 calls but limit=5 — disabled, so no raise no matter what.
    for _ in range(10):
        asyncio.run(rl.rate_limit(req))


def test_in_memory_fallback_when_no_redis(monkeypatch: pytest.MonkeyPatch):
    """No REDIS_URL set: the limiter uses the in-process counter directly,
    so a single worker hits its own limit normally."""
    rl = _reload_ratelimit(monkeypatch, enabled=True, enforce=True, limit=2)
    req = _DummyRequest(_DummyIdentity(caller_user_id="alice"))
    asyncio.run(rl.rate_limit(req))
    asyncio.run(rl.rate_limit(req))
    # Third call exceeds the limit -> 429.
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        asyncio.run(rl.rate_limit(req))
    assert exc.value.status_code == 429


# --------------------------------------------------------------------------
# Redis-backed: one store shared across two "workers".
# --------------------------------------------------------------------------


def test_redis_counter_sums_across_workers(monkeypatch: pytest.MonkeyPatch):
    """Two independent ratelimit modules (= two workers) sharing one fake
    Redis: the third request collectively must be blocked even though
    neither worker alone has seen more than two requests."""
    shared_store: Dict[str, Any] = {}
    # Worker A
    rl_a = _reload_ratelimit(
        monkeypatch, enabled=True, enforce=True, limit=2, redis_url="redis://test"
    )
    fake_a = _FakeRedis(shared_store)
    monkeypatch.setattr(rl_a.redis_client, "get_client", lambda: fake_a)
    req = _DummyRequest(_DummyIdentity(caller_user_id="alice"))
    asyncio.run(rl_a.rate_limit(req))  # count = 1

    # Worker B
    rl_b = _reload_ratelimit(
        monkeypatch, enabled=True, enforce=True, limit=2, redis_url="redis://test"
    )
    fake_b = _FakeRedis(shared_store)
    monkeypatch.setattr(rl_b.redis_client, "get_client", lambda: fake_b)
    asyncio.run(rl_b.rate_limit(req))  # count = 2 — at limit, allowed

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        asyncio.run(rl_b.rate_limit(req))  # count = 3 — rejected
    assert exc.value.status_code == 429


def test_redis_outage_falls_back_to_in_memory(monkeypatch: pytest.MonkeyPatch):
    """A Redis hiccup must not 5xx the request: a failing INCR causes the
    limiter to use the per-worker in-memory counter and otherwise behave
    normally."""
    rl = _reload_ratelimit(
        monkeypatch, enabled=True, enforce=True, limit=2, redis_url="redis://test"
    )
    monkeypatch.setattr(rl.redis_client, "get_client", lambda: _FakeRedis({}, fail=True))
    req = _DummyRequest(_DummyIdentity(caller_user_id="alice"))
    asyncio.run(rl.rate_limit(req))
    asyncio.run(rl.rate_limit(req))
    # The in-memory counter took over; the third call hits its limit.
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        asyncio.run(rl.rate_limit(req))


def test_rate_limit_key_prefers_end_user(monkeypatch: pytest.MonkeyPatch):
    rl = _reload_ratelimit(monkeypatch, enabled=False)
    req = _DummyRequest(_DummyIdentity(end_user_id="u-1", caller_user_id="bff"))
    assert rl.rate_limit_key(req.state.identity, req).startswith("eu:")


def test_rate_limit_key_falls_back_to_caller(monkeypatch: pytest.MonkeyPatch):
    rl = _reload_ratelimit(monkeypatch, enabled=False)
    req = _DummyRequest(_DummyIdentity(end_user_id=None, caller_user_id="bff"))
    assert rl.rate_limit_key(req.state.identity, req).startswith("cu:")
