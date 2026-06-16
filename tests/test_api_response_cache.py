"""Response cache: HIT/MISS, identity-bucketed keys, passthrough off.

The cache imports ``REDIS_URL`` lazily through ``redis_client.get_client``,
so the tests patch ``redis_client.get_client`` and ``is_configured`` to
control the backing store and bypass.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest

from src.api import cache, redis_client

# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class _Identity:
    end_user_id: Optional[str] = None
    caller_kind: str = "db"


class _State:
    def __init__(self, identity):
        self.identity = identity


class _URL:
    def __init__(self, path: str) -> None:
        self.path = path


class _QueryParams(dict):
    def multi_items(self):
        return list(self.items())


class _Request:
    def __init__(self, path: str, query: Optional[Dict[str, str]] = None, identity=None):
        self.state = _State(identity)
        self.url = _URL(path)
        self.query_params = _QueryParams(query or {})


class _FakeRedis:
    def __init__(self, store: Dict[str, bytes], *, fail: bool = False) -> None:
        self.store = store
        self.fail = fail
        self.gets = 0
        self.sets = 0

    async def get(self, key: str):
        self.gets += 1
        if self.fail:
            raise RuntimeError("simulated redis outage")
        return self.store.get(key)

    async def set(self, key: str, value: bytes, ex: int = 0):
        self.sets += 1
        if self.fail:
            raise RuntimeError("simulated redis outage")
        self.store[key] = value
        return True


# --------------------------------------------------------------------------
# Passthrough — off when Redis isn't configured.
# --------------------------------------------------------------------------


def test_decorator_is_passthrough_when_redis_not_configured(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(redis_client, "is_configured", lambda: False)
    called = []

    @cache.cache_response("ns", ttl_seconds=10)
    async def endpoint(request: Any):
        called.append(1)
        return {"ok": True}

    req = _Request("/api/gex/summary", {"underlying": "SPY"})
    out = asyncio.run(endpoint(request=req))
    assert out == {"ok": True}
    assert len(called) == 1


# --------------------------------------------------------------------------
# MISS → call upstream, write-back. HIT → bypass upstream.
# --------------------------------------------------------------------------


def test_miss_calls_upstream_then_writes_back(monkeypatch: pytest.MonkeyPatch):
    store: Dict[str, bytes] = {}
    fake = _FakeRedis(store)
    monkeypatch.setattr(redis_client, "is_configured", lambda: True)
    monkeypatch.setattr(redis_client, "get_client", lambda: fake)

    calls = []

    @cache.cache_response("gex_summary", ttl_seconds=30)
    async def endpoint(request: Any):
        calls.append(1)
        return {"ok": True, "value": 42}

    req = _Request("/api/gex/summary", {"underlying": "SPY"})

    out = asyncio.run(endpoint(request=req))
    # First call is a MISS — endpoint runs, writes back.
    assert out == {"ok": True, "value": 42}
    # Drain pending background writes (cache_response fires write-back
    # via asyncio.create_task in the same loop the decorator ran on).
    asyncio.run(_drain_cache_tasks())
    assert calls == [1]
    assert len(store) == 1


def test_hit_returns_cached_response_without_calling_upstream(monkeypatch: pytest.MonkeyPatch):
    store: Dict[str, bytes] = {}
    fake = _FakeRedis(store)
    monkeypatch.setattr(redis_client, "is_configured", lambda: True)
    monkeypatch.setattr(redis_client, "get_client", lambda: fake)

    calls = []

    @cache.cache_response("gex_summary", ttl_seconds=30)
    async def endpoint(request: Any):
        calls.append(1)
        return {"ok": True, "value": 42}

    req = _Request("/api/gex/summary", {"underlying": "SPY"})
    asyncio.run(endpoint(request=req))
    asyncio.run(_drain_cache_tasks())

    # Second call hits Redis.
    resp = asyncio.run(endpoint(request=req))
    assert calls == [1]  # endpoint NOT called the second time
    # The HIT path returns a Starlette Response — body is the cached bytes.
    from starlette.responses import Response

    assert isinstance(resp, Response)
    assert resp.headers.get("X-Cache") == "HIT"
    assert resp.body.startswith(b'{"ok":true')


# --------------------------------------------------------------------------
# Identity bucketing — two different identity classes never share a cache.
# --------------------------------------------------------------------------


def test_anon_and_end_user_buckets_isolate_caches(monkeypatch: pytest.MonkeyPatch):
    store: Dict[str, bytes] = {}
    fake = _FakeRedis(store)
    monkeypatch.setattr(redis_client, "is_configured", lambda: True)
    monkeypatch.setattr(redis_client, "get_client", lambda: fake)

    calls = []

    @cache.cache_response("gex_summary", ttl_seconds=30)
    async def endpoint(request: Any):
        calls.append(1)
        return {"ok": True}

    req_anon = _Request("/api/gex/summary", {"u": "SPY"}, identity=None)
    req_user = _Request("/api/gex/summary", {"u": "SPY"}, identity=_Identity(end_user_id="user_1"))

    asyncio.run(endpoint(request=req_anon))
    asyncio.run(_drain_cache_tasks())
    asyncio.run(endpoint(request=req_user))
    asyncio.run(_drain_cache_tasks())

    # Both classes ran their upstream — neither saw the other's cache.
    assert calls == [1, 1]
    assert len(store) == 2


# --------------------------------------------------------------------------
# Query-order invariance and namespace isolation.
# --------------------------------------------------------------------------


def test_query_param_order_does_not_fragment_the_cache(monkeypatch: pytest.MonkeyPatch):
    store: Dict[str, bytes] = {}
    fake = _FakeRedis(store)
    monkeypatch.setattr(redis_client, "is_configured", lambda: True)
    monkeypatch.setattr(redis_client, "get_client", lambda: fake)
    calls = []

    @cache.cache_response("gex_summary", ttl_seconds=30)
    async def endpoint(request: Any):
        calls.append(1)
        return {"ok": True}

    req1 = _Request("/api/gex/summary", {"a": "1", "b": "2"})
    req2 = _Request("/api/gex/summary", {"b": "2", "a": "1"})

    asyncio.run(endpoint(request=req1))
    asyncio.run(_drain_cache_tasks())
    asyncio.run(endpoint(request=req2))
    asyncio.run(_drain_cache_tasks())

    assert calls == [1]  # second call hit the cache
    assert len(store) == 1


def test_redis_outage_treats_get_as_miss(monkeypatch: pytest.MonkeyPatch):
    """A failing GET must not 5xx the request — degrade to MISS."""
    fake = _FakeRedis({}, fail=True)
    monkeypatch.setattr(redis_client, "is_configured", lambda: True)
    monkeypatch.setattr(redis_client, "get_client", lambda: fake)
    calls = []

    @cache.cache_response("gex_summary", ttl_seconds=30)
    async def endpoint(request: Any):
        calls.append(1)
        return {"ok": True}

    req = _Request("/api/gex/summary", {"a": "1"})
    out = asyncio.run(endpoint(request=req))
    assert out == {"ok": True}
    assert calls == [1]


# --------------------------------------------------------------------------
# Helper: drain the cache_response background write-back tasks so HIT
# tests can run deterministically against the populated store.
# --------------------------------------------------------------------------


async def _drain_cache_tasks() -> None:
    tasks = list(cache._BG_TASKS)
    if not tasks:
        return
    for t in tasks:
        try:
            await t
        except Exception:
            pass
