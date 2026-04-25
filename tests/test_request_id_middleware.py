"""Tests for src.api.middleware.RequestIdMiddleware.

Verifies that:
- A request without X-Request-Id gets one generated and echoed back.
- A request with X-Request-Id has that value preserved end-to-end.
- The request_id contextvar is set inside the route handler.
- Concurrent in-flight requests don't bleed contextvars across each other
  (this is the whole reason for using contextvars vs. threading.local).
"""

from __future__ import annotations

import asyncio
import sys
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.middleware import RequestIdMiddleware
from src.utils.logging import request_id_var


def _build_minimal_app() -> FastAPI:
    """Build a FastAPI app with just the middleware + a probe route.

    Avoids reloading src.api.main (expensive and pulls in the full DB
    surface) — we only need to test the middleware behavior here.
    """
    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)

    @app.get("/probe")
    async def probe():
        return {"request_id": request_id_var.get()}

    @app.get("/slow")
    async def slow():
        # Yield to the event loop so concurrent requests interleave
        # mid-handler. If the contextvar were a plain global, the
        # second request would clobber the first's id between yields.
        await asyncio.sleep(0.01)
        return {"request_id": request_id_var.get()}

    return app


def test_middleware_generates_request_id_when_missing():
    with TestClient(_build_minimal_app()) as client:
        response = client.get("/probe")

    assert response.status_code == 200
    request_id = response.headers.get("X-Request-Id")
    assert request_id, "expected X-Request-Id header on response"
    assert response.json()["request_id"] == request_id
    # UUID4 hex is 32 lowercase hex chars.
    assert len(request_id) == 32
    assert all(c in "0123456789abcdef" for c in request_id)


def test_middleware_preserves_incoming_request_id():
    given = "trace-abc-123"
    with TestClient(_build_minimal_app()) as client:
        response = client.get("/probe", headers={"X-Request-Id": given})

    assert response.status_code == 200
    assert response.headers["X-Request-Id"] == given
    assert response.json()["request_id"] == given


def test_middleware_resets_contextvar_after_request():
    """The middleware uses ContextVar.reset() in finally — confirm the
    var doesn't leak the previous request's id into a fresh task."""
    with TestClient(_build_minimal_app()) as client:
        client.get("/probe", headers={"X-Request-Id": "first"})
        # New TestClient call → fresh ASGI scope → contextvar reset.
        # The default sentinel is "-".
        response = client.get("/probe")

    new_id = response.json()["request_id"]
    assert new_id != "first"
    assert new_id != "-"


def test_concurrent_requests_get_distinct_ids():
    """The whole point of contextvars: per-request isolation across
    concurrent async tasks. Hammer /slow concurrently and confirm
    every response sees its own id."""
    app = _build_minimal_app()

    async def hit_one(client: TestClient, given: str):
        # TestClient is sync; run in a thread to overlap.
        return await asyncio.to_thread(client.get, "/slow", headers={"X-Request-Id": given})

    with TestClient(app) as client:

        async def go():
            ids = [f"id-{i}" for i in range(20)]
            responses = await asyncio.gather(*(hit_one(client, x) for x in ids))
            return list(zip(ids, responses))

        results = asyncio.run(go())

    for given, response in results:
        assert response.status_code == 200
        assert response.headers["X-Request-Id"] == given
        assert response.json()["request_id"] == given
