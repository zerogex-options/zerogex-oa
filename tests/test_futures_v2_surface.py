"""ES/NQ must behave identically on /api/v2 and on v1.

The futures projection middleware classifies each request against two audited
tables — a native carve-out and a projectable prefix list — both written in
v1 path form. The v2 mirror serves the same endpoints one path segment along,
so a raw-path match graded every ``/api/v2/*`` futures request as "unaudited"
and refused it, while the identical v1 call answered 200.

Two things are pinned here: the classification is version-agnostic, and the
projection happens INSIDE the v2 envelope. The second matters as much as the
first — the top-level spot substitution keys on SPOT_FIELDS at the top level,
which on an envelope holds only ``data``/``freshness``, so merely letting v2
through would have shipped a PROJECTED spot instead of the observed futures
print. That is the one outcome the middleware exists to prevent.
"""

from __future__ import annotations

import asyncio

import pytest

from src.api import futures_middleware as fm


def _classify(path: str) -> str:
    """Run one request through the middleware and report what it decided.

    ``native`` = passed through untouched, ``project`` = symbol rewritten to
    the backing index and the response wrapped for projection, ``reject`` =
    refused as unaudited.
    """
    decision = {"reached_app": False, "rewritten": None}

    async def app(scope, receive, send):
        decision["reached_app"] = True
        decision["rewritten"] = scope.get("zerogex_original_path")

    sent: list = []

    async def send(message):
        sent.append(message)

    async def receive():  # pragma: no cover - never awaited on these paths
        return {"type": "http.request", "body": b"", "more_body": False}

    scope = {
        "type": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"" if path.rstrip("/").rsplit("/", 1)[-1] == "ES" else b"symbol=ES",
        "headers": [],
        "method": "GET",
    }
    asyncio.run(fm.FuturesProjectionMiddleware(app)(scope, receive, send))

    start = next((m for m in sent if m.get("type") == "http.response.start"), None)
    if start is not None and start.get("status") == 400:
        return "reject"
    if decision["rewritten"] is not None:
        return "project"
    if decision["reached_app"]:
        return "native"
    return "project"  # wrapped for projection, inner app not yet invoked


@pytest.mark.parametrize(
    "path,expected",
    [
        # Projectable, both versions.
        ("/api/gex/summary", "project"),
        ("/api/v2/gex/summary", "project"),
        ("/api/gex/by-strike", "project"),
        ("/api/v2/gex/by-strike", "project"),
        ("/api/technicals", "project"),
        ("/api/v2/technicals", "project"),
        # The consolidated levels contract: /api/v1/levels vs /api/v2/levels.
        ("/api/v1/levels/ES", "project"),
        ("/api/v2/levels/ES", "project"),
        # Served natively from the future's own bars, both versions.
        ("/api/market/quote", "native"),
        ("/api/v2/market/quote", "native"),
        ("/api/market/session-closes", "native"),
        ("/api/v2/market/session-closes", "native"),
        # Genuinely unaudited endpoints must STILL be refused on both.
        ("/api/flow/by-contract", "reject"),
        ("/api/v2/flow/by-contract", "reject"),
        # A future version is covered the day it ships.
        ("/api/v3/gex/summary", "project"),
        # A path that merely looks versioned is not rewritten.
        ("/api/version/foo", "reject"),
    ],
)
def test_futures_classification_is_version_agnostic(path, expected):
    """Drives the real ASGI dispatch, not the helper.

    Asserting on ``_unversioned`` alone would be vacuous: the dispatch could
    stop calling it and this test would stay green while every v2 futures
    request went back to being refused.
    """
    assert _classify(path) == expected


def test_v2_envelope_is_detected_without_false_positives():
    assert fm._is_v2_envelope({"data": {"a": 1}, "freshness": {"freshness_status": "fresh"}})
    # A body that merely has a `data` key is not an envelope.
    assert not fm._is_v2_envelope({"data": 1, "spot": 2})
    assert not fm._is_v2_envelope({"data": {}, "freshness": "no"})
    assert not fm._is_v2_envelope({"data": {}, "freshness": {}, "extra": 1})
    assert not fm._is_v2_envelope([1, 2, 3])
    assert not fm._is_v2_envelope(None)


def test_projection_applies_inside_data_and_leaves_freshness_alone():
    """`data` must come out as whatever v1 would have returned — projection
    block included — and `freshness` must be passed through untouched: an
    observation instant is the same whichever price axis it is rendered on."""
    envelope = {
        "data": {"spot": 6800.0, "levels": {"call_wall": 6850.0}},
        "freshness": {
            "evaluated_at": "2026-08-22T20:00:00Z",
            "freshness_status": "fresh",
            "age_seconds": 3.0,
        },
    }
    middleware = fm.FuturesProjectionMiddleware(app=None)
    real_transform = fm.FuturesProjectionMiddleware._transform

    async def inner(self, payload, futures_symbol, index_symbol):
        # Stand in for the real projection on the unwrapped body.
        if fm._is_v2_envelope(payload):
            return await real_transform(self, payload, futures_symbol, index_symbol)
        out = dict(payload)
        out["spot"] = 6830.0
        out["projection"] = {"ratio": 1.0, "offset": 30.0}
        return out

    fm.FuturesProjectionMiddleware._transform = inner
    try:
        result = asyncio.run(real_transform(middleware, envelope, "ES", "SPX"))
    finally:
        fm.FuturesProjectionMiddleware._transform = real_transform

    assert set(result) == {"data", "freshness"}
    assert result["data"]["spot"] == 6830.0, "projection must reach the payload"
    # Inside `data`, not beside it — otherwise v2's "data is byte-for-byte the
    # v1 body" guarantee breaks for every futures request.
    assert "projection" in result["data"]
    assert "projection" not in result
    assert result["freshness"] == envelope["freshness"]
