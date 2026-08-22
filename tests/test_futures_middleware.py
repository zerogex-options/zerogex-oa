"""Coverage for the ES / NQ request-rewrite + response-projection middleware.

The middleware is what makes ES / NQ work across the whole API without every
endpoint growing a futures branch: it points an ``ES`` request at ``SPX`` on
the way in, and carries the answer onto the futures axis on the way out.

These tests drive it through a stub app rather than the real one, so the
rewrite and projection contracts are pinned independently of any endpoint.
"""

import json
from datetime import datetime, timezone

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from src.api import futures_middleware as fm
from src.jobs.futures_projection import FuturesBasis

BASIS = FuturesBasis(
    index_symbol="SPX",
    futures_symbol="ES",
    ratio=1.0067,
    source="measured",
    observed_at=datetime(2026, 8, 21, 20, 0, tzinfo=timezone.utc),
    sample_count=5,
    feed_symbol="@ES",
)


@pytest.fixture(autouse=True)
def _stub_basis(monkeypatch):
    """Pin the ratio and the live spot so tests exercise wiring, not the DB."""

    async def fake_resolve_basis(db, symbol, **kwargs):
        return BASIS if symbol.upper() in ("ES", "SPX") else None

    async def fake_live_spot(index_symbol):
        return 6650.25

    monkeypatch.setattr(fm, "resolve_basis", fake_resolve_basis)
    monkeypatch.setattr(fm, "_live_futures_spot", fake_live_spot)
    monkeypatch.setattr(fm, "_db_manager", lambda: object())


def _client() -> TestClient:
    """Stub app echoing the symbol the handler actually saw."""

    async def summary(request):
        return JSONResponse(
            {
                "symbol": request.query_params.get("symbol"),
                "spot_price": 6600.0,
                "call_wall": 6700.0,
                "net_gex": 1.23e9,
            }
        )

    async def levels(request):
        return JSONResponse({"symbol": request.path_params["symbol"], "gamma_flip": 6580.0})

    async def quote(request):
        return JSONResponse({"symbol": request.query_params.get("symbol"), "close": 6600.0})

    async def text(request):
        return PlainTextResponse("not json")

    app = Starlette(
        routes=[
            Route("/api/gex/summary", summary),
            Route("/api/v1/levels/{symbol}", levels),
            Route("/api/market/quote", quote),
            Route("/api/text", text),
        ]
    )
    app.add_middleware(fm.FuturesProjectionMiddleware)
    return TestClient(app)


def test_query_symbol_is_rewritten_and_response_relabelled():
    body = _client().get("/api/gex/summary?symbol=ES").json()
    # The handler ran as SPX; the client is told ES.
    assert body["symbol"] == "ES"
    assert body["call_wall"] == pytest.approx(6745.0)
    assert body["net_gex"] == 1.23e9  # exposure untouched


def test_spot_comes_from_the_live_futures_print_not_the_projection():
    """Overnight, SPX is frozen — a projected spot would be yesterday's."""
    body = _client().get("/api/gex/summary?symbol=ES").json()
    assert body["spot_price"] == 6650.25
    assert body["spot_price"] != pytest.approx(6600.0 * BASIS.ratio)


def test_path_addressed_symbol_is_rewritten():
    body = _client().get("/api/v1/levels/ES").json()
    assert body["symbol"] == "ES"
    assert body["gamma_flip"] == pytest.approx(6624.0)  # 6580 * 1.0067, ticked


def test_projection_metadata_is_attached():
    body = _client().get("/api/gex/summary?symbol=ES").json()
    assert body["projection"]["derived_from"] == "SPX"
    assert body["projection"]["basis_source"] == "measured"
    assert body["projection"]["basis_ratio"] == pytest.approx(1.0067)


def test_projection_header_is_set():
    resp = _client().get("/api/gex/summary?symbol=ES")
    assert resp.headers["X-ZeroGEX-Projection"] == "1"


def test_non_futures_requests_pass_through_untouched():
    resp = _client().get("/api/gex/summary?symbol=SPX")
    body = resp.json()
    assert body["symbol"] == "SPX"
    assert body["call_wall"] == 6700.0
    assert "projection" not in body
    assert "X-ZeroGEX-Projection" not in resp.headers


def test_market_quote_is_bypassed_so_its_own_bars_survive():
    """/api/market/quote serves the future's real bars; don't project them."""
    body = _client().get("/api/market/quote?symbol=ES").json()
    assert body["symbol"] == "ES"  # handler saw ES, was never rewritten
    assert body["close"] == 6600.0  # untouched by projection


def test_non_json_responses_are_left_alone():
    resp = _client().get("/api/text?symbol=ES")
    assert resp.status_code == 200
    assert resp.text == "not json"


def test_relabel_only_touches_symbol_keys():
    """A note mentioning SPX must not be rewritten into a note about ES."""
    payload = {"symbol": "SPX", "note": "SPX-derived", "nested": [{"underlying": "SPX"}]}
    out = fm._relabel(payload, "SPX", "ES")
    assert out["symbol"] == "ES"
    assert out["nested"][0]["underlying"] == "ES"
    assert out["note"] == "SPX-derived"


def test_projection_failure_fails_loudly_rather_than_shipping_cash_levels(monkeypatch):
    """Un-projected SPX levels under an ES label would be actively wrong."""

    async def boom(db, symbol, **kwargs):
        raise RuntimeError("basis exploded")

    monkeypatch.setattr(fm, "resolve_basis", boom)
    resp = _client().get("/api/gex/summary?symbol=ES")
    assert resp.status_code == 503
    assert "ES" in json.loads(resp.content)["detail"]
