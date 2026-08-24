"""Coverage for the ES / NQ request-rewrite + response-projection middleware.

The middleware is what makes ES / NQ work across the whole API without every
endpoint growing a futures branch: it points an ``ES`` request at ``SPX`` on
the way in, and carries the answer onto the futures axis on the way out.

Four buckets are pinned here, because getting any of them wrong ships a
number that looks plausible and is wrong:

* a request naming no future must pass through completely untouched;
* endpoints serving OBSERVED futures prices must be bypassed, not projected;
* an UNAUDITED endpoint must refuse rather than guess an axis — the axis is
  chosen per route from an allowlist, never inferred;
* an audited endpoint is projected, relabelled, spot-substituted and
  disclosed.

Driven through a stub app rather than the real one, so the contract is pinned
independently of any endpoint.
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

LIVE_ES = 6650.25


@pytest.fixture(autouse=True)
def _stub_basis(monkeypatch):
    """Pin the ratio and the live spot so tests exercise wiring, not the DB."""

    async def fake_resolve_basis(db, symbol, **kwargs):
        return BASIS if symbol.upper() in ("ES", "SPX") else None

    async def fake_live_spot(index_symbol):
        return LIVE_ES

    monkeypatch.setattr(fm, "resolve_basis", fake_resolve_basis)
    monkeypatch.setattr(fm, "_live_futures_spot", fake_live_spot)
    monkeypatch.setattr(fm, "_db_manager", lambda: object())


def _client() -> TestClient:
    """Stub app echoing the symbol each handler actually saw."""
    seen: dict = {}

    async def summary(request):
        return JSONResponse(
            {
                "symbol": request.query_params.get("symbol"),
                "spot_price": 6600.0,
                "call_wall": 6700.0,
                "net_gex": 1.23e9,
                "flip_distance": 0.0123,
                "strikes": [6600.0, 6650.0],
            }
        )

    async def levels(request):
        return JSONResponse({"symbol": request.path_params["symbol"], "spot": 6600.0})

    async def native(request):
        seen["native"] = request.query_params.get("symbol")
        return JSONResponse({"symbol": request.query_params.get("symbol"), "close": 6600.0})

    async def option(request):  # pragma: no cover - must never be reached
        return JSONResponse({"strike": 6600.0})

    async def card(request):
        return JSONResponse(
            {
                "symbol": request.query_params.get("symbol"),
                "spot_price": 6600.0,
                "target_price": 6650.0,
                "rationale": "Fade into the call wall; target $6,650.00. Credit $2.40.",
            }
        )

    async def text(request):
        return PlainTextResponse("not json")

    app = Starlette(
        routes=[
            Route("/api/gex/summary", summary),
            Route("/api/v1/levels/{symbol}", levels),
            Route("/api/market/quote", native),
            Route("/api/market/session-closes", native),
            Route("/api/market/session-levels", native),
            Route("/api/option/quote", option),
            Route("/api/tools/option-calculator", option),
            Route("/api/signals/action", card),
            Route("/api/flow/contracts", option),
            Route("/api/flow/smart-money", option),
            Route("/api/gex/premium_surface", option),
            Route("/api/forecast", card),
            Route("/api/technicals/text", text),
        ]
    )
    app.add_middleware(fm.FuturesProjectionMiddleware)
    return TestClient(app)


# --- bucket 1: not a future ------------------------------------------------


def test_non_futures_requests_pass_through_untouched():
    resp = _client().get("/api/gex/summary?symbol=SPX")
    body = resp.json()
    assert body["symbol"] == "SPX"
    assert body["call_wall"] == 6700.0
    assert "projection" not in body
    assert "X-ZeroGEX-Projection" not in resp.headers


# --- bucket 2: observed prices are bypassed --------------------------------


@pytest.mark.parametrize(
    "path",
    ["/api/market/quote", "/api/market/session-closes", "/api/market/session-levels"],
)
def test_observed_price_endpoints_are_bypassed(path):
    """These serve the future's OWN bars; projecting a frozen SPX print would
    report where ES stood at the bell rather than where it is now."""
    body = _client().get(f"{path}?symbol=ES").json()
    assert body["symbol"] == "ES"  # handler saw ES, was never rewritten
    assert body["close"] == 6600.0  # untouched by projection
    assert "projection" not in body


# --- bucket 3: unaudited endpoints refuse ----------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "/api/option/quote",  # an SPX contract with a scaled strike is untradable
        "/api/tools/option-calculator",
        "/api/flow/contracts",  # per-contract strikes
        "/api/flow/smart-money",
        "/api/gex/premium_surface",  # an option premium, not an index level
    ],
)
def test_unaudited_endpoints_refuse_futures(path):
    """Projecting an unaudited payload is how a cash number lands on an ES chart."""
    resp = _client().get(f"{path}?symbol=ES")
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert "ES" in detail and "SPX" in detail
    assert path in detail


# --- bucket 4: projection --------------------------------------------------


def test_query_symbol_is_rewritten_and_response_relabelled():
    body = _client().get("/api/gex/summary?symbol=ES").json()
    assert body["symbol"] == "ES"  # handler ran as SPX; client is told ES
    assert body["call_wall"] == pytest.approx(6745.0)


def test_dollar_exposures_and_ratios_survive_projection():
    body = _client().get("/api/gex/summary?symbol=ES").json()
    assert body["net_gex"] == 1.23e9
    # flip_distance is (spot - flip) / spot — a fraction, not a price.
    assert body["flip_distance"] == 0.0123


def test_bare_price_arrays_are_projected():
    """A strike ladder that is a list of plain numbers must move too, or one
    response ships two incompatible price axes."""
    body = _client().get("/api/gex/summary?symbol=ES").json()
    assert body["strikes"] == [pytest.approx(6644.25), pytest.approx(6694.5)]


def test_spot_comes_from_the_live_futures_print_not_the_projection():
    body = _client().get("/api/gex/summary?symbol=ES").json()
    assert body["spot_price"] == LIVE_ES
    assert body["spot_price"] != pytest.approx(6600.0 * BASIS.ratio)


def test_every_spot_alias_is_overridden_not_just_spot_price():
    """/api/v1/levels names it `spot`; an alias left projected would publish
    the frozen cash close as if it were the live future."""
    body = _client().get("/api/v1/levels/ES").json()
    assert body["spot"] == LIVE_ES


def test_path_addressed_symbol_is_rewritten():
    body = _client().get("/api/v1/levels/ES").json()
    assert body["symbol"] == "ES"


def test_projection_metadata_and_header_disclose_the_derivation():
    resp = _client().get("/api/gex/summary?symbol=ES")
    assert resp.headers["X-ZeroGEX-Projection"] == "1"
    meta = resp.json()["projection"]
    assert meta["derived_from"] == "SPX"
    assert meta["basis_source"] == "measured"
    assert meta["basis_ratio"] == pytest.approx(1.0067)


# --- failure modes ---------------------------------------------------------


def test_non_json_responses_are_left_alone():
    """Non-JSON on an ALLOWLISTED route still passes through unchanged."""
    resp = _client().get("/api/technicals/text?symbol=ES")
    assert resp.status_code == 200
    assert resp.text == "not json"


def test_projection_failure_fails_loudly_rather_than_shipping_cash_levels(monkeypatch):
    """Un-projected SPX levels under an ES label would be actively wrong."""

    async def boom(db, symbol, **kwargs):
        raise RuntimeError("basis exploded")

    monkeypatch.setattr(fm, "resolve_basis", boom)
    resp = _client().get("/api/gex/summary?symbol=ES")
    assert resp.status_code == 503
    assert "ES" in json.loads(resp.content)["detail"]


def test_audit_trail_records_the_requested_path_not_the_rewritten_one():
    scope = {
        "type": "http",
        "path": "/api/v1/levels/ES",
        "query_string": b"",
    }
    fm._rewrite_scope(scope, "ES", "SPX")
    assert scope["path"] == "/api/v1/levels/SPX"
    assert scope["zerogex_original_path"] == "/api/v1/levels/ES"
    assert scope["zerogex_requested_symbol"] == "ES"


def test_unrelated_query_params_survive_the_rewrite():
    scope = {
        "type": "http",
        "path": "/api/gex/heatmap",
        "query_string": b"symbol=ES&expirations=all&timeframe=5min&window_units=192",
    }
    fm._rewrite_scope(scope, "ES", "SPX")
    from urllib.parse import parse_qs

    q = parse_qs(scope["query_string"].decode())
    assert q["symbol"] == ["SPX"]
    assert q["expirations"] == ["all"]
    assert q["timeframe"] == ["5min"]
    assert q["window_units"] == ["192"]


# --- round-2 regressions ---------------------------------------------------


@pytest.mark.parametrize(
    "path",
    ["/api/gex/summary", "/api/market/quote", "/api/v1/levels/ES"],
)
def test_a_trailing_slash_never_leaks_the_backing_index(path):
    """Starlette builds its 307 from the scope. Rewriting the scope BEFORE
    routing therefore handed the caller a Location of ?symbol=SPX — raw cash
    levels, no projection block, under an ES request."""
    sep = "&" if "?" in path else "?"
    query = "" if path.endswith("/ES") else f"{sep}symbol=ES"
    resp = _client().get(f"{path}/{query}", follow_redirects=False)
    assert resp.status_code != 307, f"{path} redirected: {resp.headers.get('location')}"
    assert "symbol=SPX" not in (resp.headers.get("location") or "")


def test_string_encoded_decimals_are_projected():
    """Several models declare Decimal without json_encoders, so Pydantic v2
    serialises them as JSON strings — which a plain int/float check skips."""
    from src.jobs.futures_projection import project_payload, projection_tick

    out = project_payload(
        {"max_pain": "6620.0000", "call_notional": "1.0e8"},
        BASIS,
        tick=projection_tick("ES"),
    )
    assert isinstance(out["max_pain"], str), "the wire type must not change"
    assert float(out["max_pain"]) == pytest.approx(6664.25)
    assert out["call_notional"] == "1.0e8"  # exposure, untouched


def test_spot_derived_deltas_are_reconciled_after_substitution():
    """`difference` was computed against the frozen cash spot; once the live
    futures print replaces underlying_price the two must still subtract."""
    from src.api.futures_middleware import _reconcile_spot_derived

    payload = {"max_pain": 6700.0, "underlying_price": LIVE_ES, "difference": 80.0}
    _reconcile_spot_derived(payload, LIVE_ES)
    assert payload["difference"] == pytest.approx(6700.0 - LIVE_ES)


# --- narrative prose -------------------------------------------------------


@pytest.mark.parametrize("path", ["/api/signals/action", "/api/forecast"])
def test_signal_and_forecast_cards_are_now_served(path):
    """These were refused while the allowlist was narrow. They are audited now."""
    body = _client().get(f"{path}?symbol=ES").json()
    assert body["symbol"] == "ES"
    assert body["target_price"] == pytest.approx(6694.5)


def test_prices_quoted_in_prose_are_carried_across_too():
    """A card reading "target $6,650" beside a chart trading 6,694 is a number
    a trader could act on. Index prices in the narrative move; a $2.40 credit
    does not, because it is not on the index axis."""
    body = _client().get("/api/signals/action?symbol=ES").json()
    assert "$6,694.50" in body["rationale"]
    assert "$2.40" in body["rationale"]
    assert body["projection"]["narrative_prices_converted"] is True
