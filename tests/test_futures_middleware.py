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

import asyncio
import json
from datetime import datetime, timedelta, timezone

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

    async def vol_surface(request):
        """Mirrors the real VolSurfaceResponse: a strike axis plus an IV axis."""
        return JSONResponse(
            {
                "symbol": request.query_params.get("symbol"),
                "spot_price": 6600.0,
                "timestamp": "2026-08-21T20:00:00+00:00",
                "expirations": ["2026-09-18"],
                "strikes": [6500.0, 6600.0, 6700.0],
                "surface": [
                    {
                        "expiration": "2026-09-18",
                        "dte": 28,
                        "ivs": [
                            {"strike": 6500.0, "call_iv": 0.152, "put_iv": 0.171},
                            {"strike": 6600.0, "call_iv": 0.141, "put_iv": 0.149},
                            {"strike": 6700.0, "call_iv": 0.133, "put_iv": 0.138},
                        ],
                    }
                ],
                "atm_term_structure": [{"dte": 28, "atm_iv": 0.145}],
                "skew_25d": [{"dte": 28, "skew": 0.021}],
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
            Route("/api/gex/vol_surface", vol_surface),
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


# --- bucket 4: the vol surface carries two axes at once ---------------------
#
# /api/gex/vol_surface was refused as an "IV surface" alongside
# premium_surface, which left the /volatility page's skew chart answering 400
# for ES and NQ.  The two are not alike, and these tests pin the difference:
# the STRIKE ladder is an ordinary index price axis and must move, while the
# IV values are dimensionless rates that must not — shipping one without the
# other is what would put the skew smile over the wrong strikes.


def test_vol_surface_is_served_for_futures():
    """The regression: this answered 400 while the GEX ladders beside it
    rendered from the same SPX chain."""
    resp = _client().get("/api/gex/vol_surface?symbol=ES&underlying=ES")
    assert resp.status_code == 200
    assert resp.headers.get("X-ZeroGEX-Projection") == "1"
    assert resp.json()["symbol"] == "ES"


def test_vol_surface_strike_axis_is_projected():
    """Both the top-level ladder and the per-slice strikes, or the smile is
    drawn against an axis it was not computed on."""
    body = _client().get("/api/gex/vol_surface?symbol=ES").json()
    assert body["strikes"] == [
        pytest.approx(6543.5),
        pytest.approx(6644.25),
        pytest.approx(6745.0),
    ]
    slice_strikes = [p["strike"] for p in body["surface"][0]["ivs"]]
    assert slice_strikes == [
        pytest.approx(6543.5),
        pytest.approx(6644.25),
        pytest.approx(6745.0),
    ]


def test_vol_surface_ivs_are_not_projected():
    """An implied vol is a dimensionless rate — the same number on either
    axis. Scaling it by the basis would report a volatility nobody quoted."""
    body = _client().get("/api/gex/vol_surface?symbol=ES").json()
    ivs = body["surface"][0]["ivs"]
    assert [p["call_iv"] for p in ivs] == [0.152, 0.141, 0.133]
    assert [p["put_iv"] for p in ivs] == [0.171, 0.149, 0.138]
    assert body["atm_term_structure"][0]["atm_iv"] == 0.145
    # 25-delta skew is a difference of two IVs: vol points, not price points.
    assert body["skew_25d"][0]["skew"] == 0.021


def test_vol_surface_dte_survives_projection():
    """A day count is not a price."""
    body = _client().get("/api/gex/vol_surface?symbol=ES").json()
    assert body["surface"][0]["dte"] == 28
    assert body["atm_term_structure"][0]["dte"] == 28
    assert body["surface"][0]["expiration"] == "2026-09-18"


def test_vol_surface_spot_is_the_live_futures_print():
    """The skew chart reads ATM off spot, so a frozen SPX close here would
    place the ATM marker away from where ES actually trades."""
    body = _client().get("/api/gex/vol_surface?symbol=ES").json()
    assert body["spot_price"] == LIVE_ES


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


# --- historical (as-of) basis ----------------------------------------------
#
# A request pinned to a past instant must be projected with the basis that
# stood THEN. Anchoring it at NOW() offsets every level by however much carry
# has moved since — invisible on a chart, and corrupting in a backtest.


@pytest.mark.parametrize(
    "query,expected",
    [
        ("ts=2026-05-14T18:30:00Z", datetime(2026, 5, 14, 18, 30, tzinfo=timezone.utc)),
        ("ts=2026-05-14T18:30:00+00:00", datetime(2026, 5, 14, 18, 30, tzinfo=timezone.utc)),
        # A bare trading day anchors at its cash close, not midnight: every
        # frame in the session sits after 09:30 ET, so midnight would push the
        # basis read a full day back.
        ("date=2026-05-14", datetime(2026, 5, 14, 20, 0, tzinfo=timezone.utc)),
        ("end_date=2026-05-14", datetime(2026, 5, 14, 20, 0, tzinfo=timezone.utc)),
    ],
)
def test_asof_is_read_from_the_request(query, expected):
    scope = {"query_string": query.encode("latin-1"), "path": "/api/replay/frame"}
    assert fm._request_asof(scope) == expected


@pytest.mark.parametrize(
    "query",
    [
        "",                        # a live request
        "symbol=ES",               # no time named
        "ts=not-a-timestamp",      # unparseable: live, never a crash
        "date=2026-13-99",         # impossible date
        "start_date=2026-01-02",   # the FAR end of a range, never the anchor
    ],
)
def test_requests_without_a_usable_anchor_stay_live(query):
    scope = {"query_string": query.encode("latin-1"), "path": "/api/gex/historical"}
    assert fm._request_asof(scope) is None


def test_a_future_dated_anchor_is_treated_as_live():
    """Asking for today mid-session must read the current tape, not an empty
    window ending before the session's own prints."""
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat()
    scope = {"query_string": f"date={tomorrow}".encode("latin-1"), "path": "/api/replay/range"}
    assert fm._request_asof(scope) is None


def test_the_most_specific_anchor_wins():
    """``ts`` pins an instant; ``date`` only pins a session."""
    scope = {
        "query_string": b"date=2026-05-14&ts=2026-05-14T14:05:00Z",
        "path": "/api/replay/frame",
    }
    assert fm._request_asof(scope) == datetime(2026, 5, 14, 14, 5, tzinfo=timezone.utc)


def test_historical_request_resolves_the_basis_at_that_instant(monkeypatch):
    """End to end: the anchor reaches resolve_basis rather than being dropped."""
    seen: dict = {}

    async def capturing_resolve_basis(db, symbol, **kwargs):
        seen["at"] = kwargs.get("at")
        return BASIS

    monkeypatch.setattr(fm, "resolve_basis", capturing_resolve_basis)
    _client().get("/api/gex/summary?symbol=ES&ts=2026-05-14T18:30:00Z")
    assert seen["at"] == datetime(2026, 5, 14, 18, 30, tzinfo=timezone.utc)


def test_live_request_resolves_the_basis_with_no_anchor(monkeypatch):
    seen: dict = {"at": "unset"}

    async def capturing_resolve_basis(db, symbol, **kwargs):
        seen["at"] = kwargs.get("at")
        return BASIS

    monkeypatch.setattr(fm, "resolve_basis", capturing_resolve_basis)
    _client().get("/api/gex/summary?symbol=ES")
    assert seen["at"] is None


def test_the_anchor_survives_the_v2_envelope(monkeypatch):
    """v2 unwraps to `data` and recurses; the anchor must ride along or every
    /api/v2 historical read silently reverts to today's basis."""
    seen: dict = {"at": "unset"}

    async def capturing_resolve_basis(db, symbol, **kwargs):
        seen["at"] = kwargs.get("at")
        return BASIS

    monkeypatch.setattr(fm, "resolve_basis", capturing_resolve_basis)
    middleware = fm.FuturesProjectionMiddleware(app=None)
    envelope = {
        "data": {"spot_price": 6600.0, "call_wall": 6700.0},
        "freshness": {"freshness_status": "fresh", "age_seconds": 3.0},
    }
    at = datetime(2026, 5, 14, 18, 30, tzinfo=timezone.utc)
    asyncio.run(middleware._transform(envelope, "ES", "SPX", at))
    assert seen["at"] == at


def test_a_historical_frame_never_takes_the_live_spot(monkeypatch):
    """Today's ES print stamped on a past frame states something false about
    that frame. The projected value is the honest one."""
    called: dict = {"live": False}

    async def fake_live_spot(index_symbol):
        called["live"] = True
        return LIVE_ES

    monkeypatch.setattr(fm, "_live_futures_spot", fake_live_spot)
    middleware = fm.FuturesProjectionMiddleware(app=None)
    at = datetime(2026, 5, 14, 18, 30, tzinfo=timezone.utc)
    out = asyncio.run(middleware._transform({"spot_price": 6600.0}, "ES", "SPX", at))
    assert called["live"] is False
    assert out["spot_price"] == pytest.approx(6600.0 * BASIS.ratio, abs=0.25)


def test_a_live_request_still_prefers_the_observed_print():
    """The guard above must not disturb the live path."""
    body = _client().get("/api/gex/summary?symbol=ES").json()
    assert body["spot_price"] == LIVE_ES


# --- per-row basis on a series ---------------------------------------------


def _basis_at(ratio):
    return FuturesBasis(
        index_symbol="SPX",
        futures_symbol="ES",
        ratio=ratio,
        source="measured",
        observed_at=None,
        sample_count=5,
        feed_symbol="@ES",
    )


def test_a_multi_month_series_uses_each_row_s_own_basis(monkeypatch):
    """One ratio across a quarter offsets the far end by the whole carry cycle.
    Each row must be projected on the basis that stood in its own session."""
    # Carry decays toward expiry, so an older row sits at a wider basis.
    ratios = {"2026-02-13": 1.0090, "2026-05-14": 1.0050, "2026-08-14": 1.0010}
    seen: list = []

    async def by_anchor(db, symbol, **kwargs):
        at = kwargs.get("at")
        seen.append(at)
        return _basis_at(ratios[at.astimezone(fm.ET).date().isoformat()])

    monkeypatch.setattr(fm, "resolve_basis", by_anchor)
    middleware = fm.FuturesProjectionMiddleware(app=None)
    rows = [
        {"timestamp": f"{day}T18:30:00Z", "call_wall": 6000.0}
        for day in ("2026-02-13", "2026-05-14", "2026-08-14")
    ]
    out = asyncio.run(middleware._transform(rows, "ES", "SPX", None))

    assert [r["call_wall"] for r in out] == [
        pytest.approx(6054.0, abs=0.25),
        pytest.approx(6030.0, abs=0.25),
        pytest.approx(6006.0, abs=0.25),
    ]
    # Three sessions, three anchors — not one ratio smeared across the range.
    assert len({a.date() for a in seen}) == 3


def test_rows_in_one_session_share_a_single_basis_read(monkeypatch):
    """Per-row must not mean per-minute: a session's rows resolve once."""
    calls: list = []

    async def counting(db, symbol, **kwargs):
        calls.append(kwargs.get("at"))
        return _basis_at(1.0067)

    monkeypatch.setattr(fm, "resolve_basis", counting)
    middleware = fm.FuturesProjectionMiddleware(app=None)
    rows = [
        {"timestamp": f"2026-05-14T{14 + i // 60:02d}:{i % 60:02d}:00Z", "call_wall": 6000.0}
        for i in range(120)
    ]
    asyncio.run(middleware._transform(rows, "ES", "SPX", None))
    assert len(calls) == 1


def test_a_row_without_a_timestamp_falls_back_to_the_request_anchor(monkeypatch):
    seen: list = []

    async def capturing(db, symbol, **kwargs):
        seen.append(kwargs.get("at"))
        return _basis_at(1.0067)

    monkeypatch.setattr(fm, "resolve_basis", capturing)
    middleware = fm.FuturesProjectionMiddleware(app=None)
    at = datetime(2026, 5, 14, 18, 30, tzinfo=timezone.utc)
    rows = [{"timestamp": "2026-05-14T18:30:00Z", "call_wall": 6000.0}, {"call_wall": 6100.0}]
    out = asyncio.run(middleware._transform(rows, "ES", "SPX", at))
    assert at in seen
    assert out[1]["call_wall"] == pytest.approx(6100.0 * 1.0067, abs=0.25)
