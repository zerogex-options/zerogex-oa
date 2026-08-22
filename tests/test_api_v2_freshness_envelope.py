"""Tests for the v2 freshness envelope and the generated /api/v2 surface.

Three things a consumer of the vendor API pays for are pinned here:

  * **Coverage** — every v1 endpoint has a v2 twin.  The mirror reads
    FastAPI's route table, and FastAPI >=0.140 stores ``include_router``
    results behind an internal wrapper; a version bump that changes that
    layout would silently shrink the published surface, so the parity
    assertion below is the tripwire.
  * **Payload identity** — ``v2["data"]`` is byte-for-byte the v1 body, so
    migrating is "unwrap data" and nothing else.
  * **Envelope semantics** — the six freshness concepts are computed from
    the payload and the market session, not guessed, and the four
    status bands mean what the guide says they mean.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.api import freshness as fr
from src.api import v2 as v2mod

# A Thursday (a normal trading day) at 14:00 UTC = 10:00 ET, mid-session.
THU_REGULAR = datetime(2026, 8, 20, 14, 0, tzinfo=timezone.utc)
# Same Thursday at 02:00 UTC = 22:00 ET Wednesday — a weekday outside
# 04:00-20:00 ET.
THU_OVERNIGHT = datetime(2026, 8, 20, 2, 0, tzinfo=timezone.utc)
# Thursday 12:00 UTC = 08:00 ET, pre-market.
THU_PREMARKET = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
# A Saturday.
SAT = datetime(2026, 8, 22, 14, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Session + cadence resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "when,expected_session,expected_market_day",
    [
        (THU_REGULAR, fr.SESSION_REGULAR, True),
        (THU_PREMARKET, fr.SESSION_PRE_MARKET, True),
        (THU_OVERNIGHT, fr.SESSION_CLOSED, True),
        (SAT, fr.SESSION_CLOSED, False),
    ],
)
def test_market_context_labels_the_session(when, expected_session, expected_market_day):
    """market_session_status uses the same four-value vocabulary the
    existing /api/market/quote ``session`` field uses — a second session
    taxonomy inside one API would be worse than none."""
    session, market_day = fr.market_context(when)
    assert session == expected_session
    assert market_day is expected_market_day


def test_cadence_is_session_dependent_not_constant():
    """The analytics cycle is 60s in-session and 300s off-hours. Advertising
    a flat 60s would make every off-hours consumer read ``stale``."""
    assert fr.ANALYTICS_CYCLE.cadence_for(fr.SESSION_REGULAR, market_day=True) == 60.0
    assert fr.ANALYTICS_CYCLE.cadence_for(fr.SESSION_CLOSED, market_day=True) == 300.0


def test_no_cadence_is_advertised_on_a_non_market_day():
    """Weekends and holidays: nothing upstream can change, so no profile
    claims a cadence regardless of its closed_seconds."""
    for profile in fr.CADENCE_PROFILES.values():
        assert profile.cadence_for(fr.SESSION_REGULAR, market_day=False) is None


def test_flow_expects_no_updates_outside_the_cash_session():
    """No options flow accrues pre/post-market, so the flow profile must not
    advertise a cadence there and mark the last bucket stale overnight."""
    assert fr.FLOW_AGGREGATE.cadence_for(fr.SESSION_REGULAR, market_day=True) == 60.0
    assert fr.FLOW_AGGREGATE.cadence_for(fr.SESSION_AFTER_HOURS, market_day=True) is None


@pytest.mark.parametrize(
    "path,profile_name",
    [
        ("/api/gex/summary", "analytics_cycle"),
        ("/api/v1/levels/{symbol}", "analytics_cycle"),
        ("/api/flow/by-contract", "flow_aggregate"),
        ("/api/market/quote", "realtime_quote"),
        ("/api/signals/composite", "signals_cycle"),
        ("/api/forecast", "daily_cycle"),
        ("/api/replay/session", "historical"),
        ("/api/tools/option-calculator", "on_demand"),
        # A specific override must beat the family glob listed after it.
        ("/api/gex/historical", "historical"),
        ("/api/market/session-closes", "daily_cycle"),
    ],
)
def test_registry_resolves_paths_to_profiles(path, profile_name):
    assert fr.resolve_profile(path).name == profile_name


def test_iso_duration_rendering():
    assert fr._iso_duration(60) == "PT1M"
    assert fr._iso_duration(5) == "PT5S"
    assert fr._iso_duration(3600) == "PT1H"
    assert fr._iso_duration(86400) == "PT24H"


# ---------------------------------------------------------------------------
# freshness_status bands
# ---------------------------------------------------------------------------


def _payload_aged(seconds: float, now: datetime = THU_REGULAR):
    return {"timestamp": now - timedelta(seconds=seconds)}


@pytest.mark.parametrize(
    "age,expected",
    [
        # cadence 60s, stale window = max(60*2.5, 60) = 150s
        (10, fr.FreshnessStatus.FRESH),
        (60, fr.FreshnessStatus.FRESH),
        (90, fr.FreshnessStatus.AGING),
        (149, fr.FreshnessStatus.AGING),
        (600, fr.FreshnessStatus.STALE),
    ],
)
def test_status_bands_for_a_feed_backed_endpoint(age, expected):
    f = fr.build_freshness(_payload_aged(age), profile=fr.ANALYTICS_CYCLE, now=THU_REGULAR)
    assert f.freshness_status is expected


def test_stale_after_is_published_so_clients_do_not_guess_a_threshold():
    f = fr.build_freshness(_payload_aged(10), profile=fr.ANALYTICS_CYCLE, now=THU_REGULAR)
    assert f.stale_after == f.source_timestamp + timedelta(seconds=150)
    assert f.expected_update_cadence == "PT1M"
    assert f.expected_update_cadence_seconds == 60.0


def test_closed_market_reports_session_closed_not_stale():
    """A consumer polling overnight is not observing a fault and must not be
    given a status it would page on."""
    f = fr.build_freshness(
        {"timestamp": SAT - timedelta(days=2)}, profile=fr.ANALYTICS_CYCLE, now=SAT
    )
    assert f.freshness_status is fr.FreshnessStatus.SESSION_CLOSED
    assert f.expected_update_cadence is None
    assert f.stale_after is None
    # The age is still reported — the client can see it, it just isn't a fault.
    assert f.age_seconds > 0


def test_immutable_history_reports_static_not_session_closed():
    """'This never goes stale' and 'the market is shut' are different facts."""
    f = fr.build_freshness(
        {"timestamp": THU_REGULAR - timedelta(days=30)},
        profile=fr.HISTORICAL,
        now=THU_REGULAR,
    )
    assert f.freshness_status is fr.FreshnessStatus.STATIC


def test_payload_with_no_timestamp_reports_unknown_not_fresh():
    """Claiming freshness we cannot substantiate is the one unsafe answer."""
    f = fr.build_freshness({"legs": [], "pnl": 12.5}, profile=fr.ON_DEMAND, now=THU_REGULAR)
    assert f.freshness_status is fr.FreshnessStatus.UNKNOWN
    assert f.source_timestamp is None
    assert f.age_seconds is None
    # evaluated_at is always present: endpoint health is knowable even when
    # data age is not.
    assert f.evaluated_at == THU_REGULAR


def test_default_profile_never_claims_freshness():
    """An endpoint added without a registry entry must degrade to 'unknown',
    never to a fabricated 'fresh'."""
    assert fr.DEFAULT_PROFILE.cadence_for(fr.SESSION_REGULAR, market_day=True) is None


# ---------------------------------------------------------------------------
# Timestamp extraction
# ---------------------------------------------------------------------------


def test_generated_and_source_are_distinct_concepts():
    """The whole point of the envelope: when the data was computed and when
    the market observation happened are different facts."""
    computed = THU_REGULAR - timedelta(seconds=5)
    observed = THU_REGULAR - timedelta(seconds=45)
    f = fr.build_freshness(
        {"as_of": computed, "timestamp": observed},
        profile=fr.ANALYTICS_CYCLE,
        now=THU_REGULAR,
    )
    assert f.generated_at == computed
    assert f.source_timestamp == observed
    assert f.evaluated_at == THU_REGULAR
    # Age is measured against the observation, not the computation.
    assert f.age_seconds == 45.0


def test_generated_at_falls_back_to_evaluated_at_for_on_demand_results():
    f = fr.build_freshness({"value": 1}, profile=fr.ON_DEMAND, now=THU_REGULAR)
    assert f.generated_at == THU_REGULAR


def test_latest_event_at_is_the_newest_row_in_a_series():
    rows = [{"timestamp": THU_REGULAR - timedelta(minutes=m)} for m in range(10, 0, -1)]
    f = fr.build_freshness({"series": rows}, profile=fr.ANALYTICS_CYCLE, now=THU_REGULAR)
    assert f.latest_event_at == THU_REGULAR - timedelta(minutes=1)


def test_future_stamps_are_not_treated_as_observations():
    """A forecast's target_at is in the future; reading it as an observation
    would report negative age and a bogus 'fresh'."""
    f = fr.build_freshness(
        {
            "target_at": THU_REGULAR + timedelta(hours=6),
            "timestamp": THU_REGULAR - timedelta(hours=2),
        },
        profile=fr.DAILY_CYCLE,
        now=THU_REGULAR,
    )
    assert f.source_timestamp == THU_REGULAR - timedelta(hours=2)


def test_contract_properties_are_not_mistaken_for_observations():
    f = fr.build_freshness(
        {"expiration": "2026-12-18T21:00:00+00:00", "strike": 700.0},
        profile=fr.ANALYTICS_CYCLE,
        now=THU_REGULAR,
    )
    assert f.source_timestamp is None
    assert f.freshness_status is fr.FreshnessStatus.UNKNOWN


def test_naive_timestamps_are_read_as_utc():
    """DB stamps arrive naive-UTC; assuming ET here would shift every age by
    four or five hours and flip statuses."""
    f = fr.build_freshness(
        {"timestamp": (THU_REGULAR - timedelta(seconds=30)).replace(tzinfo=None)},
        profile=fr.ANALYTICS_CYCLE,
        now=THU_REGULAR,
    )
    assert f.age_seconds == 30.0


def test_large_list_payloads_are_sampled_at_both_ends():
    """/api/flow/by-contract can return hundreds of thousands of rows; a full
    walk would put real CPU on the hot path. Sampling both ends finds the
    newest row for time-ordered data, which is what this API returns."""
    n = 5000
    rows = [{"timestamp": THU_REGULAR - timedelta(seconds=n - i)} for i in range(n)]
    f = fr.build_freshness(rows, profile=fr.FLOW_AGGREGATE, now=THU_REGULAR)
    assert f.latest_event_at == THU_REGULAR - timedelta(seconds=1)


def test_build_freshness_never_raises_on_a_hostile_payload():
    """Freshness is metadata about a response that already succeeded; a
    failure to characterise it must not turn a good 200 into a 500."""

    class Exploding:
        def __getattr__(self, name):
            raise RuntimeError("boom")

    f = fr.build_freshness(
        {"timestamp": "not-a-date", "nested": Exploding()},
        profile=fr.ANALYTICS_CYCLE,
        now=THU_REGULAR,
    )
    assert f.freshness_status is fr.FreshnessStatus.UNKNOWN


# ---------------------------------------------------------------------------
# The mounted surface
# ---------------------------------------------------------------------------


def _summary(**overrides):
    base = {
        "timestamp": datetime(2026, 7, 6, 19, 30, tzinfo=timezone.utc),
        "symbol": "SPY",
        "spot_price": 676.04,
        "net_gex_at_spot": -1.2e9,
        "gamma_flip": 675.0,
        "call_wall": 680.0,
        "put_wall": 670.0,
        "max_pain": 676.0,
        "pin_strike": 676.0,
        "pin_score": 1.504e9,
        "pin_confidence": 0.31,
        "pin_strike_reason": None,
        # GEXSummary requires these three; /api/gex/summary 500s without them.
        "total_call_gex": 5.0e9,
        "total_put_gex": -6.2e9,
        "net_gex": -1.2e9,
    }
    base.update(overrides)
    return base


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_latest_gex_summary = AsyncMock(return_value=_summary())
    dbmod.DatabaseManager.get_latest_strike_gamma_profile = AsyncMock(
        return_value=[{"strike": 680.0, "call_gex": 5e8, "put_gex": -1e8, "net_gex": 4e8}]
    )

    from src.api.main import app

    return TestClient(app)


def _api_paths(app):
    schema = app.openapi()
    v1 = {p for p in schema["paths"] if p.startswith("/api/") and not p.startswith("/api/v2")}
    v2 = {p for p in schema["paths"] if p.startswith("/api/v2")}
    return v1, v2


def test_every_v1_endpoint_has_a_v2_twin(client: TestClient):
    """The tripwire for FastAPI's internal route layout.

    ``include_router`` results are not flattened into ``app.routes`` on
    FastAPI >=0.140; the mirror reads them through an internal accessor. If
    an upgrade changes that, this fails loudly instead of quietly publishing
    a fraction of the surface.
    """
    v1, v2 = _api_paths(client.app)
    missing = sorted(p for p in v1 if v2mod.v2_path_for(p) not in v2)
    # Operator-only control-plane routes are excluded by design; every other
    # v1 endpoint must have a twin.
    assert all(v2mod._is_control_plane(p) for p in missing), missing
    assert len(v2) == len(v1) - len(missing)
    # Guard against a partial mirror that still happens to satisfy the ratio.
    assert len(v2) > 100


def test_admin_control_plane_is_not_mirrored(client: TestClient):
    """Key management and the X-post publisher are not part of the versioned
    vendor data contract, and 'how fresh is this' is meaningless for them."""
    _, v2 = _api_paths(client.app)
    # Covers both admin surfaces: /api/admin/* and /api/tradeworkz/admin/*.
    assert not [p for p in v2 if "/admin/" in p or p.endswith("/admin")]


def test_every_mirrored_endpoint_has_a_cadence_registry_entry(client: TestClient):
    """An endpoint with no entry falls back to ON_DEMAND and reports
    freshness_status=unknown forever — correct, but useless to a consumer."""
    v1, _ = _api_paths(client.app)
    mirrored = [p for p in v1 if v2mod.v2_path_for(p) is not None]
    assert fr.audit_cadence_coverage(mirrored) == []


def test_v2_wraps_the_v1_payload_unchanged(client: TestClient):
    """The migration contract: unwrap ``data`` and you have v1 exactly."""
    with client:
        v1 = client.get("/api/v1/levels/SPY?strikes=5")
        v2 = client.get("/api/v2/levels/SPY?strikes=5")
    assert v1.status_code == v2.status_code == 200
    body = v2.json()
    assert sorted(body) == ["data", "freshness"]
    assert body["data"] == v1.json()


def test_unversioned_v1_paths_also_mirror(client: TestClient):
    with client:
        v1 = client.get("/api/gex/summary?symbol=SPY")
        v2 = client.get("/api/v2/gex/summary?symbol=SPY")
    assert v1.status_code == v2.status_code == 200
    assert v2.json()["data"] == v1.json()


def test_envelope_always_carries_every_declared_field(client: TestClient):
    """A consistent envelope is only useful if a consumer can index into it
    unconditionally, which means v2 never drops null-valued keys."""
    expected = set(fr.Freshness.model_fields)
    with client:
        for path in ("/api/v2/levels/SPY", "/api/v2/gex/summary?symbol=SPY", "/api/v2/health"):
            body = client.get(path).json()
            assert set(body["freshness"]) == expected, path


def test_freshness_headers_accompany_the_body(client: TestClient):
    """So a proxy or uptime monitor can act on staleness without parsing a
    body it may not have buffered."""
    with client:
        r = client.get("/api/v2/levels/SPY")
    assert r.headers["X-Freshness-Status"] == r.json()["freshness"]["freshness_status"]
    assert r.headers["X-Market-Session"] == r.json()["freshness"]["market_session_status"]
    assert r.headers["X-Freshness-Cadence-Profile"] == "analytics_cycle"


def test_errors_are_not_enveloped(client: TestClient):
    """A v2 error is a v1 error: enveloping error bodies would force every
    consumer to carry two error parsers, and a 404's freshness says nothing."""
    with client:
        v1 = client.get("/api/v1/levels/SPY?strikes=99999")
        v2 = client.get("/api/v2/levels/SPY?strikes=99999")
    assert v1.status_code == v2.status_code == 422
    assert "freshness" not in v2.json()


def test_v2_routes_do_not_duplicate_the_app_level_dependencies(client: TestClient):
    """The app applies api_key_auth and rate_limit globally, and they arrive
    pre-folded into each route's dependency list. Copying them onto the v2
    route would run auth twice and charge the rate limiter twice per
    request."""
    from fastapi.routing import APIRoute

    app = client.app
    global_names = {d.dependency.__name__ for d in app.router.dependencies}
    v2_routes = [r for r in app.routes if isinstance(r, APIRoute) and r.path.startswith("/api/v2")]
    assert v2_routes
    for route in v2_routes:
        names = [d.dependency.__name__ for d in route.dependencies]
        for global_name in global_names:
            assert names.count(global_name) <= 1, f"{route.path} duplicates {global_name}"


def test_v2_keeps_the_scope_gate_of_the_route_it_mirrors(client: TestClient):
    """v2 must authorise identically to v1 — a mirror that dropped the scope
    dependency would be a redistribution hole."""
    from fastapi.routing import APIRoute

    app = client.app
    by_path = {
        r.path: r for r in app.routes if isinstance(r, APIRoute) and r.path.startswith("/api/v2")
    }
    route = by_path["/api/v2/levels/{symbol}"]
    assert any(d.dependency.__name__ == "_dependency" for d in route.dependencies)


def test_v2_is_documented_in_openapi_with_the_envelope_schema(client: TestClient):
    """Customers pick a version from /docs; an undocumented v2 is unusable."""
    schema = client.app.openapi()
    op = schema["paths"]["/api/v2/levels/{symbol}"]["get"]
    ref = op["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    model = schema["components"]["schemas"][ref.rsplit("/", 1)[-1]]
    assert sorted(model["properties"]) == ["data", "freshness"]
    # And the tags keep /docs navigable rather than interleaving both versions.
    assert op["tags"] == ["Levels (v1) (v2)"] or all(t.endswith("(v2)") for t in op["tags"])


def test_mirror_supports_sync_handlers(monkeypatch: pytest.MonkeyPatch):
    """Every handler on this tree is ``async def`` today, but FastAPI accepts
    plain ``def`` too. A mirror that assumed awaitability would build a v2
    route that raised "object is not awaitable" on its first request —
    a break that would not surface until traffic hit it."""
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/api/thing")
    def sync_handler():  # deliberately not async
        return {"timestamp": THU_REGULAR.isoformat(), "value": 7}

    v2mod.mount_v2(app)
    with TestClient(app) as c:
        r = c.get("/api/v2/thing")
    assert r.status_code == 200
    body = r.json()
    assert body["data"] == {"timestamp": THU_REGULAR.isoformat(), "value": 7}
    assert body["freshness"]["source_timestamp"] is not None


def test_non_json_responses_pass_through_with_headers(monkeypatch: pytest.MonkeyPatch):
    """CSV downloads must not be wrapped — a JSON envelope would corrupt the
    file — so their freshness rides in the headers instead."""
    from fastapi import FastAPI
    from fastapi.responses import PlainTextResponse

    app = FastAPI()

    @app.get("/api/report.csv")
    async def csv_handler():
        return PlainTextResponse(content="a,b\n1,2\n", media_type="text/csv")

    v2mod.mount_v2(app)
    with TestClient(app) as c:
        r = c.get("/api/v2/report.csv")
    assert r.status_code == 200
    assert r.text == "a,b\n1,2\n"
    assert r.headers["X-Freshness-Status"]
    assert r.headers["X-Market-Session"]


def test_json_response_handlers_are_still_enveloped(monkeypatch: pytest.MonkeyPatch):
    """A handler that returns JSONResponse directly (e.g. /api/flow/series)
    must not fall out of the v2 contract just because of how it returns."""
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    app = FastAPI()

    @app.get("/api/flow/series")
    async def series():
        return JSONResponse(content=[{"timestamp": THU_REGULAR.isoformat(), "v": 1}])

    v2mod.mount_v2(app)
    with TestClient(app) as c:
        r = c.get("/api/v2/flow/series")
    body = r.json()
    assert body["data"] == [{"timestamp": THU_REGULAR.isoformat(), "v": 1}]
    assert set(body["freshness"]) == set(fr.Freshness.model_fields)
