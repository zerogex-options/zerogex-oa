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
from datetime import date, datetime, timedelta, timezone
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
    """The analytics cycle is 60s while the feed runs and unset once it stops.

    Ingestion covers 04:00-20:00 ET and stops overnight, so between 20:00 and
    04:00 every cycle recomputes the SAME 20:00 observation. Advertising a
    cadence there (this asserted 300s, matching the engine's off-hours tick)
    made every analytics endpoint report ``stale`` for eight hours a night on
    a healthy system.
    """
    assert fr.ANALYTICS_CYCLE.cadence_for(fr.SESSION_REGULAR, market_day=True) == 60.0
    assert fr.ANALYTICS_CYCLE.cadence_for(fr.SESSION_PRE_MARKET, market_day=True) == 60.0
    assert fr.ANALYTICS_CYCLE.cadence_for(fr.SESSION_CLOSED, market_day=True) is None


def test_no_feed_backed_profile_expects_updates_in_the_overnight_gap():
    """The 20:00-04:00 ET window is a feed gap, not a slow period. Any
    feed-backed profile claiming a cadence there re-creates the nightly
    false page."""
    for profile in fr.CADENCE_PROFILES.values():
        if profile.feed_backed and not profile.session_scoped:
            assert profile.cadence_for(fr.SESSION_CLOSED, market_day=True) is None, profile.name


def test_no_cadence_is_advertised_on_a_non_market_day():
    """Weekends and holidays: nothing upstream can change, so no profile
    claims a cadence regardless of its closed_seconds."""
    for profile in fr.CADENCE_PROFILES.values():
        assert profile.cadence_for(fr.SESSION_REGULAR, market_day=False) is None


def test_flow_expects_no_updates_outside_the_cash_session():
    """No options flow accrues pre/post-market, so the flow profile must not
    advertise a cadence there and mark the last bucket stale overnight.

    The in-session figure is the FIVE-minute flow bar, not the one-minute tape
    bucket. This assertion originally read 60.0 — copied from the profile as
    written rather than from what flow_by_contract actually stores — so it
    pinned the bug in place instead of catching it: a healthy flow feed read
    `stale` for half of every bar, which is what /api/v2/flow/series reported
    on the first api-test that ever ran inside market hours.
    """
    from src.config import FLOW_BAR_SECONDS

    assert fr.FLOW_AGGREGATE.cadence_for(fr.SESSION_REGULAR, market_day=True) == float(
        FLOW_BAR_SECONDS
    )
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


def _scope_gates(dependencies):
    """``(qualname, required_scopes)`` for each dependency, scopes included.

    ``require_scopes("market_raw")`` returns a closure over ``required_set``,
    so every gate looks like the same function object from the outside.
    Comparing only the function NAME is what made the original version of this
    test vacuous: swapping every gate to the cheapest scope left it green.
    Reach into the closure cell so the diff compares what is actually enforced.
    """
    out = []
    for dep in dependencies:
        fn = dep.dependency
        scopes = frozenset()
        code = getattr(fn, "__code__", None)
        if code is not None and getattr(fn, "__closure__", None):
            for name, cell in zip(code.co_freevars, fn.__closure__):
                if name == "required_set":
                    try:
                        scopes = frozenset(cell.cell_contents)
                    except Exception:  # noqa: BLE001
                        pass
        out.append((getattr(fn, "__qualname__", repr(fn)), scopes))
    return sorted(out)


def test_every_v2_route_enforces_exactly_its_v1_scopes(client: TestClient):
    """v2 must authorise identically to v1 across the WHOLE surface.

    ``market_raw`` is deliberately withheld from external/analytics-tier keys
    (see src/api/scopes.py), so a v2 route that lost or downgraded its gate
    would leak raw upstream market data to customers who did not buy it. This
    diffs all ~118 pairs including the required-scope SETS, which is what
    catches a downgrade rather than only a deletion.
    """
    from fastapi.routing import APIRoute

    app = client.app
    global_ids = {id(d) for d in app.router.dependencies}
    v2_routes = {
        r.path: r for r in app.routes if isinstance(r, APIRoute) and r.path.startswith("/api/v2")
    }

    compared = 0
    for spec in v2mod._iter_route_specs(app):
        v2_path = v2mod.v2_path_for(spec.path)
        if v2_path is None or v2_path not in v2_routes:
            continue
        expected = _scope_gates([d for d in spec.dependencies if id(d) not in global_ids])
        actual = _scope_gates(
            [d for d in v2_routes[v2_path].dependencies if id(d) not in global_ids]
        )
        assert actual == expected, f"{spec.path} -> {v2_path}: {actual} != {expected}"
        compared += 1

    assert compared > 100, f"only compared {compared} pairs"

    # Spot-check BOTH sides of the licence boundary by name, so a v2 route
    # that silently drifted across it fails here even if the pair-diff above
    # still matches. /api/option/quote enumerates the chain and stays raw;
    # /api/market/quote is the underlying's own price and must stay reachable
    # from a customer bundle.
    raw = _scope_gates(
        [d for d in v2_routes["/api/v2/option/quote"].dependencies if id(d) not in global_ids]
    )
    assert any("market_raw" in scopes for _, scopes in raw), raw

    reference = _scope_gates(
        [d for d in v2_routes["/api/v2/market/quote"].dependencies if id(d) not in global_ids]
    )
    assert any("market_reference" in scopes for _, scopes in reference), reference
    assert not any("market_raw" in scopes for _, scopes in reference), reference


def test_v2_data_is_byte_identical_for_unmodelled_routes(client: TestClient):
    """The migration contract, tested where it can actually break.

    v1 serializes a route WITH a response_model through pydantic and one
    WITHOUT through jsonable_encoder, and the two disagree on the types
    asyncpg returns: Decimal is a number vs a string, datetime is +00:00 vs
    Z. 49 of the mirrored routes have no response_model, and the original
    payload-identity test only covered two routes that both had one — so it
    could not see the divergence at all.
    """
    from decimal import Decimal

    from src.api import database as dbmod

    rows = [
        {
            "timestamp": datetime(2026, 8, 20, 14, 0, tzinfo=timezone.utc),
            "volume": 1234,
            "avg_volume": Decimal("987.65"),
            "spike_ratio": Decimal("2.50"),
        }
    ]
    dbmod.DatabaseManager.get_unusual_volume_spikes = AsyncMock(return_value=rows)

    with client:
        v1 = client.get("/api/technicals/volume-spikes?symbol=SPY")
        v2 = client.get("/api/v2/technicals/volume-spikes?symbol=SPY")

    assert v1.status_code == v2.status_code == 200
    assert v2.json()["data"] == v1.json()
    row = v2.json()["data"][0]
    assert isinstance(row["avg_volume"], float), "Decimal must stay a JSON number"
    assert row["timestamp"].endswith("+00:00"), "must keep v1's datetime form"
    # The envelope itself stays on pydantic's form on EVERY route, so a
    # consumer parses one timestamp format for freshness regardless of route.
    assert v2.json()["freshness"]["evaluated_at"].endswith("Z")


def test_serialization_path_matches_v1_on_every_route(client: TestClient):
    """Structural guard behind the test above: a v2 route must declare a
    response_model iff its v1 twin does. Any drift re-opens the encoder
    divergence on whichever route drifted."""
    from fastapi.routing import APIRoute

    app = client.app
    v2_routes = {
        r.path: r for r in app.routes if isinstance(r, APIRoute) and r.path.startswith("/api/v2")
    }
    mismatched = []
    for spec in v2mod._iter_route_specs(app):
        v2_path = v2mod.v2_path_for(spec.path)
        if v2_path is None or v2_path not in v2_routes:
            continue
        if (spec.response_model is None) != (v2_routes[v2_path].response_model is None):
            mismatched.append(spec.path)
    assert mismatched == []


def test_v2_health_probes_are_public_like_their_v1_twins(client: TestClient):
    """The allowlist is matched on the literal path, so the v2 twins have to
    be registered or an operator repointing a probe at v2 gets a 401 and a
    service that never comes up healthy."""
    from src.api.security import public_paths

    paths = public_paths()
    assert "/api/v2/health" in paths
    assert "/api/v2/health/live" in paths


def test_control_plane_exclusion_matches_whole_segments(client: TestClient):
    """Admin AND internal surfaces stay v1-only; a path that merely contains
    the substring must still be mirrored."""
    assert v2mod.v2_path_for("/api/tradeworkz/admin/reset-fleet") is None
    assert v2mod.v2_path_for("/api/tradeworkz/internal/mark-notification") is None
    assert v2mod.v2_path_for("/api/admin/api-keys") is None
    assert v2mod.v2_path_for("/api/x/administration") == "/api/v2/x/administration"


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


# ---------------------------------------------------------------------------
# Timestamp precedence (write time is not observation time)
# ---------------------------------------------------------------------------


def test_row_write_time_never_outranks_the_market_observation():
    """``option_chains`` rows are UPSERTed in 60s buckets with
    ``updated_at = NOW()`` on conflict, so re-writing an OLD snapshot bumps
    updated_at to now. Ranking it beside the observation keys let a chain
    that was ten minutes stale report ``age_seconds`` of 3 and ``fresh`` —
    the freshest-looking answer on the stalest data."""
    payload = {
        "contracts": [
            {
                "timestamp": THU_REGULAR - timedelta(minutes=10),
                "updated_at": THU_REGULAR - timedelta(seconds=3),
            }
        ]
    }
    f = fr.build_freshness(payload, profile=fr.ANALYTICS_CYCLE, now=THU_REGULAR)
    assert f.source_timestamp == THU_REGULAR - timedelta(minutes=10)
    assert f.age_seconds == 600.0
    assert f.freshness_status is fr.FreshnessStatus.STALE


def test_write_time_is_still_used_when_nothing_observed_a_market():
    """A table that only tracks write time should still get a usable answer —
    the demotion is about precedence, not exclusion."""
    f = fr.build_freshness(
        {"rows": [{"updated_at": THU_REGULAR - timedelta(seconds=20)}]},
        profile=fr.ANALYTICS_CYCLE,
        now=THU_REGULAR,
    )
    assert f.source_timestamp == THU_REGULAR - timedelta(seconds=20)


def test_a_stamp_minted_during_this_request_is_not_evidence_of_freshness():
    """/api/news/headlines stamps ``generated_at`` as it builds the response
    and returns an empty list when the upstream fetch fails. Treating that as
    a source timestamp made it report ``fresh`` with ``age_seconds`` 0.0
    during a total outage — an endpoint that could never say anything else."""
    f = fr.build_freshness(
        {"generated_at": THU_REGULAR, "headlines": []},
        profile=fr.DAILY_CYCLE,
        now=THU_REGULAR,
    )
    assert f.source_timestamp is None
    assert f.freshness_status is fr.FreshnessStatus.UNKNOWN
    # Endpoint health is still reported — that part was never in doubt.
    assert f.generated_at == THU_REGULAR


def test_an_earlier_compute_stamp_is_still_a_valid_source_proxy():
    f = fr.build_freshness(
        {"as_of": THU_REGULAR - timedelta(seconds=45)},
        profile=fr.ANALYTICS_CYCLE,
        now=THU_REGULAR,
    )
    assert f.source_timestamp == THU_REGULAR - timedelta(seconds=45)
    assert f.freshness_status is fr.FreshnessStatus.FRESH


# ---------------------------------------------------------------------------
# Scan bounds
# ---------------------------------------------------------------------------


def test_wide_stampless_leaves_do_not_hide_the_level_that_has_the_stamps():
    """/api/gex/strike-profile-timeseries is 78 buckets x ~300 strikes. The
    strikes carry no timestamps, so a depth-first walk burned ~9 ms per
    request descending 7,800 of them — on an endpoint the rewind chart polls
    at ~1 Hz. The walk is breadth-first so the bucket level is read in full
    first; this pins that the ANSWER is unchanged by that optimisation."""
    payload = {
        "buckets": [
            {
                "timestamp": THU_REGULAR - timedelta(minutes=i),
                "strikes": [{"strike": 600 + j, "net_gex": 1.0} for j in range(300)],
            }
            for i in range(78, 0, -1)
        ]
    }
    f = fr.build_freshness(payload, profile=fr.ANALYTICS_CYCLE, now=THU_REGULAR)
    assert f.latest_event_at == THU_REGULAR - timedelta(minutes=1)


def test_a_stamp_that_only_exists_deep_is_still_found():
    """The level-wise early exit must not fire before anything was found, or
    a payload whose only stamp is nested would report ``unknown``."""
    deep = {"a": {"b": {"c": {"d": [{"timestamp": THU_REGULAR - timedelta(minutes=5)}]}}}}
    generated, latest = fr._scan_timestamps(deep, THU_REGULAR)
    assert latest == THU_REGULAR - timedelta(minutes=5)


def test_huge_lists_are_not_copied_to_be_sampled():
    """Slicing head+tail out of a 500k-row body allocated the whole list
    again. Index into it instead — the scan should be effectively free."""
    import tracemalloc

    rows = [{"timestamp": THU_REGULAR - timedelta(seconds=500_000 - i)} for i in range(500_000)]
    tracemalloc.start()
    generated, latest = fr._scan_timestamps(rows, THU_REGULAR)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert latest == THU_REGULAR - timedelta(seconds=1)
    assert peak < 500_000, f"scan allocated {peak} bytes"


# ---------------------------------------------------------------------------
# Mirror robustness
# ---------------------------------------------------------------------------


def test_a_no_body_status_route_does_not_break_the_mount():
    """FastAPI asserts a 204 must carry no body when a response_model is
    attached, and mount_v2 runs at import — so mirroring one would stop
    uvicorn booting v1 as well. Adding the REST-conventional
    ``@router.delete(..., status_code=204)`` must not be an outage."""
    from fastapi import FastAPI

    app = FastAPI()

    @app.delete("/api/runs/{run_id}", status_code=204)
    async def delete_run(run_id: int):
        return None

    created = v2mod.mount_v2(app)  # must not raise
    assert "/api/v2/runs/{run_id}" not in created
    with TestClient(app) as c:
        assert c.delete("/api/runs/7").status_code == 204


def test_a_handler_taking_its_own_response_object_gets_a_working_twin():
    """Router modules use ``from __future__ import annotations``, so a
    ``response: Response`` parameter arrives as the STRING "Response" and an
    identity check never matches. The wrapper then left the handler's own
    parameter unfilled and every v2 request 500'd while v1 stayed green."""
    from fastapi import FastAPI

    module = (
        "from __future__ import annotations\n"
        "from fastapi import APIRouter, Response\n"
        "router = APIRouter(prefix='/api/cached')\n"
        "@router.get('/thing')\n"
        "async def get_thing(response: Response, symbol: str = 'SPY'):\n"
        "    response.headers['Cache-Control'] = 'public, max-age=5'\n"
        "    return {'symbol': symbol, 'timestamp': '2026-08-20T14:00:00+00:00'}\n"
    )
    ns: dict = {}
    exec(compile(module, "<router_mod>", "exec"), ns)  # noqa: S102
    app = FastAPI()
    app.include_router(ns["router"])
    v2mod.mount_v2(app)

    with TestClient(app) as c:
        v1 = c.get("/api/cached/thing")
        v2 = c.get("/api/v2/cached/thing")
    assert v2.status_code == 200, v2.text
    assert v2.json()["data"] == v1.json()
    # The handler's own header must survive alongside the freshness ones.
    assert v2.headers["Cache-Control"] == "public, max-age=5"
    assert v2.headers["X-Freshness-Status"]


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def test_early_close_days_do_not_manufacture_a_stale_afternoon(
    monkeypatch: pytest.MonkeyPatch,
):
    """13:00-16:00 ET on the Friday after Thanksgiving is not a stalled feed."""
    import src.market_calendar as mc

    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {date(2026, 11, 27)})
    half_day_afternoon = datetime(2026, 11, 27, 19, 0, tzinfo=timezone.utc)  # 14:00 ET
    session, market_day = fr.market_context(half_day_afternoon)
    assert market_day is True
    assert session == fr.SESSION_CLOSED
    f = fr.build_freshness(
        {"timestamp": datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc)},  # 13:00 ET
        profile=fr.ANALYTICS_CYCLE,
        now=half_day_afternoon,
    )
    assert f.freshness_status is fr.FreshnessStatus.SESSION_CLOSED
    # Before the early close it behaves like any other session.
    morning = datetime(2026, 11, 27, 16, 0, tzinfo=timezone.utc)  # 11:00 ET
    assert fr.market_context(morning)[0] == fr.SESSION_REGULAR


def test_daily_artifacts_age_in_trading_sessions_not_wall_clock():
    """Friday's close is the only possible answer until Monday's lands, so a
    36-hour wall-clock window reported ``stale`` every Monday morning and
    after every holiday."""
    monday_morning = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)  # 08:00 ET Mon
    friday_close = datetime(2026, 8, 21, 20, 0, tzinfo=timezone.utc)  # 16:00 ET Fri
    f = fr.build_freshness({"timestamp": friday_close}, profile=fr.DAILY_CYCLE, now=monday_morning)
    assert f.freshness_status is fr.FreshnessStatus.FRESH
    # Genuinely missing several sessions is still late.
    stale = fr.build_freshness(
        {"timestamp": datetime(2026, 8, 18, 20, 0, tzinfo=timezone.utc)},
        profile=fr.DAILY_CYCLE,
        now=datetime(2026, 8, 24, 22, 0, tzinfo=timezone.utc),
    )
    assert stale.freshness_status is fr.FreshnessStatus.STALE


def test_historical_context_is_graded_as_live_analytics():
    """/api/gex/historical-context returns the LIVE headline metrics against
    their rolling distributions. A ``historical`` classification reported
    ``static`` — never stale — on one of the fourteen endpoints the
    documented public API product sells."""
    assert fr.resolve_profile("/api/gex/historical-context").name == "analytics_cycle"
    assert fr.resolve_profile("/api/gex/historical").name == "historical"


def test_the_scan_does_not_descend_into_stampless_leaf_collections():
    """The performance fix, pinned deterministically rather than by a timer.

    Correctness tests cannot see this: descending into 7,800 strike dicts
    produces the SAME answer, just ~9 ms slower per request on an endpoint
    polled at ~1 Hz. So assert the traversal itself — once the level holding
    the timestamps has been read, the leaf level below it must never be
    touched.
    """

    class CountingDict(dict):
        """A dict that records every time the scan reads its members."""

        visits = 0

        def items(self):
            CountingDict.visits += 1
            return super().items()

    payload = {
        "buckets": [
            {
                "timestamp": THU_REGULAR - timedelta(minutes=i),
                # The wide, stamp-free leaf level.
                "strikes": [CountingDict(strike=600 + j, net_gex=1.0) for j in range(300)],
            }
            for i in range(78, 0, -1)
        ]
    }
    generated, latest = fr._scan_timestamps(payload, THU_REGULAR)

    assert latest == THU_REGULAR - timedelta(minutes=1), "answer must be unchanged"
    assert CountingDict.visits == 0, (
        f"scan descended into {CountingDict.visits} stampless leaf dicts; "
        "the level-wise early exit is not working"
    )


def test_health_makes_no_freshness_claim_it_cannot_support():
    """/api/health publishes `last_data_update` — its own REPORT about data
    freshness, not a timestamp of the health payload. Reading it as our
    observation graded a three-hour-old quote stamp as `static`, i.e. "never
    goes stale", on the one endpoint whose job is reporting data age. The
    honest answer is `unknown`: no claim, read `data_age_seconds` in the body.
    """
    payload = {
        "status": "healthy",
        "database_connected": True,
        "last_data_update": THU_REGULAR - timedelta(hours=3),
        "data_age_seconds": 10800,
    }
    f = fr.build_freshness(payload, profile=fr.resolve_profile("/api/health"), now=THU_REGULAR)
    assert f.freshness_status is fr.FreshnessStatus.UNKNOWN
    assert f.source_timestamp is None
    # Endpoint health is still observable — that part was never in doubt.
    assert f.evaluated_at == THU_REGULAR


def test_cors_exposes_every_freshness_header_the_server_sets(client: TestClient):
    """A header the browser strips is a header that does not exist for a
    cross-origin client, however correctly the server sets it. The v2 headers
    are a documented feature, so the two lists have to stay in step: add one
    to _freshness_headers without listing it here and this fails.
    """
    from starlette.middleware.cors import CORSMiddleware

    app = client.app
    cors = next(
        (m for m in app.user_middleware if m.cls is CORSMiddleware),
        None,
    )
    assert cors is not None, "CORS middleware not installed"
    exposed = {h.lower() for h in (cors.kwargs.get("expose_headers") or [])}

    # Every header _freshness_headers can emit, with all optionals populated.
    full = fr.Freshness(
        evaluated_at=THU_REGULAR,
        generated_at=THU_REGULAR,
        source_timestamp=THU_REGULAR,
        latest_event_at=THU_REGULAR,
        age_seconds=1.0,
        market_session_status=fr.SESSION_REGULAR,
        expected_update_cadence="PT1M",
        expected_update_cadence_seconds=60.0,
        cadence_profile="analytics_cycle",
        stale_after=THU_REGULAR,
        freshness_status=fr.FreshnessStatus.FRESH,
    )
    emitted = {h.lower() for h in v2mod._freshness_headers(full)}
    assert emitted <= exposed, f"not readable cross-origin: {sorted(emitted - exposed)}"


# ---------------------------------------------------------------------------
# Cadence must describe what is STORED, not how often we poll
# ---------------------------------------------------------------------------

# Thursday 13:00:00 ET — mid cash session, the window every earlier test missed.
THU_MIDSESSION = datetime(2026, 8, 20, 17, 0, tzinfo=timezone.utc)


def test_a_healthy_minute_bucketed_tape_is_never_stale():
    """The quote tape is polled every few seconds but STORED in 60s buckets
    (``_store_underlying`` floors to AGGREGATION_BUCKET_SECONDS), so the
    freshest row that can exist is up to a minute old. Grading against the 5s
    poll interval declared a healthy tape late for 39 of every 60 seconds."""
    profile = fr.resolve_profile("/api/market/quote")
    bucket = THU_MIDSESSION  # the in-progress minute's bar
    statuses = {
        fr.build_freshness(
            {"timestamp": bucket}, profile=profile, now=bucket + timedelta(seconds=s)
        ).freshness_status
        for s in range(60)
    }
    assert statuses == {fr.FreshnessStatus.FRESH}, statuses


def test_realtime_cadence_tracks_the_storage_bucket_not_the_poll_rate():
    """Drift guard. If someone re-anchors this to MARKET_HOURS_POLL_INTERVAL
    the endpoint goes back to reporting stale most of the session."""
    from src.config import AGGREGATION_BUCKET_SECONDS, MARKET_HOURS_POLL_INTERVAL

    assert fr.REALTIME_QUOTE.regular_seconds == float(AGGREGATION_BUCKET_SECONDS)
    assert fr.REALTIME_QUOTE.regular_seconds != float(MARKET_HOURS_POLL_INTERVAL)


def test_volatility_bars_are_graded_on_their_own_five_minute_cadence():
    """VIX/VXN are 5-minute bars, not the 1-minute tape. On the shared quote
    profile they read stale for most of every bar."""
    profile = fr.resolve_profile("/api/market/volatility")
    assert profile.name == "volatility_bar"
    assert profile.regular_seconds == 300.0
    statuses = {
        fr.build_freshness(
            {"timestamp": THU_MIDSESSION},
            profile=profile,
            now=THU_MIDSESSION + timedelta(seconds=s),
        ).freshness_status
        for s in range(0, 300, 10)
    }
    assert fr.FreshnessStatus.STALE not in statuses, statuses
    # It must sit ahead of the broad /api/market/* glob to win.
    assert fr.resolve_profile("/api/market/quote").name == "realtime_quote"


def test_no_feed_backed_profile_calls_a_healthy_mid_session_payload_stale():
    """The class guard for this whole family of bug.

    Every check before this ran outside market hours, where `session_closed`
    masks any cadence mismatch — which is exactly how a profile grading a
    healthy tape as stale for 65% of the session shipped unnoticed. This
    exercises mid-session explicitly: an observation one cadence old is the
    freshest thing that profile can ever see, so it must never be `stale`.
    """
    for profile in fr.CADENCE_PROFILES.values():
        cadence = profile.cadence_for(fr.SESSION_REGULAR, market_day=True)
        if cadence is None or profile.session_scoped:
            continue
        freshest_possible = THU_MIDSESSION - timedelta(seconds=cadence)
        status = fr.build_freshness(
            {"timestamp": freshest_possible}, profile=profile, now=THU_MIDSESSION
        ).freshness_status
        assert status is not fr.FreshnessStatus.STALE, (
            f"{profile.name}: an observation exactly one cadence "
            f"({cadence}s) old — the freshest this profile can ever "
            f"see — is graded {status.value}"
        )


# ---------------------------------------------------------------------------
# A window cannot be late the instant it opens
# ---------------------------------------------------------------------------


def test_a_feed_window_does_not_open_straight_into_stale():
    """stale_after anchored on source_timestamp put the clock in the PREVIOUS
    window, so at 04:00:00 ET every feed-backed endpoint flipped
    session_closed -> stale in one second and the `aging` grace band was
    structurally unreachable at exactly the boundary it exists for."""
    last_night = datetime(2026, 8, 19, 23, 59, tzinfo=timezone.utc)  # 19:59 ET
    before = datetime(2026, 8, 20, 7, 59, 59, tzinfo=timezone.utc)  # 03:59:59 ET
    after = datetime(2026, 8, 20, 8, 0, 1, tzinfo=timezone.utc)  # 04:00:01 ET

    for name in ("realtime_quote", "analytics_cycle", "signals_cycle"):
        profile = fr.CADENCE_PROFILES[name]
        assert (
            fr.build_freshness(
                {"timestamp": last_night}, profile=profile, now=before
            ).freshness_status
            is fr.FreshnessStatus.SESSION_CLOSED
        ), name
        opened = fr.build_freshness(
            {"timestamp": last_night}, profile=profile, now=after
        ).freshness_status
        assert opened is not fr.FreshnessStatus.STALE, f"{name} opened straight into stale"


def test_a_window_that_stays_empty_still_goes_stale():
    """The anchor must delay the verdict, not suppress it."""
    last_night = datetime(2026, 8, 19, 23, 59, tzinfo=timezone.utc)
    ten_past = datetime(2026, 8, 20, 8, 10, tzinfo=timezone.utc)  # 04:10 ET
    f = fr.build_freshness({"timestamp": last_night}, profile=fr.REALTIME_QUOTE, now=ten_past)
    assert f.freshness_status is fr.FreshnessStatus.STALE
    # age is still measured honestly from the observation, not the anchor
    assert f.age_seconds > 8 * 3600


# ---------------------------------------------------------------------------
# ES/NQ keep their own calendar
# ---------------------------------------------------------------------------

# Friday 01:16 ET — CME trading, NYSE shut.
FRI_OVERNIGHT = datetime(2026, 8, 21, 5, 16, tzinfo=timezone.utc)


def test_futures_are_graded_on_the_cme_session_not_the_nyse_one():
    """ES/NQ trade ~23 hours a day. On the cash calendar a dead overnight
    futures feed reported `session_closed` — no update due — while the same
    response body said `stale: true, data_age_seconds: 2701`. A monitor built
    on the envelope stayed silent through the entire outage."""
    profile = fr.resolve_profile("/api/market/quote")
    dead = fr.build_freshness(
        {"timestamp": FRI_OVERNIGHT - timedelta(seconds=2701)},
        profile=profile,
        now=FRI_OVERNIGHT,
        symbol="ES",
    )
    assert dead.freshness_status is fr.FreshnessStatus.STALE
    assert dead.market_session_status == fr.SESSION_REGULAR

    healthy = fr.build_freshness(
        {"timestamp": FRI_OVERNIGHT - timedelta(seconds=30)},
        profile=profile,
        now=FRI_OVERNIGHT,
        symbol="ES",
    )
    assert healthy.freshness_status is fr.FreshnessStatus.FRESH


def test_cash_symbols_are_unaffected_by_the_futures_path():
    """SPY overnight is genuinely closed and must stay session_closed."""
    f = fr.build_freshness(
        {"timestamp": FRI_OVERNIGHT - timedelta(seconds=2701)},
        profile=fr.resolve_profile("/api/market/quote"),
        now=FRI_OVERNIGHT,
        symbol="SPY",
    )
    assert f.freshness_status is fr.FreshnessStatus.SESSION_CLOSED
    # And with no symbol at all, behaviour is exactly as before.
    assert (
        fr.build_freshness(
            {"timestamp": FRI_OVERNIGHT - timedelta(seconds=2701)},
            profile=fr.resolve_profile("/api/market/quote"),
            now=FRI_OVERNIGHT,
        ).freshness_status
        is fr.FreshnessStatus.SESSION_CLOSED
    )


def test_the_v2_wrapper_finds_the_symbol_in_both_request_shapes():
    assert v2mod._request_symbol({"symbol": "ES", "limit": 10}) == "ES"
    assert v2mod._request_symbol({"underlying": "NQ"}) == "NQ"
    assert v2mod._request_symbol({"ticker": "VIX"}) == "VIX"
    assert v2mod._request_symbol({"limit": 5}) is None


def test_the_symbol_actually_reaches_build_freshness_through_a_real_request(
    monkeypatch: pytest.MonkeyPatch,
):
    """Asserting on _request_symbol alone is vacuous: the wrapper could stop
    passing its result and that test stays green, silently disabling futures
    grading everywhere. Spy on the real call instead — a query parameter and
    a path parameter must both arrive.
    """
    from fastapi import FastAPI

    seen: list = []
    real = v2mod.build_freshness

    def spy(payload, *, profile, now=None, symbol=None):
        seen.append(symbol)
        return real(payload, profile=profile, now=now, symbol=symbol)

    monkeypatch.setattr(v2mod, "build_freshness", spy)

    app = FastAPI()

    @app.get("/api/market/quote")
    async def quote(symbol: str = "SPY"):
        return {"symbol": symbol, "timestamp": "2026-08-20T17:00:00+00:00"}

    @app.get("/api/v1/levels/{symbol}")
    async def levels(symbol: str):
        return {"symbol": symbol, "timestamp": "2026-08-20T17:00:00+00:00"}

    v2mod.mount_v2(app)
    with TestClient(app) as c:
        assert c.get("/api/v2/market/quote?symbol=ES").status_code == 200
        assert c.get("/api/v2/levels/NQ").status_code == 200

    assert seen == ["ES", "NQ"], f"symbol did not reach build_freshness: {seen}"


# ---------------------------------------------------------------------------
# The guide is the contract document — it must not drift from the code
# ---------------------------------------------------------------------------


def _guide_cadence_rows():
    """(profile name, regular cell, extended cell) from API_Guide.md's table."""
    from pathlib import Path

    txt = Path("API_Guide.md").read_text()
    start = txt.index("| Profile | Endpoints |")
    block = txt[start : txt.index("\nA dash means", start)]
    rows = []
    for line in block.splitlines():
        if not line.startswith("| `"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        rows.append((cells[0].strip("`"), cells[2], cells[3]))
    assert rows, "cadence table not found — this guard is watching nothing"
    return rows


def _cell_seconds(cell):
    """Parse a table cell like '60 s' or '5 min'. None for a dash.

    A trailing parenthetical qualifier is allowed and ignored — option_chain
    reads "60 s (to 16:15 only)", where the figure is the cadence and the
    note is the window. The window itself is checked by the day sweep in
    test_feed_windows_across_a_day.py; this only pins the number.
    """
    import re

    cell = cell.strip()
    if cell in ("—", ""):
        return None
    m = re.match(r"([\d.]+)\s*(s|min)\b", cell)
    return None if m is None else float(m.group(1)) * (60 if m.group(2) == "min" else 1)


def test_the_guide_cadence_table_matches_the_profiles():
    """API_Guide.md is what an integrator reads and builds alert thresholds
    from — a published contract, not commentary. Retune a profile without
    touching the table and the number a customer works to is silently wrong,
    which is the same class of failure as the envelope itself disagreeing with
    the feed. Two of these numbers were quoted to an integrator by email.

    ``daily_cycle`` states its cadence as prose spanning both columns ("one
    per trading session") because it ages in sessions rather than seconds;
    that row is checked for the prose, not a figure.
    """
    for name, regular, extended in _guide_cadence_rows():
        profile = fr.CADENCE_PROFILES.get(name)
        assert profile is not None, f"the guide documents a profile the code does not have: {name}"

        if profile.session_scoped:
            assert "session" in regular.lower(), (
                f"{name} ages in trading sessions; its guide row should say so "
                f"rather than quoting {regular!r} in seconds"
            )
            continue

        assert _cell_seconds(regular) == profile.regular_seconds, (
            f"{name}: the guide advertises a regular cadence of {regular!r}, "
            f"the code publishes {profile.regular_seconds}"
        )
        assert _cell_seconds(extended) == profile.extended_seconds, (
            f"{name}: the guide advertises an extended cadence of {extended!r}, "
            f"the code publishes {profile.extended_seconds}"
        )


def test_every_profile_is_documented():
    """A profile absent from the table is one no integrator can plan around —
    which is how option_chain's 09:30-16:15 window would have gone unpublished
    while it changed the verdict on three endpoints for nine hours a day."""
    documented = {name for name, _, _ in _guide_cadence_rows()}
    assert not sorted(set(fr.CADENCE_PROFILES) - documented)
