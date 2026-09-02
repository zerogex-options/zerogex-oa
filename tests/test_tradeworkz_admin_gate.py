"""The TradeWorkz ``/admin/*`` surface must need the admin token, not a scope.

These routes operate the whole bot fleet — provision, capital changes,
reset-fleet, simulate/clear, inject-test-event — and read fleet-wide
positions and trades. They shipped behind ``require_scopes(SIGNALS)`` alone,
which is not an admin gate:

  * ``signals`` is an ordinary customer scope, bundled into ``TIER_SIGNALS``
    right next to gex/flow/technicals.
  * ``require_scopes`` is deliberately lenient. It grants a wildcard ``*``
    key, grants everyone when ``API_SCOPE_ENFORCEMENT`` is off, and grants
    the static break-glass key outright (``info is None``).

So any of those reached ``POST /api/tradeworkz/admin/reset-fleet``. The gate
is now ``require_admin`` (a matching ``X-Admin-Token``, fail-closed 503 when
unprovisioned) with the scope kept underneath as defence in depth.

The first test is the one that matters long-term: it walks the router's real
route table, so an ``/admin`` route added later cannot quietly ship on the
data-plane scope. It asserts against the mounted app's dependency graph
rather than the source text, so renaming or restructuring the gate keeps it
honest.
"""

from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from src.api import security
from src.api.routers.tradeworkz import router as tradeworkz_router
from src.api.security import require_admin


def _dependency_callables(route) -> set:
    """Every dependency callable reachable from a route, flattened."""
    found = set()
    stack = list(getattr(route.dependant, "dependencies", []))
    while stack:
        dep = stack.pop()
        if dep.call is not None:
            found.add(dep.call)
        stack.extend(dep.dependencies)
    return found


def _admin_routes():
    for route in tradeworkz_router.routes:
        path = getattr(route, "path", "")
        if path.startswith("/api/tradeworkz/admin") or path.startswith("/admin"):
            yield route


def test_every_admin_route_requires_the_admin_token():
    routes = list(_admin_routes())
    assert routes, "no /admin routes found — has the router moved?"

    unguarded = [
        f"{sorted(r.methods)} {r.path}"
        for r in routes
        if require_admin not in _dependency_callables(r)
    ]
    assert not unguarded, (
        "these /admin routes do not require the admin token and are reachable "
        f"by any key the lenient scope check grants: {unguarded}"
    )


def test_the_destructive_routes_are_covered():
    """Names the operations whose exposure prompted this, so a refactor that
    drops one from the router is visible rather than silently 'passing'."""
    paths = {r.path for r in _admin_routes()}
    for expected in (
        "/api/tradeworkz/admin/reset-fleet",
        "/api/tradeworkz/admin/provision",
        "/api/tradeworkz/admin/bots/{bot_id}/capital",
        "/api/tradeworkz/admin/simulate/clear",
        "/api/tradeworkz/admin/inject-test-event",
        "/api/tradeworkz/admin/positions",
        "/api/tradeworkz/admin/trades",
    ):
        assert expected in paths, f"{expected} is gone from the router"


def test_internal_routes_are_not_admin_gated():
    """``/internal/*`` is called by the notification job, not an operator.

    Pinned because the two surfaces sit in one router and are easy to sweep
    together — doing so would break the notification pipeline, which has no
    admin token to send.
    """
    internal = [
        r
        for r in tradeworkz_router.routes
        if getattr(r, "path", "").startswith("/api/tradeworkz/internal")
    ]
    assert internal, "no /internal routes found — has the router moved?"
    for route in internal:
        assert require_admin not in _dependency_callables(route), (
            f"{route.path} was swept into the admin gate; the notification "
            "job cannot send an admin token"
        )


class TestGateBehaviour:
    """End-to-end through the real dependency, with auth stubbed out.

    ``require_admin`` layers on top of ``api_key_auth``; these pin what the
    admin gate itself does once a caller is past that.
    """

    @staticmethod
    def _client(monkeypatch, admin_token):
        monkeypatch.setattr(security, "_KEY_ADMIN_TOKEN", admin_token)
        app = FastAPI()

        @app.get("/probe", dependencies=[Depends(require_admin)])
        async def probe():
            return {"ok": True}

        return TestClient(app)

    def test_missing_token_is_forbidden(self, monkeypatch):
        client = self._client(monkeypatch, "s3cret")
        assert client.get("/probe").status_code == 403

    def test_wrong_token_is_forbidden(self, monkeypatch):
        client = self._client(monkeypatch, "s3cret")
        assert client.get("/probe", headers={"X-Admin-Token": "nope"}).status_code == 403

    def test_correct_token_passes(self, monkeypatch):
        client = self._client(monkeypatch, "s3cret")
        assert client.get("/probe", headers={"X-Admin-Token": "s3cret"}).status_code == 200

    def test_unprovisioned_secret_fails_closed(self, monkeypatch):
        """A deploy that never set KEY_ADMIN_TOKEN must not fall open."""
        client = self._client(monkeypatch, None)
        assert client.get("/probe", headers={"X-Admin-Token": "anything"}).status_code == 503
