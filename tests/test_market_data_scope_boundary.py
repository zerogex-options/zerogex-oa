"""The MARKET_RAW boundary, asserted against the mounted route table.

``/api/market/open-interest`` shipped gated on ``MARKET_RAW`` while
returning no quoted price at all — ``open_interest`` plus a derived
``exposure``. It was the only quoteless route in that bundle, and the
identical per-strike OI was already served on ``GEX`` by
``/api/gex/by-strike`` and ``/api/gex/strike-profile-timeseries``. So the
gate withheld nothing and 403'd paying integrations that asked for the
data by its own name (reported by a customer on 2026-09-03, after the same
misclassification contributed to the 2026-08-31 enforcement incident).

These tests exist because reading ``main.py`` is not enough to catch that.
A route inherits its gate from ``include_router``, moves between routers,
and is declared far from the model it returns — the three facts you need
are in three files. So the checks below walk the REAL mounted app, read
each route's required scopes off the dependency graph (via the
``required_scopes`` attribute ``require_scopes`` attaches), and inspect the
response model's fields recursively.

The invariant is deliberately narrow and mechanical: **a payload carrying a
quoted price must require MARKET_RAW.** That is what the scope can defend
and test. It is not a claim about what is licensable — see the scopes.py
docstring and ``docs/compliance/market-data-licensing-audit-2026-09-02.md``
(F5) for the question that is still open.
"""

from __future__ import annotations

import typing
from typing import Any, Set

import pytest
from fastapi.routing import APIRoute
from pydantic import BaseModel

from src.api import scopes
from src.api.main import app

# Field names that ARE a quoted price. A payload exposing any of these is
# handing over the vendor's quote, whatever the endpoint is called.
QUOTE_FIELDS = frozenset({"bid", "ask", "last", "mid", "bid_price", "ask_price"})


class _Endpoint(typing.NamedTuple):
    path: str
    methods: typing.Tuple[str, ...]
    response_model: Any
    required_scopes: frozenset


def _scopes_from_dependant(route: Any) -> Set[str]:
    """Walk a mounted route's resolved dependency graph."""
    required: Set[str] = set()
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return required
    stack = list(dependant.dependencies)
    while stack:
        dep = stack.pop()
        if dep.call is not None:
            required |= set(getattr(dep.call, "required_scopes", frozenset()))
        stack.extend(dep.dependencies)
    return required


def _scopes_from_depends(dependencies) -> Set[str]:
    """Read scopes off a list of unresolved ``Depends`` markers."""
    required: Set[str] = set()
    for dep in dependencies or []:
        call = getattr(dep, "dependency", None)
        required |= set(getattr(call, "required_scopes", frozenset()))
    return required


def _iter_api_routes():
    """Every mounted API route, however FastAPI happens to store it.

    FastAPI >=0.140 stops flattening ``include_router`` into ``app.routes``
    — it appends one internal wrapper per call and exposes the real routes
    through ``effective_route_contexts()``. Reading only ``app.routes``
    therefore sees a handful of ``@app.get`` routes and misses every
    router-mounted endpoint, which on this tree is most of the surface
    (including ``/api/option/contract`` — one of the two things MARKET_RAW
    exists for). ``src/api/v2.py:_iter_route_specs`` solves the same
    problem for the v2 mirror and is the reference for this walk.

    The v2 twins are deliberately walked too: they inherit v1's
    dependencies, so covering them proves the mirror carries the gate
    rather than publishing an ungated copy of the same payload.
    """
    for route in list(app.routes):
        if isinstance(route, APIRoute):
            yield _Endpoint(
                path=route.path,
                methods=tuple(sorted(route.methods or ())),
                response_model=route.response_model,
                required_scopes=frozenset(_scopes_from_dependant(route)),
            )
            continue
        contexts = getattr(route, "effective_route_contexts", None)
        if contexts is None:
            continue  # websockets, /docs, /openapi.json
        for ctx in contexts():
            yield _Endpoint(
                path=ctx.path,
                methods=tuple(sorted(ctx.methods or ())),
                response_model=ctx.response_model,
                required_scopes=frozenset(_scopes_from_depends(ctx.dependencies)),
            )


def _model_fields(annotation: Any, _seen: Set[Any] | None = None) -> Set[str]:
    """Every field name reachable from a response annotation, recursively.

    Unwraps ``List[Model]``, ``Optional[Model]`` and nested models, so a
    quote field buried in ``Response.contracts[].bid`` is still found —
    which is exactly the shape ``OpenInterestResponse`` uses.
    """
    seen = _seen if _seen is not None else set()
    found: Set[str] = set()
    if annotation is None or annotation in seen:
        return found
    seen.add(annotation)

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        for name, field in annotation.model_fields.items():
            found.add(name)
            found |= _model_fields(field.annotation, seen)
        return found

    for arg in typing.get_args(annotation):
        found |= _model_fields(arg, seen)
    return found


def _data_routes():
    return [e for e in _iter_api_routes() if e.response_model is not None]


def _by_path(path: str) -> _Endpoint:
    match = next((e for e in _iter_api_routes() if e.path == path), None)
    assert match is not None, f"{path} is not mounted — has it moved or been pulled?"
    return match


def test_the_route_table_is_actually_introspectable():
    """Guard the guard: if `required_scopes` ever stops being attached, every
    assertion below would pass vacuously."""
    gated = [e for e in _data_routes() if e.required_scopes]
    assert gated, (
        "no route reported any required scope — require_scopes stopped "
        "exposing `required_scopes`, and this whole suite is now vacuous"
    )


def test_no_quote_bearing_payload_escapes_market_raw():
    """The invariant. A response carrying bid/ask/last/mid needs MARKET_RAW."""
    leaks = []
    for endpoint in _data_routes():
        fields = _model_fields(endpoint.response_model)
        quoted = fields & QUOTE_FIELDS
        if not quoted:
            continue
        if scopes.MARKET_RAW not in endpoint.required_scopes:
            leaks.append(f"{list(endpoint.methods)} {endpoint.path} exposes {sorted(quoted)}")
    assert not leaks, (
        "these routes return a quoted option price without requiring "
        f"MARKET_RAW, so any analytics-tier key can read it: {leaks}"
    )


@pytest.mark.parametrize("path", ["/api/market/open-interest", "/api/v2/market/open-interest"])
def test_open_interest_is_reachable_on_the_analytics_bundle(path):
    """The fix. OI is dealer-positioning input, not a quote.

    Pinned against the tier BUNDLES rather than the scope name, because what
    broke the customer was a bundle that could not reach the endpoint — the
    scope it happened to be called was incidental to that. The v2 twin is
    covered too: a caller told to migrate must not walk into the same 403.
    """
    endpoint = _by_path(path)
    required = endpoint.required_scopes

    # Ordered so the first failure names the consequence, not the mechanism:
    # what the customer hit was "my tier cannot reach this endpoint".
    for tier in (scopes.TIER_ANALYTICS, scopes.TIER_SIGNALS):
        assert required <= scopes.TIERS[tier], (
            f"{path} requires {sorted(required)}, which the {tier!r} tier "
            f"cannot satisfy (it grants {sorted(scopes.TIERS[tier])}) — "
            "an external key gets 403 here again"
        )
    assert scopes.MARKET_RAW not in required, (
        f"{path} is back under MARKET_RAW; it returns no quoted price, and "
        "the same per-strike OI is served on GEX by /api/gex/by-strike"
    )

    fields = _model_fields(endpoint.response_model)
    assert "open_interest" in fields
    assert not (fields & QUOTE_FIELDS), (
        "open-interest started returning a quoted price; it belongs back under " "MARKET_RAW if so"
    )


@pytest.mark.parametrize(
    "path",
    ["/api/option/quote", "/api/option/contract", "/api/tools/option-calculator"],
)
def test_the_quote_surfaces_still_require_market_raw(path):
    """Regression pin on what MARKET_RAW is actually for. If a later change
    empties the scope of every route, the invariant above passes trivially."""
    assert (
        scopes.MARKET_RAW in _by_path(path).required_scopes
    ), f"{path} returns per-contract prices and must require MARKET_RAW"


def test_premium_surface_classification_is_recorded_not_silently_passing():
    """A knowingly unresolved case, pinned so it cannot drift unnoticed.

    ``/api/gex/premium_surface`` rides GEX and returns ``premium``, its own
    model documenting it as "quoted premium used (mid, or last as
    fallback)" — a quoted price under a name QUOTE_FIELDS does not match.
    This test asserts the CURRENT state, not that it is correct. It belongs
    to the open question in the scopes.py docstring; if that question is
    answered, change this test deliberately rather than deleting it.
    """
    endpoint = _by_path("/api/gex/premium_surface")
    fields = _model_fields(endpoint.response_model)
    assert "premium" in fields, "premium_surface stopped returning `premium` — re-read this test"
    assert scopes.MARKET_RAW not in endpoint.required_scopes, (
        "premium_surface now requires MARKET_RAW — if that was deliberate, "
        "update the open question in scopes.py and this test together"
    )
