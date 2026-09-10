"""The ``/api/v2`` surface — every v1 endpoint, wrapped in a freshness envelope.

v2 is not a rewrite.  It is v1's payloads, unchanged, inside a consistent
envelope::

    {"data": <exactly the v1 body>, "freshness": {...}}

so a consumer can reason about endpoint health, response evaluation time
and underlying data freshness as three separate machine-readable facts
instead of inferring them from whichever ad-hoc timestamp field an
endpoint happened to carry.  See ``src/api/freshness.py`` for the envelope
contract.

Why the surface is **generated** rather than hand-written
--------------------------------------------------------
There are ~135 endpoints.  Hand-authoring a v2 twin for each would
guarantee the two versions drift: every future v1 change would need a
matching v2 edit, and the "consistent envelope" promise would decay the
first time someone forgot one.  :func:`mount_v2` instead mirrors the
router table at startup — one implementation, and a v1 endpoint added
tomorrow gets its v2 twin for free.

The mirror is a real ``APIRoute``, not a middleware path-rewrite, so v2
appears in ``/openapi.json`` and ``/docs`` with a real
``Envelope[OriginalModel]`` schema, and keeps the original route's
dependencies — including its scope gate, so v2 authorises identically to
v1.

Serialization: ``data`` uses v1's own encoder
---------------------------------------------
``data`` must come out byte-for-byte identical to v1, which means it has to
go through *whichever* encoder v1 used for that route — and v1 uses two.
A route with a ``response_model`` is serialized by pydantic; a route
without one is serialized by ``jsonable_encoder``.  They disagree on
exactly the types asyncpg returns: ``Decimal`` is a JSON number under
``jsonable_encoder`` and a JSON *string* under pydantic, and a tz-aware
``datetime`` renders ``+00:00`` vs ``Z``.  49 of the 125 mirrored routes
(most of ``/api/signals/*`` and ``/api/technicals/*``) have no
``response_model``, so wrapping them in a validating ``Envelope[Any]``
silently turned every price and P&L into a string.

So the mirror branches: ``Envelope[Model]`` when v1 declared a model, and
``response_model=None`` when it did not — in which case FastAPI falls back
to ``jsonable_encoder`` for the whole envelope, which is v1's encoder for
``data`` and (because ``jsonable_encoder`` delegates to pydantic for a
``BaseModel``) still the pydantic form for ``freshness``.  The envelope
therefore stays byte-identical across all 125 routes while ``data`` tracks
its own v1 route exactly.

Deliberate v1 -> v2 differences
-------------------------------
* **Every declared field is always present.**  v1's
  ``/api/market/quote`` sets ``response_model_exclude_none``; v2 does not
  propagate it.  A consistent envelope means a consumer can index
  ``freshness.source_timestamp`` unconditionally rather than testing for
  the key, and that only holds if v2 never drops null-valued keys.
* **Non-JSON responses carry the envelope in headers.**  Two endpoints
  stream CSV downloads (backtest trades, TradeWorkz audit).  A JSON
  wrapper would corrupt the download, so those pass through untouched and
  the same freshness data is emitted as ``X-Freshness-*`` headers — which
  every v2 response carries anyway.
* **Path.**  The ``/api/v1`` prefix on the consolidated levels contract is
  replaced, not nested: ``/api/v1/levels/{symbol}`` mirrors to
  ``/api/v2/levels/{symbol}``, and unversioned ``/api/gex/summary``
  mirrors to ``/api/v2/gex/summary``.  One version segment, always in the
  same position.
"""

from __future__ import annotations

import functools
import inspect
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.concurrency import run_in_threadpool

from .freshness import (
    CadenceProfile,
    Envelope,
    Freshness,
    audit_cadence_coverage,
    build_freshness,
    resolve_profile,
)

logger = logging.getLogger(__name__)


def _typed_signature(fn: Callable[..., Any]) -> inspect.Signature:
    """``inspect.signature`` with string annotations resolved.

    Router modules use ``from __future__ import annotations``, so annotations
    arrive as strings and an identity check against a type never matches.
    FastAPI resolves them the same way when it builds the real dependant;
    ``eval_str`` is the stdlib equivalent. Falls back to the unresolved
    signature if a name cannot be resolved — the wrapper still works, it just
    injects its own Response as before.
    """
    try:
        return inspect.signature(fn, eval_str=True)
    except Exception:  # noqa: BLE001
        return inspect.signature(fn)


def _is_response_type(annotation: Any) -> bool:
    """True when a parameter is annotated with Response or a subclass."""
    return isinstance(annotation, type) and issubclass(annotation, Response)


V2_PREFIX = "/api/v2"

# Status codes whose responses must not carry a body (RFC 9110). FastAPI
# asserts this when a response_model is attached, and that assert fires at
# import time — see mount_v2.
_NO_BODY_STATUS = frozenset({204, 205, 304})


_CONTROL_PLANE_SEGMENTS = ("admin", "internal")


def _is_control_plane(path: str) -> bool:
    """True for operator-only and machine-to-machine control-plane routes.

    Three surfaces qualify: ``/api/admin/*`` (key management, the X-post
    publisher), ``/api/tradeworkz/admin/*`` (bot provisioning, test-event
    injection, simulation) and ``/api/tradeworkz/internal/*`` (the
    notification queue a systemd timer drains, including a state-mutating
    POST). They mutate state, no customer versions against them, and "how
    fresh is this data" is meaningless for all of them.

    Matched on whole path SEGMENTS rather than a prefix list, so a new admin
    or internal surface under some other router is excluded automatically —
    and so a legitimate path that merely contains the substring (an
    ``/api/x/administration`` say) is not.
    """
    segments = path.split("/")
    return any(seg in _CONTROL_PLANE_SEGMENTS for seg in segments)


def v2_path_for(v1_path: str) -> Optional[str]:
    """Map a v1 route path to its v2 path, or ``None`` if it is not mirrored.

    ``/api/v1/levels/{symbol}`` -> ``/api/v2/levels/{symbol}``
    ``/api/gex/summary``        -> ``/api/v2/gex/summary``
    """
    if not v1_path.startswith("/api/"):
        return None
    if v1_path.startswith(V2_PREFIX + "/") or _is_control_plane(v1_path):
        return None
    rest = v1_path[len("/api") :]
    # Collapse an existing version segment rather than nesting under it.
    if rest.startswith("/v1/"):
        rest = rest[len("/v1") :]
    return V2_PREFIX + rest


def _operation_id(v2_path: str, methods: List[str]) -> str:
    """A unique OpenAPI operationId for a mirrored route.

    Derived from the path, not the handler name: two different routers
    here both export a handler called ``get_levels``
    (``/api/v1/levels/{symbol}`` and ``/api/forced-flow/levels``), and
    naming v2 operations after the function would collide and make one of
    them unaddressable in generated clients.
    """
    slug = v2_path.strip("/").replace("/", "_").replace("{", "").replace("}", "")
    return f"{slug}_{'_'.join(m.lower() for m in methods)}"


def _freshness_headers(freshness: Freshness) -> Dict[str, str]:
    """Envelope fields as response headers.

    Present on every v2 response, JSON or not, so a proxy, uptime monitor
    or CDN can act on staleness without parsing a body it may not have
    buffered.
    """
    headers = {
        "X-Freshness-Status": freshness.freshness_status.value,
        "X-Freshness-Evaluated-At": freshness.evaluated_at.isoformat(),
        "X-Freshness-Cadence-Profile": freshness.cadence_profile,
        "X-Market-Session": freshness.market_session_status,
    }
    if freshness.source_timestamp is not None:
        headers["X-Freshness-Source-Timestamp"] = freshness.source_timestamp.isoformat()
    if freshness.age_seconds is not None:
        headers["X-Freshness-Age-Seconds"] = f"{freshness.age_seconds:g}"
    if freshness.stale_after is not None:
        headers["X-Freshness-Stale-After"] = freshness.stale_after.isoformat()
    if freshness.expected_update_cadence is not None:
        headers["X-Freshness-Expected-Cadence"] = freshness.expected_update_cadence
    return headers


_SYMBOL_PARAMS = ("symbol", "underlying", "ticker")


def _request_symbol(kwargs: Dict[str, Any]) -> Optional[str]:
    """The instrument this request is about, from the resolved handler args.

    Freshness is graded on a market calendar, and ES/NQ do not keep the NYSE
    one — they trade the CME session. Without the symbol the envelope reported
    ``session_closed`` for every hour CME trades and NYSE does not, hiding a
    dead overnight futures feed behind "no update was due".

    Covers both shapes the API uses: a query parameter (``?symbol=ES``) and a
    path parameter (``/api/v2/levels/ES``). Returns None when the endpoint is
    not symbol-scoped, which grades on the cash calendar as before.
    """
    for name in _SYMBOL_PARAMS:
        value = kwargs.get(name)
        if isinstance(value, str) and value:
            return value
    return None


def _build_signature(orig: Callable[..., Any]) -> Tuple[inspect.Signature, Optional[str]]:
    """Return the wrapper's signature and the name of an injected ``Response``.

    The wrapper must present the *same* parameters as the original so
    FastAPI resolves identical path/query params and dependencies, then
    hands them straight through.  It additionally needs the ``Response``
    object to set the freshness headers; when the original already asks
    for one we reuse it (injecting a second is harmless but pointless),
    otherwise we append our own and strip it back out before calling.

    The injected parameter is keyword-only so it can never disturb the
    original parameter order, and FastAPI invokes endpoints with keyword
    arguments throughout.
    """
    # Resolved annotations, not raw ones: every router module here uses
    # ``from __future__ import annotations``, so annotations arrive as STRINGS
    # and a raw ``is Response`` identity check can never match. That made this
    # branch dead — and worse, a future handler using the standard
    # ``response: Response`` idiom would have had its own Response parameter
    # left unfilled, 500ing on every v2 request while v1 stayed green.
    sig = _typed_signature(orig)
    params = list(sig.parameters.values())

    for p in params:
        # Subclasses (JSONResponse, ...) count too — the same test FastAPI
        # applies when deciding what to inject.
        if _is_response_type(p.annotation):
            return sig.replace(return_annotation=inspect.Signature.empty), None

    injected = "_freshness_response"
    # Collision-proof: a handler with a parameter of this name would otherwise
    # have it silently swallowed before the call.
    while any(p.name == injected for p in params):
        injected = "_" + injected
    params.append(inspect.Parameter(injected, inspect.Parameter.KEYWORD_ONLY, annotation=Response))
    return (
        inspect.Signature(params, return_annotation=inspect.Signature.empty),
        injected,
    )


def _make_v2_endpoint(orig: Callable[..., Any], profile: CadenceProfile) -> Callable[..., Any]:
    """Wrap a v1 handler so its result is returned inside the envelope."""
    signature, injected = _build_signature(orig)
    # Every handler on this tree is currently ``async def``, but FastAPI also
    # accepts plain ``def`` (which it runs in a threadpool). Deciding here
    # rather than assuming means a sync endpoint added later gets a working
    # v2 twin instead of one that raises "object is not awaitable" on its
    # first request — a failure the mirror would not surface until traffic
    # hit it.
    is_async = inspect.iscoroutinefunction(inspect.unwrap(orig))

    @functools.wraps(orig)
    async def v2_endpoint(**kwargs: Any) -> Any:
        response: Optional[Response] = kwargs[injected] if injected else _find_response(kwargs)
        if injected:
            kwargs.pop(injected)

        # HTTPException from the original propagates untouched: a v2 error
        # is a v1 error. Enveloping error bodies would mean every consumer
        # needed two error parsers, and a 404's "freshness" says nothing.
        result = await orig(**kwargs) if is_async else await run_in_threadpool(orig, **kwargs)

        if isinstance(result, Response):
            return _wrap_response_object(result, profile, symbol=_request_symbol(kwargs))

        freshness = build_freshness(result, profile=profile, symbol=_request_symbol(kwargs))
        if response is not None:
            response.headers.update(_freshness_headers(freshness))
        return {"data": result, "freshness": freshness}

    v2_endpoint.__signature__ = signature  # type: ignore[attr-defined]
    # functools.wraps copied the original's return annotation; leaving it in
    # place would have FastAPI infer a response model from it and fight the
    # Envelope[...] we pass explicitly.
    v2_endpoint.__annotations__ = {}
    return v2_endpoint


def _find_response(kwargs: Dict[str, Any]) -> Optional[Response]:
    for value in kwargs.values():
        if isinstance(value, Response):
            return value
    return None


def _wrap_response_object(
    result: Response, profile: CadenceProfile, symbol: Optional[str] = None
) -> Response:
    """Envelope a handler that returned a ``Response`` rather than a value.

    ``JSONResponse`` bodies are decoded and re-emitted inside the envelope
    so those endpoints keep the v2 contract.  Anything else (the two CSV
    download endpoints) is a payload a JSON wrapper would corrupt, so it
    passes through with the freshness data in headers only.
    """
    if isinstance(result, JSONResponse):
        try:
            payload = json.loads(bytes(result.body))
        except (ValueError, TypeError):
            logger.warning("v2: could not decode JSONResponse body; passing through")
            payload = None
        else:
            freshness = build_freshness(payload, profile=profile, symbol=symbol)
            enveloped = JSONResponse(
                content={"data": payload, "freshness": _jsonable_freshness(freshness)},
                status_code=result.status_code,
            )
            # Preserve the original's headers (minus the length, which the
            # new body invalidates), then add ours.
            for key, value in result.headers.items():
                if key.lower() != "content-length":
                    enveloped.headers[key] = value
            enveloped.headers.update(_freshness_headers(freshness))
            return enveloped

    freshness = build_freshness(None, profile=profile, symbol=symbol)
    result.headers.update(_freshness_headers(freshness))
    return result


def _jsonable_freshness(freshness: Freshness) -> Dict[str, Any]:
    decoded: Dict[str, Any] = json.loads(freshness.model_dump_json())
    return decoded


@dataclass(frozen=True)
class _RouteSpec:
    """One mirrorable v1 route, normalised across FastAPI's two route shapes."""

    path: str
    endpoint: Callable[..., Any]
    methods: List[str]
    response_model: Any
    status_code: Optional[int]
    name: str
    summary: Optional[str]
    description: Optional[str]
    tags: List[Any]
    dependencies: List[Any]
    deprecated: Optional[bool]
    include_in_schema: bool


def _iter_route_specs(app: FastAPI) -> List[_RouteSpec]:
    """Enumerate every v1 API route, however FastAPI happens to store it.

    FastAPI >=0.140 no longer flattens ``include_router`` into
    ``app.routes``; it appends one internal ``_IncludedRouter`` per call
    and exposes the real routes through ``effective_route_contexts()``.
    Both shapes are read here so the mirror covers the whole surface on
    either layout — on this tree that is the difference between mirroring
    31 endpoints and mirroring all ~134.

    Because the included-router shape depends on a FastAPI internal, the
    parity between v1 and v2 route counts is asserted in
    ``tests/test_api_v2_freshness_envelope.py``: a future upgrade that
    changes the layout fails CI loudly instead of quietly shrinking the
    published surface.
    """
    specs: List[_RouteSpec] = []

    def add(
        *,
        path,
        endpoint,
        methods,
        response_model,
        status_code,
        name,
        summary,
        description,
        tags,
        dependencies,
        deprecated,
        include_in_schema,
    ) -> None:
        specs.append(
            _RouteSpec(
                path=path,
                endpoint=endpoint,
                methods=sorted(methods or {"GET"}),
                response_model=response_model,
                status_code=status_code,
                name=name,
                summary=summary,
                description=description,
                tags=list(tags or []),
                dependencies=list(dependencies or []),
                deprecated=deprecated,
                include_in_schema=include_in_schema,
            )
        )

    for route in list(app.routes):
        if isinstance(route, APIRoute):
            add(
                path=route.path,
                endpoint=route.endpoint,
                methods=route.methods,
                response_model=route.response_model,
                status_code=route.status_code,
                name=route.name,
                summary=route.summary,
                description=route.description,
                tags=route.tags,
                dependencies=route.dependencies,
                deprecated=route.deprecated,
                include_in_schema=route.include_in_schema,
            )
            continue

        contexts = getattr(route, "effective_route_contexts", None)
        if contexts is None:
            # APIWebSocketRoute, Starlette Route (/docs, /openapi.json) and
            # friends. A socket has no response body to envelope; the
            # stream's own messages carry their stamps.
            continue
        try:
            for ctx in contexts():
                add(
                    path=ctx.path,
                    endpoint=ctx.endpoint,
                    methods=ctx.methods,
                    response_model=ctx.response_model,
                    status_code=ctx.status_code,
                    name=ctx.name,
                    summary=ctx.summary,
                    description=ctx.description,
                    tags=ctx.tags,
                    dependencies=ctx.dependencies,
                    deprecated=ctx.deprecated,
                    include_in_schema=ctx.include_in_schema,
                )
        except Exception:  # noqa: BLE001
            logger.error(
                "v2 mirror: could not enumerate included router %r; its "
                "endpoints will NOT be exposed under /api/v2",
                getattr(route, "original_router", route),
                exc_info=True,
            )

    return specs


def mount_v2(app: FastAPI) -> List[str]:
    """Mirror every eligible ``/api`` route onto ``/api/v2``.

    Call once, **after** every router has been included — the mirror reads
    the route table, so anything registered afterwards is not mirrored.

    Returns the v2 paths created, for logging and tests.
    """
    created: List[str] = []
    seen: Dict[Tuple[str, str], str] = {}
    mirrored_v1_paths: List[str] = []

    # Dependencies FastAPI already applies to every route from the app-level
    # ``dependencies=[...]``. They arrive pre-folded into each route's
    # dependency list, and add_api_route folds them in again — so copying
    # them through would run api_key_auth twice and, worse, charge the rate
    # limiter twice per v2 request. Filtered by identity: Depends() has no
    # value equality.
    global_dep_ids = {id(dep) for dep in app.router.dependencies}

    for route in _iter_route_specs(app):
        v2_path = v2_path_for(route.path)
        if v2_path is None:
            continue

        methods = route.methods
        collision = next((m for m in methods if (v2_path, m) in seen), None)
        if collision is not None:
            # Two v1 paths collapsing to one v2 path would silently shadow
            # one of them. Skip and shout rather than serve the wrong body.
            logger.error(
                "v2 mirror: %s %s collides with %s (from %s); skipping",
                collision,
                v2_path,
                seen[(v2_path, collision)],
                route.path,
            )
            continue

        # A 204/205/304 response must not carry a body, and FastAPI asserts
        # that when a response_model is attached. Mirroring such a route would
        # raise at IMPORT time and stop uvicorn from booting v1 as well — so
        # the envelope is simply not applied to them. There are none today;
        # this exists so that adding the REST-conventional
        # `@router.delete(..., status_code=204)` is not an outage.
        if route.status_code in _NO_BODY_STATUS:
            logger.warning(
                "v2 mirror: %s returns %s (no response body permitted); " "not mirrored to %s",
                route.path,
                route.status_code,
                v2_path,
            )
            continue

        profile = resolve_profile(route.path)
        response_model = route.response_model
        # mypy cannot type a subscript whose parameter is a runtime value;
        # the route table hands us the model as data, which is the whole
        # point of generating the surface rather than declaring it.
        #
        # response_model is left None when v1 had none, so FastAPI keeps
        # v1's jsonable_encoder path for `data` (see "Serialization" above).
        # The envelope shape is still published to OpenAPI via `responses`,
        # so /docs documents these routes without pydantic touching the wire.
        envelope_model = (
            Envelope[response_model]  # type: ignore[valid-type]
            if response_model is not None
            else None
        )
        extra_responses: Dict[Any, Dict[str, Any]] = (
            {}
            if response_model is not None
            else {route.status_code or 200: {"model": Envelope[Any]}}
        )

        app.add_api_route(
            v2_path,
            _make_v2_endpoint(route.endpoint, profile),
            methods=methods,
            response_model=envelope_model,
            responses=extra_responses,
            status_code=route.status_code,
            name=_operation_id(v2_path, methods),
            summary=route.summary,
            description=route.description,
            # Suffix the tags so /docs renders a clean "GEX" / "GEX (v2)"
            # split instead of interleaving both versions of every endpoint
            # in one group. tagsSorter=alpha keeps each pair adjacent.
            tags=[f"{tag} (v2)" for tag in route.tags] or ["v2"],
            dependencies=[d for d in route.dependencies if id(d) not in global_dep_ids],
            deprecated=route.deprecated,
            include_in_schema=route.include_in_schema,
            operation_id=_operation_id(v2_path, methods),
        )

        for method in methods:
            seen[(v2_path, method)] = route.path
        created.append(v2_path)
        mirrored_v1_paths.append(route.path)

    # Keep the no-auth probes reachable on v2. Registered here rather than
    # imported into security.py so the dependency runs v2 -> security: if this
    # module ever fails to import, auth is untouched and v1 keeps serving.
    from .security import public_paths, register_public_path

    for public in public_paths():
        twin = v2_path_for(public)
        if twin is not None:
            register_public_path(twin)

    uncovered = audit_cadence_coverage(mirrored_v1_paths)
    if uncovered:
        # Not fatal — an unregistered path still gets the safe ON_DEMAND
        # default — but it means that endpoint advertises no cadence, so
        # it should be added to ENDPOINT_CADENCE.
        logger.warning(
            "v2 mirror: %d endpoint(s) have no cadence registry entry and "
            "will report freshness_status=unknown: %s",
            len(uncovered),
            ", ".join(sorted(set(uncovered))),
        )

    logger.info("v2 mirror: exposed %d endpoints under %s", len(created), V2_PREFIX)
    return created
