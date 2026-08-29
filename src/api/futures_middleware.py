"""Serve ES / NQ by projecting the SPX / NDX surfaces onto the futures axis.

A request for ``symbol=ES`` is answered by running the ordinary ``SPX``
handler and carrying its price-space fields across on the way out.  Doing
this once, in middleware, is what makes ES / NQ work across the *whole* API
— including the routers (``/api/v1/levels``, forced flow, trade bias) —
instead of requiring each of seventy-odd symbol-taking endpoints to grow its
own futures branch and drift out of step with the others.

Every request falls into exactly one of four buckets:

1. **Not a future** — passed straight through, untouched.  This is almost
   every request, and it must cost nothing: hence pure ASGI (below).
2. **Served natively** (:data:`_NATIVE_PATHS`) — the handler already reads
   the future's own bars out of ``futures_quotes``.  The symbol is NOT
   rewritten and the response is NOT projected: these endpoints carry
   OBSERVED prices, and projecting a frozen 16:00 SPX print would report
   where ES stood at the bell rather than where it is now.
3. **Projected** (:data:`_PROJECTABLE_PREFIXES`) — an endpoint whose payload
   has been audited field by field: rewrite the symbol inbound, project the
   allowlisted price fields outbound, relabel, substitute the observed futures
   print for spot, reconcile the spot-derived deltas, and attach a
   ``projection`` block recording the ratio used.
4. **Everything else** — answered 400.  Projecting an unaudited payload is
   how a cash-index number ends up drawn on a futures chart with nothing to
   mark it, so the axis is chosen per route and never guessed.

Why pure ASGI rather than ``BaseHTTPMiddleware``: the latter wraps every
request in an anyio task group and buffers through a stream even when the
middleware does nothing, which changes exception propagation and streaming
behaviour for the entire application.  Every other middleware in this app is
pure ASGI (see :mod:`src.api.middleware`); a futures feature has no business
altering how SPX requests are served.  Here, a non-futures request costs one
dict lookup and one string compare before ``self.app`` is called directly.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, time, timezone
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode
from zoneinfo import ZoneInfo

from src.jobs.futures_projection import (
    SPOT_FIELDS,
    project_payload,
    projection_metadata,
    projection_tick,
    resolve_basis,
)
from src.symbols import is_futures_symbol, resolve_futures_index

logger = logging.getLogger("zerogex.futures_middleware")

# Query parameters that name the underlying across the API surface.
_SYMBOL_PARAMS = ("symbol", "underlying")

# Endpoints that resolve futures natively from ``futures_quotes``.  Their
# prices are OBSERVED, so they must not be rewritten or projected.
_NATIVE_PATHS = frozenset(
    {
        "/api/market/quote",
        "/api/market/historical",
        "/api/market/session-closes",
        "/api/market/session-levels",
    }
)

# Endpoints whose payloads have been AUDITED field-by-field and are safe to
# project.  This is an allowlist, and it is the single most important safety
# property of the feature.
#
# The original design allowlisted FIELDS and projected any endpoint.  An audit
# showed that cannot be made safe: price values also live in endpoints with no
# response_model at all (raw dicts, so no schema to check), arrive as JSON
# strings on models that declare Decimal without json_encoders, and — on the
# signal and forecast cards — are embedded in free-text ``rationale`` prose,
# which no projector can rewrite.  A missed field renders a cash-index number
# on a futures chart with nothing to indicate it.
#
# So the axis is chosen per ROUTE.  Anything not listed here answers 400 for a
# futures symbol rather than guessing.  To add an endpoint: read its response
# model (or its raw dict), classify every numeric field, extend PRICE_FIELDS /
# NEVER_PROJECT, then add the prefix.
_PROJECTABLE_PREFIXES = (
    "/api/gex/",  # summary, profile, by-strike, heatmap, flip surfaces, regime, surfaces
    "/api/v1/levels",
    "/api/technicals",
    "/api/max-pain/",
    "/api/forced-flow/",
    "/api/signals/",
    "/api/forecast",
    "/api/scorecard/",
    "/api/replay/",
    "/api/flow/buying-pressure",
    "/api/flow/series",
    "/api/flow/market-tide",
)

# PER-CONTRACT surfaces, checked BEFORE the allowlist above so a broader
# prefix cannot accidentally admit one.  These enumerate individual option
# contracts, and an SPX contract with its strike multiplied by the basis is
# not a contract anyone can trade — the strike would no longer round-trip to
# the chain it came from.  There is no ES chain to substitute, so they refuse.
_UNSUPPORTED_PREFIXES = (
    "/api/option/",
    "/api/tools/option-calculator",
    "/api/flow/by-contract",
    "/api/flow/contracts",
    "/api/flow/smart-money",
    "/api/market/open-interest",
    # Option-premium and IV surfaces: their value axis is a premium/IV on a
    # real SPX contract, not an index level, so the axis does not carry.
    "/api/gex/premium_surface",
    "/api/gex/vol_surface",
)


def _unversioned(path: str) -> str:
    """Drop a leading ``/api/v<N>`` segment so one table covers every version.

    The API serves the same endpoints under ``/api/gex/summary`` (v1) and
    ``/api/v2/gex/summary`` (v2, the freshness-envelope surface). Classifying
    on the raw path meant a v2 request matched neither the native carve-out
    nor the projectable list, so EVERY ES/NQ call to /api/v2/* was refused
    with "this endpoint has not been audited" — while the identical v1 call
    answered 200. Normalising both the request path and the tables below
    keeps one audited list per endpoint rather than one per version, and
    covers a future /api/v3 the day it ships.

    Only classification is normalised. ``scope["path"]`` keeps its version,
    so the request still routes to the handler the caller asked for.
    """
    return _API_VERSION_RE.sub("/api", path)


_API_VERSION_RE = re.compile(r"^/api/v\d+(?=/|$)")

# Canonical (version-stripped) forms of the two tables above. ``/api/v1/levels``
# canonicalises to ``/api/levels``, which is what ``/api/v2/levels/ES`` reduces
# to as well — the whole point of comparing on this axis.
_NATIVE_PATHS_CANON = frozenset(_unversioned(p) for p in _NATIVE_PATHS)
_PROJECTABLE_PREFIXES_CANON = tuple(_unversioned(p) for p in _PROJECTABLE_PREFIXES)
_UNSUPPORTED_PREFIXES_CANON = tuple(_unversioned(p) for p in _UNSUPPORTED_PREFIXES)

# Responses larger than this are projected anyway but logged: the walk is
# O(payload) on the event loop, and a very large one is worth knowing about.
_LARGE_BODY_BYTES = 4 * 1024 * 1024


def _is_v2_envelope(payload: Any) -> bool:
    """True for a ``{"data": ..., "freshness": {...}}`` v2 response body."""
    return (
        isinstance(payload, dict)
        and set(payload) == {"data", "freshness"}
        and isinstance(payload.get("freshness"), dict)
    )


def _scope_query(scope: dict) -> list[tuple[str, str]]:
    return parse_qsl(scope.get("query_string", b"").decode("latin-1"), keep_blank_values=True)


ET = ZoneInfo("America/New_York")

# A bare trading day, e.g. the ``date`` a replay session is addressed by.
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# A UTC-offset whose ``+`` was eaten by query-string decoding: " 00:00".
_SPACE_OFFSET_RE = re.compile(r"\s(\d{2}:\d{2})$")

# Query parameters that pin a request to a PAST instant, most specific first.
# A request carrying one of these is asking "what did this look like then",
# and its basis must be the basis that stood then — see ``_request_asof``.
# ``start_date`` is deliberately absent: on a range request it is the FAR end,
# and anchoring a whole series to its oldest point is as wrong as anchoring it
# to now, just in the other direction. Range payloads are handled per-row by
# ``_asof_for_row`` instead.
_ASOF_PARAMS = ("ts", "end_date", "session_date", "date")


def _parse_asof(raw: str) -> Optional[datetime]:
    """Parse an as-of query value into an aware UTC datetime, or None.

    Accepts both an instant (``2026-05-14T18:30:00Z``) and a bare trading day
    (``2026-05-14``). A bare day resolves to its CASH CLOSE rather than
    midnight: a session's frames all sit inside 09:30-16:00 ET, and midnight ET
    precedes every one of them, so anchoring there would push the basis read a
    full day back and miss the session entirely on a Monday.
    """
    text = (raw or "").strip()
    if not text:
        return None
    # ``+`` is a space in a query string, so a caller who sends a literal
    # ``+00:00`` offset instead of ``%2B00:00`` arrives here with " 00:00".
    # Clients get this wrong constantly, and the failure mode is the one this
    # whole function exists to prevent: an unparsed anchor falls back to live
    # and silently projects a past frame on today's basis. Put the sign back.
    text = _SPACE_OFFSET_RE.sub(r"+\1", text)
    try:
        if _DATE_ONLY_RE.match(text):
            day = date.fromisoformat(text)
            return datetime.combine(day, time(16, 0), tzinfo=ET).astimezone(timezone.utc)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# Fields a time-series row carries its own instant in, most specific first.
_ROW_TS_FIELDS = ("timestamp", "bucket_ts", "frame_ts", "ts", "date")


def _row_anchor(row: Any) -> Optional[datetime]:
    """The basis anchor for one row of a series, or None if it carries no time.

    Rounded to the row's SESSION rather than its own minute, for two reasons.
    One basis per trading day keeps a 90-day series to 90 cached basis reads
    instead of one per minute. And the precision lost is below the tick a level
    is published at: basis moves ~0.5% across a whole quarterly cycle, so
    within one session it moves ~0.003% — about 0.2 points on ES, which rounds
    away against the 0.25 tick. The months-scale drift this exists to remove is
    two orders of magnitude larger.
    """
    if not isinstance(row, dict):
        return None
    for key in _ROW_TS_FIELDS:
        raw = row.get(key)
        if not isinstance(raw, str):
            continue
        parsed = _parse_asof(raw)
        if parsed is not None:
            session = parsed.astimezone(ET).date()
            return datetime.combine(session, time(16, 0), tzinfo=ET).astimezone(timezone.utc)
    return None


def _request_asof(scope: dict) -> Optional[datetime]:
    """The instant this request is asking about, or None for "now".

    Read from the query string BEFORE the scope is rewritten — the rewrite
    only touches symbol parameters, but reading first keeps the two concerns
    from having to know about each other.

    A future-dated anchor is treated as live: a caller asking for today's
    session mid-session should get the current basis, not an empty window.
    """
    now = datetime.now(timezone.utc)
    query = _scope_query(scope)
    for param in _ASOF_PARAMS:
        for key, value in query:
            if key != param:
                continue
            parsed = _parse_asof(value)
            if parsed is not None:
                return None if parsed >= now else parsed
    return None


def _futures_target(scope: dict) -> Optional[tuple[str, str]]:
    """Return ``(futures_symbol, index_symbol)`` this request asks for.

    ``None`` when the request names no first-class future — the overwhelmingly
    common case, and the one that must stay free.
    """
    for key, value in _scope_query(scope):
        if key in _SYMBOL_PARAMS and value and is_futures_symbol(value):
            index = resolve_futures_index(value)
            if index:
                return value.strip().upper(), index

    # Path-addressed symbols, e.g. /api/v1/levels/ES. Only the final segment
    # is considered, so an unrelated path can never be rewritten by accident.
    tail = scope.get("path", "").rstrip("/").rsplit("/", 1)[-1]
    if tail and is_futures_symbol(tail):
        index = resolve_futures_index(tail)
        if index:
            return tail.strip().upper(), index
    return None


def _rewrite_scope(scope: dict, futures_symbol: str, index_symbol: str) -> None:
    """Point the request at the backing index, in place.

    The pre-rewrite path and the symbol the caller actually asked for are
    stashed on the scope so the audit log can report what was requested rather
    than what was routed — an ES request attributed to SPX in the audit trail
    is a small lie that would be very annoying to debug later.
    """
    scope["zerogex_requested_symbol"] = futures_symbol
    scope["zerogex_original_path"] = scope.get("path", "")

    query = _scope_query(scope)
    if query:
        scope["query_string"] = urlencode(
            [
                (key, index_symbol if key in _SYMBOL_PARAMS and is_futures_symbol(value) else value)
                for key, value in query
            ]
        ).encode("latin-1")

    path = scope.get("path", "")
    stripped = path.rstrip("/")
    if stripped.rsplit("/", 1)[-1].upper() == futures_symbol:
        head = stripped.rsplit("/", 1)[0]
        scope["path"] = f"{head}/{index_symbol}" + ("/" if path.endswith("/") else "")
        scope["raw_path"] = scope["path"].encode("latin-1")


def _header(headers: list, name: bytes) -> bytes:
    for key, value in headers:
        if key.lower() == name:
            return value
    return b""


async def _live_futures_spot(index_symbol: str) -> Optional[float]:
    """Latest OBSERVED futures close for ``index_symbol``, if the feed has one.

    Passes the same cash-close anchor the quote endpoint uses so both callers
    hit the same cache entry with the same meaning.
    """
    try:
        from src.api.main import db_manager
        from src.market_calendar import current_cash_close_reference

        if db_manager is None:
            return None
        quote = await db_manager.get_latest_future_quote(
            index_symbol, current_cash_close_reference()
        )
        if quote and quote.get("close") is not None:
            return float(quote["close"])
    except Exception as e:
        logger.debug("live futures spot unavailable for %s: %s", index_symbol, e)
    return None


def _db_manager():
    """The live DatabaseManager, or None before startup."""
    from src.api.main import db_manager

    return db_manager


# Deltas the analytics engine computed against the CASH spot. Once the observed
# futures print is substituted for spot they describe the wrong reference, so
# they are re-derived from the values now in the payload. Each entry is
# (target, minuend, subtrahend); all three must be siblings for it to apply.
_SPOT_DERIVED = (
    ("difference", "max_pain", "underlying_price"),
    ("difference_from_underlying", "max_pain", "underlying_price"),
    ("distance_from_spot", "strike", "spot_price"),
)


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _reconcile_spot_derived(payload: Any, spot: float) -> None:
    """Re-derive spot-relative deltas in place, after the spot substitution.

    Without this, ``difference`` still says how far max pain sat from the
    FROZEN cash close while ``underlying_price`` beside it reads the live
    future — two numbers in one payload that no longer subtract to each other.
    """
    if isinstance(payload, dict):
        for target, minuend, subtrahend in _SPOT_DERIVED:
            if target not in payload:
                continue
            left = _as_number(payload.get(minuend))
            right = _as_number(payload.get(subtrahend))
            if right is None and subtrahend in ("underlying_price", "spot_price"):
                right = spot
            if left is None or right is None:
                continue
            payload[target] = (
                str(left - right) if isinstance(payload[target], str) else left - right
            )
        for value in payload.values():
            if isinstance(value, (dict, list)):
                _reconcile_spot_derived(value, spot)
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, (dict, list)):
                _reconcile_spot_derived(item, spot)


class FuturesProjectionMiddleware:
    """Answer ES / NQ requests from the SPX / NDX surfaces (pure ASGI)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        target = _futures_target(scope)
        if target is None:
            # The hot path: no future named, nothing to do, no wrapping.
            await self.app(scope, receive, send)
            return

        futures_symbol, index_symbol = target

        # Normalise the trailing slash before deciding anything. Starlette
        # would otherwise 307 to the REWRITTEN scope, handing the caller a
        # Location of ?symbol=SPX — un-projected cash levels under an ES
        # request, which is the one outcome this whole design exists to
        # prevent. It also made the exact-match native carve-out below miss
        # "/api/market/quote/" entirely.
        path = scope.get("path", "").rstrip("/") or "/"
        if path != scope.get("path"):
            scope["path"] = path
            scope["raw_path"] = path.encode("latin-1")

        # Classify on the version-stripped path so /api/v2/* is graded by the
        # same audited tables as its v1 twin (see _unversioned).
        canon = _unversioned(path)

        if canon in _NATIVE_PATHS_CANON:
            await self.app(scope, receive, send)
            return

        if canon.startswith(_UNSUPPORTED_PREFIXES_CANON) or not canon.startswith(
            _PROJECTABLE_PREFIXES_CANON
        ):
            await self._reject(send, futures_symbol, index_symbol, path)
            return

        # Read the as-of BEFORE the rewrite, while the scope still holds the
        # query exactly as the caller sent it.
        asof = _request_asof(scope)

        _rewrite_scope(scope, futures_symbol, index_symbol)
        await self._project_response(
            scope, receive, send, futures_symbol, index_symbol, asof
        )

    async def _reject(self, send, futures_symbol: str, index_symbol: str, path: str) -> None:
        """Refuse rather than guess an axis for an unaudited endpoint."""
        body = json.dumps(
            {
                "detail": (
                    f"{path} is not available for {futures_symbol}. "
                    f"{futures_symbol} carries no option chain of its own — its levels are "
                    f"{index_symbol} option-derived — and this endpoint has not been audited "
                    f"for projection onto the futures price axis. Request it as "
                    f"{index_symbol}."
                )
            }
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("latin-1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def _project_response(
        self,
        scope,
        receive,
        send,
        futures_symbol: str,
        index_symbol: str,
        asof: Optional[datetime] = None,
    ) -> None:
        status: Optional[int] = None
        headers: list = []
        chunks: list[bytes] = []
        started = False

        async def capture(message):
            nonlocal status, headers, started
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = list(message.get("headers", []))
                return
            if message["type"] != "http.response.body":
                await send(message)
                return

            chunks.append(message.get("body", b"") or b"")
            if message.get("more_body"):
                return

            body = b"".join(chunks)
            content_type = _header(headers, b"content-type").decode("latin-1", "ignore")
            if status != 200 or "application/json" not in content_type:
                await self._passthrough(send, status, headers, body)
                started = True
                return

            try:
                payload = json.loads(body)
            except (ValueError, UnicodeDecodeError):
                await self._passthrough(send, status, headers, body)
                started = True
                return

            if len(body) > _LARGE_BODY_BYTES:
                logger.info(
                    "projecting a %d-byte %s response for %s",
                    len(body),
                    scope.get("zerogex_original_path", ""),
                    futures_symbol,
                )

            try:
                projected = await self._transform(
                    payload, futures_symbol, index_symbol, asof
                )
                out = json.dumps(projected).encode("utf-8")
            except Exception as e:
                # Never ship un-projected cash levels under an ES label: that
                # would be silently wrong on a chart. Fail visibly instead,
                # keeping the upstream headers so CORS still applies.
                logger.error(
                    "futures projection failed for %s: %s", futures_symbol, e, exc_info=True
                )
                detail = json.dumps(
                    {"detail": (f"Could not project {index_symbol} levels onto {futures_symbol}.")}
                ).encode("utf-8")
                await self._passthrough(send, 503, headers, detail)
                started = True
                return

            out_headers = [
                (key, value)
                for key, value in headers
                if key.lower() not in (b"content-length", b"content-encoding")
            ]
            out_headers.append((b"content-length", str(len(out)).encode("latin-1")))
            out_headers.append((b"x-zerogex-projection", b"1"))
            await send({"type": "http.response.start", "status": 200, "headers": out_headers})
            await send({"type": "http.response.body", "body": out})
            started = True

        await self.app(scope, receive, capture)

        if not started and status is not None:
            # The app never sent a terminal body message. Relay whatever was
            # buffered rather than manufacturing an empty 200 over the top of
            # it — discarding the chunks here would turn a real response into
            # a silent Content-Length: 0.
            await self._passthrough(send, status, headers, b"".join(chunks))

    async def _passthrough(self, send, status, headers, body: bytes) -> None:
        out_headers = [
            (key, value) for key, value in (headers or []) if key.lower() != b"content-length"
        ]
        out_headers.append((b"content-length", str(len(body)).encode("latin-1")))
        await send({"type": "http.response.start", "status": status or 500, "headers": out_headers})
        await send({"type": "http.response.body", "body": body})

    async def _transform_series(
        self,
        rows: list,
        futures_symbol: str,
        index_symbol: str,
        asof: Optional[datetime] = None,
    ) -> list:
        """Project a timestamped series, each row on its own session's basis.

        Bases are resolved once per distinct session and reused, so a 90-day
        series costs 90 reads rather than one per row (and the DB layer caches
        them besides). A row with no readable timestamp falls back to the
        request-level anchor, which is the behaviour it had before.
        """
        cache: dict[Optional[datetime], Any] = {}
        tick = projection_tick(futures_symbol)
        out = []
        for row in rows:
            anchor = _row_anchor(row) or asof
            if anchor not in cache:
                cache[anchor] = await resolve_basis(_db_manager(), futures_symbol, at=anchor)
            basis = cache[anchor]
            if basis is None:
                out.append(row)
                continue
            projected = project_payload(
                row,
                basis,
                tick=tick,
                relabel=(index_symbol, futures_symbol),
            )
            if isinstance(projected, dict):
                projected["projection"] = projection_metadata(basis)
            out.append(projected)
        return out

    async def _transform(
        self,
        payload: Any,
        futures_symbol: str,
        index_symbol: str,
        asof: Optional[datetime] = None,
    ) -> Any:
        # A v2 response is {"data": <the v1 body>, "freshness": {...}}. Project
        # INSIDE `data` and leave the envelope alone, for three reasons:
        #   * the top-level spot substitution below keys on SPOT_FIELDS at the
        #     top level, which on an envelope holds only `data`/`freshness` —
        #     so ES would have shipped a PROJECTED spot instead of the observed
        #     futures print, the exact failure this module exists to prevent;
        #   * `projection` would attach beside `data` rather than inside it,
        #     breaking v2's guarantee that `data` is byte-for-byte the v1 body;
        #   * `freshness` describes when the data was observed, which is the
        #     same instant whichever price axis it is rendered on — projecting
        #     a price ratio onto it would be meaningless.
        if _is_v2_envelope(payload):
            out = dict(payload)
            out["data"] = await self._transform(
                payload["data"], futures_symbol, index_symbol, asof
            )
            return out

        # A bare list of timestamped rows is a SERIES, and one ratio cannot
        # describe it: /api/gex/historical can span months, over which basis
        # walks through whole quarterly cycles. Anchoring the series at either
        # end leaves the other end offset by that drift — tens of points on ES,
        # and a backtest reading those levels has no way to see it. Project
        # each row on the basis that stood in its own session instead.
        if isinstance(payload, list) and any(_row_anchor(row) is not None for row in payload):
            return await self._transform_series(payload, futures_symbol, index_symbol, asof)

        basis = await resolve_basis(_db_manager(), futures_symbol, at=asof)
        if basis is None:
            return payload

        # The cash level the narrative prose was written against, read BEFORE
        # anything is rewritten — the spot substitution below replaces it, and
        # projecting prose against a futures level would double-apply carry.
        narrative_reference: Optional[float] = None
        if isinstance(payload, dict):
            for spot_key in SPOT_FIELDS:
                candidate = _as_number(payload.get(spot_key))
                if candidate and candidate > 0:
                    narrative_reference = candidate
                    break

        projected = project_payload(
            payload,
            basis,
            tick=projection_tick(futures_symbol),
            relabel=(index_symbol, futures_symbol),
            narrative_reference=narrative_reference,
        )

        # Prefer the OBSERVED futures print over a projected one for spot.
        # Only the top level is substituted: a nested per-row "spot" is that
        # row's historical spot, not the live one, and must stay projected.
        #
        # A request pinned to a past instant takes NO live substitution at all:
        # the live print is today's price, and stamping it on a frame from
        # three months ago states something false about that frame. The
        # projected value — that session's index spot carried by that session's
        # basis — is the honest answer. No historical endpoint currently ships
        # a top-level spot (replay nests it under `summary`, /gex/historical
        # returns a list), so this is a guard on shape rather than a fix for a
        # live bug: the substitution keys on payload shape, and the next
        # historical endpoint that happens to expose one would inherit today's
        # price silently.
        if (
            asof is None
            and isinstance(projected, dict)
            and any(k in projected for k in SPOT_FIELDS)
        ):
            live = await _live_futures_spot(index_symbol)
            if live is not None:
                for spot_key in SPOT_FIELDS:
                    if spot_key in projected and projected[spot_key] is not None:
                        projected[spot_key] = (
                            str(live) if isinstance(projected[spot_key], str) else live
                        )
                _reconcile_spot_derived(projected, live)

        if isinstance(projected, dict):
            meta = projection_metadata(basis)
            meta["narrative_prices_converted"] = narrative_reference is not None
            projected["projection"] = meta
        return projected
