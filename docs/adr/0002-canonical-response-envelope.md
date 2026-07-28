# ADR-0002 — Canonical response envelope with freshness, status, and per-field reason codes

- **Status:** Accepted
- **Date:** 2026-07-28
- **Authors:** Michael (Founder), ZeroGEX
- **Related ADRs:** [0001](./0001-recontract-api-for-freshness-and-versioning.md),
  [0003](./0003-api-versioning-strategy.md),
  [0004](./0004-deprecation-policy-and-customer-communication.md)

---

## Context

ADR-0001 establishes that we are re-contracting the API. This ADR
defines what the new response shape *is*. It is the schema half of the
Tier 1 decision.

Baseline: today most endpoints return a payload directly (a Pydantic
model or a `List[Model]`), sometimes with `response_model_exclude_none=True`.
There is no `meta` block, no `freshness` block, no `data_status`, no
standardized error shape, no per-field null reason code, no
request-body request ID. Consumers infer everything from the payload
timestamp or from HTTP metadata.

The gap is specifically per Johnnie's July 17 email:

- `gamma_flip` is null while `gamma_flip_raw` is numeric — the consumer
  cannot tell whether the null means "no crossing in searched range",
  "insufficient data", "quality filter", or "not applicable".
- Composite scores near 52 map to different labels across SPX / SPY /
  QQQ with no self-describing metadata.
- Payload timestamps don't disambiguate exchange time vs. calc time vs.
  server time.
- No server-processing timing metadata, so consumers can't triage
  where latency is coming from.

## Decision

Every market-sensitive endpoint under `/v1/` returns a JSON response
matching the envelope defined in this ADR. The envelope has six
top-level keys, in the order listed:

```json
{
  "data": { ... },
  "meta": { ... },
  "freshness": { ... },
  "status": { ... },
  "interpretation": { ... },
  "warnings": [ ... ]
}
```

Each is defined in detail below. The envelope is emitted by a shared
FastAPI response wrapper (introduced in Tier 1); handlers return their
`data` payload and the wrapper attaches the rest.

Error responses use a separate but envelope-compatible shape based on
RFC 7807 Problem Details, wrapped so it carries the same `meta` and
`warnings` context as success responses. See §Error responses below.

---

## The envelope

### `data` — the payload

Whatever the endpoint's domain payload is: the `GEXSummary`, the
`List[GEXByStrike]`, the `TradeSignalResponse`, etc. Preserved unchanged
from the current Pydantic models (with the per-field-status treatment
described under `status` below for nullable analytics).

For list-returning endpoints, `data` is the list; there is no separate
"data.items" nesting. Metadata about the list (count, cursor, etc.)
lives in `meta`.

For error responses, `data` is `null`.

### `meta` — request and response envelope

Fixed schema. Populated by the shared response wrapper; handlers do not
touch it.

```json
{
  "request_id": "01H8XZ...",
  "server_ts": "2026-07-28T14:03:12.483Z",
  "server_processing_ms": 47.2,
  "endpoint": "/v1/gex/summary",
  "schema_version": "1.0.0",
  "build": "bd80bdd",
  "stability": "stable"
}
```

| Field | Type | Meaning |
|---|---|---|
| `request_id` | `string` (ULID or UUID hex) | Server-generated identifier. Same value echoed in `X-Request-Id` response header. Used by consumers for support tickets and Copilot citation. |
| `server_ts` | RFC 3339 UTC, millisecond precision, `Z` suffix | When the API assembled this response. Not the underlying data time (that's `freshness.source_ts`). |
| `server_processing_ms` | `float` | Wall-clock time spent inside our process handling this request. Mirrored in `X-Server-Processing-Ms` header. Excludes network. |
| `endpoint` | `string` | The canonical route path (with `/v1/` prefix), no query string. Copilot citations join on this. |
| `schema_version` | semver `string` | The v1 schema version this response conforms to. Bumped per [ADR-0003](./0003-api-versioning-strategy.md). |
| `build` | `string` | Git SHA (7-char short) of the server build. For support triage. |
| `stability` | enum: `stable` / `beta` / `experimental` / `deprecated` | Per-endpoint stability contract. `beta` and `experimental` may change without a version bump; `stable` requires ADR-0003 versioning; `deprecated` carries `Deprecation` + `Sunset` headers per ADR-0004. |

For collection endpoints, `meta` additionally carries `count`,
`window`, or `cursor` fields as needed. Those are per-endpoint and
appear in the OpenAPI spec, not in this fixed schema.

**`request_id` source of record.** The existing
`RequestIdMiddleware` (`src/api/middleware.py:46`) already generates and
echoes `X-Request-Id`. Tier 1 exposes the same value via
`request_id_var.get()` inside the response wrapper — no new middleware.

### `freshness` — data currency and provenance

Fixed schema per endpoint. This is the block that closes Johnnie's
biggest concern.

```json
{
  "source_ts": "2026-07-28T14:03:00.000Z",
  "vendor_receipt_ts": "2026-07-28T14:03:00.041Z",
  "calc_ts": "2026-07-28T14:03:04.117Z",
  "age_seconds": 8,
  "market_session": "open",
  "market_session_source": "US_EQUITY",
  "expected_cadence_seconds": 60,
  "stale_after_seconds": 180,
  "carry_forward": false,
  "as_of_date": "2026-07-28"
}
```

| Field | Type | Meaning |
|---|---|---|
| `source_ts` | RFC 3339 UTC | Vendor/exchange publication time — the earliest attributable timestamp for the underlying tick. If distinct exchange time is unavailable, this equals `vendor_receipt_ts`. |
| `vendor_receipt_ts` | RFC 3339 UTC or `null` | TradeStation's receipt time, when we can observe it. `null` when unavailable. |
| `calc_ts` | RFC 3339 UTC or `null` | When *we* computed this response's derived values. `null` for pure passthrough endpoints (`/api/market/quote`, etc.). For analytics endpoints (`/v1/gex/*`, `/v1/signals/*`) this is the analytics engine's last cycle timestamp. |
| `age_seconds` | integer | `server_ts - source_ts`, in seconds. Consumer's fast-path staleness check. |
| `market_session` | enum: `pre` / `rth` / `post` / `closed` / `holiday` | Market session at `server_ts`. Sources this from the existing `get_market_session()` helper. |
| `market_session_source` | enum: `US_EQUITY` / `INDEX` / `FUTURES` / `CBOE_VIX` | Which session calendar applies. INDEX symbols close at 16:00 with no AH; futures follow a different calendar. |
| `expected_cadence_seconds` | integer | How often this endpoint's underlying source updates in normal conditions. E.g. quotes = 1s; GEX analytics = 60s; max-pain = 86400s (daily). |
| `stale_after_seconds` | integer | Age threshold above which the consumer should treat this as stale. Endpoint-specific; typically 3-5× `expected_cadence_seconds`. |
| `carry_forward` | `bool` | `true` when the response contains carry-forward values (last-known-good) instead of a fresh update. E.g. `/api/max-pain/current` between daily refreshes. `false` for genuinely fresh data. |
| `as_of_date` | ISO 8601 date | ET calendar day the data pertains to. Used for daily-cadence endpoints and for disambiguating overnight/weekend responses. |

**Not every field appears on every endpoint.** For endpoints without
a discrete source event (e.g. `/v1/gex/expirations` which returns a
distinct-values list), `source_ts` may equal `calc_ts`. For endpoints
without an ET-calendar semantic, `as_of_date` is null. The OpenAPI spec
enumerates the required fields per endpoint.

### `status` — data quality state

Two sub-fields: `data_status` (envelope-level classification) and
`sanity_flags` (specific detected issues).

```json
{
  "data_status": "fresh",
  "sanity_flags": []
}
```

#### `data_status` — the DataStatus enum

Closed vocabulary. Classifies the response as a whole.

| Value | Meaning |
|---|---|
| `fresh` | Data is current per this endpoint's `expected_cadence_seconds`. |
| `delayed` | Data is present but older than expected cadence; still within `stale_after_seconds`. |
| `stale` | Data is older than `stale_after_seconds`. Consumer should downweight or refuse. |
| `closed_session` | Response reflects the last close (or prior close on weekend/holiday). Not stale, just closed-market. |
| `partial` | Response is present but some fields are `not_applicable` or `insufficient_data` — the payload is coherent but incomplete. |
| `unavailable` | The endpoint's data source is temporarily unreachable and no cached fallback applies. HTTP 503. |
| `fallback` | Response is served from a fallback source (e.g. cached prior day when live source is unreachable). Not stale, but of lower confidence. |
| `error` | Terminal failure. Consumer should not use `data`. HTTP 4xx or 5xx. |

Only one value per response. It's the single fast-path check consumers
gate on.

#### `sanity_flags` — the SanityFlag enum

Ordered list of specific issues detected. Zero-or-more per response.
Closed vocabulary. Present values do not necessarily invalidate the
response — they're structured hints.

| Code | Meaning |
|---|---|
| `MISSING_TIMESTAMP` | The underlying source lacks a timestamp we can reconstruct. |
| `STALE_TIMESTAMP` | Source timestamp is older than `stale_after_seconds` but not old enough for `data_status: stale` (e.g. one field's source is stale while others are fresh — response is `partial`). |
| `PARTIAL_RESPONSE` | Some expected fields are `not_applicable` or `insufficient_data`. Companion to `data_status: partial`. |
| `EMPTY_CHAIN` | The option chain we queried returned zero contracts. |
| `IMPOSSIBLE_VALUE` | A derived value violated a sanity check (e.g. negative volume, NaN GEX). Value replaced with `null` + `FieldStatus: filtered`. |
| `DELAYED_DATA` | Vendor delivered data below its own SLA. |
| `ENTITLEMENT_LIMIT` | Some fields were withheld due to the caller's entitlement tier. |
| `SYMBOL_MISMATCH` | Requested symbol resolves to a different underlying than expected (e.g. index vs future basis). |
| `CLOSED_MARKET` | Response contains no fresh data because market is closed. Companion to `data_status: closed_session`. |
| `CARRY_FORWARD` | Response contains carry-forward values. Companion to `freshness.carry_forward: true`. |
| `PROXY_USED` | A proxy source was used (e.g. VWAP for a cash index uses proxy-ETF volume). Free-text detail lives in `warnings`. |

### `interpretation` — labels, scores, semantic metadata

**This is the block that answers "what does this data mean?"** Separate
from `data` so consumers who want raw facts can strip it; consumers who
want the interpretation get it structured.

Shape depends on the endpoint. For signal endpoints:

```json
{
  "labels": {
    "direction": { "value": "bullish", "confidence": "medium" },
    "regime": { "value": "constructive", "evidence": ["net_gex_at_spot", "vwap_deviation"] }
  },
  "score_metadata": {
    "composite_score": {
      "value": 52.3,
      "range": [0, 100],
      "kind": "descriptive",
      "cadence_seconds": 60,
      "symbol_comparable": false,
      "component_weights": { "flow_bias": 0.25, "skew_delta": 0.15, "vanna_charm": 0.20, "dealer_delta": 0.15, "gex_gradient": 0.15, "positioning_trap": 0.10 },
      "components_pushed_bullish": ["vanna_charm", "gex_gradient"],
      "components_pushed_bearish": ["skew_delta"],
      "components_neutral": ["flow_bias", "dealer_delta", "positioning_trap"]
    }
  }
}
```

For non-signal endpoints (raw market data, GEX levels, etc.),
`interpretation` is `{}` or omitted.

Fields inside `score_metadata`:

- `range` — `[low, high]` bounds of the score.
- `kind` — `descriptive` (measures current state), `predictive`
  (forecasts direction), or `experimental` (research; may change without
  a version bump).
- `cadence_seconds` — recalculation cadence.
- `symbol_comparable` — `true` if this score is directly comparable
  across symbols; `false` if the score's scale is symbol-specific
  (which is exactly the SPX/SPY/QQQ ~52 case Johnnie observed).
- `component_weights` — the weights used in this cycle's composite.
- `components_pushed_{bullish,bearish,neutral}` — which components
  contributed which direction. This is the answer to "why does the
  same number produce different labels?"

`labels` maps label kinds to values with associated confidence and
evidence citations. A future Copilot uses `evidence` to explain the
label to the user.

### `warnings` — advisory notices

Ordered list of free-text-plus-code advisories. Not errors (those are
in the error envelope). Not sanity flags (those are structured codes).
Warnings are the "you might want to know" channel.

```json
[
  {
    "code": "ROUTE_DEPRECATED",
    "message": "GET /v1/legacy/market/vix is deprecated; use /v1/market/volatility?ticker=VIX.",
    "successor": "/v1/market/volatility?ticker=VIX",
    "sunset": "2027-01-31"
  }
]
```

Fields:

| Field | Type | Notes |
|---|---|---|
| `code` | closed enum | See below. |
| `message` | `string` | Human-readable free text. Safe to display to end users. Not localized. |
| `successor` | `string` (optional) | For deprecation, the successor route or field. |
| `sunset` | ISO 8601 date (optional) | For deprecation. |
| `detail` | JSON object (optional) | Per-code structured detail. |

Warning codes (closed vocabulary; extend via ADR):

- `ROUTE_DEPRECATED`
- `FIELD_DEPRECATED`
- `RATE_LIMIT_APPROACHING` (e.g. 80% of quota)
- `ENTITLEMENT_UPGRADE_AVAILABLE`
- `EXPERIMENTAL_FEATURE`
- `PROXY_SOURCE` (see also `sanity_flags: PROXY_USED`; the flag is
  structured, the warning is human-readable)

---

## Per-field null reason codes (FieldStatus / FieldValue)

**The specific fix for Johnnie's `gamma_flip` case.**

Every field on an analytics response whose null state carries semantic
meaning (i.e. isn't just "this row happens not to have this attribute")
is emitted as a `FieldValue<T>` discriminated shape:

```json
{
  "gamma_flip": {
    "value": null,
    "status": "no_crossing_in_range",
    "detail": {
      "range_searched": [552.0, 668.0],
      "span_pct": 0.10,
      "span_ladder_step": 0
    }
  },
  "gamma_flip_raw": {
    "value": 605.4,
    "status": "valid"
  }
}
```

### The FieldStatus enum

| Value | Meaning |
|---|---|
| `valid` | Value is present and passes sanity checks. |
| `no_crossing_in_range` | The searched range yielded no crossing (specific to flip / wall calculations). Detail lists the range/span searched. |
| `insufficient_data` | Not enough underlying data to compute. Detail lists what was missing (min-sample-size, min-days, etc.). |
| `filtered` | Value was computed but rejected by a quality filter. Detail describes the filter. |
| `not_applicable` | Field is meaningful for other symbol/timeframe combinations but not this one (e.g. `premarket_high` on an INDEX symbol). |
| `stale` | Field's underlying source is older than its own `stale_after_seconds`. |
| `error` | Computation errored. Detail carries the error kind (never the exception body). |

Which fields adopt `FieldValue<T>`:

- `GEXSummary`: `gamma_flip`, `gamma_flip_raw`, `gamma_flip_span_used`,
  `flip_distance`, `local_gex`, `convexity_risk`, `max_pain`,
  `call_wall`, `put_wall`, `net_gex_at_spot`.
- `GEXProfile`: `gamma_flip`, `net_gex_at_spot`, `call_wall`, `put_wall`.
- `MaxPainCurrent`: `max_pain`, `difference`.
- Signal endpoints: every nullable score component (`SignalComponent.value`
  with `applicable=false` becomes `status: "not_applicable"`).
- `SessionLevels`: every nullable level field (INDEX symbols have
  `not_applicable` for pre-market levels).

Which fields stay as bare `Optional[T]`:

- Genuinely optional fields (query-response shapes where absence has no
  semantic meaning). Examples: `underlying_price` on flow rows when the
  bucket has no reference tick; `data_symbol` on `UnderlyingQuote` when
  the index→future swap is disabled.

The audit that decides which fields go which way is a Tier 2
deliverable per endpoint.

---

## Full example — `/v1/gex/summary` response

Fresh, live-session response for SPY:

```json
{
  "data": {
    "symbol": "SPY",
    "spot_price": 605.42,
    "total_call_gex": 12857340000.0,
    "total_put_gex": -8934210000.0,
    "net_gex": 3923130000.0,
    "net_gex_at_spot": { "value": 1284200000.0, "status": "valid" },
    "gamma_flip": {
      "value": null,
      "status": "no_crossing_in_range",
      "detail": {
        "range_searched": [552.0, 668.0],
        "span_pct": 0.10,
        "span_ladder_step": 0
      }
    },
    "gamma_flip_raw": { "value": 605.4, "status": "valid" },
    "flip_distance": {
      "value": null,
      "status": "no_crossing_in_range"
    },
    "call_wall": { "value": 610.0, "status": "valid" },
    "put_wall": { "value": 600.0, "status": "valid" },
    "max_pain": { "value": 605.0, "status": "valid" }
  },
  "meta": {
    "request_id": "01HXYZ...",
    "server_ts": "2026-07-28T14:03:12.483Z",
    "server_processing_ms": 47.2,
    "endpoint": "/v1/gex/summary",
    "schema_version": "1.0.0",
    "build": "bd80bdd",
    "stability": "stable"
  },
  "freshness": {
    "source_ts": "2026-07-28T14:03:00.000Z",
    "vendor_receipt_ts": "2026-07-28T14:03:00.041Z",
    "calc_ts": "2026-07-28T14:03:04.117Z",
    "age_seconds": 8,
    "market_session": "rth",
    "market_session_source": "US_EQUITY",
    "expected_cadence_seconds": 60,
    "stale_after_seconds": 180,
    "carry_forward": false,
    "as_of_date": "2026-07-28"
  },
  "status": {
    "data_status": "fresh",
    "sanity_flags": []
  },
  "interpretation": {},
  "warnings": []
}
```

Note: `interpretation` is empty for pure GEX facts. Signal endpoints
populate it; raw endpoints don't.

## Error responses

Errors use an envelope-compatible shape. The `data` is null, an
`error` block is populated per RFC 7807, and `meta` / `freshness` are
populated to whatever extent they can be.

```json
{
  "data": null,
  "meta": {
    "request_id": "01HXYZ...",
    "server_ts": "2026-07-28T14:03:12.483Z",
    "server_processing_ms": 3.1,
    "endpoint": "/v1/gex/summary",
    "schema_version": "1.0.0",
    "build": "bd80bdd",
    "stability": "stable"
  },
  "status": {
    "data_status": "error",
    "sanity_flags": ["MISSING_TIMESTAMP"]
  },
  "warnings": [],
  "error": {
    "type": "https://docs.zerogex.io/errors/no-data-available",
    "title": "No GEX data available",
    "status": 404,
    "detail": "No gex_summary row for symbol=SPY in the last 24h.",
    "instance": "/v1/gex/summary?symbol=SPY",
    "request_id": "01HXYZ..."
  }
}
```

Error `type` URIs resolve to human-readable pages on the docs site.
Never emit stack traces or exception messages over the wire — that's
enforced by the existing `handle_api_errors` decorator; the new shape
just wraps it.

## Timestamp format

**Every** RFC 3339 timestamp in the envelope uses:

- UTC only. Never emit ET or any other offset.
- Millisecond precision (`.NNN`).
- `Z` suffix (never `+00:00`).
- No timezone in the OpenAPI schema field type — always `date-time` with
  `format: "utc-millis"` custom marker.

The existing code uses inconsistent formats (`.isoformat()`,
hand-formatted `%Y-%m-%dT%H:%M:%SZ`, ET-aware). Tier 1 introduces a
shared serializer that all envelope timestamps go through; legacy models
inside `data` may still use `.isoformat()` until Tier 2 sweeps them.

## Rationale

### Why six top-level keys, not fewer

Considered nesting `freshness`, `status`, `interpretation`, and
`warnings` under a single `meta` block. Rejected because:

1. **`freshness` is the most-inspected block** by automated consumers.
   Nesting it under `meta` makes the JSONPath `meta.freshness.age_seconds`
   longer than necessary; keeping it top-level shortens the fast-path
   check to `freshness.age_seconds`.
2. **`interpretation` is optional per endpoint** — putting it at the
   top level makes it easy for consumers to `if 'interpretation' in
   response` without descending. If it were nested it would always be
   present-as-empty-object.
3. **`warnings` is an array** — the convention across major APIs
   (Stripe, GitHub, AWS) is to expose arrays like this at the top level.

Considered exposing only `data` and `meta`, with `freshness`,
`status`, `interpretation`, `warnings` all under `meta`. Rejected for
the same reasons but stronger.

### Why FieldValue discriminated union, not sibling status fields

Considered a pattern where every nullable field has a sibling status:
`gamma_flip: null, gamma_flip_status: "no_crossing_in_range"`.
Rejected because:

1. **Doubles the field count** — 12 nullable fields on `GEXSummary`
   become 24 fields.
2. **Loses the semantic pairing** — the value and its status are the
   same conceptual thing, split across the payload.
3. **Detail belongs with the value** — the searched range is meaningful
   only in the context of the null result. A separate detail field
   compounds the sibling problem.

The `FieldValue<T>` shape scales better and mirrors patterns used in
protobuf (`google.protobuf.Value`) and other typed schema systems.

### Why `data_status` as an enum rather than a HTTP-status-shaped thing

HTTP status codes convey transport-level state. `data_status` conveys
domain-level currency. They are different axes:

- `HTTP 200 + data_status: closed_session` — response is fine, market
  is closed. Not stale, not degraded.
- `HTTP 200 + data_status: stale` — server responded but data is
  older than the endpoint's SLA.
- `HTTP 503 + data_status: unavailable` — full transport-level failure.

Consumers gate on both; conflating them into HTTP status alone forces
false conflations.

### Why not use HTTP-header-only for meta and freshness

Considered emitting `X-Freshness-Age`, `X-Data-Status`, etc. as headers
instead of body fields. Rejected because:

1. **Loggability** — a JSON body is trivial to log and diff; headers
   require additional per-consumer wiring.
2. **Browser DevTools** — bodies show in the network panel by default;
   headers require an extra click.
3. **Copilot citation** — Copilot needs the request_id and freshness
   info in the same payload as the data it's citing, not out-of-band.

Two headers are duplicated in the body for convenience: `X-Request-Id`
(mirrors `meta.request_id`) and `X-Server-Processing-Ms` (mirrors
`meta.server_processing_ms`). The header duplicates exist so proxies
and load balancers can log them without parsing JSON.

### Why RFC 3339 UTC milliseconds, not the existing formats

- **RFC 3339** is the internet-standard for machine-readable timestamps.
- **UTC only** eliminates timezone bugs at the consumer boundary. ET
  conversion happens in the frontend where the user's location context
  actually matters.
- **Millisecond precision** matches the natural resolution of tick
  data (source events don't resolve below the millisecond).
- **`Z` suffix** is unambiguous; `+00:00` is technically equivalent but
  many parsers treat them differently.

The existing `%Y-%m-%dT%H:%M:%SZ` format on `/api/flow/series` almost
gets this right — just missing the milliseconds. The v1 shape upgrades
it.

## Consequences

### Positive

- **The envelope is self-describing.** A consumer can build a general
  parser once and use it for every endpoint.
- **Freshness ambiguity is closed.** `freshness` + `status.data_status`
  answer every question Johnnie's July 17 email raised.
- **Per-field null reason codes** answer the `gamma_flip` case
  directly; no more inference.
- **Score interpretation is self-describing** via `interpretation.score_metadata`;
  the SPX/SPY/QQQ divergence Johnnie observed becomes readable from
  the payload alone.
- **Request-body request IDs unlock consumer-side triage.** Combined
  with `server_processing_ms`, consumers can prove which side of the
  boundary owns latency.
- **Copilot grounding is trivial.** Every claim it makes can cite
  `request_id` + `endpoint` + field path.
- **Sanity flags force us to admit what we don't know.** If a proxy
  source was used, or a value was clamped, the consumer sees the
  structured flag.

### Negative

- **Response payloads grow.** Every response gains the envelope
  overhead (~200-400 bytes of `meta` + `freshness` + `status`). For
  large payloads this is negligible; for tiny payloads
  (`/v1/gex/expirations`) it's noticeable percentage-wise but not
  bytes-wise.
- **Response times increase marginally.** Wrapping in the envelope
  requires an extra allocation per response. Micro-benchmark first,
  optimize if it matters.
- **Consumer migration cost.** Every existing integration must
  update. The 90-day `/legacy/` window per ADR-0003 mitigates.
- **Model complexity.** `FieldValue<T>` adds a Pydantic union type
  everywhere it's used. Discriminated unions in Pydantic 2 are
  ergonomic but represent a real new pattern for the codebase.

### Neutral

- **The Pydantic v1 → v2 `ConfigDict` migration** stays deferred. The
  envelope wraps existing legacy-style models; migration is orthogonal.
- **The existing middleware stack** — RequestId, Audit, UsageMeter —
  stays unchanged. The envelope reads from existing context vars.

## Implementation notes for Tier 1

1. **Wrapper location.** New `src/api/envelope.py` module implementing
   the wrapper as a FastAPI dependency + response processor. Handlers
   remain unchanged in signature.
2. **Types.** New `src/api/schemas/envelope.py` module for the Pydantic
   models: `MetaBlock`, `FreshnessBlock`, `StatusBlock`, `InterpretationBlock`,
   `WarningEntry`, `EnvelopeResponse[T]`, `FieldValue[T]`, `DataStatus`,
   `SanityFlag`, `FieldStatus`.
3. **Reference endpoint first.** Migrate `/v1/gex/summary` end-to-end
   as the reference. Use it in tests and docs.
4. **OpenAPI schemas.** Every envelope type gets a schema entry; the
   OpenAPI generation reflects the union types with discriminators.
5. **Golden test.** `tests/api/envelope/test_reference_response.py`
   pins the JSON structure of a known-good response and fails on any
   accidental change.

Detailed migration order per endpoint is in Tier 2's plan.

## References

- **JSON:API v1.1** — envelope pattern (`data`, `meta`, `errors`).
  https://jsonapi.org/format/1.1/
- **RFC 3339** — timestamp format.
- **RFC 7807** — Problem Details for HTTP APIs.
- **RFC 8594** — the `Sunset` HTTP header (used by ADR-0004).
- **Stripe API** — inspiration for `warnings` shape and per-response
  request IDs.
- **`docs/api-audit/types.md`** — baseline models this envelope wraps.
- **`docs/api-audit/endpoints.md`** — endpoint list this envelope applies to.
