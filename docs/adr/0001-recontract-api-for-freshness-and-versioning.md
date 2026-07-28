# ADR-0001 — Re-contract the API for freshness, semantics, and versioning

- **Status:** Accepted
- **Date:** 2026-07-28
- **Authors:** Michael (Founder), ZeroGEX
- **Related ADRs:** [0002](./0002-canonical-response-envelope.md),
  [0003](./0003-api-versioning-strategy.md),
  [0004](./0004-deprecation-policy-and-customer-communication.md)

---

## Context

ZeroGEX's public API is a structured options-market data source consumed
by automated integrators. Availability and data quality are high (a
2,787-check sample from one power user showed 99.9% source freshness
during regular hours), but three consecutive rounds of structured
feedback from that user (2026-06-14, 2026-06-21, 2026-07-17) converge
on the same critique:

> The data is great. The trust envelope around the data is not.

Specifically:

1. **Freshness ambiguity.** A `200 OK` response does not tell a consumer
   whether the payload is live, delayed, stale, carry-forward, or
   closed-session. The consumer must infer currency from a `timestamp`
   field that itself has ambiguous semantics (source-market time?
   vendor-receipt time? our calc time? API response time?).
2. **Semantic ambiguity.** Composite scores like the one at
   `/api/signals/score` return numbers around 52 while derived
   directional labels differ across SPX, SPY, and QQQ, with no
   documented mapping from score to label. Nullable analytic fields
   like `gamma_flip` return `null` with no reason code, while a sibling
   `gamma_flip_raw` returns a number — the consumer cannot tell whether
   the null means "no crossing in searched range", "insufficient data",
   "quality filter", or "not applicable".
3. **Contract ambiguity.** There is no machine-readable schema version,
   no capability discovery endpoint, no changelog, and no deprecation
   signal. On 2026-07-17 the same user discovered `/api/market/vix` had
   been removed by getting a 404; the replacement route
   (`/api/market/volatility`) existed but was undocumented and
   unannounced.

The current ZeroGEX API already has strong foundations — FastAPI +
Pydantic, per-request `X-Request-Id` middleware, structured audit logs,
scope-based authorization, a sophisticated market-session state machine.
The gap is not in infrastructure but in the *contract shape* the
consumer sees.

### Forcing functions

- **Customer feedback (Johnnie Dunkum, 2026-06-14 → 2026-07-17).** Three
  rounds of structured feedback from a technically sophisticated
  automated consumer, culminating in a specific request for the
  freshness/session object, per-field null reason codes, score
  interpretation metadata, request IDs, server-processing timing, and
  machine-readable versioning.
- **`/api/market/vix` incident (2026-07-17).** A silent route removal
  broke a live customer integration. Michael's July 17 reply to Johnnie
  contained the commitment: *"I'll definitely handle API changes more
  gracefully going forward."* That commitment is now customer-facing
  and must be honored in code.
- **Copilot roadmap dependency.** Michael's own June 22 note committed
  to ZeroGEX Copilot — plain-English regime classification, structured
  Trade Cards, Q&A grounded in real ZeroGEX data. Copilot is impossible
  to ship responsibly until every value it consumes has provenance
  (which endpoint, which request, which timestamp, which freshness
  state). Copilot amplifies whatever contract clarity the API has;
  building it on the current shape would amplify the ambiguity.
- **Company aspiration.** The stated goal is to build the foundation
  for a world-class software company. That means contract-first
  engineering, spec-as-source-of-truth, no silent breaking changes,
  documentation as a product artifact. The current shape is fine for a
  hobbyist API; it is not fine for a company building a serious
  developer product.

### Non-goals

- Adding new market-data endpoints or new analytics.
- Changing the ingestion pipeline, database schema, or analytics engine.
- Redesigning the frontend UX.
- Migrating existing Pydantic models from v1-`Config` style to
  v2-`ConfigDict` style (mechanical cleanup independent of this work).

## Decision

We are re-contracting the public API around three principles:

1. **A canonical response envelope on every market-sensitive endpoint**
   carrying `data`, `meta`, `freshness`, `status`, `sanity_flags`,
   `interpretation`, `warnings`, and a per-request `request_id`. Full
   shape in [ADR-0002](./0002-canonical-response-envelope.md).
2. **Hard-cut to `/v1/` with OpenAPI 3.1 as the source of truth,**
   with the current shape aliased under `/legacy/` for 90 days. Types
   are generated from the spec; breaking changes are CI-gated. Full
   policy in [ADR-0003](./0003-api-versioning-strategy.md).
3. **A formal deprecation policy** with RFC 8594 `Deprecation` and
   `Sunset` headers, a machine-readable endpoint changelog, in-payload
   warnings, and a customer-communication protocol. Full protocol in
   [ADR-0004](./0004-deprecation-policy-and-customer-communication.md).

These three decisions are jointly accepted — none of them ships in
isolation. Together they constitute the "Tier 1" milestone of the
re-contract rollout; the subsequent tiers migrate endpoints, add
observability, and rebuild documentation atop this foundation.

## Rationale

### Why now, rather than incrementally

The three gaps (freshness, semantics, contract) reinforce each other.
Adding a freshness object without a versioning strategy is a breaking
change in disguise. Adding versioning without a canonical envelope
freezes the ambiguous shape into `/v1/`. Adding deprecation policy
without a stable contract to deprecate against is theatre. Each
depends on the others; shipping any one alone gives up most of the
value and creates rework.

The cost of doing it now is 4-week Tier 1 work. The cost of doing it in
six months, after Copilot ships on the current contract, is a compounded
migration where every Copilot output has to be re-verified against a new
envelope shape and every third-party consumer has been trained on the
ambiguous version.

### Why not incremental additive freshness fields on the existing shape

Considered and rejected. The path would be: add `age_seconds`,
`market_session`, `data_status` as optional top-level fields to each
response, defaulted-to-null on responses that haven't been migrated. A
consumer that assumes non-null fields breaks; a consumer that codes
defensively gets no benefit until every endpoint is migrated.

The path also doesn't address the structural gaps (facts vs
interpretation, per-field null reason codes, standardized sanity flags,
request-body provenance) — those genuinely require a new envelope
shape. So we'd pay the migration cost twice: once for the additive
fields, once for the envelope.

### Why not JSON:API or another off-the-shelf envelope

Considered. JSON:API is a mature specification for envelope shape,
with `data`, `meta`, `errors`, `included`, `links`. But it's optimized
for resource-oriented CRUD APIs with pagination and relationships,
neither of which describes ZeroGEX's read-heavy analytics surface. The
`meta` block would end up carrying most of the interesting fields
anyway (`freshness`, `sanity_flags`, `interpretation`, `warnings`),
which are not standardized by JSON:API.

We take the *pattern* from JSON:API (envelope with `data` + `meta`)
without the specific spec. The v1 envelope's meta block is closer to
what a domain like ours needs and is documented explicitly in ADR-0002.

### Why hard-cut to `/v1/` rather than additive-only under existing routes

See [ADR-0003](./0003-api-versioning-strategy.md) for the full
argument. Summary: additive-only sacrifices contract clarity for
avoiding a migration window; hard-cut with `/legacy/` alias gives us a
clean contract without a hard cutover.

### Why standardize deprecation policy now

Michael's July 17 commitment to Johnnie (*"I'll definitely handle API
changes more gracefully going forward"*) is a customer-facing promise.
Honoring it requires more than intent — it requires infrastructure
(deprecation headers, changelog, notice protocol) that must exist
*before* the next deprecation, not after. See
[ADR-0004](./0004-deprecation-policy-and-customer-communication.md).

## Consequences

### Positive

- **Consumers can safely gate on data freshness** without inferring
  from ambiguous timestamps. Automated systems can treat the API as a
  first-class production data source.
- **Score interpretation is self-describing.** No more "score 52,
  what does that mean?" — the response carries the metadata inline.
- **Nullable analytics are self-explaining.** No more "why is
  `gamma_flip` null while `gamma_flip_raw` is numeric?" — the null
  carries its reason.
- **Consumer latency triage is possible.** With request IDs and
  `X-Server-Processing-Ms`, a consumer can prove their downstream
  processing is (or isn't) the bottleneck.
- **The `/api/market/vix` incident cannot recur.** Deprecation
  headers + changelog + notification protocol prevent silent removal.
- **Copilot has a foundation to build on.** Every Copilot claim can
  cite a request_id + endpoint + field, because that provenance is in
  the envelope by default.
- **We can hire against a real API contract.** New engineers see the
  envelope shape and the OpenAPI spec on day 1.

### Negative

- **~14-week rollout** across Tiers 0-5 before the migration is
  complete (Tier 6-8 add Copilot on top). That's real calendar time
  with no new customer-visible features shipping meanwhile — a hidden
  cost that customers won't see and management may be tempted to
  shortcut. The plan explicitly resists that temptation.
- **Every existing consumer sees a change.** Even a `/legacy/` alias is
  a strong nudge; sophisticated consumers will move to `/v1/` sooner
  than the 90-day window, less sophisticated ones may need direct
  outreach.
- **Documentation stops being hand-maintained.** `API_Guide.md` retires
  in favor of a generated docs site. Anyone who was in the habit of
  editing the guide directly needs to learn to edit the OpenAPI spec.
- **Increased engineering discipline.** ADRs for design changes, CI
  gates for breaking API changes, spec-first for new endpoints. This
  is intentional — it's the discipline we want — but it's real cost.

### Neutral

- **Existing infrastructure is preserved.** `RequestIdMiddleware`,
  `AuditLogMiddleware`, `handle_api_errors`, `get_market_session`,
  scope-based auth — all reused verbatim. The re-contract is a
  contract change, not an implementation rewrite.
- **Existing Pydantic models are preserved.** They become the `data`
  block inside the envelope. The v2 ConfigDict migration is a separate
  cleanup.

## References

- **Customer feedback thread**
  - Johnnie Dunkum, 2026-06-14 initial feedback (PDF).
  - Johnnie Dunkum, 2026-06-21 combined feedback (email).
  - Michael reply, 2026-06-22.
  - Michael reply on `/api/market/vix`, 2026-07-17.
  - Johnnie Dunkum, 2026-07-17 next-round feedback.
  - Michael's 2026-07-28 substantive reply (this ADR is one of its
    concrete deliverables).
- **Related work in this repo**
  - `docs/api-audit/endpoints.md` — baseline endpoint inventory.
  - `docs/api-audit/types.md` — baseline response-shape inventory.
  - `docs/api-audit/route-drift.md` — baseline drift audit.
  - `docs/design/api-gateway-architecture.md` — separate initiative
    (single-process TradeStation owner); does not conflict with this
    ADR.
- **External prior art**
  - JSON:API v1.1 — envelope pattern reference.
  - RFC 8594 — the `Sunset` header (used by ADR-0004).
  - RFC 3339 — timestamp format (used by ADR-0002).
  - RFC 7807 — Problem Details for HTTP APIs (used by ADR-0002 for
    error shapes).
  - Stripe API versioning — customer-first deprecation model (informs
    ADR-0004).
