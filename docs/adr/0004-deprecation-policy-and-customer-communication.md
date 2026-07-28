# ADR-0004 — Deprecation policy and customer communication protocol

- **Status:** Accepted
- **Date:** 2026-07-28
- **Authors:** Michael (Founder), ZeroGEX
- **Related ADRs:** [0001](./0001-recontract-api-for-freshness-and-versioning.md),
  [0002](./0002-canonical-response-envelope.md),
  [0003](./0003-api-versioning-strategy.md)

---

## Context

On 2026-07-17 an active customer integration broke when
`/api/market/vix` returned 404. The route had been intentionally
replaced by `/api/market/volatility?ticker=VIX`, but no deprecation
notice, no changelog entry, no notification, and no compat shim were
provided. The customer discovered the change by observing empty VIX
fields in their downstream model.

Michael's 2026-07-17 reply to the customer contained the concrete
commitment:

> *"I should have formally deprecated it first instead of removing it
> outright so it didn't cause downstream issues. I'll definitely handle
> API changes more gracefully going forward."*

That commitment is customer-facing and cannot be honored by intent
alone. It requires infrastructure that must exist *before* the next
deprecation, not after.

The gap is threefold:

1. **No machine signal for deprecation.** Even a well-intentioned
   engineer removing a route has no established convention for
   signaling deprecation to consumers.
2. **No changelog.** Even if the engineer signals deprecation, there
   is no persistent record customers can subscribe to.
3. **No communication protocol.** Even if signal and record exist, no
   process exists for reaching out to affected customers, giving them
   time, and confirming their migration.

## Decision

Four coordinated commitments comprise the deprecation policy:

1. **RFC 8594 headers on every deprecated response.**
   `Deprecation: true`, `Sunset: <date>`, and
   `Link: <successor>; rel="successor-version"` on every request that
   hits a deprecated route or accesses a deprecated field.
2. **In-payload `warnings` entries** with structured `code`,
   human-readable `message`, `successor`, and `sunset` fields
   (per ADR-0002's warnings shape).
3. **Machine-readable changelog** at `/v1/version/changelog` with
   per-entry `type`, `announced_at`, `sunset_at`, `route`, `successor`,
   and narrative details.
4. **A customer communication protocol** with minimum notice periods,
   an outbound notification workflow, and a follow-up procedure.

Together these guarantee that **no customer integration breaks because
of a change we made without warning again.**

The pilot for the entire policy is the retroactive deprecation of
`/api/market/vix` in Tier 1 — see below.

---

## The RFC 8594 header contract

Every response to a deprecated route or a request that would return a
deprecated field carries:

```
Deprecation: true
Sunset: Sun, 31 Jan 2027 00:00:00 GMT
Link: </v1/market/volatility?ticker=VIX>; rel="successor-version"
```

Field-level deprecations use the same header set on responses that
include the deprecated field, plus a `warnings[]` entry naming the
specific field.

Header details:

- **`Deprecation`** — [RFC 8594 §3.1](https://datatracker.ietf.org/doc/html/rfc8594#section-3.1).
  Value is `true` for currently-deprecated resources. Alternate form
  is an HTTP-date indicating *when* the resource was deprecated; we
  use the boolean form for simplicity and put the announcement date
  in the changelog.
- **`Sunset`** — [RFC 8594 §3.2](https://datatracker.ietf.org/doc/html/rfc8594#section-3.2).
  HTTP-date of scheduled removal.
- **`Link: rel="successor-version"`** —
  [RFC 5988](https://datatracker.ietf.org/doc/html/rfc5988), refined
  by [RFC 8288](https://datatracker.ietf.org/doc/html/rfc8288).
  Points to the replacement resource.

## Minimum notice periods

Every deprecation has a mandatory minimum window between announcement
and sunset, tied to the stability tier defined in ADR-0002.

| Stability at time of deprecation | Minimum notice window |
|---|---|
| `stable` | **90 days** |
| `beta` | **30 days** |
| `experimental` | **7 days** or immediate (documented in changelog) |
| Already `deprecated` | No further notice — sunset date already announced |

The stated notice is a **minimum**. Longer is fine. Shorter is not,
except for security-mandated removal (see below).

**Emergency deprecation exception.** A vulnerability or data-integrity
issue may require faster removal. In that case: sunset ASAP, immediate
outbound notification to all known consumers, ADR filed explaining the
emergency. This exception is not a general shortcut; it requires an
explicit named emergency.

## The machine-readable changelog

The customer-visible record of every API change.

### Location

- **Programmatic:** `GET /v1/version/changelog` (public, no auth
  required, rate-limited per ADR-0003).
- **Human-visible:** `https://docs.zerogex.io/v1/changelog` (rendered
  from the same data).

### Shape

```json
{
  "data": {
    "entries": [
      {
        "id": "2026-07-28-vix-route-deprecated",
        "type": "deprecated",
        "target": {
          "kind": "route",
          "path": "/legacy/market/vix",
          "method": "GET"
        },
        "announced_at": "2026-07-28",
        "sunset_at": "2027-01-31",
        "successor": "/v1/market/volatility?ticker=VIX",
        "title": "GET /api/market/vix deprecated",
        "summary": "The /api/market/vix route is deprecated in favor of /api/market/volatility, which defaults to VIX and also supports VXN (Nasdaq volatility). Migration: replace /api/market/vix with /api/market/volatility. No response-shape change.",
        "migration_guide_url": "https://docs.zerogex.io/v1/changelog/2026-07-28-vix-route-deprecated",
        "schema_version_at_announcement": "1.0.0",
        "affected_scopes": ["market_raw"]
      },
      {
        "id": "2026-08-15-v1-envelope-release",
        "type": "added",
        "target": {
          "kind": "envelope",
          "version": "1.0.0"
        },
        "announced_at": "2026-08-15",
        "title": "v1 API launched with canonical response envelope",
        "summary": "The /v1/ surface is now available with canonical response envelope (freshness, status, sanity_flags, request_id, server_processing_ms). See ADR-0002 for the shape.",
        "migration_guide_url": "https://docs.zerogex.io/v1/migration/legacy-to-v1"
      }
    ],
    "next_cursor": null
  },
  "meta": { ... },
  "freshness": { ... },
  ...
}
```

### Entry types (closed enum)

- `added` — new endpoint, new field, new enum value.
- `changed` — behavior change within a stable route (rare; should be
  patch-level only).
- `deprecated` — resource marked deprecated with a sunset date.
- `removed` — resource passed its sunset date and now returns 410.
- `security` — security-related changes.

### Data model

- **Every change to a customer-visible surface produces exactly one
  entry.** Enforced by CI: PRs that touch the OpenAPI spec must
  include a changelog entry, or explicitly label the PR
  `no-changelog-needed` (which requires reviewer approval).
- **Entries are append-only.** A deprecation entry may be *updated*
  with a `sunset_extended_at` field if the sunset date is pushed out
  (with communication to affected customers), but never removed.
- **Entry IDs are immutable.** Format:
  `YYYY-MM-DD-<kebab-case-slug>`.

### Consumers can filter

The `/v1/version/changelog` endpoint supports:

- `?since=YYYY-MM-DD` — only entries after date.
- `?type=deprecated,removed` — filter by types.
- `?scope=market_raw,gex` — filter by affected scopes.
- `?cursor=<token>` — pagination.

A consumer's daily job can call `?since=<last-poll>` and reason about
what changed. This is the machine substrate for building a webhook
subscription later (not in T1 scope).

## The customer communication protocol

Automated signals are necessary but not sufficient. World-class
customer communication requires human outreach in addition to
machine-readable signals.

### Notification tiers

Every deprecation triggers a communication workflow gated by which
customers are affected.

#### Tier 1 — All API consumers

- **Machine-readable changelog entry** at announcement time.
- **Human-readable changelog** page on docs site.
- **Deprecation banner** in the Swagger UI docs for the affected
  route.
- **RSS/Atom feed** of changelog entries (via docs site).

Runs automatically off the changelog data.

#### Tier 2 — Consumers observed calling the deprecated route

- **Directed email** to the account owner of every API key that has
  called the deprecated route in the past 30 days.
- Sent **at announcement** and again at **T-30 days** and **T-7 days**
  before sunset.
- Content: what's deprecated, what to migrate to, when the sunset is,
  a link to the migration guide, an offer to help with migration.

Requires an integrations table joining API key usage → account →
contact email. That table doesn't fully exist today. Building it is
part of T1 as a prerequisite for the policy (see Implementation notes
below).

#### Tier 3 — Named integrations

- **Direct outreach from Michael** (or the primary API owner) to any
  customer we've had a direct-line relationship with.
- **Slack / phone / email** per the customer's preferred channel.
- **Continues until acknowledged**, not just sent-and-forget.

Small customer count today, so this is manual and personal. When we
grow beyond ~20 named integrations, Tier 3 gets its own tooling.

### The deprecation workflow

```
1. Engineer proposes deprecation (in a PR + optional ADR for stable routes)
    ↓
2. CI gate confirms:
     - changelog entry present
     - sunset date ≥ minimum notice window
     - successor identified (or explicit "no successor" flag)
     - migration guide draft exists
    ↓
3. PR merges → deprecation ships in the next release
    ↓
4. On release:
     - Changelog is published
     - Banner appears on docs
     - Tier 1 automated notifications fire
     - Tier 2 email batch generates from usage table
    ↓
5. T-30 days before sunset: automated reminder emails (Tier 2)
    ↓
6. T-7 days before sunset: final reminder emails + Michael reviews
                            any customer still on the deprecated route
                            for Tier 3 direct outreach
    ↓
7. Sunset date:
     - Route returns 410 Gone with successor Link header
     - Post-sunset access is logged for 12 months to catch missed migrations
    ↓
8. 12 months after sunset: route may be removed from codebase entirely
```

### The Copilot obligation

Once ZeroGEX Copilot ships, it also serves as a deprecation-communication
surface: any customer conversation touching a deprecated field or route
gets Copilot informing them of the deprecation and offering to help
migrate.

Not in T1 scope but noted so the Copilot spec includes it.

---

## Pilot deprecation — `/api/market/vix`

The very first application of this policy. This makes the promise Michael
made to Johnnie real in code, not just email.

### Steps

1. **Restore `/api/market/vix` as a compat shim** in Tier 1 that
   proxies `/api/market/volatility?ticker=VIX`. Response shape
   unchanged from what the route returned pre-removal. Same auth,
   same rate limits.
2. **Attach the deprecation headers** to every response from the shim:
   `Deprecation: true`, `Sunset: 2027-01-31 00:00:00 GMT`,
   `Link: </api/market/volatility?ticker=VIX>; rel="successor-version"`.
3. **Add the changelog entry** — exactly as shown in the sample above,
   ID `2026-07-28-vix-route-deprecated`.
4. **Send a directed email** to Johnnie (already partially done via
   the 2026-07-17 reply; the follow-up confirms restoration + sunset
   date).
5. **Announce publicly** in the docs banner when v1 launches.

The customer already knows about the change — the pilot isn't about
re-informing them; it's about honoring the "handle API changes more
gracefully going forward" commitment by demonstrating the new policy
on the exact route that motivated it.

The 6-month sunset window is deliberate. Even though we know the sole
affected consumer has already migrated, we do the full policy on the
pilot so the operational muscle is exercised before we need it on a
route no one has migrated yet.

---

## Rationale

### Why RFC 8594 rather than custom headers

Considered custom headers (`X-ZeroGEX-Deprecated`, `X-ZeroGEX-Sunset`,
etc.). Rejected because:

1. **RFC 8594 is the standard.** SDK libraries and API clients (Insomnia,
   Postman, curl inspection tools) parse `Deprecation` / `Sunset` /
   `Link` automatically. Custom headers require every consumer to
   write parsing code.
2. **CDN and proxy support.** Cloudflare, Fastly, and other proxies
   have first-class support for these headers.
3. **Signals professionalism.** Adopting IETF standards where they
   exist is what world-class API companies do.

### Why in-payload warnings *in addition to* headers

Headers alone would suffice for programmatic consumers. But:

1. **Body warnings survive logging.** A consumer who logs response
   bodies for debugging sees the warning; header-only warnings vanish
   when the consumer only stores bodies.
2. **Body warnings survive proxy stripping.** Some corporate proxies
   strip non-standard headers. Body warnings are transport-agnostic.
3. **Body warnings can carry richer detail.** The `warnings` shape
   (ADR-0002) supports `code`, `message`, `successor`, `sunset`,
   `detail` — richer than fits in a `Link` header.

Belt-and-suspenders is intentional. The cost is a few dozen bytes per
deprecated-route response; the benefit is customers see the deprecation
via whichever channel their tooling exposes to them.

### Why 90 days for stable routes, not 60 or 180

Considered several windows:

- **30 days** — too short for automated consumers with quarterly
  release cycles. Rejected.
- **60 days** — better but still miss quarterly cycles in some
  organizations.
- **90 days** — **chosen.** One quarter is the natural planning unit
  for many customer teams. Stripe uses 90 days for API version
  deprecations, Twilio uses 12 months for major changes but 90 days
  for minor; 90 days sits at a defensible industry norm.
- **180 days** — excellent for customers, painful for us if we're
  trying to make schema improvements.

The window can be extended per-deprecation if we're not ready to
remove; the policy specifies the *minimum*.

### Why a machine-readable changelog rather than just a blog

Considered a blog / release-notes page. Rejected as the *sole*
mechanism because:

1. **Consumers can't diff a blog** for what changed since their last
   integration point. They can diff a machine-readable feed.
2. **Consumers can't filter a blog** by scope, type, or date.
3. **Automated tools** (CI checks that flag "your integration uses a
   deprecated route") need a structured feed.

We publish both. The docs site changelog page renders from the same
data as the API endpoint, so there's no divergence.

### Why require changelog entries at PR time, not release time

Because release-time changelog authoring is where drift happens. If
the engineer who made the change doesn't write the entry at PR time,
someone else has to reconstruct their intent from the diff at
release time. That's the failure mode that produced the `/api/market/vix`
incident in the first place — a change landed with no artifact
capturing customer-visible intent.

CI enforces PR-time authoring. Reviewers approve the entry as part of
the code review. By the time it merges, the entry is baked in.

## Consequences

### Positive

- **The `/api/market/vix` failure mode is architecturally impossible.**
  Deprecation without headers won't pass CI. Deprecation without a
  changelog entry won't pass CI. Deprecation without a sunset date is
  not a valid state.
- **Michael's commitment to Johnnie is honored in code**, not just
  intent.
- **Customers gain confidence.** Every future integration knows: if
  we deprecate something, they'll know weeks in advance, get a
  successor, and get direct outreach if they're actively affected.
- **Support burden drops.** "Where did this endpoint go?" support
  tickets vanish.
- **The changelog is a marketing asset.** Prospective customers see
  a serious operator with a professional API practice.

### Negative

- **PR overhead.** Every schema-touching PR now writes a changelog
  entry. That's real work per PR; amortized worth it.
- **Notification infrastructure required.** Tier 2 email requires
  usage → account → email joining that doesn't fully exist today.
  Building it is T1-blocking work — see Implementation notes.
- **Legacy shims persist.** `/api/market/vix` and future deprecated
  routes live in the codebase for 90+ days (plus 12 months of gone-sentinel).
  Code sprawl grows slightly.
- **Emergency shortcuts constrained.** The "emergency deprecation"
  exception requires an ADR — a real deterrent to using it lightly.
  Intended.

### Neutral

- **Compatible with any future SDK.** SDKs consume the changelog like
  any other client.

## Implementation notes for Tier 1

The following are prerequisites for the policy to actually work end-to-end.

1. **API-key → account → contact-email join.** Extend the `api_keys`
   table (or the account-side table it references) to always have an
   owner email. Backfill from historical account records; block new
   key provisioning without an owner email. See
   `src/api/admin_keys.py`.
2. **Usage tracking of deprecated routes.** The existing
   `UsageMeterMiddleware` records per-key request counts. Extend it
   with a per-route dimension for deprecated routes so Tier 2 notification
   generation is a simple query.
3. **The `/v1/version/changelog` endpoint.** New handler that reads
   from a `changelog.jsonl` (or equivalent structured file) in the
   repo. File is human-editable; entries are appended by PRs.
4. **CI gate for changelog entries.** A GitHub Action that:
   - Detects when the OpenAPI spec has changed.
   - Requires a matching entry in the changelog file OR the
     `no-changelog-needed` label on the PR.
   - Requires the entry's `sunset_at` to be ≥ minimum-notice-window
     from `announced_at` for `type: deprecated` entries.
5. **The email-notification workflow.** A scheduled job that on release,
   T-30, and T-7 dates queries "keys that hit deprecated routes in the
   last 30 days" and sends the templated email to their owner. Not a
   webhook system yet — start with a nightly batch.

Each of these is small individually but they add up. Budget: ~1.5
engineer-weeks inside Tier 1's overall envelope work. Non-negotiable
because they close the loop between "we have a policy on paper" and
"we execute the policy in practice."

## References

- **[RFC 8594](https://datatracker.ietf.org/doc/html/rfc8594)** —
  The Sunset HTTP Header.
- **[RFC 8288](https://datatracker.ietf.org/doc/html/rfc8288)** —
  Web Linking (`Link` header).
- **[Stripe API deprecation policy](https://stripe.com/docs/api/versioning)** —
  reference for the 90-day window and the "notify affected customers
  directly" model.
- **[Twilio deprecation policy](https://www.twilio.com/docs/usage/api-migration-changelog)** —
  reference for the notification cadence pattern.
- **[Google Cloud API deprecation policy](https://cloud.google.com/terms/deprecation)** —
  reference for stability-tier-differentiated notice periods.
- **The 2026-07-17 email exchange** — the specific customer commitment
  this policy honors.
- **[ADR-0002 §Warnings](./0002-canonical-response-envelope.md)** —
  the in-payload warnings shape.
- **[ADR-0003](./0003-api-versioning-strategy.md)** — versioning strategy
  the deprecation policy operates against.
