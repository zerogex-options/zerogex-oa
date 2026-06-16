# ZeroGEX Growth & Productization Roadmap

_Last updated: 2026-06-16_

> **Update 2026-06-16:** The Stream-1 PostHog funnel is no longer a to-do — it
> was built, shipped, merged to `release`, and verified **live** (real
> `zerogex.io` traffic confirmed flowing into PostHog). Details inline below.
> Observed already: inbound traffic arriving via `utm_source=chatgpt.com`. No
> PRs were opened (per direction); work is on branch
> `claude/zerogex-revenue-potential-rfmuxa` and in `release`.

This document captures the strategy and action plan developed for ZeroGEX.
It is organized around the two parallel streams of work:

1. **Stream 1 — Convert users into paying subscribers (B2C).**
2. **Stream 2 — Parlay the engine into a B2B data/analytics service** layered
   on top of the existing B2C product.

A third, **cross-cutting concern** — market-data licensing — touches both
streams and is broken out at the end, along with a single sequenced action
plan and a glossary.

---

## 0. Where things stand (June 2026)

**Product:** Two repos —
- `zerogex-oa`: the backend "engine" (real-time options ingestion, Greeks/IV,
  gamma-exposure analytics, flow classification, a 13+ signal engine) exposed
  via a FastAPI REST API.
- `zerogex-web`: the Next.js subscriber-facing app (dashboards, auth, Stripe
  billing) that calls the `oa` API.

**Traction (as of mid-June 2026, ~2 weeks after payments went live June 1):**
- 277 total signups with valid email.
- 15 paying subscribers (~5.4% signup→paid from a cold start).
- ~125 "Founding Members" on free trial until July 1 (discounted rates if they
  convert).
- X presence started this week (daily premarket/midday gamma + wall posts with
  site screenshots). No Discord, newsletter, or YouTube yet.

**Honest odds (conditional on executing the Stream-1 moves below well):**

| ARR target | ~1 yr | ~2 yr | ~3 yr | ~5 yr |
|-----------|-------|-------|-------|-------|
| $100K | 55–65% | **80–85%** | ~90% | ~92% |
| $250K | 18–25% | **45–55%** | ~62% | ~70% |
| $500K | ~8% | 22–30% | 38–48% | ~55% |
| $1M | <5% | 8–12% | **25–35%** | ~45% |

**Takeaway:** The engineering risk is largely retired — the engine is real and
production-grade. The remaining risk is almost entirely **distribution**
(Stream 1) and **business/legal decisions** (licensing). $100K within two years
is the base case; everything above depends on churn control and whether the
audience compounds.

---

## Stream 1 — Convert users into paying subscribers (B2C)

### The core constraint: distribution, not product

The market is proven (SpotGamma, Unusual Whales, etc. all exceed $1M ARR) and
large (millions of active options traders; ~1,700 subs at ~$50/mo = $1M ARR).
The product is built. What's missing is the **machine that turns strangers into
trials and trials into durable subscribers** — and the ability to *measure* it.

### The two numbers that govern everything (instrument these FIRST)

1. **Trial → paid conversion %** — the SaaS lifeline metric. You run a 7-day
   trial; what fraction convert?
2. **Monthly churn %** — trading tools churn hard (5–10%/mo). Churn is the
   silent cap on every ARR figure above; at 8%/mo you're refilling a leaking
   bucket.

You currently cannot see either. Until you can, every marketing hour is spent
partly blind.

### Recommended moves (in priority order)

**1. Instrument the funnel + close the loop by hand.** _(highest ROI, ~free, do now)_
- ✅ **DONE (2026-06-16) — PostHog funnel built, shipped to `release`, verified
  live.** Code in `zerogex-web` under `frontend/core/telemetry/` (`events.ts`,
  `posthog-client.ts`, `posthog-server.ts`; `TelemetryProvider.tsx`) — renamed
  from `analytics`. Off by default (no key = no-op); autocapture + session
  recording OFF (explicit events + manual pageviews only). Events wired:
  `pageviews + identify` (ClientLayout, keyed to user id), `signup` (register
  success), `first_value` "aha" (gamma-exposure first render, ref-guarded),
  `checkout_started` (pricing subscribe), and server-side via the Stripe webhook
  on status transitions: `trial_started`, `subscription_paid` (incl. trial→paid),
  `subscription_cancelled`. Client + server stitch to one person via shared user
  id. To activate: set `NEXT_PUBLIC_POSTHOG_KEY` (+ optional
  `NEXT_PUBLIC_POSTHOG_HOST`), deploy — already confirmed flowing.
- **Still to do (the human half):** build the funnel view in PostHog
  (`signup → first_value → checkout_started → trial_started →
  subscription_paid`) + a churn view off `subscription_cancelled`; and at 277
  signups you're still small enough to **email/DM every non-converter and trial
  drop-off** and ask why. That qualitative gold disappears at scale.
- _Why:_ you can't optimize a funnel you can't see; this tells you the single
  highest-leverage thing to fix.

**2. Stand up a Discord, gate premium channels to paid subs.** _(attacks churn)_
- Post intraday reads there; let members discuss; lock live/premium channels
  behind the paywall.
- _Why:_ community is competitors' real moat. It's both a **retention**
  mechanism ("I'd lose the room") and an **acquisition** flywheel (members
  share wins/screenshots/invites). Directly targets churn, the #1 structural
  risk.

**3. Convert ephemeral X reach into an owned email list (free newsletter).** _(durable top-of-funnel)_
- Beehiiv/Substack; same premarket/midday read daily, with a "full dashboards
  at zerogex.io" CTA. Pipe every X bio click and site visitor into it.
- _Why:_ X posts vanish and X owns the relationship. A newsletter is an
  **owned** audience you can market to repeatedly that survives algorithm
  changes. Newsletters convert to trials well in fintwit.

**4. (Bonus) Free public lead-magnet page.** _(SEO + top-of-funnel + licensing-clean)_
- Publish your daily SPX/SPY gamma levels + put/call walls as a free,
  watermarked, **15-min-delayed** public page that ranks for "SPX gamma levels"
  searches and gives X users a daily reason to click through.
- _Why:_ compounding SEO traffic, and it doubles as the legally-clean delayed
  view of raw data (see Licensing section).

### The Stream-1 licensing item

The `option-contracts` page on `zerogex-web` is the one consumer-facing place
that shows raw, live, per-contract option quotes — and it is the bulk of your
B2C licensing exposure. **Decide its fate** (see the Licensing section): cut,
15-min delay, or license. This single decision retires most of the risk.

---

## Stream 2 — Parlay the engine into a B2B service

### The thesis

The most valuable assets aren't "a retail website" — they're reusable:
1. A working real-time options data pipeline.
2. A **derived-analytics engine** (GEX, flip, max pain, vanna/charm, flow
   classification) — licensing-clean computed output.
3. A signal layer (13+ signals, composite Market State Index).
4. A clean, authed, scoped API.
5. A polished, embeddable frontend.
6. A **proprietary, compounding historical dataset** of computed dealer-gamma
   positioning and classified flow.

Businesses buy #2, #4, and #6.

### B2B opportunities (ranked by fit / effort)

**Tier 1 — natural parlays, near-zero marginal cost:**
- **Analytics-as-a-service API (B2B2C).** Sell derived GEX/flow/signal analytics
  via API to other businesses (smaller broker/charting apps, trading-tool
  startups, newsletters, Discords, RIAs, prop shops). One customer at
  $500–2,000/mo replaces 10–40 retail subs and churns far less.
- **White-label / embeddable widgets.** License the dashboard components to
  influencers/newsletters/platforms under their own brand ($200–1,000/mo each;
  each markets you for free).

**Tier 2 — real money, harder/slower:**
- **Alt-data: sell the historical dataset** to quant funds/researchers.
  Compounding asset; licensing-clean. _Start archiving clean daily snapshots
  now even if you sell later — value is in the length of the series._
- **Prop firms / small funds desk tool** (per-seat B2B). Bigger economics, but
  long sales cycles and heavy data-sourcing scrutiny.

**Tier 3 — domain pivot of the same engine:**
- **Crypto options gamma (Deribit).** Sidesteps the entire OPRA/exchange
  licensing problem and skews to B2B-native buyers. Second product to maintain;
  not until equities side is stable.

### What's already built (this session — `zerogex-oa`, branch `claude/zerogex-revenue-potential-rfmuxa`)

The reusable foundation for both the B2B tier **and** the B2C raw-data
lockdown. **All off by default — zero behavior change until enabled.**

- **Scope→tier authorization map** (`src/api/scopes.py`):
  - Capability scopes: `gex`, `flow`, `maxpain`, `technicals`, `signals`, and a
    dedicated `market_raw` that isolates raw, license-restricted data.
  - Tier bundles: `analytics` (derived only — the clean B2B product), `signals`
    (+signals), `full` (the only bundle granting `market_raw` — internal BFF
    only).
- **Endpoint wiring** (`src/api/main.py`): `require_scopes(...)` on all 25
  inline analytics endpoints + the routers; raw endpoints gated behind
  `market_raw`. Inert until enforcement is on; wildcard `*` keys always pass.
- **CLI** (`src/api/admin_keys.py`): `--tier analytics|signals|full` provisions
  a key with the right scope bundle in one command.
- **Durable usage metering** (`src/api/usage.py`, `api_usage_daily` table,
  `UsageMeterMiddleware`): counts requests per `(UTC day, caller, key,
  end-user)`, flushed via increment-UPSERT — survives restarts, sums across
  workers, supports per-account and per-seat (B2B2C) billing. No-op unless
  enabled.
- **Tests:** 24 new + 72 existing API tests pass; `black`/`flake8` clean.

### How to activate later (config only, no code changes)

Do this **in order** — it's gradual and reversible:

1. **Backfill scopes** on existing keys (they currently have `[]`):
   - Web BFF key → `full` (or wildcard `*`). _Must happen before step 2 or the
     website breaks._
   - B2B customer keys → `python -m src.api.admin_keys create <user> --name <x>
     --tier analytics`.
2. **`API_SCOPE_ENFORCEMENT=1`.** Before this, the system runs in **dry-run**:
   it logs "key WOULD be denied X" without denying. Watch the logs for a few
   days, confirm nothing legitimate would break, then flip it on.
3. **`API_USAGE_METERING_ENABLED=1`.** Independent and harmless; starts
   populating `api_usage_daily`.

### What's left to build (to make it a sellable B2B product)

- **Dev portal** — self-serve key generation/rotation + a usage dashboard +
  docs, so customers onboard without you running a CLI.
- **Stripe metered billing** — read `api_usage_daily` and report usage to
  Stripe (flat tier + overage). _Blocked on the licensing decision below._
- **Redis multi-worker rate limiting + response cache** — current limiter
  counts per-process; Redis shares the count across workers and caches hot
  derived responses so N customers don't multiply DB load. (Code already notes
  this upgrade path: `slowapi` + Redis.)
- **API versioning (`/v1/`) + public status page** — expected by B2B
  procurement.

### Minimal validation path (test demand cheaply)

You don't need all of the above to validate:
1. Backfill scopes + flip enforcement (~1 day).
2. Confirm metering populates (~done, just enable).
3. Hand a `--tier analytics` key to **one** design partner from your X network
   (a newsletter/Discord) at a flat monthly price. No portal, no Stripe
   automation yet.
4. If a couple stick, _then_ build the portal + automated billing + do the feed
   migration.

---

## Cross-cutting — the market-data licensing decision

### Raw vs. derived (the key distinction)

- **Raw market data** (live per-contract bid/ask/last/volume/OI, underlying
  OHLC) is heavily license-restricted for redistribution. Source is OPRA/the
  exchanges; TradeStation's API is almost certainly **personal-use, not
  redistribution-licensed.**
- **Derived analytics** (GEX, flip, max pain, signals, classified-flow
  aggregates) are broadly redistributable — they're your computed IP.

There's also a **display vs. datafeed** axis: a human-readable chart of raw
quotes ("display") is a lighter licensing category than an open consumable JSON
feed ("datafeed") — but **both still require a license for live raw data.** A
chart is not a loophole. Only **derived** data or **15-min-delayed** raw data is
free-and-clear.

### Two decisions to make

**A. B2C — the `option-contracts` page:**
- **Cut it** — removes the risk entirely; minimal product impact (most
  competitors don't show a raw chain).
- **15-min delay** — keeps the feature, becomes legally clean for free.
- **License it** — only if a live chain is genuinely strategic (audit suggests
  it isn't).

**B. B2B — the upstream feed (prerequisite for selling B2B):**
- Migrate ingestion from TradeStation to a **redistribution-licensed vendor**
  (Cboe / Polygon / dxFeed). Only `tradestation_client.py` changes; everything
  downstream stays.
- _Why required for B2B:_ as a data vendor, customers' legal teams will ask "are
  you licensed to give us this?" You need to answer "yes" with a contract.
- _Bonus:_ also kills the single-broker dependency (a resilience win).
- _This unblocks Stripe metered billing_ — no point billing for data you can't
  yet legally sell.

---

## Sequenced action plan (what to do, in what order)

**Now (Stream 1, cheap, highest leverage):**
1. ✅ PostHog + funnel instrumentation added to `zerogex-web` (live as of
   2026-06-16). Remaining: build the funnel + churn views in the PostHog UI.
2. Manually email/DM non-converters and trial drop-offs.
3. Start the free newsletter; publish the free delayed lead-magnet page.
4. Stand up Discord; gate premium channels.
5. Watch trial→paid and churn; let them tell you which ARR column you're in.

**Near-term (decisions, low effort, high consequence):**
6. Decide the `option-contracts` fate (cut / delay / license).
7. Begin archiving clean daily snapshots of derived data (alt-data asset).

**When ready to pursue B2B:**
8. Make the upstream-feed/licensing decision (vendor migration).
9. Activate scopes (backfill → `API_SCOPE_ENFORCEMENT=1`) and metering.
10. Land one B2B design partner with a manually-issued `analytics` key.
11. If it sticks: build dev portal → Stripe metered billing → Redis
    limiting/cache → API versioning + status page.

**The throughline:** the code foundation is done and dormant. The next moves are
mostly **business/marketing/legal decisions**, not engineering. Make the
licensing call and the remaining build items get a green light.

---

## Glossary

- **ARR** — Annual Recurring Revenue.
- **BFF (Backend-for-Frontend)** — the website's own server; it calls the `oa`
  API on behalf of logged-in users using one shared key.
- **Scope** — a permission label on an API key (e.g. `gex`, `market_raw`).
- **Tier** — a named bundle of scopes (e.g. `analytics`, `full`) = the unit of
  commercial packaging.
- **Backfill** — assigning scopes to keys that currently have none.
- **Metering** — counting per-customer API requests for billing/quotas.
- **Metered billing** — charging based on usage (vs. a flat subscription).
- **OPRA** — the authority governing options price data; the root of raw-data
  licensing rules.
- **Derived data** — computed analytics output (broadly redistributable).
- **B2B2C** — selling to a business that serves its own end-users with your data
  (attributed here via the website's `X-End-User-Token`).
