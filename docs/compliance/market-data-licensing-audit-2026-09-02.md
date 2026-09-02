# Market data licensing audit — 2026-09-02

**Scope:** `zerogex-oa` + `zerogex-web` at `release`.
**Status:** Engineering/risk read against published exchange policy. **Not legal advice.**
**Companion:** rendered version at the audit artifact (see PR/commit thread).

---

## Verdict

ZeroGEX is a Stripe-billed subscription product with a public website, a paid B2B API and
third-party chart integrations, running on a retail brokerage account's **non-professional**
market-data entitlement.

Every US exchange whose data we touch — OPRA (option chain), CTA/UTP (SPY, QQQ, IWM), Cboe and
Nasdaq (SPX/NDX/VIX/VXN index values), CME (ES, NQ) — defines a non-professional subscriber as an
individual using data solely for personal, non-business purposes, for their own property, and never
for the benefit of another person or entity. We use it to run a business, on behalf of paying
subscribers, and resell it through an API.

That single fact invalidates the rate class on all four feeds simultaneously and converts every
downstream distribution into unlicensed redistribution.

**The risk to price is not litigation.** It is TradeStation switching the account off with no
notice, taking the product dark while subscribers are billed, followed by a back-fee invoice.

11 findings: 4 critical, 4 high, 3 medium.

---

## Findings

Numbering is **remediation order**, not severity ranking. F6 is sixth in seriousness but takes an
afternoon and resizes F3 and F5 — run it first, in parallel.

### F1 — Commercial product on a retail brokerage feed breaches the TradeStation agreement
**Critical · Likelihood: High · Start now**

The TradeStation Technologies Subscription Agreement and the exchange subscriber agreements
accepted through it permit personal, non-business use and prohibit furnishing market data to any
other person or entity. Production ingestion holds persistent streams across ~800 option symbols
per connection, multiple connections per underlying, cash session and overnight.

The two-username arrangement aggravates rather than mitigates: splitting
`TRADESTATION_REFRESH_TOKEN` from `TRADESTATION_FUTURES_REFRESH_TOKEN` so a second,
nominally-priced account carries the CME entitlement is documented in this repository and reads as
deliberate structuring of entitlements across accounts.

Evidence: `src/tools/tradestation_whoami.py`; `docs/runbooks/es_nq_futures_rollout.md` §1a;
`src/config.py` stream fan-out sized for 1,100–1,500 symbols per process.

**Consequence:** account and API termination, likely without notice. Total product outage.
Back-billing at professional rates. Possible referral to the exchanges.
**Do:** assume this feed disappears. Stand up a licensed source *in parallel* before changing
anything about the account, so a cutoff is a migration and not an extinction event.

### F2 — Non-professional subscriber status is invalid on all four feeds
**Critical · Likelihood: High · ≤ 7 days**

OPRA: non-professional means use "only in connection with individual investment activities, not in
connection with any trade or business activities, and not for the benefit of any other person or
entity." CME additionally requires use limited to managing your own property, not third-party
property in any capacity, whether or not remunerated. CTA/UTP and Cboe track the same shape.

A subscription business with tiers, trials, founding lock-ins and a cancellation-retention flow is
a trade or business; its subscribers are other persons. This is not a risk forecast — it is the
current state.

**Consequence:** retroactive re-rating to Professional from first business use, plus back fees and
penalties. OPRA professional display alone is $31.50/user/month, before equity, index and futures
feeds. Back-billing runs from first business use, so every month of delay enlarges the invoice.
**Do:** record the date business use began and current paid-subscriber count by tier. Both are
needed for any true-up and reconstructing them later is far worse.

### F3 — OPRA chain data displayed to paying subscribers with no vendor agreement
**Critical · Likelihood: Moderate · ≤ 30 days**

`/option-contracts` renders per-contract Last, Bid, Ask, Bid Vol, Ask Vol, Volume and Open Interest
to logged-in subscribers — verbatim OPRA data on a screen someone paid to see.

`src/api/scopes.py` gets the diagnosis right: it isolates `MARKET_RAW` as license-restricted, holds
it from external customers, reserves it for the internal BFF. **But the internal BFF renders the
page subscribers look at.** Withholding it from API customers while serving it to website
subscribers is not the line the licence draws — both are redistribution to end users.

Surfaces: `/api/option/quote`, `/api/option/contract`, `/api/market/open-interest`,
`/api/flow/by-contract`, `/api/flow/contracts`, `/api/flow/smart-money`,
`/api/tools/option-calculator`.

**Licence cost if kept:** $1,500/mo OPRA redistribution + $1.25/non-pro user/mo + $31.50/pro
user/mo + monthly usage reporting and user classification.
**Do:** decide whether the per-contract quote table earns a $1,500/month floor. It probably does
not — retire it and keep the derived surfaces.

### F4 — CME futures redistributed and derived from, with neither licence
**Critical · Likelihood: Moderate · ≤ 30 days**

Real-time CME live since 2026-08-27; ES/NQ are a headline `/pricing` feature in five languages.
Four endpoints serve **observed** CME prices natively — `/api/market/quote`,
`/api/market/historical`, `/api/market/session-closes`, `/api/market/session-levels` — to
subscribers and to external keys holding `market_reference`.

Separately, `src/api/futures_middleware.py` projects SPX/NDX surfaces onto the futures axis using a
measured basis ratio and publishes that ratio. **That projection is a derived work from CME data**,
licensed separately under CME's Derived Data License, priced per instrument on a tiered schedule.

Two breaches, not one: *distribution* of observed prices, and *creation of a derived work*.

CME is the most audit-active of the four and most assertive on derived data. Mitigating: live only
since 2026-08-27, so the back-fee window is measured in weeks.

**Do:** open a Derived Data Scope of Use conversation with CME Data Services, or source ES/NQ from
a licensed redistributor. Do not un-ship quietly — the marketing pages are indexed.

### F5 — `MARKET_REFERENCE` sold to external customers on a mistaken licensing theory
**High · Likelihood: Moderate · ≤ 30 days**

`src/api/scopes.py` documents `MARKET_REFERENCE` as "the UNDERLYING's own tape", calls the derived
scopes "broadly redistributable", and bundles it into `TIER_ANALYTICS` and `TIER_SIGNALS` — the
tiers sold to external B2B customers.

The stated rationale is that "the line between the two is *what gets enumerated*, not whether the
data is upstream." **That is not the line the exchanges draw.** The trigger is whose data it is and
whether an end user sees it. SPY/QQQ/IWM real-time quotes are CTA/UTP consolidated data.
SPX/NDX/VIX/VXN are proprietary Cboe and Nasdaq index data — Cboe states external redistribution of
its index data is prohibited without a licence. Serving one symbol's live price is redistribution
just as much as walking a chain.

This is the most consequential conceptual error in the codebase: it is the written justification
currently underwriting third-party sales. It is a reclassification, not a rebuild.

**Do:** correct the docstring's theory first, then the bundle. The comment is what gets read back
in an audit as evidence of what we believed and when.

### F6 — Scope enforcement may be inert in production
**High · Likelihood: High · ≤ 72 hours**

`src/api/security.py:110` defaults `API_SCOPE_ENFORCEMENT` to `"0"`. The comment above states
"every key provisioned to date defaults to `[]` … no key→tier mapping has ever been backfilled." A
key with wildcard `"*"` always passes. `src/api/routers/tradeworkz.py:41` grants a static
break-glass key while enforcement is off. `.env.example:176` sets `1` — intent, not production
state.

Three independent ways for the control to be inert: the flag, the empty scope lists, the wildcard.
If any holds in production, **F3 stops being "displayed to our own subscribers" and becomes "sold
to third parties"** — a materially larger finding that also reaches our customers' compliance
posture.

**Do:** pull the existing `would 403 if API_SCOPE_ENFORCEMENT were on` log line for the last 30
days — that is the inventory of who is currently over-entitled. Then backfill scopes, retire the
wildcard, turn enforcement on.

### F7 — Historical storage, replay and backtesting distribute data under rights we do not hold
**High · Likelihood: Moderate · ≤ 30 days**

Historical distribution is a distinct right from real-time, licensed and priced separately by all
four exchanges. We retain per-contract history in TimescaleDB indefinitely, expose `/api/replay/`
and `/api/backtest`, and sell backtesting as a Pro feature.

CME's Subscriber Feed Distribution License for historical information distribution is $30,000 per
DCM annually; CME treats data taken more than eight hours after publication as historical. A retail
entitlement conveys no historical distribution right at all.

Sharpest edge: `/backtesting/shared/[token]` publishes backtest output to anyone holding a link,
with no authentication — derived exchange data on a public URL, outside every access control.

**Do:** put share tokens behind auth and expiry now. Then set and document a retention policy for
raw per-contract history — "indefinite" is the most expensive answer in any licensing negotiation.

### F8 — Chart-platform integrations are an undeclared onward distribution channel
**High · Likelihood: Moderate · ≤ 60 days**

NinjaTrader and Sierra Chart indicators, the shipped TradingView Pine script and the `PlotOn…`
components push levels into third-party platforms. Every vendor agreement treats onward
distribution into another vendor's environment as its own declarable, often separately-priced
channel. A distribution right we were never granted cannot be inherited or passed through.

`docs/design/pine-seeds-gamma-levels-exporter.md` needs a specific hold: it would publish daily
levels into a public GitHub repo TradingView ingests — unbounded, permanent, public redistribution.
It is shelved only because TradingView suspended new Seeds repositories. **Treat that suspension as
luck, not a control.** Do not un-shelve before licensing lands.

**Do:** write a one-page inventory of every channel data leaves through — website, API, each
indicator, each social bot. Required as an exhibit for any licence application; far cheaper now
than under audit.

### F9 — Public SEO pages disseminate derived exchange data to an unbounded audience
**Medium · Likelihood: High · ≤ 30 days**

`PUBLIC_ROUTE_PATTERNS` in `frontend/core/auth.ts` leaves `/`, `/real-time-gex-0dte`,
`/spx-gamma-levels` and all of `/education/*` open and crawlable. `gammaLevels.tsx` fetches
`/api/gex/summary` for four symbols with `revalidate = 900` — values computed from real-time
exchange data, to anyone, on a fifteen-minute cycle.

As a breach this is the mildest item; the values are derived, not substitutive. Likelihood is high
for a different reason: **this is the detection vector.** `robots.ts` and the sitemap actively
invite indexing, and a licensed competitor with an incentive to complain can find us with one
search. Exchange enforcement against small vendors often starts with a competitor's email.

**Middle path:** publish yesterday's close levels publicly, keep live values behind login. Retains
the SEO — which lives in the page existing and ranking, not in the number being 15 minutes old.

### F10 — No market-data disclosures, attribution or subscriber passthrough in the terms
**Medium · Likelihood: High · ≤ 60 days**

`frontend/app/terms/Client.i18n.ts` never mentions market data, TradeStation, exchanges, delays or
exchange disclaimers. It restricts *our users* from redistributing *our* content and says nothing
about the rights we hold upstream — an asymmetry an auditor notices immediately.

Every vendor agreement requires prescribed attribution and disclaimer language and requires the
vendor to bind its subscribers to the exchange's terms. We will need a subscriber-agreement
passthrough at signup and pro/non-pro self-certification per user — the same classification that
feeds the F3 reporting obligation. None exists today.

Credit where due: `FuturesDelayBadge` is the right pattern — disclosure derived from measured state
rather than a hardcoded string someone must remember to update. It needs a counterpart in the terms.

**Do:** add pro/non-pro self-certification to signup now, before a licence exists. Long-lead item,
harmless if unneeded, painful to retrofit across an existing user base.

### F11 — Adjacent: paid trade signals may trigger adviser or CTA registration
**Medium · Likelihood: Low · ≤ 90 days**

Outside data licensing, but it compounds F2. TradeWorkz bots decide "when to enter, how big to
size, when to add to or cut a position, and when to close" and compete on a leaderboard at
`/trading-signals`. Pro tiers sell Trade Bias, Composite Score and a named-pattern playbook.

Selling specific, actionable trade recommendations for compensation can implicate the Investment
Advisers Act, and for ES/NQ signals, CTA registration with the CFTC/NFA. The publisher's exclusion
under §202(a)(11)(D) protects bona fide publications of general and regular circulation and is a
real argument here — but a subscription service issuing specific entries and sizing sits closer to
the line than a newsletter about gamma.

The interaction is the point: **being an investment adviser is a categorical disqualifier for
non-professional data status** under every definition in F2. If the answer is "adviser", F2 hardens
from arguable to settled.

---

## Remediation plan

### Phase 0 — this week
1. Verify scope enforcement in production; enumerate every live API key and its scopes (F6).
2. Withdraw `market_raw` and `market_reference` from all external keys; notify affected integrators
   plainly — a product change now beats a breach conversation later (F3, F5).
3. Freeze new B2B API sales, the Pine Seeds exporter, and any new data-bearing channel (F8).
4. Put backtest share tokens behind auth and expiry (F7).
5. Record: date business use began, paid subscriber count by tier, external integrations, symbols
   ingested (F2).

### Phase 1 — days 7–30
1. Choose the sourcing model (below) and start vendor conversations. Nothing else unblocks first.
2. Engage a market-data licensing attorney — a genuine specialism; one with an exchange-audit
   practice pays for itself in the first negotiation.
3. Decide the raw-chain question (F3) and the public real-time question (F9) explicitly.
4. Correct the `scopes.py` licensing theory and rebundle the tiers (F5).
5. Stand the replacement feed up in parallel before touching the TradeStation account (F1).

### Phase 2 — days 30–90
1. Execute the licences or the migration; cut production over and retire the brokerage feed as a
   production dependency.
2. Ship the compliance surface: pro/non-pro self-certification, subscriber-agreement passthrough,
   attribution and exchange disclaimers (F10).
3. Build the usage-reporting pipeline — user counts by classification, per feed, monthly.
4. Set and document raw-history retention (F7).
5. Get the adviser/CTA read (F11).

### Phase 3 — days 90–180
1. Declare every distribution channel to the licensor; reopen third-party integrations under the
   new terms (F8).
2. Establish the reporting cadence and keep an audit file — agreements, monthly reports, channel
   inventory, this document and its revisions.
3. Set an annual review against the January fee-schedule updates, when all four exchanges reprice.

---

## The sourcing decision

| Option | Approach | Indicative cost |
| --- | --- | --- |
| **A** | Direct vendor agreements with OPRA, CTA/UTP, CME and Cboe. Correct, complete, heaviest possible answer for our size: four negotiations, four reporting obligations, four audit surfaces. Does not solve F1. | ≈ $60–120k/yr + overhead |
| **B — recommended** | Buy from a licensed redistributor that can extend redistribution rights (Databento, Polygon, dxFeed, ICE, Nasdaq Basic, …). They carry the exchange relationships and much of the reporting; we carry per-user fees and classification. Retires F1 outright. | ≈ $500–5,000/mo |
| **C — pair with B** | Narrow to derived-only: retire raw per-contract display and `market_reference` resale; publish only computed outputs that cannot substitute for the underlying feed. Moves us from the expensive per-user *display* regime into the cheaper negotiated *derived data* regime. `scopes.py` already half-designed this. | Cuts the fee base sharply |

**Recommendation: B + C.**

### Indicative published fees — confirm against current schedules

| Item | Regime | Fee | Applies to |
| --- | --- | --- | --- |
| OPRA redistribution | Vendor agreement | $1,500/mo | Flat, any redistributor (F3) |
| OPRA non-professional display | Per user | from $1.25/mo | Each non-pro end user (F3) |
| OPRA professional display | Per user/device | $31.50/mo | Each pro end user (F2, F3) |
| CME derived data | Derived Data License | Tiered per instrument | The basis projection (F4) |
| CME historical distribution | Subscriber Feed Distribution | $30,000/DCM/yr | Replay and backtesting (F7) |
| Cboe index data | Index feed licence | $1,900/yr admin + licence | SPX, VIX values (F5) |

---

## Limits of this audit

- **Not legal advice.** This is an engineering and risk read against publicly available exchange
  policy, prepared so a specialist can be briefed quickly rather than paid to discover our
  architecture.
- Several primary sources were unreachable from the audit environment and were read through
  secondary summaries. **The fee figures in particular must be confirmed** against current
  schedules before being relied on.
- **The actual signed agreements were not reviewed.** Pull the TradeStation Technologies
  Subscription Agreement, the brokerage market-data addenda and every exchange subscriber agreement
  clicked through, and re-run F1 and F2 against the real text. Our specific terms govern, not the
  published templates.
- Likelihood ratings are judgement, not actuarial figures. They assume current visibility and no
  adverse event; a competitor complaint or a TradeStation account review moves several up a band.
- F11 is a different regulatory regime with different counsel. It is here only because it changes
  the analysis in F2.

**Most useful next hour:** verify F6 in production, then book the market-data licensing attorney.
Everything else can wait a week. Those two cannot.
