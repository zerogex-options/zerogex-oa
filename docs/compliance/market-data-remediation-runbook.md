# Market data remediation runbook

**Companion to:** `market-data-licensing-audit-2026-09-02.md` (finding IDs F1–F11 refer to it).
**Status:** Not legal advice. Sequencing and technical steps are engineering judgement; the
disclosure timing in Stage 4 and the self-reporting question in 2.1 are legal calls.

Ordered so that everything within our own control — and everything compounding daily — happens
before anything involving a counterparty. **Stages 0 and 1 take a week and need nobody's permission.**

---

## First: the TradeStation professional-status question

**Declaring professional does not fix the problem, and doing it now makes things worse.**

Professional is a *rate class*, not a licence. A professional subscriber is still a subscriber, and
the clause prohibiting furnishing market data to any other person or entity applies to professionals
identically. Declaring fixes exactly one of eleven findings, prospectively, and leaves every
redistribution finding untouched.

**Will we get in trouble for declaring?** The notification obligation already exists and is already
running — subscriber agreements require prompt written notice when you cease to qualify, so we are
in breach of that duty today, every day. Declaring is complying late with a duty we already have.
Exposure to back fees and penalties exists whether or not we declare; the declaration doesn't create
it, it dates it.

**Will they terminate API access?** Not for the status change itself — TradeStation serves
professional accounts and runs a Professional Desk for exactly this. What ends immediately is the
free/nominal-price arrangement: those are non-professional products.

**The real risk is the second question.** The desk will ask why the status changed — what the
business is, whether there's an entity, what the data is used for. The honest answer is the
disclosure that opens the redistribution conversation, and *that* is where termination lives. Not
the checkbox.

So the sequencing is not delay for its own sake. We get one conversation with TradeStation, and we
want it **after** redistribution has stopped and a licensed feed is live — arriving as someone who
found a problem and fixed it, not someone still doing it.

**Where this becomes a legal call:** the sequencing is about stopping the harm before opening the
conversation, not concealment. But the notification duty is live and every week of delay is a week
of breach. Weighing those is counsel's judgement, not ours. Bring them this exact question in Stage
2; if they say notify now, notify now.

---

## Stage 0 — Record the facts before changing anything (1 hour, today)

**0.1 Write down the commercial facts** (F2)
- Date of first paying subscriber — the "first business use" date that sets any back-fee window.
- Current paid subscribers split by tier; all-time count.
- Every external API integration: name, key id, scopes, start date, use.
- Every symbol ingested and when each started.
- Date real-time CME went live — **2026-08-27**, per `docs/runbooks/es_nq_futures_rollout.md`.

*Check:* a dated file stored **outside the repo**, answering all of the above without opening a DB.

**0.2 Tag the current state of both repos**
```
git tag -a pre-remediation-2026-09-02 -m "state before licensing remediation"
git push origin pre-remediation-2026-09-02
```
*Check:* both tags on the remote.

**0.3 Do not contact TradeStation yet** (→ Stage 4)
No status change, no "hypothetical" call, no support ticket about redistribution. There is no
hypothetical — they have the account on screen.

---

## Stage 1 — Stop the redistribution (48–72 hours)

All within our control, costs only engineering time, and it is the part that compounds daily.

**1.1 Inventory every live API key and its scopes** (F6)
```
make api-keys-list ACTIVE=yes
```
> **Correction to earlier advice in this session:** the
> `SCOPE_DRYRUN … would 403 if API_SCOPE_ENFORCEMENT were on` line is emitted at `logger.debug`
> (`src/api/security.py`). Unless the deployed log level is DEBUG it was never written and the
> history does not exist. Query the database via `api-keys-list` — that is authoritative.

*Check:* a table of key id, user, name, scopes, last used, each marked internal (website BFF) or
external.

**1.2 Establish what enforcement actually does in production** (F6)
- Read `API_SCOPE_ENFORCEMENT` from the **deployed environment**, not `.env.example`. Code default
  is `"0"`.
- Find every key carrying wildcard `"*"` — those pass regardless of the flag.
- Find the static break-glass key granted by `src/api/routers/tradeworkz.py` when enforcement is off.

*Check:* for each external key, one sentence stating exactly which endpoints it can reach today.

**1.3 Backfill scopes, retire wildcards, turn enforcement on** (F6)
- Assign each external key `TIER_ANALYTICS` or `TIER_SIGNALS`. **Never `TIER_FULL`** — it carries
  `MARKET_RAW` and is for the internal BFF only.
- Revoke and reissue any external key holding `"*"`.
- Set `API_SCOPE_ENFORCEMENT=1` in every deployed environment.

*Check:* an external key calling `/api/option/quote` returns **403**. Test with a real key.

**1.4 Pull `MARKET_REFERENCE` out of the external tiers** (F5)
Remove it from `TIER_ANALYTICS` and `TIER_SIGNALS` in `src/api/scopes.py`. **Correct the docstring's
theory in the same commit** — it currently argues the line is "what gets enumerated", and that
paragraph is the written record of what we believed and when.

*Check:* external key → `/api/market/quote` returns 403; `/api/gex/*` and `/api/signals/*` still 200.

**1.5 Notify affected integrators — the change, not the diagnosis** (F5)
State the change and the date, offer the derived alternative. Do not write a legal conclusion into a
customer email — not concealment, but counsel hasn't confirmed the diagnosis and a counterparty is
the wrong audience for a first draft of it.

*Check:* every affected customer has a dated written notice; we kept copies.

**1.6 Close the unauthenticated backtest share route** (F7)
`/backtesting/shared/[token]` serves derived exchange data to anyone with a link. Require an
authenticated session; give tokens an expiry.

*Check:* logged-out fetch of a known-valid token returns 401.

**1.7 Retire the per-contract quote display** (F3)
`/option-contracts` plus `/api/option/*`, `/api/flow/by-contract`, `/api/flow/contracts`,
`/api/flow/smart-money`, `/api/market/open-interest`, `/api/tools/option-calculator`.

**Cut it, don't gate it.** It is the largest fee driver in the register ($1,500/mo floor before a
single user) and the least differentiated thing we sell. Subscribers pay for GEX, walls and signals,
none of which need anyone to see a bid/ask.

*Check:* no page or endpoint returns per-contract bid/ask/last/OI to any user, internal BFF included.
Grep the response models; don't trust the routing.

**1.8 Take public real-time down to prior-session close** (F9)
`frontend/core/auth.ts` leaves `/`, `/real-time-gex-0dte`, `/spx-gamma-levels` and `/education/*`
open; `gammaLevels.tsx` fetches live summaries on a 15-minute revalidate. Publish prior-session
close levels publicly with a dated label; keep live behind login. SEO value is in the page existing
and ranking, not in the number being 15 minutes old.

*Check:* logged out, `/spx-gamma-levels` shows prior-close with a visible "as of" label; logged in,
live.

**1.9 Freeze every channel not already open** (F8)
- No new B2B API customers until Stage 3 completes.
- Mark `docs/design/pine-seeds-gamma-levels-exporter.md` **HOLD — licensing** in its first three
  lines. TradingView's suspension of new Seeds repos is luck, not a control.
- No new indicator distribution, no new social/bot data channel.

---

## Stage 2 — Counsel, agreements, feed decision (weeks 1–2)

**2.1 Engage a market-data licensing attorney**
A genuine specialism — ask whether they've handled **exchange market-data audits**. Bring the audit,
the Stage 0 facts, the channel inventory (2.3), the signed agreements (2.2). Ask specifically about:
- Stage 4 timing — when and how to notify TradeStation, and whether before migration completes.
- Whether a business/entity account is cleaner than converting a personal one.
- Whether to self-report to any exchange or wait to be asked.
- The F11 adviser/CTA question on TradeWorkz.
- Realistic back-fee exposure, to price the 2.5 options against.

**2.2 Pull the actual signed agreements** (F1, F2)
The TradeStation Technologies Subscription Agreement we accepted, brokerage market-data addenda, and
every exchange subscriber agreement clicked through — for **both usernames**, since they may differ.
A written document request is an ordinary customer request and flags nothing.

*Check:* PDFs for both usernames; F1 and F2 re-run against the real text.

**2.3 Build the channel inventory** (F8)
One page: website by tier, API by scope and customer, each chart indicator, each social/bot output,
shared backtest links, anything else.

*Check:* someone who has never seen the system could name every place our data ends up.

**2.4 Get quotes from licensed redistributors**
Databento, Polygon, dxFeed, ICE, Nasdaq Basic and similar. **Tell them exactly what we're building** —
their licensing desks will say what their agreement permits, and it's the fastest education
available. Ask each:
- Does your agreement permit **our redistribution to end users** — website and API both?
- Does it cover our **derived outputs** (GEX, greeks, signals) sold commercially?
- Which of OPRA, CTA/UTP, Cboe index, CME do you cover — and where do we still need a direct
  relationship?
- What per-user reporting and classification falls on us?
- What are the historical/storage rights, for backtesting and replay?

*Check:* three **written** quotes answering all five. Verbal assurances are worth nothing in an audit.

**2.5 Decide the sourcing model**

| Option | What it is | Indicative |
| --- | --- | --- |
| A | Direct vendor agreements with all four. Complete, heaviest, still needs a non-brokerage source. | $60–120k/yr |
| **B — recommended** | Licensed redistributor that can extend redistribution rights. Retires F1 outright. | $500–5,000/mo |
| **C — pair with B** | Narrow to derived-only. Moves us from per-user display fees to negotiated derived-data fees. | Cuts the fee base |

Steps 1.4 and 1.7 already did most of C.

---

## Stage 3 — Migrate off the brokerage feed (weeks 2–8)

Until this completes, a TradeStation cutoff is an extinction event. After it, the account is just an
account.

**3.1 Stand the new feed up in parallel** (F1)
Both feeds running, separate tables or a tagged source column. **Do not cut over first.**
*Check:* both writing concurrently for a full session, no gaps.

**3.2 Validate derived outputs across both sources**
Compare GEX summary, walls, flip, max pain and greeks from each feed over a full session. Tick
granularity and timestamping differences will move the numbers — know by how much before subscribers
notice. Extend the existing freshness/validation harness rather than writing a one-off.
*Check:* written comparison over ≥5 sessions including one OPEX and one high-vol day, with
differences explained.

**3.3 Cut over, keeping TradeStation warm**
Serve from the licensed feed; leave old ingestion running but non-authoritative for a week.
*Check:* production analytics read only from the licensed source, verified in the query path.

**3.4 Retire TradeStation as a production dependency** (F1)
- Remove `TRADESTATION_REFRESH_TOKEN` and `TRADESTATION_FUTURES_REFRESH_TOKEN` from every deployed env.
- Revoke the OAuth app's authorisation from both usernames.
- Remove or quarantine the backfill tools that call it.

*Check:* **zero TradeStation API calls in production logs for seven consecutive days.** That is the
gate to Stage 4 — not "we think it's off."

**3.5 Deal with the stored history** (F7)
The TimescaleDB corpus was built from the brokerage feed. Ask counsel and the new vendor what happens
to it — some vendors' historical rights let us keep serving derived outputs from it, some don't, and
the answer decides whether backtesting survives in its current form.
*Check:* written vendor answer on pre-existing history; a documented retention policy replacing
"indefinite".

---

## Stage 4 — Resolve the TradeStation account (after 3.4, with counsel)

**4.1 Let counsel choose the shape of the disclosure**
- **Convert to professional** on the existing personal account. Simplest; professional rates apply,
  free package ends.
- **Close the second account**, keep one genuinely non-professional personal account for own trading.
  Clean, and after Stage 3 the business no longer needs the feed.
- **Open a business/entity account** with professional data. Separates personal trading from the
  company; a better structure for any later conversation.

**4.2 Answer honestly when asked**
The desk will ask what changed. **Tell them the truth.** Misrepresenting on a broker or market-data
certification converts a licensing breach into a misrepresentation — categorically worse, and it
forecloses the negotiated outcome. Being able to say "we identified this, stopped it, and migrated to
a licensed source" is worth a great deal, and is the entire reason Stages 1–3 come first.

**4.3 Expect the free data to end immediately**
Free and nominal-price packages are non-professional products. Budget for professional rates from the
change date, or for closing the account.

---

## Stage 5 — Build the compliance surface (days 30–90, then ongoing)

**5.1 Pro/non-pro self-certification at signup** (F10) — **do this first, before we have a licence.**
Long-lead item, harmless if unneeded, painful to retrofit across an existing user base. Produces the
per-user split that makes F3 reportable.
*Check:* every new user classified at signup, existing users prompted on next login, count by
classification available on demand.

**5.2 Add the market-data terms we don't have** (F10)
Source attribution and prescribed exchange disclaimers; a subscriber-agreement passthrough binding
users to exchange terms; a delay/accuracy disclosure (`FuturesDelayBadge` is already the right
pattern in the UI — the terms need the counterpart). All five locales.

**5.3 Build the usage-reporting pipeline** (F3)
User counts by classification, per feed, monthly. Assume it will be asked for on short notice and for
prior periods.
*Check:* a report generates for an arbitrary past month without manual work.

**5.4 Declare every channel to the licensor** (F8)
Hand over the 2.3 inventory. Undeclared channels turn a fee dispute into a good-faith dispute.
*Check:* written licensor acknowledgement covering each channel, indicators included.

**5.5 Reopen what we froze, under the new terms** (F8)
B2B API sales, chart integrations, Pine Seeds if it reopens — each checked against what the licence
permits, not what we hope it does.
*Check:* each reopened channel maps to a specific clause.

**5.6 Keep an audit file and review annually**
Signed agreements, monthly reports, channel inventory, the audit and this runbook with revisions.
Review each **January**, when all four exchanges reprice.

---

## Things that turn this into a worse problem

- **Don't misrepresent status or use** — on a form, a call, or to a vendor. A licensing breach is
  negotiable; a misrepresentation on a broker certification is a different category and removes our
  best outcomes.
- **Don't open a third account or move entitlements again.** F1 already reads as deliberate
  structuring across two usernames. Extending the pattern converts a licensing question into a
  bad-faith question.
- **Don't quietly un-ship ES/NQ and hope.** The pricing pages advertise them in five languages and
  are indexed and cached.
- **Don't call the Professional Desk to "ask hypothetically."** There is no hypothetical.
- **Don't write legal conclusions into customer or vendor emails.** Sequencing, not concealment.
- **Don't let Stage 1 wait for Stage 2.** Nothing in Stage 1 needs a lawyer's permission, and it is
  the part that compounds daily.

---

## If there is one hour

Steps **1.1, 1.2, 1.3**. Find out whether scope enforcement is actually running in production, list
which external keys can currently reach the option chain and the underlying quote endpoints, and shut
those off.

Everything else here is a plan. That one is a fact we don't currently have, and it determines whether
the largest finding is "we showed data to our own subscribers" or "we sold it to third parties."
Those are very different conversations, and we should know which one we're having before anyone else
does.
