# What to do about the data problem — step by step

**Companion to:** `market-data-licensing-audit-2026-09-02.md`. That one is written for a lawyer and
uses finding IDs (F1–F11). **This one is written for whoever does the work.**

Not legal advice. The technical steps and the ordering are engineering judgement. *When and how we
talk to TradeStation* is a legal decision — that's step 10, and it's in week one for that reason.

---

## The problem, once more

We bought a home cable subscription. We've opened a sports bar and we're charging people at the door
to watch the TV.

The exchanges — CME, Cboe, Nasdaq, and a group called OPRA that handles options prices — own the
actual price data. TradeStation resells it to us. When we signed up we agreed the data was **for
personal trading, and that we wouldn't pass it to anyone else.**

We now run a business on it and pass it to paying customers. That's the whole thing.

**Our own calculations — GEX, walls, flip, max pain, signals — are ours** and nobody disputes them.
The problem is only the raw prices we hand through.

---

## Week 1 — Things only we can do

No lawyer, no money, nobody's permission. **This is the part that matters most**, because every day
it isn't done the problem gets slightly bigger. The product keeps working throughout — nothing here
touches the data feed.

### 1. Write down five facts · 10 min
- The date of our **first paying customer**.
- Paying subscribers **right now**, split Basic / Pro.
- Total subscribers **ever**.
- Every company with an **API key** — name, start date, what they use it for.
- The date ES/NQ went live — **2026-08-27**, already in `docs/runbooks/es_nq_futures_rollout.md`.

*Why:* every conversation from here starts with these, and once we start changing code and revoking
keys we can't reconstruct them.

**Done when:** it's in a dated file saved somewhere that isn't the repo.

### 2. Bookmark where the code is today · 5 min
```
git tag -a pre-remediation-2026-09-02 -m "before fixing data licensing"
git push origin pre-remediation-2026-09-02
```
*Why:* "we fixed it on this date" is much stronger when we can show it.

**Done when:** the tag is on GitHub for both repos.

### 3. Find out who can currently see what · 1 hour
Run `make api-keys-list ACTIVE=yes`. For every key, write down who holds it and what it can reach.

Then check `API_SCOPE_ENFORCEMENT` on the **actual deployed server**, not `.env.example`. If it's
`0` or missing, the permission system isn't running and every key can reach everything.

> Note: don't look for the `SCOPE_DRYRUN … would 403` log line as evidence — it's emitted at
> `logger.debug`, so unless a deployment runs at DEBUG it was never written. The database is the
> authoritative source.

*Why:* this single answer decides how big the problem is. If enforcement is off, outside companies
have been able to pull raw option prices — much bigger than showing them on our own site.

**Done when:** we can say, for each outside company, exactly what their key reaches today.

### 4. Shut off the API access that shouldn't exist · 2–3 hours
- Give every outside key the `analytics` or `signals` bundle. **Never `full`** — that includes the
  raw option chain and is for our own website only.
- Any key with `*` bypasses everything. Cancel and reissue.
- Set `API_SCOPE_ENFORCEMENT=1` on the live server.

**Done when:** a real outside key against `/api/option/quote` returns **403**. Actually test it.

### 5. Stop selling the underlying price through the API · 1–2 hours
In `src/api/scopes.py`, remove `MARKET_REFERENCE` from the `analytics` and `signals` bundles.

The docstring argues this is fine to resell because we only serve one symbol's price rather than the
whole chain. **That reasoning is wrong** — SPY/QQQ prices belong to the stock exchanges, SPX/NDX to
Cboe and Nasdaq, and serving one price is reselling as much as serving a thousand. **Fix the comment
in the same commit as the code** — it's currently the written justification for selling this
externally.

**Done when:** an outside key gets 403 on `/api/market/quote` but 200 on `/api/gex/summary`.

### 6. Tell the affected companies · 30 min
Steps 4 and 5 break integrations. Email them:

> "From [date], our API no longer serves underlying price and bar data. The GEX, flow, max pain and
> signals endpoints are unchanged. If you need a price, your charting platform already has one."

**Don't explain why in legal terms** — not concealment, but counsel hasn't confirmed the diagnosis
and a customer email is the wrong place for a first attempt at describing it.

**Done when:** every affected company has a dated email and we kept copies.

### 7. Delete the option price table · half a day
`/option-contracts` shows customers real bid, ask, last, volume and open interest per contract —
the exchange's actual product on a screen someone paid for. The endpoints behind it go too:
`/api/option/*`, `/api/flow/by-contract`, `/api/flow/contracts`, `/api/flow/smart-money`,
`/api/market/open-interest`, `/api/tools/option-calculator`.

**Cut it rather than hiding it.** Licensing this costs a minimum of $1,500/month before a single
customer, and it's the least distinctive thing we sell. Nobody subscribes for a bid/ask table.

**Done when:** no page and no endpoint returns per-contract bid/ask/last/OI to anyone, our own
website included. Grep the response models; don't trust the routing.

### 8. Put the public pages on yesterday's numbers · half a day
The home page, `/spx-gamma-levels`, `/real-time-gex-0dte` and `/education/*` show near-live levels to
anyone with no login (`frontend/core/auth.ts`, `gammaLevels.tsx` with `revalidate = 900`).

Switch the logged-out view to **yesterday's closing levels**, clearly dated. Keep live behind login.

*Why:* legally it's the widest audience we serve. Practically, those pages are how an exchange or a
competitor finds us — we deliberately optimised them to rank. And we lose nothing: the SEO value is
in the page existing and ranking, not in the number being 15 minutes old.

**Done when:** in a private window `/spx-gamma-levels` shows yesterday's close with a visible date;
logged in it shows live.

### 9. Lock the shared backtest links and freeze everything new · 1–2 hours
- `/backtesting/shared/[token]` shows results to anyone with the link. Require login, add expiry.
- **Stop selling new API access** until we've switched suppliers.
- Put **HOLD — licensing** at the top of `docs/design/pine-seeds-gamma-levels-exporter.md`. That
  would publish our levels into a public GitHub repo TradingView reads. It's only shelved because
  TradingView paused new repos — luck, not a decision.
- No new chart-platform indicators until this is sorted.

**Done when:** logged out, a real share link returns not-authorised; the Pine Seeds doc says HOLD in
its first three lines.

---

## Week 1–2 — Get the right help

Two things at once. Neither waits for the other, and neither waits for week 1 to finish.

### 10. Hire a market data licensing lawyer · this week
Not a general business lawyer — this is a narrow specialty. Ask firms directly: **"have you handled
an exchange market data audit?"** If not, keep calling.

Tell them up front: *"I want to fix this and I probably want to disclose it. I need the order of
operations right."*

Give them: the audit document, the five facts from step 1, the actual contracts from step 11.

Ask these four specifically:
- **When do we tell TradeStation, and who makes the call?** Can you approach them rather than me
  phoning in?
- **Would a business account be cleaner** than converting a personal one?
- **What's the realistic exposure in dollars**, so we can weigh the options?
- **Does the trade-signals product make us an investment adviser?** Separate legal question, and it
  affects the first one.

**Done when:** someone is engaged and has the documents. Not "I emailed a firm."

### 11. Get the actual contracts · 30 min
The audit read published templates, not what we signed. Download from the account portal — **for
both usernames**, since they may differ. If they're not there, email TradeStation for copies. That's
an ordinary customer request and raises no flags.

**Done when:** PDFs for both accounts, and the lawyer has them.

### 12. Call three data companies · 2–3 hours
**Databento, Polygon, dxFeed.** Tell each exactly what we're building. Their licensing people will
say what their contract allows, and that conversation teaches more in an hour than any document.

Ask all three the same five:
- Can we **show your data to paying customers**, on a website and through our own API?
- Can we sell **calculations built from it** — gamma exposure, greeks, signals?
- Which exchanges do you cover — **options, US stocks, index values, CME futures**? Where do we still
  need our own agreement?
- What **reporting** falls on us, and how often?
- Can we **store history and sell backtesting** on it?

*Get it in writing.* A salesperson saying "yeah that's fine" is worth nothing later.

**Done when:** three written quotes answering all five. Expect $500–5,000/month.

---

## Week 2–8 — Switch data suppliers

Until this is done, TradeStation switching us off kills the product. Once done, they're just an
account. **This is what buys the ability to have the conversation calmly.**

### 13. Run the new feed alongside the old one · 1–2 weeks
Both running, separate tables or a source column. **Don't switch over yet.** The ingestion layer
already separates the data client from everything else — this is the payoff for that.

**Done when:** both feeds have written a full trading day with no gaps.

### 14. Check the numbers actually match · 1 week
Compare GEX summary, walls, flip, max pain and greeks from each feed. They **won't match exactly** —
providers time and batch ticks differently — and we need to know how far apart before subscribers do.

At least five days, including one OPEX Friday and one volatile day. Extend the existing
freshness/validation harness rather than writing a one-off.

**Done when:** the differences are written down and *explained*, not just measured.

### 15. Switch over, keeping the old feed warm · 1 day
Serve everything from the new supplier; leave TradeStation running but unused for a week so a
problem is a rollback rather than an outage.

**Done when:** live analytics read only from the new source — checked in the query path, not assumed
from config.

### 16. Turn TradeStation off properly · 1 hour
- Remove both refresh tokens from every deployed server.
- Revoke the app's access from **both** usernames.
- Disable the backfill tools that still call them.

**Done when:** **zero TradeStation API calls in production logs for seven days straight.** This is a
gate, not a step — phase 4 doesn't start until it holds.

### 17. Ask about the stored history
The price database was built from TradeStation. Ask the new supplier and the lawyer what happens to
it — some contracts let us keep using existing history, some don't, and it decides whether
backtesting survives in its current form.

**Done when:** a written answer, and a stated retention policy instead of "we keep everything."

---

## After step 16 — The TradeStation conversation

By now the violation has stopped, the product doesn't depend on them, and we can afford whatever
answer we get. **That's the whole reason it comes last.**

### 18. Let the lawyer decide the approach
- **Convert to professional.** Simplest. Professional rates; free data ends.
- **Close the second account**, keep one genuinely personal account. Clean — and after step 16 the
  business doesn't need their feed.
- **Open a business account.** Separates personal from company properly. Usually the best structure
  for any later conversation.

The lawyer may make the approach rather than us phoning in. That's normal and usually better.

### 19. Be honest, and expect the free data to end
They'll ask what changed. **Tell the truth.** Lying on a broker form turns a licensing problem —
fixable, mostly about money — into a fraud problem, which isn't.

Being able to say *"we found this, stopped it, and moved to a licensed supplier"* is worth a great
deal. That sentence is the entire reason steps 1–17 come first.

Budget for it: the free and cheap packages are personal-use products and end the day status changes.

---

## Ongoing — Keeping it clean

### 20. Ask new signups whether they're a professional · start now
A tickbox at signup: professional trader, registered with a regulator, or trading for a business?
Prompt existing users on next login.

*Why start before we have a contract:* every supplier requires it, it costs nothing if unneeded, and
retrofitting it across an existing user base is painful.

**Done when:** we can produce a professional / non-professional count on demand.

### 21. Fix the terms page
Our terms say nothing about market data — where it comes from, that it may be delayed, or the
disclaimers exchanges require. They only stop *our* users reselling *our* content, which is an
awkward look. The supplier will give exact wording. All five languages.

### 22. Keep one file with everything in it
Contracts, the audit, this runbook, monthly reports, the list of everywhere our data goes. Review
each **January** — that's when the exchanges change their prices.

---

## Things that make it worse

- **Don't lie on a form or in a call.** A licensing problem is negotiable. A false statement on a
  broker form is a different category and removes the good outcomes.
- **Don't open a third account or move entitlements again.** The two-username arrangement already
  looks deliberate; more of it turns "got the rules wrong" into "knew and worked around it."
- **Don't delete the trail.** Not volunteering is a judgement call the lawyer can make. Scrubbing git
  history or purging web archives is not.
- **Don't phone TradeStation to "ask hypothetically."** There's no hypothetical — the account is on
  their screen.
- **Don't wait for the lawyer to start week 1.** Steps 1–9 need nobody's permission and it's the part
  growing every day.

---

## The words the lawyer will use

| Term | What it means |
| --- | --- |
| Exchange | Where trading happens, and who owns the prices. CME (futures), Cboe (SPX, VIX), Nasdaq (NDX), NYSE. |
| OPRA | The single body that collects and sells *all* US options prices. Every option quote we have came from them. |
| Subscriber | Us, receiving data. Two flavours below. |
| Non-professional | An individual using data for their own trading only. Cheap or free. What we're signed up as. |
| Professional | Everyone else, including anyone using it for a business. Much more expensive. What we actually are. |
| Vendor / distributor | A company licensed to pass data on to others. Separate, more expensive agreement. What we'd need. |
| Redistribution | Showing or sending exchange data to anyone else. What our website and API do. |
| Derived data | Numbers calculated *from* prices rather than the prices themselves. Our GEX and signals. Usually much cheaper to license — our best lever. |
| Entitlement | Permission attached to a specific username for a specific feed. Ours are split across two usernames (finding F1). |
| Back-billing | Being charged retroactively for what we should have been paying. The main financial risk. |

---

## If we do nothing else this week

**Step 3.** Find out whether `API_SCOPE_ENFORCEMENT` is actually on in production, and which outside
companies can currently reach the option chain and price endpoints.

Everything else here is a plan. That one is a fact we don't have yet, and it decides whether the
biggest problem is "we showed data to our own subscribers" or "we sold it to other companies." Very
different conversations, and we should know which one we're in before anybody else does.
