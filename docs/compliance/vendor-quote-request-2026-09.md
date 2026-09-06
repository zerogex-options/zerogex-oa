# Vendor quote request — Massive, Databento, dxFeed

**Companion to:** `docs/design/realtime-market-data-vendors.md` (the shortlist) and
`docs/compliance/market-data-remediation-runbook.md` step 12 ("call three data companies … get it
in writing"). Send the common body to all three with the vendor-specific insert appended, from a
company address, with the channel inventory attached once step 1 of the runbook has produced it.

Framing note: the email describes what we are building and what we need licensed. It does not
narrate the history of the current feed; the runbook makes that a decision for counsel (step 10),
so have counsel glance at this before it goes out. Square brackets are the facts only we can fill in.

---

## Common body

**Subject:** Quote request — real-time OPRA, US equities, index values and CME for an options
analytics service (business use, derived-data distribution)

Hello [vendor licensing / sales team],

I run **ZeroGEX** ([legal entity name], [state/country], zerogex.io), a subscription options
analytics service. We compute dealer gamma exposure, gamma walls, flip levels, max pain, volatility
signals and forecast ranges from real-time option chains and publish the **computed levels** to paying
subscribers through our website, a REST API, chart-platform indicators and messaging bots. We are
moving our market-data sourcing to a licensed vendor and would like an itemised quote for the
package below, plus written answers to the questions at the end.

**About us**
- Entity: [legal name, entity type, incorporation state], operating since [date]. Data is consumed on
  our own servers; no human trades off the raw feed.
- Subscribers today: [N] paying ([N₁] self-certified non-professional, [N₂] professional) across
  [Basic / Pro] tiers; [M] external API customers holding keys; growth expectation [x] over 12 months.
- Infrastructure: [k] Linux ingestion servers (plus [k'] standby) at [hosting provider, region];
  Python 3.10/3.11; a PostgreSQL/TimescaleDB store.

**Data we need, real-time**

| Feed | Symbols | Detail |
| --- | --- | --- |
| US listed options (OPRA consolidated) | SPY, QQQ, SPXW/SPX, NDXP/NDX chains | NBBO quotes with sizes, trades, daily open interest; all listed expirations within our window; roughly 1,000–1,500 contracts per underlying, 4,000–6,000 live subscriptions in total; we compute IV and Greeks ourselves |
| US equities (consolidated SIP: CTA Network B, UTP) | SPY, QQQ | NBBO quotes **and trades** (we build up/down volume from prints) |
| Index values | SPX, NDX, VIX, VXN | at least one update per minute; tick-level preferred |
| CME futures | ES, NQ front month or continuous | trades or 1-minute bars, full CME session |
| Reference | option chains (expirations, strikes), corporate calendar | REST is fine |

Streaming delivery (WebSocket or TCP) with one session per asset class carrying the full subscription
list, and REST snapshots for whole-chain seeding. Our internal cadence is a 5-second snapshot
aggregated to 1-minute buckets, so we do not need tick-by-tick delivery guarantees beyond ordinary
completeness.

**Historical (quote separately):** 1-minute bars for the underlyings and indices, and option-chain
snapshots (quotes, OI) for the four underlyings, back to [2020 / 5 years], for backtesting.

**How the data is used — please quote two scenarios**

*Scenario A — derived data only.* Raw quotes never leave our servers. Subscribers and API customers
receive computed levels and signals only. Public pages show prior-session levels. This is our
intended steady state.

*Scenario B — Scenario A plus display of raw data to our own logged-in subscribers:* real-time
last/bid/ask for SPY, QQQ, SPX, NDX, VIX, VXN and ES/NQ on our charts and via WebSocket to the browser;
a per-contract option-chain table (bid, ask, last, size, volume, OI) for the four underlyings; and
real-time underlying prices to [M] external API customers. Please price the per-user components so we
can see what each element costs, and state which elements you would not license at our size.

For both scenarios, please itemise:
1. Your platform/subscription fee, by asset class, and any minimum term.
2. Every exchange fee passed through, with the current amount and unit: OPRA (professional
   device fees for our servers; non-professional and professional per-user fees; redistribution or
   vendor fees), CTA Network B and UTP (display per-user, non-display, access/redistribution), Cboe
   Global Indices (VIX, VXN, SPX values), Nasdaq GIDS (NDX value), CME (professional device fees,
   non-professional per-user, distributor/redistribution, non-display, **Derived Data licence**).
3. Which of those exchange agreements you hold as vendor of record on our behalf, and which we must
   sign directly.
4. Reporting we must perform (user counts by classification, cadence, tooling you provide).

**Questions we need answered in writing**
1. Does your commercial licence permit us to **create derived analytics from real-time data and
   distribute them** to paying subscribers, via our own API, and into third-party charting platforms
   (TradingView, Sierra Chart, NinjaTrader indicators)? Is any of that treated as onward
   redistribution requiring separate terms?
2. What exactly counts as "derived" under your agreement (for example: per-strike gamma exposure,
   per-strike open interest, a mid-price surface, a single underlying price)?
3. May we **store the real-time data we receive and use it for historical analytics, replay and
   backtesting features** offered to subscribers? Under what terms, and for how long may it be retained?
4. May we show **delayed (≥ 15 minute) or prior-close** data on public, unauthenticated pages?
5. What are the **subscription limits** per connection (symbols, message rate) and the recommended
   topology for 4,000–6,000 option subscriptions plus the underlyings?
6. Do you cover **NDX and VXN** index values in real time? SPX and VIX?
7. For CME: which licence category do you place us in, and do you paper the **Derived Data** licence
   for analytics (our ES/NQ levels are projections of SPX/NDX option analytics onto the futures price
   axis) or must we contract with CME Data Services directly?
8. Contract term, notice period, onboarding time, and whether a paid trial or sandbox is available.

We can start a technical evaluation immediately and would like to be in production on the new feed
within [6–8] weeks. Thank you — I'm happy to walk through the product on a call.

[Name]
[Title], ZeroGEX · [phone] · [email]

---

## Insert for Massive (formerly Polygon.io)

We understand the individual plans are for personal use and that business use requires a Business
plan. Please quote **Business** for Stocks (full consolidated SIP, not the Nasdaq Basic or single-venue
feeds), Options, Indices and Futures, and confirm: (a) which index tickers are real-time on the
Indices plan — specifically `I:SPX`, `I:NDX`, `I:VIX`, `I:VXN`; (b) that the Futures WebSocket covers
CME ES and NQ in real time and its release status; (c) whether internal derived computation alone
(Scenario A) requires Business, or only display/redistribution does; (d) whether you act as vendor of
record for OPRA, CTA/UTP, Cboe indices and CME under Business and how per-user reporting works;
(e) WebSocket subscription limits per Business connection for options.

## Insert for Databento

We would be live-streaming `OPRA.PILLAR` (parents `SPY.OPT`, `QQQ.OPT`, `SPX.OPT`/`SPXW.OPT`,
`NDX.OPT`/`NDXP.OPT`), `EQUS.SIP` (SPY, QQQ trades and NBBO), `GLBX.MDP3` (ES, NQ front month) and
`MAIN.CGIF` (VIX, VXN, and SPX if carried), roughly [N] messages per day at the schema(s) you
recommend for NBBO plus trades. Please quote: (a) the monthly licence lines for each live dataset at
professional status, and the usage-based cost at that volume, or a flat plan if better; (b) whether
**SPX** is available on `MAIN.CGIF` and whether an **NDX** value (Nasdaq GIDS) is available or planned;
(c) your redistribution / derived-data licence terms for Scenario A versus Scenario B; (d) historical
pricing for the backfill described above (`OPRA.PILLAR` and `EQUS`/`GLBX` at 1-minute OHLCV, back to
[2020]); (e) whether the CME derived-data question can be handled through you.

## Insert for dxFeed

We are interested in a single agreement covering OPRA, consolidated US equities, Cboe and Nasdaq
index values and CME through dxFeed, with dxFeed as vendor of record for our subscribers where you
offer that. Please quote the package for Scenario A and Scenario B and confirm: (a) delivery over
**dxLink WebSocket** for a Python client, with the message-rate and symbol limits per session;
(b) the per-user reporting mechanism you provide for display subscribers; (c) coverage of `NDX` and
`VXN` values in real time; (d) minimum term and onboarding lead time; (e) whether historical
time-and-sales / candles for the backfill above are included or separate.
