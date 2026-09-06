# Real-Time Market Data — TradeStation Replacement Shortlist

**Status:** decision brief · **Owner:** ZeroGEX · **Date:** 2026-09-06 · **Branch:** `release`
**Companions:** `docs/compliance/market-data-licensing-audit-2026-09-02.md` (why we must move),
`docs/compliance/market-data-remediation-runbook.md` (the order of operations; this brief answers
its step 12), `docs/design/historical-options-data-vendors.md` (the historical lake; one of its
claims is corrected in §6).

> **How to read the evidence tags.** Every vendor domain, the OPRA plan site, cboe.com, cmegroup.com,
> sec.gov and federalregister.gov are blocked by the egress policy of the environment this brief was
> written in, so nothing below was read off a pricing page today. Each fact carries a tag:
> **[V]** verified in this pass from a primary source that *was* reachable (vendor SDK repositories
> on GitHub and package metadata on PyPI — good for coverage, delivery mechanism, release cadence);
> **[R]** carried from this repository's earlier verified passes (the 2026-07 historical-vendor pass
> and the 2026-09-02 audit); **[K]** general knowledge as of mid-2026, **not** confirmed today —
> treat every price with a [K] as an order-of-magnitude anchor to be confirmed in writing.

---

## 1. What the replacement actually has to do (from the code)

The requirement is narrower than "a market data API" and wider than "an options feed". This is the
footprint `src/ingestion` has on TradeStation today, and every candidate is scored against it.

| Need | Where it comes from today | What the code consumes |
| --- | --- | --- |
| **Option chains, streaming** — SPY, `$SPXW.X` (SPXW root), QQQ, `$NDXP.X` (NDXP root); monthly `$SPX.X` / `$NDX.X` roots optional | `marketdata/stream/quotes/{symbols}`, persistent HTTP stream, chunks of ≤ 800 symbols (`src/config.py:748`), ~10 concurrent streams per account (`src/config.py:1836`) | Bid, Ask, BidSize, AskSize, Last, Volume, OpenInterest. **IV and Greeks are computed locally** (`greeks_calculator.py`, `iv_calculator.py`); the vendor's Greeks are optional. |
| **Chain discovery** | `marketdata/options/expirations/{u}`, `…/strikes/{u}` | expirations, strikes |
| **OI / IV seed** | REST `marketdata/quotes/{symbols}` at startup and strike recalibration | daily open interest |
| **Underlying 1-min bars** — SPY, QQQ, SPX, NDX | `marketdata/stream/barcharts/{symbol}` | OHLC, TotalVolume, **UpVolume / DownVolume** (the Lee-Ready split; a replacement must give trades so `_classify_volume_chunk` can rebuild it) |
| **VIX, VXN** | same bar stream on `$VIX.X`, `$VXN.X`, 5-min bars (`volatility_index_ingester.py:285`) | index value bars |
| **ES, NQ** | same bar stream on `@ES`, `@NQ`, 1-min, whole CME session (`futures_underlying_ingester.py`) | front-month continuous prints (basis engine + overnight display). ES/NQ **options are not ingested** — ES levels are projected from the SPX book (`API_Guide.md` §"ES / NQ"). |
| **Cadence** | stream state snapshotted every 5 s (`MARKET_HOURS_POLL_INTERVAL`), aggregated to 1-min buckets, analytics every ~60 s | tick-level throughput is **not** required; **whole-chain coverage is** |
| **Scale** | 1,000–1,500 option symbols per underlying (`src/config.py:742-745`) | ≈ 4,000–6,000 live option subscriptions across four underlyings, plus 8 underlying/index/futures symbols |
| **Runtime** | Linux systemd services, Python 3.10/3.11, one child process per symbol | a Python client or a plain WebSocket/TCP protocol; Windows-only clients need Wine |

Two consequences drive the ranking:

1. **The index values are the hard part, not the options.** SPX/NDX/VIX/VXN option *quotes* are all
   on the OPRA tape (SPX and VIX options trade only on Cboe, NDX options on Nasdaq's exchanges, but
   OPRA consolidates all of them). The index *values* are separate licensed products: SPX from S&P
   Dow Jones Indices (disseminated by Cboe), VIX and VXN from Cboe (Cboe Global Indices Feed), NDX
   from Nasdaq (Global Index Data Service). Several otherwise-excellent vendors carry OPRA and CME
   and stop short of one or more index values.
2. **Because we compute IV and Greeks, we want the raw layer.** Vendors that price by "Greeks
   included" are charging for something the code regenerates anyway.

---

## 2. The shortlist

Ranked by fit against the four stated criteria: (1) real-time SPY, QQQ, SPX, NDX, ES, VIX, VXN;
(2) real-time chains for SPY, QQQ, SPX, NDX; (3) business/professional use permitted; (4) a path to
redistribution rights. "Complete" means one contract can replace TradeStation outright; "pair" means
it needs a second source for what it lacks.

| # | Vendor | 7 symbols real-time | 4 chains (OPRA) | Business use | Redistribution path | Delivery | Indicative cost (confirm) | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | **dxFeed** (Devexperts) | 7/7 [K] | Yes, full OPRA [K] | Yes — institutional vendor [K] | Yes, vendor/redistribution agreements; powers retail brokers [K] | dxLink WebSocket protocol with JavaScript, Java, .NET, C/C++ and Swift clients and **no Python client** [V]; the PyPI `dxfeed` wrapper is frozen at 2021 / Python ≤ 3.9 [V] | quote-based; expect low-to-mid $1,000s/mo for OPRA + SIP + indices + CME at professional rates [K] | **Complete.** Cleanest licensing answer; weakest Python story (use dxLink directly). |
| 2 | **Massive** (formerly Polygon.io; rebranded 2025-10-30 [V]) | 7/7 likely — indices product includes `I:SPX`, `I:VIX` [V]; futures WebSocket market covers CME [V]; NDX/VXN [K] | Yes, OPRA, per-contract WebSocket + whole-chain snapshot endpoint [V] | Business tiers exist (`business.massive.com`, `fullmarket-business` feeds) [V]; individual plans are personal-use only [K] | Business plans include display/redistribution with exchange-fee pass-through; Massive is the vendor of record [K] | WebSocket per asset class + REST; official `massive` Python client v2.8.0 (2026-05) [V] | individual options tier ≈ $199/mo (not usable commercially); Business plans quote-based, historically ≈ $1–3k/mo per asset class [K] | **Complete.** Best price/coverage balance for our size; four asset-class contracts (stocks, options, indices, futures). |
| 3 | **Databento** | 6–7/7: SPY/QQQ via `EQUS.SIP` (consolidated) [V]; ES/NQ via `GLBX.MDP3` [V]; VIX/VXN via `MAIN.CGIF` (Cboe Global Indices Feed) [V]; **SPX on CGIF probable, NDX (Nasdaq GIDS) not listed** [V-by-absence] | Yes, `OPRA.PILLAR`, subscribe by parent (`SPY.OPT`) [V] | Yes — all plans are commercial; professional exchange fees passed through [K] | Redistribution/derived-data licence available on request (named as a licensed redistributor in the 2026-09 audit) [R] | Raw TCP live gateway, official `databento` Python client v0.86.0 (2026-09-01, weekly releases) [V] | usage-based or flat plans; OPRA live carries a monthly licence fee; budget ≈ $500–2,000/mo for our slice [K] | **Near-complete.** Best engineering fit (parent-symbol subscription, MBP/MBO with aggressor side for the ES aggressor-delta idea). Confirm SPX on CGIF; source NDX value elsewhere or from the NDXP chain. |
| 4 | **Barchart OnDemand** | 7/7 [K] | Yes, OPRA [K] | Yes [K] | Yes — enterprise redistribution licences; powers many broker sites [K] | REST (polling) with streaming for enterprise; no maintained Python client (community package last released 2018) [V] | quote-based, ≈ $1k+/mo [K] | **Complete.** Solid but REST-first; fits our 5-s snapshot model, not a push model. |
| 5 | **QuoteMedia** | 7/7 [K] | Yes, OPRA [K] | Yes [K] | Yes — enterprise; powers brokerage front-ends [K] | Quotestream / streaming APIs; no Python SDK [K] | quote-based, ≈ $1–3k/mo [K] | **Complete.** Enterprise-shaped; more contract than we need. |
| 6 | **DTN IQFeed** | 7/7 [K] | Yes, OPRA [K] — **but per-plan symbol caps** (hundreds to low thousands) collide with our 4–6k subscriptions [K] | Yes, at professional exchange fees [K] | **No.** IQFeed forbids redistribution; DTN's enterprise feed is a separate product [K] | Local IQConnect client over TCP; Windows binary runs on Linux under Wine + xvfb (`pyiqfeed`, protocol 6.x) [V] | ≈ $100–200/mo core + professional exchange fees [K] | **Consume-only.** Works for a derived-only product; dead end the day we want to show a quote. |
| 7 | **ThetaData** | 6/7 — stock, option and index endpoints [V]; **no CME futures** [K] | Yes, OPRA [R] | Yes — commercial licensing programme, startup tier ≈ $125/mo [R] | By separate agreement [R] | gRPC Python client v1.0.10 (2026-08, Python 3.12+) [V]; streaming via the local Theta Terminal [K] | ≈ $125–400/mo [R] | **Pair** (needs an ES source). Cheapest credible options core; keep Databento `GLBX.MDP3` beside it for ES/NQ. |
| 8 | **SpiderRock Connect** | 7/7 [K] | Yes, OPRA with vendor Greeks/surfaces [K] | Yes — institutional [K] | Yes, by agreement [K] | MLink JSON / WebSocket [K] | quote-based, ≈ $1–3k/mo+ [K] | **Complete, options-specialist.** Overpays for Greeks we compute. |
| 9 | **Cboe Data & Access Solutions** (Global Cloud, Cboe One, Global Indices Feed, LiveVol) | 5/7 — VIX/VXN/SPX values are *authoritative* here; SPY/QQQ only as Cboe One (not NBBO); no ES [K] | SPX/SPXW/VIX complete (single-listed on Cboe); SPY/QQQ/NDX chains Cboe-venue only unless bought as OPRA [K] | Yes [K] | Yes — the exchange's own licences [K] | Cboe Global Cloud streaming, REST [K] | CGIF ≈ $1,000+/mo [R]; Cboe One Options quote-based [K] | **Pair.** The index-value supplier of record; not a whole-feed replacement. |
| 10 | **Intrinio** | 2/7 — SPY/QQQ real-time only via IEX, Nasdaq Basic or Cboe One (consolidated SIP is **delayed** only) [V]; no index values or futures [V-by-absence] | Yes, OPRA WebSocket with trades, quotes, OI refresh [V] | Yes; "minimized exchange and per-user fees" [V] | Redistribution tiers offered [K] | WebSocket, `intriniorealtime` Python SDK 6.3.0 (2026-04) [V] | ≈ $400–1,500/mo [K] | **Pair.** Good chain feed; cannot carry the underlying/index side. |

### Evaluated and not shortlisted

- **Tradier** — real-time OPRA/equity streaming, but personal-use terms tied to a brokerage account;
  commercial use only through a partner agreement; no CME [K].
- **Alpaca** — OPRA options and SIP stocks over WebSocket (`alpaca-py` 0.44, `DataFeed.SIP`) [V], but
  no index values, and the data terms are personal-use unless under a Broker API/enterprise deal [K].
- **Interactive Brokers** — market-data lines are capped (≈ 100 by default, boostable to a few
  thousand); business accounts pay professional fees; **no redistribution**; poor fit for 4–6k
  streaming contracts [K].
- **Charles Schwab Trader API, tastytrade DXLink, Robinhood/Public/moomoo APIs** — individual
  account-holder use only [K].
- **MarketData.app** — the most instructive *no*. Its docs (updated 2026-09-04) [V] say self-service
  plans are non-professional, personal licences; the **Commercial Plan ($250/mo)** grants a commercial-use
  licence and lets you "compute derived metrics, signals, or aggregates … and serve those to your users
  (including over your own API)" but **includes no real-time exchange data at all** (chains and quotes
  arrive T+1; only their proprietary SmartMid price is live), because "real-time exchange data for
  professional use requires direct exchange licensing" arranged case by case. REST-only, no futures.
  Useless as a live feed for us; exactly right as a statement of the rules.
- **Nasdaq Data Link / NCDS** — Nasdaq Basic (not NBBO), NDX values, Nasdaq-venue options; no SPX,
  VIX or CME [K]. Only relevant as the NDX value supplier.
- **Xignite, ACTIV / Options IT, Refinitiv, Bloomberg B-PIPE** — enterprise feed handlers; cost and
  contract weight well beyond our size [K].
- **Finnhub, Twelve Data, EODHD, Alpha Vantage** — no real-time OPRA chains [K].
- **IEX Cloud** — shut down in August 2024 [K].

### Recommendation

- **Primary quote request: Massive (Business) and Databento**, with **dxFeed** as the third quote if
  either falls short on the index values or on redistribution terms. These three are exactly the
  three the remediation runbook (step 12) already names; this brief adds the reasons and the gaps.
- **Cboe Global Indices Feed** — either bundled by the winner or bought direct — for VIX, VXN and
  SPX values. Do not let an otherwise-complete vendor slip through with delayed index values.
- **NDX value:** confirm it explicitly with every vendor. If the winner lacks it, Nasdaq's GIDS via a
  vendor, or ThetaData's index endpoint, fills the gap; the fallback is to imply spot from the NDXP
  chain (put-call parity at the ATM strike), which the Greeks code can already do per expiry.
- **Keep the historical purchase separate** (ThetaData or Databento, per the historical brief).

---

## 3. The OPRA question — do we need a redistribution licence *now*?

Short answer: **the licence follows the quotes, not the analytics — and today's setup does hand out
quotes.** Two different things are being conflated in the question; they are priced and signed
separately.

### 3a. Subscriber status — needed in every setup, including consume-only

Anyone whose business receives real-time OPRA data is a **Professional Subscriber**. It is a fee
class, not a licence to hand data on. The vendor reports us to OPRA and bills the device-based fee.
It applies even when no human ever looks at a raw quote: a server ingesting the chain to compute GEX
is still a professional subscriber. OPRA's 2016 amendment (SR-OPRA-2016-02) removed the "datafeed"
carve-out that used to limit this for server-side recipients — that is the amendment the historical
brief cites, and it means the fee follows the data into non-display use, **not** that OPRA charges a
separate "non-display" fee [R]. As far as this pass can tell, OPRA has *no* non-display category of
its own; the ≈ $2,000/mo "OPRA non-display" line in the historical brief reads like a conflation with
the CTA/UTP non-display fees, which do exist. Confirm on the current fee schedule (see §6).

Indicative OPRA fee lines, as they appear in the 2026-09-02 audit [R] (this pass could not reach the
schedule; my own recollection of the older schedule is $30.50/device and $650/mo redistribution, so
expect a January 2025 or 2026 increase and confirm):

| Line | Figure [R] | Applies to |
| --- | --- | --- |
| Professional subscriber, device-based | $31.50 / device / month | each server or user with access — including ingestion servers |
| Non-professional subscriber | $1.25 / user / month | each retail end user who *sees* OPRA data |
| Redistribution (vendor) fee | $1,500 / month, flat | any firm that provides OPRA data to third parties |

The same shape repeats per tape: CTA/UTP for SPY and QQQ (with real non-display fees), Cboe for
VIX/VXN/SPX values, Nasdaq for NDX, CME for ES/NQ. Every one of them defines "non-professional" as an
individual using data for personal, non-business purposes — which is why the audit's F2 finding
stands regardless of vendor: as a business we are professional on all of them.

### 3b. Redistribution — needed only when OPRA data reaches someone else

OPRA's Vendor Agreement and the redistribution fee are triggered by **providing OPRA data to a third
party**, whether on a screen ("display") or as a datafeed. "Third party" includes our own paying
website subscribers, API customers and anything pushed into Sierra Chart, NinjaTrader or TradingView.
It is not triggered by **derived data** that cannot be reverse-engineered into the underlying quotes —
GEX by strike, walls, flip, max pain, signals, forecast ranges. That is the line the growth roadmap
and `src/api/scopes.py` draw, and it is the right line for OPRA.

**Where the current setup stands on that line** (from the code, matching the audit's F3–F5):

| Surface | What it emits | Whose data | Redistribution? |
| --- | --- | --- | --- |
| `/option-contracts` page via the internal BFF; `/api/option/*`, `/api/market/open-interest`, `/api/flow/by-contract`, `/api/flow/contracts`, `/api/tools/option-calculator`, premium surface | per-contract bid / ask / last / size / volume / OI | **OPRA** | **Yes — display redistribution.** Withholding `market_raw` from external keys does not change this: the website's logged-in subscribers are third parties too. |
| `WS /ws` (`src/api/routers/websockets.py`), `/api/market/quote`, `/historical`, `/session-*` | real-time SPY, QQQ, SPX, `$VIX.X`, `$VXN.X` prints and bars, to browsers and to external `analytics`-tier keys | CTA/UTP (SPY, QQQ); S&P/Cboe (SPX); Cboe (VIX, VXN); Nasdaq (NDX) | **Yes** — each under its own plan; the "reference data" theory in `scopes.py` is not a category any of them recognises. |
| ES/NQ prices shown under the index label overnight; the published basis ratio | CME prints; a derived work of CME data | **CME** | **Yes** (display) plus CME's **Derived Data** licence for the projection — CME is explicit about derived works. |
| `/api/v1/levels`, GEX, walls, flip, max pain, signals, TradeWorkz, bots, delayed tweets | computed levels | ours | **No OPRA redistribution licence needed**, provided the inputs are licensed (3a) and the vendor contract permits creating and distributing derived data — a clause to get in writing; some vendors reserve it for business tiers. |

So the answer to "do I need it in my current setup?" is:

- **If we execute the remediation runbook** (retire the per-contract surfaces, stop serving the
  underlying tape externally, keep derived-only, put public pages on yesterday's close): **no OPRA
  Vendor Agreement and no redistribution fee.** We need professional subscriber status through the
  vendor (3a), a vendor contract that says derived-data distribution is permitted, the index-value
  subscriptions for SPX/NDX/VIX/VXN, a CME professional subscription for ES/NQ, and a CME derived-data
  conversation about the basis projection. The `WS /ws` underlying push to logged-in subscribers is
  the one surface to decide on: either drop it, delay it, or license CTA/UTP + index display for
  our own users (per-user fees and monthly reporting — no OPRA involved).
- **If we keep showing quotes** — the chain page, or real-time SPY/SPX/VIX prices to users — then
  yes: the data vendor must extend redistribution rights (we become their sub-vendor / "indirect
  access" recipient), we sign the OPRA agreements the vendor requires, pay the flat redistribution
  fee plus $1.25 or $31.50 per end user per month, classify every user pro/non-pro at signup, and
  report counts monthly. The same per-user machinery then applies to CTA/UTP, Cboe, Nasdaq and CME
  for the underlying prices. That is a ≥ $1,500/mo floor before the first user, for a bid/ask table —
  the audit's F3 conclusion ("retire it") holds.
- **Delayed data is the release valve, not an exemption from the vendor.** Data ≥ 15 minutes old is
  exempt from OPRA and most exchange fees and can be shown broadly, but it still flows through a
  vendor that holds the agreements; "free delayed levels" on the public pages are fine, "free delayed
  chain" still needs the vendor's delayed-redistribution permission.

### 3c. Vendor-specific posture (what to ask for in writing)

Ask every vendor the runbook's five questions and add these two: *"Does your commercial tier permit
creating derived analytics from real-time data and distributing them to paying subscribers and via
our API?"* and *"Do you cover the OPRA professional device fees for our ingestion servers, or do we
pay them through you per device?"* Expected shape of the answers [K, confirm]:

- **Massive** — individual plans: internal use only; Business plans: display and redistribution with
  exchange fees passed through and per-user reporting; derived data allowed on Business.
- **Databento** — commercial by default; live OPRA carries a monthly licence line; redistribution
  and derived-data products need their redistribution licence (they ask for the channel inventory the
  audit's F8 already recommends writing).
- **dxFeed** — everything is contractual; they will paper OPRA, CTA/UTP, Cboe and CME vendor-of-record
  relationships for us; expect the most complete but slowest paperwork.
- **ThetaData / Intrinio** — commercial tiers cover internal use and derived output; showing raw
  quotes to users is a separate agreement.
- **IQFeed** — professional fees, no redistribution of any kind.
- **Cboe** — CGIF for index values is a subscriber licence; derived products *referencing* SPX/VIX
  may raise index-licensing questions with S&P DJI / Cboe — a gray area many small vendors live in;
  ask counsel, not the sales desk.
- **The public precedent worth quoting back to any vendor** is MarketData.app's policy set [V]: business
  use is professional; a commercial plan may permit *derived* data over your own API while forbidding
  raw responses; and "redistribution … requires a license granted directly by the exchanges — not by
  [the vendor]". Expect every vendor above to draw the same three lines, priced differently.

---

## 4. Migration notes (what changes in `src/`)

The ingestion layer already isolates the provider: `tradestation_client.py`, `stream_manager.py`,
`volatility_index_ingester.py` and `futures_underlying_ingester.py` are the only modules that speak
TradeStation; storage, Greeks, analytics and the API are provider-agnostic (the audit's F1 "stand it up
in parallel" plan relies on this). Concretely:

1. **A provider adapter with four methods** — `stream_option_quotes(symbols)`, `stream_trades_and_
   quotes(symbols)` (underlying), `stream_index_values(symbols)`, `stream_futures(symbols)` — plus
   `expirations`, `strikes`, `snapshot` for discovery and OI seeding. Keep the 5-s snapshot-and-aggregate
   loop; only the accumulator's input changes.
2. **Symbol mapping table** (`src/symbols.py`): `$SPXW.X` → root `SPXW`; `$NDXP.X` → `NDXP`; `$VIX.X` →
   `I:VIX` (Massive) / CGIF `VIX` (Databento); `$VXN.X` → `I:VXN` / `VXN`; `@ES` → the vendor's
   continuous or front-month ES (`ES.c.0`-style on Databento; Massive futures ticker); OCC option
   symbols are already canonical in `option_chains`.
3. **Up/down volume**: no vendor ships TradeStation's `UpVolume`/`DownVolume`; subscribe to trades and
   quotes for the underlyings and feed `_classify_volume_chunk` (already implemented) — the same path
   the futures-aggressor design note wants for ES (`docs/design/futures-aggressor-delta.md`).
4. **Open interest** arrives once a day from every vendor; keep the REST seed-on-start pattern.
5. **Concurrency**: one WebSocket/TCP session per asset class carrying thousands of subscriptions
   replaces the ten-stream account cap; delete the second-username workaround (audit F1) rather than
   port it.
6. **Parallel run**: land rows with a `source` column or in shadow tables, run the freshness harness
   against both feeds for five sessions including an OPEX Friday, then cut over (runbook steps 13–15).

---

## 5. Cost picture (order of magnitude, confirm every line)

| Layer | Derived-only product | Plus raw quotes to our own users |
| --- | --- | --- |
| Vendor subscription (Massive Business / Databento / dxFeed class) | ≈ $500–3,000/mo [K] | same |
| OPRA professional device fees (ingestion servers) | a few devices × ≈ $31.50 [R] | + $1.25 or $31.50 per end user [R] |
| OPRA redistribution fee | $0 | ≈ $1,500/mo flat [R] |
| CTA/UTP professional + non-display (SPY, QQQ) | modest, via vendor [K] | + per-user display fees [K] |
| Cboe Global Indices Feed (VIX, VXN, SPX values) | ≈ $1,000+/mo [R] unless bundled | + per-user index display [K] |
| Nasdaq GIDS (NDX value) | small, via vendor [K] | + per-user [K] |
| CME professional (ES, NQ) + Derived Data licence | per-device professional fee [K] + a negotiated derived-data line [R] | + per-user display [K] |

The derived-only column is the one the audit recommends (B + C); the right-hand column is the price
of keeping a bid/ask table.

### 5a. Worked example — the setup as it runs today (Branch B)

Everything the site and API do today, sourced from a licensed vendor, decomposes into a fixed block
plus a per-subscriber block. Figures are the brief's tags: [R] from the 2026-09-02 audit, [K] to
confirm; none were readable from the authoring environment.

| Fixed, per month | Massive (Business) | Databento | dxFeed |
| --- | --- | --- | --- |
| Vendor platform (four asset classes, redistribution-capable tier) | ≈ $1,000–4,000 [K] | ≈ $500–2,000 usage + licence lines [K] | ≈ $2,000–6,000 [K] |
| OPRA redistribution (vendor) fee | $1,500 [R] | $1,500 [R] | $1,500 [R] |
| OPRA professional device fees, ingestion servers | 2–4 × $31.50 [R] | same | same |
| CTA Network B + UTP access / non-display (SPY, QQQ shown live) | ≈ $500–3,000 combined [K] | same | same |
| Cboe Global Indices Feed (VIX, VXN, SPX values shown live) | ≈ $1,000+ [R] | same | same |
| Nasdaq GIDS (NDX value) | low hundreds [K] | same | same |
| CME distributor + Derived Data licence (ES/NQ display, basis projection) | ≈ $500–2,000 [K] | same | same |
| CME historical distribution, if replay/backtests keep CME data | $30,000 / DCM / year ≈ $2,500 [R] | same | same |
| **Fixed subtotal** | **≈ $7,000–15,000** | **≈ $6,500–13,000** | **≈ $8,000–17,000** |

| Per subscriber who sees real-time data, per month | Non-professional | Professional |
| --- | --- | --- |
| OPRA | $1.25 [R] | $31.50 [R] |
| CTA Network B (SPY) | ≈ $1 [K] | ≈ $23 [K] |
| UTP (QQQ) | ≈ $1 [K] | ≈ $24 [K] |
| Cboe indices (SPX, VIX, VXN) | ≈ $1–2 [K] | ≈ $10–20 [K] |
| Nasdaq GIDS (NDX) | ≈ $0.50–1 [K] | ≈ $5–10 [K] |
| CME (ES, NQ) | ≈ $3–15 [K] | ≈ $105–125 [K] |
| **Per-user subtotal** | **≈ $8–22** | **≈ $200–235** |

Illustration, at a 5% professional share and mid-range figures (≈ $14 non-pro, ≈ $215 pro):

| Paying subscribers | Per-user block / month | Fixed block / month | All-in / month |
| --- | --- | --- | --- |
| 100 | ≈ $2,400 | ≈ $7,000–15,000 | **≈ $9,500–17,500** |
| 300 | ≈ $7,200 | ≈ $7,000–15,000 | **≈ $14,000–22,000** |
| 1,000 | ≈ $24,000 | ≈ $7,000–15,000 | **≈ $31,000–39,000** |

Three things the table cannot price: the external API customers who receive real-time underlying
prices today would have to be licensed as sub-vendors or counted as our users (most vendors will refuse
the former at our size); every subscriber must self-certify pro/non-pro at signup and be reported
monthly; and a professional true-up on the existing TradeStation accounts is a separate exposure the
audit already covers. Branch A (derived-only) keeps only the vendor platform, the server device fees,
the index feeds and the CME professional/derived lines: **≈ $2,000–6,000/mo with no per-user block**,
which is why the audit's recommendation is B + C.


---

## 6. Corrections to earlier repo documents

- `docs/design/historical-options-data-vendors.md` §2a says SPX/SPXW are "Cboe-proprietary, not on the
  OPRA tape". **Wrong for option quotes**: OPRA consolidates every US listed option, including SPX,
  SPXW, VIX and NDX options (single-listing only means one exchange contributes them). **Right for the
  index value**: the SPX level is S&P DJI data disseminated via Cboe's index feed and is licensed
  separately. The same doc's "OPRA Non-Display Use ≈ $2,000/mo" should be re-read as professional
  device fees following the data into server use; the $2,000 figure matches CTA/UTP non-display
  categories rather than an OPRA line.
- `docs/compliance/market-data-licensing-audit-2026-09-02.md` and the historical brief disagree on the
  OPRA redistribution figure ($1,500 vs ≈ $650 in older schedules). Pull the current fee schedule from
  opraplan.com before quoting either.

## 7. Sources

**Reachable and read in this pass [V]:** `massive-com/client-python` (README rebrand notice dated
2025-10-30; `massive/websocket/models/common.py` Feed and Market enums including Indices, Futures and
Business feeds; `examples/` using `I:SPX`, `I:VIX`); PyPI `massive` 2.8.0 (2026-05-26) and
`polygon-api-client` 1.16.3 (2025-10-30); `databento/databento-python` (`databento/common/publishers.py`
dataset enum incl. `OPRA.PILLAR`, `EQUS.SIP`, `GLBX.MDP3`, `MAIN.CGIF`, `CGI.CGIF`; CHANGELOG through
0.86.0, 2026-09-01; parent symbology `SPY.OPT`, `ES.FUT`); PyPI `thetadata` 1.0.10 (2026-08-14, gRPC,
stock/option/index endpoints); `intrinio/intrinio-realtime-python-sdk` README (OPRA WebSocket; equities
providers IEX, DELAYED_SIP, NASDAQ_BASIC, CBOE_ONE); PyPI `intriniorealtime` 6.3.0 (2026-04-16);
`alpacahq/alpaca-py` `alpaca/data/enums.py` (`DataFeed.SIP`); `akapur/pyiqfeed` README (Wine on Linux,
protocol 6.x); `dxFeed/dxfeed-python-api` README (superseded, Python ≤ 3.9); `dxFeed/dxLink` README (client languages, no
Python); `MarketDataApp/documentation` at 2026-09-04 (`account/plans/commercial.mdx`,
`account/data-policies/data-redistribution.md`, `account/data-policies/professional-status.md`);
`alpacahq/alpaca-docs` (only the Broker-API IEX redistribution line was found); Databento's public OPRA
overview (index options SPX/VIX/NDX on OPRA; OPRA participant list).

**Blocked from this environment (not read today):** opraplan.com fee schedule and agreements,
sec.gov / federalregister.gov OPRA filings, cboe.com, cmegroup.com, massive.com, databento.com,
dxfeed.com, thetadata.net, intrinio.com, barchart.com, quotemedia.com, iqfeed.net, tradier.com,
marketdata.app. Every figure tagged [K] or [R] must be confirmed against those pages or in writing with
the vendor before it goes into a budget or a contract.
