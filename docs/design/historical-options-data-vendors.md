# Historical Options Data — Vendor Decision Brief

**Status:** decision brief (Theme A of the backtesting-enhancement program) · **Owner:** ZeroGEX
**Purpose:** choose how to acquire deep, multi-regime historical intraday option chains so the
backtester can reach past the ~90-day live window that today caps it.

> **Pricing note:** figures below are approximate, retail-tier, as of mid-2026 and are
> quote-gated or change often. Treat them as an order-of-magnitude decision aid and confirm
> on each vendor's page (linked) before committing. Several vendors bot-block automated
> fetches, so these were not machine-verified here.
>
> **Update (2026-07 verification pass):** a multi-source, adversarially-verified research pass
> against primary OPRA/SEC/Cboe sources confirmed the vendor picture but surfaced one correction
> that moves the real number up materially — **the dominant cost is not the vendor, it's the
> exchange-entitlement layer (OPRA "Non-Display Use" ≈ $2,000/mo, plus Cboe-proprietary SPX
> licensing).** This brief originally under-counted that. See the new **§2a** for the corrected
> two-layer cost model, and the private cost/break-even model for the scenario math.

---

## 1. Why this is the highest-impact investment

The engine is strong; the ceiling is **data depth**. Today `option_chains` is pruned at
`DATA_RETENTION_DAYS = 90` and the durable `option_chains_archive` only began accumulating this
spring, so a backtest can reach ~3–4 months — and only over regimes we happened to trade live.
"World-class" backtesting is *defined* by deep, clean, multi-regime history (2020 COVID, the 2022
bear, the Aug-2024 vol spike). That requires a historical **intraday option-chain** source.

This same dataset is also the compounding **B2B alt-data asset** named in
`docs/growth_and_productization_roadmap.md` (Stream 2) — so the spend does double duty.

## 2. The ZeroGEX-specific requirement (this changes the math)

**We compute our own Greeks and IV locally** (`src/ingestion/greeks_calculator.py`,
`iv_calculator.py`, Black-Scholes + Newton-Raphson). The backtester prices fills off
**bid/ask/last** and re-derives everything else. So we do **not** need to pay for a vendor's
Greeks/IV surface — we need the cheaper raw layer:

| Need | Column in `option_chains` | Notes |
|---|---|---|
| **Minute bid/ask/last/mid** | `bid, ask, last, mid` | The fill model's core input. Tick data is fine — we downsample to 1-min. |
| **Open interest** | `open_interest` | Needed to reconstruct OI-weighted GEX / max-pain historically (the archive currently omits it — a gap to close going forward regardless of vendor). |
| **Volume** | `volume`, Lee-Ready split | Nice-to-have for flow; not required for leg fills. |
| Underlying 1-min OHLC | `underlying_quotes` | Cheap/near-free from TradeStation — see the companion backfill tool; not a vendor purchase. |
| Greeks / IV | computed | **We generate these — do not pay for them.** |

Coverage that matters for us: **SPX / SPXW (0DTE, PM-settled), SPY, QQQ**, near-the-money to
the wings, all listed expirations. That's a narrow, liquid slice — far cheaper than "full OPRA"
*at the vendor layer* (but see §2a: the exchange layer does **not** shrink with a narrow slice).

## 2a. The two-layer cost model (the part this brief originally under-counted)

"A plan that lets us redistribute derived data" is not one line item — it's **two stacked
layers**, and the expensive one is not the vendor:

**Layer 1 — the vendor fee** (ThetaData / Databento / Polygon class): ~$125–400/mo for our
narrow slice. Small, scales gently. This is what the §3 table prices.

**Layer 2 — exchange entitlements that flow through regardless of vendor.** These dominate the
budget and a narrow symbol slice does **not** reduce them:

- **OPRA "Non-Display Use" fee — ≈ $2,000/mo per enterprise (Categories 1 & 2).** Computing
  GEX/vanna/charm from a *real-time* OPRA feed is a textbook non-display use — OPRA's enumerated
  list explicitly names "investment analysis," "research & analysis," "risk management." It is a
  **flat** fee (identical at 35 subscribers or 35,000), so staying small does not amortize it
  away; it's a fixed hump you clear once. A 2016 amendment (SEC Rel. 34-79153 / File
  SR-OPRA-2016-02, eff. 2016-11-01) deliberately removed "datafeed" from the fee-schedule
  footnotes so these fees **follow the data downstream** to derived-data operations — i.e.
  "it's just my derived IP" is **not** a fee exemption. Current through the Dec-2025 OPRA filing.
  - *Honest nuance:* whether our use is strictly "non-display" or falls under "redistribution"
    is mildly contestable (the derived output *is* redistributed), but it budgets the same way —
    real-time derived redistribution triggers OPRA/exchange obligations either way.
- **The swing question — does the vendor bundle it?** The fee is owed by the *direct OPRA
  data-feed recipient*. If we consume through a vendor whose commercial redistribution license
  **bundles/covers** the entitlement, we may not owe OPRA the ~$2k separately; if we must hold
  the OPRA agreement ourselves, we do. **This one answer is the difference between a ~$300/mo and
  a ~$3,000/mo all-in cost, and it must be confirmed in writing with each vendor before
  committing** (it's contractual — not answerable from a pricing page).
- **SPX/SPXW are Cboe-proprietary, not on the OPRA tape.** Index-options entitlements are
  licensed separately from OPRA-disseminated equity/ETF options, and the **SPX index _value_**
  (spot) needs a separate **Cboe Global Indices Feed (CGIF)** license, ~$1,000+/mo (Cboe
  Derivatives Market Data pricing, eff. 2026-01-01). Budget this regardless of vendor if the
  redistributable product references SPX. SPY/QQQ ETF options *are* OPRA-disseminated and avoid
  it — a lever if a free/public tier can lean on ETFs.

**The escape hatch: delayed data.** The non-display fee is defined on data received "on a
current basis." **15-minute-delayed** data largely sidesteps it and is broadly redistributable —
so the cheapest legally-clean posture is *delayed derived data for free/public tiers, real-time
reserved for paying subscribers*, exactly as the growth roadmap's `option-contracts` decision
already leans.

**Net:** the §3 table is Layer 1. The honest all-in floor for a *real-time, SPX-centric,
redistributable* product is dominated by Layer 2 — plan for the ~$2k OPRA line and the Cboe SPX
license **unless a vendor bundles them**.

## 3. Vendor comparison

| Vendor | ~Retail price (options) | Depth | Granularity | Greeks/IV | Redistribution licensing | Best fit for us |
|---|---|---|---|---|---|---|
| **ThetaData** | ~$80–160/mo tiers (+ options add-on) | ~12 yrs OPRA cached | Tick quotes/trades + minute; bulk download | Yes (don't need) | Personal-use default; commercial/redistribution by separate agreement | **Backtest data-lake front-runner.** Retail-affordable, bulk historical, popular for exactly this. |
| **Databento (OPRA.PILLAR)** | Usage-based + subs; pay-per-GB | Full OPRA history | **Tick** (MBO/MBP/quotes/trades) | No (we compute — a *fit*, not a gap) | Clear, tiered incl. redistribution options | **Strong dark-horse.** Transparent usage pricing, modern API, and we already compute Greeks. Good for a bounded SPX/SPY/QQQ pull. |
| **Polygon.io** | ~$79+/mo options add-on; unlimited calls on higher tiers | Years | Minute aggregates → tick | Yes (don't need) | Business/enterprise tier for redistribution | Good raw infra if we also want a live-feed vendor migration later. |
| **ORATS** | Higher (hundreds/mo → enterprise) | Long EOD + 1-min history | 1-min + EOD, IV-surface-grade | Yes, premium quality | Enterprise | Overkill — we'd pay for the Greeks/IV we already make. |
| **CBOE DataShop (LiveVol)** | Per-dataset, can be pricey | Authoritative, deep | Tick/quote + minute | Yes | **Best-in-class** (direct from the exchange) | The **B2B redistribution** answer when Stream-2 goes live; heavier for a pure backtest lake. |
| **dxFeed** | Enterprise | Deep | Tick + historical service | Yes | Strong redistribution | Institutional; best if we migrate the *live* feed too. |

> **Verified 2026-07 (primary sources):** **ThetaData's** commercial-licensing program is live
> (startup tier ~$125/mo, internet-delivered, no colo) and its options history runs OPRA NBBO +
> trades + **daily** OI back to ~June 2012. **Cboe DataShop's "Option Quotes"** delivers 1-min /
> custom-N-min NBBO + size, OHLC and volume with **optional OI** and Greeks as a skippable add-on,
> depth to ~2012 — but it is **delayed** (15-min intraday, daily overnight), so it fits the
> *historical lake + delayed SEO page*, **not** a real-time TradeStation replacement. Two claims
> were **refuted**: Cboe depth "back to 2004" (that's platform-wide boilerplate; the product is
> ~2012) and Cboe SPX sourced "via OPRA" (SPX is **Cboe-proprietary**). **Databento, Polygon,
> ORATS, dxFeed** terms were **not** independently verified in that pass — get live quotes; the
> cheapest legally-clean real-time vendor may be one of them, so don't default to ThetaData
> without comparing. The decisive question for every row is **not** in this table — it's the
> §2a bundling question (does the license cover OPRA non-display?), which must be gotten in writing.

## 4. Recommendation

**Two-step, matched to the two goals:**

1. **Backtest depth now (B2C):** buy a **bounded historical pull of SPX/SPXW + SPY + QQQ minute
   (or tick→minute) bid/ask/last + OI** from **ThetaData** (front-runner on price + retail fit) or
   **Databento** (if usage-based pricing on our narrow slice comes in lower, and because we compute
   Greeks anyway). Load it into `option_chains_archive`. Target the regimes worth marketing: at
   minimum 2022 (bear/high-vol) and 2023–2025, ideally back to 2020. This is the single change that
   makes the track record credible.
   - *Do not pay for Greeks/IV tiers.* Our calculator regenerates them from the raw quotes at
     ingest, exactly as it does live — one code path, no vendor lock-in on analytics.

2. **Redistribution later (B2B):** when Stream-2 (selling the dataset/analytics) is greenlit,
   license from **CBOE DataShop** or **dxFeed/Databento**, whose contracts explicitly permit
   redistribution. TradeStation's feed is personal-use only (already flagged in the growth doc), so
   it can't back a B2B product regardless.

**Before either purchase — settle the two crux questions in writing** (they gate legality *and*
cost, and neither is answerable from a pricing page; send them to ThetaData, Databento and Polygon
the same week and compare):
1. Does the vendor's commercial license *explicitly* permit **real-time redistribution of derived
   analytics** to paying subscribers and via a paid B2B API — at what tier/price?
2. Does that license **cover the OPRA non-display entitlement**, or must we hold it directly? (The
   ~$300 vs ~$3,000/mo fork — see §2a.)

**Rough budget:** the *historical* pull — a bounded 3-symbol, ~5-year options download — is
typically a **few hundred to low-thousands of dollars one-time (or a few months of a mid-tier
sub)**, small next to what it unlocks. But the *live redistribution* path carries the **§2a Layer-2
floor on top**: ~$2,000/mo OPRA non-display + ~$1,000+/mo Cboe SPX/CGIF **unless a vendor bundles
them** — the fixed hump that dwarfs the vendor fee. Confirm exact quotes before committing.

## 5. Integration path (once data is chosen)

The engine already reads `option_chains` then `option_chains_archive`, so **no engine change is
needed** — only a loader:

1. **Schema-match loader** — map the vendor's rows to the `option_chains_archive` columns
   (`option_symbol, underlying, strike, expiration, option_type, bid, ask, last, mid, volume,
   open_interest, timestamp` + computed `implied_volatility, delta, gamma, theta, vega` via our
   `GreeksCalculator`). One-time bulk backfill; idempotent upsert on `(option_symbol, timestamp)`.
2. **Widen the forward archive** — capture wings + OI going forward so future history is complete
   (the archive currently carries prices + Greeks but not `open_interest`, so archive-era max-pain
   can't be rebuilt — fix this in `src/tools/backtest_archive.py` regardless of vendor).
3. **Historical signal replay (the decoupling unlock)** — with historical chains present, re-run
   the playbook patterns offline over any window to regenerate `signal_action_cards`, so the
   backtester is no longer limited to signals emitted while we were live. Design this as a follow-up
   (`docs/design/` companion) once the data source is picked; it depends on this decision.

## 6. The decision to make

- **Approve a bounded historical options purchase** (which vendor, which symbols, how far back), OR
- **Defer** and market the record honestly as *live-tracked and deepening daily* (the archive grows
  every session) while the no-spend pieces (underlying backfill, forward-archive widening) proceed.

Everything downstream of "we have deep chains" — multi-year equity curves, walk-forward on real
regimes, the B2B dataset — is gated on this one call.

---

**Sources (verify current pricing/terms):**
[ThetaData pricing](https://www.thetadata.net/pricing) ·
[ThetaData options](https://www.thetadata.net/options-data) ·
[Databento OPRA.PILLAR](https://databento.com/datasets/OPRA.PILLAR) ·
[Polygon options](https://polygon.io/options) ·
[ORATS](https://orats.com/) ·
[CBOE DataShop](https://datashop.cboe.com/) ·
[dxFeed](https://dxfeed.com/)

**Verified 2026-07 (primary / authoritative):**
[OPRA non-display amendment — SEC/Federal Register 2016-26136](https://www.federalregister.gov/documents/2016/10/31/2016-26136/options-price-reporting-authority-notice-of-filing-and-immediate-effectiveness-of-proposed-amendment) ·
[OPRA Fee Schedule](https://cdn.opraplan.com/documents/OPRA_Fee_Schedule.pdf) ·
[Cboe DataShop — Option Quote Intervals](https://datashop.cboe.com/option-quote-intervals) ·
[Cboe derivatives market-data pricing (eff. 2026-01-01)](https://www.cboe.com/notices/content/?id=57029) ·
[ThetaData commercial licensing](https://www.thetadata.net/commercial-use)

**Refuted / do not rely on:** Cboe Option Quotes depth "back to 2004" (actual ~2012) · Cboe SPX
sourced "via OPRA" (SPX is Cboe-proprietary) · a ThetaData "40 ms / 14,000-contract" performance
benchmark (marketing). **Still unresolved (contractual, get in writing):** whether any vendor's
commercial tier explicitly grants real-time *derived* redistribution, and who bears the OPRA
non-display fee in a vendor-intermediated setup.
