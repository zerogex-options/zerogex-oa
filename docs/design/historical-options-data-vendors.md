# Historical Options Data — Vendor Decision Brief

**Status:** decision brief (Theme A of the backtesting-enhancement program) · **Owner:** ZeroGEX
**Purpose:** choose how to acquire deep, multi-regime historical intraday option chains so the
backtester can reach past the ~90-day live window that today caps it.

> **Pricing note:** figures below are approximate, retail-tier, as of mid-2026 and are
> quote-gated or change often. Treat them as an order-of-magnitude decision aid and confirm
> on each vendor's page (linked) before committing. Several vendors bot-block automated
> fetches, so these were not machine-verified here.

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
the wings, all listed expirations. That's a narrow, liquid slice — far cheaper than "full OPRA".

## 3. Vendor comparison

| Vendor | ~Retail price (options) | Depth | Granularity | Greeks/IV | Redistribution licensing | Best fit for us |
|---|---|---|---|---|---|---|
| **ThetaData** | ~$80–160/mo tiers (+ options add-on) | ~12 yrs OPRA cached | Tick quotes/trades + minute; bulk download | Yes (don't need) | Personal-use default; commercial/redistribution by separate agreement | **Backtest data-lake front-runner.** Retail-affordable, bulk historical, popular for exactly this. |
| **Databento (OPRA.PILLAR)** | Usage-based + subs; pay-per-GB | Full OPRA history | **Tick** (MBO/MBP/quotes/trades) | No (we compute — a *fit*, not a gap) | Clear, tiered incl. redistribution options | **Strong dark-horse.** Transparent usage pricing, modern API, and we already compute Greeks. Good for a bounded SPX/SPY/QQQ pull. |
| **Polygon.io** | ~$79+/mo options add-on; unlimited calls on higher tiers | Years | Minute aggregates → tick | Yes (don't need) | Business/enterprise tier for redistribution | Good raw infra if we also want a live-feed vendor migration later. |
| **ORATS** | Higher (hundreds/mo → enterprise) | Long EOD + 1-min history | 1-min + EOD, IV-surface-grade | Yes, premium quality | Enterprise | Overkill — we'd pay for the Greeks/IV we already make. |
| **CBOE DataShop (LiveVol)** | Per-dataset, can be pricey | Authoritative, deep | Tick/quote + minute | Yes | **Best-in-class** (direct from the exchange) | The **B2B redistribution** answer when Stream-2 goes live; heavier for a pure backtest lake. |
| **dxFeed** | Enterprise | Deep | Tick + historical service | Yes | Strong redistribution | Institutional; best if we migrate the *live* feed too. |

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

**Rough budget:** a bounded 3-symbol, ~5-year historical options pull is typically a **few hundred
to low-thousands of dollars one-time (or a few months of a mid-tier sub)** — small next to what it
unlocks in conversion and the B2B asset. Confirm exact quotes before committing.

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
