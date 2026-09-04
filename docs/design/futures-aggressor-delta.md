# Aggressor delta on the ES / NQ tape — feasibility

**Question:** a churned customer's model of price pressure is aggressive (market) orders
→ cumulative volume delta → "50% of the picture", with the other half being resting
limit orders. He trades ES. ZeroGEX computes an aggressor split, but on the **options**
tape. How hard is it to run the equivalent on the futures tape, and is the second half
already modelled somewhere?

**Short answer:** half of it is already in the database, unused, and better-founded on
futures than on equities. The other half has plumbing but no ingestion. And the "other
half" hypothesis is worth testing rather than repeating.

---

## 1. What exists today, on options

The pipeline is real and complete:

| Stage | Where |
|---|---|
| Lee-Ready classification | `MarketDataIngestion._classify_volume_chunk` (`src/ingestion/main_engine.py`) |
| Prior-tick NBBO selection, with staleness fallback | `_select_classify_quote` |
| Per-contract accumulation across snapshots, reset-safe | `_ingest_snapshot_into_accumulator` |
| Persistence | `flow_contract_facts.buy_volume` / `sell_volume` / `delta`, 60-second buckets (`AGGREGATION_BUCKET_SECONDS`) |
| Delta-scaled aggregate | `fetch_hedge_impulse` (`src/tradeworkz/flow_context.py`): `Σ deltaᵢ · (buy_volumeᵢ − sell_volumeᵢ) · 100` |

That last line is precisely the customer's half #1, scaled into share-equivalents. Its
docstring already tells the dealer-hedging story: "When customers aggressively buy
options the dealer inherits the opposite delta and must neutralize it in stock within
minutes."

**The classifier itself is instrument-agnostic.** `_classify_volume_chunk` takes
`(volume_delta, last, bid, ask, mid, band_pct)` and returns
`(ask_vol, mid_vol, bid_vol)`. Nothing in it knows what an option is. Porting *the
algorithm* is not the work; feeding it is.

## 2. What already exists for ES / NQ — and is not being used

`futures_quotes` carries `up_volume` and `down_volume` for ES and NQ, populated from
TradeStation's `UpVolume` / `DownVolume` bar fields by all three writers (the live
streamer `src/ingestion/futures_underlying_ingester.py`, the backfill tool
`src/tools/futures_backfill.py`, and `stream_manager`). One-minute bars, keyed by the
cash index, retained at `DATA_RETENTION_DAYS` (default 90) and extendable via
`FUTURES_BARS_RETENTION_DAYS` plus `futures_backfill`.

`SUM(up_volume − down_volume)` over a session **is a running cumulative volume delta
for ES**. It is already collected. Nothing reads it for futures.

### How good is it?

This has been reviewed before, and the caveat has to be carried honestly.
`docs/CODE_REVIEW_2026-05-15.md` §D8:

> TradeStation's bar stream provides up_volume / down_volume (Lee-Ready-like tick-test
> classification on consolidated NBBO). These are NOT trade-side attribution: a
> 1000-share print between exchanges with NBBO movements can land on either side
> depending on the order of bookkeeping events. … Signals that read
> `(up_volume − down_volume)` as "real-time directional flow" are reading classified
> tape, not flow.

Two of the three failure modes in that paragraph are **specific to the equity
consolidated tape and do not apply to CME futures**:

- *Cross-exchange fragmentation.* ES trades on one venue, in one central limit order
  book. There is no consolidated NBBO to race, and no inter-exchange bookkeeping order
  to get wrong.
- *Print-versus-quote sequencing across venues.* Same reason.

What does still apply: a tick test is a tick test. It infers the aggressor from price
movement rather than reading it. CME's own market data (MDP 3.0) publishes an
aggressor-side flag per trade — the ground truth — and TradeStation's bar API does not
surface it. So the honest description of the ES series is **"uptick/downtick-classified
volume"**, not "aggressor delta", and D8's naming recommendation applies here too.

Even so, the tick test on a single-venue futures book is materially better founded than
the same field on SPY, which is the one the review was written about.

## 3. Three ways to get a futures CVD, in ascending cost

### Tier 1 — read what is already there (hours)

Expose `up_volume − down_volume` from `futures_quotes` as a session-cumulative series
for ES/NQ. No ingestion change, no new vendor call, no schema change; ~90 days of
history already sitting there. It is a SQL sum over a table that already has the right
index (`idx_futures_quotes_index_symbol_timestamp`).

Label it as what it is — uptick-biased vs downtick-biased volume — and D8's complaint
is answered rather than repeated.

**This is the fastest path to a defensible answer for this customer, and it can be
backtested immediately**, including against the excursion measures in
`research/msi_regime_excursion/`, since both key on the same bars.

### Tier 2 — snapshot Lee-Ready on the futures tape (days)

Stream quotes for `@ES` / `@NQ` (`TradeStationClient.get_stream_quotes` →
`marketdata/stream/quotes/{symbols}`, already implemented) and run the existing
`_classify_volume_chunk` against volume deltas between snapshots, mirroring
`_ingest_snapshot_into_accumulator`.

**This is probably not worth doing, and it is worth being clear about why.** The
options path classifies a *slow* per-contract tape, where a 60-second bucket often
contains a handful of prints and one classification decision is defensible. ES trades
on the order of a million-plus contracts a day with quotes changing many times a
second. Classifying a whole minute's volume delta against one sampled prior-tick NBBO
throws away most of the information the tick test needs. TradeStation's own
`UpVolume`/`DownVolume` is a tick test performed at *tick* granularity by the vendor —
strictly more information than a snapshot reconstruction can recover.

So Tier 2 is **more engineering for a worse number than Tier 1 already provides**. It
would only make sense if the vendor's fields turn out to be unreliable for futures,
which Tier 1 would reveal.

### Tier 3 — true aggressor-side data (weeks, plus a vendor decision)

A feed that publishes the aggressor flag per trade (CME MDP 3.0, or a vendor that
passes it through). This is the only way to get what the customer means by CVD in his
own terms — "you just add up all market buy and market sell orders and you get a net
figure". It is a data-sourcing decision before it is an engineering one.

**Recommendation: Tier 1 now, and let its backtest decide whether Tier 3 is worth
buying.** If uptick/downtick-classified ES volume already carries the predictive
content the customer describes, the exact aggressor flag is a refinement. If it carries
none, that is worth knowing before signing a market-data contract.

## 4. His "other half": resting limit orders

> If you can model the other half on limit orders, you get the full picture.

### What exists

`TradeStationClient.get_market_depth_quotes` (`marketdata/marketdepth/quotes/{symbols}`,
Level 2) is implemented and reachable — and is **not used anywhere in ingestion**. Its
only caller is the ad-hoc probe at the bottom of the client (`--test depth`), which
means the first step is a five-minute experiment: run that probe against `@ES` and see
what the vendor actually returns for a futures symbol, how deep, and at what update
rate. Nothing in the codebase currently establishes that it works for futures at all.

Book data is also expensive to store (it is a stream of book states, not a bar), so
"model the other half" would mean deciding upfront what summary is kept — depth
imbalance at N levels, book pressure, replenishment rate — rather than the book itself.

### The hypothesis worth testing

There is a real argument that dealer gamma hedging *is* structurally the other half: a
long-gamma dealer sells rallies and buys dips, which is passive liquidity supply that
absorbs aggression. On that reading, GEX is a model of resting supply inferred from the
options book rather than read from the order book — and it would predict what the
customer reported, namely that walls work as levels while a coarse regime scalar does
not generate targets.

**It is a hypothesis, and it should be labelled as one until tested.** Three specific
reasons it might be wrong:

1. **Hedging is not necessarily passive.** A short-gamma dealer hedges *with* the move
   and is frequently an aggressor — the same flow that the customer's CVD would count
   on the taker side. `fetch_hedge_impulse`'s own docstring describes exactly this
   aggressive channel: an inherited delta that "must be neutralized in stock within
   minutes". So gamma hedging supplies liquidity in one regime and consumes it in the
   other, and a model that treats GEX as resting supply is only half right by
   construction.
2. **GEX is a positioning model, not a book snapshot.** Production infers dealer sign
   from `sign(type) · γ · OI` — a convention, not an observation. `research/mm_attributed_gex/`
   exists precisely because that attribution is a reconstruction; its README is blunt
   that even exchange-tagged attribution is "still a reconstruction".
3. **Options-implied supply is not order-book supply.** A dealer's hedging obligation
   at a strike says what they will need to trade, not what is currently resting on the
   bid.

### How to test it rather than assert it

The claim "GEX levels behave like resting liquidity" is falsifiable, and the
measurements are largely built:

- **Level-holding.** At call/put walls, does realized excursion *through* the level
  differ from excursion through a matched non-wall price? `excursion.py` already
  measures travel from any timestamp; `src/analytics/walls.py` supplies the levels.
- **Absorption.** Does aggressive volume (Tier 1's ES uptick/downtick series) produce
  *less* price movement per contract near a high-|GEX| strike than away from one? That
  is the resting-supply claim stated as a testable ratio, and it needs the futures CVD
  from §3 — which is the concrete reason to do Tier 1 first.
- **Sign dependence.** The absorption effect must **reverse** between long-gamma and
  short-gamma regimes if the mechanism is real. If it does not reverse, whatever is
  being measured is not dealer hedging.

That third test is the one that distinguishes the hypothesis from a coincidence, and it
is the one to run first.

## 5. Summary

| | Status | Cost |
|---|---|---|
| Options aggressor delta | Built, persisted, delta-scaled, in use | — |
| **ES/NQ aggressor proxy** | **Already collected in `futures_quotes`, unused** | **Hours** |
| ES/NQ snapshot Lee-Ready | Plumbing exists; would be worse than the above | Days, not advised |
| ES/NQ true aggressor flag | Needs a vendor decision | Weeks + data cost |
| ES/NQ order book (his half #2) | Client method exists, never used, unverified for futures | Probe first |
| GEX-as-resting-supply | Hypothesis; three tests specified above | Needs the futures CVD first |

The finding worth acting on: **the customer asked for a futures CVD, and ZeroGEX has
been quietly storing a usable approximation of one for ES and NQ the whole time.**
Naming it honestly and exposing it is a small piece of work, and it is a prerequisite
for testing the more interesting claim about what GEX actually models.
