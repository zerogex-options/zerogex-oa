# Signal component sweep — gates the data can never cross

**Why:** two defects of the same shape turned up by accident during the vendor
migration — a threshold compared against a quantity that ingestion config bounds below
it, producing a number that *looks* measured and is not. Neither announced itself. This
is a deliberate search for the rest.

**Scope:** the six `BasicSignalEngine` signals, the six registered MSI components, the three
gamma delegates `gamma_anchor` blends internally, and the database views they read. Static
analysis plus arithmetic against production config; no live data.

**Method:** every module-level threshold in `src/signals/`, checked against the widest
value production can actually produce given `INGEST_STRIKE_PCT_RANGE = 3.0`,
`INGEST_STRIKE_COUNT_MAX = 40`, `INGEST_EXPIRATIONS = 3` and the provider's declared
capabilities.

The strike-count cap binds before the percentage does on every dense chain, so the
**actual** reach is narrower than the 3% configured:

| underlying | strikes in ±3% band | capped | actual reach |
|---|---|---|---|
| SPY | 40 | 40 | ±3.03% |
| QQQ | 45 | 40 | ±2.68% |
| `$NDXP.X` | 70 | 40 | ±1.71% |
| `$SPXW.X` | 94 | 40 | **±1.29%** |

---

## S1 — Signed underlying volume dies at cutover · **resolved, `b391764` + this commit**

`tradestation.py:447` declares `signed_underlying_volume=True`. `thetadata.py:743`
declares `False`, and `_bar_from_row` sets `up_volume=None, down_volume=None` —
deliberately, because the feed genuinely cannot report a signed split.

`_upsert_underlying_quote` passes those through, so `underlying_quotes.up_volume` and
`.down_volume` become NULL for every symbol on the first cycle after cutover. **This is
a regression the migration introduces, not a pre-existing gap.**

Four views are built on those columns:

| view | uses | survives on unsigned volume? |
|---|---|---|
| `underlying_vwap_deviation` | `SUM(close * (up+down))`, `SUM(up+down)` | **yes** — needs the total only |
| `opening_range_breakout` | `AVG(up+down)`, `STDDEV_SAMP(up+down)` | **yes** — total only |
| `unusual_volume_spikes` | `AVG/STDDEV(up+down)`, plus `up/(up+down)*100` | **partly** — spike detection yes, buy-pressure column no |
| `underlying_buying_pressure` | `up - down`, `up/(up+down)`, bias labels | **no** — needs the split |

Consumers: `src/api/queries/technicals.py`, `src/api/queries/signals.py`,
`src/api/main.py`, `src/tradeworkz/context.py`, `src/signals/unified_signal_engine.py`.
So this reaches customer-facing API endpoints, TradeWorkz bot decision context, and the
MSI engine's VWAP input at once.

`unified_signal_engine.py:297` is the sharpest case: cash indices have no volume of
their own, so SPX and NDX VWAP is computed from an ETF proxy's `up_volume + down_volume`.
With NULLs, `COALESCE(pv.volume, 0)` makes `cum_vol` zero and `vwap` comes back NULL.

**What is recoverable.** ThetaData's `stock_snapshot_ohlc` does carry volume
(11,145,125 for QQQ on the 2026-09-22 probe) — but cumulative daily and **unsigned**,
and it is a *realtime* endpoint, not Market Value. Differencing that cumulative figure
per poll restores the three total-only views.

The licensing half of that is closer to settled than it looks. `option_snapshot_ohlc` and
`option_snapshot_open_interest` are already called unconditionally after cutover —
`market_value_endpoints` swaps the *quote* call only (`thetadata.py:903`) — and F4 of the
step-14 findings records that exposure as knowingly accepted, because open interest has no
Market Value equivalent and every gamma figure is weighted by it. Adding `stock_snapshot_ohlc`
is the same posture rather than a new one. Two things still need asking rather than assuming:
it is the **equity** tape (CTA/UTP), not the OPRA exposure F4 accepted, so it is a different
entitlement; and Exhibit A's stocks line was amended to cover the adjusted bid and ask, which
does not obviously reach a plain OHLC call. One email to Bill, alongside the open VIX/VXN
question (F5).

`underlying_buying_pressure` has no path at all: no ThetaData endpoint supplies a signed split
for equities.

### What was done

The premise turned out to be wrong in a useful way. `up_volume` / `down_volume` are a
*classification* — `schema.sql` says so itself where it defines `underlying_buying_pressure`,
and the "Buying"/"Selling" labels were walked back once already for claiming more precision than
a tick test has. What nobody had noticed is that their **sum was also the only record of how much
traded at all**. Two different facts in one pair of columns. That conflation, not the missing
split, is what turned "the feed cannot classify" into "VWAP, opening range and volume spikes all
go dark".

So they were separated:

- `underlying_quotes` gained a nullable `volume` column — NULL means unknown, 0 means nothing
  traded, no `DEFAULT` collapses the two.
- The ingestion payload stopped dropping it. `stream_manager` has carried `volume` in its bar
  dict since it was written (TradeStation's `TotalVolume`); `_store_underlying` built a payload
  without it.
- `src/underlying_volume_sql.py` now names the two expressions once. The total-volume expression
  had been written out by hand in **22 places** across four Python modules and four views; **8**
  value columns and **3** label columns answered `50` / `⚪ Neutral` when the split was absent.
  All of them now read the shared fragments. That spread is the actual lesson here — the defect
  propagated because it was spelled out by hand everywhere instead of named once.
- `underlying_backfill` writes `TotalVolume` into the new column, so backfilled minutes stop
  reading as zero volume. Null, not zero, where the historical endpoint omits it.
- `quote_broadcaster` already forwarded a `volume` key to every websocket subscriber and nothing
  ever populated it, so the frontend had been receiving `volume: null` on every tick.
- On the ThetaData side, `_SessionVolumeDelta` converts the vendor's running **daily** cumulative
  into the per-bar figure `Bar.volume` is defined to mean.

**What is still lost.** The uptick/downtick split itself, and only that: `buy_pct`,
`period_buy_pct`, `uptick_vol_pct`, `tick_bias`. They now return NULL and a `⚪ No Tick Data`
label rather than the `50` and `⚪ Neutral` they used to — which was the same defect this
document is about, sitting inside the fix for this document's worst finding. `volume`,
`vwap`, the opening range and spike z-scores all survive intact.

**Why this is safe to ship before cutover.** The provider abstraction is not yet wired into
production ingestion, so TradeStation starts filling the new column now and the views read it
while TradeStation is still the source. Any defect surfaces while rollback is free; at cutover
ThetaData continues filling the same column.

---

## S2 — `skew_delta` abstains structurally on SPX and NDX · **resolved, this commit**

`unified_signal_engine.py:613-637` sources the component's only input:

```sql
(option_type = 'P' AND strike BETWEEN close*0.95 AND close*0.98)   -- OTM puts, 2-5% OTM
(option_type = 'C' AND strike BETWEEN close*1.02 AND close*1.05)   -- OTM calls
```

Against the reach table above:

| underlying | put band 95–98% | call band 102–105% | result |
|---|---|---|---|
| SPY | 1.03pp reachable | 1.03pp | thin sliver |
| QQQ | 0.68pp | 0.68pp | thin sliver |
| `$NDXP.X` | **0.00pp** | **0.00pp** | **abstains always** |
| `$SPXW.X` | **0.00pp** | **0.00pp** | **abstains always** |

`SkewDeltaComponent.compute` returns `0.0` when either IV is missing, so on the two
index underlyings the signal has never produced a non-abstain reading. On SPY and QQQ it
averages IV over roughly a one-percentage-point sliver at the very edge of the chain —
the least reliable strikes in it.

Unlike S3 this one is *not* physics: OTM IV at 2–5% is perfectly real and measurable.
The chain simply does not reach it.

### What was done — and why not the obvious thing

The obvious fix is to widen the chain until 2–5% is reachable. That was the wrong fix, and
measuring first is what showed it.

**2–5% of spot is a ~30 DTE equity convention.** Production ingests 0–2 DTE. Computed against
this repo's own Black-Scholes at production tenors and IVs, a 2% OTM SPY put at 0.5 DTE is past
the 15-delta strike by a factor of four — its IV is a tail artefact and its quote is a penny
wide. So on SPY and QQQ, where the band *did* reach, it was sampling the least meaningful
strikes in the chain. Widening would have bought more of that on SPX and NDX.

**Delta is tenor-invariant; percent of spot is not.** That is the property this signal needs,
and it is why 25-delta is the risk-reversal convention everywhere else in options analytics.
Measured:

| underlying | chain reach | 25Δ strike, 0.5 DTE | 1 DTE | 2 DTE |
|---|---|---|---|---|
| SPY | ±3.03% | 0.32% | 0.44% | 0.62% |
| QQQ | ±2.68% | 0.42% | 0.58% | 0.82% |
| `$NDXP.X` | ±1.71% | 0.44% | 0.62% | 0.86% |
| `$SPXW.X` | **±1.29%** | 0.29% | 0.41% | 0.57% |

Comfortably inside the chain on all four, SPX included — with no config change and no extra
data. `option_chains.delta` is already computed and stored per contract.

So the selection moved to `ABS(delta) BETWEEN 0.15 AND 0.35`, symmetric around 25 delta. The
band rather than a point is what makes the sample robust to one bad IV solve on a discrete
ladder. `delta IS NOT NULL` plus the existing `implied_volatility IS NOT NULL` filter means no
contract is selected on the strength of a delta derived from the 0.20 default IV.

`skew_available` is now published beside the score, because `ComponentBase` defines `0.0` as
*both* "neutral" and "insufficient data" and the score alone cannot tell a caller which it got.
The sampled contract counts, mean |delta| and mean moneyness are published too, so the selection
is auditable from stored rows.

**What is explicitly NOT fixed: the calibration.** `_SKEW_BASELINE = 0.02` and
`_SKEW_SATURATION = 0.04` were never fitted to data under either selection, and changing which
strikes are sampled changes the distribution of `spread`. The level of this score is unverified
— for the same reason it was unverified before, not a new one. No normalizer was invented for
it, because one fitted to no data is a constant with more moving parts. The measurement is now
possible without new machinery: one RTH session of `signal_component_scores.context_values`
gives the per-underlying distribution of `spread`.

**Calibrate both copies together.** `src/signals/advanced/range_break_imminence.py` carries its
own `_SKEW_BASELINE` / `_SKEW_SATURATION` (`SIGNAL_RBI_*`, same defaults) reading the same two
IVs, and weights its skew sub-score at **30** — against this component's 0.04. A
miscalibration costs far more there.

---

## S3 — `gex_gradient` wing damper (already fixed to report itself)

Recorded here for completeness; see commits `bdc51d8` and `4b705bf`.

`_WING_WINDOW_PCT = 0.04` gates a confidence damper that has never fired, because no
ingested strike reaches 4%. Measured through `_calculate_gex_by_strike`, the share of
|net_gex| beyond 4% at the tenors production ingests is QQQ 0.001, SPY 0.000, SPX 0.000
— so unlike S2, **widening the chain would not help**: at 0–2 DTE there is no gamma out
there to find. The damper now logs when the window is unreachable and publishes
`wing_window_reached` beside `wing_fraction`. The score is unchanged, pending a decision
on narrowing the window to ~2%.

Related and lower severity: `_ATM_WINDOW_PCT = 0.015` makes **every** SPX strike "at the
money" (reach ±1.29%), so `atm_gamma_abs` equals total gamma there. Reporting-only — it
does not enter `compute` — but the published field means less than it appears to.

---

## Checked and clear

| component | threshold | verdict |
|---|---|---|
| `flip_distance` | sat 0.5–5%, fallback 2% | flip comes from the spot-shift profile, bounded by `MAX_FLIP_DISTANCE_PCT = 8%`, not by chain reach — reachable |
| `price_vs_max_gamma` | sat 0.3–3%, fallback 1% | max-gamma strike is inside the chain, so distance is bounded by reach (≤1.29% SPX) — reachable, saturates rather than going inert |
| `net_gex_sign` | scale 2.0e9 | absolute exposure, no distance or tenor gate |
| `dealer_delta_pressure` | norm 3.0e8 | absolute, no gate |
| `vanna_charm_flow` | norms 1.5e8 / 1.0e10 | absolute, no gate |
| `put_call_ratio_state` | saturation 0.4 | ratio of OI sums, no distance gate |
| `local_gamma` | ±1% of spot (`main_engine.py:2927`) | inside the chain on all four — but SPX's ±1.29% reach leaves only 0.29pp of margin, the thinnest in the system |
| `gamma_anchor` | 0.45/0.35/0.20 blend | pure blend of `flip_distance`, `local_gamma`, `price_vs_max_gamma` — inherits all three, no gate of its own |
| `volatility_regime` | VIX centred 20, ±10 | `$VIX.X` is ingested, so the realized-vol fallback should not fire (see note) |
| `positioning_trap` | P/C bands 0.95–1.05, flip sat 0.5%, gex scale 5.0e8 | flip distance is profile-bounded at 8%, the rest are absolute or bar-based |
| `tape_flow_bias` | min premium 2.5e5 | option volume × price — reachable, with the dependency noted below |
| `order_flow_imbalance` | min premium 1.0e5 | same |
| `momentum`, `swing_reversal` | bar counts | underlying bars only |

Two of these are clear conditionally rather than absolutely, which is worth writing down.

`local_gamma` divides `local_gex` (a sum of **absolute** net GEX) by a normalizer from
`component_normalizer_cache`, and falls back to `max(abs(ctx.net_gex), 1.0)` — a **signed** sum —
when the cache has no row. Those are not the same quantity: Σ|x| ≥ |Σx| always, and near a
zero-crossing of `net_gex` (F2's QQQ case) the denominator collapses, `ratio` clamps at 1.0 and the
score pins at −1.0, maximum pinning, whatever the real density. The cache *is* populated for this
field — `normalizer_cache_refresh.py:155`, annotated "consumed by local_gamma" — so the fallback is
not the production path. It is one stale nightly job away from being one.

`volatility_regime` reads `vix_level` and only falls back to realized vol from recent closes.
That fallback compares a per-**bar** return standard deviation against 0.002 / 0.003, which are
daily-scale numbers; on one-minute bars realized runs around 0.0005 and the score would sit near
−0.5 permanently, reading "low vol" as though measured. Same conditional status: `$VIX.X` is
ingested, so this should never be reached.

The two flow components are clear only because `option_snapshot_ohlc` survives cutover. That
data is not Market Value. If F4's accepted exposure is ever reversed they go dark together with
open interest — and with every GEX figure, so that reversal ends the product rather than
degrading it. Written down so the dependency is on the record, not because it is likely.

---

## Found in passing, not fixed here

`get_flow_buying_pressure` (`src/api/database.py`) and the `flow-buying-pressure` Makefile target
both difference the underlying's volume columns with `LAG()`:

```sql
GREATEST(up_volume - LAG(up_volume) OVER (PARTITION BY symbol, DATE(...) ORDER BY timestamp), 0)
```

That is only meaningful if `underlying_quotes.up_volume` is **session-cumulative**. Two things say
it is **per-bar**: `get_stream_bars()` returns `TotalVolume / UpVolume / DownVolume` on each
one-minute bar, and `underlying_vwap_deviation` accumulates `SUM(up_volume + down_volume)` across
the session, which is only correct on per-bar rows. If that reading is right, `period_buy_pct` is
the ratio of minute-over-minute *increases* in classified volume rather than of volume — noisy
rather than visibly broken, which is how it would survive unnoticed.

Probable origin: option flow in this codebase genuinely *is* session-cumulative and is correctly
`LAG()`-differenced (`tests/test_ingestion_volume_baseline.py` documents that for `option_chains`).
The pattern looks copied from there onto a per-bar table.

**Not fixed here, and not asserted as fact** — it needs confirming against a session's rows first,
which this sweep did not do. The two copies must be fixed together or neither. If the data says
*cumulative* instead, then `underlying_vwap_deviation` is what is wrong, which is a much larger
finding.

---

## Ranked

1. **S1** — ~~regression, blocks cutover~~ **done.** Total volume is now its own column, read
   through one shared expression by all eighteen former hand-written sites. Only the tick-test
   split is lost, and it abstains rather than publishing a fabricated 50%. One question still
   goes to ThetaData: `stock_snapshot_ohlc` is the **equity** tape (CTA/UTP), not the OPRA
   exposure F4 accepted, and Exhibit A's stocks line covers the adjusted bid and ask — which
   does not obviously reach a plain OHLC call. Send it with F5.
2. **S2** — ~~pre-existing~~ **selection fixed.** All four underlyings now sample a reachable,
   tenor-invariant strike, and the widening that looked necessary turned out to be the wrong
   fix. Calibration of the baseline and saturation is deliberately left open and is now
   measurable from stored rows — covering `range_break_imminence`'s duplicate constants too.
3. **S3** — pre-existing, now self-reporting, effect measured at ≤0.1%. No urgency.

All three share one shape: **a number that reads as measured when the measurement was
never possible.** The fix pattern that worked for S3 — publish whether the input was
reachable, next to the value — applies to the other two.
