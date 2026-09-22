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

## S1 — Signed underlying volume dies at cutover · **blocks step 15**

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
for equities. That view goes dark whatever is decided — the only question is whether it is
removed or left publishing NULLs.

---

## S2 — `skew_delta` abstains structurally on SPX and NDX

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
The chain simply does not reach it. Widening `INGEST_STRIKE_PCT_RANGE` to 5% with a
matching `INGEST_STRIKE_COUNT_MAX` would make the component work as designed.

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

## Ranked

1. **S1** — regression, blocks cutover, reaches paying customers. Three of the four views are
   recoverable by differencing `stock_snapshot_ohlc` volume per poll, on the same licensing
   posture F4 already accepted for options; `underlying_buying_pressure` is not recoverable at
   all. Decide before step 15, and put the equity-tape question to ThetaData in the same message
   as F5.
2. **S2** — pre-existing. Two of four underlyings have never had a working skew signal.
   Fixable by widening the chain, which is a real cost and a separate decision.
3. **S3** — pre-existing, now self-reporting, effect measured at ≤0.1%. No urgency.

All three share one shape: **a number that reads as measured when the measurement was
never possible.** The fix pattern that worked for S3 — publish whether the input was
reachable, next to the value — applies to the other two.
