# ZeroGEX Backend Audit — 2026-11-22

Scope: `zerogex-oa` at `release` `cafc2bd`, reviewed as both a software engineer
(correctness, security, reliability, performance) and a quantitative analyst
(are the published numbers what they claim to be). The website review is in
`zerogex-web/docs/code-review-2026-11-22.md`.

All fixes are on branch `claude/gifted-noether-uje8y8`, one commit per finding.
Each defect fix has a regression test that fails on the old code; each query
rewrite has a parity test showing identical results on Postgres 16. Each commit
message has the full reasoning and the numbers behind it; this document is the
index.

---

## Deploy notes

**No schema changes.** Nothing in this branch needs `make schema-apply`.

**Check before Friday 2026-11-27 (early close).** Five fixes below key off the
`NYSE_HALF_DAYS` list in `/home/ubuntu/zerogex-oa/.env`. If the line is
missing, they do nothing and the old 16:00 behavior stays. `.env.example`
carries the right list (`2026-11-27,2026-12-24,...`). Check with:

```bash
grep NYSE_HALF_DAYS /home/ubuntu/zerogex-oa/.env
```

```bash
make calendar-check
```

**What will look different after the deploy (all intended):**

- The `zero_dte_imbalance_drift` playbook pattern starts producing cards. It
  has never fired in production (it looked up a signal name that doesn't exist).
- Dealer vanna and charm flip sign on put-heavy books. `vanna_charm_flow`,
  `eod_pressure`, `market_pressure` and the TradeWorkz flow context can point
  the other way from before. That is the correction.
- Net GEX and the walls near spot get smaller on 0DTE-heavy books (contracts
  with no solved IV no longer get made-up gamma).
- Order-flow imbalance stops reading ±1.00 on busy two-way tapes, so the MSI
  can read lower on those days.
- The scorecard's regime labels swap: a high MSI is now called "short gamma".
- Backtests a member saved through their own API key before this deploy are no
  longer visible from that key (they were stored with no owner, and ownerless
  rows are now private). Reassign on request; SQL below.
- A TradeWorkz follow with malformed `channels` now gets a 422 instead of being
  stored.

**After the deploy (optional, can wait):**

1. *Re-fit the dealer vanna/charm normalizers sooner.* The nightly 04:30 ET
   refresh samples the last 20 calendar days, so on its own it takes about
   three weeks to forget the old sign. To re-fit after one session instead,
   change the existing `NORMALIZER_DEPLOY_CUTOFF` line in `.env` to the deploy
   date (a bare date means midnight ET; for a mid-session deploy use the form
   `2026-11-23T11:00`). An invalid value makes the refresh refuse to run.

   ```
   NORMALIZER_DEPLOY_CUTOFF=2026-11-23
   ```

   The commit messages for the dealer-delta and order-flow fixes also say to
   run the refresh; that was wrong: those two signals use fixed scales, not
   the cache. `local_gex` shifts a little with the no-IV fix and re-fits on
   its own.

2. *Find TradeWorkz follows stored with a bad shape* (they no longer break
   anything, but they don't deliver either):

   ```sql
   SELECT end_user, bot_id, channels FROM tw_bot_followers
   WHERE jsonb_typeof(channels) <> 'object'
      OR EXISTS (SELECT 1 FROM jsonb_each(channels) e
                 WHERE e.key NOT IN ('in_app','email','webhook')
                    OR jsonb_typeof(e.value) <> 'boolean');
   ```

3. *Reassign a backtest run or config saved through an API key*, if a member
   asks (`<user_id>` is the account that owns the key):

   ```sql
   UPDATE backtest_configs SET end_user = 'key:<user_id>' WHERE id = <id>;
   ```

   ```sql
   UPDATE backtest_runs SET end_user = 'key:<user_id>' WHERE id = <id>;
   ```

4. *Past forecast receipts* were graded with the $1.00 pin default and the 0.5%
   regime threshold instead of what each morning committed (see F1). The
   verdict columns can be regraded from the stored actuals, but that needs a
   small tested tool, which this branch does not include. It can wait.

---

## Fixed

### Published numbers (quant correctness)

**F1. Forecast receipts graded against the wrong tolerance** — `27a4362`
The 16:05 grader read `pin_tolerance`, `regime_move_threshold` and the raw band
with `row.get()`, but its SELECT never named those columns, so asyncpg returned
`None` and every receipt fell back to defaults: pins at $1.00 (SPX commits
~$10-20), regime calls at 0.5% instead of the VIX-scaled threshold, and the
`raw_*` verdicts NULL. The calibrator then kept widening the published
tolerance against a grade that never used it.

**F2. Scorecard called trend days "long gamma"** — `19d4abb`
`_label_regime` mapped a high MSI to long gamma. The MSI runs high when dealers
are short gamma (its top band is `trend_expansion`), so the 16:15 tweet and
`/api/scorecard/*` had the two regimes swapped.

**F3. Dealer-delta estimate's moneyness slope was backwards** — `d799f7c`
All four copies of the per-strike delta proxy gave in-the-money calls low
deltas and out-of-the-money calls high ones (spot 606: the 594 call 0.30, the
618 call 0.70). It feeds `dealer_delta_pressure` (17 MSI points),
`market_pressure`, `range_break_imminence`, Trade Bias and Action Card
direction. One shared helper, now correct.

**F4. Order-flow imbalance saturated on two-way tapes** — `818910a`
`(call_net − put_net) / (|call_net| + |put_net|)` reads ±1 for any
opposite-signed pair: a $54M session with calls +$60k and puts −$50k scored
+1.00, enough MSI points to change the regime label that gates every playbook
pattern. Now divided by gross premium traded (as `tape_flow_bias` already was).

**F5. Dealer vanna/charm inverted every put** — `f75817d`
GEX and the gamma flip assume dealers are long calls and short puts; the
vanna/charm columns used `−(call + put)`, the flow of a dealer long everything,
so a put-heavy book read the opposite direction from `close_charm_flow`. Now
`put − call`, checked against a full reprice of the dealer book.

**F6. Contracts with no solved IV added made-up gamma to GEX** — `81896f1`
Unsolvable contracts (zero-bid wings, ITM 0DTE) were priced at a flat 20% vol,
and that gamma went into `gex_by_strike`. On a production-shaped book about
half of all 0DTE contracts had no IV: net GEX +8.6%, strikes within 0.5% of
spot +30-37%. They now count for open interest and volume only.

**F7. AM-settled SPX/NDX monthlies solved to the wrong expiry** — `98b8459`
The Greeks used the 09:30 settlement but the IV solver fitted to 16:00, so IV
came out low and gamma high (3.9x on expiry morning for the 2026-12-18
quarterly). The solver now gets the same settlement time.

**F8. Yesterday's option volume booked as today's 09:30 flow** — `0f8386b`
A contract that had not traded yet today kept yesterday's cumulative volume in
the stream, and the 09:30 accumulator booked all of it as one bucket: a phantom
bucket per contract, Tuesday to Friday, in `option_chains`,
`flow_contract_facts`, PCR and the flow series. Stored volumes now carry the
day they were read on.

**F9. Playbook patterns looked up names that don't exist** — `6295c9c`
Patterns asked for `0dte_position_imbalance` (emitted as
`zero_dte_position_imbalance`) and the label "Range Fade" (renamed
"Range-Bound"). `zero_dte_imbalance_drift` never fired; the iron condor's
"too directional" veto never applied and it refused the most range-bound state.
A new test fails if a pattern references a signal or label the engines don't
produce.

**F10. Selling an option with no bid filled at the last print or the ask** — `92a78b0`
A long 0DTE call quoted 0.00/0.05 with a morning print at 0.90 was closed at
0.90, so its stop never fired; a credit spread could book more than its maximum
gain. TradeWorkz marks, the research harness and member backtests all use this
model. A sale into no bid now fills at 0.

**F11. Morning SPX/NDX anchor read the futures roll as an overnight move** — `cdc2fba`
The pre-market index anchor adds the change in the continuous future; across a
roll the two prints are different contracts (Sep 2026: ES basis 8bp → 94bp), so
the immutable morning band would have been ~60 SPX / ~250 NDX points high. It
now falls back to the cash close across a roll. Next roll: the session into
Friday 2026-12-11.

**F12. Receipt tweet labeled realized vol with the morning's prediction** — `9fcc96a`
"Realized vol: compression (1.42× normal)". The label now comes from the
realized ratio.

### Early-close days (2026-11-27, 2026-12-24)

**H1. 0DTE options expired at 16:00 instead of 13:00** — `f20489c`
Three phantom hours of time value: 0DTE IV solved too low all day (0.059
against a true 0.12 at noon), and the contract stayed "alive" after it stopped
trading. The engine's roll-off, ATM-IV window and spread-stats freeze also
keyed off 16:00.

**H2. Session closes took a 16:00 after-hours bar** — `20f665c`
An ETF's "close" became its 16:00 after-hours price (the site header all
weekend after the half day); an index skipped the half day entirely.

**H3. End-of-day signal ramps counted down to 16:00** — `a780ccb`
EOD Pressure read zero through the real last 90 minutes, then switched on after
the close.

**H4. Forecast receipt graded the range through 16:00** — `886f928`
Three hours of thin after-hours ETF prints counted toward the graded low/high.

**H5. Intraday cone kept issuing claims after 13:00** — `ba5ad0d`
Fires ran to 15:30 with horizons to 17:30, graded against after-hours prints or
an index frozen at 13:00 (a flat tape "holds" every band). Last fire is now
12:30 on a half day. Also fixed: the writer's holiday check compared a string
to a set of dates.

### Reliability and security

**R1. One malformed TradeWorkz follow could freeze a bot** — `7fc9f2d`
`channels` was stored as whatever JSON the caller sent; the fan-out runs inside
every open and close, so a list or a long key made each trade raise and roll
back until the row was removed by hand. Any signed-in account could do it. The
body is validated now (422), and the fan-out tolerates rows already stored.

**R2. A member's backtest stopped live bots' exits** — `9fbbc1f`
The backtest replay clock was module-global. A replay in an API worker that
also ran the TradeWorkz loop made live bots read replay time: a position past
its stop came back "inside its minimum hold". The clock is now per-context.

**R3. Junk API keys could exhaust worker memory** — `c4713d7`
The key cache stored every miss forever with no size limit. Now an LRU capped at
10,000 entries; misses live 10 seconds.

**R4. Backtests saved through an API key were readable by other members** — `1f3ba73`
Direct API callers were stored with no owner, and a missing owner passed every
ownership check, so another Pro member's key could list, publish, edit or
delete them. The owner is now the key's account, and ownerless rows are private.

**R5. Caller-chosen request IDs went into every log line** — `6ee6c6d`
An 8 KB `X-Request-Id` on 500 rejected requests a second made the audit log
~37x larger, so the 2 GB journal rotated every service's history away in 10-15
minutes; the header could also forge audit lines. Only a short safe token is
adopted now.

**R6. TradeWorkz trade-audit `since`/`until` always returned 500** — `cd98ab1`
Raw strings were bound to `timestamptz`. They are parsed now; a bad value is a
400.

### Performance

**P1. GEX history reads scanned all history for their bucket floor** — `dd06057`, `2fe308b`
Five `gex_summary` reads (historical GEX, strike profile, max-pain series,
heatmap) skipped the tiered floor: ~290 ms vs ~0.2 ms on 266K rows, per request,
growing daily. Parity-tested against Postgres 16.

**P2. Live session levels were three full scans** — `d13da37`
Timezone casts in every WHERE defeated the index: ~380 ms on 8.5 months of SPY,
from 04:00 ET on. Now plain time ranges, parity-tested on six calendar cases.

**P3. Flow "Prior" session lookup defeated the index** — `8a09e19`
~55 ms vs ~0.05 ms per uncached poll.

**P4. News headlines fetched CNBC on every request** — `06b8444`
Three sequential feeds per request on the shared thread pool stalled unrelated
routes under load and risked an IP block. Cached per worker for 120 seconds,
one fetch at a time.

---

## Deferred

Found and checked, not fixed in this pass. "Can wait" means no member-facing
harm beyond what is described.

### High

| ID | Finding | Why deferred |
|----|---------|--------------|
| D1 | `flow_contract_facts` is written only when someone reads a flow endpoint, with a 90-minute catch-up cap, so a quiet stretch leaves permanent holes in the flow history. | Moving the writes into the pipeline is an architectural change with a backfill plan; it deserves its own branch. |
| I3 | SPY/QQQ 1-minute bars from ThetaData hold about one second of volume, so the −1 s bucket shift used for the old feed may misplace volume. | Needs a live ThetaData capture to confirm before changing how bars are bucketed. |
| T3 | Member backtests read after-hours bars, and trades opened late in the day are dropped. | Changes every saved backtest's results; should ship with a before/after comparison on stored runs. |

### Medium

| ID | Finding | Why deferred |
|----|---------|--------------|
| J4 | Catch-up timer runs after an outage: the writer has no pre-open guard, the receipt and scorecard use the clock date, nothing records that a post went out. | Only bites after an outage spanning a scheduled fire; needs a decision on catch-up behavior. |
| J7 | The pin-tolerance calibrator oscillates. | Its inputs were wrong until F1; re-evaluate after a few weeks of correct grades. |
| T4 | Resetting a bot's starting capital without current/peak equity produces fake returns. | Admin-only path; low frequency. |
| T5 | Expired-option intrinsic fallback applies during the expiration-day session. | Needs a pricing-policy decision. |
| T6 | No fleet-wide risk cap; the one-contract minimum ignores cost. | Product decision. |
| T7 | Backtest CAGR/Sharpe use the first-to-last trade span instead of the test window. | Changes reported stats for every saved run; ship with T3. |
| S5 | The playbook grader counts the issuing bar's prices from before the card was issued. | Changes the published hit rates; needs a regrade plan. |
| S6 | The live playbook cycle skips the multi-day gates and MSI snapshots. | Larger change to the cycle. |
| S7 | Signals don't bound the age of the GEX rows they read. | Needs a staleness policy per signal. |
| S8 | The 120-bar close history spans sessions at the open. | Design choice per signal. |
| S9 | `eod_pressure`'s charm scale (2e7) is about 500x below today's values, so its charm term is always saturated. | Re-fit only after a week of post-F5 data, since F5 changes charm. |
| S10 | Vol-expansion readiness scales as 1/S; its `net_gex` normalizer is never written. | Needs a re-specification, not a patch. |
| D4 | `get_component_signals_bulk` scans all rows for its DISTINCT ON. | Query rewrite; measured cost is moderate. |
| D5 | A fact reprocess can't zero a stale delta. | Tied to D1. |
| D6 | Flow-series tail polls bypass the cache. | Moderate load; can wait. |
| D7 | The strike-profile live tip is cached 30 s per worker. | Minor staleness; can wait. |
| D8 | Daily candles are grouped by UTC day, so in winter the 19:00-20:00 ET after-hours slice lands in the next day's candle. | Small and only with extended-hours bars; can wait. |
| D11 | Vol and premium surfaces can use the after-hours leftover chain and a UTC `CURRENT_DATE`. | Plausible, not reproduced. |
| I4 | Volume during an unobserved gap is booked in one minute and one direction. | Needs a policy for gaps. |
| I5 | No watchdog on the option poller; a hung call freezes the chain. | Plausible, not reproduced. |
| I6 | A band change stalls the main loop (sleep in the poller, per-contract commits). | Performance only; can wait. |
| I7 | Strike lists are refetched uncached at every recalibration. | Performance only; can wait. |
| Q3 | The 30-minute time-to-expiry floor also applies to the IV solver and forced-flow legs. | Needs a modeling decision. |
| Q4 | IV is solved from a stale `last` with no age or bid/ask check. | Needs a quote-quality policy. |
| Q5 | SPY/QQQ discrete dividends are not modeled. | Design change. |
| A6 | Admin regenerate and some TradeWorkz admin routes block the event loop. | Admin-only traffic. |

### Low

J8 wall touch graded from one side only · J9 cone skill baseline pooled across
horizons · J10 receipts don't check bar completeness · J11 base-rate `held()`
censoring bias · J12 wall alerts dropped after a detector reset (plausible) ·
T8 leg quotes take the nearest strike at any distance · T9 custom-strategy DTE
uses calendar days and the UTC date · T10/T11 possible double-book or same-tick
re-entry (plausible) · S12 per-cycle load (unbounded VWAP view, 30-minute skew
scan each second) · S13 abstaining basic signals still vote with the regime
tilt · D12 duplicate Action Cards possible (needs a unique constraint) · D13
replay session dates aggregate all of `gex_summary`, uncached · D14 weekday
session bounds ignore holidays · D15 option-contract history freezes at 16:00
for an hour · I8 poller keeps a stale bid · I9 TradeStation force-refresh can
return a rejected token · I10 VIX/VXN rewrite the same row every second · A7 a
revoked key stays cached in the other three workers until its TTL; a DB error
reads as 401 · Q8 extra gamma-flip ladder rungs never change the flip.

---

## Verification

- Full suite (`-m "not integration"`): the same four failures as a clean
  `release` checkout and nothing else (the two listed in `CLAUDE.md`, plus
  `test_ingestion_provider_selection` and `test_tradeworkz_any_symbol`, which
  also fail on `release` on a weekend). 7,556 passed. The three commits made
  after that run (H4, H5 and a docstring) were checked with every test file
  that imports what they changed.
- Integration tests against Postgres 16: bucket-floor parity (gex and
  max-pain reads, with and without the tiers), session levels on six calendar
  cases, and half-day closes and receipt bars.
