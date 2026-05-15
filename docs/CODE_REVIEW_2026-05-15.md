# ZeroGEX Code & Schema Review — 2026-05-15

Scope: full repository at HEAD of `claude/code-database-review-8Nwtu`. Focus on
technical correctness, financial-math correctness, and database schema design.

Findings are graded:

- **🔴 Defect** — code is wrong, will produce incorrect numbers or break in
  production. Fix required.
- **🟡 Design issue** — code works as written, but the design has a tradeoff
  worth surfacing (silent data loss, ambiguous convention, etc.).
- **🟢 Cleanup** — cosmetic / cruft / cost-only; doesn't affect correctness.

Severity ordering within each grade is approximate. Line numbers and file
paths are at HEAD as of 2026-05-15.

---

## 🔴 Defects — Financial Correctness

### F1. Put charm formula is wrong (production trading signal)

**File:** `src/analytics/main_engine.py:362–392` (`_calculate_charm`)

The code computes:

```python
call_charm = -φ(d1) * (2rT - d2·σ·√T) / (2T·σ·√T)
if option_type == "C":
    charm = call_charm
else:  # Put
    charm = call_charm + r * exp(-r*T)   # ← wrong
charm_per_day = charm / 365.0
```

The `r * exp(-r·T)` term added to puts is incorrect. Standard Black-Scholes
charm with no dividend yield (which is the q=0 model used everywhere else in
this codebase — see `_calculate_d1_d2`, which omits q):

```
charm_call = -φ(d1) · (2rT - d2·σ·√T) / (2T·σ·√T)
charm_put  = charm_call          (identical when q=0)
```

This follows from put-call parity: `Δ_put = Δ_call − 1`, so `∂Δ_put/∂t =
∂Δ_call/∂t`. The bug appears to confuse charm with theta (which *does* differ
between calls and puts via `r·K·e^(−rT)` terms).

**Impact:** wrong `charm_exposure` and `put_charm_exposure` for every put in
`gex_by_strike`. These feed into `dealer_charm_exposure` (negated for dealer
convention) and propagate to:

- the `eod_pressure` advanced signal
- the `vanna_charm_flow` basic signal
- the `vanna_charm_glide` playbook pattern
- the time-decay arm of the EOD-pressure scoring

For a near-ATM 0DTE put at the end of the day, the extra `r·e^(-rT) / 365`
term is on the order of `0.05 / 365 ≈ 1.4 × 10⁻⁴` per share. Multiplied by
contract notional `OI × 100 × S`, on SPX this is meaningful: for a single
strike with 50k OI at S=$5500, the per-strike error is
`50_000 × 100 × 5500 × 1.4e-4 ≈ $3.8M`, in the *opposite direction* of what's
intended (the formula systematically biases dealer put charm one way).

**Fix:** remove the `r * exp(-r*T)` term from the put branch. The same
function exists logically (and is correct) when q=0.

---

### F2. Max pain pools all expirations into a single payout

**File:** `src/analytics/main_engine.py:546–605` (`_calculate_max_pain`)

The function iterates `for opt in options` across every expiration in the
snapshot and aggregates `(test_strike − strike) × OI × 100` into a single
`total_payout[test_strike]` per candidate strike. The single "max pain"
strike returned is the strike that minimizes payout summed *across all
expirations*.

This is not how max pain is defined. Max pain is per-expiration: the
strike at which option writers' total payout is minimized *at that
expiration's settlement*. Pooling all expirations conflates:

- $5500 SPX puts expiring tomorrow (very high gamma, near-zero time value),
- $5500 SPX puts expiring in 30 days,
- $5500 SPX puts expiring in 6 months.

These are different contracts with different settlement points. Summing
their intrinsic payouts at a single hypothetical "test_strike" treats them
as if they all settled at the same time, which is wrong.

**Impact:** the persisted `gex_summary.max_pain` is a synthetic blended
number that doesn't correspond to any actual settlement event. Downstream
signals (`EODPressureSignal._pin_target`, the pin-gravity component in
`pin_risk_premium_sell` playbook) anchor on this value as a meaningful
pin level. It isn't.

The API counterpart at `src/api/database.py:_refresh_max_pain_snapshot`
correctly computes max pain per expiration (see `best_per_exp`,
`expiration_payload`), then surfaces only the first-expiration value as
the headline `max_pain` (LIMIT 1, ORDER BY expiration). So there are
actually two different "max pain" definitions running side by side in the
codebase — the API uses front-month, the analytics engine writes a
blended value, and downstream signals read whichever one happens to be
nearest.

**Fix:** compute max pain per expiration. Store the front-month (nearest
non-zero-DTE) value in `gex_summary.max_pain` for backward compatibility,
or — better — change the schema so `gex_summary` references one
`max_pain` per expiration (new table or JSONB column) and migrate
downstream consumers to read the front-month explicitly.

Quick interim fix (front-month only, preserving the column):

```python
front_exp = min((opt["expiration"] for opt in options
                 if opt["expiration"] > timestamp.date()),
                default=None)
if front_exp is None:
    max_pain = None
else:
    front = [o for o in options if o["expiration"] == front_exp]
    max_pain = self._calculate_max_pain(front)
```

---

### F3. Analytics snapshot silently drops contracts past LIMIT 2000

**File:** `src/analytics/main_engine.py:251–280` (`_get_snapshot`)

The DISTINCT-ON query has `LIMIT 2000`. With the `ORDER BY oc.option_symbol,
oc.timestamp DESC` required by DISTINCT ON, the 2000-row cap returns the
first 2000 *option_symbols by lexicographic order*. Any contract whose
option_symbol sorts after the 2000th survivor is silently dropped from GEX,
max pain, vanna/charm exposure, and walls calculations.

For SPX, an active session has ~600–1200 strikes across the front 6
expirations × 2 sides ≈ 7k–14k unique option_symbols. The LIMIT silently
drops the back of that list.

**Impact:** the side that gets dropped depends on the option-symbol format
TradeStation returns (e.g., `SPXW 261219P05500000` vs `SPX 260116C04500000`).
Empirically TradeStation symbols are stable, so the *same* contracts get
dropped on every cycle, which means GEX/max-pain/walls have a fixed,
unmodeled bias — they exclude a particular subset of strikes/expirations.

**Fix:** either

1. Remove `LIMIT 2000` and rely on the lookback window + expiration cutoff
   to bound the result set (typical session has ~700–1200 distinct contracts
   anyway). At minimum, log a warning when the returned row count equals the
   LIMIT (currently `len(options) == 2000` would be a silent indicator).
2. Or, if the LIMIT is intentional load protection, lift it to a value
   far above the maximum realistic chain size (e.g., 50000) and gate on
   that with a hard error if hit.

---

### F4. Day-rollover bug in option volume baseline (silent data loss)

**File:** `src/ingestion/main_engine.py:616–658` (`_get_option_volume_baseline`)

The baseline cache returns the latest pre-bucket `volume` row regardless of
session date. TradeStation cumulative volume *resets to 0* at session start.
On the first bucket of a new session:

- baseline ← cached/persisted value from yesterday's close (e.g., 50,000)
- current bucket's cumulative volume ← today's running total (starts at 0,
  grows during the bucket to e.g. 800)
- delta = `max(current - baseline, 0)` = `max(800 - 50000, 0)` = `0`

The session's opening minute is the single most informative bucket for flow
classification — and it gets silently zeroed.

This is *not* mitigated by the in-DB view `option_chains_with_deltas` —
that view partitions LAG by `DATE(timestamp AT TZ 'America/New_York')`,
which is correct. But the ingestion engine's *own* baseline read (used to
classify ask/mid/bid volume into `flow_contract_facts`) is NOT
session-partitioned. So the canonical flow facts table is wrong at
session open, even though the volume_delta view is right.

**Fix:** add a session-date guard to `_get_option_volume_baseline`. Either:

1. Compare the baseline row's timestamp's session-date against the bucket's
   session-date; if they differ, treat baseline as 0.
2. Or scope the lookup query to `timestamp >= session_open_et(bucket)`.

The cache also needs to invalidate at session boundary — currently a stale
cache entry from yesterday survives until TTL expires.

---

### F5. Greeks computed against stale underlying price

**File:** `src/ingestion/main_engine.py:347–394`

`_enrich_with_greeks()` uses `self.latest_underlying_price`, set in
`_store_underlying()` whenever a new underlying bar lands. There's no
staleness check. Edge cases that produce stale prices:

- Pre-market: underlying bars arrive slowly (sometimes minutes apart)
  while option polling continues at the configured cadence.
- Underlying-bar polling failure: a transient API error on the underlying
  fetch doesn't pause option-Greeks calculation. Options continue to be
  enriched with the last known price.
- Halt: trading halt freezes the underlying bar stream but options may
  still quote.

Greeks (especially delta and gamma) are highly sensitive to S near the
strike. A 5-minute-stale price on a 0DTE 5500-strike SPX call when spot
moved from 5499 → 5501 changes delta by ~0.3 and gamma by orders of
magnitude. These wrong Greeks get persisted to `option_chains.delta`,
`option_chains.gamma` and feed every downstream calculation.

**Fix:** pass the underlying-bar timestamp alongside the price into the
engine, and reject Greeks calculation (or fall back to None) when
`option_timestamp - underlying_timestamp` exceeds a threshold
(e.g., 60–90 seconds). Log a metric so operators can detect the staleness.

---

### F6. Dead schema: 8 tables created then unconditionally dropped, but production code still writes/reads them

**File:** `setup/database/schema.sql:949–1281`

Lines 949–1267 define 8 tables: `trade_signals`, `signal_accuracy`,
`position_optimizer_signals`, `position_optimizer_accuracy`,
`signal_engine_trade_ideas`, `consolidated_trade_signals`,
`consolidated_signal_accuracy`, `consolidated_position_accuracy`. Lines
1274–1281 unconditionally `DROP TABLE … CASCADE` all 8.

If this schema file is ever re-run (which is the entire point of the
`IF NOT EXISTS` / idempotent design), the drops execute and the tables
disappear. Meanwhile, production code at:

- `src/signals/position_optimizer_engine.py:1458` — `INSERT INTO position_optimizer_signals (...)`
- `src/signals/position_optimizer_engine.py:1636` — `SELECT ... FROM position_optimizer_signals`
- `src/api/queries/signals.py:109` — `SELECT … FROM consolidated_trade_signals`
- `src/api/queries/signals.py:962` — second `consolidated_trade_signals` read
- `src/api/queries/signals.py:1069, 1111` — two `signal_engine_trade_ideas` reads
- `src/api/main.py:44, 245` — registers `trade_signals` router

…all reference tables the schema deletes. The router for `trade_signals`
is mounted at app startup; any endpoint that hits a dropped table returns
"relation does not exist" at request time (likely caught and converted to
a 500 by the global error handler, but masked from operators).

**Fix:** decide whether to keep or remove these tables. If they're
deprecated, delete the CREATE blocks and the corresponding code paths in
`position_optimizer_engine.py` and `queries/signals.py`. If they're still
needed, remove the DROP statements and complete the migration to
`signal_scores`/`signal_trades` properly.

The "unified signal engine (v2)" comment at schema.sql:1269 suggests the
intent was to migrate, but the migration is half-finished: new code uses
the v2 tables, old code still writes to v1.

---

## 🔴 Defects — Code Correctness

### F7. Flow endpoints catch all exceptions and return `[]`

**File:** `src/api/database.py` — multiple locations
(1599–1604, 1884–1887, 1955–1958, 2047–2050, 2132–2135 and similar)

```python
except asyncio.TimeoutError:
    logger.warning(f"Flow query timed out for {symbol}, returning empty")
    return []
except Exception as e:
    logger.warning(f"Flow query failed for {symbol} (returning empty): {e!r}")
    return []
```

Any database error — auth failure, connection drop, query syntax error
introduced by a future change, planner regression — surfaces as an empty
list to the client. Clients see "200 OK, no flow data" and cannot
distinguish "no contracts traded this minute" from "database is down".

**Impact:** silent dashboards. Frontend traders watching for flow during
a regression will see empty bars and assume the market is dead, while the
backend is broken. Monitoring on flow queries needs to alert on log
warnings, not on HTTP status — which is a fragile alerting contract.

**Fix:** narrow the `except` to the specific transient errors that warrant
empty-fallback (`asyncio.TimeoutError`, `asyncpg.PostgresConnectionError`).
Other exceptions should raise and be handled by the framework's 5xx
handler, which is visible to monitoring.

---

### F8. Schema view `dealer_hedging_pressure` mixes book-side and dealer-side conventions

**File:** `setup/database/schema.sql:844–889`

```sql
SUM(delta * open_interest * 100) AS expected_hedge_shares
…
WHEN COALESCE(d.expected_hedge_shares, 0) > 1000000 THEN '🔴 Heavy Sell-Hedging Risk'
WHEN COALESCE(d.expected_hedge_shares, 0) < -1000000 THEN '🟢 Heavy Buy-Hedging Risk'
```

The math sums `delta × OI × 100` across all contracts. This is the
*market-aggregate* delta (the holders' book), not the dealer's hedge.
If we assume dealers are short the retail book (the standard convention
this codebase uses elsewhere — see `dealer_charm_exposure = -charm_exposure`),
then dealers' delta is the *negative* of the sum, and dealers need to hold
`+expected_hedge_shares` shares to be hedged.

The labels are then ambiguous:

- "Sell-Hedging Risk" when `expected_hedge_shares > 1M` reads as "dealers
  will sell to hedge", but if dealers already hold the long-share hedge,
  this is their current position, not a transaction.
- The risk of forced selling actually depends on `∂(expected_hedge_shares)/∂S`
  (i.e., aggregate gamma), not on `expected_hedge_shares` itself.

Either the math is right and the labels are wrong, or vice versa. The
view is consumed by `make ...` shortcut queries — limited blast radius —
but the same sign convention reappears in production code: the
`dealer_delta_pressure` signal component in `src/signals/basic/`.

**Fix:** make the convention explicit in the view name and labels:
either `customer_aggregate_delta_shares` with sentiment labels, or
`dealer_required_hedge_shares` with "needs to BUY to hedge" / "needs to
SELL to hedge" labels. Don't mix them.

---

### F9. `gamma_exposure_levels` view uses the per-share gamma convention, missing the `S² × 0.01` normalization

**File:** `setup/database/schema.sql:891–937`

```sql
SUM(CASE WHEN option_type = 'C' THEN gamma * open_interest * 100
         ELSE -gamma * open_interest * 100 END) AS net_gex
```

Compare to the canonical formula used everywhere else in the codebase
(`src/analytics/main_engine.py:439`, `walls.py:22`):

```
γ × OI × 100 × S² × 0.01
```

The view drops the `× S² × 0.01` factor that converts share-equivalent
hedge exposure into dollar gamma per 1% move. The numbers from this view
are ~`S²/100`× off from numbers persisted in `gex_by_strike.net_gex`.

For SPY at $450, that's `450² / 100 ≈ 2,025` × off. For SPX at $5500,
that's `5500² / 100 ≈ 302,500` × off.

The hardcoded threshold `> 1000000` / `< -1000000` in the GEX-level label
CASE then means very different things on SPX vs SPY — and *neither* of
those thresholds is calibrated to this view's specific (un-normalized)
units. The labels are essentially decorative noise.

**Fix:** either delete this view (it appears unused by production code —
`grep gamma_exposure_levels src` is empty), or rewrite it to use the
canonical `γ × OI × 100 × S² × 0.01` formula and per-symbol thresholds.

---

### F10. `_classify_volume_chunk` falls back silently on locked/crossed quotes

**File:** `src/ingestion/main_engine.py:574–585`

```python
if bid is None or ask is None or ask <= bid:
    dist_to_ask = abs(last - ask) if ask is not None else float("inf")
    dist_to_mid = abs(last - effective_mid)
    dist_to_bid = abs(last - bid) if bid is not None else float("inf")
    min_dist = min(dist_to_ask, dist_to_mid, dist_to_bid)
    …
```

When `ask <= bid` (crossed or locked market — common during quotes
flicker or wide spreads on the open), the code falls into nearest-neighbor
classification with no logging. That's reasonable behavior on its face,
but:

- A persistently crossed contract (data feed glitch, halted contract)
  will route 100% of its volume through this fallback, biasing flow
  classification systematically.
- There's no metric or warning to surface that this is happening, so
  operators can't see when a contract's flow is being scored by a
  degraded code path.

This pairs badly with F4: at session open, both bugs amplify each other.

**Fix:** add a counter / structured log warning when `ask <= bid` (and
the count over a session — easy with a class-level counter, dumped on
flush). For very persistent crossed quotes (say >10 consecutive ticks),
escalate to `error` level and route the volume to `mid_volume` instead
of nearest-neighbor.

---

## 🟡 Design issues

### D1. `max_gamma_strike` picks a single (strike, expiration) pair, not the strike with maximum aggregate gamma

**File:** `src/analytics/main_engine.py:669`

```python
max_gamma_strike = max(gex_by_strike, key=lambda x: abs(x["net_gex"]))
```

`gex_by_strike` is one row per `(strike, expiration)`. Picking the max
over those rows finds the single (strike, expiration) pair with the
largest absolute net GEX — but the persisted `max_gamma_strike` column
only stores the strike (the expiration is discarded). A strike that's
moderate on every expiration but huge in aggregate (the typical case)
will be passed over in favor of an extreme single-expiration outlier.

Industry convention (SpotGamma, SqueezeMetrics) is the strike with
largest aggregate gamma exposure across all expirations.

**Fix:** aggregate `net_gex` by strike across expirations, then take the
max. Same pattern as the gamma flip calculation, which *does* aggregate
(`_calculate_gamma_flip_point` lines 622–629).

```python
agg = defaultdict(float)
for r in gex_by_strike:
    agg[r["strike"]] += r["net_gex"]
max_strike, max_val = max(agg.items(), key=lambda kv: abs(kv[1]))
```

### D2. Time-to-expiration uses 16:00 ET regardless of expiration style

**File:** `src/market_calendar.py:84–106`

All expirations are anchored at 16:00 ET. This is correct for PM-settled
options (SPXW, SPY) but wrong for AM-settled monthly SPX expirations
(third Friday) which settle at the *open* via the SOQ. A 0DTE AM-settled
SPX option has zero time value at 09:30 ET, not 16:00 ET — using 16:00
adds ~6.5 hours of phantom time value, inflating delta toward 0.5 and
gamma toward the centerline at expiration.

**Impact:** for monthly SPX expirations on the third Friday morning,
Greeks are systematically wrong for ~6.5 hours. After SOQ the contract
doesn't trade so the wrong Greeks aren't propagated further, but max
pain and GEX calculations on that morning include phantom time value.

The schema doesn't carry an AM/PM-settlement flag per expiration, so
fixing this requires either:

1. A settlement-style table or column populated from TradeStation
   metadata (or hardcoded for SPX 3rd-Friday rule).
2. Or simply ignore AM-settled expirations on settlement day (a one-line
   filter in the snapshot query that drops them after 09:30 ET).

### D3. Hardcoded thresholds aren't symbol-aware

Several places hardcode thresholds calibrated for one underlying:

- `dealer_hedging_pressure` view: `±1,000,000` for hedge-shares labels.
  On SPX (~$5500 spot, ~$1M notional/contract), an OI of 1000 contracts
  saturates this label; on SPY (~$500 spot, ~$50K notional/contract),
  it takes 20× more OI.
- `gamma_exposure_levels` view: `±1,000,000` for GEX labels.
- `flow_smart_money` refresh (analytics_main_engine:1196–1198): premium
  cutoffs of $50K / $100K / $250K / $500K. Same dollar cutoff classifies
  a "large" SPX trade as "small" SPY trade.
- `put_call_ratio_state.py:15`: `(pcr - 1.0) / 0.4` saturation. PCR
  distributions look different on SPX vs SPY.

**Fix:** make these per-symbol via `component_normalizer_cache` (which
already exists for some fields) or a static config table. Easiest
first step: add a per-symbol `notional_scale = S × 100` and divide
shares-based thresholds by it.

### D4. Schema accretion: 70+ idempotent migrations inline

**File:** `setup/database/schema.sql` (1794 lines)

The schema runs CREATE → ALTER (idempotent column adds) → DROP →
RECREATE for many tables. This makes the schema history hard to read,
and any future migration has to reason about all the conditional ALTERs
that may or may not have run. Concrete examples:

- `option_chains`: 9 columns added via idempotent DO-blocks (lines 85–150)
  that should be in the CREATE TABLE for fresh installs.
- `gex_by_strike`: 7 columns added in two batches via DO-blocks.
- The 8 dropped legacy signal tables (F6).
- The `signal_component_scores.raw_score → clamped_score` rename
  migration (lines 1506–1520) duplicating the column definition.

**Fix:** consolidate the schema file. Move idempotent column adds into
the CREATE TABLE statement (it's already wrapped in `IF NOT EXISTS`,
so fresh installs are unaffected, and existing installs are migrated
once). Move the historical migration steps to a separate
`migrations/` directory with monotonically-numbered files. Keep
`schema.sql` as the *target* state.

### D5. `option_chains_with_deltas` exists but `signed_volume / buy_volume / sell_volume` from `flow_contract_facts` are duplicated logic

`flow_contract_facts` persists `volume_delta`, `signed_volume`,
`buy_volume`, `sell_volume`, etc. The `option_chains_with_deltas` view
recomputes `volume_delta` via window functions. If a future bug fixes
one and not the other (or the ingestion engine's classification
disagrees with the view's LAG-based recomputation), downstream
queries that mix the two get inconsistent flow numbers.

**Fix:** pick one canonical source of `volume_delta` — almost certainly
`flow_contract_facts` (since it carries the buy/sell classification too)
— and either delete the view or rewrite it as a thin shim around
`flow_contract_facts`.

### D6. `flow_smart_money` "unusual activity score" lumps premium and IV with hardcoded thresholds

**File:** `src/analytics/main_engine.py:1196–1199`

```sql
CASE WHEN volume_delta >= 500 THEN 4
     WHEN volume_delta >= 200 THEN 3
     WHEN volume_delta >= 100 THEN 2
     WHEN volume_delta >= 50  THEN 1 ELSE 0 END
+ CASE WHEN volume_delta * last * 100 >= 500000 THEN 4 …
+ CASE WHEN implied_volatility > 1.0 THEN 2 WHEN implied_volatility > 0.6 THEN 1 ELSE 0 END
```

The score conflates contract count, premium notional, and IV — adding
them is unitless arithmetic. For SPX vs SPY, the volume thresholds mean
very different things (SPX 500 contracts = ~$275M notional; SPY 500
contracts = ~$2.25M). IV thresholds of 60% / 100% saturate immediately
on weekly SPY options, and rarely fire on 90DTE SPX puts during a
normal regime.

**Fix:** ladder the score per-symbol. The current SQL is hard to test
or tune. If this signal is consumed by production trading code, port
it into Python with explicit per-symbol calibration so it can be
unit-tested.

### D7. `_refresh_flow_caches` runs in the analytics engine but writes to API-layer caches

**File:** `src/analytics/main_engine.py:1023–1245`

The analytics engine refreshes `flow_by_contract` and `flow_smart_money`
tables — these are read by the API layer (`src/api/database.py`). The
analytics engine doesn't know what the API needs cached; the API doesn't
know when the analytics engine has finished a cycle. This produces:

- Cache hits with stale data during slow analytics cycles.
- Wasted refresh work when the API isn't being polled.
- A circular dependency: the analytics engine's `flow_cache_refresh_min_seconds`
  controls how stale the API's data can be, but the analytics engine
  doesn't know about API-layer hot symbols.

**Fix:** move cache refresh into the API layer itself (with proper
per-symbol scheduling). Or, if it must stay in analytics, document
explicitly which symbols and which TTL — and remove
`_do_refresh_flow_cache` from `src/api/database.py:_refresh_max_pain_snapshot`
(which appears to do its own LATERAL backfill).

### D8. Underlying-volume "buying pressure" uses up_volume / down_volume from TradeStation, which is exchange-best-effort

**File:** `setup/database/schema.sql:683-706` (`underlying_buying_pressure`)
and various basic signals.

TradeStation's bar stream provides up_volume / down_volume (Lee-Ready-like
tick-test classification on consolidated NBBO). These are NOT trade-side
attribution: a 1000-share print between exchanges with NBBO movements
can land on either side depending on the order of bookkeeping events.
On a fast-moving open, up/down volume is materially noisier than the
exchange-side reported volume.

Signals that read `(up_volume - down_volume)` as "real-time directional
flow" are reading classified tape, not flow. The label "buying pressure"
implies more than what's actually measured.

This isn't a defect per se — the README says "Up/Down volume breakdown"
honestly — but the downstream signal naming (`tape_flow_bias`,
`order_flow_imbalance`) and the dashboard label "🟢 Strong Buying" /
"❌ Selling" overstate the precision of what's measured.

**Fix:** rename the view to `underlying_uptick_volume_ratio` and label
the dashboard outputs as "uptick-biased / downtick-biased" rather than
"buying / selling". (Or, if budget allows, switch to actual trade-side
data from a feed that provides it.)

---

## 🟢 Cleanup / cost

### C1. Connection pattern is verbose and error-prone

**File:** `src/analytics/main_engine.py` — `_store_gex_by_strike`,
`_store_gex_summary`, `_store_calculation_results`

The conn/cursor passing pattern (lines 742–760, 848–867) takes 18 lines
of branching to either reuse a passed-in connection or open a new one.
This duplicated in two places, with a bug-prone constraint that "conn
and cursor must be provided together". A simpler pattern: always pass
both, and have callers use the `db_connection()` context manager.

### C2. `make` is the build-and-ops surface; some targets have hidden coupling

`Makefile` is 143k. Many of the SQL queries embedded inside `make`
targets reproduce logic that exists in `src/api/queries/`. If the API
query is updated, the Makefile target silently diverges. (E.g.,
`make gex-summary` runs a hand-rolled SQL against `gex_summary` —
if the table schema changes, the Makefile fails at runtime.)

**Fix:** make the Makefile targets call into the API client or a small
read-only CLI in `src/tools/` so the SQL is in one place.

### C3. The hot index for `_get_snapshot` is documented as not being used by the planner

**File:** `setup/database/schema.sql:172–219`

The comment block explains in detail that
`idx_option_chains_underlying_option_symbol_ts_gamma_covering` was
built to fix the May 13, 2026 wedge but the planner never picks it for
that query. It remains in the index list because it serves
LATERAL-style per-contract lookups. This is fine, but the comment
should be hoisted into a runbook so future operators don't waste time
investigating whether to drop it.

### C4. `IV_MIN=0.01`, `IV_MAX=5.0` clamp without telemetry

**File:** `src/ingestion/iv_calculator.py:212`

When Newton-Raphson hits the IV ceiling/floor, the value is clamped and
silently persisted. Iron-condor strikes near zero gamma frequently
hit the floor. There's no counter or warning, so operators don't see
how often this happens or whether the bounds need adjustment.

### C5. Dead defensive code in `enrich_option_data`

**File:** `src/ingestion/greeks_calculator.py:289–397`

The function has ~10 defensive `if foo is None` branches that each
return option_data with None Greeks. Most of these can't actually be
reached given the upstream call site, and the rest could be one
exception handler. The function is 109 lines of which ~60 are
defensive scaffolding.

This isn't a bug — but it makes the actual computation hard to find
and review.

---

## What's solid

For balance, these areas reviewed clean (no defects found):

- **Greeks formulas** (delta, gamma, theta, vega) in
  `src/ingestion/greeks_calculator.py` — correct Black-Scholes math
  for q=0, sign conventions match standards. Theta-per-day and
  vega-per-1pp scalings are the standard "trader's Greeks" convention.
- **IV Newton-Raphson solver** — correct convergence with intrinsic-value
  guard, NaN propagation guard, step-size clamp (line 197-205).
- **Call/Put wall calculation** (`src/analytics/walls.py`) — correct
  industry-standard definition (gamma exposure-ranked above/below spot
  with tiebreaker), and the SQL counterpart matches the Python helper.
- **SQL injection surface** — the dynamic ORDER BY / bucket helpers
  in `src/api/queries/_sql_helpers.py` use closed allowlists; user
  inputs are parameterized.
- **Connection pool** — `src/database/connection.py` and the API's
  `_acquire_connection` consistently use context managers.
- **Timezone handling** — DATE() casts in views all use
  `AT TIME ZONE 'America/New_York'`; the `market_calendar` module
  centralizes the ET timezone constant correctly.
- **Foreign keys + cascade** on symbol-keyed tables — schema is
  consistent about referential integrity.

---

## Priority ranking for fixes

If I were sizing the work, I'd order it:

1. **F1** (put charm) — single-line code change, propagates everywhere.
2. **F6** (dead schema vs live code) — production code path broken on
   any schema re-run.
3. **F3** (LIMIT 2000 silent truncation) — single line, eliminates a
   systematic GEX bias.
4. **F4** (day-rollover volume baseline) — needs a session-date guard,
   maybe 20 LoC.
5. **F2** (max pain across expirations) — design call: front-month
   only, or per-expiration schema. Schema migration.
6. **F5** (stale underlying in Greeks) — needs a staleness check and a
   metric, ~30 LoC.
7. **F7** (silent except in API) — narrow the except clauses, audit
   monitoring.
8. **D1** (max_gamma_strike aggregation) — one-line fix, no schema impact.
9. The rest as scheduled.

Items in 🟡 / 🟢 are cleanup / design work, not urgent.

---

*Generated 2026-05-15 against `claude/code-database-review-8Nwtu` HEAD.*

---

## Fixes applied on this branch

The following items have been fixed in the same commit series as this review.
Items not listed here remain open and need separate triage.

### F1 — Put charm formula

`src/analytics/main_engine.py:_calculate_charm` — removed the spurious
`r * exp(-r * T)` term added to puts.  Charm now matches call charm
(correct at q=0).  `option_type` parameter retained for caller
compatibility; it's no longer branched on.

### F2 — Max pain per expiration

`src/analytics/main_engine.py`:
- Existing `_calculate_max_pain` is now documented as
  single-expiration-only; callers must pre-filter.
- Added `_calculate_max_pain_by_expiration` which returns
  ``{expiration → strike}``.
- `_calculate_gex_summary` now picks the front-month (nearest
  non-expired) value for the scalar ``max_pain`` field and persists the
  full per-expiration dict.

`setup/database/schema.sql` — added idempotent
``gex_summary.max_pain_by_expiration`` JSONB column.

### F3 — Snapshot row cap

`src/analytics/main_engine.py:_get_snapshot` — replaced hardcoded
``LIMIT 2000`` with ``ANALYTICS_SNAPSHOT_MAX_ROWS`` env var (default
50000) and a warning log when the cap is hit.

### F4 — Day-rollover volume baseline

`src/ingestion/main_engine.py:_get_option_volume_baseline`:
- Cache key is now ``(option_symbol, session_date_ET)``.
- DB lookup is scoped to the bucket's ET session, so a prior session's
  closing volume can never be returned as today's baseline.
- `_invalidate_option_volume_baseline` walks all date-variant entries
  for an option_symbol.

### F5 — Stale-underlying guard for Greeks

`src/ingestion/main_engine.py`:
- New `latest_underlying_timestamp` paired with the cached price.
- `_enrich_with_greeks` rejects Greeks calculation when the underlying
  price is older than ``GREEKS_MAX_UNDERLYING_AGE_SECONDS`` (default 90s).
- Reject counter ``greeks_stale_underlying_rejects`` is incremented and
  logged every 100 events so operators can see rate.

### F6 — Legacy v1 schema cleanup

`setup/database/schema.sql` — removed the 330 lines of dead CREATE
blocks for ``trade_signals``, ``signal_accuracy``,
``position_optimizer_signals``, ``position_optimizer_accuracy``,
``signal_engine_trade_ideas``, ``consolidated_trade_signals``,
``consolidated_signal_accuracy``, ``consolidated_position_accuracy``.
DROP statements retained so existing deployments are cleaned up on next
schema run.

`src/signals/position_optimizer_engine.py` — removed `_store_signal`,
`_update_accuracy`, `_snapshot_accuracy`, `_proxy_realized_return`,
`_extract_strikes`, `run_calculation`, `main()`, and the
`PositionOptimizerAccuracySnapshot` dataclass.  Module is now
library-only; `portfolio_engine.py` still uses `_generate_candidates`
which remains.

`src/api/queries/signals.py` — removed `get_trade_signal`,
`get_signal_accuracy`, `get_position_optimizer_signal`,
`get_position_optimizer_accuracy`, the legacy `get_signal_history`
(which was already shadowed by the v2 definition further down the file),
and `get_current_signal_with_trades`.  None of these had external
callers.  The router file ``api/routers/trade_signals.py`` was kept —
inspection showed it already reads from the v2 tables
(``signal_trades``, ``signal_scores``).

### D1 — `max_gamma_strike` aggregation

`src/analytics/main_engine.py:_calculate_gex_summary` — now aggregates
``net_gex`` by strike across expirations before finding the maximum,
matching industry convention.

### Verification

- All 787 unit tests pass (skipped: 2 unchanged).
- Test files exercising the changed paths
  (`test_main_engine_quant_calcs.py`,
  `test_analytics_snapshot_cold_start_lookback.py`,
  `test_analytics_flow_refresh_toggle.py`) pass without modification.
- Import smoke tests succeed for all modified modules.

### Open items (not fixed in this pass)

- F7 — silent except in flow endpoints
- F8 — `dealer_hedging_pressure` view sign-convention labels
- F9 — `gamma_exposure_levels` missing `S² × 0.01` factor
- F10 — `_classify_volume_chunk` silent crossed-quote fallback
- All 🟡 design items (D2–D8)
- All 🟢 cleanup items (C1–C5)

---

## Second fix pass (2026-05-15, follow-up commit)

Everything above's "open items" addressed except C1/C2 (see note).

### F7 — narrowed flow-endpoint excepts

`src/api/database.py` — removed the five
``except Exception: return []`` blocks in `get_flow`,
`get_flow_series`, `get_flow_contracts`, `get_smart_money_flow`,
`get_flow_buying_pressure`.  Only `asyncio.TimeoutError` still maps
to the empty fallback; any other DB error now propagates to the
framework's 5xx handler so monitoring sees it.  Exposed a latent
test-setup bug in `test_api_flow_series.py` (instance-level mock
wiped by the lifespan's `db_manager = DatabaseManager()`); fixed by
class-level patching in `_attach_by_contract_mock`.

### F8 — `dealer_hedging_pressure` honest labels

`setup/database/schema.sql` — documented that
`expected_hedge_shares` is the dealer's static hedge position
(positive = dealer long shares), added a per-symbol
`notional_scale` CTE so the label threshold scales across SPX/SPY,
and relabeled to "Dealer Long/Short/Balanced Hedge" (the old
"Sell-Hedging Risk" conflated position level with hedge flow,
which is a gamma property not a delta one).

### F9 — `gamma_exposure_levels` canonical formula

`setup/database/schema.sql` — rewritten to use the canonical
`γ × OI × 100 × S² × 0.01` formula (joining `latest_spot`),
consistent with `gex_by_strike.net_gex` and `walls.py`.  Threshold
is now spot-derived instead of a hardcoded ±$1M.

### F10 — crossed-quote telemetry

`src/ingestion/main_engine.py` — `_classify_volume_chunk` now
increments `_classify_fallback_count` and warns every 1000th
fallback (getattr-guarded so `__new__`-built test fixtures don't
AttributeError).

### D2 — AM-settled SPX expirations

`src/market_calendar.py` — added `is_spx_am_settled_expiration`
(3rd-Friday rule) and `expiration_close_time_et`.  The analytics
engine's `_calculate_time_to_expiration` now anchors at 09:30 ET
for AM-settled SPX monthlies, and `_get_snapshot` drops AM-settled
SPX rows once their 09:30 SOQ has passed (SPXW weeklies on the
same `$SPX.X` underlying are excluded from the filter via the
`SPXW` symbol prefix).

### D3 — symbol-aware PCR saturation

`src/signals/components/put_call_ratio_state.py` — the hardcoded
`/ 0.4` saturation is now resolved per-symbol from
`ctx.extra["normalizers"]`, a `PCR_SATURATION_<SYMBOL>` env var,
`PCR_SATURATION_DEFAULT`, or the legacy 0.4 default in that order.

### D4 — schema migration consolidation

`setup/database/schema.sql` — collapsed the 9 `option_chains`,
6 `gex_summary`, and 7 `gex_by_strike` per-column `DO $$ … IF NOT
EXISTS (SELECT information_schema) …` blocks into single
`ALTER TABLE … ADD COLUMN IF NOT EXISTS` statements.  ~150 lines
removed; behavior identical for fresh installs and existing
deployments.

### D5 — canonical volume_delta source

`setup/database/schema.sql` — `option_chains_with_deltas` carries
a header comment marking it backward-compat-only and pointing
flow consumers at the canonical `flow_contract_facts`.

### D6 — symbol-aware smart-money score

`src/analytics/main_engine.py` — `flow_smart_money` premium tiers
now scale with `notional_per_contract = spot * 100`; volume tiers
and premium multiples are env-tunable
(`SMART_MONEY_{VOL_T1..4,PREM_T1..4_NOTIONAL_X}`).  Calibration
notes in `docs/runbooks/smart_money_calibration.md`.

### D7 — flow-cache ownership documented

`src/api/database.py` — `_refresh_flow_cache` docstring now states
the analytics engine is the steady-state writer and the API path
is an idempotent per-request backstop, with the
`ANALYTICS_FLOW_CACHE_REFRESH_ENABLED=false` lever called out.

### D8 — honest tape-bias labels

`setup/database/schema.sql` — `underlying_buying_pressure`
relabeled to uptick/downtick-bias terminology with canonical
column names (`uptick_minus_downtick_vol`, `uptick_vol_pct`,
`tick_bias`).  `vol` / `buy_pct` / `momentum` retained as
backward-compat aliases.

### C3 — index runbook

`setup/database/schema.sql` — the 36-line incident narrative on
`idx_option_chains_underlying_option_symbol_ts_gamma_covering`
condensed to 8 lines; full history moved to
`docs/runbooks/option_chains_indexing.md`.

### C4 — IV clamp telemetry

`src/ingestion/iv_calculator.py` — counts floor/ceiling clamp
hits (`_iv_clamp_floor_hits` / `_iv_clamp_ceiling_hits`) and warns
every 1000th so operators can tell when IV_MIN/IV_MAX are
miscalibrated.

### C5 — `enrich_option_data` simplified

`src/ingestion/greeks_calculator.py` — removed the dead
None/non-dict defensive branches (the IV calculator and
`calculate_all_greeks` never return those); 109 lines → ~55, single
consolidated None-Greeks path.

### Not done: C1, C2

- **C1** (verbose conn/cursor pattern in the analytics store
  methods) — left as-is.  The branching is intentional for the
  single-transaction `_store_calculation_results` grouping;
  refactoring risks changing commit semantics for no correctness
  gain.
- **C2** (Makefile ↔ API query duplication) — out of scope for an
  in-place edit pass; it's a structural refactor (move embedded
  SQL into a read-only CLI) better done as its own change.

### Verification (second pass)

- 789 unit tests pass (2 new vs. first pass — the previously
  silent flow-error path is now exercised).
- AM-settled date logic unit-checked against May 2026 (3rd
  Friday = May 15); caught and fixed a `str.rstrip(".X")`
  character-set bug that turned `"SPX"` into `"SP"`.
- Import smoke tests succeed for all modified modules.

---

## Third pass — independent re-review (2026-05-15, branch `claude/code-database-review-4kGEl`)

Fresh full-repo pass. The core financial math (Black-Scholes Greeks, IV
Newton-Raphson, GEX `γ·OI·100·S²·0.01`, vanna `-φ(d1)·d2/σ`, charm,
per-expiration max-pain, gamma-flip aggregation, walls) was independently
re-derived and confirmed **correct** as left by passes 1–2. The findings
below are NEW (not covered by F1–F10 / D1–D8 / C1–C5).

### Fixed this pass

**N1 🔴 `get_signal_hit_rate` multiplied a numeric by the `direction`
TEXT column** — `src/api/queries/signals.py`. `signal_events.direction`
is `VARCHAR(16)` ('bullish'/'bearish'/...). `... / close_at_emit *
direction` made Postgres cast `'bullish'`→`double precision` and throw
`invalid input syntax`. The whole query was caught by the `except` and
returned `None`, so the signal hit-rate endpoint was **permanently
dead** whenever any resolved event existed. Replaced with
`CASE direction WHEN 'bullish' THEN 1 WHEN 'bearish' THEN -1 ELSE 0 END`.
Also fixed the hit-rate denominator (was `wins / resolved`, diluting the
rate with any non-win/non-loss outcome label → now `wins / (wins+losses)`).

**N2 🔴 Trading-day-boundary contamination in four SQL windows** — same
class as the prior timezone fixes but in spots they missed. Each blends
the prior session / overnight gap into the first ~30 bars of a session,
producing **false signals every single market open**:
- `schema.sql` `unusual_volume_spikes` — 30-bar `AVG`/`STDDEV` had no
  ET-day partition (the sibling `underlying_vwap_deviation` one line
  above does). Fired "🚨 Extreme Spike" on routine opens daily.
- `schema.sql` `dealer_hedging_pressure` — `LAG(close)` for
  `price_change` had no day partition → overnight gap reported as a
  per-minute change at the open.
- `schema.sql` `underlying_vwap_deviation` — a zero-volume bar makes
  VWAP `NULL`; all `CASE` comparisons go `NULL` and fell through to a
  spurious "❌ Below VWAP". Added a leading "⚪ No Volume" branch.
- `api/queries/technicals.py` — `avg_volume`, `volume_stddev`,
  `price_change_5min` weren't day-partitioned (sibling `cum_pv`/`cum_vol`
  in the same CTE are) → false volume-spike / price-divergence labels at
  the open.

**N3 🔴 `regime_tilt` last-resort fabricated a price-quantized
direction** — `src/signals/components/spectrum.py`. With no directional
cues it returned a *signed* tilt from `close % 7.0` — SPX at 5103.5 →
strongly bearish, 5104.5 → less bearish — feeding spurious, price-level-
dependent votes into the MSI composite via `ensure_non_zero`. Replaced
with a fixed, sign-neutral `_LAST_RESORT_MIN` floor (still satisfies the
"never exactly 0" contract without inventing a market read).

**N4 🟡→fixed `get_signal_confluence_matrix` self-join double-counted**
— `src/api/queries/signals.py`. `comp a JOIN comp b USING(timestamp)`
had no `a.component_name < b.component_name`, so it emitted the c1=c2
diagonal (a component always agrees with itself → fake 100% confluence)
and both (X,Y) and (Y,X). Added the predicate; the Python consumer now
mirrors each unordered pair into both off-diagonal cells and leaves the
diagonal as the honest empty cell.

**N5 🔴 `_merge_bar` fabricated `now()` for an unparseable bar
timestamp** — `src/ingestion/stream_manager.py`. A bar whose `TimeStamp`
failed `safe_datetime` was stamped with wall-clock `now()`, bucketed
into the *current* minute, and the unconditional underlying-OHLC upsert
overwrote that minute's real bar — corrupting the spot price Greeks are
computed against. Now the malformed bar is dropped with a warning.

Verification: full suite **869 passed, 1 skipped**; `black` clean; no
new `flake8` findings vs HEAD on the five touched files.

### N6 🔴 Fixed — Ingestion lost classified flow on DB write failure / backoff skip

**File:** `src/ingestion/main_engine.py` (`_write_option_rows`)

Traced in full. The per-bucket delta math in `_prepare_option_agg` is
correct, but it clears/seeds the buffer and advances the volume baseline
*before* the write is attempted. On a DB write failure (or a
circuit-breaker backoff skip — which previously dropped `rows`
outright), the computed aggregates were lost permanently: the only
"recovery" was invalidating the baseline cache, but after a bucket
rollover the buffer holds a `_SEED_FLAG` snapshot whose path never
reconsults the baseline, and the genuine-first-obs path (the only one
that does) is unreachable post-rollover. Net: a transient DB blip at a
minute boundary silently zeroed that minute's `ask/mid/bid_volume`.

**Fix (surgical, write-path-only — the delta/seed/baseline math is
untouched):** `_write_option_rows` now retains the exact agg dicts it
failed to persist (or skipped during backoff) in a bounded buffer and
prepends them to the next attempt. This is *exact*: a rolled-back /
skipped transaction commits nothing, and the upsert sums flow fields
additively, so `_coalesce_option_rows` + the additive upsert apply each
agg's volume exactly once when it finally commits. The buffer is bounded
(`OPTION_FAILED_ROWS_RETAIN_MAX`, default 20000); on overflow it drops
the oldest with an ERROR log — strictly better than the prior silent,
unbounded loss. New suite `tests/test_ingestion_failed_write_retention.py`
(6 tests) pins: loss repaired, no double-count across repeated retries,
happy path neither retains nor re-writes, backoff-skip retained not
dropped, bounded-drops-oldest, and an end-to-end rollover-residual repro
driving the real `_prepare_option_agg`. Full suite: 875 passed, 1
skipped; `black` clean; no new `flake8` vs HEAD.

### N7 🔴 Fixed — Underlying OHLC upsert regressed intra-minute high/low

**File:** `src/ingestion/main_engine.py` (`_upsert_underlying_quote`)

Investigated the feed path before touching it. `_merge_bar`
(`stream_manager.py`) carries forward **volume only** — it does *not*
maintain a running max-high / min-low. The stream re-sends the
in-progress minute bar repeatedly; on a reconnect or out-of-order
delivery a later partial can report a High below / Low above an earlier
partial of the same minute. The `ON CONFLICT … high = EXCLUDED.high`
unconditional overwrite then regressed the stored extreme — directly
wrong for range/volatility/strike-selection logic that reads these bars.

**Fix (conflict-clause only, single writer, NULL-safe):**
`open = COALESCE(underlying_quotes.open, EXCLUDED.open)` (first-seen),
`high = GREATEST(…)`, `low = LEAST(…)`; `close = EXCLUDED.close`
(last-tick-wins, already correct) and volumes unchanged. Safe even if
TradeStation sends an authoritative final bar — its H/L bound all
partials, so `GREATEST/LEAST` returns the final values anyway. The
INSERT column/param contract is unchanged. Pinned by
`tests/test_idempotent_writes_and_ohlc_merge.py`.

### N8 🔴 Fixed — Duplicate signal_events / signal_action_cards on restart

**Files:** `src/signals/unified_signal_engine.py`,
`src/signals/playbook/cycle.py`, `src/api/queries/signals.py`

`signal_events` and `signal_action_cards` have no UNIQUE on their
logical key; the in-memory hysteresis only dedups within one process.
A restart / overlapping cycle re-emitting the same
`(underlying, signal_name, timestamp)` / `(underlying, pattern,
timestamp)` double-inserts — double-counting realized hit-rate and
double-firing downstream. Rather than add a UNIQUE constraint (a
deployment-time risk if duplicates already exist), all three writers
now use an idempotent `INSERT … SELECT … WHERE NOT EXISTS` guard on the
logical key — **no schema change, no migration/dedup risk**, only an
exact-duplicate is suppressed. asyncpg path reuses positional params
($1/$3/$2) so its arg count is unchanged. Pinned by
`tests/test_idempotent_writes_and_ohlc_merge.py` (sync, async, and the
real `_persist_advanced_signals` path); the existing
`test_playbook_cycle_integration.py` still passes (param contract
preserved). Full suite: **880 passed, 1 skipped**; `black` clean; no
new `flake8` vs HEAD.

### Identified, intentionally NOT auto-fixed (need a decision / larger change)
- **🟡 `option_chains.volume` stores cumulative daily volume**, while
  `ask/mid/bid_volume` are per-bucket deltas. Any consumer reconciling
  them is wrong. Decide: dedicated period-volume column vs. loud doc.
- **🟡 `playbook/backtest.py` resolves on bar *close* only** (no
  OHLC) and lets target win same-bar ties. MAE/MFE understated; bias
  direction ambiguous. Needs OHLC plumbing into `quotes`.
- **🟡 `scoring_engine` abstain-replacement** substitutes a regime
  tilt for every abstaining component; the tilts share inputs (not
  orthogonal) so several can push the composite the same way →
  over-confident regime labels on sparse data. Design refactor.
- **🟡 gamma-flip uses per-strike net-GEX zero crossing**, not the
  cumulative-GEX or recomputed-GEX(S) convention (SpotGamma-style).
  Legitimate simplification but differs from industry; flag for product
  decision.
- **🟡 vanna/charm exposure normalize by `OI·100·S`** while GEX uses
  `OI·100·S²·0.01`. Internally consistent but a different unit basis;
  document the convention.
- **🟢 Not a bug (verified):** `flow_smart_money.unusual_activity_score
  NUMERIC(5,2)` is fine — the score is hard-capped at 10 by
  `LEAST(10, …)`, so no overflow is possible.
- Minor/low-confidence: `/api/health` exact-match vs trailing slash,
  holiday-unaware session cutoffs, `minutes_since_open` clock
  arithmetic (formula is actually correct for ≥09:30), VIX `_seeded`
  on first partial payload, stream-snapshot reuse semantics.
