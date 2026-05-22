# Volume Tracking — End-to-End Architectural Review

**Scope:** Ingestion Engine and Analytics Engine. Specifically, how option contract volume (raw, classified, and signed/gross) is captured from TradeStation, persisted, transformed, and consumed by every downstream metric and signal.

**Read style:** Every claim in this document includes a `file:line` reference. The reader is expected to verify, not trust. Where I have not verified something directly (e.g. live DB shape, integration test pass/fail), I say so.

**Status of system:** The architecture has *already converged on the right model* (session-cumulative storage + LAG-based delta recovery) after at least one major prior bug ("the prior additive-upsert design", documented in `src/ingestion/main_engine.py:84-90`). The recurring bug surface today is not the data model — it is contract / naming / load-bearing-patch ambiguity at layer boundaries.

---

## 0. Investigation methodology

| Step | What I did | How to audit |
|---|---|---|
| 1 | Read both existing architecture diagrams cover-to-cover | `docs/architecture/ingestion_engine_diagram.md`, `docs/architecture/analytics_engine_diagram.md` |
| 2 | Read the full DB schema (`setup/database/schema.sql`, 1655 lines), with focus on every `volume`-bearing column and its `COMMENT ON COLUMN` | `setup/database/schema.sql` |
| 3 | Read the ingestion writer's accumulator + UPSERT path | `src/ingestion/main_engine.py` (specifically `_FlowAccumulator`, `_ingest_snapshot_into_accumulator`, `_OPTION_UPSERT_SQL`, `_coalesce_option_rows`) |
| 4 | Read the stream-merger volume handling | `src/ingestion/stream_manager.py:385-454` |
| 5 | Read the API's `flow_contract_facts` INSERT (the LAG-delta materialization) | `src/api/database.py:631-881` |
| 6 | Read the analytics engine's `_refresh_flow_caches` (`flow_by_contract` + `flow_smart_money` writers) | `src/analytics/main_engine.py:2419-2687` |
| 7 | Read the canonical 5-min flow-series CTE + both UPSERT forms | `src/flow_series_sql.py` |
| 8 | Read the GEX writers that aggregate `option_chains.volume` | `src/analytics/main_engine.py:810-940, 1880-1955` |
| 9 | Sent three parallel sub-agents: (a) consumer audit, (b) defensive-pattern hunt, (c) test inventory | results integrated into §3 and §4 |
| 10 | Spot-verified the consumer-audit conclusion against `flow_smart_money` readers (which the audit had not fully traced) — found one real divergence: `src/signals/position_optimizer_engine.py:433-441` |

**What I did *not* do:** I did not run the test suite, query a live DB, or attempt to reproduce any of the historical bugs. All findings are static-analysis only. Any item marked **(unverified at runtime)** should be confirmed empirically before acting on it.

---

## 1. Volume data flow end-to-end

The pipeline is **TradeStation → stream merger → in-memory per-contract accumulator → option_chains → flow_contract_facts (LAG-derived deltas) → flow_by_contract (cumulative re-aggregated) → flow_series_5min** with a parallel branch into `flow_smart_money`, `gex_by_strike.{call,put}_volume`, and `gex_summary.total_{call,put}_volume`.

```
TradeStation                                    [external]
   |  Volume = SESSION-CUMULATIVE per contract,
   |  resets to 0 at 09:30 ET cash open.
   v
OptionStreamAccumulator                         src/ingestion/stream_manager.py:236-460
   |  In-memory merge of streaming JSON quote messages.
   |  PATCH: Volume only overwrites when > 0  (line 421-430)
   v
_FlowAccumulator (per option, per ET session)   src/ingestion/main_engine.py:75-105
   |  Holds: last_volume_cum, ask_cum, mid_cum, bid_cum, last_bid/ask/mid.
   |  Computes per-snapshot vol_delta and Lee-Ready-classifies it.
   |  PATCH: watermark re-anchor on vendor reset (line 966-967)
   v
option_chains                                   setup/database/schema.sql:59-82
   |  PK (option_symbol, timestamp). Volume + ask/mid/bid_volume are
   |  SESSION-CUMULATIVE monotonic. UPSERT uses GREATEST.
   |  COMMENT ON COLUMN documents the contract (lines 113-120).
   v
   |\------------------- flow_contract_facts ---  src/api/database.py:694-881
   |                       PK (timestamp, symbol, option_symbol).
   |                       DELTAS via LAG within ET session-date.
   |                       PATCH: NULL LAG -> full volume (line 782);
   |                              date mismatch -> full volume (line 786).
   |                              buy/sell extrapolated from ask/bid ratio
   |                              (lines 822-833).
   |
   |\----- gex_by_strike (per cycle snapshot)
   |          call_volume, put_volume = SUM(option_chains.volume)
   |          per (strike, expiration).  CUMULATIVE per snapshot.
   |          src/analytics/main_engine.py:819, 826
   |
   |\----- gex_summary (per cycle snapshot)
   |          total_call_volume / total_put_volume = SUM across all strikes
   |          CUMULATIVE per snapshot.
   |          src/analytics/main_engine.py:1894-1895
   |
   |\----- flow_smart_money (per cycle)
   |          PATCH-IDENTICAL LAG-delta as flow_contract_facts but with
   |          a 2-minute lookback window (lines 2587-2598).
   |          Column NAMED "total_volume" but stores a 1-bucket delta.
   |          src/analytics/main_engine.py:2575-2677
   v
flow_by_contract                                src/analytics/main_engine.py:2472-2530
   |  PK (timestamp, symbol, option_type, strike, expiration).
   |  raw_volume = SUM(volume_delta) from session_open through bucket_end
   |  --> DAY-TO-DATE CUMULATIVE per contract per 5-min bucket.
   |  HAVING SUM(volume_delta) > 0  (line 2510) -- sparse on never-traded
   |  but dense once a contract trades at least once in the session.
   v
flow_series_5min                                src/flow_series_sql.py
      PK (symbol, bar_start).  *_cum columns are session-cumulative
      cross-contract aggregates.  Two writer forms (see §1.5).
```

### 1.1  Ingestion: TradeStation → in-memory state

**Source semantic (TradeStation):**
TradeStation's streaming option quote field `Volume` is **session-cumulative per contract** with a reset at 09:30 ET cash open. This is asserted by `src/ingestion/stream_manager.py:806-809` ("Volume is cumulative for the day; the first ~30 min after open are a natural ramp...") and by `src/ingestion/main_engine.py:949-963` ("TradeStation's cumulative volume resets to 0 at the 09:30 ET cash open"). It is also reflected in the existing diagram at `docs/architecture/ingestion_engine_diagram.md:436-439`.

**Stream merger** (`src/ingestion/stream_manager.py:385-454`):

Quote messages arrive as partial JSON deltas. The merger maintains `_state[symbol]` per option contract and merges incoming fields. Critically (lines 421-430):

```python
# Volume: only overwrite when > 0 — streaming deltas frequently
# send Volume=0 between trades, which would erase the accumulated
# cumulative daily volume (same pattern as OI/IV below).
vol_val = q.get("Volume")
if vol_val is not None:
    try:
        if int(vol_val) > 0:
            merged["Volume"] = vol_val
    except (ValueError, TypeError):
        pass
```

This is **load-bearing patch #1**. It exists because TradeStation occasionally publishes quote updates whose `Volume` field is 0 (e.g., NBBO changes without a trade). A naive overwrite would clobber the running cumulative.

**Side effect** to be aware of: if a daily restart does not happen, the merger holds the prior session's residual cumulative through 09:30 ET until the first trade of the new session arrives with `Volume > 0`. Any option_chains row written in that window carries the **prior session's cumulative**. The downstream watermark re-anchor catches this in-memory; whether already-written rows can be corrupted is discussed in §3.4.

### 1.2  `_FlowAccumulator` (per option, per ET session)

Defined at `src/ingestion/main_engine.py:75-105`. Holds `session_date`, `last_volume_cum` (the watermark), `ask_cum`, `mid_cum`, `bid_cum`, and the most recent NBBO (`last_bid`, `last_ask`, `last_mid`) used as the prior tick for the next Lee-Ready classification.

The accumulator is **keyed by `(option_symbol, ET calendar date)`** via `_bucket_session_date()` at `src/ingestion/main_engine.py:848-858`:

```python
def _bucket_session_date(bucket: datetime) -> _date:
    ...
    return bucket_et.date()
```

**This is where the architectural mismatch lives**: the *vendor* resets cumulative volume at **09:30 ET**, but the accumulator's session key is **midnight ET**. The two boundaries do not coincide, which forces patch #2 below.

**Hydration** (`src/ingestion/main_engine.py:860-914`): on first observation of a `(contract, session_date)` pair, the accumulator queries `option_chains` for the latest persisted row in this ET session (start of session_date midnight ET → now) and re-seeds `last_volume_cum`, `ask_cum`, `mid_cum`, `bid_cum`, and the NBBO from it. This is replay-safe: a restart in the middle of the session recovers the same monotonic state.

**Snapshot ingestion** (`src/ingestion/main_engine.py:933-1002`):

```python
curr_vol = int(snap.get("volume") or 0)
if curr_vol < acc.last_volume_cum:
    acc.last_volume_cum = 0                        # PATCH #2 — re-anchor
vol_delta = max(curr_vol - acc.last_volume_cum, 0)
if vol_delta > 0:
    skip = FLOW_CLASSIFY_SKIP_OPEN_AUCTION and self._is_opening_auction_bucket(bucket)
    if skip:
        acc.mid_cum += vol_delta                   # auction routes to mid
    else:
        av, mv, bv = self._classify_volume_chunk(vol_delta, snap.get("last"),
                                                  prior_bid, prior_ask, prior_mid)
        acc.ask_cum += av
        acc.mid_cum += mv
        acc.bid_cum += bv
if curr_vol > acc.last_volume_cum:
    acc.last_volume_cum = curr_vol
```

**Load-bearing patch #2** is the `if curr_vol < acc.last_volume_cum: acc.last_volume_cum = 0` re-anchor at lines 966-967. Without it: any 00:00→09:30 ET snapshot hydrates the accumulator with the *prior* session's residual cumulative as `last_volume_cum`. When the vendor then resets at 09:30 ET and a trade arrives with `curr_vol = N` where `N < residual`, `vol_delta = max(N - residual, 0) = 0`, and every subsequent cash-session trade is silently swallowed for the rest of the day. This is the bug regressed against by `tests/test_ingestion_volume_classification.py::test_accumulator_reanchors_watermark_on_vendor_cumulative_reset` (per the sub-agent test inventory).

**Opening-auction carve-out** (lines 970-972): if the bucket is exactly 09:30 ET, all `vol_delta` routes to `mid_cum` regardless of Lee-Ready classification, because the opening cross price is not comparable to NBBO. The per-row invariant `ask_volume + mid_volume + bid_volume == volume` holds modulo this carve-out (`setup/database/schema.sql:106-108`).

### 1.3  `option_chains` UPSERT

Defined at `src/ingestion/main_engine.py:1098-1136`. Every monotonic numeric column (`volume`, `open_interest`, `ask_volume`, `mid_volume`, `bid_volume`) merges via `GREATEST`:

```sql
volume       = GREATEST(option_chains.volume, EXCLUDED.volume),
ask_volume   = GREATEST(option_chains.ask_volume, EXCLUDED.ask_volume),
mid_volume   = GREATEST(option_chains.mid_volume, EXCLUDED.mid_volume),
bid_volume   = GREATEST(option_chains.bid_volume, EXCLUDED.bid_volume),
open_interest= GREATEST(option_chains.open_interest, EXCLUDED.open_interest),
```

with an `IS DISTINCT FROM` guard suppressing no-op writes. This makes the UPSERT **idempotent under replay** (lines 1090-1097) and is the reason the retain-and-retry queue at `src/ingestion/main_engine.py:1186-1219` is safe.

Schema contract documented at `setup/database/schema.sql:113-120`:
> `option_chains.volume`: *Session-cumulative raw contract volume (resets at cash open, monotonic intraday). NOT per-minute. Use flow_contract_facts for period volume.*

### 1.4  `flow_contract_facts` (deltas)

Written by `src/api/database.py:694-875` from `option_chains` rows via a per-symbol LAG, with a seed row from before the backfill window via a LATERAL JOIN to set the LAG anchor (lines 722-764). The delta CASE (lines 781-787, repeated for `ask_volume` at 788-794 and `bid_volume` at 795-801):

```sql
CASE
    WHEN LAG(s.volume) OVER w IS NULL THEN COALESCE(s.volume, 0)
    WHEN (LAG(s.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
        = (s.timestamp AT TIME ZONE 'America/New_York')::date
        THEN GREATEST(COALESCE(s.volume, 0) - COALESCE(LAG(s.volume) OVER w, 0), 0)
    ELSE COALESCE(s.volume, 0)
END::bigint AS volume_delta
```

Three branches, three edge cases:

| Branch | Meaning | Failure mode |
|---|---|---|
| `LAG IS NULL` | This row is the first observation for this contract within the seed+window union | First-row attribution: full cumulative attributed as one delta. If the seed query missed a real prior-day row, **double counts** at session start. |
| `LAG.date == curr.date` | Same ET calendar day | The normal case. `GREATEST(..., 0)` ensures a non-negative delta. |
| `else` (LAG from a prior ET date) | Crossed midnight ET | Treat as session start: attribute the full current cumulative as a delta. |

Notice this delta is keyed on **ET calendar date** (midnight ET), not on 09:30 ET — same convention as the accumulator. This means: a row at 02:00 ET on day N+1 with `volume = X` will, against a LAG row from 23:00 ET on day N with `volume = Y`, produce `volume_delta = X` (falling into the third branch). If the contract is in continuous extended-hours trading, this overstates the delta by `Y`. **In practice, options have no real overnight ETH liquidity, so this is rarely exercised**, but it is the mirror image of the watermark-re-anchor patch on the in-memory side and arises from the same root cause (calendar-day session keying).

**buy_volume / sell_volume scaling** (`src/api/database.py:822-833`): a deliberate semantic re-classification at the delta layer.

```sql
-- Scale buy/sell volumes to account for unclassified volume.
-- The classified subset (ask + bid) provides the directional
-- signal; extrapolate to the full volume_delta assuming the
-- unclassified portion has the same buy/sell ratio.
CASE WHEN (ask_vol_delta + bid_vol_delta) > 0
     THEN (ask_vol_delta::numeric / (ask_vol_delta + bid_vol_delta) * volume_delta)::bigint
     ELSE 0 END AS buy_volume,
CASE WHEN (ask_vol_delta + bid_vol_delta) > 0
     THEN (bid_vol_delta::numeric / (ask_vol_delta + bid_vol_delta) * volume_delta)::bigint
     ELSE 0 END AS sell_volume,
```

Note: `buy_volume + sell_volume == volume_delta` (modulo rounding), **not** `ask_vol_delta + bid_vol_delta`. The mid-classified volume is silently re-distributed in proportion to `ask:bid`. This is reasonable, but **it is a value-changing transformation that nothing in the schema comment documents**.

UPSERT semantics at `src/api/database.py:861-870`: straight `EXCLUDED.x` overwrite, **no GREATEST** — these are pre-computed deltas, not running cumulatives.

`WHERE volume_delta > 0` filter at line 860 makes `flow_contract_facts` a **sparse table** (no rows for buckets where a contract didn't trade).

### 1.5  `flow_by_contract` (re-cumulated per-contract)

Written by `src/analytics/main_engine.py:2472-2530`. Each refresh writes **two rows per active contract** (prev 5-min bucket + curr 5-min bucket):

```sql
SELECT
    bt.bucket_start                              AS timestamp,
    f.symbol, f.option_type, f.strike, f.expiration,
    SUM(f.volume_delta)::bigint                  AS raw_volume,
    SUM(f.premium_delta)::numeric                AS raw_premium,
    SUM(f.buy_volume - f.sell_volume)::bigint    AS net_volume,
    SUM(f.buy_premium - f.sell_premium)::numeric AS net_premium,
    ...
FROM flow_contract_facts f
CROSS JOIN bucket_targets bt
WHERE f.symbol = %s
  AND f.timestamp >= %s::timestamptz  -- session_open (09:30 ET)
  AND f.timestamp <  bt.bucket_end
GROUP BY bt.bucket_start, f.symbol, f.option_type, f.strike, f.expiration
HAVING SUM(f.volume_delta) > 0
```

So `flow_by_contract.raw_volume` at bucket `B` = sum of `volume_delta` from `[09:30 ET, B+5min)` — i.e., **day-to-date cumulative per contract as of the bucket boundary**, exactly as the schema comment claims (`setup/database/schema.sql:523-525`). Once a contract has *any* trade in the session, the `HAVING > 0` filter keeps emitting a row for it on every subsequent bucket too (because the SUM-from-session-open stays positive). So `flow_by_contract` is **dense per-contract after first trade, sparse before**.

`net_volume` and `net_premium` are the **buy − sell** signed cumulative — derived from the already-extrapolated `buy_volume / sell_volume / buy_premium / sell_premium` in `flow_contract_facts`.

UPSERT (`src/analytics/main_engine.py:2511-2518`): straight `EXCLUDED.x` overwrite, because the SUM from session_open is *always* recomputable as the latest cumulative value.

### 1.6  `flow_series_5min` (cross-contract cumulative)

Two writer paths share a canonical contract (per the docstring at `src/flow_series_sql.py:1-27`):

| Path | Source | Use |
|---|---|---|
| `SNAPSHOT_UPSERT_PSYCOPG2` (`src/flow_series_sql.py:248-261`) | `flow_by_contract` → LAG per contract → per-bar SUM → `OVER w_cum` cumulative | cold-start / gap-fill, full session window |
| `SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2` (`src/flow_series_sql.py:320-423`) | `flow_by_contract` → direct cross-contract SUM at target bar | steady-state, only `(prev_bar, curr_bar)` written each cycle |

The two are claimed to be **algebraically equivalent** at `src/flow_series_sql.py:282-296`. The equivalence depends on the **dense-after-first-trade** property of `flow_by_contract` discussed above; if that property breaks (e.g., if `HAVING SUM > 0` were ever relaxed or if a bucket has no rows for a previously-active contract due to ingestion gaps), the incremental form would *under-report* compared to the LAG-and-recum form. The integration test `tests/test_flow_series_parity.py::test_snapshot_matches_live_cte_row_for_row` is the gate for this (per the sub-agent test inventory) — **but it only runs against a real DB** (it is skipped without `POSTGRES_TEST_URL`). In CI without a DB, parity is not asserted.

**1.7  Side branches**

| Table | Writer | Semantic of the volume-bearing column |
|---|---|---|
| `gex_by_strike.call_volume` / `put_volume` | `src/analytics/main_engine.py:819, 826` | **SUM of session-cumulative** `option_chains.volume` across contracts at a given (strike, expiration). Effectively a per-snapshot session-cumulative aggregate. |
| `gex_summary.total_call_volume` / `total_put_volume` | `src/analytics/main_engine.py:1894-1895` | Same as above, summed across all strikes. |
| `flow_smart_money.total_volume` | `src/analytics/main_engine.py:2575-2677` | A LAG-derived **1-minute delta** despite the column name. See §3.2. |

---

## 2. Root architectural issue

**It is not the data model.** The data model is essentially correct: session-cumulative storage + LAG-based delta recovery is a standard pattern, well-justified in the comment at `src/ingestion/main_engine.py:84-90` (the prior "additive-upsert" design failed because *equal* consecutive cumulatives have LAG-delta `0`, silently dropping flow whenever two buckets happened to contain identical activity).

The recurring bugs come from **three compounding contract/typing failures**:

### 2.1  No domain types

Every volume value in Python is `int` (or `bigint` in SQL). There is no `VolumeDelta` vs `VolumeCumulative` vs `VolumeSessionToDate` vs `VolumeNetDirectional` distinction. Mypy can't catch a function that takes a delta and is given a cumulative. The only safety net is a code comment or schema `COMMENT ON COLUMN`.

Examples where typing would have caught a bug at compile time:

- `src/signals/position_optimizer_engine.py:433-441` reads `SUM(total_premium)` from `flow_smart_money` and assigns it to `smart_call` / `smart_put`. The same `smart_call` / `smart_put` names elsewhere (`src/signals/components/order_flow_imbalance.py:44-45`) refer to *signed net premium* from the canonical `flow_contract_facts` aggregation in `src/signals/unified_signal_engine.py:546-564`. **Same name, two different semantics** — gross filtered notional vs. signed net premium. See §3.2 for the consequence.
- `flow_smart_money.total_volume` (named "total") is computed at `src/analytics/main_engine.py:2587-2593` as a single-bucket LAG delta — exactly the kind of name/semantic divorce that surfaces as bugs only when a new consumer naively reads the column.

### 2.2  Inconsistent column naming

The codebase uses several naming conventions for volume, none of which is consistently mapped to a semantic:

| Suffix / naming | Example | Actual semantic |
|---|---|---|
| (none) | `option_chains.volume` | Session-cumulative |
| (none) | `gex_by_strike.call_volume` / `put_volume` | Session-cumulative aggregate |
| `total_` | `gex_summary.total_call_volume` | Session-cumulative aggregate |
| `total_` | `flow_smart_money.total_volume` | **1-minute delta** (column-name lies) |
| `_delta` | `flow_contract_facts.volume_delta` | Per-bucket delta |
| `raw_` | `flow_by_contract.raw_volume` | Day-to-date cumulative per contract |
| `_cum` | `flow_series_5min.call_volume_cum` | Session-cumulative cross-contract |

The honest cases are `_delta` and `_cum`. The misleading cases are `total_` (used for two different semantics across two tables) and the unsuffixed `volume` (sometimes raw, sometimes aggregate). Every naming convention except `_delta` / `_cum` has at least one place where the name does not match the semantic.

### 2.3  Schema documentation drift

`option_chains.{volume, ask_volume, mid_volume, bid_volume}` have explicit `COMMENT ON COLUMN` at `setup/database/schema.sql:113-120`. They are the only volume-bearing columns with schema-level semantic documentation.

Other columns have *no* schema-level comment:
- `gex_by_strike.call_volume`, `put_volume`
- `gex_summary.total_call_volume`, `total_put_volume`
- `flow_by_contract.raw_volume`, `net_volume` (the comment at `setup/database/schema.sql:523-525` documents the table but is not attached to columns)
- `flow_smart_money.total_volume` — **no schema comment that this is actually a 1-minute delta**

A future developer querying `\d+ flow_smart_money` in psql will see `total_volume BIGINT` with no commentary, and is likely to assume it means session-cumulative or window-aggregate.

### 2.4  Calendar-day session keying (midnight ET vs 09:30 ET)

The accumulator (`src/ingestion/main_engine.py:848-858`), the `flow_contract_facts` LAG CASE (`src/api/database.py:783-787`), and `option_chains_with_deltas` view (`setup/database/schema.sql:464-473`) all use **ET calendar date** as the session partition. TradeStation resets at **09:30 ET**. The mismatch creates a several-hour window each morning where in-memory accumulator state and the cumulative on disk are from the *prior* session, requiring patch #2 (the watermark re-anchor) and the `WHEN LAG IS NULL THEN COALESCE(volume, 0)` branch in every LAG-delta site. Aligning on a 09:30-ET-anchored session key would obsolete both patches.

### 2.5  Summary

It is **all four** of the user's enumerated possibilities:
- **Contract/typing problem** (§2.1) — primary
- **Schema problem** (§2.2, §2.3) — secondary, compounds typing problem
- **Ingestion problem** (§2.4) — the load-bearing patches all trace to this
- **Consumer problem** (§3.2) — `position_optimizer_engine` reads the wrong semantic from `flow_smart_money`, an artifact of (§2.1) and (§2.2)

The good news: the **data shape** is correct. The bad news: the **interpretation** of that shape lives in code comments, not in types or schema. Every new consumer is one careless `SELECT total_volume FROM flow_smart_money` away from a bug.

---

## 3. Downstream regression surface

For each consumer or persisted metric that depends on volume tracking, I classify as **Currently correct**, **Currently patched-but-working** (and what patch makes it work), or **Potentially silently wrong** (and what would expose it). The reference source for each is given.

### 3.1  Currently correct

| Consumer | File:line | Reads | Why it's correct |
|---|---|---|---|
| `_FlowAccumulator` ingest path | `src/ingestion/main_engine.py:933-1002` | TS stream `Volume` | Watermark math is right *given* the load-bearing patches at lines 421-430 (stream) and 966-967 (accumulator). |
| `option_chains` UPSERT | `src/ingestion/main_engine.py:1098-1136` | n/a (writer) | GREATEST + IS DISTINCT FROM guard → idempotent under replay. Regression-tested at `tests/test_ingestion_volume_baseline.py:138`. |
| `flow_contract_facts` LAG materialization | `src/api/database.py:694-881` | `option_chains.{volume, ask_volume, bid_volume}` | The seed-row LATERAL JOIN (lines 722-764) sets the LAG anchor correctly within the lookback window; the date-mismatch CASE handles cross-session correctly. (See §3.4 for the residual edge case.) |
| `flow_by_contract` writer | `src/analytics/main_engine.py:2472-2530` | `flow_contract_facts.{volume_delta, premium_delta, buy_*, sell_*}` | SUM-from-session-open is correct cumulative arithmetic over per-bucket deltas. |
| `flow_series_5min` canonical CTE | `src/flow_series_sql.py:34-161` | `flow_by_contract` | LAG within contract → per-bar SUM → unbounded-preceding cumulative is correct. |
| `tape_flow_bias` | `src/signals/basic/tape_flow_bias.py:99-113` | `flow_by_type` (15-min SUM of `flow_contract_facts.buy_premium/sell_premium`) | Treats buy/sell premium as deltas, computes `bp - sp`. Semantic-correct. |
| `order_flow_imbalance` | `src/signals/components/order_flow_imbalance.py:44-55` | `ctx.smart_call` / `ctx.smart_put` from `unified_signal_engine.py:546-564` | Reads canonical signed net premium. |
| `zero_dte_position_imbalance` | `src/signals/advanced/zero_dte_position_imbalance.py:21-95` | `flow_zero_dte` from `unified_signal_engine.py:608-634` | Reads canonical buy/sell premium delta. |
| `trap_detection` | `src/signals/advanced/trap_detection.py:70-118` | `call_flow_delta` / `put_flow_delta` from `unified_signal_engine.py:648-675` | Reads difference-of-two-windows; canonical. |
| `/api/flow/smart-money` | `src/api/database.py:2102-2156` | `flow_contract_facts.{volume_delta, premium_delta, buy_premium, sell_premium}` | Despite the endpoint name, this reads canonical deltas from `flow_contract_facts`, **not** from `flow_smart_money`. |
| `gex_by_strike` writer | `src/analytics/main_engine.py:819, 826` | `option_chains.volume` per contract | Aggregates session-cumulative across contracts at a (strike, expiration). Stored value is *cumulative-snapshot* and downstream consumers (PCR) treat it that way. |
| `put_call_ratio_state` | `src/signals/components/put_call_ratio_state.py` | `gex_summary.total_put_volume / gex_summary.total_call_volume` | A ratio of two cumulatives is dimensionally fine (both cumulatives reset together at 09:30 ET, so the ratio is meaningful intraday). |

### 3.2  Potentially silently wrong

| Consumer | File:line | Issue | What would expose it |
|---|---|---|---|
| `PositionOptimizerEngine._fetch_market_context` | `src/signals/position_optimizer_engine.py:433-448` | Reads `SUM(total_premium) FROM flow_smart_money` over a 30-min window and assigns to `smart_call` / `smart_put`. But `flow_smart_money.total_premium` is **gross** (`volume_delta * last * 100`, not signed), and it's only over the **filtered "unusual"** subset (lines 2634-2638 in analytics). The same `smart_call` / `smart_put` names elsewhere refer to canonical *signed* net premium from `flow_contract_facts`. | A back-test comparing PositionOptimizer recommendations against UnifiedSignalEngine outputs would show divergence when the unusual-activity tier filter is active. **This is the same anti-pattern the `unified_signal_engine.py:533-540` comment says was already fixed elsewhere.** |
| `unified_signal_engine` fallback path | `src/signals/unified_signal_engine.py:577-600` | When the primary `flow_contract_facts` query fails, the fallback reads `total_premium` from `flow_smart_money` and assigns to `sm_call` / `sm_put`. Same semantic mismatch as above — gross-filtered, not signed. Then `sm_call_gross = abs(sm_call)`. | A statement timeout or DB issue that triggers the fallback path would silently degrade *every* downstream signal that consumes `ctx.smart_call` / `smart_put`. Hard to observe without explicit telemetry on which path was used. |
| `flow_smart_money.total_volume` | `src/analytics/main_engine.py:2575-2677` (writer) | Stored value is a 1-bucket LAG delta; column is named "total_volume". No schema `COMMENT ON COLUMN`. | Any future query like `SELECT SUM(total_volume) FROM flow_smart_money WHERE ts >= ...` will silently produce a sum-of-1-bucket-deltas where the author probably intended a session-cumulative-aggregate. Not currently exposed because the only existing such consumers (the two in `position_optimizer_engine` and the `unified_signal_engine` fallback) already happen to want a window-sum-of-events, but the next consumer is a likely casualty. |
| Smart-money LAG window | `src/analytics/main_engine.py:2596-2598` | `flow_smart_money` insertion LAG looks back **2 minutes** for the prior `oc.volume`. If a contract is illiquid enough that its prior row is >2 min old, the LAG is NULL and the CASE at line 2588 attributes **the entire session-cumulative volume** as this bucket's delta. | The contract's first observation of the day after a >2-min gap (very common for far-OTM strikes that trade thinly) gets falsely classified as a multi-thousand-volume "event" and likely tier-4-scored. Would manifest as spurious "🔥 Massive Block" entries on illiquid contracts. **(unverified at runtime)** |
| Stream merger / accumulator interaction at 09:30 ET | `src/ingestion/stream_manager.py:421-430` + `src/ingestion/main_engine.py:966-967` | Between 09:30 ET and the first new-session trade, the stream merger holds the prior session's cumulative (because incoming `Volume=0` is ignored). Rows written to `option_chains` during this window carry the *prior* cumulative. The GREATEST upsert at `src/ingestion/main_engine.py:1110` *protects* this stale value: when the first trade arrives and the engine tries to UPSERT with the smaller post-reset cumulative, GREATEST keeps the larger stale value. The accumulator's watermark re-anchor recovers the **in-memory** state correctly but **does not rewrite the already-persisted rows**. | The morning's first option_chains rows for a still-trading contract would carry the prior session's cumulative; the LAG-delta in `flow_contract_facts` would then attribute that residual as `volume_delta` on the first row of the new session (third CASE branch, `ELSE COALESCE(s.volume, 0)`). Mitigations exist: (a) if ingestion restarts daily, the stream merger starts fresh and never carries prior state across 09:30 ET, and (b) the `_seed_from_rest` (`stream_manager.py:283-298`) on initialization fetches a fresh REST snapshot. **In practice, this only fires when the engine runs continuously across the cash-open boundary AND the contract has streaming activity pre-09:30 ET, which is uncommon for options.** Worth measuring. **(unverified at runtime)** |

### 3.3  Currently patched-but-working (load-bearing patches that, if naively removed, will regress)

| Patch | File:line | What breaks if removed |
|---|---|---|
| Watermark re-anchor on vendor reset | `src/ingestion/main_engine.py:966-967` | Continuous-run engine drops all post-09:30 ET volume forever, because `vol_delta = max(curr - residual, 0) = 0`. Regression-tested. |
| Stream merger skip-when-Volume-zero | `src/ingestion/stream_manager.py:421-430` | `Volume=0` quote updates clobber the accumulator's running cumulative, producing rows with `volume=0` mid-session that violate the monotonicity invariant. The GREATEST UPSERT would partially mask this, but accumulator math would still bug. |
| First-row-of-session LAG fallback | `src/api/database.py:782` (`WHEN LAG IS NULL THEN COALESCE(volume, 0)`) — same pattern at `src/analytics/main_engine.py:2588`, `setup/database/schema.sql:464-473` | First row of every contract's session in `flow_contract_facts` gets `volume_delta = 0` instead of the seed cumulative. Drops *all* volume that occurred before the LAG window's start. |
| ET-date boundary detection in LAG CASE | `src/api/database.py:783-786` (and repeats) | If LAG is from prior ET date, the simple `GREATEST(curr - prev, 0)` would be `0` (because curr post-reset is *less* than prev pre-reset). The `ELSE COALESCE(volume, 0)` fallback recognises the boundary and attributes the full current cumulative. |
| Opening-auction carve-out | `src/ingestion/main_engine.py:970-972` | Lee-Ready classifier runs against post-open NBBO with auction print → misclassifies the 09:30 cross into ask or bid. The schema invariant `ask + mid + bid == volume` would still hold, but ask/bid would be polluted with auction volume. |
| GREATEST on every cumulative column in UPSERT | `src/ingestion/main_engine.py:1110-1115` | Replay/retry safety lost. The retain-and-retry queue at `src/ingestion/main_engine.py:1186-1219` assumes idempotency and would otherwise double-count or under-count on retry. |
| `_coalesce_option_rows` uses `max` not `sum` | `src/ingestion/main_engine.py:1138-1184` | If two snapshots for the same `(option_symbol, timestamp)` are coalesced via `sum`, classified volume doubles. The current `max` is correct because the values are cumulative-snapshots. |
| `HAVING SUM(volume_delta) > 0` in `flow_by_contract` writer | `src/analytics/main_engine.py:2510` | Without it, sparse table grows to `O(contracts × buckets)` and queries scan way more rows. |
| `IS DISTINCT FROM` write-suppression in `flow_series_5min` UPSERT | `src/flow_series_sql.py:259-260, 419-422` | Every cycle would rewrite 78 closed bars worth of rows even though they're window-invariant. Performance regression, not correctness. |
| Buy/sell extrapolation to full `volume_delta` | `src/api/database.py:822-833` | Without it, `buy_volume + sell_volume` would equal `ask_vol_delta + bid_vol_delta` (i.e., total *minus* mid-classified). All downstream signals consuming `buy_volume`/`sell_volume` as if they sum to `volume_delta` would silently under-count by the mid-classified share. |

### 3.4  Other observations

- **`option_chains_with_deltas` view is defined but unused in code** (verified by `grep` against `src/`). It exists at `setup/database/schema.sql:446-484` for ad-hoc dashboard / DBA queries. It is *not* the canonical materialization (that's `flow_contract_facts`). Worth either documenting more prominently or dropping.
- **`flow_smart_money` is written every cycle but read by only three sites**: `src/signals/position_optimizer_engine.py:436` (the wrong-semantic case in §3.2), `src/signals/unified_signal_engine.py:586` (the fallback in §3.2), and the existence-check at `src/api/database.py:885`. The public smart-money API endpoint at `src/api/database.py:2102` reads `flow_contract_facts`, not `flow_smart_money`. **The table's value proposition is unclear** given that the canonical source is `flow_contract_facts` and the only readers either consume it wrong or use it as a fallback.

---

## 4. Recommendation: harden, don't redesign

The system has already converged on the architecturally-correct pattern. A clean redesign of the volume model would deliver minimal correctness benefit at significant risk:

| Argument | For redesign | For hardening |
|---|---|---|
| Data model is wrong | No — cumulative-storage + LAG-delta-recovery is correct | Confirmed |
| Load-bearing patches are accumulating | Yes — patches #1 (Volume=0 skip), #2 (watermark re-anchor), #3 (LAG-NULL fallback), #4 (date-boundary CASE) all trace to one root cause: calendar-day session keying ≠ vendor reset time | Each patch is regression-tested individually; they compose cleanly |
| Naming / semantic ambiguity at consumer boundary | Yes — `flow_smart_money.total_volume`, `gex_*.{call,put}_volume` are documentation bugs waiting to break a future signal | These can be fixed in place without touching the data model |
| Existing test coverage | Comprehensive on the critical path (watermark, vendor-reset, replay, classification, parity); thin on the new bugs found in this review (gross-vs-signed in position_optimizer; the stream-merger × accumulator 09:30 race) | Adding two more tests is much cheaper than re-architecting |
| Risk of silent regression in a redesign | High — the parity test (`tests/test_flow_series_parity.py`) requires a live DB and is **not run in default CI**, so a redesign would lose its main correctness gate at the moment it's needed most | Hardening adds tests; redesign would need to add the same tests *plus* a migration verification harness |
| Production cost | Multi-week migration + dual-write + parity-verification + cutover + monitoring | Hours-to-days of focused fixes |

**Recommendation: harden in place.** The redesign cost is not justified given the system is correct today and most of the residual risk is naming/contract drift, not data-model wrongness.

### Ordered hardening sequence

Listed by priority (highest first). Each item is independent — they can be done one at a time and reviewed in isolation. **Each item should pass a parity check against today's behavior before merging** (concrete checks listed inline).

1. **Fix `PositionOptimizerEngine` to read canonical signed net premium.**
   - Change `src/signals/position_optimizer_engine.py:433-441` to query `flow_contract_facts.{buy_premium, sell_premium}` directly (mirror of `src/signals/unified_signal_engine.py:546-564`), and assign `smart_call = SUM(buy_premium - sell_premium)`, not `SUM(total_premium)`.
   - **Parity check:** Run the position optimizer in shadow mode for one trading session and compare its recommendations against the existing implementation. Flag every divergence. Expected: divergences should be in the direction of *less spurious "smart-money" confirmation* during periods of balanced buy/sell flow.
   - **Why first:** This is the only consumer I found that has a real semantic bug (vs. a naming / latent risk). Lowest cost, highest correctness benefit.

2. **Rename `flow_smart_money.total_volume` → `flow_smart_money.volume_delta_1bar` and add a schema `COMMENT ON COLUMN`.**
   - Migration: `ALTER TABLE flow_smart_money RENAME COLUMN total_volume TO volume_delta_1bar; ALTER TABLE flow_smart_money RENAME COLUMN total_premium TO premium_delta_1bar;`
   - Update writer at `src/analytics/main_engine.py:2575-2677` and the two readers in §3.2.
   - Add `COMMENT ON COLUMN flow_smart_money.volume_delta_1bar IS 'Single-bar (≤2 min lookback) LAG-derived volume delta for an "unusual activity" event. NOT session-cumulative.';`
   - **Parity check:** Verify no other code references `total_volume` or `total_premium` on `flow_smart_money` (`grep -r "flow_smart_money" src/ | grep -i "total_"`). Verify the existing API endpoint at `src/api/database.py:2102` is untouched (it reads `flow_contract_facts`, not this table).
   - **Why second:** Cheap, removes the worst naming trap. Use the opportunity to also fix `position_optimizer_engine` (item 1) to use the canonical source instead of this table.

3. **Audit and decide on `flow_smart_money`'s existence.**
   - Given the canonical smart-money API reads `flow_contract_facts`, and the only two remaining readers are the position-optimizer (fixed in item 1) and the unified-signal-engine fallback path (which should be re-pointed to `flow_contract_facts` with its own try/except), `flow_smart_money` may have no remaining justification.
   - Decision: drop the table (and the 100+ lines of writer at `src/analytics/main_engine.py:2575-2683`) **or** repurpose it as the canonical "smart-money event roster" used by an API endpoint. Pick one.
   - **Parity check:** Before any drop, log every read of the table for one full trading day to confirm zero non-fallback callers.

4. **Add a schema `COMMENT ON COLUMN` for every volume-bearing column without one.**
   - `gex_by_strike.call_volume` / `put_volume`: "Per-(strike, expiration) SUM of session-cumulative `option_chains.volume` at the snapshot timestamp. Snapshot value, not a per-bar delta. Resets at 09:30 ET."
   - `gex_summary.total_call_volume` / `total_put_volume`: equivalent text, across all strikes.
   - `flow_by_contract.raw_volume` / `net_volume` / `raw_premium` / `net_premium`: "Day-to-date cumulative for this contract through the end of the 5-minute bucket. Resets at 09:30 ET. Per-bucket delta is LAG within partition (option_type, strike, expiration) ordered by timestamp."
   - **Parity check:** none required — comments don't change values.
   - **Why:** This is the single highest-leverage step for preventing future regressions. Every future consumer reading the column gets the contract from `\d+`.

5. **Add an integration test for the 09:30 ET continuous-run scenario.**
   - Specifically: simulate an `OptionStreamAccumulator` that carries prior-session cumulative state through the 09:30 ET boundary, then receives a post-reset `Volume = N < prior_residual`. Assert that the *persisted* `option_chains.volume` rows do not carry the prior residual into the new session, OR that the downstream `flow_contract_facts` LAG-delta correctly attributes flow to the first new-session bucket.
   - If the test reveals a real bug (per the analysis in §3.2 row 5), fix by either:
     - daily-restarting the ingestion engine across 09:30 ET, OR
     - having the engine detect the 09:30 ET boundary and force a fresh `_seed_from_rest` to refresh the merger state.
   - **Parity check:** None required for the test itself. If a fix is required, the fix should leave the `flow_contract_facts.volume_delta` output identical on backtest data for a clean (restarted-overnight) day.

6. **Add a regression test for the `position_optimizer_engine` × `unified_signal_engine` semantic alignment.**
   - Build a synthetic `flow_smart_money` row set (filtered, gross premium) and a synthetic `flow_contract_facts` row set (full, signed premium) and assert that the *fixed* (item 1) position optimizer produces the same `smart_call` / `smart_put` values as `unified_signal_engine.py:546-564`.

7. **Make the parity test (`tests/test_flow_series_parity.py`) runnable in CI.**
   - Currently only runs against a live DB (per the sub-agent inventory). Provide a docker-compose service or a CI step that spins up Postgres, seeds a deterministic fixture, and runs the parity test. Without this, the *incremental* vs *canonical* `flow_series_5min` UPSERT formulations could silently diverge on a real bug.

8. **(Optional, larger) Address the calendar-day vs 09:30-ET session keying mismatch.**
   - Currently `_bucket_session_date()` uses calendar date. Change it to compute the *cash-session date* (the date of the 09:30 ET open whose session covers the given timestamp). For timestamps ≥ 09:30 ET, this is the same as the calendar date. For timestamps < 09:30 ET, this is *yesterday*'s date (since they belong to the prior session, before the 09:30 reset).
   - Same change in `src/api/database.py:783-786` (and three repeats) — partition the LAG by the cash-session date, not the calendar date.
   - This would obsolete patch #2 (the watermark re-anchor), patch #4 (the date-mismatch CASE), and the load-bearing first-row-LAG-NULL fallback for all *intra-session* rows (only true cold-start rows would still hit it).
   - **Parity check:** Critical. Backtest a full week of historical data using both keying conventions and assert every `flow_contract_facts.volume_delta` row matches.
   - **Why optional:** This is the only item that crosses into "redesign" territory. It would simplify the codebase but is the highest-risk item. **Only do it after items 1-7 are stable** so the test coverage is sufficient to catch regressions.

### Items NOT to do

- **Do not change the cumulative-storage + LAG-delta model.** It is correct, well-justified (see `src/ingestion/main_engine.py:84-90`), and consistent across writers and readers.
- **Do not consolidate `option_chains.{ask_volume, mid_volume, bid_volume}` into `flow_contract_facts.{buy_volume, sell_volume, mid_volume}` at the writer level.** The current split (raw cumulative storage + delta-time reclassification with buy/sell extrapolation) is intentional: it preserves the schema invariant `ask + mid + bid == volume` on the storage side while letting consumers reason about "buy-vs-sell directional pressure" at the delta layer.
- **Do not drop the `IS DISTINCT FROM` guard** anywhere. It is what makes every monotonic UPSERT idempotent under replay.
- **Do not drop `option_chains_with_deltas` without first confirming no dashboard / DBA query uses it** (the codebase does not, but operations may).

---

## 5. Open questions / things I could not verify statically

1. **Does the ingestion engine restart daily?** If yes, several of the §3.2 row-5 / §3.3 concerns (stream merger holding prior-session state) are moot. If no, item 5 in the hardening sequence is mandatory, not optional. Confirm via systemd unit / Makefile target.
2. **Is `tests/test_flow_series_parity.py` actually run in CI?** Per the sub-agent test inventory, it requires a live Postgres URL via env var. Confirm CI config to know whether the parity guarantee is enforced or aspirational.
3. **How often does the `position_optimizer_engine.py:433-441` smart-money query actually fire?** If the position optimizer is rarely invoked, the impact of fix item 1 is small. If it's central to the trade-recommendation path, the fix is urgent.
4. **Real-data measurement of the `flow_smart_money` 2-minute LAG window failure mode** (§3.2 row 4). A targeted query on a recent trading day would tell us whether the spurious "first observation after a >2-min gap" event has actually happened.
5. **The `WHEN LAG IS NULL THEN COALESCE(s.volume, 0)` first-row fallback in `flow_contract_facts`**: does the LATERAL seed query at `src/api/database.py:722-764` always find a prior row when one exists in the table? Need to confirm by querying for cases where the seed CTE produces zero rows for an active symbol.

---

## 6. Appendix: tables and their volume semantics at a glance

| Table | Column | Semantic | Resets at | Storage form |
|---|---|---|---|---|
| `option_chains` | `volume` | Session-cumulative raw | 09:30 ET (vendor) | GREATEST UPSERT |
| `option_chains` | `ask_volume` | Session-cumulative Lee-Ready ask | 09:30 ET | GREATEST UPSERT |
| `option_chains` | `mid_volume` | Session-cumulative Lee-Ready mid (+auction) | 09:30 ET | GREATEST UPSERT |
| `option_chains` | `bid_volume` | Session-cumulative Lee-Ready bid | 09:30 ET | GREATEST UPSERT |
| `flow_contract_facts` | `volume_delta` | Per-bucket delta (LAG within ET-date) | n/a | EXCLUDED overwrite |
| `flow_contract_facts` | `buy_volume`, `sell_volume` | Per-bucket delta, ask/bid-ratio-extrapolated to total | n/a | EXCLUDED overwrite |
| `flow_contract_facts` | `premium_delta`, `signed_premium`, `buy_premium`, `sell_premium` | Per-bucket delta in dollars (volume_delta × trade_price × 100) | n/a | EXCLUDED overwrite |
| `flow_by_contract` | `raw_volume`, `raw_premium`, `net_volume`, `net_premium` | Day-to-date cumulative per contract per 5-min bucket | 09:30 ET (computation) | EXCLUDED overwrite |
| `flow_smart_money` | `total_volume` ⚠ | **1-bucket LAG delta** despite the column name | 09:30 ET implicitly | EXCLUDED overwrite |
| `flow_smart_money` | `total_premium` ⚠ | **1-bucket LAG delta × price × 100** | 09:30 ET implicitly | EXCLUDED overwrite |
| `flow_series_5min` | `*_cum` columns | Cross-contract session-cumulative | 09:30 ET (window) | IS DISTINCT FROM guard |
| `gex_by_strike` | `call_volume`, `put_volume` | Per-snapshot session-cumulative SUM across contracts at a (strike, exp) | 09:30 ET (data) | IS DISTINCT FROM guard |
| `gex_summary` | `total_call_volume`, `total_put_volume` | Per-snapshot session-cumulative SUM across all strikes | 09:30 ET (data) | IS DISTINCT FROM guard |

⚠ = column name does not match semantic. See item 2 in §4.
