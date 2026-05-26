# Analytics Engine Snapshot Incident — Postmortem

**Date:** 2026-05-26
**Severity:** Sev-2 (intraday: degraded analytics + cascading API timeouts; no data loss)
**Status:** Resolved
**Branch / PR:** `claude/inspiring-bohr-3MwjS` (5 commits)

## TL;DR

The analytics-engine snapshot stage degraded from its expected ~70 ms warm
to **55–84 seconds**, overrunning the 60 s cycle interval and cascading
into API `canceling statement due to statement timeout` errors. Root cause
was structural — a `DISTINCT ON` query against the intraday-growing
`option_chains` table whose working set (≈ 23 GB heap + indexes) does not
fit in the database's 444 MB `shared_buffers` on slow EBS storage. No plan
choice or index fixes that.

The fix is a **maintained cache table** (`option_chains_latest`) holding
one row per `option_symbol`, populated by the ingestion writer in the
same transaction as the history write. The analytics snapshot now does a
single indexed lookup over ~2 000 rows instead of a `DISTINCT ON` over
hundreds of thousands of intraday rows. The change is feature-flagged
(`ANALYTICS_USE_LATEST_CACHE`, default off) with automatic fallback to
the legacy path on cache miss.

**Before / after, from production logs:**

| Symbol | Snapshot stage | Full cycle |
|---|---|---|
| SPX | 84.0 s → **0.48 s** (175×) | 78 s → 3.56 s |
| SPY | 55.6 s → **1.82 s** (30×) | 76 s → 5.85 s |

## Symptoms (in order of operator-visibility)

1. **Snapshot stage breakdown ballooning** in cycle-overrun warnings:
   ```
   WARNING - Calculation took 76.7s, longer than interval (60s).
   Stage timings: snapshot=69.0s, refresh_flow_caches=4.8s, ...
   ```
2. **API errors cascading from the analytics module:**
   ```
   ERROR - src.analytics.main_engine - Error fetching analytics snapshot:
   canceling statement due to statement timeout
   ```
3. **Downstream API timeouts on flow queries** sharing the same DB pool:
   `Flow contracts query timed out`, `Flow series query timed out`, etc.

The warnings started at 12:44 UTC (~14 min after market open) and the
cascade hit ~13:19 UTC.

## Root Cause

The snapshot query at `src/analytics/main_engine.py:_SNAPSHOT_QUERY`:

```sql
SELECT DISTINCT ON (oc.option_symbol)
    oc.option_symbol, oc.strike, ..., oc.timestamp
FROM option_chains oc
WHERE oc.underlying = %s
  AND oc.timestamp BETWEEN %s AND %s
  AND oc.expiration > %s
  AND oc.gamma IS NOT NULL
ORDER BY oc.option_symbol, oc.timestamp DESC
LIMIT %s
```

This produces "the latest quote per contract in the lookback window" by
sorting every matching row and deduping. The cost is dominated by the
**number of rows in the window**, not the number of contracts returned.
With ~7 k–14 k SPX option symbols and ingestion writing every minute, a
2 h window holds hundreds of thousands of rows.

**Three independent factors compounded:**

1. **Plan choice.** `EXPLAIN (ANALYZE, BUFFERS)` showed the planner picked
   a `BitmapAnd` of two large index scans
   (`idx_option_chains_timestamp` ∩ `idx_option_chains_underlying_exp_strike`),
   together touching **5.2 million SPX rows** to produce 1 960 final
   rows. Forcing `enable_bitmapscan=off` did not improve it — the
   planner ignored the "perfect" partial index
   (`idx_option_chains_underlying_ts_gamma`). This was documented in
   `main_engine.py:388-400` from the May 13 2026 incident; this is a
   known PostgreSQL planner pathology with this query shape.

2. **Memory ceiling.**
   - `option_chains` heap: 7.66 GB
   - `option_chains` total relation: 23 GB
   - `shared_buffers`: 444 MB (≈ 2% of relation)
   - `effective_cache_size`: 1.5 GB

   The planner correctly assumes the working set won't fit, but no plan
   choice fixes that.

3. **Disk speed.** 4.5 ms average per page read (≈ 10 × slower than
   gp3 should be). With the working set spilling to disk on every
   cycle, each query touched tens of thousands of pages, multiplying
   the cost.

**Intraday accumulation amplifies all three:** as the day progresses,
more rows fall inside the 2 h window, more `option_chains` UPSERTs create
dead tuples (until `autovacuum` reclaims), and the buffer pool churns
more. By midday the per-cycle cost regularly exceeded 60 s, wedging the
engine in a state where every cycle overran the next.

## Why The Obvious Fixes Don't Work

| Attempted / considered | Outcome |
|---|---|
| `VACUUM ANALYZE option_chains` | Marginal. Dropped dead-tuple % from ~10 % to 2.4 %. Did **not** restore performance because bloat was a contributor, not the root cause. |
| Force `enable_bitmapscan=off` | Did not pick the partial index; chose a different bad index (`idx_option_chains_timestamp_expiration`) with similar cost. |
| Add covering index | Already documented as ineffective in `main_engine.py:388-400` — the planner refuses to use it for this query shape. |
| Lower `ANALYTICS_SNAPSHOT_LOOKBACK_HOURS` from 2 h to 0.5 h | Useful band-aid (4× working-set reduction) but does not change cost class; would regress if chain size grows or buffer pool gets colder. |
| Size up the database | Would help (and probably should still happen — see Action Items) but kicks the can down the road without fixing the structural issue. |

## What We Did

### Layer 1 — Immediate mitigations (no code change, applied first)

These bought time during the incident but are not the durable fix:

1. `ANALYTICS_SNAPSHOT_LOOKBACK_HOURS=0.5` — shrunk rolling window 4×.
2. `ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS=150000` — prevented cycles
   from getting killed at the pool-wide 90 s ceiling and piling onto
   each other.
3. `VACUUM (ANALYZE, VERBOSE) option_chains` — cleaned up dead tuples
   accumulated from intraday UPSERTs.

### Layer 2 — Structural fix (5 commits on the branch)

The cost class of the read is the actual problem. The fix replaces the
`DISTINCT ON` over rolling history with a single indexed lookup against
a maintained cache table.

| # | Commit | What it does |
|---|---|---|
| 1 | `analytics: stagger per-symbol worker startup` | Adds `ANALYTICS_WORKER_STAGGER_SECONDS=auto`. With 3 workers on a 60 s cycle, worker `i` starts at `i * 20 s` so per-cycle snapshot queries don't all hit the DB at the same instant. |
| 2 | `schema: add option_chains_latest cache table` | New table with one row per `option_symbol`, partial index on `(underlying, timestamp DESC) WHERE gamma IS NOT NULL`. Empty on create. |
| 3 | `ingestion: dual-write to option_chains_latest` | Every batch of option-quote UPSERTs into `option_chains` is followed in the same transaction by a second UPSERT into `option_chains_latest`, keyed by `option_symbol`. Cache stays in sync with history; either both write or both roll back. |
| 4 | `analytics: feature-flag snapshot read against cache` | When `ANALYTICS_USE_LATEST_CACHE=true`, `_get_snapshot()` reads from `option_chains_latest` instead of `DISTINCT ON`-ing `option_chains`. On empty result or query error, automatic fallback to the legacy path with a logged warning — so a too-early flag flip cannot leave analytics blind. |
| 5 | `ingestion: pre-dedupe cache UPSERT by option_symbol` | Bug fix discovered live. PostgreSQL refuses to UPDATE the same conflict-target row twice in one `INSERT ... ON CONFLICT` statement; the cache UPSERT (keyed by `option_symbol` alone) trips that when a batch contains multiple timestamps for the same contract. Pre-dedupe to one row per symbol before the cache write. |

### What didn't change

- `option_chains` (history table) — still receives every quote, one row
  per `(option_symbol, timestamp)`. The full per-quote history is
  preserved; the cache is a separate small mirror, not a replacement.
- Every other query against `option_chains` (signals, walls, API
  endpoints other than `_get_snapshot`) is unchanged.
- API `gex_flip_horizon` router still calls `engine._get_snapshot()`,
  which means it automatically benefits from the cache flag.

## Activation sequence (also in `setup/database/schema.sql` header)

1. `make schema-apply` — creates the empty cache table.
2. `sudo systemctl restart zerogex-oa-ingestion` — dual-write begins.
3. Wait 5–30 min during RTH for cache to populate. Verify:
   ```sql
   SELECT underlying, count(*), max(timestamp) AT TIME ZONE 'US/Eastern'
   FROM option_chains_latest GROUP BY underlying;
   ```
4. Add `ANALYTICS_USE_LATEST_CACHE=true` to `.env`.
5. `sudo systemctl restart zerogex-oa-analytics zerogex-oa-api`.

## Timeline

| Time (ET) | Event |
|---|---|
| ~08:44 | First `Flow contracts query timed out` warnings; ingestion still running normally. |
| ~09:19 | API `canceling statement due to statement timeout` cascade begins; multiple per second. |
| ~09:29 | Cycle-overrun warnings sustained at ~76 s. Operator opens troubleshooting session. |
| 09:30-10:00 | Diagnosis: identified snapshot stage as dominant, confirmed via stage timings. |
| 10:00 | `VACUUM (ANALYZE) option_chains` run — 950 s elapsed, removed 4.26 M dead index entries. |
| 10:15-10:50 | Three `EXPLAIN (ANALYZE, BUFFERS)` runs at varying `enable_bitmapscan` settings; confirmed plan + memory + disk co-pathology. |
| 11:00 | Layer 1 mitigations applied (`ANALYTICS_SNAPSHOT_LOOKBACK_HOURS=0.5`, statement timeout, worker stagger from PR commit 1). |
| 11:30 | Schema migration applied via inline SQL (full `schema-apply` was blocked on locks). |
| 11:35 | Ingestion restarted; dual-write live. |
| 11:40 | Cache verification: ~3 600 rows total across symbols, lag < 2 min. |
| 11:41 | `ANALYTICS_USE_LATEST_CACHE=true` set; analytics restarted. **Snapshot stage drops to 0.48–1.82 s.** |
| 11:44 | Ingestion enters circuit-breaker — `ON CONFLICT DO UPDATE command cannot affect row a second time`. Bug in cache UPSERT. |
| 11:50 | Fix committed and deployed (Layer 2 commit 5). Ingestion + API restarted. |
| ~11:55 | All services clean; cache lag stable at ~20 s. **Resolved.** |

## What Worked / What Didn't

### Worked
- **Feature flag + automatic fallback.** The cache cutover was reversible
  at every step. The brief ingestion bug (commit 5 territory) only
  triggered circuit-breaker, not data loss, because the cache UPSERT
  was atomic with history in one transaction.
- **Single-table migration via inline SQL.** `make schema-apply` was
  blocked on lock contention with the busy `option_chains` table;
  applying just the new table's DDL via `psql` unblocked us.
- **Reading the EXPLAIN output before reaching for code changes.**
  The plan + buffer numbers ruled out indexing as a fix and pointed
  squarely at structural cost.

### Didn't
- **Initial assumption that vacuum + tuning would fix it.** Burned ~30
  minutes on the vacuum (which itself caused IO load while running)
  before the EXPLAIN showed the structural cost class.
- **First version of the cache UPSERT.** Did not handle multiple
  timestamps for the same contract within one batch. Regression test
  added in commit 5.
- **Forgetting to restart the API service.** After the first analytics
  cutover, the API was still running the legacy slow path because its
  PIDs predated the deploy. Required a second round of restarts.

## Lessons / Action Items

### Process
- **When stage timings show one stage dominating by 100×, suspect cost
  class, not tuning.** A 70 ms → 70 s regression on the same SQL is
  almost never bloat alone.
- **Restart every service that loads the changed module.** The API
  loads `src.analytics.main_engine` for the `gex_flip_horizon` router;
  it needs to be restarted whenever analytics code or env vars change.
- **Test the dual-write path with realistic batch shapes**, not just
  single-row or single-timestamp batches. The dedup bug would have been
  caught by a test like the one in
  `test_cache_upsert_deduped_by_option_symbol_when_batch_has_multiple_timestamps`.

### Tech (open)
- [ ] **Size up RDS.** Current 444 MB `shared_buffers` is too small for
      the 23 GB working set; the cache fix sidesteps it for the
      analytics path but every other query against `option_chains`
      still pays the cost. Suggested target: `db.m5.large` (8 GB RAM,
      ~$120/mo at us-east-2 on-demand).
- [ ] **Investigate disk performance.** 4.5 ms/page read is ~10× slower
      than gp3 should be. Could be EBS volume type (gp2 with exhausted
      burst credits?), instance-store throttling, or wider noisy-
      neighbor issues. Run a benchmark off-hours.
- [ ] **`/dev/root` at 90% full** (separate from this incident but
      surfaced during diagnosis). Investigate with
      `du -sh /var/lib/* | sort -h | tail -10` and
      `journalctl --disk-usage`.
- [ ] **Generalize the cache pattern** if other queries against
      `option_chains` start showing the same regression. The dual-write
      machinery is now in place; adding a second cache table for a
      different aggregation would be small.
- [ ] **Revert incident-mode env tweaks** when convenient:
      `ANALYTICS_SNAPSHOT_LOOKBACK_HOURS=0.5` and
      `ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS=150000`. Not breaking
      anything; just no longer needed.

### Tech (done in this work)
- [x] Worker stagger so concurrent snapshot queries don't compound DB
      contention.
- [x] Cache table + ingestion dual-write + analytics read path behind
      feature flag.
- [x] Regression test for the multi-timestamp-per-symbol batch case.
- [x] Postmortem doc (this file).

## References

- Branch: `claude/inspiring-bohr-3MwjS`
- Commits: 848ef9c, 186d9e5, 22fc7e8, d6441f7, 4f7769a
- Related prior incident: May 13 2026 (documented inline at
  `src/analytics/main_engine.py:388-400`)
- Activation steps: `setup/database/schema.sql` header comment
