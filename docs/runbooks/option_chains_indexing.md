# option_chains indexing — runbook

Background captured during the May 13–15, 2026 investigation into
analytics `_get_snapshot()` wedge incidents, plus the May 23 drop
of two confirmed-dead indexes.

## TL;DR

Two indexes were dropped on **May 23, 2026** after live audit confirmed
both were dead:

| Index | Size | Reason |
|-------|------|--------|
| `idx_option_chains_underlying_option_symbol_ts_gamma_covering` | 21 GB | Last scan May 15; planner always picked the non-partial sibling `idx_option_chains_underlying_option_symbol_timestamp` for the LATERAL lookup it was designed for. INCLUDE list missing `ask_volume`/`bid_volume`/`mid` precluded Index Only Scan. |
| `idx_option_chains_underlying_timestamp_option_symbol`         | 3.5 GB | +3 scans in 17h vs. 2.7M scans on the sibling `idx_option_chains_underlying_timestamp` with the same `(underlying, timestamp DESC)` prefix. The trailing `option_symbol` column gave no measurable selectivity benefit. |

Combined reclaim: **~24 GB** + per-row write overhead on every ingestion
UPSERT. See "Audit & drop — May 23, 2026" below for the evidence trail.

## What `_get_snapshot` actually does

Three sequential queries (intentional — combining them as one CTE
forces the planner to treat the latest-ts as unknown at plan time):

1. Latest option-chain timestamp for the underlying.
2. Underlying close at or before that timestamp.
3. `DISTINCT ON (option_symbol)` walk over the lookback window with
   `ORDER BY option_symbol, timestamp DESC` and an expiration cutoff.

Plan choice for query #3 is left to the optimizer:

| Lookback | Plan | Wall (warm) |
|----------|------|-------------|
| 2h (steady-state) | Index Scan + in-memory quicksort | ~70 ms |
| 96h (cold-start)  | Parallel Bitmap Heap Scan + external merge sort | ~40 sec |

The cold-start path can blow past the pool's 90s `statement_timeout`
when the buffer pool is cold (just after autovacuum eviction).

## What we tried that didn't work

- **Building the partial covering index** — verified via EXPLAIN
  ANALYZE, both with the index present and after `DROP INDEX` inside
  a rolled-back transaction: identical plans, ~55s warm at 96h
  lookback. The bitmap-heap-scan wins the cost model regardless.
- **LATERAL rewrite of `_get_snapshot()`** — prototyped and rejected.
  Regressed the 2h steady-state path 5x (354ms vs 70ms) for a 25%
  improvement on the 96h cold-start (29s vs 38s). Net negative.

## What actually fixed the wedge

- `ANALYTICS_SNAPSHOT_LOOKBACK_HOURS=2` for steady-state cycles.
- `ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS=96` one-shot first
  cycle on process start (consumed flag prevents retry loop).
- `DB_STATEMENT_TIMEOUT_MS=90000` pool-level backstop.

See `src/analytics/main_engine.py:_get_snapshot()`.

## Per-contract LATERAL lookups (post-May-23)

`src/api/database.py:_do_refresh_flow_cache()`'s LATERAL backfill —
~15s cadence under `/api/gex/contract_flow` polling — is now served
by the non-partial sibling
`idx_option_chains_underlying_option_symbol_timestamp` (same key,
no `WHERE gamma IS NOT NULL`, no `INCLUDE` list). EXPLAIN on
2026-05-23 confirmed this is the plan choice; idx_scan on that
sibling stood at ~200M lifetime, vs. 2,447 (frozen since May 15) on
the dropped covering index. Heap fetches per row are unchanged from
the pre-drop state because the dropped index never achieved Index
Only Scan anyway (missing `ask_volume`/`bid_volume`/`mid` from its
INCLUDE list).

## Auditing & pruning (May 21, 2026)

The May 21 incident — cycle-after-cycle `_get_snapshot` timing out
at the configured `statement_timeout` even after restart — traced
to a buffer pool too small for the working set (default
`shared_buffers=128MB` against ~65 GB of table + indexes). Two
related findings from the audit:

1. **`idx_option_chains_underlying` is effectively dead.** 35 scans
   observed (lifetime of `pg_stat_user_indexes`), 359 MB on disk.
   Every query that filters by `underlying` has a composite index
   whose key starts with `underlying` — the single-column variant
   carries write amplification with no read benefit. Drop with
   `make db-drop-underused-option-chains-idx CONFIRM=yes`.

2. **The 19 GB covering index is over-engineered for its actual
   user.** `idx_option_chains_underlying_option_symbol_ts_gamma_covering`
   was built to convert `_do_refresh_flow_cache`'s LATERAL backfill
   into an Index Only Scan, but its `INCLUDE` list lacks
   `ask_volume`, `bid_volume`, and `mid` — three columns the
   backfill SELECTs — so the planner still has to fetch the heap
   anyway. Meanwhile `idx_option_chains_underlying_option_symbol_timestamp`
   (2.7 GB, 185 M scans) serves the same key with one heap fetch
   per row, and is dominantly preferred by the planner.
   **Resolved: dropped on May 23, 2026** — see next section.

3. **The 3.3 GB `idx_option_chains_underlying_timestamp_option_symbol`
   is redundant.** Same `(underlying, timestamp DESC)` prefix as
   `idx_option_chains_underlying_timestamp` (1 GB), but the latter
   is used ~400× more often. The trailing `option_symbol` key column
   adds no measurable selectivity for the query shapes the planner
   sees. **Resolved: dropped on May 23, 2026.**

### Running the audit

```sh
make db-index-audit                    # default TABLE=option_chains
make db-index-audit TABLE=signal_scores
```

Output ranks indexes by scan count and surfaces `tuples_per_scan`
(planner-doing-big-scans signal) plus a bloat estimate. Always
sanity-check `pg_stat_statements` for the SPECIFIC query patterns
hitting a candidate before dropping.

## Audit & drop — May 23, 2026

Read-only audit run via `make db-drop-candidate-audit` (script:
`setup/database/diagnostics/drop_candidate_audit.sql`) confirmed
both indexes flagged by the May 21 review were dead:

- Top-20 `pg_stat_statements` for `option_chains` showed no query
  whose plan would prefer either suspect.
- `EXPLAIN` on five representative shapes (LATERAL per-contract
  lookup, range scan, `_get_snapshot` DISTINCT ON, quote-endpoint
  control, per-contract history) showed the planner picking either
  the healthy sibling or a different access path in every case.
- Live `pg_stat_user_indexes` counters: covering index frozen at
  2,447 scans (last scan **May 15**); ts_option_symbol moved from
  6,878 → 6,881 in 17 h (+0.04%, indistinguishable from noise).

Drops executed via:

```sh
make db-drop-distinct-on-index CONFIRM=yes
make query SQL="DROP INDEX CONCURRENTLY idx_option_chains_underlying_timestamp_option_symbol;"
```

Post-drop footprint: 67 GB → **46 GB** total table size (heap 7.6 GB
+ remaining indexes 39 GB). 14 indexes remain.

## Buffer pool sizing

The query-plan choice (bitmap heap scan vs. index scan) is
half the picture; the other half is whether the chosen pages
stay in cache between cycles. The May 21 audit showed
`shared_buffers=128MB` against 65 GB of working set —
~0.2 % cache coverage — so every analytics cycle re-read pages
from disk at EBS-typical ~10 ms/page. With three analytics
workers querying different underlyings, the buffer pool churned
constantly and no cycle ever found a warm pool.

```sh
make db-tune-suggest
```

Computes recommended `shared_buffers`, `effective_cache_size`,
`work_mem`, `maintenance_work_mem`, `random_page_cost`, and
`effective_io_concurrency` from `/proc/meminfo` + actual table
sizes. Diagnostic only — prints the `ALTER SYSTEM SET` commands.

`shared_buffers` requires a postgres restart; the rest reload via
`SELECT pg_reload_conf();`. See
`docs/runbooks/postgres_tuning.md` for the full procedure.
