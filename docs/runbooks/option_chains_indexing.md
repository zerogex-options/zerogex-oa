# option_chains indexing — runbook

Background captured during the May 13–15, 2026 investigation into
analytics `_get_snapshot()` wedge incidents.

## TL;DR

`idx_option_chains_underlying_option_symbol_ts_gamma_covering`
(partial covering index, `WHERE gamma IS NOT NULL`) was originally
built to fix the May 13 `_get_snapshot()` wedge. **The planner does
not pick it for that query at any lookback width.** It IS picked for
per-contract LATERAL lookups and remains the canonical access path
for those — see Live users below before dropping it.

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

## Live users of `idx_option_chains_underlying_option_symbol_ts_gamma_covering`

- `src/api/database.py:_do_refresh_flow_cache()` LATERAL backfill —
  ~15s cadence under `/api/gex/contract_flow` polling. As of May
  2026, `pg_stat_user_indexes` attributed ~2.4k scans / ~115M
  tuples-read to this query alone.

Before dropping this index, audit `pg_stat_user_indexes` for fresh
scan activity and migrate any active users to an alternative plan
first — per-contract lookups otherwise regress to seq-scan or
bitmap-heap-scan of the whole window, which is orders of magnitude
slower.

## Building in production

```sh
make db-add-distinct-on-index
```

Uses `CREATE INDEX CONCURRENTLY` to avoid blocking the
`option_chains` writers. The `setup/database/schema.sql` entry
serves fresh installs and idempotent retries.
