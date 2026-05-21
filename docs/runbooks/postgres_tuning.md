# PostgreSQL tuning — runbook

Captured during the May 21, 2026 investigation into the analytics
`_get_snapshot` timeout wedge. Default-installed PostgreSQL is sized
for a laptop, not a 65 GB-table workload; left untuned it produces
exactly the failure mode in that incident: every analytics cycle hits
its `statement_timeout` because the buffer pool can't hold the working
set warm between cycles.

## TL;DR

```sh
make db-tune-suggest
```

Prints recommended `ALTER SYSTEM SET` commands sized to host RAM and
actual table footprint. Diagnostic only — review then run.

## Settings that matter for this workload

### `shared_buffers` (the big one)

Postgres-internal page cache. Default `128MB` is wrong for any
serious workload — the OS page cache picks up the slack but with
extra copy overhead. Recommended: **~25 % of host RAM**, capped at
~8 GB on hosts ≤ 32 GB.

Why it matters for the analytics snapshot:
* `option_chains` is ~65 GB (heap + indexes).
* Default `shared_buffers=128MB` ≈ **0.2 %** of that.
* Each `_get_snapshot` cycle reads ~50–100 k pages from disk
  (~ 10 ms/page on EBS gp3 cold) = 60–120 s of pure I/O per cycle.
* Three concurrent analytics workers querying different underlyings
  evict each other's warm pages every cycle → cycle 2 is just as
  cold as cycle 1.
* Bumping to 25 % of RAM lets the working set stay resident
  across cycles → cycle 2+ drops to single-digit seconds.

`shared_buffers` is the only one of these that requires a **restart**
(it's allocated at postmaster startup):

```sql
ALTER SYSTEM SET shared_buffers = '8GB';
```
```sh
sudo systemctl restart postgresql
make psql -c 'SHOW shared_buffers'   # verify
```

### `effective_cache_size`

Planner estimate of how much filesystem cache + `shared_buffers` is
available for query execution. **Does NOT actually allocate memory** —
it only nudges the planner toward index scans when it believes the
relevant pages are likely cached. Recommended: **~75 % of host RAM**.

Setting too low makes the planner prefer sequential scans over index
scans (assumes nothing's cached). Default `4GB` is fine on a 16 GB
host; bump it proportionally on bigger boxes.

```sql
ALTER SYSTEM SET effective_cache_size = '24GB';
SELECT pg_reload_conf();   -- reloads, no restart
```

### `random_page_cost`

Planner's cost estimate for a random-access page read, relative to
`seq_page_cost=1.0`. Default `4.0` assumes a spinning disk where
random reads are 4× slower than sequential. On **SSD / EBS gp3 / NVMe**
the actual ratio is closer to 1.1.

The planner uses this to choose between a (random-access) Index Scan
and a (sequential) Bitmap Heap Scan or Seq Scan. The wrong default
on SSD biases it toward bigger sequential scans — exactly what we
saw in the May 21 EXPLAIN output, where the snapshot picked
`idx_option_chains_timestamp` (big time-range scan + filter) instead
of `idx_option_chains_underlying_ts_gamma` (small selective partial
index).

```sql
ALTER SYSTEM SET random_page_cost = '1.1';
SELECT pg_reload_conf();
```

Sanity-check the impact by re-running the diagnostic:

```sh
make analytics-snapshot-diagnose UNDERLYING=SPY
```

If the plan shifts to `Index Scan using idx_option_chains_underlying_ts_gamma`
and the Buffers line shows mostly `shared hit` (vs. `read`), the new
cost model is working.

### `work_mem`

Per-sort, per-hash-node memory before spilling to disk. Default `4MB`
is enough for trivial queries but causes the analytics DISTINCT ON
sort to spill if the working set exceeds it. The cost is multiplied
by every concurrent sort/hash node, so don't set it too high.

Recommended: **~RAM / 200** (e.g. 32 GB host → 160 MB work_mem).

```sql
ALTER SYSTEM SET work_mem = '160MB';
SELECT pg_reload_conf();
```

### `effective_io_concurrency`

How many concurrent I/O operations the storage can absorb. Default `1`
assumes a single spindle. EBS gp3 sustains 16k IOPS and benefits from
**200**; NVMe goes higher.

```sql
ALTER SYSTEM SET effective_io_concurrency = '200';
SELECT pg_reload_conf();
```

Mainly affects bitmap heap scans (lets the kernel prefetch).

### `maintenance_work_mem`

Memory for VACUUM, CREATE INDEX, ALTER TABLE ADD FOREIGN KEY. Default
`64MB` makes index rebuilds and big VACUUMs unnecessarily slow.
Recommended: **~RAM / 16, capped at 2 GB**.

```sql
ALTER SYSTEM SET maintenance_work_mem = '2GB';
SELECT pg_reload_conf();
```

## Standard procedure

1. `make db-tune-suggest` — print recommendations.
2. `make psql` — open a session.
3. Run the `ALTER SYSTEM SET` commands you want.
4. `SELECT pg_reload_conf();` — picks up everything except
   `shared_buffers`.
5. `sudo systemctl restart postgresql` if `shared_buffers` was
   changed.
6. Re-run `make analytics-snapshot-diagnose UNDERLYING=SPY` to
   verify the plan + Buffers line. The key signal of success is
   `Buffers: shared hit=N` dominating `read=M` once the pool has
   warmed (1–2 minutes of normal traffic).
7. `make services-health` — confirm `snapshot=…` stage timings in
   the analytics WARNING are now sub-second on cycle 2+.

## Rollback

`ALTER SYSTEM SET <name> = DEFAULT;` then `SELECT pg_reload_conf();`
(or restart for `shared_buffers`). The previous values are also
preserved in `postgresql.auto.conf` — back it up before changes if
you want a literal-restore option.

## Why `shared_buffers` instead of just bigger statement_timeouts

You can keep raising `ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS` to
match how slow a cold-pool cycle takes, but that's symptomatic
treatment: every cycle stays cold, every cycle takes minutes, and
downstream consumers (signals engine, API flow_series) see stale
data the whole time. Sizing `shared_buffers` correctly turns
"every cycle is a cold cycle" into "cycle 1 is cold, cycles 2+ are
fast" — and lets you put `ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS`
back to a tight ceiling, where it belongs.
