# PostgreSQL tuning — runbook

Captured during the May 21, 2026 investigation into the analytics
`_get_snapshot` timeout wedge. The default-installed (or in our
case Aurora-default on a small instance class) Postgres is sized
for a workload with ~100 MB of hot data, not a 66 GB-table workload;
left as-is it produces exactly the failure mode in that incident:
every analytics cycle hits its `statement_timeout` because the buffer
pool can't hold the working set warm between cycles.

## TL;DR

```sh
make db-tune-suggest
```

Detects Aurora vs vanilla, prints platform-correct recommendations
(ALTER SYSTEM for dynamic params, AWS CLI parameter-group commands
for Aurora's managed-only params). Diagnostic only.

## Aurora-specific procedure

ZeroGEX runs on **Aurora PostgreSQL**, which changes a few things:

* `shared_buffers` is managed by Aurora through the cluster's **DB
  Parameter Group**. `ALTER SYSTEM SET shared_buffers = ...` does
  not work — you have to modify the parameter group and reboot the
  writer. The Aurora default is `{DBInstanceClassMemory*3/4}/8KB`
  on r5/r6 classes (≈ 75 % of RAM), reduced on t3/t4g micro classes.
* Dynamic settings (`random_page_cost`, `effective_cache_size`,
  `effective_io_concurrency`, `work_mem`, `maintenance_work_mem`)
  still work via `ALTER SYSTEM SET` + `pg_reload_conf()`. No reboot
  needed.
* The "host RAM" in our app box (`/proc/meminfo`) is NOT the Aurora
  instance's RAM — those are different machines. Size recommendations
  off the **Aurora writer's instance class**, not the EC2 host.

### Changing `shared_buffers` on Aurora

1. Find the cluster + writer + parameter group:
   ```sh
   aws rds describe-db-clusters \
     --query 'DBClusters[*].[DBClusterIdentifier,DBClusterParameterGroup]' \
     --output table

   aws rds describe-db-instances \
     --query 'DBInstances[*].[DBInstanceIdentifier,DBInstanceClass,DBParameterGroups[0].DBParameterGroupName]' \
     --output table
   ```

2. Inspect the current `shared_buffers` value:
   ```sh
   aws rds describe-db-parameters \
     --db-parameter-group-name <name-from-step-1> \
     --query "Parameters[?ParameterName=='shared_buffers']"
   ```

3. (If overriding the Aurora default) modify it. The value is in
   8 KB pages; use the Aurora-specific formula syntax to scale
   automatically with future instance class changes:
   ```sh
   aws rds modify-db-parameter-group \
     --db-parameter-group-name <name> \
     --parameters "ParameterName=shared_buffers,ParameterValue={DBInstanceClassMemory/16384},ApplyMethod=pending-reboot"
   ```

4. Reboot the writer (3–5 min downtime if no failover replica;
   instant if there is one — Aurora promotes a replica):
   ```sh
   aws rds reboot-db-instance --db-instance-identifier <writer-id>
   ```

5. Verify:
   ```sh
   make psql -c 'SHOW shared_buffers;'
   ```

### Sizing for a 66 GB working set

| Instance | RAM | Aurora-default shared_buffers | Working-set coverage |
|---|---|---|---|
| db.t3.small | 2 GB | ~500 MB | **0.7 %** — every cycle is cold |
| db.t3.medium | 4 GB | ~1 GB | 1.5 % — still mostly cold |
| db.r5.large | 16 GB | ~12 GB | ~18 % — cycle 2+ should stay warm |
| db.r5.xlarge | 32 GB | ~24 GB | ~36 % — comfortable + ingestion bursts |
| db.r5.2xlarge | 64 GB | ~48 GB | ~73 % — close to whole-table coverage |

There is no parameter tweak that gets you from "structurally cold every
cycle" to "hot cycles" without enough actual RAM. The right answer for
a 66 GB hot table is to upsize the writer; a parallel answer is to
shrink the hot table (archive rows older than 7–14 days to a separate
table that's cold-tier-only).

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

## Standard procedure (Aurora)

1. `make db-tune-suggest` — print platform-correct recommendations.
2. `make psql` — open a session against the writer.
3. Apply dynamic settings:
   ```sql
   ALTER SYSTEM SET random_page_cost = '1.1';
   ALTER SYSTEM SET effective_cache_size = '<3/4 of writer RAM>';
   ALTER SYSTEM SET effective_io_concurrency = '200';
   SELECT pg_reload_conf();
   ```
4. If `shared_buffers` also needs to change, modify the DB
   Parameter Group via AWS CLI (see above) and reboot the writer.
5. `make analytics-snapshot-diagnose UNDERLYING=SPY` — verify the
   plan + Buffers line. Success signals:
   * Plan now uses `idx_option_chains_underlying_ts_gamma` (the
     partial index), NOT `idx_option_chains_timestamp` + Filter.
   * `Buffers: shared hit=N` dominates `read=M` once warmed
     (allow 1–2 minutes of normal traffic).
6. `make services-health` — `snapshot=…` stage timings in the
   analytics WARNING should drop to single-digit seconds on cycle 2+.

## Rollback

For dynamic settings: `ALTER SYSTEM SET <name> = DEFAULT;` then
`SELECT pg_reload_conf();`.

For Aurora parameter-group settings (`shared_buffers`): revert via
`aws rds modify-db-parameter-group` setting `ParameterValue=` to the
Aurora default formula (`{DBInstanceClassMemory*3/4}/8192` for r5/r6
classes, `{DBInstanceClassMemory/16384}` for the t3/t4g classes), then
reboot.

The previous values for ALTER SYSTEM settings are preserved in
`postgresql.auto.conf` on vanilla; on Aurora the equivalent is the
parameter-group version history visible in the RDS console.

## Why `random_page_cost` matters most on a small instance

You can keep raising `ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS` to
match how slow a cold-pool cycle takes, but that's symptomatic
treatment. Two structural fixes:

* `random_page_cost=1.1` shifts the planner to use the **smaller
  selective index** (`idx_option_chains_underlying_ts_gamma`,
  ~1 GB partial) instead of the big `idx_option_chains_timestamp`
  with a post-filter. Fewer pages touched per cycle → less cold I/O
  → faster regardless of buffer-pool size. **Biggest leverage on
  any small instance** because it cuts the working set per query,
  not just the cache for it.
* `shared_buffers` sized to cover a meaningful chunk of the working
  set turns "every cycle is cold" into "cycle 1 is cold, cycles 2+
  are warm". Requires the instance to have enough RAM to begin
  with — on db.t3.small (2 GB) there's nowhere for it to go.

If you're stuck on the small instance for cost reasons, the
two-step recovery is: (1) `random_page_cost=1.1` to cut per-cycle
work, then (2) `ANALYTICS_SNAPSHOT_LOOKBACK_HOURS=0.25` to cut it
further still.
