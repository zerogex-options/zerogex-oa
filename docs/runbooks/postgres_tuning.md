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

* **`ALTER SYSTEM SET` is rejected by Aurora for EVERY parameter,
  not just static ones.** Even "dynamic" settings have to go
  through the cluster's **DB Cluster Parameter Group**, applied
  via AWS CLI or console.
* Dynamic params (`random_page_cost`, `effective_cache_size`,
  `effective_io_concurrency`, `work_mem`, `maintenance_work_mem`)
  apply **immediately** when modified via the cluster parameter
  group — `ApplyMethod=immediate`, no reboot. Static params
  (`shared_buffers`, `max_connections`) need `ApplyMethod=pending-reboot`
  + an actual writer reboot.
* Aurora has TWO kinds of parameter groups: **DB Parameter Group**
  (per-instance, e.g. `max_connections`) and **DB Cluster
  Parameter Group** (cluster-wide, e.g. `random_page_cost`,
  `shared_buffers`). Most tuning lives on the cluster group.
* If the cluster is using the AWS default group (e.g.
  `default.aurora-postgresql17`), AWS prohibits modifying it.
  Create a custom one first via
  `aws rds create-db-cluster-parameter-group`, then attach via
  `aws rds modify-db-cluster --apply-immediately`.
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
random reads are 4× slower than sequential. On **SSD / EBS gp3 / NVMe /
Aurora storage** the actual ratio is closer to 1.1.

The planner uses this to choose between a (random-access) Index Scan
and a (sequential) Bitmap Heap Scan or Seq Scan. The wrong default
biases it toward bigger sequential scans — exactly what we saw in
the May 21 EXPLAIN output, where the snapshot picked
`idx_option_chains_timestamp` (big time-range scan + filter) instead
of `idx_option_chains_underlying_ts_gamma` (small selective partial
index).

On Aurora (via cluster parameter group, applies immediately):
```sh
aws rds modify-db-cluster-parameter-group \
  --db-cluster-parameter-group-name zerogex-cluster-pg \
  --parameters 'ParameterName=random_page_cost,ParameterValue=1.1,ApplyMethod=immediate'
```

On vanilla Postgres:
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
2. Identify the cluster + parameter group:
   ```sh
   aws rds describe-db-clusters \
     --query 'DBClusters[*].[DBClusterIdentifier,DBClusterParameterGroup]' \
     --output table
   ```
3. If the cluster is on `default.aurora-postgresql<N>` (AWS-default,
   not modifiable), create + attach a custom one:
   ```sh
   aws rds create-db-cluster-parameter-group \
     --db-cluster-parameter-group-name zerogex-cluster-pg \
     --db-parameter-group-family aurora-postgresql17 \
     --description 'Tuned for ZeroGEX analytics workload'
   aws rds modify-db-cluster \
     --db-cluster-identifier <cluster-id> \
     --db-cluster-parameter-group-name zerogex-cluster-pg \
     --apply-immediately
   ```
4. Apply dynamic settings (`ApplyMethod=immediate` — no reboot):
   ```sh
   aws rds modify-db-cluster-parameter-group \
     --db-cluster-parameter-group-name zerogex-cluster-pg \
     --parameters \
       'ParameterName=random_page_cost,ParameterValue=1.1,ApplyMethod=immediate' \
       'ParameterName=effective_io_concurrency,ParameterValue=200,ApplyMethod=immediate' \
       'ParameterName=effective_cache_size,ParameterValue={DBInstanceClassMemory*3/4/8192},ApplyMethod=immediate'
   ```
5. Verify the writer picked up the change:
   ```sh
   make psql -c "SHOW random_page_cost; SHOW effective_io_concurrency; SHOW effective_cache_size;"
   ```
6. If `shared_buffers` also needs changing, use
   `ApplyMethod=pending-reboot` and then
   `aws rds reboot-db-instance --db-instance-identifier <writer-id>`.
7. `make analytics-snapshot-diagnose UNDERLYING=SPY` — verify the
   plan + Buffers line. Success signals:
   * Plan now uses `idx_option_chains_underlying_ts_gamma` (the
     partial index), NOT `idx_option_chains_timestamp` + Filter.
   * `Buffers: shared hit=N` dominates `read=M` once warmed
     (allow 1–2 minutes of normal traffic).
8. `make services-health` — `snapshot=…` stage timings in the
   analytics WARNING should drop to single-digit seconds on cycle 2+.

## Rollback

Set a parameter back to default via the RDS console (Reset button)
or:
```sh
aws rds reset-db-cluster-parameter-group \
  --db-cluster-parameter-group-name zerogex-cluster-pg \
  --parameters ParameterName=random_page_cost,ApplyMethod=immediate
```

The parameter-group version history is visible in the RDS console
under the parameter group's "Last edited" entries.

## Common mistake: `ALTER SYSTEM SET` on Aurora

```
zerogexdb=> ALTER SYSTEM SET random_page_cost = '1.1';
ERROR:  ALTER SYSTEM command is not supported
```

This is intentional — Aurora intercepts and rejects `ALTER SYSTEM`
because configuration is managed via parameter groups (the AWS-
managed source of truth). Don't try to work around this; just use
the AWS CLI commands above. Vanilla / self-hosted Postgres
deployments are the only place where `ALTER SYSTEM SET` works.

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
