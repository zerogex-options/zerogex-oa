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

## RDS PostgreSQL procedure (this is what ZeroGEX runs on)

ZeroGEX runs on **RDS PostgreSQL** (managed Postgres, not
Aurora). Key things to know:

* **`ALTER SYSTEM SET` is rejected on RDS PG too** — error:
  `ALTER SYSTEM command is not supported`. Config goes through the
  **DB Parameter Group** (instance-level — there are no cluster
  parameter groups on plain RDS PG, unlike Aurora).
* Dynamic params (`random_page_cost`, `effective_cache_size`,
  `effective_io_concurrency`, `work_mem`) apply within ~30 s of
  saving in the parameter group — no reboot.
* **Caveat**: if the parameter group has ANY pending static change
  (`shared_buffers`, `max_connections`, `wal_buffers`, etc.), the
  instance shows a **Pending reboot** badge AND the dynamic
  changes can be held too until the reboot completes. If
  verification shows old values, reboot the instance.
* The host's `/proc/meminfo` is NOT the DB instance's RAM — those
  are different machines. Size recommendations off the **RDS
  instance class** (db.t3.small = 2 GB, db.r5.large = 16 GB, etc.),
  not the EC2 host.

### Procedure (Console, no AWS CLI needed)

1. **Identify the attached parameter group**: RDS Console →
   Databases → click instance → **Configuration** tab → note the
   "DB parameter group" name.
2. **If on `default.postgresN`** (the AWS-default group, not
   editable): Parameter groups → **Create parameter group** →
   Engine type=PostgreSQL, Family=postgresN, name `zerogex-pg`,
   description "Tuned for ZeroGEX analytics workload" → Create.
   Then Databases → click instance → **Modify** → Database options
   → DB parameter group → select `zerogex-pg` → Continue → Apply
   immediately → Modify DB instance.
3. **Edit parameters**: Parameter groups → click your custom
   group → **Edit parameters** → use the search box:
   * `random_page_cost` → `1.1`
   * `effective_io_concurrency` → `200`
   * `effective_cache_size` → integer pages (e.g. `196608` =
     1.5 GB) if you want to nudge the planner higher
   * `shared_buffers` only if you want to override the AWS default
     (static — needs reboot)
4. **Save changes** at the bottom (the button — clicking into a
   cell only stages, Save commits).
5. **If "Pending reboot" badge appears**: Actions → **Reboot**.
   Leave "Reboot with failover" unchecked on single-instance
   setups. ~60 s downtime; services reconnect automatically via
   the pool's retry logic.
6. **Verify**:
   ```sh
   make psql -c "SHOW random_page_cost; SHOW effective_io_concurrency; SHOW effective_cache_size;"
   ```
   Expect 1.1, 200, and whatever you set for cache_size.

## Aurora-specific procedure

If you ever migrate to Aurora (or run multiple clusters), Aurora
adds a second kind of parameter group (cluster-level) on top of
the instance-level one RDS PG has:

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

**Applied 2026-05-28:** the writer was upsized **db.t3.small → db.r6g.large**
(16 GB) when this failure mode resurfaced as `/api/flow` + `/api/gex` query
timeouts during the cash session — see the **2026-05-28 resize** section at the
end of this runbook for the full before/after and the exact parameter values.
Caveat: the table's `shared_buffers` column is the **Aurora** default (~75 % of
RAM); on **RDS PG** (what we run) the default is ~25 %, so r6g.large landed at
~3.8 GB `shared_buffers`, not ~12 GB — the OS page cache holds the rest.

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

## 2026-05-28 resize: db.t3.small → db.r6g.large

Second time the "buffer pool can't hold the working set" mode bit — this time
surfacing as a steady stream of `src.api.database` WARNINGs through the cash
session:

```
Flow contracts query timed out for SPX, returning empty
GEX heatmap query timed out for SPY timeframe=5min window=270, returning empty
Flow series query timed out for SPY, returning empty
```

Those are the per-endpoint 10–15 s `asyncio.wait_for` guards in
`src/api/database.py` firing and returning `[]` — symptom, not cause.

### Root cause

Storage I/O saturation on `db.t3.small` (2 GB RAM, gp3 100 GiB / 3000 IOPS),
from two compounding instance-class limits:

1. **2 GB RAM** → page cache far too small for the 66 GB hot set; nearly every
   read missed cache and hit disk.
2. **t3 burstable EBS exhausted mid-session.** CloudWatch over the warning
   window showed the cliff: `TotalIOPS` ~3,100 → ~1,000, `ReadLatency` ~10 ms →
   30–44 ms, `DiskQueueDepth` → ~40 — the IOPS collapse landed at 16:10 UTC, the
   same minute as the first warning (12:10 ET). The gp3 volume provisions 3000
   IOPS, but the t3.small instance can't sustain pushing them (it's the
   *instance's* EBS credit that runs out, not the volume's).

Two amplifiers, each confirmed and fixed independently so the resize was
isolated to the storage tier:

* **API CFS throttling.** `zerogex-oa-api.service` ran `CPUQuota=100%` for 2
  uvicorn workers → bursty event-loop freezes (`cpu.stat` showed ~9.5 % of
  windows throttled) stacked scheduling delay on top of slow reads. Fixed:
  `CPUQuota=200%` (validated 0 % throttle under load).
* **Stale visibility map on `flow_by_contract`.** The covering-index Index-Only
  Scan was doing 116 k heap fetches — default autovacuum `scale_factor=0.2` is
  far too lazy for a table UPSERTed every ~60 s. Fixed:
  `make db-tune-flow-tables-autovacuum` (scale_factor → 0.02) + one-off
  `make db-vacuum-flow-tables`.

### Why r6g.large (16 GB), not m6g.large (8 GB)

Against the 66 GB hot table (sizing table above): 8 GB is ~9 % coverage (flow/gex
cache but get evicted by `option_chains` scans); 16 GB is ~18 % — the "cycle 2+
stays warm" entry point. `db.r6g.large` is the current-gen Graviton equivalent of
the table's `db.r5.large`, with **sustained** (non-burstable) EBS — which is what
removes the t3 cliff. r-class (more RAM per vCPU) fits a RAM/IO-bound workload
better than m-class.

### Changes applied

* Instance class `db.t3.small` → `db.r6g.large` (Modify → Apply immediately;
  single-AZ → ~5 min reboot, services reconnect via the pool retry logic).
* Param group `zerogex-pg`: `effective_cache_size` → **`1572864`** (8 kB pages =
  12 GB = 75 % of 16 GB; dynamic, no extra reboot).
* `shared_buffers` left on `{DBInstanceClassMemory/32768}` → auto-scaled to
  **3,987,104 kB ≈ 3.8 GB** (RDS PG default ≈ 25 % of usable RAM).
* `random_page_cost=1.1`, `effective_io_concurrency=200` unchanged.

### Before/after — `make flow-explain FLOW_SYMBOL=SPX`, query [5] (SPX, 24 h)

| Stage | Execution | Per-page read | Cache hits | Heap Fetches |
|---|---|---|---|---|
| t3.small, stale VM | 27.3 s | 3.3 ms | 150 | 116,295 |
| t3.small, post-vacuum | 24.3 s | 11.6 ms | 150 | 9,781 |
| **r6g.large, post-resize** | **1.46 s** | **0.34 ms** | **4,172** | 65,068 |

The middle row is the lesson: the vacuum dropped heap fetches 116 k → 9.8 k and
pages read ~4×, yet wall-clock barely moved because per-page latency had *risen*
(burst variance) — a real fix **masked by storage latency**. The resize delivered
0.34 ms/page sustained + cache hits, and the query (and the timeouts) resolved.
Planning time also fell 144 ms → 0.3 ms once catalog pages stayed cached.

### Diagnostic order (to reproduce the reasoning)

1. App CPU throttle — `cpu.stat` `nr_throttled`/`throttled_usec` delta over 60 s.
2. `make db-diagnostics` — waits / blockers / dead tuples. (Here: 21 of 31
   sessions on `DataFileRead`/`BufferIo`, **0 blocking chains** → I/O-bound, not
   locks.)
3. `EXPLAIN (ANALYZE, BUFFERS)` on the slow query — divide `I/O Timings: shared
   read` by pages read for per-page latency. **Few pages but slow per-page =
   storage tier, not a bad plan.**
4. `make flow-explain` / `make db-explain-confluence-matrix` — `Heap Fetches`
   (stale VM?) and `shared hit` vs `read`.
5. RDS Console → Monitoring — `ReadLatency`, `DiskQueueDepth`, `ReadIOPS` vs
   provisioned, `FreeableMemory`. (No `BurstBalance` graph on gp3 — that's
   expected; the burst that ran out is the instance's EBS credit.)

**Lesson:** more RAM would have *masked* the stale-VM heap fetches by caching
them — vacuum is the clean fix for the fetches, the resize is for the storage
tier. Both were needed; neither substituted for the other.
