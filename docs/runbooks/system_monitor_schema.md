# system-monitor state file — schema (v2)

Reference for the JSON written by
[`src/tools/system_monitor.py`](../../src/tools/system_monitor.py),
driven once per minute by `zerogex-oa-system-monitor.timer`. Intended
audience: frontend / dashboard developers consuming the file directly,
plus operators inspecting state by hand.

**Location on the production host:** `~ubuntu/monitoring/state.json`
**Update cadence:** rewritten every 60 s
**Atomicity:** written via temp-file + `rename(2)`, so a reader never
sees a partial JSON document — open + parse is safe at any time
(including mid-tick).

---

## Top-level shape

```jsonc
{
  "version": 2,
  "last_sample_iso": "2026-05-26T11:34:15-04:00",
  "last_sample_ts":  1748277255.0,
  "last_cpu_stat":   { /* internal — ignore */ },
  "hourly": [ /* hourly buckets, oldest→newest, retention 720 (30 days) */ ],
  "daily":  [ /*  daily buckets, oldest→newest, retention 90 days     */ ]
}
```

| Field | Type | Meaning |
|---|---|---|
| `version` | int | Schema version. Bumps on shape changes. Currently `2`. |
| `last_sample_iso` | string (ISO-8601) | Wall-clock of the last tick, in the host's local TZ (ET on prod). |
| `last_sample_ts` | float | Unix epoch seconds equivalent of `last_sample_iso`. |
| `last_cpu_stat` | object | **Internal** — carries `/proc/stat` counters between ticks so the next CPU% can be a real delta. Frontends should not read this. |
| `hourly` | array | One entry per hour, sorted **oldest → newest**. The trailing entry is the currently-open hour and is rewritten every minute. |
| `daily` | array | Same, per day. |

---

## Bucket shape (identical for `hourly[]` and `daily[]`)

```jsonc
{
  "bucket_start": "2026-05-26T11:00:00-04:00",
  "metrics": {
    "cpu_pct":             { "max": 78.2, "avg": 45.1 },
    "mem_pct":             { "max": 62.0, "avg": 58.9 },
    "cycle_time_s":        { "max": 4.12, "avg": 3.48, "median": 3.51, "count": 1820 },
    "disk_root_pct":       { "latest": 90.0 },
    "disk_var_log_pct":    { "latest": 13.0 },
    "errors_by_service":   { "zerogex-oa-ingestion": 0, "zerogex-oa-analytics": 1,
                             "zerogex-oa-signals": 0,   "zerogex-oa-api": 0 },
    "warnings_by_service": { "zerogex-oa-ingestion": 2, "zerogex-oa-analytics": 0,
                             "zerogex-oa-signals": 1,   "zerogex-oa-api": 3 },
    "sample_count": 47
  },
  "samples": [ /* internal — only present on the trailing entry, ignore */ ]
}
```

### Per-field reference

| Path | Type | Unit | Meaning |
|---|---|---|---|
| `bucket_start` | string (ISO-8601) | local TZ | Start of the hour (`HH:00:00`) or day (`00:00:00`). Use as the x-axis label. |
| `metrics.cpu_pct.max` | float \| null | % (0–100) | Peak CPU utilisation across the minute samples in this bucket. |
| `metrics.cpu_pct.avg` | float \| null | % (0–100) | Mean CPU utilisation. |
| `metrics.mem_pct.max` | float \| null | % (0–100) | Peak memory utilisation (`1 − MemAvailable/MemTotal`). |
| `metrics.mem_pct.avg` | float \| null | % (0–100) | Mean memory utilisation. |
| `metrics.cycle_time_s.max` | float \| null | seconds | Longest analytics cycle (`Stage timings (total Xs)`) seen in the bucket. |
| `metrics.cycle_time_s.avg` | float \| null | seconds | Mean cycle time. |
| `metrics.cycle_time_s.median` | float \| null | seconds | Median cycle time. |
| `metrics.cycle_time_s.count` | int | n | Number of analytics cycles observed (≠ `sample_count`; many cycles per minute). |
| `metrics.disk_root_pct.latest` | float \| null | % (0–100) | Most recent `df` reading for `/`. |
| `metrics.disk_var_log_pct.latest` | float \| null | % (0–100) | Most recent `df` reading for `/var/log`. |
| `metrics.errors_by_service.<svc>` | int | count | Total ` - ERROR - ` log lines from that systemd unit in the bucket. |
| `metrics.warnings_by_service.<svc>` | int | count | Total ` - WARNING - ` log lines. |
| `metrics.sample_count` | int | n | Number of 1-minute monitor ticks folded into this bucket (≤ 60 hourly, ≤ 1440 daily). |

### Service dimension keys

`errors_by_service` and `warnings_by_service` always carry exactly
these four keys, even when the count is zero — so the dashboard sees
a stable categorical dimension:

- `zerogex-oa-ingestion`
- `zerogex-oa-analytics`
- `zerogex-oa-signals`
- `zerogex-oa-api`

### Null semantics

- Any numeric metric may be `null` when no underlying reading was
  collectable in the bucket (journalctl permission denied for a tick,
  `df` transient failure, etc.). Render as **gap** in a chart, **not**
  as `0`.
- `cycle_time_s.{max,avg,median}` are `null` with `count: 0` when no
  analytics cycles ran in the bucket — normal off-hours / weekends.
- `disk_*_pct.latest` is `null` only if every tick in the bucket
  failed to read `df`. Otherwise it's the most recent non-null
  reading (transient failures don't blank the bucket).

---

## Consumption patterns

**Current-status panel** (single most-recent reading):

```js
const live = state.hourly.at(-1).metrics;
// → live.cpu_pct.max, live.cycle_time_s.median, live.disk_root_pct.latest, …
```

**Last 24 h of CPU max** (line chart):

```js
state.hourly.slice(-24).map(b => ({
  x: b.bucket_start,
  y: b.metrics.cpu_pct.max,
}));
```

**Per-engine error stacked bars over the last 7 days**:

```js
state.daily.slice(-7).flatMap(b =>
  Object.entries(b.metrics.errors_by_service).map(([engine, count]) => ({
    x: b.bucket_start,
    engine,
    count,
  }))
);
```

**Cycle-time distribution band** (max/median fill, line for avg):

```js
state.hourly.map(b => ({
  x: b.bucket_start,
  high: b.metrics.cycle_time_s.max,
  mid:  b.metrics.cycle_time_s.avg,
  low:  b.metrics.cycle_time_s.median,
}));
```

---

## Suggested thresholds (for in-chart annotations / alert badges)

| Metric | Watch | Investigate |
|---|---|---|
| `cpu_pct.max` | > 70 | > 85 |
| `mem_pct.max` | > 80 | > 90 |
| `disk_*_pct.latest` | > 85 | > 92 |
| `cycle_time_s.max` | > 6 s | > 10 s |
| `errors_by_service.*` | any > 0 within market hours | any > 5 in a single hour |

---

## Schema version history

| Version | Date | Change |
|---|---|---|
| 1 | 2026-05-26 | Initial release. Disk metrics carried `{max, avg}`. |
| 2 | 2026-05-26 | `disk_*_pct` simplified to `{latest}` only. v1 buckets are auto-migrated on load in `src/tools/system_monitor.py:_migrate_state_in_place` — frontends never see mixed shapes. |

## Operator cheatsheet

```bash
# Pretty-print the latest hourly + daily summary
make system-monitor-show

# Dump the entire state file for piping into jq
make system-monitor-show-json | jq '.hourly[-1].metrics'

# Force a tick (don't wait for the timer)
sudo systemctl start zerogex-oa-system-monitor.service

# Timer status + recent log
make system-monitor-status
```
