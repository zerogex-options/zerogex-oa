# Strike-profile timeseries stampede — runbook

Captured during the Aug 21, 2026 cash-session incident: the rewind chart
went blank for every user while `make api-test` reported 83/83 endpoints
healthy. Diagnosis took several hours, almost all of it spent on things
that turned out not to be the cause. This exists so the next one takes
minutes.

## TL;DR

```sh
make services-check SINCE="1 hour ago" LEVEL=warnings   # NOT LEVEL=errors
```

If you see a stream of `Strike-profile timeseries query timed out ...
returning empty`, set the dial in `.env` and restart:

```sh
STRIKE_PROFILE_TIMESERIES_MAX_WINDOW_UNITS=24
sudo systemctl restart zerogex-oa-api
systemctl show zerogex-oa-api -p MainPID    # MUST change — see below
```

Charts come back with less history. Put it back to `480` once the
per-poll cost is actually fixed.

## Symptom, and why the obvious check misses it

`/api/gex/strike-profile-timeseries` is guarded by a 15 s
`asyncio.wait_for`. On expiry it logs a WARNING and returns `[]` — with
**HTTP 200**. So:

* `make api-test` reports every endpoint green. It does not test this
  endpoint at all, and even if it did, `[]` is a 2xx.
* `make services-check LEVEL=errors` shows nothing. These are warnings.
* Users see blank charts. Nothing in the default health surface says so.

**A green endpoint sweep does not mean the data is intact.** Several read
paths return `[]` on timeout by design; the warning log is the only place
that shows up.

## Why it does not recover on its own

A timed-out read returns `[]` *without* writing the cache — deliberate, so
a failure is never served as a cached answer. But that makes the failure
self-sustaining:

    poll -> cache miss -> expensive query -> exceeds 15 s -> [] -> nothing cached
         -> next poll -> cache miss -> ...

The 30 s TTL that exists to absorb this load can never engage, because
nothing ever completes to populate it. **Restarting does not help** — this
was confirmed twice on Aug 21, including two restarts that changed
nothing. There is no state to clear; the next poll re-enters the loop.

## Diagnosis

**1. Confirm it's this endpoint.**

```sh
make services-check SINCE="1 hour ago" LEVEL=warnings
```

**2. Read the elapsed times.** The 15 s ceiling covers the fetch only —
`wait_for` starts *after* a pool connection is in hand
(`src/api/database.py`, the `started =` comment). So:

* elapsed ≈ 15–17 s → the QUERY is over budget
* elapsed ≫ 15 s (25 s+) → pool queueing on top; you are also saturated

**3. Check whether it's CPU or I/O.** While a request is in flight:

```sh
make psql -c "SELECT pid, now()-query_start AS elapsed, wait_event_type, wait_event \
FROM pg_stat_activity WHERE state='active' AND query ILIKE '%bucket_reps%';"
```

An active backend with **empty `wait_event`** is burning CPU, not waiting
on storage or locks. On Aug 21 all 6–7 concurrent copies were CPU-bound,
which is what ruled out the buffer-pool/storage theory. Note
`db.r6g.large` is **2 vCPU** — seven CPU-bound queries there is a ~3.5x
slowdown on its own.

**4. Bisect the window.** Payload is flat at **~12,380 bytes per bucket**,
so `size_download` reads out the effective window directly:

```sh
KEY=$(grep -E '^OPS_API_KEY=' .env | head -1 | cut -d= -f2- | tr -d '"'"'"'')
for W in 10 20 24 32 40 78; do
  curl -s -o /dev/null -w "window_units=$W  http=%{http_code}  bytes=%{size_download}  %{time_total}s\n" \
    -H "X-API-Key: $KEY" \
    "http://127.0.0.1:8000/api/gex/strike-profile-timeseries?symbol=SPX&timeframe=5min&window_units=$W&expirations=all"
done
```

`bytes=2` is `[]`, i.e. it did not finish. Always include `%{http_code}`
and `%{size_download}` — a sub-millisecond row with no status is a 401,
not a fast query. (`$OPS_API_KEY` lives in `.env`; an interactive shell
has not sourced it.)

Set the dial just under the largest window that completes, with headroom.
Aug 21 measured: 24 → 5.3 s, 32 → 13.0 s, 40 and above → never finished.
32 was too close to the ceiling to be safe under load; **24** was used.

## Verify the restart actually happened

An hour was lost on Aug 21 to a `systemctl restart` that silently did not
run. Python loads source at import, so a process started before a merge is
still on the old code no matter what the working tree says.

```sh
systemctl show zerogex-oa-api -p MainPID   # before
sudo systemctl restart zerogex-oa-api
systemctl show zerogex-oa-api -p MainPID   # MUST differ
```

Cross-check the PID in the journal lines too — warnings tagged with the
pre-restart PID mean the new code is not running.

Note `TimeoutStopSec=30` is shorter than these queries, so a restart under
load takes the full 30 s and ends in SIGKILL. Expected, not an error.

## Confirming the fix

```sh
curl -s -o /dev/null -w "bytes=%{size_download}  %{time_total}s\n" -H "X-API-Key: $KEY" \
  "http://127.0.0.1:8000/api/gex/strike-profile-timeseries?symbol=SPX&timeframe=5min&window_units=78&expirations=all"
```

* bytes ≈ `12380 × cap` → the cap is live and the read completes
* single-digit ms → the cache is populating, which is the actual signal
  that the loop is broken
* `bytes=2` → still failing; lower the dial

Then `make services-check SINCE="3 minutes ago" LEVEL=warnings` should be
clean. Warnings in the first minute are the cold start.

## What this is NOT

Ruled out on Aug 21, each after real measurement. Do not re-litigate these
without new evidence:

| Suspected | Measured | Verdict |
|---|---|---|
| EC2 disk full | `make disk-clean` clears apt/snap caches; DB is separate RDS | Unrelated |
| Table bloat | `flow_by_contract` 0.4 % dead, autovacuum current | Not the cause |
| Storage I/O | 0.10 ms/block during VACUUM, 0.92 ms/block random — vs 4.5 ms/page in the May-26 incident | Adequate |
| Stale deploy | file mtime vs worker start time | Code was current |
| Retention drift | `DATA_RETENTION_DAYS=60`, as intended | Correct |
| Event-loop starvation | `/api/health/live` (DB-free) returned 0.7 ms while `/api/health` swung to 4.8 s | API process healthy |

`/api/health/live` vs `/api/health` is the cheapest discriminator you have:
if `live` is fast and `deep` is slow, the API process is fine and the
problem is past the DB connection.

## Raising the cap back

`MAX_WINDOW_UNITS` is low because a single query for the full window could
not fit inside the read's 15 s guard. Chunking removes that constraint:

```
STRIKE_PROFILE_TIMESERIES_CHUNK_UNITS=24
STRIKE_PROFILE_TIMESERIES_BUCKET_CACHE_TTL_SECONDS=900
STRIKE_PROFILE_TIMESERIES_MAX_WINDOW_UNITS=480
```

The window is then fetched as several bucket-aligned queries that each
finish, so window size no longer decides whether anything completes. Total DB
work per *cold* fetch is unchanged — this trades "never succeeds" for
"succeeds in N round trips" — but the result lands, and landing is what
populates the cache the rest of the TTL is served from.

Chunk edges are cut on real bucket timestamps from
`_strike_profile_bucket_list`, never wall-clock arithmetic, so a bucket is
always read whole. Parity with the single-query result is pinned by
`tests/test_strike_profile_timeseries_chunked_parity.py` (integration-marked;
covers chunk sizes that divide evenly, leave a remainder, and leave a
remainder of one, plus the expiration-filter and cash-index parameter-index
shifts).

**Order of operations.** Turn chunking on first, confirm the endpoint still
returns a full payload and the warnings stay quiet, then raise the cap — one
change at a time, so a regression is attributable.

**If it still cannot keep up**, the constraint is throughput, not query size:
several distinct keys (SPX/NDX x timeframes x expiration filters) each paying
a cold fetch per TTL on 2 vCPU. Raise `CACHE_TTL_SECONDS` before reaching for
an instance resize — at 5 min buckets a 30 s TTL is far shorter than the data
actually changes.

## Confirmed in production (2026-08-24)

Settings `CHUNK_UNITS=24`, `BUCKET_CACHE_TTL_SECONDS=900`,
`MAX_WINDOW_UNITS=480`, measured 40 minutes into a live cash session on the
same instance that failed on 2026-08-21:

| | 2026-08-21 (broken) | 2026-08-24 (fixed) |
|---|---|---|
| `window_units=78`, 5min, all expirations | `[]` after 15-20 s, every poll | 943,717 B in **0.049 s** |
| `window_units=47` (cold window key) | — | 560,084 B in **0.034 s** |
| API timeout warnings | 779 in 80 min | **0** |

Read the run-to-run shape, not just the best number:

```
run1 window=78  0.190s     <- second uvicorn worker still cold
run1 window=47  0.712s
run2 window=78  0.062s     <- both workers warm
run3 window=78  0.049s
```

Caches are per-worker, so with `--workers 2` it takes a couple of requests
after a restart before both are warm. A *fully* cold fetch of the 78-bucket
window (immediately after a restart, both caches empty) took **24.8 s** — four
chunks back to back. That is the worst case, it is bounded to roughly once per
worker per `BUCKET_CACHE_TTL_SECONDS`, and single-flight means concurrent
callers wait on it rather than each paying it. Note it *succeeded* at 24.8 s
despite the 15 s guard: the guard is per QUERY, and no chunk exceeded it.

## Why the full window is affordable again

Per-bucket caching is what makes a wide window cheap rather than merely
reachable. Every bucket except the newest is closed — fixed representative
timestamp, final OHLC, settled per-strike aggregate — so re-aggregating them
on every poll is pure waste. With
`STRIKE_PROFILE_TIMESERIES_BUCKET_CACHE_TTL_SECONDS` set, a poll reads the
live bucket and takes the rest from memory: O(window) becomes O(1).

The bucket key carries no window size, so windows of different widths share
the buckets they overlap on instead of each paying for a private copy.

**The live bucket is never cached and always re-read.** That is the invariant
the whole scheme rests on — a cached live bucket freezes the newest bar,
which is the one a trader is actually watching, and a parity check against
static data cannot see it. `test_the_live_bucket_is_never_served_stale`
mutates the newest bucket between passes specifically to catch that.

Three dials, three different jobs:

* **Chunking** — decides whether a query *completes* (window size vs the 15 s
  guard).
* **Per-bucket TTL** — decides what a poll *costs* once it does.
* **The cap** — the incident tourniquet. Leave it at 480 in normal operation.

## Historical: how it got here

Four changes, in the order they landed, each fixing something the previous
one did not:

1. **Single-flight** (`DatabaseManager._single_flight`, 1ff3f5a) — collapses
   concurrent callers on one key to a single read. Confirmed in production: a
   follower was observed joining an in-flight read and returning in 4.6 s
   having done no DB work of its own. Not sufficient alone, for two reasons
   worth remembering: it only dedupes *identical* keys (the frontend polls at
   least five distinct ones), and removing duplicate work cannot make one
   over-budget query cheaper.
2. **The window cap** (1d8eb15) — the tourniquet that got charts rendering
   the same afternoon, at reduced history depth.
3. **Chunking** (d0016dc) — removes window size as the thing that decides
   whether a query completes, which is what allowed the cap to come back off.
4. **Per-bucket caching** — removes the per-poll cost, which is what makes a
   full-session window affordable rather than merely possible.

The measurements that drove each step are in the sections above; the short
version is that the first three make the read *land* and only the fourth
makes it *cheap*.
