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

## The real fix

The cap is a tourniquet. The endpoint recomputes the entire window on every
poll, but only the newest bucket can have changed — at `window_units=78`
and 5 min buckets, that is 6.5 hours of settled, immutable history
re-aggregated once per second per client.

Two changes exist and are not enough on their own:

* **Single-flight** (`DatabaseManager._single_flight`, 1ff3f5a) dedupes
  concurrent callers on one key. It works — followers were observed
  joining an in-flight read and returning with no DB work — but it only
  dedupes *identical* keys, and the frontend polls at least five distinct
  ones (SPX/NDX × 5min/1min × expiration filters).
* **The window cap** (1d8eb15) keeps each query inside the ceiling so
  *something* completes and caches.

The repair is per-bucket caching: cache each closed bucket under
`spt:{symbol}:{timeframe}:{scope}:{bucket_ts}` with a long TTL (they are
immutable), then serve a request from cached buckets and query only the
live one. Cost per poll goes O(window) → O(1), and window fragmentation
disappears because a 78-bucket and a 72-bucket request share 72 entries.

It needs the read split into a cheap bucket-list query plus an
explicit-bucket fetch, preserving the LATERAL fence, the optimisation-fence
CTEs, and the `ORDER BY` contiguity the wall fold depends on — so it wants
`tests/test_strike_profile_timeseries_expiration_sum.py` (integration-
marked, needs a scratch Postgres DSN) run for parity. Not a mid-session
change.
