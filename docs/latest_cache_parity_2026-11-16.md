# option_chains_latest parity and the ANALYTICS_USE_LATEST_CACHE decision

**Date:** 2026-11-16
**Branch:** `claude/latest-cache-current-oi`
**Context:** RDS (4 vCPU) at 97-100% CPU all afternoon. The #1 Top SQL statement
was `AnalyticsEngine._SNAPSHOT_QUERY` (the DISTINCT ON over `option_chains`):
3.32 average active sessions, 1.94 calls/s, ~1.7 s each.

## Bottom line

1. **The cache was not safe to read before this branch.** Its cumulative
   columns could only go up, across sessions: open interest stuck at the
   highest value ever seen, and volume held yesterday's total until today's
   passed it. And a contract requoted between the cycle's two reads dropped
   out of that cycle. Both are fixed here.
2. **After the fix the two paths agree exactly** in a full-session replay
   through the real ingestion and analytics code (results below): every
   contract's OI and volume, every strike, and the walls, flip and net GEX.
   In the same replay the old code showed yesterday's OI all day on every
   contract whose open interest fell overnight, and put net GEX at spot 7-15%
   too high.
3. **Recommendation.** Deploy this branch after a close without touching the
   flag, then run `python -m src.tools.latest_cache_parity` (added on this
   branch) through the next session. If the flag is off today (check with
   `grep -n '^ANALYTICS_USE_LATEST_CACHE' ~/zerogex-oa/.env`) and that run
   passes, turn it on after that session's close. If it is already on, leave
   it on: production has been reading the inflated numbers, and this deploy
   is their fix.
4. **The flag will not remove all of the #1 query.** The forced-flow
   session-surface past columns run the same SQL outside the flag, and a bug
   makes them re-run on every 30-second refresh. Measure it with the commands
   below; it is worth its own fix.

## What was wrong

### Bug 1: the cache's cumulative columns never went down

`_OPTION_LATEST_UPSERT_SQL` set `volume`, `open_interest`, `ask_volume`,
`mid_volume` and `bid_volume` to `GREATEST(stored, incoming)`. History
(`option_chains`) is keyed by `(option_symbol, timestamp)`, so the same
GREATEST there spans one minute: a retry cannot decrement, and the next
minute's row holds whatever was written. The cache keeps one row per contract
across minutes and sessions, so the same GREATEST pinned open interest to the
highest value ever seen and kept yesterday's session volume until today's
passed it.

### Bug 2: a contract requoted mid-cycle dropped out of the cache snapshot

`_get_snapshot` reads the anchor (newest `option_chains` timestamp) and then
the cache with `timestamp <= anchor`. Ingestion moves a contract's cache row to
a new minute on its first quote in that minute, so a quote landing between the
two reads replaced the row with one newer than the anchor and the bound dropped
the contract. History keeps the older row, so the DISTINCT ON path never had
this.

## The fixes

**Bug 1.** The cumulative columns now follow history's rule exactly:
`GREATEST` when the write is for the minute the cache row already holds, the
written value when it is for a later minute. An older minute still never
reaches the update (the existing `WHERE EXCLUDED.timestamp >= stored` guard).
So the cache row is, by construction, the newest history row, which is what
the DISTINCT ON returns. No schema change.

I keyed the reset on the minute rather than the 09:30 ET session boundary
first suggested, because a session-wide GREATEST still diverges from history:
a minute's open interest is the max over its buffered quotes, including the
quote carried over from the previous minute (`_prepare_option_agg`), so the
first row of a session can carry yesterday's OI, and a session-wide GREATEST
then holds it all day while history moves on a minute later. The replay below
shows exactly that. The minute rule also costs one timestamp comparison per
row instead of time-zone and holiday arithmetic on the hottest write path.

**Bug 2.** `_CACHE_ANCHOR_SLACK = timedelta(minutes=5)`; the cache read's
upper bound is `anchor + slack`. The history query is still bounded exactly at
the anchor.

Contracts that did not quote today keep yesterday's cache row, which is the
same row history's DISTINCT ON returns whenever its window reaches back that
far (the first cycle after a restart, off-hours). In steady state both paths
leave such a contract out, because both apply the same lookback window.

## Who reads the cumulative columns from the cache

| Reader | Reads OI / volume from the cache? | Effect |
|---|---|---|
| Analytics snapshot `_get_snapshot` with the flag on (also the API's forced-flow book, gamma surface and flip-horizon endpoints, which call the same method) | Yes: GEX multiplies gamma by OI; volume totals | Now current OI and today's volume |
| `src/tradeworkz/chain.py` live chain | No: symbol, expiration, strike, type | None |
| `/api/option/contract` | No: resolves the option symbol only; bars come from `option_chains` | None |
| `src/tools/capacity_check.py` | No: counts, timestamps, `updated_at` | None |
| `src/tools/market_tide_healthcheck.py` | No: `MAX(timestamp)`, existence | None |
| `src/tools/ingestion_universe_validate.py` | Yes: `oi_sum` display column (exit code depends on freshness only) | Shows current OI |

## Parity check

**What was run.** The production database is not reachable from the
development environment, so this is a replay through the real code against a
scratch Postgres 16: a SPY-like chain of 610 contracts (61 strikes, 5
expirations, calls and puts), the last hour of Thursday 2026-11-12 and all of
Friday 2026-11-13, quotes every 30 seconds, the vendor's 09:30 volume reset,
and overnight open-interest changes on most of the chain (230 contracts, 38%,
fell 5-50%). One quote stream went through the real ingestion path
(`_store_option_batch`: the per-contract accumulator, the minute rollover with
its carried-over quote, the dual write) into three copies of the tables that
differ only in the cache upsert. At 16 checkpoints from 09:31 to 15:59,
`latest_cache_parity` read both snapshot paths in one transaction and ran the
engine's own GEX code on each.

| | Old code (`release`) | Reset at the 09:30 session | Per-minute (this branch) |
|---|---|---|---|
| Checkpoints passing | 0 of 16 | 0 of 16 | **16 of 16** |
| Contracts with the wrong OI | 230 (every one whose OI fell), all day | 230, all day | 0 |
| Contracts with the wrong volume | all 610 at 09:35, still 428 at 15:59 | 0 | 0 |
| Open interest on the wrong strike | 9-11% of the total; worst strike off by 80,712 contracts | same | 0 |
| Net GEX at spot, cache vs history | 7-15% too high | same | identical |
| Call wall | wrong at 6 of 16 checkpoints | same | identical |
| Max-gamma strike | wrong at 2 of 16 | same | identical |
| Gamma flip | off by 0.03-0.39 points | same | identical |
| Put wall, max pain | matched | matched | identical |

The middle column is the session-keyed design. It fixes volume but not open
interest, for the reason given under "The fixes".

The branch's integration tests (`tests/test_option_chains_latest_upsert_parity.py`,
run with `LATEST_CACHE_PARITY_TEST_DSN` against any scratch Postgres) replay a
smaller version of this, plus a randomized replay with out-of-order minutes,
re-sent rows and volume re-anchors, and a Greeks-gap case. Three of the five
fail against the old SQL; the other two pin behavior the fix keeps.

This is synthetic data through real code. It shows the logic; the production
run below is what clears the flag.

## Production check before turning the flag on

On the box, during a regular session, after this branch is deployed:

```bash
cd ~/zerogex-oa && nohup venv/bin/python -m src.tools.latest_cache_parity --every 600 --until 16:10 > ~/latest_cache_parity.log 2>&1 &
```

It samples every symbol in `ANALYTICS_UNDERLYINGS` every 10 minutes. Each
sample reads both snapshot paths in one read-only `REPEATABLE READ`
transaction, so they see identical data, and costs one history snapshot query
per symbol. The last line is `PARITY: PASS` or `PARITY: FAIL`. A sample fails
on any contract whose OI or volume differs for a reason other than a Greeks
gap (see below), or when a wall, max-gamma strike or max pain differs, the
flip moves more than 0.1% of spot, or net GEX moves more than 1%.

**Expected, by-design differences** (reported, not failed): a contract whose
newest history row has no gamma (ingestion's Greeks gate). History then uses an
older gamma-bearing row or drops the contract; the cache keeps the newest row
with its last good Greeks. These are mostly an off-hours effect.

Two more differences the tool does not exercise: the first cycle after a
restart on stale data uses the 96-hour cold-start window on the history path
but the steady-state window on the cache path, and the cache path's slack lets
it see quotes up to five minutes newer than the anchor. Neither changes a
steady-state cycle.

## The forced-flow share of the #1 query

`/api/forced-flow/session-surface` rebuilds each past column with `_load_asof`
(`src/api/routers/forced_flow.py`), which runs `AnalyticsEngine._SNAPSHOT_QUERY`
directly. `ANALYTICS_USE_LATEST_CACHE` cannot touch it.

Those columns are cached per uvicorn worker by price grid, and the grid moves
with every cent of spot: `_quantize_band` snaps to 0.1% of the current spot,
and the default band is spot +/- 2%, so both edges scale with spot and the
cache key changes on nearly every refresh. Checked directly against the code: a
build at 14:00 ET re-ran all 21 past columns' queries after a 3-cent spot move,
and none when spot was unchanged.

The background warmer runs in each of the 4 API workers, for SPY, SPX, QQQ and
NDX, every 30 seconds from 09:30 to 16:15 ET. So each worker re-runs about
4 x (minutes since the open / 13) as-of queries per pass: about 60 per worker
per pass by mid-afternoon. If those queries take anywhere near the 1.7 s Top SQL
average, the warmers alone can keep three to four sessions busy in this
statement, which is about what Database Insights showed. That makes it likely
the largest share of the calls; how much of the CPU it is depends on how
expensive its 20-minute as-of window is next to the analytics 2-hour window.

Measure it from logs that already exist (the API logs one line per build with
its cold-bucket count; the analytics engine logs one line per snapshot):

```bash
TZ=America/New_York journalctl -u zerogex-oa-api --since "2026-11-16 13:00" --until "2026-11-16 16:00" -o cat | grep -oP '\(\K[0-9]+(?= cold buckets\))' | awk '{s+=$1} END {printf "forced-flow as-of queries: %d, %.2f per second\n", s, s/10800}'
```

```bash
TZ=America/New_York journalctl -u zerogex-oa-api --since "2026-11-16 13:00" --until "2026-11-16 16:00" -o cat | grep -c 'options with Greeks (latest-per-contract'
```

```bash
TZ=America/New_York journalctl -u zerogex-oa-analytics --since "2026-11-16 13:00" --until "2026-11-16 16:00" -o cat | grep -c 'options with Greeks (latest-per-contract'
```

The second and third counts are snapshot loads in the API and in analytics
(divide by 10800 for per second). With the flag off, each is one history query;
with it on, they read the cache instead. If the analytics engines run on the
separate engine box, run the third command there.

**Worth its own fix: yes.** Keeping the grid stable through the day (re-grid
only when spot or the day's range actually leaves the band) would cut those
queries to about one per symbol per worker every 13 minutes, the newly-past
column. It is independent of the flag and of this branch.

## Test suite

`pytest -m "not integration"` (Python 3.11, `--no-cov`), clean `release` at
`b82a4ec` vs this branch, same environment:

| | Failed | Passed | Skipped |
|---|---|---|---|
| `release` | 2 | 7408 | 6 |
| this branch | 2 | 7411 | 6 |

The failure sets are identical:
`test_api_v2_freshness_envelope.py::test_the_guide_cadence_table_matches_the_profiles`
and `test_signal_engine_log_volume.py::test_an_unchanged_playbook_card_stops_logging_at_info`
(the order-dependent one). The 13 other failures recorded in CLAUDE.md on
2026-10-01 did not occur on either side in this environment. The three new
passes are the new unit tests; the five integration tests run separately
against Postgres (above) and pass.
