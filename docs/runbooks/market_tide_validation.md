# Market Tide Validation

Market Tide is a cross-symbol composite of `gex_summary`, `option_chains`, and
`flow_contract_facts`. The reading is **persisted** to `market_tide_snapshots`
and served as a pure cache read by `/api/flow/market-tide` — it is no longer
recomputed inline on every request.

- A refresh job (`src/tools/market_tide_refresh.py`, wired to the
  `zerogex-oa-market-tide-refresh` timer) writes one snapshot per window every
  5 minutes through the cash session and freezes the final row at the 16:00 ET
  close.
- A backfill (`src/tools/market_tide_backfill.py`) seeds prior sessions from
  retained history.
- The endpoint returns the most recent snapshot per window, so the metric
  stays populated after hours and over weekends (users see the previous
  session, frozen at its close) instead of collapsing to `insufficient_data`.

Only **publishable** readings are stored: the universe is active symbols that
have both chain and GEX data (contract-root aliases excluded), and a reading is
publishable when at least 60% have a GEX snapshot and option-chain heartbeat
within ten minutes of the common data anchor. The anchor is frozen at 16:00 ET
after the cash close so the GEX and flow windows describe one comparable
session. If the snapshot table is empty (a fresh install before the first
refresh/backfill), the endpoint falls back to a live compute, which can be
`insufficient_data` outside the session — run the backfill to fix that.

## 1. Call the API directly

Test the backend before debugging the web page:

```bash
curl -sS \
  -H "X-API-Key: $API_KEY" \
  "https://YOUR_API_HOST/api/flow/market-tide?window=15" | python -m json.tool
```

Interpret the result:

- HTTP 200 with a numeric `score`: the backend is working; inspect the web
  client's API base URL, API key/scopes, and response handling.
- HTTP 200 with `score: null` and `label: insufficient_data`: no publishable
  snapshot exists yet and the live fallback is below 60% participation. Expected
  only before the first `make market-tide-backfill` on a fresh install (or if
  the refresh timer is disabled); once snapshots exist the endpoint serves the
  last publishable reading frozen at the close instead. Run the snapshot
  healthcheck in Section 4.
- HTTP 401/403: the key is absent or lacks the options-flow scope.
- HTTP 500: inspect the API log and run the database healthcheck below.
- HTTP 404: the deployed API version does not contain the Market Tide route or
  the frontend is pointed at the wrong backend. Note the route lives under the
  `/api/flow/` prefix so it is proxied by the web BFF's `/api/flow/[...rest]`
  handler; a top-level `/api/market-tide` path has no BFF route and 404s at the
  Next.js layer before ever reaching this backend.

Also verify every supported window:

```bash
for window in 5 15 30 60; do
  curl -fsS -H "X-API-Key: $API_KEY" \
    "https://YOUR_API_HOST/api/flow/market-tide?window=$window" \
    | python -c 'import json,sys; d=json.load(sys.stdin); print(d["score"], d["label"], d["participation_pct"])'
done
```

## 2. Run the database healthcheck

Run this on the API host with the normal ZeroGEX database environment loaded:

```bash
python -m src.tools.market_tide_healthcheck
```

For machine-readable output:

```bash
python -m src.tools.market_tide_healthcheck --json
```

The command reports the common anchor and, for every active symbol:

- latest option-chain heartbeat;
- latest GEX timestamp;
- latest sparse flow fact;
- 30-day flow and gamma normalization sample counts; and
- a readiness status such as `ready`, `missing_flow_input`, `missing_gex`, or
  `stale_or_misaligned`.

Exit code 0 means participation is at least 60%, exit code 1 means the metric
is not publishable, and exit code 2 means the database query failed.

## 3. Understand quiet symbols

`flow_contract_facts` is sparse and only receives a row when option volume
changes. Market Tide uses the newest of the latest chain snapshot and latest
classified flow fact as proof that the flow input is alive. This lets a recent
fact cover an ETF latest-cache pause near the close, while the chain timestamp
still keeps a fresh, quiet symbol eligible with zero current flow.

## 4. Backfill and refresh

Market Tide is persisted to `market_tide_snapshots` and served as a cache read,
so both a one-time backfill and an ongoing refresh must be in place.

**Seed prior sessions (one-time, after `make schema-apply`):**

```bash
make market-tide-backfill                          # last 90 days, one row per 16:00 ET close
make market-tide-backfill MARKET_TIDE_CADENCE=5    # also fill the intraday series
make market-tide-backfill MARKET_TIDE_START=2026-06-01 MARKET_TIDE_END=2026-06-30
```

Backfill reconstructs each session from retained `option_chains` /
`gex_summary` / `flow_contract_facts` history (pruned at `DATA_RETENTION_DAYS`
≈ 90d, which bounds how far back it can reach). It is idempotent and stores only
publishable readings, so thin historical days are simply skipped. In-progress
days are skipped too (the 16:00 close anchor is not yet fresh), so it is safe to
run any time — the refresh timer captures today.

**Keep it live (ongoing):**

```bash
make market-tide-refresh-install   # enable the every-5-min cash-session timer
make market-tide-refresh-status    # timer state + last/next fire + recent log
make market-tide-refresh           # run one refresh by hand (RTH-gated; add --force to override)
```

The refresh self-gates to 09:30–16:10 ET; outside the session it is a no-op, so
the every-5-minute timer only does real work during the regular session.

**Verify the table is populated:**

```bash
make market-tide-snapshot-healthcheck              # exit 0 = every window has a snapshot; 1 = run the backfill
python -m src.tools.market_tide_snapshot_healthcheck --json
```

`market_tide_healthcheck` (Section 2) inspects the *upstream inputs* that decide
whether a new reading can publish; `market_tide_snapshot_healthcheck` inspects
the *persisted output*. Both are useful: the former explains a low-participation
session, the latter confirms the backfill/refresh actually wrote rows.

Note: the normalizer scales the reading uses (`component_normalizer_cache`,
`gex_historical_stats`) are the current nightly caches, so a backfilled
historical reading uses the as-of-now 30-day scale — an accepted approximation
shared with the live path.

### Prerequisites for a fresh reading

For a *new* reading to be publishable at all (live or backfilled):

1. The option-chain ingester must be writing `option_chains` snapshots.
2. The analytics engine must be writing `gex_summary` rows.
3. `flow_contract_facts` should contain trade-derived deltas. A quiet interval
   may contain no facts and is still valid (the chain heartbeat keeps a quiet
   symbol eligible).

The `flow_by_contract_backfill` and `flow_series_5min_backfill` tools do not
create missing `flow_contract_facts`; they roll up facts that already exist. If
raw snapshots exist but facts are absent, repair/replay the canonical flow-fact
materialization first, then re-run `make market-tide-backfill`.

## 5. Quick database checks

```sql
SELECT symbol, is_active FROM symbols ORDER BY symbol;

SELECT underlying, MAX(timestamp) AS latest_chain
FROM option_chains_latest
GROUP BY underlying
ORDER BY underlying;

SELECT underlying, MAX(timestamp) AS latest_gex
FROM gex_summary
GROUP BY underlying
ORDER BY underlying;

SELECT symbol, MAX(timestamp) AS latest_flow_fact, COUNT(*) AS facts_today
FROM flow_contract_facts
WHERE timestamp >= CURRENT_DATE
GROUP BY symbol
ORDER BY symbol;

-- Persisted snapshots the endpoint actually serves (latest per window).
SELECT window_minutes,
       MAX(snapshot_ts) AS latest_snapshot,
       COUNT(*) AS rows
FROM market_tide_snapshots
GROUP BY window_minutes
ORDER BY window_minutes;
```

An active registry entry with no corresponding chain/GEX feed reduces
participation. Mark unsupported symbols inactive or restore their ingestion;
do not lower the 60% threshold merely to hide a broken universe.
