# Market Tide Validation

Market Tide is calculated on demand from `gex_summary`,
`option_chains_latest`, and `flow_contract_facts`. It does not have its own
table, job, or backfill. A current score becomes publishable when at least 60%
of active symbols have a GEX snapshot and option-chain heartbeat within ten
minutes of the common data anchor.

## 1. Call the API directly

Test the backend before debugging the web page:

```bash
curl -sS \
  -H "X-API-Key: $API_KEY" \
  "https://YOUR_API_HOST/api/market-tide?window=15" | python -m json.tool
```

Interpret the result:

- HTTP 200 with a numeric `score`: the backend is working; inspect the web
  client's API base URL, API key/scopes, and response handling.
- HTTP 200 with `score: null` and `label: insufficient_data`: the endpoint is
  healthy, but upstream participation is below 60%.
- HTTP 401/403: the key is absent or lacks the options-flow scope.
- HTTP 500: inspect the API log and run the database healthcheck below.
- HTTP 404: the deployed API version does not contain the Market Tide route or
  the frontend is pointed at the wrong backend.

Also verify every supported window:

```bash
for window in 5 15 30 60; do
  curl -fsS -H "X-API-Key: $API_KEY" \
    "https://YOUR_API_HOST/api/market-tide?window=$window" \
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
- a readiness status such as `ready`, `missing_chain`, `stale_chain`,
  `missing_gex`, or `stale_gex`.

Exit code 0 means participation is at least 60%, exit code 1 means the metric
is not publishable, and exit code 2 means the database query failed.

## 3. Understand quiet symbols

`flow_contract_facts` is sparse and only receives a row when option volume
changes. The absence of a recent fact can legitimately mean zero flow. Market
Tide therefore uses `option_chains_latest.timestamp` as the flow-feed heartbeat
and treats a fresh, quiet symbol as zero current flow. Do not use the timestamp
of the last flow fact as a feed-health test.

## 4. Can Market Tide be backfilled?

There is no Market Tide dataset to backfill. For today's display:

1. The option-chain ingester must be writing current snapshots.
2. The analytics engine must be writing current `gex_summary` rows.
3. `flow_contract_facts` should contain today's trade-derived deltas. A quiet
   interval may contain no facts and is still valid.

If those inputs exist, the next API request calculates the reading immediately;
the API caches it for only 30 seconds. Restarting the API or waiting for that
cache to expire is sufficient after repairing an upstream feed.

The existing `flow_by_contract_backfill` and `flow_series_5min_backfill` tools
do not create missing `flow_contract_facts`; they roll up facts that already
exist. If historical option-chain snapshots were never captured, exact
intraday flow cannot be reconstructed from the latest chain alone. If raw
snapshots do exist but facts are absent, repair/replay the canonical flow-fact
materialization rather than inserting synthetic Market Tide rows.

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
```

An active registry entry with no corresponding chain/GEX feed reduces
participation. Mark unsupported symbols inactive or restore their ingestion;
do not lower the 60% threshold merely to hide a broken universe.
