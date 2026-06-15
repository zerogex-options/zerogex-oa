# API Gateway Architecture — Single-Process TradeStation Owner

> **Status:** Design proposal, not yet implemented. Authored 2026-06-15
> after the pre-market rate-limit incident as the structural successor to
> the DB-mediated rate-limit governor.

## 1. Current State Summary

- **One Python process per underlying.** `src/ingestion/main_engine.py:main()` (lines ~1958-2077) parses `INGEST_UNDERLYINGS`, then for each symbol calls `Process(target=run_for_symbol, ...).start()`. `run_for_symbol` constructs its own `TradeStationClient(client_id, client_secret, refresh_token, ...)`, calls `attach_db_writer(client)`, then runs `IngestionEngine(client, underlying=symbol).run()`. VIX gets a fourth process via `run_vix_ingester` → `src/ingestion/vix_ingester.py:main()`, which independently constructs *another* `TradeStationClient` (vix_ingester.py:371-376). All four processes are children of a single `systemd` unit (`setup/systemd/zerogex-oa-ingestion.service`) — `Restart=always`, `TimeoutStopSec=30`, `MemoryMax=2G`.
- **Per-process auth and HTTP state.** Each `TradeStationClient.__init__` instantiates its own `TradeStationAuth` (tradestation_client.py:80). The only cross-process coordination is a flock'd file at `/tmp/tradestation_token_cache.json` (tradestation_auth.py:17, 76, 231). N processes mean N refresh attempts at expiry, N concurrent stream readers, N independent retry budgets on 429/5xx.
- **Per-process streaming connections.** Each `IngestionEngine.run_streaming()` (main_engine.py:1828-1902) builds its own `StreamManager` (stream_manager.py:1018), which spawns daemon threads via `OptionStreamAccumulator` (stream_manager.py:268) — one HTTP `iter_lines()` reader per ~chunk of option symbols (`STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION`) plus one underlying bar stream. With 3 underlyings × ~2 chunks each + VIX, total live streams approach 10 — the documented Option Quote Stream concurrent cap *per API key*.
- **DB-mediated rate-limit coordination (a hack we just shipped).** `TradeStationClient._gate_for_rate_limit` (tradestation_client.py:146-247) syncs a 5-min counter across processes via the `tradestation_api_calls` table, summing via `ON CONFLICT … call_count + EXCLUDED.call_count`. The reader/writer are wired by `src/ingestion/api_call_tracker.py:attach_db_writer`. Estimation is stale up to `TS_RATE_LIMIT_SYNC_INTERVAL`; under-counts our own in-flight delta.
- **No central observability of ingestion health.** `[DB-METRICS]` lines (main_engine.py:1720) are per-process; no aggregate "how many streams are open, how many quotes/sec across the fleet, am I near the cap" signal exists.
- **Today's outage symptom.** Aggregated per-key API usage saturated quota even though each process believed it was well under cap — the underlying problem is that *the API key, not the process, is the unit of constraint at TradeStation*.

## 2. Proposed Architecture

A single **Gateway Process** owns everything that the API key cares about. Per-underlying **Consumer Processes** (or threads — see below) hold no TradeStation state at all.

**Gateway responsibilities:**
- One `TradeStationAuth` instance; one access token, one refresh loop.
- One `TradeStationClient` — every REST call funnels through here, so the rate-limit governor (today's hack, plus item #2's response-header-driven version) becomes a *local in-memory* counter with no DB round-trip.
- All streaming HTTP connections (option-quote chunks, underlying bar streams, VIX bar stream). Concurrency budget is a single integer in the gateway, not an emergent property.
- Per-connection backpressure, reconnect/backoff, the `_DecodeErrorTracker`, the cap-suspect-seconds heuristic — already exist in `StreamManager` and move wholesale into the gateway.
- A small router: each inbound payload is tagged with `(underlying, type)` and pushed to the appropriate consumer's outbound queue.

**Consumer responsibilities (per underlying):**
- Receive `{type: underlying|option_batch|flush_options, data: ...}` messages — same shape `StreamManager.stream()` yields today (stream_manager.py:1702-1721) — and feed them into `IngestionEngine._store_underlying`, `_store_option_batch`, `_flush_all_buffers`.
- Per-symbol analytics: the `_FlowAccumulator` state (main_engine.py:84), the bucket aggregation, Greeks/IV, the parity-guard signatures, the dual-UPSERT into `option_chains` + `option_chains_latest`.
- DB write path stays unchanged — the circuit breaker, `_pending_failed_option_rows`, and backoff logic are all consumer-local.

**IPC boundary: `multiprocessing.Queue` (stdlib).**

Why: (1) zerogex has *zero* existing dependency on Redis, ZMQ, or gRPC — `grep -ri redis src/ pyproject.toml` only matches a docstring and a comment, never an import. (2) The codebase already imports `multiprocessing.Process` in three engines and the deploy/systemd model assumes a single unit fanning out children. Adding `Queue` is the smallest possible delta. (3) `multiprocessing.Queue` is a pickling SimpleQueue on top of a pipe — ample for thousands of dict messages/sec when payloads are small (option-quote dicts are ~10 fields). (4) Trade-offs vs. alternatives:

- *Redis Pub/Sub*: drags in a new infra dependency and another failure mode; the per-key rate-limit benefit dies because the gateway already gets it for free.
- *ZMQ*: better throughput, but adds a wire-format decision and a non-trivial dep for a problem we're not actually throughput-bound on.
- *In-process threading (single OS process)*: simplest by far, removes IPC altogether. Trade-off is that one consumer's bug (e.g., a Greeks calculator OOM) crashes the whole feed. The current N-process model deliberately isolates that.
- *gRPC*: vastly over-engineered.

**Buffering and backpressure.** Each consumer queue is bounded (`maxsize=10_000` option batches ≈ several minutes of headroom). On `queue.Full` the gateway logs a `WARN` and *drops the oldest* message for that consumer (consumers are idempotent: option upserts use `GREATEST` per main_engine.py:498-549, so dropping a partial bucket update self-heals from the next stream tick). The gateway never blocks — backpressure on one slow consumer must never stall the TradeStation reader. A central `WatermarkMonitor` thread inside the gateway publishes `[QUEUE-WATERMARK]` log lines per consumer every 60s.

## 3. Migration Strategy

We never run "new world only" until "new world" has matched "old world" against live data for at least a full trading week.

1. **Step 1 — Introduce the gateway as a passthrough, gated by `INGEST_USE_GATEWAY=false`.** Implement `src/ingestion/gateway.py` defining `Gateway(client, consumers: dict[str, mp.Queue])`. Add a `--gateway` flag to `main()`. When off (the default), `main()` behaves exactly as today. Ship and deploy with the flag off so the binary is identical operationally.

2. **Step 2 — Build the consumer adapter without removing the old engine.** Add `IngestionEngine.run_from_queue(queue)` that drains a `multiprocessing.Queue` and dispatches to the same `_store_*` methods `stream()` currently feeds. Both `run()` (existing) and `run_from_queue()` (new) call identical downstream code paths — only the input source differs. Cover with unit tests that pump synthetic items into the queue and verify identical DB write payloads vs. a `StreamManager.stream()` mock.

3. **Step 3 — Parallel-run on a non-prod symbol set.** Deploy with `INGEST_USE_GATEWAY=true INGEST_UNDERLYINGS=SPY` against the dev DB schema. Compare `option_chains` and `tradestation_api_calls` row-for-row against a baseline run from the same source. Run for one full RTH session. Verify no new `[CIRCUIT-BREAKER]`, `[QUEUE-WATERMARK]`, or stream-cap-suspect WARNINGs.

4. **Step 4 — Canary on prod with one symbol.** Set `INGEST_USE_GATEWAY=true INGEST_UNDERLYINGS=QQQ` while SPY/$SPXW.X/VIX continue using the old path. The two paths share the API key and the `tradestation_api_calls` table — by design, since the gateway's local counter and the legacy DB-summed counter must agree. Watch for 24x5.

5. **Step 5 — Full cutover with rollback button.** Promote all symbols to the gateway. Keep `INGEST_USE_GATEWAY=false` valid for at least one quarter so a single env-var flip rolls back without code changes.

6. **Step 6 — Remove the DB-mediated rate-limit governor.** Only after a clean month under the gateway. `_gate_for_rate_limit` collapses to a local-only check; `tradestation_api_calls` becomes a pure observability table.

## 4. Risks & Open Questions

- **Gateway as single point of failure.** Today a SPY-process crash leaves QQQ and SPX still feeding. Under the gateway, a gateway crash silences everything. Mitigation: `systemd` `Restart=always` already exists; consumers must tolerate a queue going silent for ~15s without exiting. *Question: do we want consumers to exit (so the whole unit restarts cleanly) or to wait?*
- **Gateway redeploy = full feed gap.** Today rolling-restart per process means at most one symbol is dark at a time. Gateway redeploy means all symbols are dark for the restart window. *Question: is a 10-15s blanket gap during deploys acceptable, or do we need a second hot-standby gateway (significant complexity)?*
- **Queue throughput at RTH peak.** Option-quote firehose at the open: rough order-of-magnitude is 3 underlyings × ~80 contracts × a few updates/sec = low thousands of dicts/sec total. `multiprocessing.Queue` benchmarks comfortably handle that, but we must *measure* on the prod box, not assume. Build a load test before Step 3.
- **Pickle cost.** Every queue put pickles. Option dicts are small (~10 fields) but volume matters. If pickle becomes a hotspot, switch to passing already-encoded JSON bytes (the gateway just decoded them — re-encoding to bytes-only is cheap) or to a shared-memory ring buffer. Measure first.
- **`/tmp/tradestation_token_cache.json` becomes vestigial.** With one auth instance the file lock is dead weight. Leave the file path in place during migration (Step 1-5) so a rollback to the old multi-process path still works.
- **Consumer-local state hydration on reconnect.** Today each process's `_FlowAccumulator` (main_engine.py:84) is hydrated from the DB on first observation of each `(contract, session)` pair. That behavior is unchanged — but verify that a gateway-restart-only event (consumers still running) doesn't accidentally double-emit historical quotes that re-hydrate already-current accumulators.

## 5. Recommendation: Is This Worth Doing?

**Conditional yes, with explicit deferral until after item #2 has bedded in.** Here is the honest read:

Item #2 (response-header-driven rate limiter using TradeStation's headers, which reflect *aggregate per-key* usage) closes the rate-limit-coordination gap *for free* without restructuring anything. Each process learns from response headers what the whole key has consumed; processes will self-coordinate to within a header-update interval. That is genuinely good enough for the rate-limit motivation alone.

What item #2 does **not** address: (a) concurrent-stream-cap coordination (no per-key header for "you have 8/10 stream slots open"; this is exactly the failure mode of today's incident), (b) auth/refresh duplication (cheap but a real correctness hazard during token rotation under load), (c) the absence of a single place to observe ingestion health. The gateway refactor is the structural fix for (a) — there is no header-based shortcut to "decide which streams to open first across processes." If concurrent-stream-cap exhaustion is plausibly going to bite again (and the 2026-06 incident logs suggest yes), the gateway is the right structural answer, not a bandaid.

**Verdict:** Land item #2 first; let it run 2-3 trading weeks. If concurrent-stream-cap or auth-thrash incidents recur, the gateway is worth the 3-4 week effort (steps 1-5 above) and the operational complexity of a single point of failure. If item #2 plus a couple of small per-key concurrent-stream counters (cheap follow-up) keep things quiet, the gateway is "nice to have, defer indefinitely."
