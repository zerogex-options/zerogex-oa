-- ============================================================
-- option_chains drop-candidate audit
-- ============================================================
-- Two indexes are flagged in docs/runbooks/option_chains_indexing.md
-- for possible removal.  This script gathers the evidence needed
-- to confirm both are dead and safe to drop.
--
--   (1) idx_option_chains_underlying_option_symbol_ts_gamma_covering
--       ~19 GB, partial WHERE gamma IS NOT NULL.  Designed to serve
--       _do_refresh_flow_cache's LATERAL backfill as an Index Only
--       Scan, but its INCLUDE list is missing ask_volume / bid_volume
--       / mid -- columns the LATERAL SELECTs -- so the planner must
--       still hit the heap.  Sibling without the partial / INCLUDE:
--       idx_option_chains_underlying_option_symbol_timestamp.
--
--   (2) idx_option_chains_underlying_timestamp_option_symbol
--       ~3.3 GB, key (underlying, timestamp DESC, option_symbol).
--       Sibling idx_option_chains_underlying_timestamp shares the
--       (underlying, timestamp DESC) prefix and is preferred by the
--       planner ~400x more often.
--
-- Read-only.  Nothing is dropped from this file.
-- Paste-ready drop commands are printed at the end for use AFTER
-- review.
--
-- Invoke via the make wrapper:
--   make db-drop-candidate-audit                              # SPY, 2h
--   make db-drop-candidate-audit UNDERLYING=SPX LOOKBACK_HOURS=96
-- Or directly:
--   psql ... -v underlying=SPY -v lookback_hours=2 \
--            -f setup/database/diagnostics/drop_candidate_audit.sql
-- ============================================================

\set ON_ERROR_STOP off
\set AUTOCOMMIT on
\timing on
\pset pager off

-- Defaults if -v wasn't passed
\if :{?underlying}
\else
    \set underlying SPY
\endif
\if :{?lookback_hours}
\else
    \set lookback_hours 2
\endif

\echo ============================================================
\echo option_chains drop-candidate audit
\echo ============================================================
\echo Underlying:    :underlying
\echo Lookback (h):  :lookback_hours
\echo
\echo Suspect 1: idx_option_chains_underlying_option_symbol_ts_gamma_covering
\echo Suspect 2: idx_option_chains_underlying_timestamp_option_symbol
\echo ------------------------------------------------------------

-- Precondition: pg_stat_statements must be installed
SELECT EXISTS (
    SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
) AS pg_stat_statements_installed;

\echo
\echo === [1/3] pg_stat_statements: top 20 by total_exec_time touching option_chains ===
\echo Look for query shapes NOT covered by the EXPLAIN bank in [2/3].
\echo If you see one, paste it back and we add a targeted EXPLAIN before dropping.

SELECT
    queryid,
    calls,
    ROUND(total_exec_time::numeric / 1000, 1) AS total_sec,
    ROUND(mean_exec_time::numeric, 2)         AS mean_ms,
    ROUND(stddev_exec_time::numeric, 2)       AS stddev_ms,
    rows,
    LEFT(regexp_replace(query, E'\\s+', ' ', 'g'), 240) AS query_excerpt
FROM pg_stat_statements
WHERE query ILIKE '%option_chains%'
ORDER BY total_exec_time DESC
LIMIT 20;

-- ------------------------------------------------------------
-- Capture live values so EXPLAIN sees realistic selectivity stats.
-- ------------------------------------------------------------
SELECT timestamp AS latest_ts
FROM option_chains
WHERE underlying = :'underlying'
ORDER BY timestamp DESC
LIMIT 1
\gset

SELECT option_symbol AS sample_symbol
FROM option_chains
WHERE underlying = :'underlying'
  AND timestamp = :'latest_ts'::timestamptz
LIMIT 1
\gset

\echo
\echo === [2/3] EXPLAIN bank ===
\echo Pure planner output (no ANALYZE -- runs in ms).
\echo For each plan: read the index name on each Index Scan / Bitmap Index Scan / Index Only Scan node.
\echo Latest ts:      :latest_ts
\echo Sample symbol:  :sample_symbol

\echo
\echo --- (a) _do_refresh_flow_cache seed_rows LATERAL: per-contract latest-before-window ---
\echo     Shape: WHERE underlying = X AND option_symbol = Y AND timestamp < $ ORDER BY timestamp DESC LIMIT 1
\echo     SUSPECT 1 is purpose-built for this key order.  Healthy fallback if dropped:
\echo     idx_option_chains_underlying_option_symbol_timestamp (same key, non-partial, no INCLUDE).

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT oc.timestamp, oc.option_symbol, oc.strike, oc.expiration, oc.option_type,
       oc.volume, oc.ask_volume, oc.bid_volume, oc.last, oc.mid, oc.bid, oc.ask,
       oc.implied_volatility, oc.delta
FROM option_chains oc
WHERE oc.underlying = :'underlying'
  AND oc.option_symbol = :'sample_symbol'
  AND oc.timestamp < :'latest_ts'::timestamptz
ORDER BY oc.timestamp DESC
LIMIT 1;

\echo
\echo --- (b) _do_refresh_flow_cache window_rows: range scan over backfill window ---
\echo     Shape: WHERE underlying = X AND timestamp BETWEEN a AND b
\echo     SUSPECT 2 candidate via (underlying, timestamp DESC) prefix.  Healthy fallback:
\echo     idx_option_chains_underlying_timestamp (same prefix, smaller).

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT oc.timestamp, oc.option_symbol, oc.strike, oc.expiration, oc.option_type,
       oc.volume, oc.ask_volume, oc.bid_volume, oc.last, oc.mid, oc.bid, oc.ask,
       oc.implied_volatility, oc.delta
FROM option_chains oc
WHERE oc.underlying = :'underlying'
  AND oc.timestamp >= :'latest_ts'::timestamptz - (:lookback_hours * INTERVAL '1 hour')
  AND oc.timestamp <= :'latest_ts'::timestamptz;

\echo
\echo --- (c) _get_snapshot step 3: DISTINCT ON over lookback (the wedge case) ---
\echo     Shape: WHERE underlying = X AND timestamp BETWEEN ... AND gamma IS NOT NULL
\echo            ORDER BY option_symbol, timestamp DESC
\echo     SUSPECT 2 candidate via key prefix.  Other plausible picks:
\echo     idx_option_chains_underlying_ts_gamma (partial, smaller).
\echo     Re-run with LOOKBACK_HOURS=96 to repro cold-start bitmap-heap-scan path.

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT DISTINCT ON (oc.option_symbol)
       oc.option_symbol, oc.strike, oc.expiration, oc.timestamp,
       oc.delta, oc.gamma, oc.implied_volatility
FROM option_chains oc
WHERE oc.underlying = :'underlying'
  AND oc.timestamp <= :'latest_ts'::timestamptz
  AND oc.timestamp >= :'latest_ts'::timestamptz - (:lookback_hours * INTERVAL '1 hour')
  AND oc.gamma IS NOT NULL
ORDER BY oc.option_symbol, oc.timestamp DESC
LIMIT 2000;

\echo
\echo --- (d) /api/option/quote: latest row per underlying (CONTROL) ---
\echo     Shape: WHERE underlying = X ORDER BY timestamp DESC LIMIT 1 (full SELECT list)
\echo     Should pick idx_option_chains_underlying_ts_quote_covering (Index Only Scan).
\echo     Neither suspect should appear -- if one does, that's a planner anomaly worth a look.

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT bid, ask, volume, open_interest, strike, expiration, option_type, timestamp
FROM option_chains
WHERE underlying = :'underlying'
ORDER BY timestamp DESC
LIMIT 1;

\echo
\echo --- (e) per-contract history scan: (underlying, option_symbol) lookup ---
\echo     Shape: WHERE underlying = X AND option_symbol = Y ORDER BY timestamp DESC LIMIT N
\echo     Should pick idx_option_chains_underlying_option_symbol_timestamp.
\echo     SUSPECT 1 could also be picked when planner thinks the partial WHERE gamma IS NOT NULL
\echo     prunes meaningfully.

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT timestamp, last, bid, ask, volume, delta, gamma, implied_volatility
FROM option_chains
WHERE underlying = :'underlying'
  AND option_symbol = :'sample_symbol'
ORDER BY timestamp DESC
LIMIT 200;

\echo
\echo === [3/3] pg_stat_user_indexes: live scan counters ===
\echo Both suspects + their healthy siblings, for direct comparison.
\echo Compare idx_scan and last_idx_scan to the values from the previous session.
\echo Material growth in either suspect means an active user is still hitting it.
\echo Reference (previous session):
\echo   suspect 1 (ts_gamma_covering):  ~2447 scans
\echo   suspect 2 (ts_option_symbol):   ~6878 scans

SELECT
    indexrelname AS index,
    idx_scan,
    last_idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    CASE WHEN idx_scan > 0
         THEN ROUND(idx_tup_read::numeric / idx_scan, 1)
    END AS tuples_per_scan,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname IN (
    -- suspects
    'idx_option_chains_underlying_option_symbol_ts_gamma_covering',
    'idx_option_chains_underlying_timestamp_option_symbol',
    -- healthy siblings (must still be present and used after any drop)
    'idx_option_chains_underlying_option_symbol_timestamp',
    'idx_option_chains_underlying_timestamp',
    'idx_option_chains_underlying_ts_gamma',
    'idx_option_chains_underlying_ts_quote_covering'
)
ORDER BY pg_relation_size(indexrelid) DESC;

\echo
\echo ============================================================
\echo Decision guide
\echo ============================================================
\echo [1/3] -- if any top-20 query shape isn't covered by (a)-(e), STOP and add it.
\echo
\echo [2/3] -- scan the index name in each plan's Scan node:
\echo
\echo   * If NEITHER suspect appears in ANY plan, both are dead.
\echo     Proceed with the drops below.
\echo
\echo   * If SUSPECT 1 (ts_gamma_covering) appears in (a) or (e):
\echo     - check whether the chosen plan is Index Only Scan.  If yes,
\echo       dropping will force heap fetches via the non-partial sibling.
\echo     - check whether the alternative plan with the suspect dropped
\echo       (run the same EXPLAIN inside BEGIN; DROP INDEX; EXPLAIN; ROLLBACK;)
\echo       is materially slower.  If not, drop is safe.
\echo     - STOP and report back if uncertain.
\echo
\echo   * If SUSPECT 2 (ts_option_symbol) appears in (b) or (c):
\echo     - compare estimated cost vs. idx_option_chains_underlying_timestamp.
\echo       If suspect is meaningfully cheaper, the option_symbol column is
\echo       contributing selectivity; STOP and report back.
\echo     - If costs are within noise, drop is safe.
\echo
\echo [3/3] -- material scan growth (>10-20%% over reference) means recent active use.
\echo         Cross-reference with [1/3] to find the responsible query.
\echo
\echo ============================================================
\echo Paste-ready drop commands -- DO NOT RUN until above confirms
\echo ============================================================
\echo
\echo   # Suspect 1 (~19 GB)
\echo   make db-drop-distinct-on-index CONFIRM=yes
\echo
\echo   # Suspect 2 (~3.3 GB)
\echo   make query SQL="DROP INDEX CONCURRENTLY idx_option_chains_underlying_timestamp_option_symbol;"
\echo
\echo   # Verify after each drop:
\echo   make db-index-audit
\echo
