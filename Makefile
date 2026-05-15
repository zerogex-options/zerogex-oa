# ZeroGEX Database Query Shortcuts & Service Management
# ======================================================
# Usage: make <target>
#
# Common queries for monitoring and debugging the ZeroGEX platform
#
# ------------------------------------------------------------------------
# Variable naming convention (READ BEFORE ADDING NEW TARGETS / VARIABLES)
# ------------------------------------------------------------------------
# GNU make pre-binds several variables from the environment and from its
# own built-ins. They are ALWAYS set, regardless of whether the caller
# passes them on the command line — so `$(if $(VAR),...)` is always
# truthy and `[ -z "$(VAR)" ]` is never empty if VAR is one of these.
#
# Do NOT reuse these names as project-specific make variables:
#
#   USER         (Unix login name, e.g. "ubuntu")
#   SHELL        (the shell make uses for recipes)
#   MAKE, MAKEFLAGS, MAKEFILES, MAKEFILE_LIST, MAKE_HOST, MFLAGS,
#   MAKECMDGOALS, MAKELEVEL, MAKEOVERRIDES
#   CURDIR, PWD
#   .DEFAULT_GOAL, .RECIPEPREFIX, .SUFFIXES, .VARIABLES
#
# Convention: prefix project-specific make-passable variables to avoid
# collisions, e.g. KEY_USER (the api_keys CLI user_id, see ec6670a),
# WEB_*, ZEROGEX_*, FLOW_SYMBOL, UNDERLYING_LIVE_SYMBOL, etc. If you must
# accept a value via a name that overlaps a built-in, guard with
# `$(filter command line,$(origin VAR))` so only explicit command-line
# assignments are honored.

# Load database connection from .env
-include .env
export

# PostgreSQL connection string (keepalives prevent RDS from dropping idle SSL connections)
PSQL = PGPASSFILE=~/.pgpass psql "sslmode=require host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME) keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3"

# Service names
INGESTION_SERVICE = zerogex-oa-ingestion
ANALYTICS_SERVICE = zerogex-oa-analytics
API_SERVICE = zerogex-oa-api
SIGNALS_SERVICE = zerogex-oa-signals

# Optional filter for db-tail targets (e.g. make db-tail-option-chains UNDERLYING=SPY)
UNDERLYING ?=

# Python virtual environment
VENV_PYTHON = venv/bin/python

# Underlying symbol filter — used by flow, signal, and max-pain queries
# Override with: make flow-by-contract FLOW_SYMBOL=QQQ
FLOW_SYMBOL ?= SPY
UNDERLYING_LIVE_SYMBOL ?= SPY
FLOW_REBUILD_DATE ?= $(shell TZ=America/New_York date +%F)
FLOW_REBUILD_START_ET ?= 09:30
FLOW_REBUILD_END_ET ?= 16:15

# Colors for output
BLUE = \033[0;34m
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m

# =============================================================================
# Macros — generate repetitive targets for each service / table
# =============================================================================
# SERVICE_TARGETS: systemctl start/stop/restart/status/enable/disable + journalctl logs
# $(1) = target prefix   (e.g. ingestion)
# $(2) = Make variable    (e.g. INGESTION_SERVICE)
# $(3) = human label      (e.g. Ingestion)

define SERVICE_TARGETS

.PHONY: $(1)-start
$(1)-start: ## Start the $(3) service
	@echo "$$(GREEN)Starting $$($(2))...$$(NC)"
	@sudo systemctl start $$($(2))
	@sleep 2
	@sudo systemctl status $$($(2)) --no-pager

.PHONY: $(1)-stop
$(1)-stop: ## Stop the $(3) service
	@echo "$$(YELLOW)Stopping $$($(2))...$$(NC)"
	@sudo systemctl stop $$($(2))
	@sleep 1
	@echo "$$(GREEN)Service stopped$$(NC)"

.PHONY: $(1)-restart
$(1)-restart: ## Restart the $(3) service
	@echo "$$(YELLOW)Restarting $$($(2))...$$(NC)"
	@sudo systemctl restart $$($(2))
	@sleep 2
	@sudo systemctl status $$($(2)) --no-pager

.PHONY: $(1)-status
$(1)-status: ## Show $(3) service status
	@sudo systemctl status $$($(2)) --no-pager -l

.PHONY: $(1)-enable
$(1)-enable: ## Enable $(3) service to start on boot
	@echo "$$(GREEN)Enabling $$($(2)) to start on boot...$$(NC)"
	@sudo systemctl enable $$($(2))
	@echo "$$(GREEN)Service enabled$$(NC)"

.PHONY: $(1)-disable
$(1)-disable: ## Disable $(3) service from starting on boot
	@echo "$$(YELLOW)Disabling $$($(2)) from starting on boot...$$(NC)"
	@sudo systemctl disable $$($(2))
	@echo "$$(YELLOW)Service disabled$$(NC)"

.PHONY: $(1)-logs
$(1)-logs: ## Watch $(3) logs in real-time (Ctrl+C to stop)
	@echo "$$(BLUE)=== Watching $(3) Logs (Ctrl+C to stop) ===$$(NC)"
	@sudo journalctl -u $$($(2)) -f -n 50

.PHONY: $(1)-logs-tail
$(1)-logs-tail: ## Show last 100 $(3) log lines
	@echo "$$(GREEN)Last 100 $(3) log lines:$$(NC)"
	@sudo journalctl -u $$($(2)) -n 100 --no-pager

.PHONY: $(1)-logs-errors
$(1)-logs-errors: ## Show recent $(3) errors
	@echo "$$(BLUE)=== Recent $(3) Errors ===$$(NC)"
	@sudo journalctl -u $$($(2)) -p err -n 50 --no-pager

endef

$(eval $(call SERVICE_TARGETS,ingestion,INGESTION_SERVICE,Ingestion))
$(eval $(call SERVICE_TARGETS,analytics,ANALYTICS_SERVICE,Analytics))
$(eval $(call SERVICE_TARGETS,api,API_SERVICE,API))
$(eval $(call SERVICE_TARGETS,signals,SIGNALS_SERVICE,Signals))

# DB_TAIL: show 20 most recent rows from a table with optional UNDERLYING filter
# $(1) = target suffix   (e.g. option-chains)
# $(2) = table name       (e.g. option_chains)
# $(3) = filter column    (e.g. underlying)
# $(4) = order column     (e.g. timestamp)

define DB_TAIL

.PHONY: db-tail-$(1)
db-tail-$(1): ## Show 20 most recent rows from $(2) (UNDERLYING=X to filter)
	@echo "$$(BLUE)=== $(2) (last 20) ===$$(NC)"
	@$$(PSQL) -c "SELECT * FROM $(2) $$(if $$(UNDERLYING),WHERE $(3)='$$(UNDERLYING)',) ORDER BY $(4) DESC LIMIT 20;"

endef

$(eval $(call DB_TAIL,symbols,symbols,symbol,created_at))
$(eval $(call DB_TAIL,underlying-quotes,underlying_quotes,symbol,timestamp))
$(eval $(call DB_TAIL,option-chains,option_chains,underlying,timestamp))
$(eval $(call DB_TAIL,gex-summary,gex_summary,underlying,timestamp))
$(eval $(call DB_TAIL,gex-by-strike,gex_by_strike,underlying,timestamp))
$(eval $(call DB_TAIL,flow-by-contract,flow_by_contract,symbol,timestamp))
$(eval $(call DB_TAIL,flow-smart-money,flow_smart_money,symbol,timestamp))
$(eval $(call DB_TAIL,signal-scores,signal_scores,underlying,timestamp))
$(eval $(call DB_TAIL,signal-trades,signal_trades,underlying,opened_at))

.PHONY: db-tail-vix-bars
db-tail-vix-bars: ## Show 20 most recent rows from vix_bars
	@echo "$(BLUE)=== vix_bars (last 20) ===$(NC)"
	@$(PSQL) -c "SELECT * FROM vix_bars ORDER BY timestamp DESC LIMIT 20;"

.PHONY: db-tail-api-calls
db-tail-api-calls: ## Show 50 most recent rows from tradestation_api_calls
	@echo "$(BLUE)=== tradestation_api_calls (last 50) ===$(NC)"
	@$(PSQL) -c "SELECT * FROM tradestation_api_calls ORDER BY window_start DESC LIMIT 50;"

.PHONY: db-diagnostics
db-diagnostics: ## Run DB diagnostics snapshot (sessions, waits, blockers, slow queries, dead tuples)
	@echo "$(BLUE)=== DB Diagnostics Snapshot ===$(NC)"
	@printf "%s\n" \
		"\\echo [1/5] Session + wait overview" \
		"SELECT now() AS captured_at, count(*) AS total_sessions, count(*) FILTER (WHERE state = 'active') AS active_sessions, count(*) FILTER (WHERE wait_event_type IS NOT NULL) AS waiting_sessions FROM pg_stat_activity WHERE datname = current_database();" \
		"SELECT state, wait_event_type, wait_event, count(*) AS sessions FROM pg_stat_activity WHERE datname = current_database() GROUP BY 1,2,3 ORDER BY sessions DESC, state;" \
		"\\echo [2/5] Blocking chains" \
		"SELECT blocked.pid AS blocked_pid, blocked.usename AS blocked_user, now() - blocked.query_start AS blocked_for, blocker.pid AS blocker_pid, blocker.usename AS blocker_user, now() - blocker.query_start AS blocker_running_for, LEFT(blocked.query, 140) AS blocked_query, LEFT(blocker.query, 140) AS blocker_query FROM pg_stat_activity blocked JOIN pg_stat_activity blocker ON blocker.pid = ANY(pg_blocking_pids(blocked.pid)) WHERE blocked.datname = current_database();" \
		"\\echo [3/5] Long-running active queries" \
		"SELECT pid, usename, application_name, state, now() - query_start AS runtime, wait_event_type, wait_event, LEFT(query, 220) AS query FROM pg_stat_activity WHERE datname = current_database() AND state <> 'idle' ORDER BY runtime DESC LIMIT 30;" \
		"\\echo [4/5] pg_stat_statements (if enabled)" \
		"SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') AS has_pgss \\gset" \
		"\\if :has_pgss" \
		"SELECT query, calls, mean_exec_time, total_exec_time FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;" \
		"\\else" \
		"\\echo pg_stat_statements not available (extension disabled)." \
		"\\endif" \
		"\\echo [5/5] Dead tuple pressure" \
		"SELECT relname, n_dead_tup, n_live_tup, ROUND(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 1) AS dead_pct FROM pg_stat_user_tables WHERE n_dead_tup > 1000 ORDER BY n_dead_tup DESC LIMIT 25;" \
		| $(PSQL) -v ON_ERROR_STOP=0

.PHONY: analytics-snapshot-diagnose
analytics-snapshot-diagnose: ## Diagnose slow _get_snapshot: runs EXPLAIN ANALYZE + index/hypertable stats (UNDERLYING=SPX)
	@echo "$(BLUE)=== AnalyticsEngine._get_snapshot() Diagnosis ===$(NC)"
	@UNDERLYING=$${UNDERLYING:-SPX}; \
	LOOKBACK=$${LOOKBACK_MINUTES:-5}; \
	echo "$(YELLOW)Underlying=$$UNDERLYING  Lookback=$$LOOKBACK min$(NC)"; \
	printf "%s\n" \
		"\\echo [1/7] Table size + approximate row count" \
		"SELECT pg_size_pretty(pg_total_relation_size('option_chains')) AS total_size, pg_size_pretty(pg_relation_size('option_chains')) AS table_size, pg_size_pretty(pg_total_relation_size('option_chains') - pg_relation_size('option_chains')) AS index_and_toast, (SELECT reltuples::bigint FROM pg_class WHERE relname = 'option_chains') AS approx_rows;" \
		"\\echo [2/7] Dead tuples + last autovacuum (bloat check)" \
		"SELECT relname, n_live_tup, n_dead_tup, ROUND(n_dead_tup::numeric / NULLIF(n_live_tup,0) * 100, 1) AS dead_pct, last_autovacuum, last_autoanalyze FROM pg_stat_user_tables WHERE relname = 'option_chains';" \
		"\\echo [3/7] TimescaleDB chunks in window (if applicable)" \
		"SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') AS has_tsdb \\gset" \
		"\\if :has_tsdb" \
		"SELECT count(*) AS chunks_total, count(*) FILTER (WHERE range_end > NOW() - INTERVAL '1 hour') AS chunks_recent FROM timescaledb_information.chunks WHERE hypertable_name = 'option_chains';" \
		"\\else" \
		"\\echo (timescaledb not installed; skipping chunk inventory)" \
		"\\endif" \
		"\\echo [4/7] Indexes defined on option_chains" \
		"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'option_chains' ORDER BY indexname;" \
		"\\echo [5/7] Index usage stats (zero reads = unused / planner not picking it)" \
		"SELECT indexrelname, idx_scan, idx_tup_read, pg_size_pretty(pg_relation_size(indexrelid)) AS size FROM pg_stat_user_indexes WHERE relname = 'option_chains' ORDER BY idx_scan DESC;" \
		"\\echo [6/7] EXPLAIN: step 1 of new 3-query path (latest timestamp)" \
		"EXPLAIN (ANALYZE, BUFFERS) SELECT timestamp FROM option_chains WHERE underlying = '$$UNDERLYING' ORDER BY timestamp DESC LIMIT 1;" \
		"\\echo [7/7] EXPLAIN: step 3 of new path (latest-per-contract with literal timestamps)" \
		"SELECT timestamp AS ts FROM option_chains WHERE underlying = '$$UNDERLYING' ORDER BY timestamp DESC LIMIT 1 \\gset" \
		"\\echo Using latest ts = :ts  lookback_minutes = $$LOOKBACK" \
		"EXPLAIN (ANALYZE, BUFFERS) SELECT DISTINCT ON (oc.option_symbol) oc.option_symbol, oc.strike, oc.expiration, oc.timestamp FROM option_chains oc WHERE oc.underlying = '$$UNDERLYING' AND oc.timestamp <= :'ts'::timestamptz AND oc.timestamp >= :'ts'::timestamptz - ($$LOOKBACK * INTERVAL '1 minute') AND oc.gamma IS NOT NULL ORDER BY oc.option_symbol, oc.timestamp DESC LIMIT 2000;" \
		| $(PSQL) -v ON_ERROR_STOP=0

.PHONY: analytics-snapshot-explain
analytics-snapshot-explain: ## EXPLAIN (no ANALYZE) of the _get_snapshot query — returns in ms
	@UNDERLYING=$${UNDERLYING:-SPX}; \
	LOOKBACK=$${LOOKBACK_MINUTES:-5}; \
	echo "$(BLUE)=== EXPLAIN ($$UNDERLYING, $$LOOKBACK min) ===$(NC)"; \
	printf "%s\n" \
		"EXPLAIN (VERBOSE) WITH latest_ts AS (SELECT timestamp AS ts FROM option_chains WHERE underlying = '$$UNDERLYING' ORDER BY timestamp DESC LIMIT 1), latest_per_contract AS (SELECT DISTINCT ON (oc.option_symbol) oc.option_symbol, oc.timestamp FROM option_chains oc, latest_ts lt WHERE oc.underlying = '$$UNDERLYING' AND oc.timestamp <= lt.ts AND oc.timestamp >= (lt.ts - ($$LOOKBACK * INTERVAL '1 minute')) AND oc.gamma IS NOT NULL ORDER BY oc.option_symbol, oc.timestamp DESC) SELECT lt.ts, lpc.option_symbol FROM latest_ts lt LEFT JOIN latest_per_contract lpc ON TRUE;" \
		| $(PSQL) -v ON_ERROR_STOP=0

.PHONY: db-add-distinct-on-index
db-add-distinct-on-index: ## Build idx_option_chains_underlying_option_symbol_ts_gamma_covering CONCURRENTLY (~6 GB). Pass CONFIRM=yes to execute.
	@echo "$(BLUE)=== Building idx_option_chains_underlying_option_symbol_ts_gamma_covering CONCURRENTLY ===$(NC)"
	@echo "$(YELLOW)Partial covering index keyed on (underlying, option_symbol, timestamp DESC)$(NC)"
	@echo "$(YELLOW)WHERE gamma IS NOT NULL, with the full Greeks/quote SELECT list in INCLUDE.$(NC)"
	@echo "$(YELLOW)Serves queries that filter by both underlying AND a specific option_symbol$(NC)"
	@echo "$(YELLOW)with ORDER BY timestamp DESC -- notably _do_refresh_flow_cache()'s LATERAL$(NC)"
	@echo "$(YELLOW)backfill in src/api/database.py:628 (the dominant pg_stat_user_indexes user).$(NC)"
	@echo "$(YELLOW)Definition:$(NC)"
	@echo "$(YELLOW)  ON option_chains(underlying, option_symbol, timestamp DESC)$(NC)"
	@echo "$(YELLOW)  INCLUDE (strike, option_type, expiration, last, bid, ask,$(NC)"
	@echo "$(YELLOW)           volume, open_interest, delta, gamma, theta, vega,$(NC)"
	@echo "$(YELLOW)           implied_volatility)$(NC)"
	@echo "$(YELLOW)  WHERE gamma IS NOT NULL$(NC)"
	@echo "$(YELLOW)Measured size: ~6 GB on the production table (after bloat from initial build).$(NC)"
	@echo "$(YELLOW)NOTE: this index does NOT speed up _get_snapshot() -- the planner picks$(NC)"
	@echo "$(YELLOW)bitmap-heap-scan there.  See setup/database/schema.sql for the full context.$(NC)"
	@echo "$(YELLOW)Build is non-blocking (CREATE INDEX CONCURRENTLY) but holds a session;$(NC)"
	@echo "$(YELLOW)allow ~15-30 minutes on the production table size.  Run inside tmux:$(NC)"
	@echo "$(YELLOW)  tmux new -s indexbuild  &&  make db-add-distinct-on-index CONFIRM=yes$(NC)"
	@if [ "$${CONFIRM}" != "yes" ]; then \
		echo "$(YELLOW)Dry run. Re-run with CONFIRM=yes to actually build.$(NC)"; \
	else \
		printf "%s\n" \
			"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_option_chains_underlying_option_symbol_ts_gamma_covering ON option_chains(underlying, option_symbol, timestamp DESC) INCLUDE (strike, option_type, expiration, last, bid, ask, volume, open_interest, delta, gamma, theta, vega, implied_volatility) WHERE gamma IS NOT NULL;" \
			| $(PSQL) -v ON_ERROR_STOP=1; \
		echo "$(GREEN)✓ Index built.$(NC)"; \
	fi

.PHONY: db-add-confluence-matrix-index
db-add-confluence-matrix-index: ## Build idx_signal_component_scores_underlying_ts_comp_clamped_covering CONCURRENTLY. Pass CONFIRM=yes to execute.
	@echo "$(BLUE)=== Building idx_signal_component_scores_underlying_ts_comp_clamped_covering CONCURRENTLY ===$(NC)"
	@echo "$(YELLOW)Covering index for /api/signals/{basic,advanced}/confluence-matrix.$(NC)"
	@echo "$(YELLOW)Key: (underlying, timestamp DESC, component_name); INCLUDE (clamped_score).$(NC)"
	@echo "$(YELLOW)The endpoint reads N component scores across the recent lookback window,$(NC)"
	@echo "$(YELLOW)and the heap rows carry a JSONB context_values column that makes random$(NC)"
	@echo "$(YELLOW)heap fetches disproportionately expensive.  With every filter column in$(NC)"
	@echo "$(YELLOW)the key and clamped_score in INCLUDE, the planner can satisfy the read$(NC)"
	@echo "$(YELLOW)with an Index Only Scan and zero heap fetches.$(NC)"
	@if [ "$${CONFIRM}" != "yes" ]; then \
		echo "$(YELLOW)Dry run. Re-run with CONFIRM=yes to actually build.$(NC)"; \
	else \
		printf "%s\n" \
			"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signal_component_scores_underlying_ts_comp_clamped_covering ON signal_component_scores(underlying, timestamp DESC, component_name) INCLUDE (clamped_score);" \
			| $(PSQL) -v ON_ERROR_STOP=1; \
		echo "$(GREEN)✓ Index built. Verify via 'make db-verify-confluence-matrix-index' and 'make db-explain-confluence-matrix UNDERLYING=SPY'.$(NC)"; \
	fi

.PHONY: db-verify-confluence-matrix-index
db-verify-confluence-matrix-index: ## Confirm the confluence-matrix covering index exists and is VALID.
	@echo "$(BLUE)=== Verifying idx_signal_component_scores_underlying_ts_comp_clamped_covering ===$(NC)"
	@$(PSQL) -c "SELECT indexrelid::regclass AS index, pg_size_pretty(pg_relation_size(indexrelid)) AS size, indisvalid, indisready FROM pg_index WHERE indexrelid = 'idx_signal_component_scores_underlying_ts_comp_clamped_covering'::regclass;"

.PHONY: db-explain-confluence-matrix
db-explain-confluence-matrix: ## EXPLAIN (ANALYZE, BUFFERS) the confluence-matrix inner read (UNDERLYING=SPY LOOKBACK=120). Confirms Index Only Scan + Heap Fetches: 0.
	@echo "$(BLUE)=== EXPLAIN ANALYZE confluence-matrix inner read (UNDERLYING=$(or $(UNDERLYING),SPY), LOOKBACK=$(or $(LOOKBACK),120)) ===$(NC)"
	@echo "$(YELLOW)Look for: 'Index Only Scan using idx_signal_component_scores_underlying_ts_comp_clamped_covering' + 'Heap Fetches: 0'.$(NC)"
	@printf "EXPLAIN (ANALYZE, BUFFERS) WITH recent AS (SELECT timestamp, composite_score FROM signal_scores WHERE underlying = '%s' ORDER BY timestamp DESC LIMIT %s) SELECT scs.timestamp, scs.component_name, scs.clamped_score FROM recent r JOIN signal_component_scores scs ON scs.underlying = '%s' AND scs.timestamp = r.timestamp WHERE scs.component_name = ANY(ARRAY['tape_flow_bias','skew_delta','vanna_charm_flow','dealer_delta_pressure','gex_gradient','positioning_trap']);\n" \
		"$(or $(UNDERLYING),SPY)" "$(or $(LOOKBACK),120)" "$(or $(UNDERLYING),SPY)" \
		| $(PSQL) -v ON_ERROR_STOP=1

.PHONY: db-add-signal-scores-composite-index
db-add-signal-scores-composite-index: ## Build idx_signal_scores_underlying_ts_composite_covering CONCURRENTLY. Pass CONFIRM=yes to execute.
	@echo "$(BLUE)=== Building idx_signal_scores_underlying_ts_composite_covering CONCURRENTLY ===$(NC)"
	@echo "$(YELLOW)Covering index for the OUTER read in /api/signals/{basic,advanced}/confluence-matrix.$(NC)"
	@echo "$(YELLOW)Key: (underlying, timestamp DESC); INCLUDE (composite_score).$(NC)"
	@echo "$(YELLOW)signal_scores rows carry a fat 'components' JSONB column — only ~4 tuples$(NC)"
	@echo "$(YELLOW)per heap page — so LIMIT N scans pay ~N cold heap reads at remote-disk$(NC)"
	@echo "$(YELLOW)latencies (~50 ms/block).  With composite_score in INCLUDE the planner$(NC)"
	@echo "$(YELLOW)can satisfy the read from a tight Index Only Scan and skip the JSONB$(NC)"
	@echo "$(YELLOW)heap entirely.$(NC)"
	@if [ "$${CONFIRM}" != "yes" ]; then \
		echo "$(YELLOW)Dry run. Re-run with CONFIRM=yes to actually build.$(NC)"; \
	else \
		printf "%s\n" \
			"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signal_scores_underlying_ts_composite_covering ON signal_scores(underlying, timestamp DESC) INCLUDE (composite_score);" \
			| $(PSQL) -v ON_ERROR_STOP=1; \
		echo "$(GREEN)✓ Index built. Re-run 'make db-explain-confluence-matrix' — the outer Limit should now use the new index.$(NC)"; \
	fi

.PHONY: db-tune-signal-tables-autovacuum
db-tune-signal-tables-autovacuum: ## ALTER TABLE signal_scores + signal_component_scores to trigger autovacuum aggressively (keeps VM current for IOS).
	@echo "$(BLUE)=== Applying aggressive autovacuum settings to signal_scores + signal_component_scores ===$(NC)"
	@echo "$(YELLOW)Both tables are appended every scoring cycle.  Default autovacuum$(NC)"
	@echo "$(YELLOW)(scale_factor=0.2) waits until 20% of the table is dead/dirty before$(NC)"
	@echo "$(YELLOW)running — far too late for the visibility map to stay current on the$(NC)"
	@echo "$(YELLOW)latest tuples, which is exactly what /api/signals/.../confluence-matrix$(NC)"
	@echo "$(YELLOW)reads.  Lower to 2% + a small absolute threshold.$(NC)"
	@$(PSQL) -c "ALTER TABLE signal_scores SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 500, autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 500);"
	@$(PSQL) -c "ALTER TABLE signal_component_scores SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 1000);"
	@$(PSQL) -c "SELECT relname, reloptions FROM pg_class WHERE relname IN ('signal_scores','signal_component_scores');"
	@echo "$(GREEN)✓ Autovacuum tuned. Settings take effect on the next autovacuum cycle.$(NC)"

.PHONY: db-vacuum-confluence-matrix-tables
db-vacuum-confluence-matrix-tables: ## VACUUM (ANALYZE) signal_scores + signal_component_scores. Needed for Index Only Scan (refreshes visibility map) and planner stats.
	@echo "$(BLUE)=== VACUUM ANALYZE signal_scores, signal_component_scores ===$(NC)"
	@echo "$(YELLOW)Refreshes the visibility map (so IOS can skip heap fetches) and$(NC)"
	@echo "$(YELLOW)planner statistics.  Non-blocking — concurrent readers/writers proceed.$(NC)"
	@$(PSQL) -c "VACUUM (ANALYZE, VERBOSE) signal_scores;"
	@$(PSQL) -c "VACUUM (ANALYZE, VERBOSE) signal_component_scores;"
	@echo "$(GREEN)✓ Done. Re-run 'make db-explain-confluence-matrix' — Heap Fetches should be 0.$(NC)"

.PHONY: api-time-confluence-matrix
api-time-confluence-matrix: ## Time /api/signals/{basic,advanced}/confluence-matrix end-to-end (uses OPS_API_KEY / API_KEY from .env).
	@echo "$(BLUE)=== Timing /api/signals/{basic,advanced}/confluence-matrix ===$(NC)"
	@BASE_URL="http://localhost:8000"; \
	SYMBOL="$(or $(SYMBOL),SPY)"; \
	KEY=$$(grep -E '^OPS_API_KEY=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
	KEY_SRC="OPS_API_KEY"; \
	if [ -z "$$KEY" ]; then \
		KEY=$$(grep -E '^API_KEY=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
		KEY_SRC="API_KEY (break-glass)"; \
	fi; \
	if [ -z "$$KEY" ]; then \
		echo "$(RED)✗ No OPS_API_KEY (or API_KEY) in .env — every protected endpoint will 401.$(NC)"; \
		exit 1; \
	fi; \
	echo "$(YELLOW)Auth: sending X-API-Key from $$KEY_SRC (length=$${#KEY})$(NC)"; \
	timed_curl() { \
		url="$$1"; label="$$2"; \
		out=$$(curl -s -o /tmp/cm.json -w "%{http_code} %{time_total}s" \
			-H "X-API-Key: $$KEY" "$$url"); \
		code=$$(echo "$$out" | awk '{print $$1}'); \
		t=$$(echo "$$out" | awk '{print $$2}'); \
		if [ "$$code" = "200" ]; then \
			sample=$$(python3 -c 'import json,sys; d=json.load(open("/tmp/cm.json")); print("sample_count="+str(d.get("sample_count","?")))' 2>/dev/null); \
			echo "$(GREEN)✅ $$label  HTTP $$code  $$t  $$sample$(NC)"; \
		else \
			echo "$(RED)❌ $$label  HTTP $$code  $$t$(NC)"; \
			head -c 200 /tmp/cm.json; echo; \
		fi; \
	}; \
	echo "$(YELLOW)-- cold (first call after restart hits DB)$(NC)"; \
	timed_curl "$$BASE_URL/api/signals/basic/confluence-matrix?symbol=$$SYMBOL&lookback=120"     "basic    lookback=120 "; \
	echo "$(YELLOW)-- warm (should hit the _analytics_cache_ttl_seconds cache)$(NC)"; \
	timed_curl "$$BASE_URL/api/signals/basic/confluence-matrix?symbol=$$SYMBOL&lookback=120"     "basic    lookback=120 "; \
	echo "$(YELLOW)-- stress with max lookback$(NC)"; \
	timed_curl "$$BASE_URL/api/signals/basic/confluence-matrix?symbol=$$SYMBOL&lookback=2000"    "basic    lookback=2000"; \
	echo "$(YELLOW)-- companion advanced endpoint (same code path)$(NC)"; \
	timed_curl "$$BASE_URL/api/signals/advanced/confluence-matrix?symbol=$$SYMBOL&lookback=120"  "advanced lookback=120 "

.PHONY: db-drop-distinct-on-index
db-drop-distinct-on-index: ## DROP CONCURRENTLY idx_option_chains_underlying_option_symbol_ts_gamma_covering (~6 GB). PRECONDITION: migrate flow-cache backfill first. Pass CONFIRM=yes to execute.
	@echo "$(BLUE)=== Dropping idx_option_chains_underlying_option_symbol_ts_gamma_covering CONCURRENTLY ===$(NC)"
	@echo "$(RED)PRECONDITION CHECK -- this index has known live users.  Per the$(NC)"
	@echo "$(RED)May 14-15, 2026 investigation:$(NC)"
	@echo "$(RED)  * src/api/database.py:_do_refresh_flow_cache() LATERAL backfill$(NC)"
	@echo "$(RED)    uses this index at ~15s cadence under /api/gex/contract_flow$(NC)"
	@echo "$(RED)    polling.  Dropping without migrating that query first regresses$(NC)"
	@echo "$(RED)    flow-cache refresh latency by orders of magnitude.$(NC)"
	@echo "$(RED)BEFORE running this drop, do all of the following:$(NC)"
	@echo "$(RED)  1. Confirm flow-cache backfill has been migrated to an alternative$(NC)"
	@echo "$(RED)     plan (e.g. materialized latest-per-contract view, or a narrower$(NC)"
	@echo "$(RED)     index that covers the LATERAL).$(NC)"
	@echo "$(RED)  2. Verify idx_scan stopped accruing on this index via$(NC)"
	@echo "$(RED)     pg_stat_user_indexes (wait at least one trading session).$(NC)"
	@echo "$(RED)  3. Audit pg_stat_statements for any other queries hitting it.$(NC)"
	@echo "$(YELLOW)Note: this index does NOT speed up _get_snapshot() -- bitmap-heap-scan$(NC)"
	@echo "$(YELLOW)wins that plan choice -- so dropping it has no effect there.  But the$(NC)"
	@echo "$(YELLOW)other queries pay the cost.  See setup/database/schema.sql.$(NC)"
	@echo "$(YELLOW)Sanity-check current scan activity:$(NC)"
	@echo "$(YELLOW)  SELECT idx_scan, last_idx_scan FROM pg_stat_user_indexes$(NC)"
	@echo "$(YELLOW)    WHERE indexrelname = 'idx_option_chains_underlying_option_symbol_ts_gamma_covering';$(NC)"
	@echo "$(YELLOW)DROP CONCURRENTLY can park waiting for in-flight snapshots; run inside tmux.$(NC)"
	@if [ "$${CONFIRM}" != "yes" ]; then \
		echo "$(YELLOW)Dry run. Re-run with CONFIRM=yes to actually drop$(NC)"; \
		echo "$(YELLOW)(only do so after the preconditions above are met).$(NC)"; \
	else \
		printf "%s\n" \
			"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_underlying_option_symbol_ts_gamma_covering;" \
			| $(PSQL) -v ON_ERROR_STOP=1; \
		echo "$(GREEN)✓ Covering index dropped. Reclaimed ~6 GB.$(NC)"; \
	fi

.PHONY: db-drop-narrow-partial-index
db-drop-narrow-partial-index: ## DROP CONCURRENTLY the narrow idx_option_chains_underlying_option_symbol_ts_gamma (~1.6 GB; subsumed by _covering). Pass CONFIRM=yes to execute.
	@echo "$(BLUE)=== Dropping idx_option_chains_underlying_option_symbol_ts_gamma CONCURRENTLY ===$(NC)"
	@echo "$(YELLOW)The narrow partial index INCLUDE (expiration) is strictly subsumed by$(NC)"
	@echo "$(YELLOW)idx_option_chains_underlying_option_symbol_ts_gamma_covering (same key,$(NC)"
	@echo "$(YELLOW)same WHERE predicate, fuller INCLUDE list).  Once the covering index is$(NC)"
	@echo "$(YELLOW)in place and the planner is using it, the narrow one is dead weight$(NC)"
	@echo "$(YELLOW)(~1.6 GB on disk + per-insert write overhead on every ingestion row).$(NC)"
	@echo "$(YELLOW)DROP CONCURRENTLY can park waiting for old snapshots from in-flight$(NC)"
	@echo "$(YELLOW)analytics queries; allow several minutes, run inside tmux to be safe.$(NC)"
	@if [ "$${CONFIRM}" != "yes" ]; then \
		echo "$(YELLOW)Dry run. Re-run with CONFIRM=yes to actually drop.$(NC)"; \
	else \
		printf "%s\n" \
			"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_underlying_option_symbol_ts_gamma;" \
			| $(PSQL) -v ON_ERROR_STOP=1; \
		echo "$(GREEN)✓ Narrow partial index dropped.$(NC)"; \
	fi

.PHONY: db-drop-unused-indexes
db-drop-unused-indexes: ## Drop 4 indexes with idx_scan=0 (~2.8 GB reclaimed). Review output first; pass CONFIRM=yes to execute.
	@echo "$(BLUE)=== Dropping unused option_chains indexes ===$(NC)"
	@echo "$(YELLOW)Based on pg_stat_user_indexes idx_scan=0 over the life of these stats.$(NC)"
	@echo "$(YELLOW)  idx_option_chains_gamma_oi             (1560 MB)$(NC)"
	@echo "$(YELLOW)  idx_option_chains_iv_volume            (694 MB)$(NC)"
	@echo "$(YELLOW)  idx_option_chains_expiration_range     (386 MB)$(NC)"
	@echo "$(YELLOW)  idx_option_chains_timestamp_volfilter  (190 MB)$(NC)"
	@if [ "$${CONFIRM}" != "yes" ]; then \
		echo "$(YELLOW)Dry run. Re-run with CONFIRM=yes to actually drop.$(NC)"; \
	else \
		printf "%s\n" \
			"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_gamma_oi;" \
			"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_iv_volume;" \
			"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_expiration_range;" \
			"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_timestamp_volfilter;" \
			| $(PSQL) -v ON_ERROR_STOP=1; \
		echo "$(GREEN)✓ Unused indexes dropped. Run 'make analytics-snapshot-diagnose' to re-check.$(NC)"; \
	fi

.PHONY: flow-explain
flow-explain: ## Diagnose /api/flow/series query planner choice on flow_by_contract (FLOW_SYMBOL=SPY)
	@echo "$(BLUE)=== flow_by_contract Query Planner Diagnosis ===$(NC)"
	@SYMBOL=$${FLOW_SYMBOL:-SPY}; \
	HOURS=$${FLOW_DIAG_HOURS:-24}; \
	STRIKES=$${FLOW_DIAG_STRIKES:-600,605,610}; \
	EXPS_DAYS=$${FLOW_DIAG_EXPIRATION_DAYS:-30,60}; \
	echo "$(YELLOW)Symbol=$$SYMBOL  Window=$$HOURS h  Strikes=[$$STRIKES]  Expirations=CURRENT_DATE+[$$EXPS_DAYS] days$(NC)"; \
	echo "$(YELLOW)Override with: FLOW_SYMBOL FLOW_DIAG_HOURS FLOW_DIAG_STRIKES FLOW_DIAG_EXPIRATION_DAYS$(NC)"; \
	echo "$(YELLOW)Tip: get realistic strikes via 'make psql' →$(NC)"; \
	echo "$(YELLOW)  SELECT strike FROM flow_by_contract WHERE symbol = '$$SYMBOL' GROUP BY strike ORDER BY COUNT(*) DESC LIMIT 5;$(NC)"; \
	printf "%s\n" \
		"\\echo [1/7] Table size + approximate row count" \
		"SELECT pg_size_pretty(pg_total_relation_size('flow_by_contract')) AS total_size, pg_size_pretty(pg_relation_size('flow_by_contract')) AS table_size, pg_size_pretty(pg_total_relation_size('flow_by_contract') - pg_relation_size('flow_by_contract')) AS index_and_toast, (SELECT reltuples::bigint FROM pg_class WHERE relname = 'flow_by_contract') AS approx_rows;" \
		"\\echo [2/7] Dead tuples + last vacuum/analyze (high dead_pct = bloat)" \
		"SELECT relname, n_live_tup, n_dead_tup, ROUND(n_dead_tup::numeric / NULLIF(n_live_tup,0) * 100, 1) AS dead_pct, last_autovacuum, last_autoanalyze FROM pg_stat_user_tables WHERE relname = 'flow_by_contract';" \
		"\\echo [3/7] Indexes defined on flow_by_contract" \
		"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'flow_by_contract' ORDER BY indexname;" \
		"\\echo [4/7] Index usage stats (low idx_scan + large size = bloat candidate)" \
		"SELECT indexrelname, idx_scan, idx_tup_read, idx_tup_fetch, pg_size_pretty(pg_relation_size(indexrelid)) AS size FROM pg_stat_user_indexes WHERE relname = 'flow_by_contract' ORDER BY idx_scan DESC;" \
		"\\echo [5/7] EXPLAIN ANALYZE: baseline (symbol, timestamp) — the most common shape" \
		"EXPLAIN (ANALYZE, BUFFERS) SELECT timestamp, option_type, strike, expiration, raw_volume, net_volume, net_premium FROM flow_by_contract WHERE symbol = '$$SYMBOL' AND timestamp >= NOW() - ($$HOURS * INTERVAL '1 hour') AND timestamp <= NOW();" \
		"\\echo [6/7] EXPLAIN ANALYZE: + strike filter only" \
		"EXPLAIN (ANALYZE, BUFFERS) SELECT timestamp, option_type, strike, expiration, raw_volume, net_volume, net_premium FROM flow_by_contract WHERE symbol = '$$SYMBOL' AND timestamp >= NOW() - ($$HOURS * INTERVAL '1 hour') AND timestamp <= NOW() AND strike = ANY(ARRAY[$$STRIKES]::numeric[]);" \
		"\\echo [7/7] EXPLAIN ANALYZE: + strike AND expiration filter (the case the 4-col composite would help)" \
		"EXPLAIN (ANALYZE, BUFFERS) SELECT timestamp, option_type, strike, expiration, raw_volume, net_volume, net_premium FROM flow_by_contract WHERE symbol = '$$SYMBOL' AND timestamp >= NOW() - ($$HOURS * INTERVAL '1 hour') AND timestamp <= NOW() AND strike = ANY(ARRAY[$$STRIKES]::numeric[]) AND expiration = ANY(ARRAY(SELECT (CURRENT_DATE + (d || ' days')::interval)::date FROM unnest(string_to_array('$$EXPS_DAYS', ',')::int[]) AS d));" \
		| $(PSQL) -v ON_ERROR_STOP=0
	@echo ""
	@echo "$(GREEN)What to look for:$(NC)"
	@echo "  • $(GREEN)Index Scan on idx_flow_by_contract_*$(NC) → planner picked an existing index. Good."
	@echo "  • $(YELLOW)Bitmap Heap Scan with BitmapAnd$(NC) → planner combined two existing indexes. Usually still fast."
	@echo "  • $(RED)Seq Scan$(NC) on a multi-million-row table → existing indexes can't help; new index may be justified."
	@echo "  • Compare 'actual time' between [5] and [7]. If [7] is >10× slower AND [4] shows the existing"
	@echo "    strike/expiration indexes have low idx_scan counts, a 4-col composite is worth proposing."
	@echo "  • If [5]–[7] are all sub-100ms with Index Scan, the existing index set is fine — don't add more."
	@echo "  • $(RED)Index size >> table size$(NC) in [1] AND $(RED)low idx_scan$(NC) in [4] → consider DROP."
	@echo "  • $(RED)High dead_pct$(NC) in [2] AND large idx size in [4] → REINDEX CONCURRENTLY may shrink the index."

.PHONY: flow-index-prune
flow-index-prune: ## Drop idx_flow_by_contract_symbol_ts_strike (~55 MB; planner doesn't use it). Pass CONFIRM=yes to execute.
	@echo "$(BLUE)=== Pruning idx_flow_by_contract_symbol_ts_strike ===$(NC)"
	@echo "$(YELLOW)Production EXPLAIN ANALYZE confirmed the planner picks$(NC)"
	@echo "$(YELLOW)idx_flow_by_contract_symbol_ts_type for strike-only filters$(NC)"
	@echo "$(YELLOW)and idx_flow_by_contract_symbol_ts_exp for strike+expiration.$(NC)"
	@echo "$(YELLOW)idx_flow_by_contract_symbol_ts_strike: 55 MB, ~0.001%% of total scans.$(NC)"
	@echo "$(YELLOW)Re-run 'make flow-explain' afterwards to confirm fallback latency stays acceptable.$(NC)"
	@if [ "$${CONFIRM}" != "yes" ]; then \
		echo "$(YELLOW)Dry run. Re-run with CONFIRM=yes to actually drop.$(NC)"; \
	else \
		printf "%s\n" \
			"DROP INDEX CONCURRENTLY IF EXISTS idx_flow_by_contract_symbol_ts_strike;" \
			| $(PSQL) -v ON_ERROR_STOP=1; \
		echo "$(GREEN)✓ Index dropped. Run 'make flow-explain' to confirm planner fallback.$(NC)"; \
	fi

.PHONY: help
help: ## Show this help message
	@echo "=========================================="
	@echo "$(BLUE)ZeroGEX Management & Database Shortcuts$(NC)"
	@echo "=========================================="
	@echo ""
	@echo "$(GREEN)Platform Deployment:$(NC)"
	@echo "  make deploy             - Deploy Options Analytics platform"
	@echo "  make deploy-from        - Deploy Options Analytics platform (start-from)"
	@echo "  make deploy-validate    - Validate Options Analytics platform deployment"
	@echo "  make staging-smoke      - Run post-deploy staging smoke checklist"
	@echo ""
	@echo "$(GREEN)Ingestion Service Management:$(NC)"
	@echo "  make ingestion-start    - Start the ingestion service"
	@echo "  make ingestion-stop     - Stop the ingestion service"
	@echo "  make ingestion-restart  - Restart the ingestion service"
	@echo "  make ingestion-status   - Show ingestion service status"
	@echo "  make ingestion-enable   - Enable ingestion service to start on boot"
	@echo "  make ingestion-disable  - Disable ingestion service from starting on boot"
	@echo "  make ingestion-health   - Show ingestion service health and recent errors"
	@echo ""
	@echo "$(GREEN)Analytics Service Management:$(NC)"
	@echo "  make analytics-start    - Start the analytics service"
	@echo "  make analytics-stop     - Stop the analytics service"
	@echo "  make analytics-restart  - Restart the analytics service"
	@echo "  make analytics-status   - Show analytics service status"
	@echo "  make analytics-enable   - Enable analytics service to start on boot"
	@echo "  make analytics-disable  - Disable analytics service from starting on boot"
	@echo "  make analytics-health   - Show analytics service health and recent errors"
	@echo ""
	@echo "$(GREEN)Signals Service Management:$(NC)"
	@echo "  make signals-start      - Start the signals service"
	@echo "  make signals-stop       - Stop the signals service"
	@echo "  make signals-restart    - Restart the signals service"
	@echo "  make signals-status     - Show signals service status"
	@echo "  make signals-enable     - Enable signals service to start on boot"
	@echo "  make signals-disable    - Disable signals service from starting on boot"
	@echo "  make signals-health     - Show signals service health and recent errors"
	@echo ""
	@echo "$(GREEN)API Server:$(NC)"
	@echo "  make api-dev             - Run API in development mode (hot reload)"
	@echo "  make api-prod            - Run API in production mode (4 workers)"
	@echo "  make api-start           - Start API systemd service"
	@echo "  make api-stop            - Stop API systemd service"
	@echo "  make api-restart         - Restart API systemd service"
	@echo "  make api-status          - Check API service status"
	@echo "  make api-enable          - Enable API service to start on boot"
	@echo "  make api-disable         - Disable API service from starting on boot"
	@echo "  make api-health          - Show API service health and recent errors"
	@echo "  make api-test            - Test API endpoints"
	@echo "  make api-install-service - Install API as systemd service"
	@echo "  make api-keys-create USER=<id> NAME=<label> - Issue a per-user API key"
	@echo "  make api-keys-list [USER=<id>] [ACTIVE=yes] - List per-user API keys"
	@echo "  make api-keys-revoke ID=<n>    - Revoke a per-user API key"
	@echo "  make db-maintain-install - Install daily DB maintenance timer (prune + vacuum)"
	@echo "  make normalizer-cache-install - Install nightly normalizer-refresh timer (04:30 ET)"
	@echo "  make normalizer-cache-status  - Show normalizer-refresh timer status + recent log"
	@echo "  make normalizer-cache-healthcheck - Flag stale cache rows (exit 1 = stale, for monitoring)"
	@echo "  make alert-template-install   - Install zerogex-alert@.service + sample env (slack/sns/pagerduty/webhook)"
	@echo "  make alert-template-test      - Fire a synthetic alert through the template"
	@echo ""
	@echo "$(GREEN)Logs (all services):$(NC)"
	@echo "  make {service}-logs             - Show live logs (Ctrl+C to exit)"
	@echo "  make {service}-logs-tail        - Show last 100 log lines"
	@echo "  make {service}-logs-errors      - Show recent errors"
	@echo "  (service = ingestion | analytics | signals | api)"
	@echo "  make logs-grep PATTERN=\"text\" - Search all service logs for pattern"
	@echo "  make logs-clear                 - Clear journals + system/nginx/etc. logs (rotated + gzipped)"
	@echo ""
	@echo "$(GREEN)Run Components:$(NC)"
	@echo "  make run-auth           - Test TradeStation authentication"
	@echo "  make run-client         - Test TradeStation API client"
	@echo "    TEST=<all|quote|bars|stream-bars|options|search|market-hours|depth>"
	@echo "    SYMBOL=<sym>  BARS_BACK=<n>  INTERVAL=<n>  UNIT=<Minute|Daily|Weekly|Monthly>"
	@echo "    QUERY=<str>   DEBUG=1        TEST_HISTORICAL=1"
	@echo "  make alias-check        - Resolve aliases to TradeStation symbols"
	@echo "    ALIAS=<name> ALIASES=\"A=$$A.B,C=$$C.D\" INPUT=\"SPY,<alias>\""
	@echo "  make run-ingest-alias   - Run ingestion with alias-aware underlyings"
	@echo "    INPUT=\"SPY,SPX\" ALIASES=\"SPX=$$$$SPX.X\" (or ALIAS+TICKER)"
	@echo "  make run-stream         - Test real-time streaming"
	@echo "  make run-ingest         - Run main ingestion engine"
	@echo "  make run-analytics      - Run analytics engine"
	@echo "  make run-analytics-once - Run analytics once (testing)"
	@echo "  make run-greeks         - Test Greeks calculator"
	@echo "  make run-iv             - Test IV calculator"
	@echo "  make run-config         - Show current configuration"
	@echo ""
	@echo "$(GREEN)Quick Stats:$(NC)"
	@echo "  make stats              - Show overall data statistics"
	@echo "  make latest             - Show latest data from all tables"
	@echo "  make today              - Show today's data summary"
	@echo ""
	@echo "$(GREEN)Underlying Quotes:$(NC)"
	@echo "  make underlying         - Last 10 underlying bars"
	@echo "  make underlying-live    - Live latest underlying row (1s refresh, overwrite)"
	@echo "  make underlying-live-raw - Live latest underlying row (raw values, 1s refresh)"
	@echo "  make underlying-today   - Today's underlying bars"
	@echo "  make underlying-volume  - Volume analysis for today"
	@echo ""
	@echo "$(GREEN)Option Chains:$(NC)"
	@echo "  make options            - Last 10 option quotes"
	@echo "  make options-latest     - Latest option quotes"
	@echo "  make options-today      - Today's option activity"
	@echo "  make options-strikes    - Active strikes summary"
	@echo "  make options-raw        - Raw option data"
	@echo "  make options-fields     - Check field population"
	@echo ""
	@echo "$(GREEN)Greeks & Analytics:$(NC)"
	@echo "  make greeks             - Latest Greeks by strike"
	@echo "  make greeks-summary     - Greeks summary statistics"
	@echo "  make gex-summary        - Latest GEX summary"
	@echo "  make gex-strikes        - GEX by strike (top 20)"
	@echo "  make gex-preview        - Preview GEX calculation data"
	@echo ""
	@echo "$(GREEN)Real-Time Flow Analysis:$(NC)"
	@echo "  make flow-by-contract     - Unified 5-min flow by (type, strike, expiration)"
	@echo "    (override underlying: FLOW_SYMBOL=QQQ)"
	@echo "  make flow-smart-money     - Unusual activity detection"
	@echo "  make flow-buying-pressure - Underlying buying/selling pressure"
	@echo "  make flow-live            - Combined real-time flow dashboard"
	@echo "  make flow-reset-session-facts - Delete one ET session from flow_contract_facts"
	@echo "  make flow-rebuild-session - Reset one ET session, restart API, trigger flow refresh"
	@echo "    (override: FLOW_SYMBOL=SPX FLOW_REBUILD_DATE=2026-04-10 FLOW_REBUILD_START_ET=09:30 FLOW_REBUILD_END_ET=16:15)"
	@echo ""
	@echo "$(GREEN)Technicals Support:$(NC)"
	@echo "  make vwap               - VWAP deviation tracker"
	@echo "  make orb                - Opening range breakout status"
	@echo "  make hedge-pressure     - Dealer hedging pressure"
	@echo "  make volume-spikes      - Unusual volume detection"
	@echo "  make divergence         - Momentum divergence signals"
	@echo "  make technicals         - Combined technicals dashboard"
	@echo ""
	@echo "$(GREEN)Max Pain:$(NC)"
	@echo "  make max-pain-current      - Max-pain snapshot"
	@echo "  make max-pain-expirations  - All expirations from latest snapshot"
	@echo "  make max-pain-strikes      - Detailed strike breakdown for nearest expiration"
	@echo ""
	@echo "$(GREEN)Signals:$(NC)"
	@echo "  make signals                  - Latest consolidated signal snapshot"
	@echo "  make signals-detail           - Full consolidated signal detail"
	@echo "  make signals-raw              - Raw latest rows from consolidated_trade_signals"
	@echo "  make signals-components       - Consolidated component-group breakdown"
	@echo "  make signals-exhaustion       - Latest ZeroGEX Exhaustion from consolidated payload"
	@echo "  make signals-history          - Managed trade history with outcomes + P&L"
	@echo "  make signals-wipe-open        - Delete all OPEN signal trades (with confirmation)"
	@echo "  make signals-wipe-all         - Delete ALL signal trades (with confirmation)"
	@echo "  make signals-fresh-start      - Clear historical signal-trade state for a fresh run"
	@echo "  make signals-trades           - Latest managed trades from signal_engine_trade_ideas"
	@echo "  make signals-trades-raw       - Raw managed-trade rows from signal_engine_trade_ideas"
	@echo "  make signals-all-symbols      - Latest consolidated signal for every tracked symbol"
	@echo "  make signals-accuracy         - Consolidated win rate by timeframe + strength (last 30 days)"
	@echo "  make signals-accuracy-daily   - Daily consolidated accuracy (last 14 days)"
	@echo "  make signals-accuracy-all     - Full consolidated accuracy table"
	@echo "  make vol-signals              - Latest volatility-expansion signal"
	@echo "  make vol-signals-components   - Vol-expansion component breakdown"
	@echo "  make api-test-vol-signals     - Test /api/signals/advanced/vol-expansion endpoint"
	@echo "  make signals-logs             - Watch Signals service logs live"
	@echo "  make signals-logs-tail        - Last 100 Signals log lines"
	@echo "  make signals-logs-errors      - Show recent Signals service errors"
	@echo "  make signals-logs-cycles      - Show each completed signal engine cycle"
	@echo "  make api-test-signals         - Test trade/history/position endpoints"
	@echo "  make api-test-signals-summary - One-liner trade status + history summary"
	@echo ""
	@echo "$(GREEN)Data Quality:$(NC)"
	@echo "  make gaps               - Check for data gaps"
	@echo "  make gaps-today         - Today's data gaps"
	@echo "  make quality            - Data quality report"
	@echo ""
	@echo "$(GREEN)Data Management:$(NC)"
	@echo "  make clear-data         - Clear all data (with confirmation)"
	@echo "  make clear-options      - Clear only option chains"
	@echo "  make clear-underlying   - Clear only underlying quotes"
	@echo ""
	@echo "$(GREEN)Database Schema:$(NC)"
	@echo "  make symbol-add        - Upsert one row into required symbols table"
	@echo "    SYMBOL=<sym> NAME=<name> ASSET_TYPE=<EQUITY|INDEX|ETF> IS_ACTIVE=<true|false>"
	@echo "  make pull               - git pull --ff-only + schema-apply (use after updating)"
	@echo "  make schema-apply       - Apply/update setup/database/schema.sql"
	@echo "  make schema-verify      - Verify schema components exist"
	@echo "  make schema-backup      - Backup current schema to file"
	@echo ""
	@echo "$(GREEN)DB Table Tail:$(NC)"
	@echo "  make db-tail-symbols              - Last 20 rows from symbols"
	@echo "  make db-tail-underlying-quotes    - Last 20 rows from underlying_quotes"
	@echo "  make db-tail-option-chains        - Last 20 rows from option_chains"
	@echo "  make db-tail-gex-summary          - Last 20 rows from gex_summary"
	@echo "  make db-tail-gex-by-strike        - Last 20 rows from gex_by_strike"
	@echo "  make db-tail-flow-by-contract     - Last 20 rows from flow_by_contract"
	@echo "  make db-tail-flow-smart-money     - Last 20 rows from flow_smart_money"
	@echo "  make db-tail-trade-signals        - Last 20 rows from trade_signals"
	@echo "  make db-tail-signals-accuracy      - Last 20 rows from signal_accuracy"
	@echo "  make db-tail-position-optimizer-signals - Last 20 rows from position_optimizer_signals"
	@echo "  make db-tail-position-optimizer-accuracy - Last 20 rows from position_optimizer_accuracy"
	@echo "  make db-tail-vix-bars             - Last 20 rows from vix_bars"
	@echo "  make db-tail-api-calls            - Last 50 rows from tradestation_api_calls"
	@echo "  make db-diagnostics               - DB diagnostics (sessions, locks, waits, slow queries)"
	@echo "  make flow-explain                 - EXPLAIN ANALYZE flow_by_contract queries (FLOW_SYMBOL=SPY)"
	@echo "  make flow-index-prune             - Drop idx_flow_by_contract_symbol_ts_strike (CONFIRM=yes)"
	@echo ""
	@echo "$(GREEN)Maintenance:$(NC)"
	@echo "  make vacuum             - Vacuum analyze all tables"
	@echo "  make db-maintain        - Full maintenance: prune old data, vacuum full, reindex"
	@echo "  make db-prune           - Delete data older than DATA_RETENTION_DAYS (default 90)"
	@echo "  make db-size            - Show table sizes"
	@echo "  make refresh-views      - Refresh materialized views"
	@echo "  make db-prune-legacy    - Drop obsolete legacy refresh/materialized-view artifacts"
	@echo ""
	@echo "$(GREEN)Interactive:$(NC)"
	@echo "  make psql             - Open PostgreSQL shell"
	@echo "  make query SQL=\"...\"  - Run custom query"
	@echo ""
	@echo "$(GREEN)Quality / CI:$(NC)"
	@echo "  make test               - Run pytest with coverage"
	@echo "  make test-fast          - Run pytest without coverage (faster)"
	@echo "  make lint               - Run flake8 on src + tests"
	@echo "  make fmt                - Run black on src + tests (writes changes)"
	@echo "  make fmt-check          - Run black --check (no writes; CI-style)"
	@echo "  make type-check         - Run mypy on src"
	@echo "  make ci                 - fmt-check + lint + type-check + test"
	@echo "  make install-dev        - Install package + dev extras into active env"


# =============================================================================
# Quality / CI
# =============================================================================
# Use the venv Python if it exists; otherwise fall back to the active interpreter.
PY ?= $(shell test -x venv/bin/python && echo venv/bin/python || command -v python3 || echo python)

.PHONY: install-dev
install-dev: ## Install package + dev/metrics/greeks/api extras into the active env
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev,metrics,greeks,api]"

.PHONY: test
test: ## Run pytest with coverage (per pyproject addopts)
	$(PY) -m pytest

.PHONY: test-fast
test-fast: ## Run pytest without coverage (faster local iteration)
	$(PY) -m pytest --no-cov -q

.PHONY: lint
lint: ## Run flake8 on src + tests
	$(PY) -m flake8 src tests

.PHONY: fmt
fmt: ## Run black on src + tests (writes changes)
	$(PY) -m black src tests

.PHONY: fmt-check
fmt-check: ## Run black --check on src + tests (CI-style; no writes)
	$(PY) -m black --check src tests

.PHONY: type-check
type-check: ## Run mypy on src
	$(PY) -m mypy src

.PHONY: ci
ci: fmt-check lint type-check test ## Run the full local CI suite (fmt-check, lint, type-check, test)
	@echo "$(GREEN)✓ CI checks passed$(NC)"


# =============================================================================
# Platform Deployment
# =============================================================================

.PHONY: deploy
deploy: ## Deploy Options Analytics platform
	@echo "$(GREEN)Deploying ZeroGEX Options Analytics Platform to ~/zerogex-oa...$(NC)"
	@./deploy/deploy.sh

.PHONY: deploy-from
deploy-from: ## Deploy Options Analytics platform from step (use: make deploy-from STEP="<step>")
	@echo "$(GREEN)Deploying ZeroGEX Options Analytics Platform to ~/zerogex-oa starting from step $(STEP)...$(NC)"
	@./deploy/deploy.sh --start-from "$(STEP)"

.PHONY: deploy-validate
deploy-validate: ## Validate Options Analytics platform deployment
	@echo "$(GREEN)Validating Options Analytics platform deployment...$(NC)"
	@./deploy/deploy.sh --start-from "validation"

# =============================================================================
# Service Health Checks (kept inline — shell variable escaping is clearer here)
# =============================================================================

.PHONY: ingestion-health
ingestion-health: ## Check ingestion service health and recent errors
	@echo "$(GREEN)Ingestion Service Health Check$(NC)"
	@echo "===================="
	@echo ""
	@if systemctl is-active --quiet $(INGESTION_SERVICE); then \
		echo "Status: $(GREEN)ACTIVE$(NC)"; \
	else \
		echo "Status: $(RED)INACTIVE$(NC)"; \
	fi
	@echo ""
	@UPTIME=$$(systemctl show $(INGESTION_SERVICE) --property=ActiveEnterTimestamp --value); \
	if [ -n "$$UPTIME" ]; then \
		echo "Started: $$UPTIME"; \
	fi
	@echo ""
	@MEMORY=$$(systemctl show $(INGESTION_SERVICE) --property=MemoryCurrent --value); \
	if [ "$$MEMORY" != "[not set]" ] && [ -n "$$MEMORY" ]; then \
		MEMORY_MB=$$(($$MEMORY / 1024 / 1024)); \
		echo "Memory: $${MEMORY_MB} MB"; \
	fi
	@echo ""
	@echo "Recent Errors (last 10):"
	@echo "------------------------"
	@sudo journalctl -u $(INGESTION_SERVICE) -n 500 --no-pager | grep " - ERROR - " | tail -10 || echo "No recent errors"
	@echo ""
	@echo "Recent Warnings (last 5):"
	@echo "-------------------------"
	@sudo journalctl -u $(INGESTION_SERVICE) -n 500 --no-pager | grep " - WARNING - " | tail -5 || echo "No recent warnings"

.PHONY: analytics-health
analytics-health: ## Check analytics service health and recent errors
	@echo "$(GREEN)Analytics Service Health Check$(NC)"
	@echo "===================="
	@echo ""
	@if systemctl is-active --quiet $(ANALYTICS_SERVICE); then \
		echo "Status: $(GREEN)ACTIVE$(NC)"; \
	else \
		echo "Status: $(RED)INACTIVE$(NC)"; \
	fi
	@echo ""
	@UPTIME=$$(systemctl show $(ANALYTICS_SERVICE) --property=ActiveEnterTimestamp --value); \
	if [ -n "$$UPTIME" ]; then \
		echo "Started: $$UPTIME"; \
	fi
	@echo ""
	@MEMORY=$$(systemctl show $(ANALYTICS_SERVICE) --property=MemoryCurrent --value); \
	if [ "$$MEMORY" != "[not set]" ] && [ -n "$$MEMORY" ]; then \
		MEMORY_MB=$$(($$MEMORY / 1024 / 1024)); \
		echo "Memory: $${MEMORY_MB} MB"; \
	fi
	@echo ""
	@echo "Recent Errors (last 10):"
	@echo "------------------------"
	@sudo journalctl -u $(ANALYTICS_SERVICE) -n 500 --no-pager | grep " - ERROR - " | tail -10 || echo "No recent errors"
	@echo ""
	@echo "Recent Warnings (last 5):"
	@echo "-------------------------"
	@sudo journalctl -u $(ANALYTICS_SERVICE) -n 500 --no-pager | grep " - WARNING - " | tail -5 || echo "No recent warnings"


.PHONY: signals-health
signals-health: ## Check signals service health and recent errors
	@echo "$(GREEN)Signals Service Health Check$(NC)"
	@echo "===================="
	@echo ""
	@if systemctl is-active --quiet $(SIGNALS_SERVICE); then 		echo "Status: $(GREEN)ACTIVE$(NC)"; 	else 		echo "Status: $(RED)INACTIVE$(NC)"; 	fi
	@echo ""
	@UPTIME=$$(systemctl show $(SIGNALS_SERVICE) --property=ActiveEnterTimestamp --value); 	if [ -n "$$UPTIME" ]; then 		echo "Started: $$UPTIME"; 	fi
	@echo ""
	@MEMORY=$$(systemctl show $(SIGNALS_SERVICE) --property=MemoryCurrent --value); 	if [ "$$MEMORY" != "[not set]" ] && [ -n "$$MEMORY" ]; then 		MEMORY_MB=$$(($$MEMORY / 1024 / 1024)); 		echo "Memory: $${MEMORY_MB} MB"; 	fi
	@echo ""
	@echo "Recent Errors (last 10):"
	@echo "------------------------"
	@sudo journalctl -u $(SIGNALS_SERVICE) -n 500 --no-pager | grep " - ERROR - " | tail -10 || echo "No recent errors"
	@echo ""
	@echo "Recent Warnings (last 5):"
	@echo "-------------------------"
	@sudo journalctl -u $(SIGNALS_SERVICE) -n 500 --no-pager | grep " - WARNING - " | tail -5 || echo "No recent warnings"

.PHONY: api-health
api-health: ## Check API service health and recent errors
	@echo "$(GREEN)API Service Health Check$(NC)"
	@echo "===================="
	@echo ""
	@if systemctl is-active --quiet $(API_SERVICE); then \
		echo "Status: $(GREEN)ACTIVE$(NC)"; \
	else \
		echo "Status: $(RED)INACTIVE$(NC)"; \
	fi
	@echo ""
	@UPTIME=$$(systemctl show $(API_SERVICE) --property=ActiveEnterTimestamp --value); \
	if [ -n "$$UPTIME" ]; then \
		echo "Started: $$UPTIME"; \
	fi
	@echo ""
	@MEMORY=$$(systemctl show $(API_SERVICE) --property=MemoryCurrent --value); \
	if [ "$$MEMORY" != "[not set]" ] && [ -n "$$MEMORY" ]; then \
		MEMORY_MB=$$(($$MEMORY / 1024 / 1024)); \
		echo "Memory: $${MEMORY_MB} MB"; \
	fi
	@echo ""
	@echo "Recent Errors (last 10):"
	@echo "------------------------"
	@sudo journalctl -u $(API_SERVICE) -n 500 --no-pager | grep " - ERROR - " | tail -10 || echo "No recent errors"
	@echo ""
	@echo "Recent Warnings (last 5):"
	@echo "-------------------------"
	@sudo journalctl -u $(API_SERVICE) -n 500 --no-pager | grep " - WARNING - " | tail -5 || echo "No recent warnings"

# =============================================================================
# Cross-Service Log Utilities
# =============================================================================

.PHONY: logs-grep
logs-grep: ## Grep logs for specific pattern (use: make logs-grep PATTERN="Greeks")
	@echo "$(BLUE)=== Searching Ingestion Logs ===$(NC)"
	@sudo journalctl -u $(INGESTION_SERVICE) -n 1000 --no-pager | grep "$(PATTERN)" || echo "No matches in ingestion logs"
	@echo ""
	@echo "$(BLUE)=== Searching Analytics Logs ===$(NC)"
	@sudo journalctl -u $(ANALYTICS_SERVICE) -n 1000 --no-pager | grep "$(PATTERN)" || echo "No matches in analytics logs"

.PHONY: logs-clear
logs-clear: ## Interactive log cleanup (prompts; calls logs-clear-noconfirm)
	@echo "$(RED)⚠️  WARNING: This permanently deletes service journals AND system logs.$(NC)"
	@echo "$(YELLOW)Targets:$(NC)"
	@echo "  • journalctl: $(INGESTION_SERVICE), $(ANALYTICS_SERVICE), $(API_SERVICE), $(SIGNALS_SERVICE)"
	@echo "  • journalctl: cap total to 100M"
	@echo "  • /var/log/syslog, auth.log, kern.log, dpkg.log, ufw.log, fail2ban.log (active + rotated)"
	@echo "  • /var/log/nginx/{access,error}.log (active + rotated, reload nginx)"
	@echo "  • /var/log/letsencrypt/, /var/log/zerogex/ (active + rotated)"
	@echo "  • /var/log/apt/, /var/log/unattended-upgrades/ (active + rotated)"
	@read -p "Type 'yes' to confirm: " confirm; \
	if [ "$$confirm" != "yes" ]; then \
		echo "$(RED)❌ Aborted$(NC)"; exit 0; \
	fi; \
	$(MAKE) logs-clear-noconfirm

.PHONY: logs-clear-noconfirm
logs-clear-noconfirm: ## Non-interactive log cleanup (driven by zerogex-oa-logs-clear.timer)
	@echo "$(BLUE)Disk usage BEFORE:$(NC)"; df -h / | tail -1; echo ""
	@echo "$(YELLOW)→ journalctl: rotate + vacuum service journals + cap total to 100M...$(NC)"
	@sudo journalctl --rotate
	@sudo journalctl --vacuum-time=1s \
		-u $(INGESTION_SERVICE) -u $(ANALYTICS_SERVICE) \
		-u $(API_SERVICE) -u $(SIGNALS_SERVICE)
	@sudo journalctl --vacuum-size=100M
	@echo "$(YELLOW)→ Truncating active syslog-group files...$(NC)"
	@for f in /var/log/syslog /var/log/auth.log /var/log/kern.log \
	         /var/log/dpkg.log /var/log/ufw.log /var/log/fail2ban.log \
	         /var/log/letsencrypt/letsencrypt.log \
	         /var/log/apt/term.log /var/log/apt/history.log; do \
		[ -f "$$f" ] && sudo truncate -s 0 "$$f" && echo "    cleared $$f"; \
	done; true
	@echo "$(YELLOW)→ Truncating /var/log/zerogex/*.log...$(NC)"
	@sudo find /var/log/zerogex -maxdepth 2 -type f -name '*.log' \
		-exec truncate -s 0 {} \; 2>/dev/null || true
	@echo "$(YELLOW)→ Truncating /var/log/unattended-upgrades/*.log...$(NC)"
	@sudo find /var/log/unattended-upgrades -maxdepth 2 -type f -name '*.log' \
		-exec truncate -s 0 {} \; 2>/dev/null || true
	@echo "$(YELLOW)→ Truncating nginx access/error logs...$(NC)"
	@for f in /var/log/nginx/access.log /var/log/nginx/error.log; do \
		[ -f "$$f" ] && sudo truncate -s 0 "$$f" && echo "    cleared $$f"; \
	done; true
	@echo "$(YELLOW)→ Removing rotated + gzipped log files under /var/log/...$(NC)"
	@sudo find /var/log -maxdepth 3 -type f \( \
		-name '*.gz' -o \
		-name '*.xz' -o \
		-name '*.old' -o \
		-regex '.*\.[0-9]+\(\.log\)?$$' -o \
		-regex '.*\.log\.[0-9]+$$' \
	\) -delete -print 2>/dev/null | sed 's/^/    deleted /' || true
	@echo "$(YELLOW)→ Reopening file handles (rsyslog HUP, nginx reload)...$(NC)"
	@sudo systemctl kill -s HUP rsyslog 2>/dev/null || true
	@sudo systemctl reload nginx 2>/dev/null || true
	@echo ""
	@echo "$(BLUE)Disk usage AFTER:$(NC)"; df -h / | tail -1
	@echo "$(GREEN)✅ Logs cleared$(NC)"

# =============================================================================
# Run Components (from run.py)
# =============================================================================

.PHONY: run-auth
run-auth: ## Test TradeStation authentication
	@echo "$(BLUE)=== Testing TradeStation Authentication ===$(NC)"
	@$(VENV_PYTHON) -m src.ingestion.tradestation_auth

.PHONY: run-client
run-client: ## Test TradeStation API client (TEST, SYMBOL, BARS_BACK, INTERVAL, UNIT, QUERY, DEBUG, TEST_HISTORICAL)
	@echo "$(BLUE)=== Testing TradeStation Client ===$(NC)"
	@$(VENV_PYTHON) -m src.ingestion.tradestation_client \
		$(if $(TEST),--test '$(TEST)') \
		$(if $(SYMBOL),--symbol '$(SYMBOL)') \
		$(if $(BARS_BACK),--bars-back '$(BARS_BACK)') \
		$(if $(INTERVAL),--interval '$(INTERVAL)') \
		$(if $(UNIT),--unit '$(UNIT)') \
		$(if $(QUERY),--query '$(QUERY)') \
		$(if $(DEBUG),--debug) \
		$(if $(TEST_HISTORICAL),--test-historical)

.PHONY: alias-check
alias-check: ## Resolve alias mapping (optional: ALIASES, ALIAS, INPUT)
	@echo "$(BLUE)=== Symbol Alias Resolution Check ===$(NC)"
	@ALIASES_ENV='$(ALIASES)'; \
	if [ -z "$$ALIASES_ENV" ] && [ -n "$(ALIAS)" ] && [ -n "$(TICKER)" ]; then \
		ALIASES_ENV='$(ALIAS)=$(TICKER)'; \
	fi; \
	INPUT_VALUE='$(or $(INPUT),SPY)'; \
	if [ -z "$$ALIASES_ENV" ]; then \
		echo "$(YELLOW)No ALIASES/ALIAS provided; using current SYMBOL_ALIASES from environment/.env$(NC)"; \
		$(VENV_PYTHON) -c "from src.symbols import parse_underlyings, get_symbol_aliases; print('SYMBOL_ALIASES=', get_symbol_aliases()); print('INPUT=', '$$INPUT_VALUE'); print('RESOLVED=', parse_underlyings('$$INPUT_VALUE'))"; \
	else \
		$(VENV_PYTHON) -c "import os,sys; from src.symbols import parse_underlyings, get_symbol_aliases; os.environ['SYMBOL_ALIASES']=sys.argv[1]; input_value=sys.argv[2]; print('SYMBOL_ALIASES=', get_symbol_aliases()); print('INPUT=', input_value); print('RESOLVED=', parse_underlyings(input_value))" "$$ALIASES_ENV" "$$INPUT_VALUE"; \
	fi

.PHONY: run-stream
run-stream: ## Test real-time streaming
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)TESTING STREAM MANAGER$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "Note: This is a standalone test of the streaming component."
	@echo "      For production streaming, use 'make run-ingest' or 'make ingestion-start'"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@$(VENV_PYTHON) -m src.ingestion.stream_manager

.PHONY: run-ingest
run-ingest: ## Run main ingestion engine (forward-only)
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)RUNNING MAIN INGESTION ENGINE (FORWARD-ONLY)$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "Note: Main engine only streams forward-looking data."
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@$(VENV_PYTHON) -m src.ingestion.main_engine

.PHONY: run-ingest-alias
run-ingest-alias: ## Run ingestion engine with alias-aware INPUT (INPUT, ALIASES or ALIAS+TICKER)
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)RUNNING INGESTION WITH ALIAS RESOLUTION$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@ALIASES_ENV='$(ALIASES)'; \
	if [ -z "$$ALIASES_ENV" ] && [ -n "$(ALIAS)" ] && [ -n "$(TICKER)" ]; then \
		ALIASES_ENV='$(ALIAS)=$(TICKER)'; \
	fi; \
	INPUT_VALUE='$(or $(INPUT),SPY)'; \
	if [ -z "$$ALIASES_ENV" ]; then \
		RESOLVED="$$( $(VENV_PYTHON) -c "from src.symbols import parse_underlyings; print(','.join(parse_underlyings('$$INPUT_VALUE')))")"; \
		echo "Using environment/.env SYMBOL_ALIASES"; \
	else \
		RESOLVED="$$( $(VENV_PYTHON) -c "import os,sys; from src.symbols import parse_underlyings; os.environ['SYMBOL_ALIASES']=sys.argv[1]; print(','.join(parse_underlyings(sys.argv[2])))" "$$ALIASES_ENV" "$$INPUT_VALUE")"; \
		echo "Using override SYMBOL_ALIASES=$$ALIASES_ENV"; \
	fi; \
	echo "INPUT=$$INPUT_VALUE"; \
	echo "RESOLVED_UNDERLYINGS=$$RESOLVED"; \
	if [ -z "$$RESOLVED" ]; then \
		echo "$(RED)No resolved underlyings; aborting$(NC)"; \
		exit 1; \
	fi; \
	$(VENV_PYTHON) -m src.ingestion.main_engine --underlyings "$$RESOLVED" \
		$(if $(EXPIRATIONS),--expirations '$(EXPIRATIONS)') \
		$(if $(STRIKE_PCT),--strike-pct '$(STRIKE_PCT)') \
		$(if $(DEBUG),--debug)

.PHONY: run-analytics
run-analytics: ## Run analytics engine
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)RUNNING ANALYTICS ENGINE$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "Note: Analytics engine calculates GEX and Max Pain from database data."
	@echo "      Runs independently of ingestion on configured interval."
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@$(VENV_PYTHON) -m src.analytics.main_engine

.PHONY: run-analytics-once
run-analytics-once: ## Run analytics once (testing)
	@echo "$(BLUE)=== Running Analytics Once (Testing) ===$(NC)"
	@$(VENV_PYTHON) -m src.analytics.main_engine --once

.PHONY: run-greeks
run-greeks: ## Test Greeks calculator
	@echo "$(BLUE)=== Testing Greeks Calculator ===$(NC)"
	@$(VENV_PYTHON) -m src.ingestion.greeks_calculator

.PHONY: run-iv
run-iv: ## Test IV calculator
	@echo "$(BLUE)=== Testing IV Calculator ===$(NC)"
	@$(VENV_PYTHON) -m src.ingestion.iv_calculator

.PHONY: run-config
run-config: ## Show current configuration
	@echo "$(BLUE)=== ZeroGEX Configuration ===$(NC)"
	@$(VENV_PYTHON) -c "from src.config import print_config; print_config()"

# =============================================================================
# Quick Stats
# =============================================================================

.PHONY: stats
stats: ## Show overall data statistics
	@echo "$(BLUE)=== ZeroGEX Data Statistics ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			'Underlying Bars' as table_name, \
			COUNT(*) as total_rows, \
			MIN(timestamp AT TIME ZONE 'America/New_York') as earliest, \
			MAX(timestamp AT TIME ZONE 'America/New_York') as latest \
		FROM underlying_quotes \
		UNION ALL \
		SELECT \
			'Option Quotes', \
			COUNT(*), \
			MIN(timestamp AT TIME ZONE 'America/New_York'), \
			MAX(timestamp AT TIME ZONE 'America/New_York') \
		FROM option_chains \
		UNION ALL \
		SELECT \
			'GEX Summary', \
			COUNT(*), \
			MIN(timestamp AT TIME ZONE 'America/New_York'), \
			MAX(timestamp AT TIME ZONE 'America/New_York') \
		FROM gex_summary \
		UNION ALL \
		SELECT \
			'GEX by Strike', \
			COUNT(*), \
			MIN(timestamp AT TIME ZONE 'America/New_York'), \
			MAX(timestamp AT TIME ZONE 'America/New_York') \
		FROM gex_by_strike;"

.PHONY: latest
latest: ## Show latest data from all tables
	@echo "$(BLUE)=== Latest Underlying Bar ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			symbol, \
			timestamp AT TIME ZONE 'America/New_York' as time_et, \
			open, high, low, close, \
			up_volume, down_volume, \
			CASE \
				WHEN (up_volume + down_volume) > 0 \
				THEN ROUND((up_volume::numeric / (up_volume + down_volume) * 100), 1) \
				ELSE 0 \
			END as up_pct \
		FROM underlying_quotes \
		ORDER BY timestamp DESC \
		LIMIT 1;"
	@echo ""
	@echo "$(BLUE)=== Latest Option Quotes (Top 5 by Volume) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			option_symbol, \
			timestamp AT TIME ZONE 'America/New_York' as time_et, \
			strike, \
			expiration, \
			option_type, \
			last, \
			volume, \
			open_interest, \
			delta, \
			gamma \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		ORDER BY volume DESC \
		LIMIT 5;"
	@echo ""
	@echo "$(BLUE)=== Latest GEX Summary ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			underlying, \
			timestamp AT TIME ZONE 'America/New_York' as time_et, \
			max_gamma_strike, \
			max_gamma_value, \
			gamma_flip_point, \
			max_pain, \
			put_call_ratio, \
			total_net_gex \
		FROM gex_summary \
		ORDER BY timestamp DESC \
		LIMIT 1;"

.PHONY: today
today: ## Show today's data summary
	@echo "$(BLUE)=== Today's Data Summary ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			COUNT(*) as bars_today, \
			MIN(timestamp AT TIME ZONE 'America/New_York') as first_bar, \
			MAX(timestamp AT TIME ZONE 'America/New_York') as last_bar, \
			SUM(up_volume + down_volume) as total_volume, \
			ROUND(AVG(close), 2) as avg_price \
		FROM underlying_quotes \
		WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE;"
	@echo ""
	@$(PSQL) -c "\
		SELECT \
			COUNT(DISTINCT option_symbol) as unique_contracts, \
			COUNT(*) as total_quotes, \
			SUM(volume) as total_volume, \
			COUNT(DISTINCT strike) as unique_strikes \
		FROM option_chains \
		WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE;"
	@echo ""
	@$(PSQL) -c "\
		SELECT \
			COUNT(*) as gex_calculations, \
			MIN(timestamp AT TIME ZONE 'America/New_York') as first_calc, \
			MAX(timestamp AT TIME ZONE 'America/New_York') as last_calc \
		FROM gex_summary \
		WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE;"

# =============================================================================
# Underlying Quotes
# =============================================================================

.PHONY: underlying
underlying: ## Last 10 underlying bars
	@echo "$(BLUE)=== Last 10 Underlying Bars ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			symbol, \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI') as time_et, \
			open, high, low, close, \
			up_volume, down_volume, \
			CASE \
				WHEN (up_volume + down_volume) > 0 \
				THEN ROUND((up_volume::numeric / (up_volume + down_volume) * 100), 1) \
				ELSE 0 \
			END as up_pct \
		FROM underlying_quotes \
		ORDER BY timestamp DESC \
		LIMIT 10;"

.PHONY: underlying-live
underlying-live: ## Live latest underlying row (default SPY) refreshed every second in-place
	@clear
	@echo "$(BLUE)=== Live Underlying ($(UNDERLYING_LIVE_SYMBOL)) — Ctrl+C to stop ===$(NC)"
	@while true; do \
		output="$$( $(PSQL) -c "\
				SELECT \
					symbol, \
					timestamp, \
					open, \
					high, \
					low, \
					close, \
					ROUND(((close - open) / NULLIF(open, 0) * 100), 2) AS pct_change, \
					up_volume, \
					down_volume, \
					created_at, \
					updated_at \
				FROM underlying_quotes \
			WHERE symbol = '$(UNDERLYING_LIVE_SYMBOL)' \
			ORDER BY timestamp DESC \
			LIMIT 1;" )"; \
		printf "\033[H"; \
		echo "$$output"; \
		sleep 1; \
	done

.PHONY: underlying-live-raw
underlying-live-raw: ## Live latest underlying row (raw values, default SPY) refreshed every second in-place
	@echo "$(BLUE)=== Live Underlying Raw ($(UNDERLYING_LIVE_SYMBOL)) — Ctrl+C to stop ===$(NC)"
	@while true; do \
		clear; \
		echo "$(BLUE)=== Live Underlying Raw ($(UNDERLYING_LIVE_SYMBOL)) — Ctrl+C to stop ===$(NC)"; \
		$(PSQL) -c "\
			SELECT \
				symbol, \
				timestamp, \
				open, \
				high, \
				low, \
				close, \
				up_volume, \
				down_volume, \
				created_at, \
				updated_at \
			FROM underlying_quotes \
			WHERE symbol = '$(UNDERLYING_LIVE_SYMBOL)' \
			ORDER BY timestamp DESC \
			LIMIT 1;"; \
		sleep 1; \
	done

.PHONY: underlying-today
underlying-today: ## Today's underlying bars
	@echo "$(BLUE)=== Today's Underlying Bars ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time_et, \
			open, high, low, close, \
			up_volume + down_volume as total_volume, \
			ROUND((up_volume::numeric / NULLIF(up_volume + down_volume, 0) * 100), 1) as up_pct \
		FROM underlying_quotes \
		WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE \
		ORDER BY timestamp DESC;"

.PHONY: underlying-volume
underlying-volume: ## Volume analysis for today
	@echo "$(BLUE)=== Today's Volume Analysis ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			COUNT(*) as bars, \
			SUM(up_volume) as total_up_volume, \
			SUM(down_volume) as total_down_volume, \
			SUM(up_volume + down_volume) as total_volume, \
			ROUND(AVG(up_volume::numeric / NULLIF(up_volume + down_volume, 0) * 100), 1) as avg_buying_pressure_pct, \
			ROUND(MIN(close), 2) as low_price, \
			ROUND(MAX(close), 2) as high_price \
		FROM underlying_quotes \
		WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE;"

# =============================================================================
# Option Chains
# =============================================================================

.PHONY: options-raw
options-raw: ## Show raw option data (what's actually stored)
	@echo "$(BLUE)=== Raw Option Data (Last 5 Records) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			option_symbol, \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI') as time_et, \
			strike, \
			option_type, \
			last, \
			bid, \
			ask, \
			volume, \
			open_interest, \
			implied_volatility, \
			delta, \
			gamma, \
			theta, \
			vega, \
			updated_at AT TIME ZONE 'America/New_York' as updated_et \
		FROM option_chains \
		ORDER BY timestamp DESC \
		LIMIT 5;"

.PHONY: options-fields
options-fields: ## Check which fields are populated
	@echo "$(BLUE)=== Option Fields Population Check ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			COUNT(*) as total_records, \
			COUNT(last) FILTER (WHERE last > 0) as has_last, \
			COUNT(bid) FILTER (WHERE bid > 0) as has_bid, \
			COUNT(ask) FILTER (WHERE ask > 0) as has_ask, \
			COUNT(volume) FILTER (WHERE volume > 0) as has_volume, \
			COUNT(open_interest) FILTER (WHERE open_interest > 0) as has_open_interest, \
			COUNT(implied_volatility) FILTER (WHERE implied_volatility IS NOT NULL) as has_iv, \
			COUNT(delta) FILTER (WHERE delta IS NOT NULL) as has_delta, \
			COUNT(gamma) FILTER (WHERE gamma IS NOT NULL) as has_gamma, \
			COUNT(theta) FILTER (WHERE theta IS NOT NULL) as has_theta, \
			COUNT(vega) FILTER (WHERE vega IS NOT NULL) as has_vega \
		FROM option_chains \
		WHERE timestamp > NOW() - INTERVAL '1 hour';"
	@echo ""
	@echo "$(YELLOW)Note: OpenInterest is typically 0 in real-time data.$(NC)"
	@echo "      OI is updated once daily after market settlement."

.PHONY: options
options: ## Last 10 option quotes
	@echo "$(BLUE)=== Last 10 Option Quotes ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			option_symbol, \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI') as time_et, \
			strike, \
			expiration, \
			option_type, \
			last, \
			volume, \
			open_interest \
		FROM option_chains \
		ORDER BY timestamp DESC, volume DESC \
		LIMIT 10;"

.PHONY: options-latest
options-latest: ## Latest option quotes (top 10 by volume)
	@echo "$(BLUE)=== Latest Option Quotes (Top 10 by Volume) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			option_symbol, \
			strike, \
			expiration, \
			option_type, \
			last, \
			volume, \
			open_interest, \
			delta, \
			gamma \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		ORDER BY volume DESC \
		LIMIT 10;"

.PHONY: options-today
options-today: ## Today's option activity summary
	@echo "$(BLUE)=== Today's Option Activity ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			expiration, \
			option_type, \
			COUNT(DISTINCT option_symbol) as contracts, \
			SUM(volume) as total_volume, \
			SUM(open_interest) as total_oi, \
			ROUND(AVG(last), 2) as avg_price \
		FROM option_chains \
		WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE \
		GROUP BY expiration, option_type \
		ORDER BY expiration, option_type;"

.PHONY: options-strikes
options-strikes: ## Active strikes summary
	@echo "$(BLUE)=== Active Strikes (Latest Timestamp) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			strike, \
			expiration, \
			COUNT(*) FILTER (WHERE option_type = 'C') as calls, \
			COUNT(*) FILTER (WHERE option_type = 'P') as puts, \
			SUM(volume) FILTER (WHERE option_type = 'C') as call_volume, \
			SUM(volume) FILTER (WHERE option_type = 'P') as put_volume \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		GROUP BY strike, expiration \
		ORDER BY expiration, strike;"

# =============================================================================
# Greeks & Analytics
# =============================================================================

.PHONY: greeks
greeks: ## Latest Greeks by strike
	@echo "$(BLUE)=== Latest Greeks by Strike ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			strike, \
			expiration, \
			option_type, \
			last, \
			delta, \
			gamma, \
			theta, \
			vega, \
			volume, \
			open_interest \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		AND (delta IS NOT NULL OR gamma IS NOT NULL) \
		ORDER BY expiration, strike, option_type;"

.PHONY: greeks-summary
greeks-summary: ## Greeks summary statistics
	@echo "$(BLUE)=== Greeks Summary (Latest Data) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			option_type, \
			COUNT(*) as contracts, \
			ROUND(AVG(delta), 4) as avg_delta, \
			ROUND(AVG(gamma), 6) as avg_gamma, \
			ROUND(AVG(theta), 4) as avg_theta, \
			ROUND(AVG(vega), 4) as avg_vega \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		AND delta IS NOT NULL \
		GROUP BY option_type;"

.PHONY: gex-summary
gex-summary: ## Show latest GEX summary
	@echo "$(BLUE)=== Latest GEX Summary ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			underlying, \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI') as time_et, \
			max_gamma_strike, \
			TO_CHAR(max_gamma_value, 'FM999,999,999') as max_gamma, \
			gamma_flip_point, \
			put_call_ratio, \
			max_pain, \
			TO_CHAR(total_net_gex, 'FM999,999,999') as net_gex, \
			TO_CHAR(created_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI:SS') as calculated_at \
		FROM gex_summary \
		ORDER BY timestamp DESC \
		LIMIT 10;"

.PHONY: gex-strikes
gex-strikes: ## Show GEX by strike (top 20)
	@echo "$(BLUE)=== GEX by Strike (Top 20) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			strike, \
			expiration, \
			TO_CHAR(net_gex, 'FM999,999,999') as net_gex, \
			TO_CHAR(call_oi, 'FM999,999') as call_oi, \
			TO_CHAR(put_oi, 'FM999,999') as put_oi, \
			TO_CHAR(vanna_exposure, 'FM999,999') as vanna, \
			TO_CHAR(charm_exposure, 'FM999,999') as charm \
		FROM gex_by_strike \
		WHERE timestamp = (SELECT MAX(timestamp) FROM gex_by_strike) \
		ORDER BY ABS(net_gex) DESC \
		LIMIT 20;"

.PHONY: gex-preview
gex-preview: ## Preview GEX calculation data
	@echo "$(BLUE)=== GEX Preview (Gamma by Strike) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			strike, \
			expiration, \
			SUM(gamma) FILTER (WHERE option_type = 'C') as call_gamma, \
			SUM(gamma) FILTER (WHERE option_type = 'P') as put_gamma, \
			SUM(gamma * open_interest) FILTER (WHERE option_type = 'C') as call_gex, \
			SUM(gamma * open_interest) FILTER (WHERE option_type = 'P') as put_gex, \
			SUM(volume) FILTER (WHERE option_type = 'C') as call_vol, \
			SUM(volume) FILTER (WHERE option_type = 'P') as put_vol \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		AND gamma IS NOT NULL \
		GROUP BY strike, expiration \
		ORDER BY expiration, strike;"

# =============================================================================
# Real-Time Flow Analysis
# =============================================================================

.PHONY: flow-by-contract
flow-by-contract: ## Unified 5-min flow by (type, strike, expiration) (default: SPY, override: make flow-by-contract FLOW_SYMBOL=QQQ)
	@echo "$(BLUE)=== Option Flow by Contract ($(FLOW_SYMBOL), Most Recent 20 Rows) ===$(NC)"
	@echo "$(BLUE)Values are session day-to-date cumulative per contract (reset at 09:30 ET).$(NC)"
	@$(PSQL) -c "\
		SET statement_timeout = '10s'; \
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time, \
			symbol, \
			option_type AS typ, \
			strike, \
			expiration, \
			(expiration - CURRENT_DATE) as dte, \
			raw_volume, \
			TO_CHAR(raw_premium, 'FM999,999,999') as raw_premium, \
			net_volume, \
			TO_CHAR(net_premium, 'FM999,999,999') as net_premium, \
			TO_CHAR(underlying_price, 'FM999,990.99') as underlying_price \
		FROM flow_by_contract \
		WHERE symbol = '$(FLOW_SYMBOL)' \
		ORDER BY timestamp DESC, option_type, strike, expiration \
		LIMIT 20;"

.PHONY: flow-smart-money
flow-smart-money: ## Unusual activity detection
	@echo "$(BLUE)=== Smart Money Flow / Unusual Activity (SPY Current Session Top 20 by Notional) ===$(NC)"
	@$(VENV_PYTHON) -m src.tools.flow_smart_money_cli

.PHONY: flow-buying-pressure
flow-buying-pressure: ## Underlying buying/selling pressure
	@echo "$(BLUE)=== Underlying Buying Pressure (Most Recent 20 Rows) ===$(NC)"
	@$(PSQL) -c "\
		WITH quote_deltas AS ( \
			SELECT \
				timestamp, symbol, close, up_volume, down_volume, \
				COALESCE( \
					GREATEST( \
						up_volume - LAG(up_volume) OVER ( \
							PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York') \
							ORDER BY timestamp \
						), \
						0 \
					), \
					0 \
				) AS up_volume_delta, \
				COALESCE( \
					GREATEST( \
						down_volume - LAG(down_volume) OVER ( \
							PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York') \
							ORDER BY timestamp \
						), \
						0 \
					), \
					0 \
				) AS down_volume_delta \
			FROM underlying_quotes \
			WHERE symbol = '$(FLOW_SYMBOL)' \
		) \
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time, \
			symbol, \
			ROUND(close, 2) as price, \
			(up_volume_delta + down_volume_delta) as volume, \
			ROUND(CASE WHEN (up_volume + down_volume) > 0 THEN up_volume::numeric / (up_volume + down_volume) * 100 ELSE 50 END, 2) as buy_pct, \
			ROUND(CASE WHEN (up_volume_delta + down_volume_delta) > 0 THEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) * 100 ELSE 50 END, 2) as period_buy_pct, \
			ROUND(close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp), 2) as price_chg, \
			CASE \
				WHEN (up_volume_delta + down_volume_delta) = 0 THEN '⚪ Neutral' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) > 0.7 THEN '🟢 Strong Buying' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) > 0.55 THEN '✅ Buying' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) >= 0.45 THEN '⚪ Neutral' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) >= 0.3 THEN '❌ Selling' \
				ELSE '🔴 Strong Selling' \
			END as momentum \
		FROM quote_deltas \
		ORDER BY timestamp DESC \
		LIMIT 20;"

.PHONY: flow-live
flow-live: ## Combined real-time flow dashboard
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)REAL-TIME FLOW DASHBOARD$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@echo "$(GREEN)1. UNDERLYING BUYING PRESSURE (Last 10 Bars)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		WITH quote_deltas AS ( \
			SELECT \
				timestamp, symbol, close, up_volume, down_volume, \
				COALESCE( \
					GREATEST( \
						up_volume - LAG(up_volume) OVER ( \
							PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York') \
							ORDER BY timestamp \
						), \
						0 \
					), \
					0 \
				) AS up_volume_delta, \
				COALESCE( \
					GREATEST( \
						down_volume - LAG(down_volume) OVER ( \
							PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York') \
							ORDER BY timestamp \
						), \
						0 \
					), \
					0 \
				) AS down_volume_delta \
			FROM underlying_quotes \
			WHERE symbol = '$(FLOW_SYMBOL)' \
		) \
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time, \
			ROUND(close, 2) as price, \
			(up_volume_delta + down_volume_delta) as vol, \
			ROUND(CASE WHEN (up_volume_delta + down_volume_delta) > 0 THEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) * 100 ELSE 50 END, 2) as buy_pct, \
			CASE \
				WHEN (up_volume_delta + down_volume_delta) = 0 THEN '⚪ Neutral' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) > 0.7 THEN '🟢 Strong Buying' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) > 0.55 THEN '✅ Buying' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) >= 0.45 THEN '⚪ Neutral' \
				WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) >= 0.3 THEN '❌ Selling' \
				ELSE '🔴 Strong Selling' \
			END as momentum \
		FROM quote_deltas \
		ORDER BY timestamp DESC \
		LIMIT 10;"
	@echo ""
	@echo "$(GREEN)2. PUTS VS CALLS FLOW (Last 30 Minutes, per 5-min bucket)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SET statement_timeout = '10s'; \
		SELECT \
			TO_CHAR(bucket_ts AT TIME ZONE 'America/New_York', 'HH24:MI') as time, \
			SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE 0 END)::bigint as calls, \
			SUM(CASE WHEN option_type = 'P' THEN volume_delta ELSE 0 END)::bigint as puts, \
			SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE -volume_delta END)::bigint as net, \
			ROUND( \
				SUM(CASE WHEN option_type = 'P' THEN volume_delta ELSE 0 END)::numeric \
				/ NULLIF(SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE 0 END), 0), 2 \
			) as pc_ratio \
		FROM ( \
			SELECT \
				to_timestamp(floor(extract(epoch FROM timestamp) / 300) * 300) AS bucket_ts, \
				option_type, \
				volume_delta \
			FROM flow_contract_facts \
			WHERE symbol = '$(FLOW_SYMBOL)' \
			  AND timestamp > NOW() - INTERVAL '30 minutes' \
		) b \
		GROUP BY bucket_ts \
		ORDER BY bucket_ts DESC \
		LIMIT 10;"
	@echo ""
	@echo "$(GREEN)3. SMART MONEY / UNUSUAL ACTIVITY (Top 10)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		WITH option_chain_deltas AS ( \
			SELECT \
				timestamp, option_symbol, underlying, option_type, last, implied_volatility, delta, expiration, \
				COALESCE( \
					GREATEST( \
						volume - LAG(volume) OVER ( \
							PARTITION BY option_symbol, DATE(timestamp AT TIME ZONE 'America/New_York') \
							ORDER BY timestamp \
						), \
						0 \
					), \
					0 \
				) AS volume_delta \
			FROM option_chains \
			WHERE underlying = '$(FLOW_SYMBOL)' \
		), scored AS ( \
			SELECT \
				timestamp AT TIME ZONE 'America/New_York' as time_et, \
				option_symbol, option_type, volume_delta as flow, \
				CASE \
					WHEN volume_delta >= 500 THEN '🔥 Massive Block' \
					WHEN volume_delta >= 200 THEN '📦 Large Block' \
					WHEN volume_delta >= 100 THEN '📊 Medium Block' \
					ELSE '💼 Standard' \
				END as size_class, \
				LEAST(10, GREATEST(0, \
					CASE WHEN volume_delta >= 500 THEN 3 WHEN volume_delta >= 200 THEN 2 WHEN volume_delta >= 100 THEN 1 ELSE 0 END + \
					CASE WHEN volume_delta * last * 100 >= 500000 THEN 3 WHEN volume_delta * last * 100 >= 250000 THEN 2 WHEN volume_delta * last * 100 >= 100000 THEN 1 ELSE 0 END + \
					CASE WHEN implied_volatility > 1.0 THEN 2 WHEN implied_volatility > 0.6 THEN 1 ELSE 0 END + \
					CASE WHEN ABS(delta) < 0.15 THEN 1 ELSE 0 END + \
					CASE WHEN (expiration - CURRENT_DATE) <= 2 THEN 1 ELSE 0 END \
				)) as unusual_score \
			FROM option_chain_deltas \
			WHERE volume_delta > 0 \
		) \
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			SUBSTRING(option_symbol, 1, 15) as contract, \
			option_type as type, \
			flow, \
			unusual_score as score, \
			size_class \
		FROM scored \
		ORDER BY unusual_score DESC, flow DESC \
		LIMIT 10;"
	@echo ""
	@echo "$(GREEN)4. TOP STRIKES BY FLOW (Top 10)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SET statement_timeout = '10s'; \
		WITH strike_window AS ( \
			SELECT \
				strike, \
				SUM(volume_delta)::bigint AS total_flow, \
				SUM(CASE WHEN option_type = 'C' \
				         THEN volume_delta \
				         ELSE -volume_delta END)::bigint AS net_flow \
			FROM flow_contract_facts \
			WHERE symbol = '$(FLOW_SYMBOL)' \
				AND timestamp > NOW() - INTERVAL '30 minutes' \
			GROUP BY strike \
		) \
		SELECT \
			strike, \
			total_flow as total, \
			net_flow, \
			CASE \
				WHEN net_flow > 100 THEN '🟢 Calls' \
				WHEN net_flow < -100 THEN '🔴 Puts' \
				ELSE '⚪ Mixed' \
			END as bias \
		FROM strike_window \
		ORDER BY total_flow DESC \
		LIMIT 10;"
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"

# =============================================================================
# Technicals Decision Support
# =============================================================================

.PHONY: flow-reset-session-facts
flow-reset-session-facts: ## Delete one ET session from flow_contract_facts (defaults to today ET)
	@echo "$(YELLOW)Deleting flow_contract_facts for $(FLOW_SYMBOL) on $(FLOW_REBUILD_DATE) ET ($(FLOW_REBUILD_START_ET)-$(FLOW_REBUILD_END_ET))...$(NC)"
	@$(PSQL) -c " \
		WITH deleted AS ( \
			DELETE FROM flow_contract_facts \
			WHERE symbol = '$(FLOW_SYMBOL)' \
			  AND (timestamp AT TIME ZONE 'America/New_York')::date = DATE '$(FLOW_REBUILD_DATE)' \
			  AND (timestamp AT TIME ZONE 'America/New_York')::time >= TIME '$(FLOW_REBUILD_START_ET)' \
			  AND (timestamp AT TIME ZONE 'America/New_York')::time <= TIME '$(FLOW_REBUILD_END_ET)' \
			RETURNING 1 \
		) \
		SELECT COUNT(*) AS deleted_rows FROM deleted;"

.PHONY: flow-rebuild-session
flow-rebuild-session: flow-reset-session-facts ## Reset one ET session, restart API, and trigger flow refresh
	@echo "$(YELLOW)Restarting API service to pick up env (e.g. FLOW_CANONICAL_BACKFILL_MINUTES)...$(NC)"
	@$(MAKE) api-restart
	@echo "$(YELLOW)Waiting for API readiness (/api/health)...$(NC)"
	@for i in $$(seq 1 30); do \
		if curl -fsS "http://localhost:8000/api/health" > /dev/null; then \
			echo "$(GREEN)API is ready.$(NC)"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "$(RED)API did not become ready within 30s.$(NC)"; \
			exit 1; \
		fi; \
		sleep 1; \
	done
	@echo "$(YELLOW)Triggering /api/flow/by-contract refresh for $(FLOW_SYMBOL)...$(NC)"
	@curl -fsS "http://localhost:8000/api/flow/by-contract?symbol=$(FLOW_SYMBOL)&session=current" > /dev/null
	@echo "$(GREEN)Rebuild trigger sent. Validate with: make flow-by-contract FLOW_SYMBOL=$(FLOW_SYMBOL)$(NC)"

.PHONY: vwap
vwap: ## VWAP deviation tracker
	@echo "$(BLUE)=== VWAP Deviation (Last 30 mins) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			symbol, \
			ROUND(price, 2) as price, \
			ROUND(vwap, 2) as vwap, \
			vwap_deviation_pct as vwap_dev, \
			vwap_position \
		FROM underlying_vwap_deviation \
		WHERE timestamp > NOW() - INTERVAL '30 minutes' \
		ORDER BY timestamp DESC \
		LIMIT 30;"

.PHONY: orb
orb: ## Opening range breakout status
	@echo "$(BLUE)=== Opening Range Breakout ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			symbol, \
			ROUND(current_price, 2) as price, \
			ROUND(orb_high, 2) as orb_high, \
			ROUND(orb_low, 2) as orb_low, \
			ROUND(orb_range, 2) as range, \
			orb_status \
		FROM opening_range_breakout \
		ORDER BY timestamp DESC \
		LIMIT 10;"

.PHONY: hedge-pressure
hedge-pressure: ## Dealer hedging pressure
	@echo "$(BLUE)=== Dealer Hedging Pressure (Last 30 mins) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			symbol, \
			ROUND(current_price, 2) as price, \
			ROUND(price_change, 2) as chg, \
			TO_CHAR(expected_hedge_shares, 'FM999,999') as hedge_shares, \
			hedge_pressure \
		FROM dealer_hedging_pressure \
		ORDER BY timestamp DESC \
		LIMIT 20;"

.PHONY: volume-spikes
volume-spikes: ## Unusual volume detection
	@echo "$(BLUE)=== Unusual Volume Spikes ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			symbol, \
			ROUND(price, 2) as price, \
			TO_CHAR(current_volume, 'FM999,999') as volume, \
			volume_ratio as vol_ratio, \
			volume_sigma as sigma, \
			buying_pressure_pct as buy_pct, \
			volume_class \
		FROM unusual_volume_spikes \
		ORDER BY volume_sigma DESC \
		LIMIT 20;"

.PHONY: divergence
divergence: ## Momentum divergence signals
	@echo "$(BLUE)=== Momentum Divergence Signals (Most Recent 20 Rows) ===$(NC)"
	@$(PSQL) -c "\
		SET statement_timeout = '10s'; \
		WITH option_flow AS ( \
			SELECT \
				to_timestamp(floor(extract(epoch FROM timestamp) / 300) * 300) AS timestamp, \
				symbol, \
				SUM(CASE WHEN option_type = 'C' THEN premium_delta ELSE -premium_delta END)::numeric AS net_option_flow \
			FROM flow_contract_facts \
			WHERE symbol = '$(FLOW_SYMBOL)' \
			GROUP BY 1, 2 \
		), base AS ( \
			SELECT \
				u.timestamp, \
				u.symbol, \
				u.close as price, \
				u.close - LAG(u.close, 5) OVER (PARTITION BY u.symbol ORDER BY u.timestamp) AS price_change_5min, \
				(u.up_volume - u.down_volume)::bigint AS net_volume, \
				of.net_option_flow \
			FROM underlying_quotes u \
			LEFT JOIN option_flow of ON of.timestamp = u.timestamp AND of.symbol = u.symbol \
			WHERE u.symbol = '$(FLOW_SYMBOL)' \
		) \
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time, \
			symbol, \
			ROUND(price, 2) as price, \
			ROUND(price_change_5min, 2) as chg_5m, \
			TO_CHAR(net_option_flow, 'FM999,999') as opt_flow, \
			CASE \
				WHEN price_change_5min > 0 AND net_option_flow < -50000 THEN '🚨 Bearish Divergence (Price Up, Puts Buying)' \
				WHEN price_change_5min < 0 AND net_option_flow > 50000 THEN '🚨 Bullish Divergence (Price Down, Calls Buying)' \
				WHEN price_change_5min > 0 AND net_option_flow > 50000 THEN '🟢 Bullish Confirmation' \
				WHEN price_change_5min < 0 AND net_option_flow < -50000 THEN '🔴 Bearish Confirmation' \
				WHEN price_change_5min > 0 AND net_volume < 0 THEN '⚠️ Weak Rally (Selling Volume)' \
				WHEN price_change_5min < 0 AND net_volume > 0 THEN '⚠️ Weak Selloff (Buying Volume)' \
				ELSE '⚪ Neutral' \
			END AS divergence_signal \
		FROM base \
		WHERE price_change_5min IS NOT NULL \
		ORDER BY timestamp DESC \
		LIMIT 20;"

.PHONY: technicals
technicals: ## Combined technicals dashboard
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)TECHNICALS DASHBOARD$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@echo "$(GREEN)1. VWAP DEVIATION$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			ROUND(price, 2) as price, \
			ROUND(vwap, 2) as vwap, \
			vwap_deviation_pct as dev, \
			vwap_position \
		FROM underlying_vwap_deviation \
		WHERE timestamp > NOW() - INTERVAL '30 minutes' \
		ORDER BY timestamp DESC \
		LIMIT 10;"
	@echo ""
	@echo "$(GREEN)2. OPENING RANGE BREAKOUT$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			ROUND(current_price, 2) as price, \
			ROUND(orb_high, 2) as high, \
			ROUND(orb_low, 2) as low, \
			orb_status \
		FROM opening_range_breakout \
		ORDER BY timestamp DESC \
		LIMIT 5;"
	@echo ""
	@echo "$(GREEN)4. VOLUME SPIKES (Top 10)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			ROUND(price, 2) as price, \
			volume_sigma as sigma, \
			buying_pressure_pct as buy_pct, \
			volume_class \
		FROM unusual_volume_spikes \
		ORDER BY volume_sigma DESC \
		LIMIT 10;"
	@echo ""
	@echo "$(GREEN)5. DIVERGENCE SIGNALS$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SET statement_timeout = '10s'; \
		WITH option_flow AS ( \
			SELECT \
				to_timestamp(floor(extract(epoch FROM timestamp) / 300) * 300) AS timestamp, \
				symbol, \
				SUM(CASE WHEN option_type = 'C' THEN premium_delta ELSE -premium_delta END)::numeric AS net_option_flow \
			FROM flow_contract_facts \
			WHERE symbol = '$(FLOW_SYMBOL)' \
				AND timestamp > NOW() - INTERVAL '70 minutes' \
			GROUP BY 1, 2 \
		), base AS ( \
			SELECT \
				u.timestamp, \
				u.close as price, \
				u.close - LAG(u.close, 5) OVER (PARTITION BY u.symbol ORDER BY u.timestamp) AS price_change_5min, \
				(u.up_volume - u.down_volume)::bigint AS net_volume, \
				of.net_option_flow \
			FROM underlying_quotes u \
			LEFT JOIN option_flow of ON of.timestamp = u.timestamp AND of.symbol = u.symbol \
			WHERE u.symbol = '$(FLOW_SYMBOL)' \
				AND u.timestamp > NOW() - INTERVAL '70 minutes' \
		) \
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time, \
			ROUND(price, 2) as price, \
			ROUND(price_change_5min, 2) as chg_5m, \
			CASE \
				WHEN price_change_5min > 0 AND net_option_flow < -50000 THEN '🚨 Bearish Divergence (Price Up, Puts Buying)' \
				WHEN price_change_5min < 0 AND net_option_flow > 50000 THEN '🚨 Bullish Divergence (Price Down, Calls Buying)' \
				WHEN price_change_5min > 0 AND net_option_flow > 50000 THEN '🟢 Bullish Confirmation' \
				WHEN price_change_5min < 0 AND net_option_flow < -50000 THEN '🔴 Bearish Confirmation' \
				WHEN price_change_5min > 0 AND net_volume < 0 THEN '⚠️ Weak Rally (Selling Volume)' \
				WHEN price_change_5min < 0 AND net_volume > 0 THEN '⚠️ Weak Selloff (Buying Volume)' \
				ELSE '⚪ Neutral' \
			END AS divergence_signal \
		FROM base \
		WHERE price_change_5min IS NOT NULL \
			AND ( \
				(price_change_5min > 0 AND net_option_flow < -50000) OR \
				(price_change_5min < 0 AND net_option_flow > 50000) OR \
				(price_change_5min > 0 AND net_option_flow > 50000) OR \
				(price_change_5min < 0 AND net_option_flow < -50000) OR \
				(price_change_5min > 0 AND net_volume < 0) OR \
				(price_change_5min < 0 AND net_volume > 0) \
			) \
		ORDER BY timestamp DESC \
		LIMIT 10;"
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"

# =============================================================================
# Max Pain
# =============================================================================

.PHONY: max-pain-current
max-pain-current: ## Latest max pain snapshot (default: SPY, override: make max-pain-current FLOW_SYMBOL=QQQ)
	@echo "=== Max Pain Current ($(FLOW_SYMBOL), Latest OI Snapshot) ==="
	@$(PSQL) -c "\
		SELECT \
			symbol, \
			as_of_date, \
			TO_CHAR(source_timestamp AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI:SS TZ') AS source_timestamp_et, \
			underlying_price, \
			max_pain, \
			difference, \
			jsonb_array_length(expirations) AS num_expirations \
		FROM max_pain_oi_snapshot \
		WHERE symbol = '$(FLOW_SYMBOL)' \
		ORDER BY as_of_date DESC \
		LIMIT 1;"

.PHONY: max-pain-expirations
max-pain-expirations: ## Max pain by expiration (default: SPY, override: make max-pain-expirations FLOW_SYMBOL=QQQ)
	@echo "=== Max Pain by Expiration ($(FLOW_SYMBOL), Latest Snapshot) ==="
	@$(PSQL) -c "\
		WITH latest_snapshot AS ( \
			SELECT as_of_date \
			FROM max_pain_oi_snapshot \
			WHERE symbol = '$(FLOW_SYMBOL)' \
			ORDER BY as_of_date DESC \
			LIMIT 1 \
		) \
		SELECT \
			e.expiration, \
			e.max_pain, \
			e.difference_from_underlying, \
			jsonb_array_length(e.strikes) AS num_strikes \
		FROM max_pain_oi_snapshot_expiration e \
		WHERE e.symbol = '$(FLOW_SYMBOL)' \
			AND e.as_of_date = (SELECT as_of_date FROM latest_snapshot) \
		ORDER BY e.expiration;"

.PHONY: max-pain-strikes
max-pain-strikes: ## Max pain strikes for nearest expiration (default: SPY, override: make max-pain-strikes FLOW_SYMBOL=QQQ)
	@echo "=== Max Pain Strikes for Nearest Expiration ($(FLOW_SYMBOL)) ==="
	@$(PSQL) -c "\
		WITH latest_snapshot AS ( \
			SELECT as_of_date \
			FROM max_pain_oi_snapshot \
			WHERE symbol = '$(FLOW_SYMBOL)' \
			ORDER BY as_of_date DESC \
			LIMIT 1 \
		), \
		nearest_exp AS ( \
			SELECT expiration, strikes \
			FROM max_pain_oi_snapshot_expiration \
			WHERE symbol = '$(FLOW_SYMBOL)' \
				AND as_of_date = (SELECT as_of_date FROM latest_snapshot) \
			ORDER BY expiration \
			LIMIT 1 \
		) \
		SELECT \
			(strike->>'settlement_price')::numeric AS settlement_price, \
			(strike->>'call_notional')::numeric AS call_notional, \
			(strike->>'put_notional')::numeric AS put_notional, \
			(strike->>'total_notional')::numeric AS total_notional \
		FROM nearest_exp, \
			jsonb_array_elements(strikes) AS strike \
		ORDER BY (strike->>'settlement_price')::numeric \
		LIMIT 20;"

# =============================================================================
# Signal Engine — DB Queries
# =============================================================================

.PHONY: signals
signals: ## Alias for latest score snapshot
	@$(MAKE) signals-score FLOW_SYMBOL=$(FLOW_SYMBOL)

.PHONY: signals-detail
signals-detail: ## Alias for latest score snapshot with components
	@$(MAKE) signals-score FLOW_SYMBOL=$(FLOW_SYMBOL)

.PHONY: signals-raw
signals-raw: ## Raw latest rows from signal_scores
	@$(eval LIMIT ?= 10)
	@$(PSQL) -c "SELECT * FROM signal_scores WHERE underlying='$(FLOW_SYMBOL)' ORDER BY timestamp DESC LIMIT $(LIMIT);"

.PHONY: signals-components
signals-components: ## Show latest component payload keys
	@$(PSQL) -c "WITH latest AS (SELECT components FROM signal_scores WHERE underlying='$(FLOW_SYMBOL)' ORDER BY timestamp DESC LIMIT 1) SELECT key AS component, value FROM latest, jsonb_each(components) ORDER BY key;"

.PHONY: signals-live
signals-live: ## Open/live signal trades from DB
	@echo "$(BLUE)=== Live Signal Trades ===$(NC)"
	@$(PSQL) -c "SELECT underlying, opened_at AT TIME ZONE 'America/New_York' AS opened_et, direction, option_symbol, entry_price, current_price, quantity_open, total_pnl, pnl_percent FROM signal_trades WHERE status='open' ORDER BY opened_at DESC;"

.PHONY: signals-wipe-open
signals-wipe-open: ## Delete all OPEN signal trades (with confirmation)
	@echo "$(RED)⚠️  WARNING: This will permanently delete ALL open signal trades!$(NC)"
	@read -p "Are you sure? Type 'yes' to confirm: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "$(YELLOW)Deleting open signal trades...$(NC)"; \
		$(PSQL) -c "DELETE FROM signal_trades WHERE status = 'open';"; \
		echo "$(GREEN)✅ All open signal trades deleted$(NC)"; \
	else \
		echo "$(RED)❌ Aborted$(NC)"; \
	fi

.PHONY: signals-wipe-all
signals-wipe-all: ## Delete ALL signal trades (open + closed) (with confirmation)
	@echo "$(RED)⚠️  WARNING: This will permanently delete ALL signal trades (open AND closed)!$(NC)"
	@read -p "Are you sure? Type 'yes' to confirm: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "$(YELLOW)Deleting all signal trades...$(NC)"; \
		$(PSQL) -c "DELETE FROM signal_trades;"; \
		echo "$(GREEN)✅ All signal trades deleted$(NC)"; \
	else \
		echo "$(RED)❌ Aborted$(NC)"; \
	fi

.PHONY: signals-fresh-start
signals-fresh-start: ## Clear historic signal-trade state (signal_trades + portfolio_snapshots + signal_engine_trade_ideas)
	@echo "$(RED)⚠️  WARNING: This will delete historical signal-trade records for a fresh start.$(NC)"
	@read -p "Are you sure? Type 'yes' to confirm: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "$(YELLOW)Deleting signal trade history tables...$(NC)"; \
		set -e; \
		$(PSQL) -v ON_ERROR_STOP=1 -c "DELETE FROM signal_trades;"; \
		$(PSQL) -v ON_ERROR_STOP=1 -c "DELETE FROM portfolio_snapshots;"; \
		if $(PSQL) -Atqc "SELECT to_regclass('public.signal_engine_trade_ideas') IS NOT NULL;" | grep -q '^t$$'; then \
			$(PSQL) -v ON_ERROR_STOP=1 -c "DELETE FROM signal_engine_trade_ideas;"; \
		fi; \
		echo "$(GREEN)✅ Signal trade history cleared (fresh start).$(NC)"; \
	else \
		echo "$(RED)❌ Aborted$(NC)"; \
	fi

.PHONY: signals-history
signals-history: ## Closed trade history with outcomes and PnL
	@$(eval LIMIT ?= 100)
	@echo "$(BLUE)=== Closed Signal Trades ($(FLOW_SYMBOL), limit=$(LIMIT)) ===$(NC)"
	@$(PSQL) -c "SELECT underlying, signal_timestamp AT TIME ZONE 'America/New_York' AS signal_time_et, opened_at AT TIME ZONE 'America/New_York' AS opened_et, closed_at AT TIME ZONE 'America/New_York' AS closed_et, direction, option_symbol, entry_price, current_price, quantity_initial, realized_pnl, unrealized_pnl, total_pnl, pnl_percent, CASE WHEN total_pnl > 0 THEN 'win' WHEN total_pnl < 0 THEN 'loss' ELSE 'flat' END AS outcome FROM signal_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT $(LIMIT);"

.PHONY: signals-score
signals-score: ## Latest score snapshot from DB
	@echo "$(BLUE)=== Latest Signal Score ($(FLOW_SYMBOL)) ===$(NC)"
	@$(PSQL) -c "SELECT underlying, timestamp AT TIME ZONE 'America/New_York' AS time_et, direction, composite_score, normalized_score, components FROM signal_scores WHERE underlying='$(FLOW_SYMBOL)' ORDER BY timestamp DESC LIMIT 1;"

.PHONY: signals-score-history
signals-score-history: ## Score history from DB
	@$(eval LIMIT ?= 100)
	@echo "$(BLUE)=== Signal Score History ($(FLOW_SYMBOL), limit=$(LIMIT)) ===$(NC)"
	@$(PSQL) -c "SELECT underlying, timestamp AT TIME ZONE 'America/New_York' AS time_et, direction, composite_score, normalized_score FROM signal_scores WHERE ORDER BY timestamp DESC LIMIT $(LIMIT);"

.PHONY: signals-vol-expansion
signals-vol-expansion: ## Latest volatility-expansion score (0-100) from signal_component_scores
	@echo "$(BLUE)=== Volatility Expansion Signal ($(FLOW_SYMBOL)) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			underlying, \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') AS time, \
			ROUND(clamped_score::numeric * 100, 1) AS score, \
			CASE WHEN clamped_score > 0 THEN 'bullish' WHEN clamped_score < 0 THEN 'bearish' ELSE 'neutral' END AS direction, \
			ROUND(clamped_score::numeric, 4) AS clamped_score, \
			context_values \
		FROM signal_component_scores \
		WHERE underlying = '$(FLOW_SYMBOL)' \
		  AND component_name = 'vol_expansion' \
		ORDER BY timestamp DESC \
		LIMIT 1;"

# =============================================================================
# Signal Engine — Logs
# =============================================================================

.PHONY: signals-logs-cycles
signals-logs-cycles: ## Show completed Signal Engine cycles
	@echo "$(BLUE)=== Signal Engine Cycle History ===$(NC)"
	@sudo journalctl -u $(SIGNALS_SERVICE) -n 2000 --no-pager | grep -i "SignalEngineService cycle" | tail -50

# =============================================================================
# API Signals — Endpoint Tests
# =============================================================================

.PHONY: api-test-signals
api-test-signals: ## Test active /api/signals endpoints
	@echo "$(BLUE)=== Testing /api/signals Endpoints ===$(NC)"
	@echo ""
	@echo "$(GREEN)Live Signals/Trades:$(NC)"
	@curl -s "http://localhost:8000/api/signals/trades-live" | python3 -m json.tool
	@echo ""
	@echo "$(GREEN)History:$(NC)"
	@curl -s "http://localhost:8000/api/signals/trades-history?limit=20" | python3 -m json.tool
	@echo ""
	@echo "$(GREEN)Latest Score:$(NC)"
	@curl -s "http://localhost:8000/api/signals/score?underlying=SPY" | python3 -m json.tool

.PHONY: api-test-vol-signals
api-test-vol-signals: ## Test /api/signals/advanced/vol-expansion endpoint
	@echo "$(BLUE)=== Testing /api/signals/advanced/vol-expansion ===$(NC)"
	@curl -s "http://localhost:8000/api/signals/advanced/vol-expansion?symbol=SPY" | python3 -m json.tool

.PHONY: api-test-signals-summary
api-test-signals-summary: ## Quick one-liner status for trade + history summary
	@echo "$(BLUE)=== Signals Summary ===$(NC)"
	@curl -s "http://localhost:8000/api/signals/trades-live" 2>/dev/null 		| python3 -c 'import sys,json; d=json.load(sys.stdin); print(f"open_trades={d.get('count')}")' 		|| echo "(no live data yet)"
	@curl -s "http://localhost:8000/api/signals/trades-history?limit=50" 2>/dev/null 		| python3 -c 'import sys,json; d=json.load(sys.stdin); s=d.get("summary",{}); print(f"trades={s.get('total_trades')} wins={s.get('wins')} losses={s.get('losses')} total_pnl={s.get('total_pnl')}")' 		|| echo "(no history data yet)"

# =============================================================================
# Data Quality
# =============================================================================

.PHONY: gaps
gaps: ## Check for data gaps in underlying quotes
	@echo "$(BLUE)=== Data Gaps (>5 minutes) ===$(NC)"
	@$(PSQL) -c "\
		WITH time_gaps AS ( \
			SELECT \
				timestamp AT TIME ZONE 'America/New_York' as current_time, \
				LAG(timestamp AT TIME ZONE 'America/New_York') OVER (ORDER BY timestamp) as prev_time, \
				EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp)))/60 as gap_minutes \
			FROM underlying_quotes \
			WHERE timestamp > NOW() - INTERVAL '7 days' \
		) \
		SELECT \
			prev_time, \
			current_time, \
			ROUND(gap_minutes::numeric, 1) as gap_minutes \
		FROM time_gaps \
		WHERE gap_minutes > 5 \
		ORDER BY current_time DESC \
		LIMIT 20;"

.PHONY: gaps-today
gaps-today: ## Today's data gaps
	@echo "$(BLUE)=== Today's Data Gaps ===$(NC)"
	@$(PSQL) -c "\
		WITH time_gaps AS ( \
			SELECT \
				timestamp AT TIME ZONE 'America/New_York' as curr_time, \
				LAG(timestamp AT TIME ZONE 'America/New_York') OVER (ORDER BY timestamp) as prev_time, \
				EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp)))/60 as gap_minutes \
			FROM underlying_quotes \
			WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE \
		) \
		SELECT \
			TO_CHAR(prev_time, 'HH24:MI') as from_time, \
			TO_CHAR(curr_time, 'HH24:MI') as to_time, \
			ROUND(gap_minutes::numeric, 1) as gap_minutes \
		FROM time_gaps \
		WHERE gap_minutes > 2 \
		ORDER BY curr_time;"

.PHONY: quality
quality: ## Data quality report
	@echo "$(BLUE)=== Data Quality Report ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			'Underlying' as data_type, \
			COUNT(*) as total_records, \
			COUNT(*) FILTER (WHERE up_volume = 0 AND down_volume = 0) as zero_volume_records, \
			COUNT(*) FILTER (WHERE close = 0) as zero_price_records, \
			ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - timestamp))), 2) as avg_lag_seconds \
		FROM underlying_quotes \
		WHERE timestamp > NOW() - INTERVAL '24 hours' \
		UNION ALL \
		SELECT \
			'Options', \
			COUNT(*), \
			COUNT(*) FILTER (WHERE volume = 0), \
			COUNT(*) FILTER (WHERE last = 0), \
			ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - timestamp))), 2) \
		FROM option_chains \
		WHERE timestamp > NOW() - INTERVAL '24 hours';"

# =============================================================================
# Data Management
# =============================================================================

.PHONY: clear-data
clear-data: ## Clear all data from tables (keeps schema)
	@echo "$(RED)⚠️  WARNING: This will delete ALL data from tables!$(NC)"
	@read -p "Are you sure? Type 'yes' to confirm: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "$(YELLOW)Clearing all data...$(NC)"; \
		$(PSQL) -c "TRUNCATE TABLE underlying_quotes, option_chains, gex_summary, gex_by_strike, flow_contract_facts, flow_by_contract, flow_smart_money, max_pain_oi_snapshot, max_pain_oi_snapshot_expiration RESTART IDENTITY CASCADE;"; \
		echo "$(GREEN)✅ All data cleared$(NC)"; \
	else \
		echo "$(RED)❌ Aborted$(NC)"; \
	fi

.PHONY: clear-options
clear-options: ## Clear only option chains data
	@echo "$(YELLOW)Clearing option chains...$(NC)"
	@$(PSQL) -c "TRUNCATE TABLE option_chains RESTART IDENTITY CASCADE;"
	@echo "$(GREEN)✅ Option chains cleared$(NC)"

.PHONY: clear-underlying
clear-underlying: ## Clear only underlying quotes
	@echo "$(YELLOW)Clearing underlying quotes...$(NC)"
	@$(PSQL) -c "TRUNCATE TABLE underlying_quotes RESTART IDENTITY CASCADE;"
	@echo "$(GREEN)✅ Underlying quotes cleared$(NC)"

# =============================================================================
# Database Schema Management
# =============================================================================

.PHONY: symbol-add
symbol-add: ## Upsert into symbols table (required: SYMBOL; optional: NAME, ASSET_TYPE, IS_ACTIVE)
	@echo "$(BLUE)=== Upserting symbol into symbols table ===$(NC)"
	@if [ -z "$(SYMBOL)" ]; then \
		echo "$(RED)❌ SYMBOL is required$(NC)"; \
		echo "Usage: make symbol-add SYMBOL=SPY NAME='SPDR S&P 500 ETF' ASSET_TYPE=ETF IS_ACTIVE=true"; \
		exit 1; \
	fi
	@SYMBOL_UPPER="$$(echo '$(SYMBOL)' | tr '[:lower:]' '[:upper:]')"; \
	ASSET_UPPER="$$(echo '$(or $(ASSET_TYPE),EQUITY)' | tr '[:lower:]' '[:upper:]')"; \
	ACTIVE_VAL='$(or $(IS_ACTIVE),true)'; \
	if [ "$$ASSET_UPPER" != "EQUITY" ] && [ "$$ASSET_UPPER" != "INDEX" ] && [ "$$ASSET_UPPER" != "ETF" ]; then \
		echo "$(RED)❌ ASSET_TYPE must be one of: EQUITY, INDEX, ETF$(NC)"; \
		exit 1; \
	fi; \
	if [ "$$ACTIVE_VAL" != "true" ] && [ "$$ACTIVE_VAL" != "false" ]; then \
		echo "$(RED)❌ IS_ACTIVE must be true or false$(NC)"; \
		exit 1; \
	fi; \
	$(PSQL) -c "\
		INSERT INTO symbols (symbol, name, asset_type, is_active) \
		VALUES ('$$SYMBOL_UPPER', NULLIF('$(NAME)', ''), '$$ASSET_UPPER', $$ACTIVE_VAL) \
		ON CONFLICT (symbol) DO UPDATE SET \
			name = EXCLUDED.name, \
			asset_type = EXCLUDED.asset_type, \
			is_active = EXCLUDED.is_active, \
			updated_at = NOW();"; \
	echo "$(GREEN)✅ Upserted symbol $$SYMBOL_UPPER$(NC)"

.PHONY: schema-apply
schema-apply: ## Apply schema (idempotent; aborts on DB contention — pass FORCE=yes to override)
	@echo "$(BLUE)=== Applying Database Schema ===$(NC)"
	@echo "$(YELLOW)Pre-flight: checking for long-running queries / lock waits...$(NC)"
	@BUSY_COUNT=$$($(PSQL) -tA -c "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND ((state = 'active' AND now() - query_start > interval '5 seconds') OR (wait_event_type = 'Lock' AND now() - query_start > interval '1 second'))" | tr -d '[:space:]'); \
	if [ -z "$$BUSY_COUNT" ]; then \
		echo "$(RED)❌ Pre-flight check could not query pg_stat_activity (DB unreachable?). Aborting.$(NC)"; \
		exit 1; \
	fi; \
	if [ "$$BUSY_COUNT" -gt 0 ]; then \
		echo "$(RED)❌ $$BUSY_COUNT query/queries are active >5s or waiting on locks >1s:$(NC)"; \
		$(PSQL) -c "SELECT pid, now() - query_start AS duration, state, wait_event_type, substr(query, 1, 100) AS query FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND ((state = 'active' AND now() - query_start > interval '5 seconds') OR (wait_event_type = 'Lock' AND now() - query_start > interval '1 second')) ORDER BY query_start;"; \
		if [ "$$FORCE" = "yes" ]; then \
			echo "$(YELLOW)⚠️  Proceeding anyway because FORCE=yes was passed.$(NC)"; \
		else \
			echo "$(YELLOW)Wait for the queries above to drain, or rerun with: make schema-apply FORCE=yes$(NC)"; \
			exit 1; \
		fi; \
	else \
		echo "$(GREEN)✓ No long-running queries or lock waits — proceeding.$(NC)"; \
	fi
	@echo "$(YELLOW)Running schema.sql on $(DB_HOST)...$(NC)"
	@PGPASSFILE=~/.pgpass psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) -d $(DB_NAME) -v ON_ERROR_STOP=1 -f setup/database/schema.sql
	@echo ""
	@echo "$(GREEN)✅ Schema applied successfully$(NC)"
	@echo ""
	@echo "$(BLUE)Verifying tables and views...$(NC)"
	@$(PSQL) -c "\
		SELECT 'Tables:' as type, COUNT(*) as count FROM pg_tables WHERE schemaname = 'public' \
		UNION ALL \
		SELECT 'Views:', COUNT(*) FROM pg_views WHERE schemaname = 'public' \
		UNION ALL \
		SELECT 'Indexes:', COUNT(*) FROM pg_indexes WHERE schemaname = 'public';"

# `make pull` exists because a bare `git pull` skips the database side of
# the deploy — schema.sql lives in version control but no git hook re-runs
# it on update.  That's how a stale `unusual_volume_spikes` view shipped
# without `up_volume`/`down_volume` (commit 2a69be1) silently broke
# /api/technicals/volume-spikes for canonical symbols until someone re-ran
# `make schema-apply` by hand.  This target wraps pull + schema-apply so
# the schema can't drift again, and reminds the operator to restart
# services when Python code changes too.
.PHONY: pull
pull: ## git pull --ff-only + make schema-apply (use after pulling new commits)
	@echo "$(BLUE)=== Pulling latest from origin ===$(NC)"
	@git pull --ff-only
	@echo ""
	@$(MAKE) --no-print-directory schema-apply
	@echo ""
	@echo "$(YELLOW)Note: if files under src/ changed in this pull, restart the affected services:$(NC)"
	@echo "  sudo systemctl restart $(API_SERVICE) $(INGESTION_SERVICE) $(ANALYTICS_SERVICE) $(SIGNALS_SERVICE)"

.PHONY: schema-verify
schema-verify: ## Verify schema components exist
	@echo "$(BLUE)=== Schema Verification ===$(NC)"
	@echo ""
	@echo "$(GREEN)Tables:$(NC)"
	@$(PSQL) -c "\
		SELECT tablename FROM pg_tables \
		WHERE schemaname = 'public' \
		ORDER BY tablename;"
	@echo ""
	@echo "$(GREEN)Views:$(NC)"
	@$(PSQL) -c "\
		SELECT viewname FROM pg_views \
		WHERE schemaname = 'public' \
		ORDER BY viewname;"
	@echo ""
	@echo "$(GREEN)Functions:$(NC)"
	@$(PSQL) -c "\
		SELECT routine_name FROM information_schema.routines \
		WHERE routine_schema = 'public' \
		ORDER BY routine_name;"

.PHONY: schema-backup
schema-backup: ## Backup current schema to file
	@echo "$(BLUE)=== Backing Up Schema ===$(NC)"
	@BACKUP_FILE="setup/database/schema_backup_$$(date +%Y%m%d_%H%M%S).sql"; \
	echo "$(YELLOW)Creating backup: $$BACKUP_FILE$(NC)"; \
	PGPASSFILE=~/.pgpass pg_dump -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) -d $(DB_NAME) --schema-only -f $$BACKUP_FILE; \
	echo "$(GREEN)✅ Schema backed up to $$BACKUP_FILE$(NC)"

# Refresh per-symbol normalizer rows so signal saturation tracks real
# magnitude distributions instead of falling back to env-var defaults.
# Override SYMBOLS / WINDOW_DAYS to scope the refresh.
NORMALIZER_SYMBOLS ?=
NORMALIZER_WINDOW_DAYS ?= 20

.PHONY: normalizer-cache-refresh
normalizer-cache-refresh: ## Recompute component_normalizer_cache p05/p50/p95/std (run nightly)
	@echo "$(BLUE)=== Refreshing component_normalizer_cache ===$(NC)"
	@$(PY) -m src.tools.normalizer_cache_refresh \
		$(if $(NORMALIZER_SYMBOLS),--symbols $(NORMALIZER_SYMBOLS)) \
		--window-days $(NORMALIZER_WINDOW_DAYS)

.PHONY: normalizer-cache-dry-run
normalizer-cache-dry-run: ## Compute distributions without writing (preview only)
	@echo "$(BLUE)=== Normalizer Cache (dry-run) ===$(NC)"
	@$(PY) -m src.tools.normalizer_cache_refresh \
		$(if $(NORMALIZER_SYMBOLS),--symbols $(NORMALIZER_SYMBOLS)) \
		--window-days $(NORMALIZER_WINDOW_DAYS) --dry-run

# Override SYMBOLS to scope; the snapshot covers current + prior session
# (everything /api/flow/series can request).
FLOW_SERIES_SYMBOLS ?= SPY

.PHONY: flow-series-backfill
flow-series-backfill: ## Backfill flow_series_5min (current + prior session) before flipping FLOW_SERIES_USE_SNAPSHOT
	@echo "$(BLUE)=== Backfilling flow_series_5min ===$(NC)"
	@$(PY) -m src.tools.flow_series_5min_backfill --symbols $(FLOW_SERIES_SYMBOLS)

# Verification gate for phase-1 -> phase-2: diff the snapshot against the
# live CTE row-for-row. DSN is auto-derived from the same DB_* vars
# schema-apply uses (.env), authenticating via ~/.pgpass exactly like the
# PSQL target. Override with FLOW_SERIES_PARITY_DSN=... for an ad-hoc DB.
FLOW_SERIES_PARITY_DSN ?=
FLOW_SERIES_PARITY_SYMBOL ?= SPY
FLOW_SERIES_PARITY_SESSION ?= prior

.PHONY: flow-series-parity
flow-series-parity: ## Diff flow_series_5min vs the live CTE (auto-uses DB_* from .env; override FLOW_SERIES_PARITY_DSN=...)
	@echo "$(BLUE)=== flow_series_5min parity vs live CTE ===$(NC)"
	@DSN="$(FLOW_SERIES_PARITY_DSN)"; \
	if [ -z "$$DSN" ]; then \
		DSN="postgresql://$(DB_USER)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)?sslmode=require"; \
		echo "$(YELLOW)Auto-derived DSN from .env DB_*: $$DSN$(NC)"; \
	fi; \
	PGPASSFILE="$${PGPASSFILE:-$$HOME/.pgpass}" \
		FLOW_SERIES_PARITY_DSN="$$DSN" \
		FLOW_SERIES_PARITY_SYMBOL="$(FLOW_SERIES_PARITY_SYMBOL)" \
		FLOW_SERIES_PARITY_SESSION="$(FLOW_SERIES_PARITY_SESSION)" \
		$(PY) -m pytest tests/test_flow_series_parity.py -m integration --no-cov -q

# =============================================================================
# Maintenance
# =============================================================================

.PHONY: vacuum
vacuum: ## Vacuum analyze all tables
	@echo "$(YELLOW)Running VACUUM ANALYZE on all tables...$(NC)"
	@for tbl in $(DB_MAINTAIN_TABLES); do \
		echo "  VACUUM ANALYZE $$tbl ..."; \
		$(PSQL) -c "VACUUM ANALYZE $$tbl;" || echo "$(RED)  ⚠️  Failed for $$tbl, continuing...$(NC)"; \
	done
	@echo "$(GREEN)✅ Done$(NC)"

DATA_RETENTION_DAYS ?= 90

# Helper: all tables that hold timestamped data and need regular maintenance.
DB_MAINTAIN_TABLES = option_chains underlying_quotes gex_summary gex_by_strike \
	flow_contract_facts flow_by_contract \
	flow_smart_money trade_signals \
	position_optimizer_signals

.PHONY: db-prune
db-prune: ## Delete data older than DATA_RETENTION_DAYS (default 90)
	@echo "$(YELLOW)Pruning data older than $(DATA_RETENTION_DAYS) days...$(NC)"
	@for tbl in $(DB_MAINTAIN_TABLES); do \
		echo "  Pruning $$tbl ..."; \
		if $(PSQL) -tAc "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='$$tbl'" | grep -q 1; then \
			$(PSQL) -c "DELETE FROM $$tbl WHERE timestamp < NOW() - INTERVAL '$(DATA_RETENTION_DAYS) days';"; \
		else \
			echo "    ⚠️  Table $$tbl does not exist, skipping"; \
		fi; \
	done
	@echo "$(GREEN)✅ Prune complete$(NC)"

.PHONY: db-maintain
db-maintain: ## Full maintenance: prune old data, vacuum full, reindex (run with services stopped)
	@echo "$(BLUE)=== Full Database Maintenance ===$(NC)"
	@echo "$(RED)⚠️  This can take several minutes on large databases. Services should be stopped.$(NC)"
	@echo ""
	@echo "$(YELLOW)Step 1/3: Pruning data older than $(DATA_RETENTION_DAYS) days...$(NC)"
	@$(MAKE) db-prune
	@echo ""
	@echo "$(YELLOW)Step 2/3: Running VACUUM FULL + REINDEX per table (reclaims disk space)...$(NC)"
	@for tbl in $(DB_MAINTAIN_TABLES); do \
		if $(PSQL) -tAc "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='$$tbl'" | grep -q 1; then \
			echo "  VACUUM FULL $$tbl ..."; \
			$(PSQL) -c "VACUUM FULL ANALYZE $$tbl;" || echo "$(RED)  ⚠️  VACUUM FULL failed for $$tbl, continuing...$(NC)"; \
			echo "  REINDEX $$tbl ..."; \
			$(PSQL) -c "REINDEX TABLE $$tbl;" || echo "$(RED)  ⚠️  REINDEX failed for $$tbl, continuing...$(NC)"; \
		else \
			echo "  ⚠️  Table $$tbl does not exist, skipping"; \
		fi; \
	done
	@echo ""
	@echo "$(YELLOW)Step 3/3: Updating planner statistics...$(NC)"
	@$(PSQL) -c "ANALYZE;"
	@echo ""
	@echo "$(GREEN)✅ Full maintenance complete$(NC)"
	@$(MAKE) db-size

.PHONY: db-maintain-managed
db-maintain-managed: ## Stop services → run db-maintain → restart services (used by zerogex-oa-db-vacuum-full.timer)
	@echo "$(BLUE)=== Managed Full DB Maintenance ===$(NC)"
	@trap 'echo "$(YELLOW)→ Restarting services...$(NC)"; sudo systemctl start zerogex-oa-ingestion zerogex-oa-analytics zerogex-oa-signals zerogex-oa-api' EXIT; \
	echo "$(YELLOW)→ Stopping services for VACUUM FULL...$(NC)"; \
	sudo systemctl stop zerogex-oa-api zerogex-oa-signals zerogex-oa-analytics zerogex-oa-ingestion; \
	$(MAKE) db-maintain
	@echo "$(GREEN)✅ Managed maintenance complete$(NC)"

.PHONY: db-size
db-size: ## Show sizes for all tables in the database
	@echo "$(BLUE)=== Table Sizes ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			tablename, \
			pg_size_pretty(pg_total_relation_size('public.'||tablename)) AS total_size, \
			pg_size_pretty(pg_relation_size('public.'||tablename)) AS table_size, \
			pg_size_pretty(pg_total_relation_size('public.'||tablename) - pg_relation_size('public.'||tablename)) AS index_size \
		FROM pg_tables \
		WHERE schemaname = 'public' \
		ORDER BY pg_total_relation_size('public.'||tablename) DESC;"

.PHONY: db-prune-legacy
db-prune-legacy: ## Drop obsolete legacy refresh/materialized-view artifacts
	@echo "$(BLUE)=== Pruning legacy materialized-view refresh artifacts ===$(NC)"
	@printf "%s\n" \
		"DROP FUNCTION IF EXISTS refresh_all_materialized_views();" \
		"DROP FUNCTION IF EXISTS refresh_delta_views();" \
		"SELECT 'DROP MATERIALIZED VIEW underlying_quotes_with_deltas CASCADE' WHERE EXISTS (SELECT 1 FROM pg_class WHERE relname = 'underlying_quotes_with_deltas' AND relkind = 'm')" \
		"UNION ALL SELECT 'DROP VIEW underlying_quotes_with_deltas CASCADE' WHERE EXISTS (SELECT 1 FROM pg_class WHERE relname = 'underlying_quotes_with_deltas' AND relkind = 'v')" \
		"UNION ALL SELECT 'DROP MATERIALIZED VIEW option_chains_with_deltas CASCADE' WHERE EXISTS (SELECT 1 FROM pg_class WHERE relname = 'option_chains_with_deltas' AND relkind = 'm')" \
		"UNION ALL SELECT 'DROP VIEW option_chains_with_deltas CASCADE' WHERE EXISTS (SELECT 1 FROM pg_class WHERE relname = 'option_chains_with_deltas' AND relkind = 'v');" \
		"\\gexec" \
		"SELECT 'legacy artifacts pruned' AS status;" \
	| $(PSQL)

# =============================================================================
# Interactive
# =============================================================================

.PHONY: psql
psql: ## Open PostgreSQL shell
	@$(PSQL)

.PHONY: query
query: ## Run custom query (use: make query SQL="SELECT * FROM ...")
	@$(PSQL) -c "$(SQL)"

# =============================================================================
# DB Table Tail — targets generated by DB_TAIL macro (see top of file)
# =============================================================================

# =============================================================================
# Advanced Queries
# =============================================================================

.PHONY: check-streaming
check-streaming: ## Check if data is actively streaming
	@echo "$(BLUE)=== Streaming Health Check ===$(NC)"
	@echo "Latest underlying bar:"
	@$(PSQL) -t -c "\
		SELECT \
			timestamp AT TIME ZONE 'America/New_York' as time_et, \
			AGE(NOW(), timestamp) as age \
		FROM underlying_quotes \
		ORDER BY timestamp DESC \
		LIMIT 1;"
	@echo ""
	@echo "Latest option quote:"
	@$(PSQL) -t -c "\
		SELECT \
			timestamp AT TIME ZONE 'America/New_York' as time_et, \
			AGE(NOW(), timestamp) as age \
		FROM option_chains \
		ORDER BY timestamp DESC \
		LIMIT 1;"
	@echo ""
	@echo "Latest GEX calculation:"
	@$(PSQL) -t -c "\
		SELECT \
			timestamp AT TIME ZONE 'America/New_York' as time_et, \
			AGE(NOW(), timestamp) as age \
		FROM gex_summary \
		ORDER BY timestamp DESC \
		LIMIT 1;"
	@echo ""
	@echo "$(YELLOW)If age is > 5 minutes during market hours, services may be stuck.$(NC)"

.PHONY: volume-profile
volume-profile: ## Volume profile for today
	@echo "$(BLUE)=== Today's Volume Profile (by minute) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time_et, \
			close as price, \
			up_volume + down_volume as volume, \
			LPAD('█', (up_volume + down_volume)::int / 10000, '█') as volume_bar \
		FROM underlying_quotes \
		WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE \
		ORDER BY timestamp;"

.PHONY: flow
flow: ## Directional flow analysis (last 20 bars)
	@echo "$(BLUE)=== Directional Flow Analysis ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time_et, \
			close, \
			up_volume, \
			down_volume, \
			ROUND((up_volume::numeric / NULLIF(up_volume + down_volume, 0) * 100), 1) as buying_pct, \
			CASE \
				WHEN up_volume > down_volume * 1.2 THEN '🟢 Strong Buy' \
				WHEN up_volume > down_volume THEN '✅ Buy' \
				WHEN down_volume > up_volume * 1.2 THEN '🔴 Strong Sell' \
				WHEN down_volume > up_volume THEN '❌ Sell' \
				ELSE '⚪ Neutral' \
			END as flow \
		FROM underlying_quotes \
		ORDER BY timestamp DESC \
		LIMIT 20;"

.PHONY: expiration-summary
expiration-summary: ## Summary by expiration
	@echo "$(BLUE)=== Option Activity by Expiration ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			expiration, \
			option_type, \
			COUNT(DISTINCT option_symbol) as contracts, \
			SUM(volume) as total_volume, \
			SUM(open_interest) as total_oi, \
			ROUND(AVG(last), 2) as avg_price \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		GROUP BY expiration, option_type \
		ORDER BY expiration, option_type;"

.PHONY: atm-options
atm-options: ## At-the-money options analysis
	@echo "$(BLUE)=== At-The-Money Options ===$(NC)"
	@$(PSQL) -c "\
		WITH current_price AS ( \
			SELECT close FROM underlying_quotes ORDER BY timestamp DESC LIMIT 1 \
		) \
		SELECT \
			o.strike, \
			o.expiration, \
			o.option_type, \
			o.last, \
			o.volume, \
			o.open_interest, \
			o.delta, \
			o.gamma, \
			ABS(o.strike - cp.close) as distance_from_price \
		FROM option_chains o, current_price cp \
		WHERE o.timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		AND ABS(o.strike - cp.close) < 5.0 \
		ORDER BY o.expiration, o.strike, o.option_type;"

# =============================================================================
# API Server Commands
# =============================================================================

.PHONY: api-dev
api-dev: ## Run API server in development mode
	@echo "$(BLUE)=== Starting API Server (Development) ===$(NC)"
	@echo "$(YELLOW)API will be available at http://localhost:8000$(NC)"
	@echo "$(YELLOW)API docs at http://localhost:8000/docs$(NC)"
	@echo ""
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

.PHONY: api-prod
api-prod: ## Run API server in production mode
	@echo "$(BLUE)=== Starting API Server (Production) ===$(NC)"
	uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4

# api-start/stop/restart/status/enable/disable/logs/logs-tail/logs-errors
# and api-health are generated by SERVICE_TARGETS macro and health section above.

.PHONY: api-test
api-test: ## Test ALL API endpoints — HTTP code, time, size in an aligned table
	@BASE_URL="http://localhost:8000"; \
	SYMBOL="SPY"; \
	TIMEFRAMES="1min 5min 15min 1hr 1day"; \
	KEY=$$(grep -E '^OPS_API_KEY=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
	KEY_SRC="OPS_API_KEY"; \
	if [ -z "$$KEY" ]; then \
		KEY=$$(grep -E '^API_KEY=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
		KEY_SRC="API_KEY (break-glass)"; \
	fi; \
	echo "$(BLUE)=== Testing All API Endpoints ===$(NC)"; \
	if [ -n "$$KEY" ]; then \
		echo "$(YELLOW)Auth: sending X-API-Key from $$KEY_SRC (length=$${#KEY})$(NC)"; \
	else \
		echo "$(RED)✗ No OPS_API_KEY (or API_KEY) in .env — every protected endpoint will 401.$(NC)"; \
		echo "$(YELLOW)  Mint one with:  make api-keys-create USER=ops NAME=ops-make-tests$(NC)"; \
		echo "$(YELLOW)  then paste it as OPS_API_KEY=... in .env and re-run.$(NC)"; \
	fi; \
	LOG="/tmp/api-timings.tsv"; \
	: > "$$LOG"; \
	echo ""; \
	printf "%-4s  %9s  %12s  %s\n" "HTTP" "TIME(s)" "SIZE(B)" "ENDPOINT"; \
	printf "%-4s  %9s  %12s  %s\n" "----" "---------" "------------" "--------------------------------------------------"; \
	hit() { \
		if [ -n "$$KEY" ]; then \
			out=$$(curl -s -o /dev/null -w "%{http_code}\t%{time_total}\t%{size_download}" -H "X-API-Key: $$KEY" "$$BASE_URL$$1"); \
		else \
			out=$$(curl -s -o /dev/null -w "%{http_code}\t%{time_total}\t%{size_download}" "$$BASE_URL$$1"); \
		fi; \
		code=$$(printf '%s' "$$out" | cut -f1); \
		ttot=$$(printf '%s' "$$out" | cut -f2); \
		size=$$(printf '%s' "$$out" | cut -f3); \
		ttot_fmt=$$(awk -v t="$$ttot" 'BEGIN{printf "%.3f", (t==""?0:t)}'); \
		printf "%s\t%s\t%s\t%s\n" "$$code" "$$ttot_fmt" "$$size" "$$1" >> "$$LOG"; \
		case "$$code" in \
			2*) printf "$(GREEN)%-4s$(NC)  %9s  %12s  %s\n" "$$code" "$$ttot_fmt" "$$size" "$$1" ;; \
			*)  printf "$(RED)%-4s$(NC)  %9s  %12s  %s\n" "$$code" "$$ttot_fmt" "$$size" "$$1" ;; \
		esac; \
	}; \
	hit "/api/health"; \
	hit "/api/gex/summary?symbol=$$SYMBOL"; \
	hit "/api/gex/by-strike?symbol=$$SYMBOL&limit=10&sort_by=distance"; \
	hit "/api/gex/by-strike?symbol=$$SYMBOL&limit=10&sort_by=impact"; \
	hit "/api/gex/vol_surface?symbol=$$SYMBOL"; \
	hit "/api/market/quote?symbol=$$SYMBOL"; \
	hit "/api/market/session-closes?symbol=$$SYMBOL"; \
	hit "/api/market/vix"; \
	hit "/api/market/open-interest?underlying=$$SYMBOL"; \
	hit "/api/option/quote?underlying=$$SYMBOL&type=C"; \
	hit "/api/max-pain/current?symbol=$$SYMBOL&strike_limit=100"; \
	hit "/api/technicals?symbol=$$SYMBOL"; \
	hit "/api/technicals/dealer-hedging?symbol=$$SYMBOL&limit=10"; \
	hit "/api/technicals/volume-spikes?symbol=$$SYMBOL&limit=10"; \
	hit "/api/flow/by-contract?symbol=$$SYMBOL"; \
	hit "/api/flow/by-contract?symbol=$$SYMBOL&intervals=12"; \
	hit "/api/flow/series?symbol=$$SYMBOL"; \
	hit "/api/flow/contracts?symbol=$$SYMBOL"; \
	hit "/api/flow/smart-money?symbol=$$SYMBOL&window_minutes=60&limit=10"; \
	hit "/api/flow/buying-pressure?symbol=$$SYMBOL"; \
	hit "/api/signals/trades-live"; \
	hit "/api/signals/trades-history?limit=20"; \
	hit "/api/signals/score?underlying=$$SYMBOL"; \
	hit "/api/signals/score-history?underlying=$$SYMBOL&limit=20"; \
	hit "/api/signals/action?underlying=$$SYMBOL"; \
	hit "/api/signals/basic?symbol=$$SYMBOL"; \
	hit "/api/signals/basic/tape-flow-bias?symbol=$$SYMBOL"; \
	hit "/api/signals/basic/skew-delta?symbol=$$SYMBOL"; \
	hit "/api/signals/basic/vanna-charm-flow?symbol=$$SYMBOL"; \
	hit "/api/signals/basic/dealer-delta-pressure?symbol=$$SYMBOL"; \
	hit "/api/signals/basic/gex-gradient?symbol=$$SYMBOL"; \
	hit "/api/signals/basic/positioning-trap?symbol=$$SYMBOL"; \
	hit "/api/signals/basic/confluence-matrix?symbol=$$SYMBOL&lookback=120"; \
	hit "/api/signals/advanced/vol-expansion?symbol=$$SYMBOL"; \
	hit "/api/signals/advanced/eod-pressure?symbol=$$SYMBOL"; \
	hit "/api/signals/advanced/squeeze-setup?symbol=$$SYMBOL"; \
	hit "/api/signals/advanced/trap-detection?symbol=$$SYMBOL"; \
	hit "/api/signals/advanced/0dte-position-imbalance?symbol=$$SYMBOL"; \
	hit "/api/signals/advanced/gamma-vwap-confluence?symbol=$$SYMBOL"; \
	hit "/api/signals/advanced/range-break-imminence?symbol=$$SYMBOL"; \
	hit "/api/signals/advanced/confluence-matrix?symbol=$$SYMBOL&lookback=120"; \
	hit "/api/signals/vol_expansion/events?symbol=$$SYMBOL"; \
	hit "/api/signals/eod_pressure/events?symbol=$$SYMBOL"; \
	for TF in $$TIMEFRAMES; do \
		hit "/api/gex/historical?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		hit "/api/gex/heatmap?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		hit "/api/max-pain/timeseries?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		hit "/api/market/historical?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		hit "/api/technicals/vwap-deviation?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		hit "/api/technicals/opening-range?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		hit "/api/technicals/momentum-divergence?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
	done; \
	hit "/docs"; \
	hit "/redoc"; \
	hit "/openapi.json"; \
	echo ""; \
	echo "$(BLUE)=== Summary (sorted slowest first) ===$(NC)"; \
	sort -t"$$(printf '\t')" -k2 -rn "$$LOG" | head -10 | \
		awk -F'\t' '{ printf "  %9ss  %12s B  %s  %s\n", $$2, $$3, $$1, $$4 }'; \
	awk -F'\t' '{ \
		n++; tot+=$$2; \
		if ($$2+0 > smax+0) { smax=$$2+0; sp=$$4 } \
		if ($$1 ~ /^2/) ok++; else fail++; \
	} END { \
		printf "\nEndpoints tested : %d\n", n; \
		printf "Total time       : %.3fs\n", tot; \
		if (n>0) printf "Average time     : %.3fs\n", tot/n; \
		printf "Slowest          : %.3fs  %s\n", smax, sp; \
		printf "2xx OK           : %d\n", ok+0; \
		printf "Non-2xx          : %d\n", fail+0; \
	}' "$$LOG"; \
	echo ""; \
	echo "$(YELLOW)Full TSV log: $$LOG$(NC)"; \
	FAILED=$$(awk -F'\t' '$$1 !~ /^2/ {c++} END{print c+0}' "$$LOG"); \
	if [ "$$FAILED" -gt 0 ]; then \
		echo "$(RED)✗ $$FAILED endpoint(s) returned non-2xx — see table above.$(NC)"; \
		exit 1; \
	fi; \
	echo "$(GREEN)✅ All endpoints returned 2xx$(NC)"

.PHONY: staging-smoke
staging-smoke: ## Run post-deploy staging smoke checklist
	@echo "$(BLUE)=== Staging Smoke Checklist ===$(NC)"
	@echo "$(YELLOW)Checking systemd services...$(NC)"
	@systemctl is-active --quiet $(INGESTION_SERVICE) && echo "$(GREEN)✅ Ingestion service active$(NC)" || (echo "$(RED)❌ Ingestion service inactive$(NC)" && exit 1)
	@systemctl is-active --quiet $(ANALYTICS_SERVICE) && echo "$(GREEN)✅ Analytics service active$(NC)" || (echo "$(RED)❌ Analytics service inactive$(NC)" && exit 1)
	@systemctl is-active --quiet $(API_SERVICE) && echo "$(GREEN)✅ API service active$(NC)" || (echo "$(RED)❌ API service inactive$(NC)" && exit 1)
	@echo ""
	@echo "$(YELLOW)Checking core API endpoints...$(NC)"
	@KEY=$$(grep -E '^OPS_API_KEY=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
	if [ -z "$$KEY" ]; then \
		KEY=$$(grep -E '^API_KEY=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
	fi; \
	AUTH=""; \
	if [ -n "$$KEY" ]; then AUTH="-H X-API-Key:$$KEY"; fi; \
	curl -fsS $$AUTH "http://localhost:8000/api/health" > /dev/null && echo "$(GREEN)✅ /api/health$(NC)" || (echo "$(RED)❌ /api/health$(NC)" && exit 1); \
	curl -fsS $$AUTH "http://localhost:8000/api/gex/summary?symbol=SPY" > /dev/null && echo "$(GREEN)✅ /api/gex/summary$(NC)" || (echo "$(RED)❌ /api/gex/summary$(NC)" && exit 1); \
	curl -fsS $$AUTH "http://localhost:8000/api/market/quote?symbol=SPY" > /dev/null && echo "$(GREEN)✅ /api/market/quote$(NC)" || (echo "$(RED)❌ /api/market/quote$(NC)" && exit 1); \
	curl -fsS $$AUTH "http://localhost:8000/api/flow/by-contract?symbol=SPY" > /dev/null && echo "$(GREEN)✅ /api/flow/by-contract$(NC)" || (echo "$(RED)❌ /api/flow/by-contract$(NC)" && exit 1)
	@echo ""
	@echo "$(GREEN)✅ Staging smoke checklist passed$(NC)"

.PHONY: api-install-service
api-install-service: ## Install API as systemd service
	@echo "$(BLUE)=== Installing API Systemd Service ===$(NC)"
	@sudo cp setup/systemd/$(API_SERVICE).service /etc/systemd/system/
	@sudo systemctl daemon-reload
	@sudo systemctl enable $(API_SERVICE)
	@echo "$(GREEN)✅ API service installed$(NC)"
	@echo "$(YELLOW)Start with: make api-start$(NC)"

.PHONY: api-rotate-key
# `api-rotate-key` predates the per-user-key system. Since 5c7b6df,
# deploy/steps/125.api_auth removes the legacy nginx X-API-Key injection
# rather than refreshing it — so this target's only effect is to mint a
# new static API_KEY in .env and restart the API. After Phase 7 (API_KEY
# commented out), the target is meaningless and should not be run.
#
# Surviving use case: regenerate the break-glass static credential
# in a hypothetical emergency (e.g. you must temporarily disable
# per-user auth and force every caller onto a single shared secret).
# That should be rare enough to gate on CONFIRM=yes.
api-rotate-key: ## Regenerate break-glass static API_KEY in .env (CONFIRM=yes required). Rare; per-user keys are primary.
	@if [ "$(CONFIRM)" != "yes" ]; then \
		echo "$(RED)✗ Refusing to rotate the static API_KEY without CONFIRM=yes.$(NC)"; \
		echo "  This is the break-glass shared secret, not a per-user key."; \
		echo "  For per-user keys, use:  make api-keys-create USER=<id> NAME=<label>"; \
		echo "  If you really mean to rotate the static credential, re-run with:"; \
		echo "      make api-rotate-key CONFIRM=yes"; \
		exit 1; \
	fi
	@echo "$(BLUE)=== Rotating break-glass API_KEY ===$(NC)"
	@test -f .env || (echo "$(RED)✗ .env not found$(NC)" && exit 1)
	@NEW_KEY=$$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))'); \
	if grep -qE '^API_KEY=' .env; then \
		sed -i "s|^API_KEY=.*|API_KEY=$$NEW_KEY|" .env; \
	else \
		printf 'API_KEY=%s\n' "$$NEW_KEY" >> .env; \
	fi; \
	chmod 600 .env; \
	echo "$(GREEN)✓ New static key written to .env$(NC)"
	@echo "$(YELLOW)→ Re-running deploy/steps/125.api_auth (removes legacy nginx include if still present, then restarts the API)...$(NC)"
	@bash deploy/steps/125.api_auth
	@echo "$(GREEN)✅ Rotation complete. Note: the new static key is NOT distributed to any caller — only api_key_auth's static-match path will accept it.$(NC)"

.PHONY: api-show-key
# Recipe uses bash ${var:offset:len} substring syntax — Ubuntu's /bin/sh
# (dash) doesn't support it.
api-show-key: SHELL := /bin/bash
api-show-key: ## Print a fingerprint of the static API_KEY from .env (length + first 8 / last 4 chars)
	@test -f .env || (echo "$(RED)✗ .env not found$(NC)" && exit 1)
	@KEY=$$(grep -E '^API_KEY=' .env | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
	if [ -z "$$KEY" ]; then \
		echo "$(YELLOW)⚠ API_KEY is empty — static-key auth is disabled (per-user keys still work).$(NC)"; \
	else \
		echo "API_KEY length: $${#KEY}"; \
		echo "First 8 chars:  $${KEY:0:8}..."; \
		echo "Last 4 chars:   ...$${KEY: -4}"; \
	fi

# -----------------------------------------------------------------------------
# Per-user API keys (api_keys table — see src/api/admin_keys.py)
# -----------------------------------------------------------------------------
# $(USER) is a built-in make variable that defaults to $LOGNAME (e.g.
# "ubuntu"), so `$(if $(USER),...)` is always truthy and `[ -z "$(USER)" ]`
# is never empty. Only treat USER as the API key's user_id when it was
# explicitly set on the make command line — otherwise these targets would
# silently filter `api-keys-list` by user_id="ubuntu" (returning "(no keys)")
# and let `api-keys-create NAME=foo` mint a key for user_id="ubuntu".
KEY_USER := $(if $(filter command line,$(origin USER)),$(USER),)

.PHONY: api-keys-create
api-keys-create: ## Create a per-user API key (USER=<id> NAME=<label> [SCOPE=<s>])
	@if [ -z "$(KEY_USER)" ] || [ -z "$(NAME)" ]; then \
		echo "$(RED)✗ Usage: make api-keys-create USER=<user_id> NAME=<label> [SCOPE=<scope>]$(NC)"; \
		echo "  Example: make api-keys-create USER=alice@example.com NAME=alice-laptop"; \
		exit 1; \
	fi
	@$(VENV_PYTHON) -m src.api.admin_keys create "$(KEY_USER)" --name "$(NAME)" \
		$(if $(SCOPE),--scope "$(SCOPE)",)

.PHONY: api-keys-list
api-keys-list: ## List per-user API keys (USER=<id> filters by user; ACTIVE=yes hides revoked)
	@$(VENV_PYTHON) -m src.api.admin_keys list \
		$(if $(KEY_USER),--user-id "$(KEY_USER)",) \
		$(if $(ACTIVE),--active,)

.PHONY: api-keys-revoke
api-keys-revoke: ## Revoke a per-user API key (ID=<numeric id from api-keys-list>)
	@if [ -z "$(ID)" ]; then \
		echo "$(RED)✗ Usage: make api-keys-revoke ID=<numeric id>$(NC)"; \
		echo "  Find the id with: make api-keys-list"; \
		exit 1; \
	fi
	@$(VENV_PYTHON) -m src.api.admin_keys revoke "$(ID)"

.PHONY: db-maintain-install
db-maintain-install: ## Install daily DB maintenance timer (prune old data + vacuum)
	@echo "$(BLUE)=== Installing DB Maintenance Timer ===$(NC)"
	@sudo cp setup/systemd/zerogex-oa-db-maintain.service /etc/systemd/system/
	@sudo cp setup/systemd/zerogex-oa-db-maintain.timer /etc/systemd/system/
	@sudo systemctl daemon-reload
	@sudo systemctl enable --now zerogex-oa-db-maintain.timer
	@echo "$(GREEN)✅ DB maintenance timer installed and started$(NC)"
	@echo "$(YELLOW)Runs daily at 2:00 AM. Check status: systemctl status zerogex-oa-db-maintain.timer$(NC)"
	@echo "$(YELLOW)View logs: journalctl -u zerogex-oa-db-maintain$(NC)"

.PHONY: normalizer-cache-install
normalizer-cache-install: ## Install nightly refresh + drift-detect healthcheck timers
	@echo "$(BLUE)=== Installing Normalizer Cache Refresh + Healthcheck Timers ===$(NC)"
	@sudo cp setup/systemd/zerogex-oa-normalizer-refresh.service /etc/systemd/system/
	@sudo cp setup/systemd/zerogex-oa-normalizer-refresh.timer /etc/systemd/system/
	@sudo cp setup/systemd/zerogex-oa-normalizer-healthcheck.service /etc/systemd/system/
	@sudo cp setup/systemd/zerogex-oa-normalizer-healthcheck.timer /etc/systemd/system/
	@sudo systemctl daemon-reload
	@sudo systemctl enable --now zerogex-oa-normalizer-refresh.timer
	@sudo systemctl enable --now zerogex-oa-normalizer-healthcheck.timer
	@echo "$(GREEN)✅ Normalizer-refresh timer (04:30 ET) installed and started$(NC)"
	@echo "$(GREEN)✅ Normalizer-healthcheck timer (12:00 ET drift-detect) installed and started$(NC)"
	@echo "$(YELLOW)Refresh status:     systemctl status zerogex-oa-normalizer-refresh.timer$(NC)"
	@echo "$(YELLOW)Healthcheck status: systemctl status zerogex-oa-normalizer-healthcheck.timer$(NC)"
	@echo "$(YELLOW)Logs:               journalctl -u zerogex-oa-normalizer-refresh -u zerogex-oa-normalizer-healthcheck$(NC)"
	@echo "$(YELLOW)Trigger now:        sudo systemctl start zerogex-oa-normalizer-refresh.service$(NC)"

.PHONY: normalizer-cache-status
normalizer-cache-status: ## Show refresh + healthcheck timer status + last/next fire + recent log
	@echo "$(BLUE)=== Normalizer Cache Timers ===$(NC)"
	@systemctl list-timers --all --no-pager \
		'zerogex-oa-normalizer-refresh.timer' \
		'zerogex-oa-normalizer-healthcheck.timer' || true
	@echo ""
	@echo "$(BLUE)Refresh service — last run:$(NC)"
	@systemctl status zerogex-oa-normalizer-refresh.service --no-pager -l || true
	@echo ""
	@echo "$(BLUE)Healthcheck service — last run:$(NC)"
	@systemctl status zerogex-oa-normalizer-healthcheck.service --no-pager -l || true
	@echo ""
	@echo "$(BLUE)Recent log lines (both units):$(NC)"
	@sudo journalctl \
		-u zerogex-oa-normalizer-refresh \
		-u zerogex-oa-normalizer-healthcheck \
		-n 30 --no-pager || true

# Default freshness threshold for the healthcheck.  The timer fires at
# 04:30 ET nightly with up to 5 min jitter; 36 h leaves room for a
# single missed cycle (e.g. a planned reboot) without alerting.
NORMALIZER_MAX_AGE_HOURS ?= 36

.PHONY: normalizer-cache-healthcheck
normalizer-cache-healthcheck: ## Verify cache rows are fresh (exit 0=ok, 1=stale, 2=db error)
	@$(PY) -m src.tools.normalizer_cache_healthcheck \
		--max-age-hours $(NORMALIZER_MAX_AGE_HOURS)

.PHONY: normalizer-cache-healthcheck-strict
normalizer-cache-healthcheck-strict: ## Healthcheck that also fails on missing rows
	@$(PY) -m src.tools.normalizer_cache_healthcheck \
		--max-age-hours $(NORMALIZER_MAX_AGE_HOURS) --strict

.PHONY: normalizer-cache-healthcheck-json
normalizer-cache-healthcheck-json: ## Healthcheck output as JSON (for monitoring scrapers)
	@$(PY) -m src.tools.normalizer_cache_healthcheck \
		--max-age-hours $(NORMALIZER_MAX_AGE_HOURS) --json

.PHONY: alert-template-install
alert-template-install: ## Install zerogex-alert@.service template + sample env (does NOT enable OnFailure= hooks)
	@echo "$(BLUE)=== Installing failure-alert template ===$(NC)"
	@command -v jq >/dev/null 2>&1 || { \
		echo "$(YELLOW)Note: jq is required for slack/pagerduty/webhook backends; installing...$(NC)"; \
		sudo apt-get install -y jq; \
	}
	@sudo cp setup/systemd/zerogex-alert@.service /etc/systemd/system/
	@sudo install -d -o root -g ubuntu -m 0750 /etc/zerogex
	@if [ ! -f /etc/zerogex/alert.env ]; then \
		sudo install -o root -g ubuntu -m 0640 setup/systemd/zerogex-alert.env.example /etc/zerogex/alert.env; \
		echo "$(GREEN)✅ Sample config written to /etc/zerogex/alert.env (backend = stderr by default)$(NC)"; \
	else \
		echo "$(YELLOW)⚠ /etc/zerogex/alert.env already exists — leaving in place; reference setup/systemd/zerogex-alert.env.example for new options$(NC)"; \
	fi
	@sudo systemctl daemon-reload
	@echo "$(GREEN)✅ zerogex-alert@.service template installed$(NC)"
	@echo ""
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. Edit /etc/zerogex/alert.env, choose ALERT_BACKEND, fill in secret(s)."
	@echo "  2. Smoke-test: make alert-template-test"
	@echo "  3. Enable wiring on the peer units (uncomment OnFailure= line):"
	@echo "       sudo systemctl edit zerogex-oa-normalizer-refresh.service"
	@echo "       sudo systemctl edit zerogex-oa-normalizer-healthcheck.service"
	@echo "     Add:"
	@echo "       [Unit]"
	@echo "       OnFailure=zerogex-alert@%n.service"
	@echo "     (or uncomment the existing line in setup/systemd/<unit>.service and re-run normalizer-cache-install)"

.PHONY: alert-template-test
alert-template-test: ## Fire one synthetic alert through the template (verifies dispatcher + backend)
	@echo "$(BLUE)=== Sending synthetic alert via current ALERT_BACKEND ===$(NC)"
	@sudo systemctl start 'zerogex-alert@zerogex-oa-normalizer-healthcheck.service'
	@echo "$(GREEN)✅ Triggered. View result:$(NC)"
	@echo "  journalctl -u 'zerogex-alert@zerogex-oa-normalizer-healthcheck.service' -n 30 --no-pager"
