# ZeroGEX Database Query Shortcuts & Service Management
# ======================================================
# Usage: make <target>
#
# Common queries for monitoring and debugging the ZeroGEX platform

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
		exit 0; \
	fi
	@printf "%s\n" \
		"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_gamma_oi;" \
		"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_iv_volume;" \
		"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_expiration_range;" \
		"DROP INDEX CONCURRENTLY IF EXISTS idx_option_chains_timestamp_volfilter;" \
		| $(PSQL) -v ON_ERROR_STOP=1
	@echo "$(GREEN)✓ Unused indexes dropped. Run 'make analytics-snapshot-diagnose' to re-check.$(NC)"

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
		exit 0; \
	fi
	@printf "%s\n" \
		"DROP INDEX CONCURRENTLY IF EXISTS idx_flow_by_contract_symbol_ts_strike;" \
		| $(PSQL) -v ON_ERROR_STOP=1
	@echo "$(GREEN)✓ Index dropped. Run 'make flow-explain' to confirm planner fallback.$(NC)"

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
	@echo "  make db-maintain-install - Install daily DB maintenance timer (prune + vacuum)"
	@echo ""
	@echo "$(GREEN)Logs (all services):$(NC)"
	@echo "  make {service}-logs             - Show live logs (Ctrl+C to exit)"
	@echo "  make {service}-logs-tail        - Show last 100 log lines"
	@echo "  make {service}-logs-errors      - Show recent errors"
	@echo "  (service = ingestion | analytics | signals | api)"
	@echo "  make logs-grep PATTERN=\"text\" - Search all service logs for pattern"
	@echo "  make logs-clear                 - Clear all journalctl logs for services"
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
logs-clear: ## Clear all journalctl logs for the services
	@echo "$(RED)⚠️  WARNING: This will permanently delete ALL logs for ZeroGEX services!$(NC)"
	@read -p "Are you sure? Type 'yes' to confirm: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "$(YELLOW)Clearing logs...$(NC)"; \
		sudo journalctl --rotate; \
		sudo journalctl --vacuum-time=1s -u $(INGESTION_SERVICE); \
		sudo journalctl --vacuum-time=1s -u $(ANALYTICS_SERVICE); \
		echo "$(YELLOW)Truncating /var/log/syslog...$(NC)"; \
		sudo truncate -s 0 /var/log/syslog; \
		echo "$(GREEN)✅ Logs cleared for ZeroGEX services$(NC)"; \
	else \
		echo "$(RED)❌ Aborted$(NC)"; \
	fi

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
schema-apply: ## Apply/update database schema (idempotent)
	@echo "$(BLUE)=== Applying Database Schema ===$(NC)"
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
api-test: ## Test ALL API endpoints
	@echo "$(BLUE)=== Testing All API Endpoints ===$(NC)"
	@BASE_URL="http://localhost:8000"; \
	SYMBOL="SPY"; \
	TIMEFRAMES="1min 5min 15min 1hr 1day"; \
	SIGNAL_TIMEFRAMES="intraday swing multi_day"; \
	PASSED=0; \
	FAILED=0; \
	test_endpoint() { \
		path="$$1"; \
		url="$$BASE_URL$$path"; \
		if curl -fsS "$$url" > /dev/null; then \
			echo "$(GREEN)✅ $$path$(NC)"; PASSED=$$((PASSED+1)); \
		else \
			echo "$(RED)❌ $$path$(NC)"; FAILED=$$((FAILED+1)); \
		fi; \
	}; \
	echo "$(YELLOW)Core endpoints$(NC)"; \
	test_endpoint "/api/health"; \
	test_endpoint "/api/gex/summary?symbol=$$SYMBOL"; \
	test_endpoint "/api/gex/by-strike?symbol=$$SYMBOL&limit=10&sort_by=distance"; \
	test_endpoint "/api/gex/by-strike?symbol=$$SYMBOL&limit=10&sort_by=impact"; \
	test_endpoint "/api/market/quote?symbol=$$SYMBOL"; \
	test_endpoint "/api/market/session-closes?symbol=$$SYMBOL"; \
	test_endpoint "/api/option/quote?underlying=$$SYMBOL&type=C"; \
	test_endpoint "/api/technicals/dealer-hedging?symbol=$$SYMBOL&limit=10"; \
	test_endpoint "/api/technicals/volume-spikes?symbol=$$SYMBOL&limit=10"; \
	test_endpoint "/docs"; \
	test_endpoint "/redoc"; \
	test_endpoint "/openapi.json"; \
	echo ""; \
	echo "$(YELLOW)Timeframe endpoints (GEX, market, technicals)$(NC)"; \
	for TF in $$TIMEFRAMES; do \
		test_endpoint "/api/gex/historical?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		test_endpoint "/api/gex/heatmap?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		test_endpoint "/api/max-pain/timeseries?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		test_endpoint "/api/market/historical?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		test_endpoint "/api/technicals/vwap-deviation?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		test_endpoint "/api/technicals/opening-range?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
		test_endpoint "/api/technicals/momentum-divergence?symbol=$$SYMBOL&window_units=10&timeframe=$$TF"; \
	done; \
	echo ""; \
	echo "$(YELLOW)Flow endpoints$(NC)"; \
	test_endpoint "/api/flow/by-contract?symbol=$$SYMBOL"; \
	test_endpoint "/api/flow/by-contract?symbol=$$SYMBOL&intervals=12"; \
	test_endpoint "/api/flow/smart-money?symbol=$$SYMBOL&window_minutes=60&limit=10"; \
	test_endpoint "/api/flow/buying-pressure?symbol=$$SYMBOL"; \
	echo ""; \
	echo "$(YELLOW)Max pain current snapshot$(NC)"; \
	test_endpoint "/api/max-pain/current?symbol=$$SYMBOL&strike_limit=100"; \
	echo ""; \
	echo "$(YELLOW)Volatility endpoints$(NC)"; \
	test_endpoint "/api/market/vix"; \
	test_endpoint "/api/gex/vol_surface?symbol=$$SYMBOL"; \
	echo ""; \
	echo "$(YELLOW)Trade signal endpoints$(NC)"; \
	test_endpoint "/api/signals/trades-live"; \
	test_endpoint "/api/signals/trades-history?limit=20"; \
	test_endpoint "/api/signals/score?underlying=$$SYMBOL"; \
	test_endpoint "/api/signals/score-history?underlying=$$SYMBOL&limit=20"; \
	test_endpoint "/api/signals/advanced/vol-expansion?symbol=$$SYMBOL"; \
	test_endpoint "/api/signals/advanced/eod-pressure?symbol=$$SYMBOL"; \
	echo ""; \
	echo "$(BLUE)=== API Test Report ===$(NC)"; \
	echo "$(GREEN)Passed: $$PASSED$(NC)"; \
	echo "$(RED)Failed: $$FAILED$(NC)"; \
	if [ $$FAILED -gt 0 ]; then exit 1; fi

.PHONY: staging-smoke
staging-smoke: ## Run post-deploy staging smoke checklist
	@echo "$(BLUE)=== Staging Smoke Checklist ===$(NC)"
	@echo "$(YELLOW)Checking systemd services...$(NC)"
	@systemctl is-active --quiet $(INGESTION_SERVICE) && echo "$(GREEN)✅ Ingestion service active$(NC)" || (echo "$(RED)❌ Ingestion service inactive$(NC)" && exit 1)
	@systemctl is-active --quiet $(ANALYTICS_SERVICE) && echo "$(GREEN)✅ Analytics service active$(NC)" || (echo "$(RED)❌ Analytics service inactive$(NC)" && exit 1)
	@systemctl is-active --quiet $(API_SERVICE) && echo "$(GREEN)✅ API service active$(NC)" || (echo "$(RED)❌ API service inactive$(NC)" && exit 1)
	@echo ""
	@echo "$(YELLOW)Checking core API endpoints...$(NC)"
	@curl -fsS "http://localhost:8000/api/health" > /dev/null && echo "$(GREEN)✅ /api/health$(NC)" || (echo "$(RED)❌ /api/health$(NC)" && exit 1)
	@curl -fsS "http://localhost:8000/api/gex/summary?symbol=SPY" > /dev/null && echo "$(GREEN)✅ /api/gex/summary$(NC)" || (echo "$(RED)❌ /api/gex/summary$(NC)" && exit 1)
	@curl -fsS "http://localhost:8000/api/market/quote?symbol=SPY" > /dev/null && echo "$(GREEN)✅ /api/market/quote$(NC)" || (echo "$(RED)❌ /api/market/quote$(NC)" && exit 1)
	@curl -fsS "http://localhost:8000/api/flow/by-contract?symbol=SPY" > /dev/null && echo "$(GREEN)✅ /api/flow/by-contract$(NC)" || (echo "$(RED)❌ /api/flow/by-contract$(NC)" && exit 1)
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
api-rotate-key: ## Rotate API_KEY in .env and re-sync nginx include (idempotent)
	@echo "$(BLUE)=== Rotating API_KEY ===$(NC)"
	@test -f .env || (echo "$(RED)✗ .env not found$(NC)" && exit 1)
	@NEW_KEY=$$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))'); \
	if grep -qE '^API_KEY=' .env; then \
		sed -i "s|^API_KEY=.*|API_KEY=$$NEW_KEY|" .env; \
	else \
		printf 'API_KEY=%s\n' "$$NEW_KEY" >> .env; \
	fi; \
	chmod 600 .env; \
	echo "$(GREEN)✓ New key written to .env$(NC)"
	@echo "$(YELLOW)→ Re-running deploy/steps/125.api_auth to sync nginx + restart API...$(NC)"
	@bash deploy/steps/125.api_auth
	@echo "$(GREEN)✅ Rotation complete$(NC)"

.PHONY: api-show-key
api-show-key: ## Print the current API_KEY from .env (for debugging nginx mismatch)
	@test -f .env || (echo "$(RED)✗ .env not found$(NC)" && exit 1)
	@KEY=$$(grep -E '^API_KEY=' .env | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
	if [ -z "$$KEY" ]; then \
		echo "$(YELLOW)⚠ API_KEY is empty — auth is disabled$(NC)"; \
	else \
		echo "API_KEY length: $${#KEY}"; \
		echo "First 8 chars:  $${KEY:0:8}..."; \
		echo "Last 4 chars:   ...$${KEY: -4}"; \
	fi
	@NGINX_FILE=/etc/nginx/conf.d/zerogex-api-key.conf; \
	if sudo test -f $$NGINX_FILE; then \
		NGINX_KEY=$$(sudo grep -oE '"[^"]+"' $$NGINX_FILE | tr -d '"'); \
		echo "nginx file:     $$NGINX_FILE"; \
		echo "nginx key len:  $${#NGINX_KEY}"; \
		echo "nginx first 8:  $${NGINX_KEY:0:8}..."; \
		echo "nginx last 4:   ...$${NGINX_KEY: -4}"; \
	else \
		echo "$(YELLOW)⚠ $$NGINX_FILE not present (step 125 hasn't run)$(NC)"; \
	fi

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
