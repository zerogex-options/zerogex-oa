# ZeroGEX Database Query Shortcuts & Service Management
# ======================================================
# Usage: make <target>
#
# Common queries for monitoring and debugging the ZeroGEX platform

# Load database connection from .env
include .env
export

# PostgreSQL connection string
PSQL = PGPASSFILE=~/.pgpass psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) -d $(DB_NAME)

# Service names
INGESTION_SERVICE = zerogex-oa-ingestion
ANALYTICS_SERVICE = zerogex-oa-analytics

# Python virtual environment
VENV_PYTHON = venv/bin/python

# Colors for output
BLUE = \033[0;34m
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m

.PHONY: help
help: ## Show this help message
	@echo "$(BLUE)ZeroGEX Management & Database Shortcuts$(NC)"
	@echo "=========================================="
	@echo ""
	@echo "$(GREEN)Platform Deployment:$(NC)"
	@echo "  make deploy             - Deploy Options Analytics platform"
	@echo "  make deploy-from        - Deploy Options Analytics platform (start-from)"
	@echo "  make deploy-validate    - Validate Options Analytics platform deployment"
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
	@echo "$(GREEN)Logs:$(NC)"
	@echo "  make ingestion-logs          - Show live ingestion logs (Ctrl+C to exit)"
	@echo "  make ingestion-logs-tail     - Show last 100 ingestion log lines"
	@echo "  make ingestion-logs-errors   - Show recent ingestion errors"
	@echo "  make analytics-logs          - Show live analytics logs (Ctrl+C to exit)"
	@echo "  make analytics-logs-tail     - Show last 100 analytics log lines"
	@echo "  make analytics-logs-errors   - Show recent analytics errors"
	@echo "  make logs-grep PATTERN=\"text\" - Search logs for pattern"
	@echo "  make logs-clear              - Clear all journalctl logs for services"
	@echo ""
	@echo "$(GREEN)Run Components:$(NC)"
	@echo "  make run-auth           - Test TradeStation authentication"
	@echo "  make run-client         - Test TradeStation API client"
	@echo "  make run-backfill       - Run historical data backfill"
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
	@echo "  make underlying-latest  - Latest underlying bar"
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
	@echo "  make flow-by-type       - Puts vs calls flow (all strikes/expirations)"
	@echo "  make flow-by-strike     - Flow by strike level"
	@echo "  make flow-by-expiration - Flow by expiration date"
	@echo "  make flow-smart-money   - Unusual activity detection"
	@echo "  make flow-buying-pressure - Underlying buying/selling pressure"
	@echo "  make flow-live          - Combined real-time flow dashboard"
	@echo ""
	@echo "$(GREEN)Day Trading Support:$(NC)"
	@echo "  make vwap               - VWAP deviation tracker"
	@echo "  make orb                - Opening range breakout status"
	@echo "  make gamma-levels       - Key gamma exposure levels"
	@echo "  make hedge-pressure     - Dealer hedging pressure"
	@echo "  make volume-spikes      - Unusual volume detection"
	@echo "  make divergence         - Momentum divergence signals"
	@echo "  make day-trading        - Combined day trading dashboard"
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
	@echo "$(GREEN)Maintenance:$(NC)"
	@echo "  make vacuum             - Vacuum analyze all tables"
	@echo "  make size               - Show table sizes"
	@echo "  make refresh-views      - Refresh materialized views"
	@echo ""
	@echo "$(GREEN)Interactive:$(NC)"
	@echo "  make psql               - Open PostgreSQL shell"
	@echo "  make query SQL=\"...\"    - Run custom query"


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
# Ingestion Service Management
# =============================================================================

.PHONY: ingestion-start
ingestion-start: ## Start the ingestion service
	@echo "$(GREEN)Starting $(INGESTION_SERVICE)...$(NC)"
	@sudo systemctl start $(INGESTION_SERVICE)
	@sleep 2
	@sudo systemctl status $(INGESTION_SERVICE) --no-pager

.PHONY: ingestion-stop
ingestion-stop: ## Stop the ingestion service
	@echo "$(YELLOW)Stopping $(INGESTION_SERVICE)...$(NC)"
	@sudo systemctl stop $(INGESTION_SERVICE)
	@sleep 1
	@echo "$(GREEN)Service stopped$(NC)"

.PHONY: ingestion-restart
ingestion-restart: ## Restart the ingestion service
	@echo "$(YELLOW)Restarting $(INGESTION_SERVICE)...$(NC)"
	@sudo systemctl restart $(INGESTION_SERVICE)
	@sleep 2
	@sudo systemctl status $(INGESTION_SERVICE) --no-pager

.PHONY: ingestion-status
ingestion-status: ## Show ingestion service status
	@sudo systemctl status $(INGESTION_SERVICE) --no-pager -l

.PHONY: ingestion-enable
ingestion-enable: ## Enable ingestion service to start on boot
	@echo "$(GREEN)Enabling $(INGESTION_SERVICE) to start on boot...$(NC)"
	@sudo systemctl enable $(INGESTION_SERVICE)
	@echo "$(GREEN)Service enabled$(NC)"

.PHONY: ingestion-disable
ingestion-disable: ## Disable ingestion service from starting on boot
	@echo "$(YELLOW)Disabling $(INGESTION_SERVICE) from starting on boot...$(NC)"
	@sudo systemctl disable $(INGESTION_SERVICE)
	@echo "$(YELLOW)Service disabled$(NC)"

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
	@sudo journalctl -u $(INGESTION_SERVICE) -p err -n 10 --no-pager || echo "No recent errors"
	@echo ""
	@echo "Recent Warnings (last 5):"
	@echo "-------------------------"
	@sudo journalctl -u $(INGESTION_SERVICE) -p warning -n 5 --no-pager || echo "No recent warnings"

# =============================================================================
# Analytics Service Management
# =============================================================================

.PHONY: analytics-start
analytics-start: ## Start the analytics service
	@echo "$(GREEN)Starting $(ANALYTICS_SERVICE)...$(NC)"
	@sudo systemctl start $(ANALYTICS_SERVICE)
	@sleep 2
	@sudo systemctl status $(ANALYTICS_SERVICE) --no-pager

.PHONY: analytics-stop
analytics-stop: ## Stop the analytics service
	@echo "$(YELLOW)Stopping $(ANALYTICS_SERVICE)...$(NC)"
	@sudo systemctl stop $(ANALYTICS_SERVICE)
	@sleep 1
	@echo "$(GREEN)Service stopped$(NC)"

.PHONY: analytics-restart
analytics-restart: ## Restart the analytics service
	@echo "$(YELLOW)Restarting $(ANALYTICS_SERVICE)...$(NC)"
	@sudo systemctl restart $(ANALYTICS_SERVICE)
	@sleep 2
	@sudo systemctl status $(ANALYTICS_SERVICE) --no-pager

.PHONY: analytics-status
analytics-status: ## Show analytics service status
	@sudo systemctl status $(ANALYTICS_SERVICE) --no-pager -l

.PHONY: analytics-enable
analytics-enable: ## Enable analytics service to start on boot
	@echo "$(GREEN)Enabling $(ANALYTICS_SERVICE) to start on boot...$(NC)"
	@sudo systemctl enable $(ANALYTICS_SERVICE)
	@echo "$(GREEN)Service enabled$(NC)"

.PHONY: analytics-disable
analytics-disable: ## Disable analytics service from starting on boot
	@echo "$(YELLOW)Disabling $(ANALYTICS_SERVICE) from starting on boot...$(NC)"
	@sudo systemctl disable $(ANALYTICS_SERVICE)
	@echo "$(YELLOW)Service disabled$(NC)"

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
	@sudo journalctl -u $(ANALYTICS_SERVICE) -p err -n 10 --no-pager || echo "No recent errors"
	@echo ""
	@echo "Recent Warnings (last 5):"
	@echo "-------------------------"
	@sudo journalctl -u $(ANALYTICS_SERVICE) -p warning -n 5 --no-pager || echo "No recent warnings"

# =============================================================================
# Logs
# =============================================================================

.PHONY: ingestion-logs
ingestion-logs: ## Watch ingestion logs in real-time (Ctrl+C to stop)
	@echo "$(BLUE)=== Watching Ingestion Logs (Ctrl+C to stop) ===$(NC)"
	@sudo journalctl -u $(INGESTION_SERVICE) -f -n 50

.PHONY: ingestion-logs-tail
ingestion-logs-tail: ## Show last 100 ingestion log lines
	@echo "$(GREEN)Last 100 ingestion log lines:$(NC)"
	@sudo journalctl -u $(INGESTION_SERVICE) -n 100 --no-pager

.PHONY: ingestion-logs-errors
ingestion-logs-errors: ## Show recent ingestion errors
	@echo "$(BLUE)=== Recent Ingestion Errors ===$(NC)"
	@sudo journalctl -u $(INGESTION_SERVICE) -p err -n 50 --no-pager

.PHONY: analytics-logs
analytics-logs: ## Watch analytics logs in real-time (Ctrl+C to stop)
	@echo "$(BLUE)=== Watching Analytics Logs (Ctrl+C to stop) ===$(NC)"
	@sudo journalctl -u $(ANALYTICS_SERVICE) -f -n 50

.PHONY: analytics-logs-tail
analytics-logs-tail: ## Show last 100 analytics log lines
	@echo "$(GREEN)Last 100 analytics log lines:$(NC)"
	@sudo journalctl -u $(ANALYTICS_SERVICE) -n 100 --no-pager

.PHONY: analytics-logs-errors
analytics-logs-errors: ## Show recent analytics errors
	@echo "$(BLUE)=== Recent Analytics Errors ===$(NC)"
	@sudo journalctl -u $(ANALYTICS_SERVICE) -p err -n 50 --no-pager

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
run-client: ## Test TradeStation API client
	@echo "$(BLUE)=== Testing TradeStation Client ===$(NC)"
	@$(VENV_PYTHON) -m src.ingestion.tradestation_client

.PHONY: run-backfill
run-backfill: ## Run historical data backfill
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)RUNNING INDEPENDENT BACKFILL$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "Note: Backfill runs independently and stores data directly."
	@echo "      Use this to populate historical data as needed."
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@$(VENV_PYTHON) -m src.ingestion.backfill_manager

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
	@echo "      For historical backfill, run 'make run-backfill'"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@$(VENV_PYTHON) -m src.ingestion.main_engine

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

.PHONY: underlying-latest
underlying-latest: ## Latest underlying bar
	@$(PSQL) -c "\
		SELECT \
			symbol, \
			timestamp AT TIME ZONE 'America/New_York' as time_et, \
			open, high, low, close, \
			up_volume, down_volume, \
			ROUND((up_volume::numeric / (up_volume + down_volume) * 100), 1) as buying_pressure_pct \
		FROM underlying_quotes \
		ORDER BY timestamp DESC \
		LIMIT 1;"

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

.PHONY: flow-by-type
flow-by-type: ## Puts vs calls flow (all strikes/expirations)
	@echo "$(BLUE)=== Option Flow by Type (Last Hour) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			underlying, \
			call_flow, \
			TO_CHAR(call_notional, 'FM999,999,999') as call_notional, \
			put_flow, \
			TO_CHAR(put_notional, 'FM999,999,999') as put_notional, \
			net_flow, \
			TO_CHAR(net_notional, 'FM999,999,999') as net_notional, \
			put_call_ratio as pc_ratio, \
			put_call_notional_ratio as pc_not_ratio \
		FROM option_flow_by_type \
		WHERE timestamp > NOW() - INTERVAL '1 hour' \
		ORDER BY timestamp DESC \
		LIMIT 20;"

.PHONY: flow-by-strike
flow-by-strike: ## Flow by strike level
	@echo "$(BLUE)=== Option Flow by Strike (Last Hour, Top 15) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			underlying, \
			strike, \
			call_flow, \
			TO_CHAR(call_notional, 'FM999,999') as call_notional, \
			put_flow, \
			TO_CHAR(put_notional, 'FM999,999') as put_notional, \
			TO_CHAR(total_notional, 'FM999,999') as total_notional \
		FROM option_flow_by_strike \
		WHERE timestamp > NOW() - INTERVAL '1 hour' \
		ORDER BY total_notional DESC \
		LIMIT 15;"

.PHONY: flow-by-expiration
flow-by-expiration: ## Flow by expiration date
	@echo "$(BLUE)=== Option Flow by Expiration (Last Hour) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			underlying, \
			expiration, \
			days_to_expiry as dte, \
			call_flow, \
			TO_CHAR(call_notional, 'FM999,999') as call_notional, \
			put_flow, \
			TO_CHAR(put_notional, 'FM999,999') as put_notional, \
			TO_CHAR(total_notional, 'FM999,999') as total_notional \
		FROM option_flow_by_expiration \
		WHERE timestamp > NOW() - INTERVAL '1 hour' \
		ORDER BY timestamp DESC, total_notional DESC \
		LIMIT 20;"

.PHONY: flow-smart-money
flow-smart-money: ## Unusual activity detection
	@echo "$(BLUE)=== Smart Money Flow / Unusual Activity (Last Hour) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			SUBSTRING(option_symbol, 1, 15) as contract, \
			strike, \
			expiration, \
			days_to_expiry as dte, \
			option_type, \
			flow, \
			TO_CHAR(notional, 'FM999,999') as notional, \
			ROUND(price, 2) as price, \
			unusual_score as score, \
			notional_class, \
			size_class \
		FROM option_flow_smart_money \
		WHERE timestamp > NOW() - INTERVAL '1 hour' \
		ORDER BY unusual_score DESC, notional DESC \
		LIMIT 25;"

.PHONY: flow-buying-pressure
flow-buying-pressure: ## Underlying buying/selling pressure
	@echo "$(BLUE)=== Underlying Buying Pressure (Last 30 Bars) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			symbol, \
			ROUND(close, 2) as price, \
			total_volume_delta as volume, \
			buying_pressure_pct as buy_pct, \
			period_buying_pressure_pct as period_buy_pct, \
			ROUND(price_change, 2) as price_chg, \
			momentum \
		FROM underlying_buying_pressure \
		ORDER BY timestamp DESC \
		LIMIT 30;"

.PHONY: flow-live
flow-live: ## Combined real-time flow dashboard
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)REAL-TIME FLOW DASHBOARD$(NC)"
	@echo "$(BLUE)================================================================================$(NC)"
	@echo ""
	@echo "$(GREEN)1. UNDERLYING BUYING PRESSURE (Last 10 Bars)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			ROUND(close, 2) as price, \
			total_volume_delta as vol, \
			period_buying_pressure_pct as buy_pct, \
			momentum \
		FROM underlying_buying_pressure \
		ORDER BY timestamp DESC \
		LIMIT 10;" 2>/dev/null
	@echo ""
	@echo "$(GREEN)2. PUTS VS CALLS FLOW (Last 10 Minutes)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			call_flow as calls, \
			put_flow as puts, \
			net_flow as net, \
			put_call_ratio as pc_ratio \
		FROM option_flow_by_type \
		WHERE timestamp > NOW() - INTERVAL '30 minutes' \
		ORDER BY timestamp DESC \
		LIMIT 10;" 2>/dev/null
	@echo ""
	@echo "$(GREEN)3. SMART MONEY / UNUSUAL ACTIVITY (Top 10)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			SUBSTRING(option_symbol, 1, 15) as contract, \
			option_type as type, \
			flow, \
			unusual_score as score, \
			size_class \
		FROM option_flow_smart_money \
		WHERE timestamp > NOW() - INTERVAL '1 hour' \
		ORDER BY unusual_score DESC, flow DESC \
		LIMIT 10;" 2>/dev/null
	@echo ""
	@echo "$(GREEN)4. TOP STRIKES BY FLOW (Top 10)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			strike, \
			call_flow as calls, \
			put_flow as puts, \
			net_flow as net, \
			total_flow as total \
		FROM option_flow_by_strike \
		WHERE timestamp > NOW() - INTERVAL '30 minutes' \
		ORDER BY total_flow DESC \
		LIMIT 10;" 2>/dev/null
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"

# =============================================================================
# Day Trading Decision Support
# =============================================================================

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

.PHONY: gamma-levels
gamma-levels: ## Key gamma exposure levels
	@echo "$(BLUE)=== Gamma Exposure Levels (Top 15) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			underlying, \
			strike, \
			TO_CHAR(net_gex, 'FM999,999,999') as net_gex, \
			TO_CHAR(total_oi, 'FM999,999') as oi, \
			gex_level \
		FROM gamma_exposure_levels \
		ORDER BY ABS(net_gex) DESC \
		LIMIT 15;"

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
	@echo "$(BLUE)=== Momentum Divergence Signals (Last Hour) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			symbol, \
			ROUND(price, 2) as price, \
			ROUND(price_change_5min, 2) as chg_5m, \
			TO_CHAR(net_option_flow, 'FM999,999') as opt_flow, \
			divergence_signal \
		FROM momentum_divergence \
		WHERE divergence_signal != '⚪ Neutral' \
		ORDER BY timestamp DESC \
		LIMIT 20;"

.PHONY: day-trading
day-trading: ## Combined day trading dashboard
	@echo "$(BLUE)================================================================================$(NC)"
	@echo "$(BLUE)DAY TRADING DASHBOARD$(NC)"
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
		LIMIT 10;" 2>/dev/null
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
		LIMIT 5;" 2>/dev/null
	@echo ""
	@echo "$(GREEN)3. GAMMA LEVELS (Top 10)$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			strike, \
			TO_CHAR(net_gex, 'FM999,999,999') as net_gex, \
			gex_level \
		FROM gamma_exposure_levels \
		ORDER BY ABS(net_gex) DESC \
		LIMIT 10;" 2>/dev/null
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
		LIMIT 10;" 2>/dev/null
	@echo ""
	@echo "$(GREEN)5. DIVERGENCE SIGNALS$(NC)"
	@echo "--------------------------------------------------------------------------------"
	@$(PSQL) -c "\
		SELECT \
			TO_CHAR(time_et, 'HH24:MI') as time, \
			ROUND(price, 2) as price, \
			ROUND(price_change_5min, 2) as chg_5m, \
			divergence_signal \
		FROM momentum_divergence \
		WHERE divergence_signal != '⚪ Neutral' \
		ORDER BY timestamp DESC \
		LIMIT 10;" 2>/dev/null
	@echo ""
	@echo "$(BLUE)================================================================================$(NC)"

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
				timestamp AT TIME ZONE 'America/New_York' as current_time, \
				LAG(timestamp AT TIME ZONE 'America/New_York') OVER (ORDER BY timestamp) as prev_time, \
				EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp)))/60 as gap_minutes \
			FROM underlying_quotes \
			WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE \
		) \
		SELECT \
			TO_CHAR(prev_time, 'HH24:MI') as from_time, \
			TO_CHAR(current_time, 'HH24:MI') as to_time, \
			ROUND(gap_minutes::numeric, 1) as gap_minutes \
		FROM time_gaps \
		WHERE gap_minutes > 2 \
		ORDER BY current_time;"

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
		$(PSQL) -c "TRUNCATE TABLE underlying_quotes, option_chains, gex_summary, gex_by_strike, data_quality_log, ingestion_metrics RESTART IDENTITY CASCADE;"; \
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

.PHONY: schema-apply
schema-apply: ## Apply/update database schema (idempotent)
	@echo "$(BLUE)=== Applying Database Schema ===$(NC)"
	@echo "$(YELLOW)Running schema.sql on $(DB_HOST)...$(NC)"
	@PGPASSFILE=~/.pgpass psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) -d $(DB_NAME) -f setup/database/schema.sql
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
	@$(PSQL) -c "VACUUM ANALYZE underlying_quotes;"
	@$(PSQL) -c "VACUUM ANALYZE option_chains;"
	@$(PSQL) -c "VACUUM ANALYZE gex_summary;"
	@$(PSQL) -c "VACUUM ANALYZE gex_by_strike;"
	@echo "$(GREEN)✅ Done$(NC)"

.PHONY: size
size: ## Show table sizes
	@echo "$(BLUE)=== Table Sizes ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			schemaname, \
			tablename, \
			pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size, \
			pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size, \
			pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS index_size \
		FROM pg_tables \
		WHERE schemaname = 'public' \
		AND tablename IN ('underlying_quotes', 'option_chains', 'symbols', 'gex_summary', 'gex_by_strike') \
		ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"

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
			COUNT(DISTINCT option_symbol) as contracts, \
			SUM(volume) FILTER (WHERE option_type = 'C') as call_volume, \
			SUM(volume) FILTER (WHERE option_type = 'P') as put_volume, \
			ROUND(SUM(volume) FILTER (WHERE option_type = 'P')::numeric / NULLIF(SUM(volume) FILTER (WHERE option_type = 'C'), 0), 2) as put_call_ratio, \
			SUM(open_interest) FILTER (WHERE option_type = 'C') as call_oi, \
			SUM(open_interest) FILTER (WHERE option_type = 'P') as put_oi \
		FROM option_chains \
		WHERE timestamp = (SELECT MAX(timestamp) FROM option_chains) \
		GROUP BY expiration \
		ORDER BY expiration;"

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
