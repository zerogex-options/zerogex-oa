# ZeroGEX Database Query Shortcuts
# ===================================
# Usage: make <target>
#
# Common queries for monitoring and debugging the ZeroGEX platform

# Load database connection from .env
include .env
export

# PostgreSQL connection string
PSQL = PGPASSFILE=~/.pgpass psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) -d $(DB_NAME)

# Colors for output
BLUE = \033[0;34m
GREEN = \033[0;32m
YELLOW = \033[1;33m
NC = \033[0m

.PHONY: help
help: ## Show this help message
	@echo "$(BLUE)ZeroGEX Database Query Shortcuts$(NC)"
	@echo "=================================="
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
	@echo ""
	@echo "$(GREEN)Greeks & Analytics:$(NC)"
	@echo "  make greeks             - Latest Greeks by strike"
	@echo "  make greeks-summary     - Greeks summary statistics"
	@echo "  make gex-preview        - Preview GEX calculation data"
	@echo ""
	@echo "$(GREEN)Data Quality:$(NC)"
	@echo "  make gaps               - Check for data gaps"
	@echo "  make gaps-today         - Today's data gaps"
	@echo "  make quality            - Data quality report"
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
		FROM option_chains;"

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

.PHONY: underlying-count
underlying-count: ## Count underlying bars by hour
	@echo "$(BLUE)=== Underlying Bars by Hour (Last 24h) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			DATE_TRUNC('hour', timestamp AT TIME ZONE 'America/New_York') as hour_et, \
			COUNT(*) as bars \
		FROM underlying_quotes \
		WHERE timestamp > NOW() - INTERVAL '24 hours' \
		GROUP BY DATE_TRUNC('hour', timestamp AT TIME ZONE 'America/New_York') \
		ORDER BY hour_et DESC;"

.PHONY: options-count
options-count: ## Count option quotes by hour
	@echo "$(BLUE)=== Option Quotes by Hour (Last 24h) ===$(NC)"
	@$(PSQL) -c "\
		SELECT \
			DATE_TRUNC('hour', timestamp AT TIME ZONE 'America/New_York') as hour_et, \
			COUNT(*) as quotes, \
			COUNT(DISTINCT option_symbol) as unique_contracts \
		FROM option_chains \
		WHERE timestamp > NOW() - INTERVAL '24 hours' \
		GROUP BY DATE_TRUNC('hour', timestamp AT TIME ZONE 'America/New_York') \
		ORDER BY hour_et DESC;"

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
	@echo "$(YELLOW)If age is > 5 minutes during market hours, streaming may be stuck.$(NC)"

.PHONY: logs
logs: ## Watch ingestion logs in real-time
	@echo "$(BLUE)=== Watching ZeroGEX Logs (Ctrl+C to stop) ===$(NC)"
	@sudo journalctl -u zerogex-oa-ingestion -f -n 50

.PHONY: logs-grep
logs-grep: ## Grep logs for specific pattern (use: make logs-grep PATTERN="Greeks")
	@sudo journalctl -u zerogex-oa-ingestion -n 1000 --no-pager | grep "$(PATTERN)" || echo "No matches found for: $(PATTERN)"

.PHONY: logs-errors
logs-errors: ## Show recent errors in logs
	@echo "$(BLUE)=== Recent Errors ===$(NC)"
	@sudo journalctl -u zerogex-oa-ingestion -p err -n 50 --no-pager

.PHONY: check-config
check-config: ## Check ZeroGEX configuration
	@echo "$(BLUE)=== ZeroGEX Configuration Check ===$(NC)"
	@echo "Checking .env file..."
	@grep "GREEKS_ENABLED" .env || echo "GREEKS_ENABLED not found in .env"
	@grep "INGEST_UNDERLYING" .env || echo "INGEST_UNDERLYING not found in .env"
	@grep "INGEST_EXPIRATIONS" .env || echo "INGEST_EXPIRATIONS not found in .env"
	@echo ""
	@echo "Checking if ingestion is running..."
	@ps aux | grep "[p]ython.*main_engine" || echo "Main engine not running"

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
# Maintenance
# =============================================================================

.PHONY: vacuum
vacuum: ## Vacuum analyze all tables
	@echo "$(YELLOW)Running VACUUM ANALYZE on all tables...$(NC)"
	@$(PSQL) -c "VACUUM ANALYZE underlying_quotes;"
	@$(PSQL) -c "VACUUM ANALYZE option_chains;"
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

.PHONY: refresh-views
refresh-views: ## Refresh materialized views
	@echo "$(YELLOW)Refreshing materialized views...$(NC)"
	@$(PSQL) -c "REFRESH MATERIALIZED VIEW CONCURRENTLY underlying_quotes_with_deltas;"
	@$(PSQL) -c "REFRESH MATERIALIZED VIEW CONCURRENTLY option_chains_with_deltas;"
	@echo "$(GREEN)✅ Done$(NC)"

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
