-- ZeroGEX Database Schema
-- Migration 001: Create underlying_quotes and option_chains tables
-- 
-- Run with: psql -h <host> -U <user> -d zerogex -f sql/001_create_tables.sql

-- =============================================================================
-- Underlying Quotes Table
-- =============================================================================
-- Stores 1-minute aggregated underlying symbol quotes
--
-- Primary Key: (symbol, timestamp)
-- Timestamp: Stored in ET with timezone awareness (1-minute intervals)
-- Volume: Stores RAW cumulative volume values (not deltas)
-- =============================================================================

CREATE TABLE IF NOT EXISTS underlying_quotes (
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(12, 4) NOT NULL,
    high NUMERIC(12, 4) NOT NULL,
    low NUMERIC(12, 4) NOT NULL,
    close NUMERIC(12, 4) NOT NULL,
    up_volume BIGINT DEFAULT 0,
    down_volume BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, timestamp)
);

-- Create index on timestamp for time-series queries
CREATE INDEX IF NOT EXISTS idx_underlying_quotes_timestamp ON underlying_quotes(timestamp DESC);

-- Create index on symbol for filtering
CREATE INDEX IF NOT EXISTS idx_underlying_quotes_symbol ON underlying_quotes(symbol);

-- Add comment to table
COMMENT ON TABLE underlying_quotes IS 'Stores 1-minute aggregated underlying symbol quotes from TradeStation';

-- Add comments to columns
COMMENT ON COLUMN underlying_quotes.timestamp IS 'Quote timestamp in ET (1-minute bucket boundaries)';
COMMENT ON COLUMN underlying_quotes.open IS 'Open price for the 1-minute bar (first value in bucket)';
COMMENT ON COLUMN underlying_quotes.high IS 'High price for the 1-minute bar (max value in bucket)';
COMMENT ON COLUMN underlying_quotes.low IS 'Low price for the 1-minute bar (min value in bucket)';
COMMENT ON COLUMN underlying_quotes.close IS 'Close price for the 1-minute bar (last value in bucket)';
COMMENT ON COLUMN underlying_quotes.up_volume IS 'RAW cumulative uptick volume (use LAG() to calculate deltas in queries)';
COMMENT ON COLUMN underlying_quotes.down_volume IS 'RAW cumulative downtick volume (use LAG() to calculate deltas in queries)';


-- =============================================================================
-- Option Chains Table
-- =============================================================================
-- Stores 1-minute aggregated option contract data
--
-- Primary Key: (option_symbol, timestamp)
-- Timestamp: Stored in ET with timezone awareness (1-minute intervals)
-- Volume/OI: Stores RAW cumulative values (not deltas)
--           Use LAG() window functions to calculate deltas in queries
-- =============================================================================

CREATE TABLE IF NOT EXISTS option_chains (
    option_symbol VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    underlying VARCHAR(10) NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    option_type CHAR(1) NOT NULL CHECK (option_type IN ('C', 'P')),
    last NUMERIC(12, 4),
    bid NUMERIC(12, 4),
    ask NUMERIC(12, 4),
    volume BIGINT DEFAULT 0,
    open_interest BIGINT DEFAULT 0,
    implied_volatility NUMERIC(8, 6),
    delta NUMERIC(8, 6),
    gamma NUMERIC(10, 8),
    theta NUMERIC(10, 6),
    vega NUMERIC(10, 6),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (option_symbol, timestamp),

    -- Ensure option_type is either 'C' or 'P'
    CONSTRAINT option_type_check CHECK (option_type IN ('C', 'P'))
);

-- Create index on timestamp for time-series queries
CREATE INDEX IF NOT EXISTS idx_option_chains_timestamp ON option_chains(timestamp DESC);

-- Create index on underlying for filtering by stock
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying ON option_chains(underlying);

-- Create index on expiration for filtering by date
CREATE INDEX IF NOT EXISTS idx_option_chains_expiration ON option_chains(expiration);

-- Create composite index for common queries (underlying + expiration + strike)
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_exp_strike ON option_chains(underlying, expiration, strike);

-- Add comment to table
COMMENT ON TABLE option_chains IS 'Stores 1-minute aggregated option chain data from TradeStation';

-- Add comments to columns
COMMENT ON COLUMN option_chains.option_symbol IS 'TradeStation option symbol (e.g., SPY 260221C450)';
COMMENT ON COLUMN option_chains.timestamp IS 'Quote timestamp in ET (1-minute bucket boundaries)';
COMMENT ON COLUMN option_chains.underlying IS 'Underlying symbol (e.g., SPY)';
COMMENT ON COLUMN option_chains.strike IS 'Option strike price';
COMMENT ON COLUMN option_chains.expiration IS 'Option expiration date';
COMMENT ON COLUMN option_chains.option_type IS 'Option type: C (call) or P (put)';
COMMENT ON COLUMN option_chains.last IS 'Last trade price for the 1-minute bar (last value in bucket)';
COMMENT ON COLUMN option_chains.bid IS 'Bid price (last value in bucket)';
COMMENT ON COLUMN option_chains.ask IS 'Ask price (last value in bucket)';
COMMENT ON COLUMN option_chains.volume IS 'RAW cumulative volume (use LAG() to calculate deltas in queries)';
COMMENT ON COLUMN option_chains.open_interest IS 'RAW open interest (use LAG() to calculate deltas in queries)';
COMMENT ON COLUMN option_chains.implied_volatility IS 'Implied volatility (if available from API)';
COMMENT ON COLUMN option_chains.delta IS 'Option delta (if available from API)';
COMMENT ON COLUMN option_chains.gamma IS 'Option gamma (if available from API)';
COMMENT ON COLUMN option_chains.theta IS 'Option theta (if available from API)';
COMMENT ON COLUMN option_chains.vega IS 'Option vega (if available from API)';


-- =============================================================================
-- GEX Calculations Table
-- =============================================================================
-- Stores calculated gamma exposure values
-- =============================================================================

CREATE TABLE IF NOT EXISTS gex_calculations (
    underlying VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    total_gamma NUMERIC(20, 8),
    call_gamma NUMERIC(20, 8),
    put_gamma NUMERIC(20, 8),
    net_gex NUMERIC(20, 2),
    call_oi BIGINT DEFAULT 0,
    put_oi BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (underlying, timestamp, strike)
);

CREATE INDEX IF NOT EXISTS idx_gex_timestamp ON gex_calculations(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_gex_underlying ON gex_calculations(underlying);

COMMENT ON TABLE gex_calculations IS 'Calculated gamma exposure (GEX) values per strike';
COMMENT ON COLUMN gex_calculations.net_gex IS 'Net gamma exposure: (call_gamma * call_oi * 100 * spot^2) - (put_gamma * put_oi * 100 * spot^2)';


-- =============================================================================
-- Data Quality Monitoring Table
-- =============================================================================
-- Tracks data quality metrics and gaps
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_quality_log (
    id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(10),
    issue_type VARCHAR(100) NOT NULL,
    issue_description TEXT,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    severity VARCHAR(20) CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_quality_timestamp ON data_quality_log(check_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_quality_resolved ON data_quality_log(resolved);

COMMENT ON TABLE data_quality_log IS 'Tracks data quality issues, gaps, and staleness';


-- =============================================================================
-- Ingestion Metrics Table
-- =============================================================================
-- Tracks ingestion pipeline performance and health
-- =============================================================================

CREATE TABLE IF NOT EXISTS ingestion_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metric_type VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(20, 4),
    labels JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON ingestion_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_type ON ingestion_metrics(metric_type);

COMMENT ON TABLE ingestion_metrics IS 'Ingestion pipeline metrics for monitoring and alerting';


-- =============================================================================
-- Trigger Functions for updated_at
-- =============================================================================
-- Automatically update updated_at timestamp on row updates

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to underlying_quotes
DROP TRIGGER IF EXISTS update_underlying_quotes_updated_at ON underlying_quotes;
CREATE TRIGGER update_underlying_quotes_updated_at
    BEFORE UPDATE ON underlying_quotes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to option_chains
DROP TRIGGER IF EXISTS update_option_chains_updated_at ON option_chains;
CREATE TRIGGER update_option_chains_updated_at
    BEFORE UPDATE ON option_chains
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- =============================================================================
-- Helper Views for Delta Calculations
-- =============================================================================

-- View to calculate volume deltas for underlying quotes
CREATE OR REPLACE VIEW underlying_quotes_with_deltas AS
SELECT 
    symbol,
    timestamp,
    open,
    high,
    low,
    close,
    up_volume,
    down_volume,
    up_volume - LAG(up_volume) OVER (PARTITION BY symbol ORDER BY timestamp) AS up_volume_delta,
    down_volume - LAG(down_volume) OVER (PARTITION BY symbol ORDER BY timestamp) AS down_volume_delta,
    created_at,
    updated_at
FROM underlying_quotes;

COMMENT ON VIEW underlying_quotes_with_deltas IS 'Underlying quotes with calculated volume deltas';


-- View to calculate volume/OI deltas for options
CREATE OR REPLACE VIEW option_chains_with_deltas AS
SELECT 
    option_symbol,
    timestamp,
    underlying,
    strike,
    expiration,
    option_type,
    last,
    bid,
    ask,
    volume,
    open_interest,
    volume - LAG(volume) OVER (PARTITION BY option_symbol ORDER BY timestamp) AS volume_delta,
    open_interest - LAG(open_interest) OVER (PARTITION BY option_symbol ORDER BY timestamp) AS oi_delta,
    implied_volatility,
    delta,
    gamma,
    theta,
    vega,
    created_at,
    updated_at
FROM option_chains;

COMMENT ON VIEW option_chains_with_deltas IS 'Option chains with calculated volume and open interest deltas';


-- =============================================================================
-- Grant Permissions (Optional)
-- =============================================================================
-- Uncomment and modify as needed for your database users

-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO zerogex_app;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO zerogex_app;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO zerogex_app;


-- =============================================================================
-- Verification
-- =============================================================================

\echo '✅ Tables created successfully'
\echo ''
\echo 'Verify with:'
\echo '  \dt'
\echo '  \dv'
\echo '  SELECT * FROM underlying_quotes LIMIT 5;'
\echo '  SELECT * FROM option_chains LIMIT 5;'
\echo '  SELECT * FROM underlying_quotes_with_deltas LIMIT 5;'
