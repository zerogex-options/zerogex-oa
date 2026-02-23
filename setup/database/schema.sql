-- =============================================================================
-- ZeroGEX Complete Database Schema with Real-Time Flow Views
-- =============================================================================
-- Single idempotent schema file - safe to run multiple times
-- Run with: psql -h <host> -U <user> -d zerogex -f schema.sql
-- =============================================================================

-- =============================================================================
-- Symbols Reference Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    asset_type VARCHAR(20) CHECK (asset_type IN ('EQUITY', 'INDEX', 'ETF')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_symbols_active ON symbols(is_active) WHERE is_active = TRUE;


-- =============================================================================
-- Underlying Quotes Table
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
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_underlying_quotes_timestamp ON underlying_quotes(timestamp DESC);

-- Add foreign key if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_uq_symbol'
    ) THEN
        ALTER TABLE underlying_quotes 
        ADD CONSTRAINT fk_uq_symbol 
        FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

-- Add constraints if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'check_positive_prices'
    ) THEN
        ALTER TABLE underlying_quotes 
        ADD CONSTRAINT check_positive_prices 
        CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0);
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'check_high_low'
    ) THEN
        ALTER TABLE underlying_quotes 
        ADD CONSTRAINT check_high_low 
        CHECK (high >= low);
    END IF;
END $$;


-- =============================================================================
-- Option Chains Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS option_chains (
    option_symbol VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    underlying VARCHAR(10) NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    option_type CHAR(1) NOT NULL,
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
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (option_symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_option_chains_timestamp ON option_chains(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying ON option_chains(underlying);
CREATE INDEX IF NOT EXISTS idx_option_chains_expiration ON option_chains(expiration);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_timestamp ON option_chains(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_exp_strike ON option_chains(underlying, expiration, strike);

-- Add foreign key if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_oc_underlying'
    ) THEN
        ALTER TABLE option_chains 
        ADD CONSTRAINT fk_oc_underlying 
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

-- Add constraints if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'option_type_check'
    ) THEN
        ALTER TABLE option_chains 
        ADD CONSTRAINT option_type_check 
        CHECK (option_type IN ('C', 'P'));
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'check_positive_strike'
    ) THEN
        ALTER TABLE option_chains 
        ADD CONSTRAINT check_positive_strike 
        CHECK (strike > 0);
    END IF;
END $$;


-- =============================================================================
-- GEX Summary Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS gex_summary (
    underlying VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    max_gamma_strike NUMERIC(12, 4),
    max_gamma_value DOUBLE PRECISION,
    gamma_flip_point DOUBLE PRECISION,
    put_call_ratio DOUBLE PRECISION,
    max_pain DOUBLE PRECISION,
    total_call_volume BIGINT DEFAULT 0,
    total_put_volume BIGINT DEFAULT 0,
    total_call_oi BIGINT DEFAULT 0,
    total_put_oi BIGINT DEFAULT 0,
    total_net_gex DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (underlying, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_gex_summary_timestamp ON gex_summary(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_gex_summary_underlying ON gex_summary(underlying);

-- Add foreign key if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_gex_summary_underlying'
    ) THEN
        ALTER TABLE gex_summary 
        ADD CONSTRAINT fk_gex_summary_underlying 
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;


-- =============================================================================
-- GEX By Strike Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS gex_by_strike (
    underlying VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    total_gamma DOUBLE PRECISION,
    call_gamma DOUBLE PRECISION,
    put_gamma DOUBLE PRECISION,
    net_gex DOUBLE PRECISION,
    call_volume BIGINT DEFAULT 0,
    put_volume BIGINT DEFAULT 0,
    call_oi BIGINT DEFAULT 0,
    put_oi BIGINT DEFAULT 0,
    vanna_exposure DOUBLE PRECISION,
    charm_exposure DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (underlying, timestamp, strike, expiration)
);

CREATE INDEX IF NOT EXISTS idx_gex_by_strike_timestamp ON gex_by_strike(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_gex_by_strike_underlying ON gex_by_strike(underlying);
CREATE INDEX IF NOT EXISTS idx_gex_by_strike_expiration ON gex_by_strike(expiration);

-- Add foreign key if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_gex_strike_underlying'
    ) THEN
        ALTER TABLE gex_by_strike 
        ADD CONSTRAINT fk_gex_strike_underlying 
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;


-- =============================================================================
-- Data Quality Log Table
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


-- =============================================================================
-- Ingestion Metrics Table
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


-- =============================================================================
-- Data Retention Policy Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_retention_policy (
    table_name VARCHAR(100) PRIMARY KEY,
    retention_days INTEGER NOT NULL CHECK (retention_days > 0),
    last_cleanup TIMESTAMPTZ,
    enabled BOOLEAN DEFAULT TRUE,
    notes TEXT
);

-- Insert default policies (will skip if already exists)
INSERT INTO data_retention_policy (table_name, retention_days, notes) VALUES
    ('underlying_quotes', 90, 'Keep 3 months of 1-minute underlying data'),
    ('option_chains', 90, 'Keep 3 months of 1-minute option chain data'),
    ('gex_summary', 90, 'Keep 3 months of GEX summary data'),
    ('gex_by_strike', 90, 'Keep 3 months of per-strike GEX data'),
    ('data_quality_log', 365, 'Keep 1 year of quality logs for analysis'),
    ('ingestion_metrics', 30, 'Keep 1 month of metrics data')
ON CONFLICT (table_name) DO NOTHING;


-- =============================================================================
-- Trigger Function for updated_at
-- =============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply triggers
DROP TRIGGER IF EXISTS update_symbols_updated_at ON symbols;
CREATE TRIGGER update_symbols_updated_at
    BEFORE UPDATE ON symbols
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_underlying_quotes_updated_at ON underlying_quotes;
CREATE TRIGGER update_underlying_quotes_updated_at
    BEFORE UPDATE ON underlying_quotes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_option_chains_updated_at ON option_chains;
CREATE TRIGGER update_option_chains_updated_at
    BEFORE UPDATE ON option_chains
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- =============================================================================
-- Cleanup Function
-- =============================================================================

CREATE OR REPLACE FUNCTION cleanup_old_data()
RETURNS TABLE(table_name TEXT, rows_deleted BIGINT, cleanup_time TIMESTAMPTZ) AS $$
DECLARE
    policy RECORD;
    deleted_count BIGINT;
    cleanup_ts TIMESTAMPTZ;
BEGIN
    FOR policy IN 
        SELECT * FROM data_retention_policy WHERE enabled = TRUE
    LOOP
        cleanup_ts := NOW();
        
        CASE policy.table_name
            WHEN 'underlying_quotes', 'option_chains', 'gex_summary', 'gex_by_strike' THEN
                EXECUTE format(
                    'DELETE FROM %I WHERE timestamp < NOW() - INTERVAL ''%s days''',
                    policy.table_name,
                    policy.retention_days
                );
            WHEN 'data_quality_log' THEN
                EXECUTE format(
                    'DELETE FROM %I WHERE check_timestamp < NOW() - INTERVAL ''%s days''',
                    policy.table_name,
                    policy.retention_days
                );
            WHEN 'ingestion_metrics' THEN
                EXECUTE format(
                    'DELETE FROM %I WHERE timestamp < NOW() - INTERVAL ''%s days''',
                    policy.table_name,
                    policy.retention_days
                );
            ELSE
                CONTINUE;
        END CASE;
        
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        
        UPDATE data_retention_policy 
        SET last_cleanup = cleanup_ts 
        WHERE data_retention_policy.table_name = policy.table_name;
        
        table_name := policy.table_name;
        rows_deleted := deleted_count;
        cleanup_time := cleanup_ts;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Materialized Views
-- =============================================================================

-- Drop old regular views if they exist
DROP VIEW IF EXISTS underlying_quotes_with_deltas;
DROP VIEW IF EXISTS option_chains_with_deltas;

-- Create materialized views
CREATE MATERIALIZED VIEW IF NOT EXISTS underlying_quotes_with_deltas AS
SELECT
    symbol,
    timestamp,
    open, high, low, close,
    up_volume, down_volume,
    COALESCE(up_volume - LAG(up_volume) OVER (PARTITION BY symbol ORDER BY timestamp), 0) AS up_volume_delta,
    COALESCE(down_volume - LAG(down_volume) OVER (PARTITION BY symbol ORDER BY timestamp), 0) AS down_volume_delta,
    updated_at
FROM underlying_quotes;

CREATE UNIQUE INDEX IF NOT EXISTS idx_uq_deltas_symbol_timestamp 
    ON underlying_quotes_with_deltas(symbol, timestamp DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS option_chains_with_deltas AS
SELECT
    option_symbol,
    timestamp,
    underlying,
    strike,
    expiration,
    option_type,
    last, bid, ask,
    volume, open_interest,
    COALESCE(volume - LAG(volume) OVER (PARTITION BY option_symbol ORDER BY timestamp), 0) AS volume_delta,
    COALESCE(open_interest - LAG(open_interest) OVER (PARTITION BY option_symbol ORDER BY timestamp), 0) AS oi_delta,
    implied_volatility,
    delta, gamma, theta, vega,
    updated_at
FROM option_chains;

CREATE UNIQUE INDEX IF NOT EXISTS idx_oc_deltas_symbol_timestamp 
    ON option_chains_with_deltas(option_symbol, timestamp DESC);


-- =============================================================================
-- Refresh Function
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_delta_views()
RETURNS TABLE(view_name TEXT, refresh_status TEXT) AS $$
BEGIN
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY underlying_quotes_with_deltas;
        view_name := 'underlying_quotes_with_deltas';
        refresh_status := 'SUCCESS';
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        view_name := 'underlying_quotes_with_deltas';
        refresh_status := 'FAILED: ' || SQLERRM;
        RETURN NEXT;
    END;
    
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY option_chains_with_deltas;
        view_name := 'option_chains_with_deltas';
        refresh_status := 'SUCCESS';
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        view_name := 'option_chains_with_deltas';
        refresh_status := 'FAILED: ' || SQLERRM;
        RETURN NEXT;
    END;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Real-Time Options Flow & Buying Pressure Views
-- =============================================================================
-- Created for real-time trading decisions with zero lag
-- All views are regular (non-materialized) for instant data
-- Now includes notional value calculations for better trade assessment
-- =============================================================================

-- Drop existing views first (required when adding new columns)
DROP VIEW IF EXISTS option_flow_by_type CASCADE;
DROP VIEW IF EXISTS option_flow_by_strike CASCADE;
DROP VIEW IF EXISTS option_flow_by_expiration CASCADE;
DROP VIEW IF EXISTS option_flow_smart_money CASCADE;
-- underlying_buying_pressure doesn't depend on others, no CASCADE needed
DROP VIEW IF EXISTS underlying_buying_pressure;

-- =============================================================================
-- View 1: Option Flow by Type (Puts vs Calls)
-- =============================================================================
-- Shows aggregate puts vs calls flow across all strikes and expirations
-- Use case: Overall market sentiment, put/call ratio tracking

CREATE VIEW option_flow_by_type AS
SELECT
    timestamp AT TIME ZONE 'America/New_York' as time_et,
    timestamp,
    underlying,
    -- Call flow
    SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_notional,
    COUNT(DISTINCT option_symbol) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_contracts,
    -- Put flow
    SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_notional,
    COUNT(DISTINCT option_symbol) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_contracts,
    -- Net flow (calls - puts)
    SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0) -
    SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as net_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0) -
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as net_notional,
    -- Put/Call ratios
    ROUND(
        SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0)::numeric /
        NULLIF(SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0), 0),
        3
    ) as put_call_ratio,
    ROUND(
        SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0)::numeric /
        NULLIF(SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0), 0),
        3
    ) as put_call_notional_ratio,
    -- Total flow
    SUM(volume_delta) FILTER (WHERE volume_delta > 0) as total_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE volume_delta > 0) as total_notional,
    COUNT(DISTINCT option_symbol) FILTER (WHERE volume_delta > 0) as total_contracts
FROM option_chains_with_deltas
WHERE volume_delta > 0  -- Only actual trades
GROUP BY timestamp, underlying
ORDER BY timestamp DESC;

COMMENT ON VIEW option_flow_by_type IS
'Real-time puts vs calls flow with notional values aggregated across all strikes and expirations. Zero lag.';


-- =============================================================================
-- View 2: Option Flow by Strike
-- =============================================================================
-- Shows flow aggregated by strike across all expirations
-- Use case: Identifying key strike levels with heavy flow

CREATE VIEW option_flow_by_strike AS
SELECT
    timestamp AT TIME ZONE 'America/New_York' as time_et,
    timestamp,
    underlying,
    strike,
    -- Call flow at this strike
    SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_notional,
    COUNT(DISTINCT expiration) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_expirations,
    -- Put flow at this strike
    SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_notional,
    COUNT(DISTINCT expiration) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_expirations,
    -- Net flow at strike
    SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0) -
    SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as net_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0) -
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as net_notional,
    -- Total flow at strike
    SUM(volume_delta) FILTER (WHERE volume_delta > 0) as total_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE volume_delta > 0) as total_notional,
    -- Average Greeks at this strike (for context)
    ROUND(AVG(delta) FILTER (WHERE volume_delta > 0), 4) as avg_delta,
    ROUND(AVG(gamma) FILTER (WHERE volume_delta > 0), 6) as avg_gamma,
    ROUND(AVG(implied_volatility) FILTER (WHERE volume_delta > 0), 4) as avg_iv,
    -- Average price (useful for context)
    ROUND(AVG(last) FILTER (WHERE volume_delta > 0), 2) as avg_price
FROM option_chains_with_deltas
WHERE volume_delta > 0
GROUP BY timestamp, underlying, strike
ORDER BY timestamp DESC, total_notional DESC;

COMMENT ON VIEW option_flow_by_strike IS
'Real-time flow by strike level with notional values across all expirations. Shows key strike concentration.';


-- =============================================================================
-- View 3: Option Flow by Expiration
-- =============================================================================
-- Shows flow aggregated by expiration across all strikes
-- Use case: Identifying which expiry cycles are getting action

CREATE VIEW option_flow_by_expiration AS
SELECT
    timestamp AT TIME ZONE 'America/New_York' as time_et,
    timestamp,
    underlying,
    expiration,
    -- Days to expiration
    (expiration - CURRENT_DATE) as days_to_expiry,
    -- Call flow for this expiration
    SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_notional,
    COUNT(DISTINCT strike) FILTER (WHERE option_type = 'C' AND volume_delta > 0) as call_strikes,
    -- Put flow for this expiration
    SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_notional,
    COUNT(DISTINCT strike) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as put_strikes,
    -- Net flow
    SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0) -
    SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as net_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0) -
    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0) as net_notional,
    -- Total flow
    SUM(volume_delta) FILTER (WHERE volume_delta > 0) as total_flow,
    SUM(volume_delta * last * 100) FILTER (WHERE volume_delta > 0) as total_notional,
    COUNT(DISTINCT option_symbol) FILTER (WHERE volume_delta > 0) as total_contracts,
    -- Average Greeks for this expiration
    ROUND(AVG(implied_volatility) FILTER (WHERE volume_delta > 0), 4) as avg_iv,
    ROUND(AVG(theta) FILTER (WHERE volume_delta > 0), 4) as avg_theta
FROM option_chains_with_deltas
WHERE volume_delta > 0
GROUP BY timestamp, underlying, expiration
ORDER BY timestamp DESC, days_to_expiry ASC;

COMMENT ON VIEW option_flow_by_expiration IS
'Real-time flow by expiration with notional values across all strikes. Shows which expiry cycles are active.';


-- =============================================================================
-- View 4: Smart Money Flow (Unusual Activity)
-- =============================================================================
-- Filters for potentially significant trades indicating informed trading
-- Use case: Spotting unusual activity, large blocks, high IV plays

CREATE VIEW option_flow_smart_money AS
SELECT
    timestamp AT TIME ZONE 'America/New_York' as time_et,
    timestamp,
    option_symbol,
    underlying,
    strike,
    expiration,
    (expiration - CURRENT_DATE) as days_to_expiry,
    option_type,
    -- Flow metrics
    volume_delta as flow,
    volume_delta * last * 100 as notional,
    last as price,
    last - LAG(last) OVER (PARTITION BY option_symbol ORDER BY timestamp) as price_change,
    -- Greeks context
    ROUND(delta, 4) as delta,
    ROUND(gamma, 6) as gamma,
    ROUND(implied_volatility, 4) as iv,
    ROUND(theta, 4) as theta,
    ROUND(vega, 4) as vega,
    -- Classification flags
    CASE
        WHEN volume_delta >= 500 THEN '🔥 Massive Block'
        WHEN volume_delta >= 200 THEN '📦 Large Block'
        WHEN volume_delta >= 100 THEN '📊 Medium Block'
        ELSE '💼 Standard'
    END as size_class,
    CASE
        WHEN volume_delta * last * 100 >= 500000 THEN '💰 $500K+'
        WHEN volume_delta * last * 100 >= 250000 THEN '💵 $250K+'
        WHEN volume_delta * last * 100 >= 100000 THEN '💸 $100K+'
        WHEN volume_delta * last * 100 >= 50000 THEN '💳 $50K+'
        ELSE '💴 <$50K'
    END as notional_class,
    CASE
        WHEN implied_volatility > 1.0 THEN '⚡ Extreme IV'
        WHEN implied_volatility > 0.6 THEN '🌩️ Very High IV'
        WHEN implied_volatility > 0.4 THEN '☁️ High IV'
        ELSE '🌤️ Normal IV'
    END as iv_class,
    CASE
        WHEN ABS(delta) < 0.15 THEN '💰 Deep OTM'
        WHEN ABS(delta) < 0.35 THEN '🎯 OTM'
        WHEN ABS(delta) < 0.65 THEN '⚖️ ATM'
        ELSE '💎 ITM'
    END as moneyness,
    -- Enhanced unusual activity score (0-10) with notional weighting
    LEAST(10, GREATEST(0,
        -- Volume component (0-3 points)
        CASE
            WHEN volume_delta >= 500 THEN 3
            WHEN volume_delta >= 200 THEN 2
            WHEN volume_delta >= 100 THEN 1
            ELSE 0
        END +
        -- Notional component (0-3 points)
        CASE
            WHEN volume_delta * last * 100 >= 500000 THEN 3
            WHEN volume_delta * last * 100 >= 250000 THEN 2
            WHEN volume_delta * last * 100 >= 100000 THEN 1
            ELSE 0
        END +
        -- IV component (0-2 points)
        CASE
            WHEN implied_volatility > 1.0 THEN 2
            WHEN implied_volatility > 0.6 THEN 1
            ELSE 0
        END +
        -- Deep OTM component (0-1 point)
        CASE
            WHEN ABS(delta) < 0.15 THEN 1
            ELSE 0
        END +
        -- Short DTE component (0-1 point)
        CASE
            WHEN (expiration - CURRENT_DATE) <= 2 THEN 1
            ELSE 0
        END
    )) as unusual_score
FROM option_chains_with_deltas
WHERE
    volume_delta > 0
    AND (
        volume_delta >= 50  -- Minimum volume threshold
        OR volume_delta * last * 100 >= 50000  -- Or $50K+ notional
        OR implied_volatility > 0.4  -- High IV plays
        OR (ABS(delta) < 0.15 AND volume_delta >= 20)  -- Deep OTM with decent volume
    )
ORDER BY timestamp DESC, unusual_score DESC, notional DESC;

COMMENT ON VIEW option_flow_smart_money IS
'Real-time unusual activity detection with notional values. Filters for large blocks, high IV, and deep OTM plays.';


-- =============================================================================
-- View 5: Underlying Buying Pressure Time Series
-- =============================================================================
-- Shows directional flow in the underlying over time
-- Use case: Correlate with option flow to spot hedging, confirm trends

CREATE VIEW underlying_buying_pressure AS
SELECT
    timestamp AT TIME ZONE 'America/New_York' as time_et,
    timestamp,
    symbol,
    -- OHLC
    open,
    high,
    low,
    close,
    -- Volume breakdown
    up_volume,
    down_volume,
    up_volume + down_volume as total_volume,
    -- Volume deltas (from view)
    up_volume_delta,
    down_volume_delta,
    up_volume_delta + down_volume_delta as total_volume_delta,
    -- Buying pressure percentage
    ROUND(
        CASE
            WHEN (up_volume + down_volume) > 0
            THEN (up_volume::numeric / (up_volume + down_volume) * 100)
            ELSE 50
        END,
        2
    ) as buying_pressure_pct,
    -- Delta-based buying pressure (actual trades this period)
    ROUND(
        CASE
            WHEN (up_volume_delta + down_volume_delta) > 0
            THEN (up_volume_delta::numeric / (up_volume_delta + down_volume_delta) * 100)
            ELSE 50
        END,
        2
    ) as period_buying_pressure_pct,
    -- Price change
    close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) as price_change,
    ROUND(
        ((close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)) /
        NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp), 0) * 100),
        3
    ) as price_change_pct,
    -- Momentum classification
    CASE
        WHEN up_volume_delta::numeric / NULLIF(up_volume_delta + down_volume_delta, 0) > 0.7
            THEN '🟢 Strong Buying'
        WHEN up_volume_delta::numeric / NULLIF(up_volume_delta + down_volume_delta, 0) > 0.55
            THEN '✅ Buying'
        WHEN up_volume_delta::numeric / NULLIF(up_volume_delta + down_volume_delta, 0) >= 0.45
            THEN '⚪ Neutral'
        WHEN up_volume_delta::numeric / NULLIF(up_volume_delta + down_volume_delta, 0) >= 0.3
            THEN '❌ Selling'
        ELSE '🔴 Strong Selling'
    END as momentum
FROM underlying_quotes_with_deltas
ORDER BY timestamp DESC;

COMMENT ON VIEW underlying_buying_pressure IS
'Real-time buying vs selling pressure in underlying. Shows directional flow and momentum.';


-- =============================================================================
-- Performance Indexes for Flow Views
-- =============================================================================
-- These indexes optimize the real-time views for fast queries

-- Index for time-based filtering (most common use case)
CREATE INDEX IF NOT EXISTS idx_option_chains_timestamp_volfilter 
    ON option_chains(timestamp DESC) 
    WHERE volume > 0;

CREATE INDEX IF NOT EXISTS idx_underlying_quotes_timestamp_desc
    ON underlying_quotes(timestamp DESC);

-- Composite indexes for multi-column GROUP BY queries
CREATE INDEX IF NOT EXISTS idx_option_chains_timestamp_strike 
    ON option_chains(timestamp DESC, strike);

CREATE INDEX IF NOT EXISTS idx_option_chains_timestamp_expiration 
    ON option_chains(timestamp DESC, expiration);

CREATE INDEX IF NOT EXISTS idx_option_chains_timestamp_type 
    ON option_chains(timestamp DESC, option_type);

-- Index for smart money filtering
CREATE INDEX IF NOT EXISTS idx_option_chains_iv_volume 
    ON option_chains(implied_volatility, volume) 
    WHERE implied_volatility IS NOT NULL;


-- =============================================================================
-- Verification & Stats
-- =============================================================================

\echo ''
\echo '✅ ZeroGEX schema setup complete'
\echo ''

-- Verify all tables exist
SELECT 
    tablename,
    schemaname
FROM pg_tables
WHERE schemaname = 'public'
    AND tablename IN (
        'symbols',
        'underlying_quotes',
        'option_chains',
        'gex_summary',
        'gex_by_strike',
        'data_quality_log',
        'ingestion_metrics',
        'data_retention_policy'
    )
ORDER BY tablename;

\echo ''

-- Verify all views exist
SELECT 
    viewname,
    definition IS NOT NULL as has_definition
FROM pg_views 
WHERE schemaname = 'public' 
    AND viewname IN (
        'option_flow_by_type',
        'option_flow_by_strike', 
        'option_flow_by_expiration',
        'option_flow_smart_money',
        'underlying_buying_pressure'
    )
ORDER BY viewname;

\echo ''

-- Verify materialized views exist
SELECT 
    matviewname,
    schemaname
FROM pg_matviews
WHERE schemaname = 'public'
    AND matviewname IN (
        'underlying_quotes_with_deltas',
        'option_chains_with_deltas'
    )
ORDER BY matviewname;

\echo ''
\echo 'Next steps:'
\echo '  1. Add symbols: INSERT INTO symbols (symbol, name, asset_type) VALUES (''SPY'', ''SPDR S&P 500'', ''ETF'');'
\echo '  2. Test functions: SELECT * FROM refresh_delta_views();'
\echo '  3. Test cleanup: SELECT * FROM cleanup_old_data();'
\echo '  4. Test flow views: SELECT * FROM option_flow_by_type LIMIT 5;'
\echo ''
\echo 'Makefile shortcuts for flow analysis:'
\echo '  make flow-by-type'
\echo '  make flow-by-strike'
\echo '  make flow-by-expiration'
\echo '  make flow-smart-money'
\echo '  make flow-buying-pressure'
\echo '  make flow-live              # Combined dashboard'
\echo ''
