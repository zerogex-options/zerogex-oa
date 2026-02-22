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
CREATE INDEX idx_underlying_quotes_timestamp ON underlying_quotes(timestamp DESC);

-- Create index on symbol for filtering
CREATE INDEX idx_underlying_quotes_symbol ON underlying_quotes(symbol);

-- Add comment to table
COMMENT ON TABLE underlying_quotes IS 'Stores 1-minute aggregated underlying symbol quotes from TradeStation';

-- Add comments to columns
COMMENT ON COLUMN underlying_quotes.timestamp IS 'Quote timestamp in ET (1-minute intervals)';
COMMENT ON COLUMN underlying_quotes.open IS 'Open price for the 1-minute bar';
COMMENT ON COLUMN underlying_quotes.high IS 'High price for the 1-minute bar';
COMMENT ON COLUMN underlying_quotes.low IS 'Low price for the 1-minute bar';
COMMENT ON COLUMN underlying_quotes.close IS 'Close price for the 1-minute bar';
COMMENT ON COLUMN underlying_quotes.up_volume IS 'Volume of upticks';
COMMENT ON COLUMN underlying_quotes.down_volume IS 'Volume of downticks';


-- =============================================================================
-- Option Chains Table
-- =============================================================================
-- Stores 1-minute aggregated option contract data
--
-- Primary Key: (option_symbol, timestamp)
-- Timestamp: Stored in ET with timezone awareness (1-minute intervals)
-- Volume/OI: Stored as deltas from previous 1-minute bar
-- =============================================================================

CREATE TABLE IF NOT EXISTS option_chains (
    option_symbol VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    underlying VARCHAR(10) NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    option_type CHAR(1) NOT NULL CHECK (option_type IN ('C', 'P')),
    last NUMERIC(12, 4),
    volume BIGINT DEFAULT 0,
    daily_open_interest BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (option_symbol, timestamp),

    -- Ensure option_type is either 'C' or 'P'
    CONSTRAINT option_type_check CHECK (option_type IN ('C', 'P'))
);

-- Create index on timestamp for time-series queries
CREATE INDEX idx_option_chains_timestamp ON option_chains(timestamp DESC);

-- Create index on underlying for filtering by stock
CREATE INDEX idx_option_chains_underlying ON option_chains(underlying);

-- Create index on expiration for filtering by date
CREATE INDEX idx_option_chains_expiration ON option_chains(expiration);

-- Create composite index for common queries (underlying + expiration + strike)
CREATE INDEX idx_option_chains_underlying_exp_strike ON option_chains(underlying, expiration, strike);

-- Add comment to table
COMMENT ON TABLE option_chains IS 'Stores 1-minute aggregated option chain data from TradeStation';

-- Add comments to columns
COMMENT ON COLUMN option_chains.option_symbol IS 'TradeStation option symbol (e.g., SPY 260221C450)';
COMMENT ON COLUMN option_chains.timestamp IS 'Quote timestamp in ET (1-minute intervals)';
COMMENT ON COLUMN option_chains.underlying IS 'Underlying symbol (e.g., SPY)';
COMMENT ON COLUMN option_chains.strike IS 'Option strike price';
COMMENT ON COLUMN option_chains.expiration IS 'Option expiration date';
COMMENT ON COLUMN option_chains.option_type IS 'Option type: C (call) or P (put)';
COMMENT ON COLUMN option_chains.last IS 'Last trade price for the 1-minute bar';
COMMENT ON COLUMN option_chains.volume IS 'Volume delta from previous 1-minute bar';
COMMENT ON COLUMN option_chains.daily_open_interest IS 'Open interest delta from previous 1-minute bar';


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
CREATE TRIGGER update_underlying_quotes_updated_at
    BEFORE UPDATE ON underlying_quotes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to option_chains
CREATE TRIGGER update_option_chains_updated_at
    BEFORE UPDATE ON option_chains
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- =============================================================================
-- Grant Permissions (Optional)
-- =============================================================================
-- Uncomment and modify as needed for your database users

-- GRANT SELECT, INSERT, UPDATE, DELETE ON underlying_quotes TO zerogex_app;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON option_chains TO zerogex_app;


-- =============================================================================
-- Verification
-- =============================================================================

\echo '✅ Tables created successfully'
\echo ''
\echo 'Verify with:'
\echo '  \dt'
\echo '  SELECT * FROM underlying_quotes LIMIT 5;'
\echo '  SELECT * FROM option_chains LIMIT 5;'
