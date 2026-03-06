-- =============================================================================
-- ZeroGEX Schema (Idempotent, minimal production footprint)
-- =============================================================================

-- Keep legacy support table for existing foreign-key relationships.
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
-- REQUIRED CORE TABLES (kept as-is)
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
CREATE INDEX IF NOT EXISTS idx_underlying_quotes_symbol_timestamp
    ON underlying_quotes(symbol, timestamp DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_uq_symbol') THEN
        ALTER TABLE underlying_quotes
        ADD CONSTRAINT fk_uq_symbol
        FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_positive_prices') THEN
        ALTER TABLE underlying_quotes
        ADD CONSTRAINT check_positive_prices
        CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_high_low') THEN
        ALTER TABLE underlying_quotes
        ADD CONSTRAINT check_high_low
        CHECK (high >= low);
    END IF;
END $$;

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
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_time_type_strike
    ON option_chains(underlying, timestamp DESC, option_type, strike);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_option_symbol_timestamp
    ON option_chains(underlying, option_symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_timestamp_option_symbol
    ON option_chains(underlying, timestamp DESC, option_symbol);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_oc_underlying') THEN
        ALTER TABLE option_chains
        ADD CONSTRAINT fk_oc_underlying
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'option_type_check') THEN
        ALTER TABLE option_chains
        ADD CONSTRAINT option_type_check
        CHECK (option_type IN ('C', 'P'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_positive_strike') THEN
        ALTER TABLE option_chains
        ADD CONSTRAINT check_positive_strike
        CHECK (strike > 0);
    END IF;
END $$;

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
CREATE INDEX IF NOT EXISTS idx_gex_summary_underlying_timestamp ON gex_summary(underlying, timestamp DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_gex_summary_underlying') THEN
        ALTER TABLE gex_summary
        ADD CONSTRAINT fk_gex_summary_underlying
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

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
CREATE INDEX IF NOT EXISTS idx_gex_by_strike_underlying_timestamp_strike ON gex_by_strike(underlying, timestamp DESC, strike);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_gex_strike_underlying') THEN
        ALTER TABLE gex_by_strike
        ADD CONSTRAINT fk_gex_strike_underlying
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

-- =============================================================================
-- Remove legacy/non-essential objects (safe cleanup during migration)
-- =============================================================================
DROP TABLE IF EXISTS data_quality_log CASCADE;
DROP TABLE IF EXISTS ingestion_metrics CASCADE;
DROP TABLE IF EXISTS data_retention_policy CASCADE;
DROP FUNCTION IF EXISTS cleanup_old_data();

-- =============================================================================
-- Shared utility
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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
-- Base deltas
-- =============================================================================
DROP VIEW IF EXISTS option_chains_with_deltas CASCADE;
CREATE VIEW option_chains_with_deltas AS
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
    COALESCE(
        GREATEST(
            volume - LAG(volume) OVER (
                PARTITION BY option_symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
                ORDER BY timestamp
            ),
            0
        ),
        0
    ) AS volume_delta,
    COALESCE(open_interest - LAG(open_interest) OVER (PARTITION BY option_symbol ORDER BY timestamp), 0) AS oi_delta,
    implied_volatility,
    delta,
    gamma,
    theta,
    vega,
    updated_at
FROM option_chains;

-- =============================================================================
-- Real-time flow cache tables
-- =============================================================================
CREATE TABLE IF NOT EXISTS flow_cache_by_type_minute (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    option_type CHAR(1) NOT NULL CHECK (option_type IN ('C', 'P')),
    total_volume BIGINT NOT NULL,
    total_premium NUMERIC(18, 2) NOT NULL,
    avg_iv NUMERIC(10, 6),
    net_delta NUMERIC(18, 4),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, symbol, option_type)
);

CREATE TABLE IF NOT EXISTS flow_cache_by_strike_minute (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    total_volume BIGINT NOT NULL,
    total_premium NUMERIC(18, 2) NOT NULL,
    avg_iv NUMERIC(10, 6),
    net_delta NUMERIC(18, 4),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, symbol, strike)
);

CREATE TABLE IF NOT EXISTS flow_cache_by_expiration_minute (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    expiration DATE NOT NULL,
    total_volume BIGINT NOT NULL,
    total_premium NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, symbol, expiration)
);

CREATE TABLE IF NOT EXISTS flow_cache_smart_money_minute (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    option_symbol VARCHAR(50) NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    option_type CHAR(1) NOT NULL CHECK (option_type IN ('C', 'P')),
    total_volume BIGINT NOT NULL,
    total_premium NUMERIC(18, 2) NOT NULL,
    avg_iv NUMERIC(10, 6),
    avg_delta NUMERIC(10, 6),
    unusual_activity_score NUMERIC(5, 2),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, symbol, option_symbol)
);

CREATE INDEX IF NOT EXISTS idx_flow_cache_by_type_symbol_ts
    ON flow_cache_by_type_minute(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_flow_cache_by_strike_symbol_ts
    ON flow_cache_by_strike_minute(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_flow_cache_by_expiration_symbol_ts
    ON flow_cache_by_expiration_minute(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_flow_cache_smart_money_symbol_ts
    ON flow_cache_smart_money_minute(symbol, timestamp DESC);

-- =============================================================================
-- Interval flow views: 1min/5min/15min/1hr/1day
-- =============================================================================

-- Flow by Type views
DROP VIEW IF EXISTS flow_by_type_1min CASCADE;
DROP VIEW IF EXISTS flow_by_type_5min CASCADE;
DROP VIEW IF EXISTS flow_by_type_15min CASCADE;
DROP VIEW IF EXISTS flow_by_type_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_type_1day CASCADE;

CREATE VIEW flow_by_type_1min AS
SELECT
    timestamp,
    symbol,
    option_type,
    total_volume AS volume,
    total_premium AS premium
FROM flow_cache_by_type_minute;

CREATE VIEW flow_by_type_5min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes' AS timestamp,
    symbol,
    option_type,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_type_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_type_15min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes' AS timestamp,
    symbol,
    option_type,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_type_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_type_1hr AS
SELECT
    date_trunc('hour', timestamp) + INTERVAL '1 hour' AS timestamp,
    symbol,
    option_type,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_type_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_type_1day AS
SELECT
    date_trunc('day', timestamp) + INTERVAL '1 day' AS timestamp,
    symbol,
    option_type,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_type_minute
GROUP BY 1, 2, 3;

-- Flow by Strike views
DROP VIEW IF EXISTS flow_by_strike_1min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_5min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_15min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_strike_1day CASCADE;

CREATE VIEW flow_by_strike_1min AS
SELECT
    timestamp,
    symbol,
    strike,
    total_volume AS volume,
    total_premium AS premium,
    net_delta AS net_volume,
    net_delta * (total_premium::numeric / NULLIF(total_volume, 0)) AS net_premium
FROM flow_cache_by_strike_minute;

CREATE VIEW flow_by_strike_5min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium,
    SUM(net_delta)::BIGINT AS net_volume,
    SUM(net_delta * (total_premium::numeric / NULLIF(total_volume, 0)))::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_strike_15min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium,
    SUM(net_delta)::BIGINT AS net_volume,
    SUM(net_delta * (total_premium::numeric / NULLIF(total_volume, 0)))::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_strike_1hr AS
SELECT
    date_trunc('hour', timestamp) + INTERVAL '1 hour' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium,
    SUM(net_delta)::BIGINT AS net_volume,
    SUM(net_delta * (total_premium::numeric / NULLIF(total_volume, 0)))::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_strike_1day AS
SELECT
    date_trunc('day', timestamp) + INTERVAL '1 day' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium,
    SUM(net_delta)::BIGINT AS net_volume,
    SUM(net_delta * (total_premium::numeric / NULLIF(total_volume, 0)))::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

-- Flow by Expiration views
DROP VIEW IF EXISTS flow_by_expiration_1min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_5min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_15min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_1day CASCADE;

CREATE VIEW flow_by_expiration_1min AS
WITH call_put_split AS (
    SELECT
        c.timestamp,
        c.symbol,
        e.expiration,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_volume ELSE 0 END)::BIGINT AS call_volume,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_volume ELSE 0 END)::BIGINT AS put_volume,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_premium ELSE 0 END)::NUMERIC AS call_premium,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_premium ELSE 0 END)::NUMERIC AS put_premium
    FROM flow_cache_by_type_minute c
    JOIN flow_cache_by_expiration_minute e ON e.timestamp = c.timestamp AND e.symbol = c.symbol
    GROUP BY c.timestamp, c.symbol, e.expiration
)
SELECT
    e.timestamp,
    e.symbol,
    e.expiration,
    e.total_volume AS volume,
    e.total_premium AS premium,
    COALESCE(s.call_volume - s.put_volume, 0)::BIGINT AS net_volume,
    COALESCE(s.call_premium - s.put_premium, 0)::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_expiration_minute e
LEFT JOIN call_put_split s ON s.timestamp = e.timestamp AND s.symbol = e.symbol AND s.expiration = e.expiration;

CREATE VIEW flow_by_expiration_5min AS
WITH call_put_split AS (
    SELECT
        date_trunc('hour', c.timestamp) + FLOOR(EXTRACT(MINUTE FROM c.timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes' AS bucket_ts,
        c.symbol,
        e.expiration,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_volume ELSE 0 END)::BIGINT AS call_volume,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_volume ELSE 0 END)::BIGINT AS put_volume,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_premium ELSE 0 END)::NUMERIC AS call_premium,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_premium ELSE 0 END)::NUMERIC AS put_premium
    FROM flow_cache_by_type_minute c
    JOIN flow_cache_by_expiration_minute e ON e.timestamp = c.timestamp AND e.symbol = c.symbol
    GROUP BY 1, 2, e.expiration
)
SELECT
    date_trunc('hour', e.timestamp) + FLOOR(EXTRACT(MINUTE FROM e.timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes' AS timestamp,
    e.symbol,
    e.expiration,
    SUM(e.total_volume)::BIGINT AS volume,
    SUM(e.total_premium)::NUMERIC(18, 2) AS premium,
    COALESCE(MAX(s.call_volume - s.put_volume), 0)::BIGINT AS net_volume,
    COALESCE(MAX(s.call_premium - s.put_premium), 0)::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_expiration_minute e
LEFT JOIN call_put_split s ON s.bucket_ts = date_trunc('hour', e.timestamp) + FLOOR(EXTRACT(MINUTE FROM e.timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes'
    AND s.symbol = e.symbol AND s.expiration = e.expiration
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_expiration_15min AS
WITH call_put_split AS (
    SELECT
        date_trunc('hour', c.timestamp) + FLOOR(EXTRACT(MINUTE FROM c.timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes' AS bucket_ts,
        c.symbol,
        e.expiration,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_volume ELSE 0 END)::BIGINT AS call_volume,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_volume ELSE 0 END)::BIGINT AS put_volume,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_premium ELSE 0 END)::NUMERIC AS call_premium,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_premium ELSE 0 END)::NUMERIC AS put_premium
    FROM flow_cache_by_type_minute c
    JOIN flow_cache_by_expiration_minute e ON e.timestamp = c.timestamp AND e.symbol = c.symbol
    GROUP BY 1, 2, e.expiration
)
SELECT
    date_trunc('hour', e.timestamp) + FLOOR(EXTRACT(MINUTE FROM e.timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes' AS timestamp,
    e.symbol,
    e.expiration,
    SUM(e.total_volume)::BIGINT AS volume,
    SUM(e.total_premium)::NUMERIC(18, 2) AS premium,
    COALESCE(MAX(s.call_volume - s.put_volume), 0)::BIGINT AS net_volume,
    COALESCE(MAX(s.call_premium - s.put_premium), 0)::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_expiration_minute e
LEFT JOIN call_put_split s ON s.bucket_ts = date_trunc('hour', e.timestamp) + FLOOR(EXTRACT(MINUTE FROM e.timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes'
    AND s.symbol = e.symbol AND s.expiration = e.expiration
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_expiration_1hr AS
WITH call_put_split AS (
    SELECT
        date_trunc('hour', c.timestamp) + INTERVAL '1 hour' AS bucket_ts,
        c.symbol,
        e.expiration,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_volume ELSE 0 END)::BIGINT AS call_volume,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_volume ELSE 0 END)::BIGINT AS put_volume,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_premium ELSE 0 END)::NUMERIC AS call_premium,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_premium ELSE 0 END)::NUMERIC AS put_premium
    FROM flow_cache_by_type_minute c
    JOIN flow_cache_by_expiration_minute e ON e.timestamp = c.timestamp AND e.symbol = c.symbol
    GROUP BY 1, 2, e.expiration
)
SELECT
    date_trunc('hour', e.timestamp) + INTERVAL '1 hour' AS timestamp,
    e.symbol,
    e.expiration,
    SUM(e.total_volume)::BIGINT AS volume,
    SUM(e.total_premium)::NUMERIC(18, 2) AS premium,
    COALESCE(MAX(s.call_volume - s.put_volume), 0)::BIGINT AS net_volume,
    COALESCE(MAX(s.call_premium - s.put_premium), 0)::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_expiration_minute e
LEFT JOIN call_put_split s ON s.bucket_ts = date_trunc('hour', e.timestamp) + INTERVAL '1 hour'
    AND s.symbol = e.symbol AND s.expiration = e.expiration
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_expiration_1day AS
WITH call_put_split AS (
    SELECT
        date_trunc('day', c.timestamp) + INTERVAL '1 day' AS bucket_ts,
        c.symbol,
        e.expiration,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_volume ELSE 0 END)::BIGINT AS call_volume,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_volume ELSE 0 END)::BIGINT AS put_volume,
        SUM(CASE WHEN c.option_type = 'C' THEN c.total_premium ELSE 0 END)::NUMERIC AS call_premium,
        SUM(CASE WHEN c.option_type = 'P' THEN c.total_premium ELSE 0 END)::NUMERIC AS put_premium
    FROM flow_cache_by_type_minute c
    JOIN flow_cache_by_expiration_minute e ON e.timestamp = c.timestamp AND e.symbol = c.symbol
    GROUP BY 1, 2, e.expiration
)
SELECT
    date_trunc('day', e.timestamp) + INTERVAL '1 day' AS timestamp,
    e.symbol,
    e.expiration,
    SUM(e.total_volume)::BIGINT AS volume,
    SUM(e.total_premium)::NUMERIC(18, 2) AS premium,
    COALESCE(MAX(s.call_volume - s.put_volume), 0)::BIGINT AS net_volume,
    COALESCE(MAX(s.call_premium - s.put_premium), 0)::NUMERIC(18, 2) AS net_premium
FROM flow_cache_by_expiration_minute e
LEFT JOIN call_put_split s ON s.bucket_ts = date_trunc('day', e.timestamp) + INTERVAL '1 day'
    AND s.symbol = e.symbol AND s.expiration = e.expiration
GROUP BY 1, 2, 3;

-- =============================================================================
-- Flow smart money + buying pressure
-- =============================================================================

-- Smart money interval views: 1min/5min/15min/1hr/1day
DROP VIEW IF EXISTS flow_smart_money_1min CASCADE;
DROP VIEW IF EXISTS flow_smart_money_5min CASCADE;
DROP VIEW IF EXISTS flow_smart_money_15min CASCADE;
DROP VIEW IF EXISTS flow_smart_money_1hr CASCADE;
DROP VIEW IF EXISTS flow_smart_money_1day CASCADE;

CREATE VIEW flow_smart_money_1min AS
SELECT
    timestamp,
    symbol,
    option_symbol AS contract,
    strike,
    expiration,
    (expiration - CURRENT_DATE) AS dte,
    option_type,
    total_volume AS flow,
    total_premium AS notional,
    avg_delta AS delta,
    unusual_activity_score AS score
FROM flow_cache_smart_money_minute;

CREATE VIEW flow_smart_money_5min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes' AS timestamp,
    symbol,
    option_symbol AS contract,
    strike,
    expiration,
    (expiration - CURRENT_DATE) AS dte,
    option_type,
    SUM(total_volume)::BIGINT AS flow,
    SUM(total_premium)::NUMERIC(18, 2) AS notional,
    AVG(avg_delta)::NUMERIC(10, 6) AS delta,
    MAX(unusual_activity_score)::NUMERIC(5, 2) AS score
FROM flow_cache_smart_money_minute
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE VIEW flow_smart_money_15min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes' AS timestamp,
    symbol,
    option_symbol AS contract,
    strike,
    expiration,
    (expiration - CURRENT_DATE) AS dte,
    option_type,
    SUM(total_volume)::BIGINT AS flow,
    SUM(total_premium)::NUMERIC(18, 2) AS notional,
    AVG(avg_delta)::NUMERIC(10, 6) AS delta,
    MAX(unusual_activity_score)::NUMERIC(5, 2) AS score
FROM flow_cache_smart_money_minute
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE VIEW flow_smart_money_1hr AS
SELECT
    date_trunc('hour', timestamp) + INTERVAL '1 hour' AS timestamp,
    symbol,
    option_symbol AS contract,
    strike,
    expiration,
    (expiration - CURRENT_DATE) AS dte,
    option_type,
    SUM(total_volume)::BIGINT AS flow,
    SUM(total_premium)::NUMERIC(18, 2) AS notional,
    AVG(avg_delta)::NUMERIC(10, 6) AS delta,
    MAX(unusual_activity_score)::NUMERIC(5, 2) AS score
FROM flow_cache_smart_money_minute
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE VIEW flow_smart_money_1day AS
SELECT
    date_trunc('day', timestamp) + INTERVAL '1 day' AS timestamp,
    symbol,
    option_symbol AS contract,
    strike,
    expiration,
    (expiration - CURRENT_DATE) AS dte,
    option_type,
    SUM(total_volume)::BIGINT AS flow,
    SUM(total_premium)::NUMERIC(18, 2) AS notional,
    AVG(avg_delta)::NUMERIC(10, 6) AS delta,
    MAX(unusual_activity_score)::NUMERIC(5, 2) AS score
FROM flow_cache_smart_money_minute
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- Legacy view with formatting (for backwards compatibility)
DROP VIEW IF EXISTS option_flow_smart_money CASCADE;
CREATE VIEW option_flow_smart_money AS
SELECT
    c.timestamp AT TIME ZONE 'America/New_York' AS time_et,
    c.timestamp,
    c.option_symbol AS contract,
    c.strike,
    c.expiration,
    (c.expiration - CURRENT_DATE) AS dte,
    c.option_type,
    c.total_volume AS flow,
    c.total_premium AS notional,
    ROUND((c.total_premium / NULLIF(c.total_volume, 0) / 100)::numeric, 2) AS price,
    c.unusual_activity_score AS score,
    CASE
        WHEN c.total_premium >= 500000 THEN '💰 $500K+'
        WHEN c.total_premium >= 250000 THEN '💵 $250K+'
        WHEN c.total_premium >= 100000 THEN '💸 $100K+'
        WHEN c.total_premium >= 50000 THEN '💳 $50K+'
        ELSE '💴 <$50K'
    END AS notional_class,
    CASE
        WHEN c.total_volume >= 500 THEN '🔥 Massive Block'
        WHEN c.total_volume >= 200 THEN '📦 Large Block'
        WHEN c.total_volume >= 100 THEN '📊 Medium Block'
        ELSE '💼 Standard'
    END AS size_class
FROM flow_cache_smart_money_minute c;

DROP VIEW IF EXISTS underlying_buying_pressure CASCADE;
CREATE VIEW underlying_buying_pressure AS
SELECT
    q.timestamp AT TIME ZONE 'America/New_York' AS time_et,
    q.timestamp,
    q.symbol,
    q.close AS price,
    (q.up_volume - q.down_volume)::bigint AS vol,
    ROUND(COALESCE((q.up_volume::numeric / NULLIF((q.up_volume + q.down_volume), 0)) * 100, 50), 2) AS buy_pct,
    CASE
        WHEN (q.up_volume - q.down_volume) >= 50000 THEN '🟢 Strong Buying'
        WHEN (q.up_volume - q.down_volume) > 0 THEN '✅ Buying'
        WHEN (q.up_volume - q.down_volume) <= -50000 THEN '❌ Selling'
        ELSE '⚪ Neutral'
    END AS momentum
FROM underlying_quotes q;

-- =============================================================================
-- Day-trading views (Makefile + API)
-- =============================================================================
DROP VIEW IF EXISTS underlying_vwap_deviation CASCADE;
CREATE VIEW underlying_vwap_deviation AS
WITH base AS (
    SELECT
        symbol,
        timestamp,
        close AS price,
        (up_volume + down_volume) AS volume,
        SUM(close * (up_volume + down_volume)) OVER (
            PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_pv,
        SUM(up_volume + down_volume) OVER (
            PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_vol
    FROM underlying_quotes
)
SELECT
    timestamp AT TIME ZONE 'America/New_York' AS time_et,
    timestamp,
    symbol,
    price,
    (cum_pv / NULLIF(cum_vol, 0))::numeric(12,4) AS vwap,
    ROUND(((price - (cum_pv / NULLIF(cum_vol, 0))) / NULLIF((cum_pv / NULLIF(cum_vol, 0)), 0) * 100)::numeric, 3) AS vwap_deviation_pct,
    volume,
    CASE
        WHEN price > (cum_pv / NULLIF(cum_vol, 0)) * 1.002 THEN '🔥 Extended Above VWAP'
        WHEN price > (cum_pv / NULLIF(cum_vol, 0)) THEN '✅ Above VWAP'
        WHEN price < (cum_pv / NULLIF(cum_vol, 0)) * 0.998 THEN '🔥 Extended Below VWAP'
        ELSE '❌ Below VWAP'
    END AS vwap_position
FROM base;

DROP VIEW IF EXISTS opening_range_breakout CASCADE;
CREATE VIEW opening_range_breakout AS
WITH first_30min AS (
    SELECT
        symbol,
        DATE(timestamp AT TIME ZONE 'America/New_York') AS trade_date,
        MAX(high) AS orb_high,
        MIN(low) AS orb_low,
        MAX(high) - MIN(low) AS orb_range
    FROM underlying_quotes
    WHERE EXTRACT(HOUR FROM timestamp AT TIME ZONE 'America/New_York') = 9
      AND EXTRACT(MINUTE FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 30 AND 59
    GROUP BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
)
SELECT
    q.timestamp AT TIME ZONE 'America/New_York' AS time_et,
    q.timestamp,
    q.symbol,
    q.close AS current_price,
    orb.orb_high,
    orb.orb_low,
    orb.orb_range,
    ROUND(q.close - orb.orb_high, 2) AS distance_above_orb_high,
    ROUND(orb.orb_low - q.close, 2) AS distance_below_orb_low,
    ROUND((q.close - orb.orb_low) / NULLIF(orb.orb_range, 0) * 100, 1) AS orb_pct,
    CASE
        WHEN q.close > orb.orb_high THEN '🚀 ORB Breakout (Long)'
        WHEN q.close < orb.orb_low THEN '💥 ORB Breakdown (Short)'
        WHEN q.close >= orb.orb_high * 0.998 THEN '⚡ Near ORB High'
        WHEN q.close <= orb.orb_low * 1.002 THEN '⚡ Near ORB Low'
        ELSE '⏸️ Inside ORB'
    END AS orb_status,
    (q.up_volume + q.down_volume) AS volume
FROM underlying_quotes q
JOIN first_30min orb
  ON q.symbol = orb.symbol
 AND DATE(q.timestamp AT TIME ZONE 'America/New_York') = orb.trade_date;

DROP VIEW IF EXISTS gamma_exposure_levels CASCADE;
CREATE VIEW gamma_exposure_levels AS
WITH latest_options AS (
    SELECT DISTINCT ON (option_symbol)
        option_symbol,
        underlying,
        strike,
        option_type,
        gamma,
        open_interest
    FROM option_chains
    WHERE timestamp >= NOW() - INTERVAL '10 minutes'
      AND gamma IS NOT NULL
      AND open_interest > 0
    ORDER BY option_symbol, timestamp DESC
)
SELECT
    underlying,
    strike,
    SUM(CASE WHEN option_type = 'C' THEN gamma * open_interest * 100 ELSE -gamma * open_interest * 100 END) AS net_gex,
    SUM(ABS(gamma * open_interest * 100)) AS total_gex,
    SUM(gamma * open_interest * 100) FILTER (WHERE option_type = 'C') AS call_gex,
    SUM(gamma * open_interest * 100) FILTER (WHERE option_type = 'P') AS put_gex,
    COUNT(*) AS num_contracts,
    SUM(open_interest) AS total_oi,
    CASE
        WHEN SUM(CASE WHEN option_type = 'C' THEN gamma * open_interest * 100 ELSE -gamma * open_interest * 100 END) > 1000000 THEN '🟢 Strong +GEX'
        WHEN SUM(CASE WHEN option_type = 'C' THEN gamma * open_interest * 100 ELSE -gamma * open_interest * 100 END) < -1000000 THEN '🔴 Strong -GEX'
        ELSE '⚪ Neutral GEX'
    END AS gex_level
FROM latest_options
GROUP BY underlying, strike;
