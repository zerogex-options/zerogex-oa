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

DROP VIEW IF EXISTS flow_by_strike_1min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_5min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_15min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_strike_1day CASCADE;

CREATE VIEW flow_by_strike_1min AS
SELECT timestamp, symbol, strike, total_volume AS volume, total_premium AS premium
FROM flow_cache_by_strike_minute;

CREATE VIEW flow_by_strike_5min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_strike_15min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_strike_1hr AS
SELECT
    date_trunc('hour', timestamp) + INTERVAL '1 hour' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_strike_1day AS
SELECT
    date_trunc('day', timestamp) + INTERVAL '1 day' AS timestamp,
    symbol,
    strike,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_strike_minute
GROUP BY 1, 2, 3;

DROP VIEW IF EXISTS flow_by_expiration_1min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_5min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_15min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_1day CASCADE;

CREATE VIEW flow_by_expiration_1min AS
SELECT timestamp, symbol, expiration, total_volume AS volume, total_premium AS premium
FROM flow_cache_by_expiration_minute;

CREATE VIEW flow_by_expiration_5min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5) * INTERVAL '5 minutes' + INTERVAL '5 minutes' AS timestamp,
    symbol,
    expiration,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_expiration_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_expiration_15min AS
SELECT
    date_trunc('hour', timestamp) + FLOOR(EXTRACT(MINUTE FROM timestamp) / 15) * INTERVAL '15 minutes' + INTERVAL '15 minutes' AS timestamp,
    symbol,
    expiration,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_expiration_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_expiration_1hr AS
SELECT
    date_trunc('hour', timestamp) + INTERVAL '1 hour' AS timestamp,
    symbol,
    expiration,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_expiration_minute
GROUP BY 1, 2, 3;

CREATE VIEW flow_by_expiration_1day AS
SELECT
    date_trunc('day', timestamp) + INTERVAL '1 day' AS timestamp,
    symbol,
    expiration,
    SUM(total_volume)::BIGINT AS volume,
    SUM(total_premium)::NUMERIC(18, 2) AS premium
FROM flow_cache_by_expiration_minute
GROUP BY 1, 2, 3;

-- =============================================================================
-- Flow smart money + buying pressure
-- =============================================================================
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

DROP VIEW IF EXISTS dealer_hedging_pressure CASCADE;
CREATE VIEW dealer_hedging_pressure AS
WITH latest_price AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        timestamp,
        close AS current_price,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) AS price_change
    FROM underlying_quotes
    ORDER BY symbol, timestamp DESC
),
latest_delta AS (
    SELECT
        underlying AS symbol,
        SUM(delta * open_interest * 100)::numeric AS expected_hedge_shares
    FROM (
        SELECT DISTINCT ON (option_symbol)
            option_symbol,
            underlying,
            delta,
            open_interest,
            timestamp
        FROM option_chains
        WHERE timestamp >= NOW() - INTERVAL '10 minutes'
          AND delta IS NOT NULL
          AND open_interest > 0
        ORDER BY option_symbol, timestamp DESC
    ) t
    GROUP BY underlying
)
SELECT
    p.timestamp AT TIME ZONE 'America/New_York' AS time_et,
    p.timestamp,
    p.symbol,
    p.current_price,
    p.price_change,
    COALESCE(d.expected_hedge_shares, 0) AS expected_hedge_shares,
    CASE
        WHEN COALESCE(d.expected_hedge_shares, 0) > 1000000 THEN '🔴 Heavy Sell-Hedging Risk'
        WHEN COALESCE(d.expected_hedge_shares, 0) < -1000000 THEN '🟢 Heavy Buy-Hedging Risk'
        ELSE '⚪ Balanced Hedging'
    END AS hedge_pressure
FROM latest_price p
LEFT JOIN latest_delta d ON d.symbol = p.symbol;

DROP VIEW IF EXISTS unusual_volume_spikes CASCADE;
CREATE VIEW unusual_volume_spikes AS
WITH base AS (
    SELECT
        timestamp AT TIME ZONE 'America/New_York' AS time_et,
        timestamp,
        symbol,
        close AS price,
        (up_volume + down_volume) AS current_volume,
        AVG(up_volume + down_volume) OVER (
            PARTITION BY symbol
            ORDER BY timestamp
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS avg_volume,
        STDDEV_SAMP(up_volume + down_volume) OVER (
            PARTITION BY symbol
            ORDER BY timestamp
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS volume_stddev,
        ROUND(COALESCE((up_volume::numeric / NULLIF(up_volume + down_volume, 0)) * 100, 50), 2) AS buying_pressure_pct
    FROM underlying_quotes
)
SELECT
    time_et,
    timestamp,
    symbol,
    price,
    current_volume,
    COALESCE(avg_volume, 0)::numeric(18,2) AS avg_volume,
    ROUND(COALESCE((current_volume - avg_volume) / NULLIF(volume_stddev, 0), 0)::numeric, 2) AS volume_sigma,
    ROUND(COALESCE(current_volume / NULLIF(avg_volume, 0), 1)::numeric, 2) AS volume_ratio,
    buying_pressure_pct,
    CASE
        WHEN COALESCE((current_volume - avg_volume) / NULLIF(volume_stddev, 0), 0) >= 3 THEN '🚨 Extreme Spike'
        WHEN COALESCE((current_volume - avg_volume) / NULLIF(volume_stddev, 0), 0) >= 2 THEN '⚡ High Spike'
        WHEN COALESCE((current_volume - avg_volume) / NULLIF(volume_stddev, 0), 0) >= 1 THEN '📈 Moderate Spike'
        ELSE '⚪ Normal'
    END AS volume_class
FROM base;

DROP VIEW IF EXISTS momentum_divergence CASCADE;
CREATE VIEW momentum_divergence AS
WITH option_flow AS (
    SELECT
        timestamp,
        symbol,
        SUM(CASE WHEN option_type = 'C' THEN total_premium ELSE -total_premium END)::numeric AS net_option_flow
    FROM flow_cache_by_type_minute
    GROUP BY timestamp, symbol
),
base AS (
    SELECT
        u.timestamp AT TIME ZONE 'America/New_York' AS time_et,
        u.timestamp,
        u.symbol,
        u.close AS price,
        u.close - LAG(u.close, 5) OVER (PARTITION BY u.symbol ORDER BY u.timestamp) AS price_change_5min,
        (u.up_volume - u.down_volume)::bigint AS net_volume,
        o.net_option_flow
    FROM underlying_quotes u
    LEFT JOIN option_flow o ON o.timestamp = u.timestamp AND o.symbol = u.symbol
)
SELECT
    time_et,
    timestamp,
    symbol,
    price,
    ROUND(price_change_5min::numeric, 2) AS price_change_5min,
    net_volume,
    net_option_flow,
    CASE
        WHEN price_change_5min > 0 AND net_option_flow < -50000 THEN '🚨 Bearish Divergence (Price Up, Puts Buying)'
        WHEN price_change_5min < 0 AND net_option_flow > 50000 THEN '🚨 Bullish Divergence (Price Down, Calls Buying)'
        WHEN price_change_5min > 0 AND net_option_flow > 50000 THEN '🟢 Bullish Confirmation'
        WHEN price_change_5min < 0 AND net_option_flow < -50000 THEN '🔴 Bearish Confirmation'
        WHEN price_change_5min > 0 AND net_volume < 0 THEN '⚠️ Weak Rally (Selling Volume)'
        WHEN price_change_5min < 0 AND net_volume > 0 THEN '⚠️ Weak Selloff (Buying Volume)'
        ELSE '⚪ Neutral'
    END AS divergence_signal
FROM base
WHERE price_change_5min IS NOT NULL;

-- =============================================================================
-- Max pain cache tables for /api/max-pain/current
-- =============================================================================
CREATE TABLE IF NOT EXISTS max_pain_oi_snapshot (
    symbol VARCHAR(10) NOT NULL,
    as_of_date DATE NOT NULL,
    source_timestamp TIMESTAMPTZ NOT NULL,
    underlying_price NUMERIC(12, 4) NOT NULL,
    max_pain NUMERIC(12, 4) NOT NULL,
    difference NUMERIC(12, 4) NOT NULL,
    expirations JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, as_of_date)
);

CREATE TABLE IF NOT EXISTS max_pain_oi_snapshot_expiration (
    symbol VARCHAR(10) NOT NULL,
    as_of_date DATE NOT NULL,
    source_timestamp TIMESTAMPTZ NOT NULL,
    expiration DATE NOT NULL,
    max_pain NUMERIC(12, 4) NOT NULL,
    difference_from_underlying NUMERIC(12, 4) NOT NULL,
    strikes JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, as_of_date, expiration)
);

CREATE INDEX IF NOT EXISTS idx_max_pain_oi_snapshot_symbol_date
    ON max_pain_oi_snapshot(symbol, as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_max_pain_oi_snapshot_exp_symbol_exp
    ON max_pain_oi_snapshot_expiration(symbol, as_of_date DESC, expiration);
