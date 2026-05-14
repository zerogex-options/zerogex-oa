-- =============================================================================
-- ZeroGEX Schema (Idempotent, minimal production footprint)
-- =============================================================================

-- Required symbol registry used by FK relationships.
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
    mid NUMERIC(12, 4),
    volume BIGINT DEFAULT 0,
    open_interest BIGINT DEFAULT 0,
    ask_volume BIGINT DEFAULT 0,
    mid_volume BIGINT DEFAULT 0,
    bid_volume BIGINT DEFAULT 0,
    implied_volatility NUMERIC(8, 6),
    delta NUMERIC(8, 6),
    gamma NUMERIC(10, 8),
    theta NUMERIC(10, 6),
    vega NUMERIC(10, 6),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (option_symbol, timestamp)
);

-- Idempotent migration: add new columns if they don't exist yet (for existing databases)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='mid'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN mid NUMERIC(12, 4);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='ask_volume'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN ask_volume BIGINT DEFAULT 0;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='mid_volume'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN mid_volume BIGINT DEFAULT 0;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='bid_volume'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN bid_volume BIGINT DEFAULT 0;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='implied_volatility'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN implied_volatility NUMERIC(8, 6);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='delta'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN delta NUMERIC(8, 6);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='gamma'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN gamma NUMERIC(10, 8);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='theta'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN theta NUMERIC(10, 6);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='option_chains' AND column_name='vega'
    ) THEN
        ALTER TABLE option_chains ADD COLUMN vega NUMERIC(10, 6);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_option_chains_timestamp ON option_chains(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying ON option_chains(underlying);
CREATE INDEX IF NOT EXISTS idx_option_chains_expiration ON option_chains(expiration);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_timestamp ON option_chains(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_ts_gamma
    ON option_chains(underlying, timestamp DESC)
    WHERE gamma IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_exp_strike ON option_chains(underlying, expiration, strike);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_time_type_strike
    ON option_chains(underlying, timestamp DESC, option_type, strike);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_option_symbol_timestamp
    ON option_chains(underlying, option_symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_timestamp_option_symbol
    ON option_chains(underlying, timestamp DESC, option_symbol);
CREATE INDEX IF NOT EXISTS idx_option_chains_option_symbol_timestamp
    ON option_chains(option_symbol, timestamp DESC);
-- Covering index for /api/option/quote: lets the planner satisfy
-- "WHERE underlying = $1 [optional filters] ORDER BY timestamp DESC LIMIT 1"
-- as an index-only scan when only `underlying` is supplied (the hot path)
-- and avoids heap fetches for the full SELECT list.
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_ts_quote_covering
    ON option_chains(underlying, timestamp DESC)
    INCLUDE (bid, ask, volume, open_interest, strike, expiration, option_type);
-- Partial covering index for AnalyticsEngine._get_snapshot()'s
-- DISTINCT ON (option_symbol) ... ORDER BY option_symbol, timestamp DESC query.
-- Matches the ORDER BY natively (no Sort step), restricts the index to rows
-- with Greeks populated (no heap fetch to evaluate gamma IS NOT NULL), and
-- carries expiration in INCLUDE so the expiration > min_expiration filter
-- can also be evaluated from the index leaf.  Without this index the planner
-- falls back to idx_option_chains_underlying_option_symbol_timestamp and
-- heap-fetches every candidate row to evaluate gamma IS NOT NULL, which on
-- a 96-hour lookback wedges the query for tens of minutes (see commit
-- history / PR description for the May 13 incident EXPLAIN plans).  Build
-- with CREATE INDEX CONCURRENTLY in production via
-- ``make db-add-distinct-on-index`` to avoid blocking the option_chains
-- writers; this schema entry serves fresh installs and idempotent retries.
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_option_symbol_ts_gamma
    ON option_chains(underlying, option_symbol, timestamp DESC)
    INCLUDE (expiration)
    WHERE gamma IS NOT NULL;

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

-- =============================================================================
-- VIX rolling window of 5-minute bars — used by /api/market/vix endpoint
-- (level score uses latest close; momentum score needs a multi-bar window).
-- Populated by the ingestion engine's VIX poller.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vix_bars (
    timestamp TIMESTAMPTZ PRIMARY KEY,
    open NUMERIC(10, 4),
    high NUMERIC(10, 4),
    low NUMERIC(10, 4),
    close NUMERIC(10, 4) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vix_bars_timestamp ON vix_bars(timestamp DESC);

-- =============================================================================
-- TradeStation API call counts per 5-minute UTC window.
-- Each ingestion process upserts its window count at window rollover; the
-- ON CONFLICT clause sums counts across processes that share the same window.
-- =============================================================================
CREATE TABLE IF NOT EXISTS tradestation_api_calls (
    window_start TIMESTAMPTZ PRIMARY KEY,
    call_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tradestation_api_calls_window_start
    ON tradestation_api_calls(window_start DESC);

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
    flip_distance DOUBLE PRECISION,
    local_gex DOUBLE PRECISION,
    convexity_risk DOUBLE PRECISION,
    -- Canonical Call/Put Wall strikes (industry-standard: maximum dollar
    -- gamma exposure on each side of spot, with nearest-to-spot tiebreaker).
    -- Computed by the Analytics Engine and consumed by all downstream
    -- endpoints / signals as the single source of record.  See
    -- src/analytics/walls.py.
    call_wall NUMERIC(12, 4),
    put_wall  NUMERIC(12, 4),
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

-- Idempotent migration: intraday risk fields for enriched GEX summary.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='gex_summary' AND column_name='flip_distance'
    ) THEN
        ALTER TABLE gex_summary
            ADD COLUMN flip_distance DOUBLE PRECISION;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='gex_summary' AND column_name='local_gex'
    ) THEN
        ALTER TABLE gex_summary
            ADD COLUMN local_gex DOUBLE PRECISION;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='gex_summary' AND column_name='convexity_risk'
    ) THEN
        ALTER TABLE gex_summary
            ADD COLUMN convexity_risk DOUBLE PRECISION;
    END IF;

    -- Canonical Call/Put Wall strikes.  Backfilled NULL for historical rows;
    -- the Analytics Engine populates them on each new run.  See
    -- src/analytics/walls.py for the canonical definition.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='gex_summary' AND column_name='call_wall'
    ) THEN
        ALTER TABLE gex_summary
            ADD COLUMN call_wall NUMERIC(12, 4);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='gex_summary' AND column_name='put_wall'
    ) THEN
        ALTER TABLE gex_summary
            ADD COLUMN put_wall NUMERIC(12, 4);
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

-- Idempotent migration: split per-type call/put greeks AND store the
-- dealer-sign convention (dealers are net short the book, so dealer
-- charm/vanna flow is the negative of market-aggregated charm/vanna).
-- Downstream signals that want a tradable "dealer hedging flow" should
-- read dealer_charm_exposure / dealer_vanna_exposure; the raw
-- charm_exposure / vanna_exposure columns remain for backwards compat.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='gex_by_strike' AND column_name='dealer_charm_exposure'
    ) THEN
        ALTER TABLE gex_by_strike
            ADD COLUMN call_charm_exposure DOUBLE PRECISION,
            ADD COLUMN put_charm_exposure  DOUBLE PRECISION,
            ADD COLUMN call_vanna_exposure DOUBLE PRECISION,
            ADD COLUMN put_vanna_exposure  DOUBLE PRECISION,
            ADD COLUMN dealer_charm_exposure DOUBLE PRECISION,
            ADD COLUMN dealer_vanna_exposure DOUBLE PRECISION,
            ADD COLUMN expiration_bucket VARCHAR(16);
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

DROP TRIGGER IF EXISTS update_vix_bars_updated_at ON vix_bars;
CREATE TRIGGER update_vix_bars_updated_at
    BEFORE UPDATE ON vix_bars
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_tradestation_api_calls_updated_at ON tradestation_api_calls;
CREATE TRIGGER update_tradestation_api_calls_updated_at
    BEFORE UPDATE ON tradestation_api_calls
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
    mid,
    volume,
    open_interest,
    ask_volume,
    mid_volume,
    bid_volume,
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
    COALESCE(open_interest - LAG(open_interest) OVER (
        PARTITION BY option_symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
        ORDER BY timestamp
    ), 0) AS oi_delta,
    implied_volatility,
    delta,
    gamma,
    theta,
    vega,
    updated_at
FROM option_chains;

-- =============================================================================
-- Canonical option flow facts + legacy rollup cache tables
-- =============================================================================
CREATE TABLE IF NOT EXISTS flow_contract_facts (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    option_symbol VARCHAR(50) NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    option_type CHAR(1) NOT NULL CHECK (option_type IN ('C', 'P')),
    volume_delta BIGINT NOT NULL,
    premium_delta NUMERIC(18, 2) NOT NULL,
    signed_volume BIGINT NOT NULL,
    signed_premium NUMERIC(18, 2) NOT NULL,
    buy_volume BIGINT NOT NULL DEFAULT 0,
    sell_volume BIGINT NOT NULL DEFAULT 0,
    buy_premium NUMERIC(18, 2) NOT NULL DEFAULT 0,
    sell_premium NUMERIC(18, 2) NOT NULL DEFAULT 0,
    implied_volatility NUMERIC(10, 6),
    delta NUMERIC(10, 6),
    underlying_price NUMERIC(12, 4),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, symbol, option_symbol)
);

CREATE INDEX IF NOT EXISTS idx_flow_contract_facts_symbol_ts
    ON flow_contract_facts(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_flow_contract_facts_symbol_ts_strike
    ON flow_contract_facts(symbol, timestamp DESC, strike);
CREATE INDEX IF NOT EXISTS idx_flow_contract_facts_symbol_ts_exp
    ON flow_contract_facts(symbol, timestamp DESC, expiration);
CREATE INDEX IF NOT EXISTS idx_flow_contract_facts_symbol_ts_type
    ON flow_contract_facts(symbol, timestamp DESC, option_type);

-- Unified 5-minute-bucketed flow rollup keyed by (type, strike, expiration).
-- Replaces the legacy flow_by_type / flow_by_strike / flow_by_expiration
-- cache tables, which have been consolidated into a single source of truth.
-- Each row stores DAY-TO-DATE cumulative values for one contract as of the
-- end of its 5-minute bucket. The session is aligned to TradeStation's RTH
-- window (09:30–16:15 ET), so cumulative counters reset at 09:30 ET.
DROP TABLE IF EXISTS flow_by_type CASCADE;
DROP TABLE IF EXISTS flow_by_strike CASCADE;
DROP TABLE IF EXISTS flow_by_expiration CASCADE;

CREATE TABLE IF NOT EXISTS flow_by_contract (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    option_type CHAR(1) NOT NULL CHECK (option_type IN ('C', 'P')),
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    raw_volume BIGINT NOT NULL DEFAULT 0,
    raw_premium NUMERIC(18, 2) NOT NULL DEFAULT 0,
    net_volume BIGINT NOT NULL DEFAULT 0,
    net_premium NUMERIC(18, 2) NOT NULL DEFAULT 0,
    underlying_price NUMERIC(12, 4),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, symbol, option_type, strike, expiration)
);

-- Idempotent migration: add the new cumulative columns and drop legacy
-- bucket-sum columns on existing installations. Safe to run repeatedly.
ALTER TABLE flow_by_contract ADD COLUMN IF NOT EXISTS raw_volume BIGINT NOT NULL DEFAULT 0;
ALTER TABLE flow_by_contract ADD COLUMN IF NOT EXISTS raw_premium NUMERIC(18, 2) NOT NULL DEFAULT 0;
ALTER TABLE flow_by_contract ADD COLUMN IF NOT EXISTS net_volume BIGINT NOT NULL DEFAULT 0;
ALTER TABLE flow_by_contract ADD COLUMN IF NOT EXISTS net_premium NUMERIC(18, 2) NOT NULL DEFAULT 0;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS total_volume;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS total_premium;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS buy_volume;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS sell_volume;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS buy_premium;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS sell_premium;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS avg_iv;
ALTER TABLE flow_by_contract DROP COLUMN IF EXISTS avg_delta;

CREATE TABLE IF NOT EXISTS flow_smart_money (
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
    underlying_price NUMERIC(12, 4),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, symbol, option_symbol)
);

CREATE INDEX IF NOT EXISTS idx_flow_by_contract_symbol_ts
    ON flow_by_contract(symbol, timestamp DESC);
-- idx_flow_by_contract_symbol_ts_strike intentionally omitted.
-- See `make flow-index-prune`: planner consistently prefers
-- idx_flow_by_contract_symbol_ts_type + heap recheck for strike
-- filters; the strike index was 55 MB (50% of all flow index storage)
-- for 0.001% of total scans. Existing deployments can drop it via
-- `make flow-index-prune CONFIRM=yes`.
CREATE INDEX IF NOT EXISTS idx_flow_by_contract_symbol_ts_exp
    ON flow_by_contract(symbol, timestamp DESC, expiration);
CREATE INDEX IF NOT EXISTS idx_flow_by_contract_symbol_ts_type
    ON flow_by_contract(symbol, timestamp DESC, option_type);
-- Covering index for /api/flow/series. Carries the columns the
-- filtered CTE selects so the planner can satisfy
-- "WHERE symbol = $1 AND timestamp BETWEEN $2 AND $3" as an
-- index-only scan and skip the ~3,000 heap-page bitmap-heap-scan that
-- dominates execution time on a cache miss (see database.py
-- get_flow_series, the `filtered` CTE). Stays useful even after the
-- planned flow_series_5min snapshot table lands — at that point the
-- /api/flow/series read path bypasses flow_by_contract entirely and
-- this index can be DROP'd CONCURRENTLY to reclaim ~200 MB.
CREATE INDEX IF NOT EXISTS idx_flow_by_contract_symbol_ts_series_covering
    ON flow_by_contract(symbol, timestamp DESC)
    INCLUDE (option_type, strike, expiration, raw_volume, net_volume, net_premium);
CREATE INDEX IF NOT EXISTS idx_flow_smart_money_symbol_ts
    ON flow_smart_money(symbol, timestamp DESC);

-- Symbol FKs on the flow tables. Mirrors the pattern other tables use
-- (option_chains, gex_summary, gex_by_strike) so deleting a symbol
-- cascades through the flow rollups instead of leaving dangling rows.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_flow_contract_facts_symbol') THEN
        ALTER TABLE flow_contract_facts
        ADD CONSTRAINT fk_flow_contract_facts_symbol
        FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_flow_by_contract_symbol') THEN
        ALTER TABLE flow_by_contract
        ADD CONSTRAINT fk_flow_by_contract_symbol
        FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_flow_smart_money_symbol') THEN
        ALTER TABLE flow_smart_money
        ADD CONSTRAINT fk_flow_smart_money_symbol
        FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

-- =============================================================================
-- Interval flow views removed — queries go directly to the 1-min cache tables.
-- Drop any existing views from prior installations.
-- =============================================================================

DROP VIEW IF EXISTS flow_by_type_1min CASCADE;
DROP VIEW IF EXISTS flow_by_type_5min CASCADE;
DROP VIEW IF EXISTS flow_by_type_15min CASCADE;
DROP VIEW IF EXISTS flow_by_type_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_type_1day CASCADE;

DROP VIEW IF EXISTS flow_by_strike_1min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_5min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_15min CASCADE;
DROP VIEW IF EXISTS flow_by_strike_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_strike_1day CASCADE;

DROP VIEW IF EXISTS flow_by_expiration_1min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_5min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_15min CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_1hr CASCADE;
DROP VIEW IF EXISTS flow_by_expiration_1day CASCADE;

DROP VIEW IF EXISTS flow_smart_money_1min CASCADE;
DROP VIEW IF EXISTS flow_smart_money_5min CASCADE;
DROP VIEW IF EXISTS flow_smart_money_15min CASCADE;
DROP VIEW IF EXISTS flow_smart_money_1hr CASCADE;
DROP VIEW IF EXISTS flow_smart_money_1day CASCADE;

DROP VIEW IF EXISTS option_flow_smart_money CASCADE;

-- =============================================================================
-- Flow buying pressure + technicals views (kept)
-- =============================================================================

DROP VIEW IF EXISTS underlying_daily_volume CASCADE;
CREATE VIEW underlying_daily_volume AS
SELECT
    symbol,
    DATE(timestamp AT TIME ZONE 'America/New_York') AS trade_date_et,
    SUM(COALESCE(up_volume, 0) + COALESCE(down_volume, 0))::bigint AS cumulative_daily_volume
FROM underlying_quotes
GROUP BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York');

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
-- Technicals views (Makefile + API)
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

DROP VIEW IF EXISTS unusual_volume_spikes CASCADE;
CREATE VIEW unusual_volume_spikes AS
WITH base AS (
    SELECT
        timestamp AT TIME ZONE 'America/New_York' AS time_et,
        timestamp,
        symbol,
        close AS price,
        up_volume,
        down_volume,
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
        ROUND(
            COALESCE(
                up_volume::numeric / NULLIF((up_volume + down_volume)::numeric, 0) * 100,
                50
            ),
            2
        ) AS buying_pressure_pct
    FROM underlying_quotes
)
SELECT
    time_et,
    timestamp,
    symbol,
    price,
    up_volume,
    down_volume,
    current_volume,
    COALESCE(avg_volume, 0)::numeric(18,2) AS avg_volume,
    ROUND(
        COALESCE((current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0), 0),
        2
    ) AS volume_sigma,
    ROUND(
        COALESCE(current_volume::numeric / NULLIF(avg_volume, 0), 1),
        2
    ) AS volume_ratio,
    buying_pressure_pct,
    CASE
        WHEN COALESCE((current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0), 0) >= 3
            THEN '🚨 Extreme Spike'
        WHEN COALESCE((current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0), 0) >= 2
            THEN '⚡ High Spike'
        WHEN COALESCE((current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0), 0) >= 1
            THEN '📈 Moderate Spike'
        ELSE '⚪ Normal'
    END AS volume_class
FROM base;

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
        SUM(delta * open_interest::numeric * 100) AS expected_hedge_shares
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
        WHEN COALESCE(d.expected_hedge_shares, 0) > 1000000
            THEN '🔴 Heavy Sell-Hedging Risk'
        WHEN COALESCE(d.expected_hedge_shares, 0) < -1000000
            THEN '🟢 Heavy Buy-Hedging Risk'
        ELSE '⚪ Balanced Hedging'
    END AS hedge_pressure
FROM latest_price p
LEFT JOIN latest_delta d ON d.symbol::text = p.symbol::text;

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
    SUM(
        CASE
            WHEN option_type = 'C' THEN gamma * open_interest::numeric * 100
            ELSE -gamma * open_interest::numeric * 100
        END
    ) AS net_gex,
    SUM(ABS(gamma * open_interest::numeric * 100)) AS total_gex,
    SUM(gamma * open_interest::numeric * 100) FILTER (WHERE option_type = 'C') AS call_gex,
    SUM(gamma * open_interest::numeric * 100) FILTER (WHERE option_type = 'P') AS put_gex,
    COUNT(*) AS num_contracts,
    SUM(open_interest) AS total_oi,
    CASE
        WHEN SUM(
            CASE
                WHEN option_type = 'C' THEN gamma * open_interest::numeric * 100
                ELSE -gamma * open_interest::numeric * 100
            END
        ) > 1000000 THEN '🟢 Strong +GEX'
        WHEN SUM(
            CASE
                WHEN option_type = 'C' THEN gamma * open_interest::numeric * 100
                ELSE -gamma * open_interest::numeric * 100
            END
        ) < -1000000 THEN '🔴 Strong -GEX'
        ELSE '⚪ Neutral GEX'
    END AS gex_level
FROM latest_options
GROUP BY underlying, strike;

-- =============================================================================
-- Trade Signals Tables
-- Append to the bottom of setup/database/schema.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- trade_signals
-- Written by AnalyticsEngine every ~5 min.
-- One row per (underlying, timestamp, timeframe).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trade_signals (
    underlying      VARCHAR(10)     NOT NULL,
    timestamp       TIMESTAMPTZ     NOT NULL,
    timeframe       VARCHAR(20)     NOT NULL CHECK (timeframe IN ('intraday', 'swing', 'multi_day')),

    -- Composite score
    composite_score         INTEGER         NOT NULL,
    max_possible_score      INTEGER         NOT NULL,
    normalized_score        NUMERIC(6, 4)   NOT NULL,   -- 0.000 – 1.000

    -- Direction / strength
    direction               VARCHAR(10)     NOT NULL CHECK (direction IN ('bullish', 'bearish', 'neutral')),
    strength                VARCHAR(10)     NOT NULL CHECK (strength IN ('high', 'medium', 'low')),
    estimated_win_pct       NUMERIC(6, 4)   NOT NULL,

    -- Trade idea
    trade_type              VARCHAR(30)     NOT NULL,
    trade_rationale         TEXT,
    target_expiry           VARCHAR(20),
    suggested_strikes       VARCHAR(100),

    -- Raw context values stored for display / backtesting
    current_price           NUMERIC(12, 4),
    net_gex                 DOUBLE PRECISION,
    gamma_flip              DOUBLE PRECISION,
    price_vs_flip           NUMERIC(8, 4),
    vwap                    NUMERIC(12, 4),
    vwap_deviation_pct      NUMERIC(8, 4),
    put_call_ratio          DOUBLE PRECISION,
    dealer_net_delta        DOUBLE PRECISION,
    smart_money_direction   VARCHAR(10),
    unusual_volume_detected BOOLEAN         DEFAULT FALSE,
    orb_breakout_direction  VARCHAR(10),

    -- Full component breakdown (JSON array of SignalComponent objects)
    components              JSONB,

    created_at              TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (underlying, timestamp, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_trade_signals_underlying_ts
    ON trade_signals(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trade_signals_underlying_tf_ts
    ON trade_signals(underlying, timeframe, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trade_signals_direction
    ON trade_signals(direction, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trade_signals_strength
    ON trade_signals(strength, timestamp DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_trade_signals_underlying') THEN
        ALTER TABLE trade_signals
        ADD CONSTRAINT fk_trade_signals_underlying
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;


-- ---------------------------------------------------------------------------
-- signal_accuracy
-- One row per (underlying, trade_date, timeframe, strength_bucket).
-- Updated nightly (or on demand) by comparing past signals to actual outcomes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_accuracy (
    underlying      VARCHAR(10)     NOT NULL,
    trade_date      DATE            NOT NULL,
    timeframe       VARCHAR(20)     NOT NULL CHECK (timeframe IN ('intraday', 'swing', 'multi_day')),
    strength_bucket VARCHAR(10)     NOT NULL CHECK (strength_bucket IN ('high', 'medium', 'low')),

    total_signals   INTEGER         NOT NULL DEFAULT 0,
    correct_signals INTEGER         NOT NULL DEFAULT 0,
    win_pct         NUMERIC(6, 4),   -- NULL until at least 1 signal resolved

    updated_at      TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (underlying, trade_date, timeframe, strength_bucket)
);

CREATE INDEX IF NOT EXISTS idx_signal_accuracy_underlying_date
    ON signal_accuracy(underlying, trade_date DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_signal_accuracy_underlying') THEN
        ALTER TABLE signal_accuracy
        ADD CONSTRAINT fk_signal_accuracy_underlying
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- position_optimizer_signals
-- Written by PositionOptimizerEngine every ~5 min.
-- One row per (underlying, timestamp).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS position_optimizer_signals (
    underlying                  VARCHAR(10)   NOT NULL,
    timestamp                   TIMESTAMPTZ   NOT NULL,
    signal_timestamp            TIMESTAMPTZ   NOT NULL,
    signal_timeframe            VARCHAR(20)   NOT NULL CHECK (signal_timeframe IN ('intraday', 'swing', 'multi_day')),
    signal_direction            VARCHAR(10)   NOT NULL CHECK (signal_direction IN ('bullish', 'bearish', 'neutral')),
    signal_strength             VARCHAR(10)   NOT NULL CHECK (signal_strength IN ('high', 'medium', 'low')),
    trade_type                  VARCHAR(40)   NOT NULL,
    current_price               NUMERIC(12, 4),
    composite_score             NUMERIC(10, 2) NOT NULL,
    max_possible_score          INTEGER       NOT NULL,
    normalized_score            NUMERIC(6, 4) NOT NULL,
    top_strategy_type           VARCHAR(40)   NOT NULL,
    top_expiry                  DATE          NOT NULL,
    top_dte                     INTEGER       NOT NULL,
    top_strikes                 VARCHAR(120)  NOT NULL,
    top_probability_of_profit   NUMERIC(6, 4) NOT NULL,
    top_expected_value          NUMERIC(12, 2) NOT NULL,
    top_max_profit              NUMERIC(12, 2) NOT NULL,
    top_max_loss                NUMERIC(12, 2) NOT NULL,
    top_kelly_fraction          NUMERIC(6, 4) NOT NULL,
    top_sharpe_like_ratio       NUMERIC(8, 4),
    top_liquidity_score         NUMERIC(6, 4),
    top_market_structure_fit    NUMERIC(6, 4),
    top_reasoning               JSONB,
    candidates                  JSONB,
    updated_at                  TIMESTAMPTZ   DEFAULT NOW(),
    created_at                  TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (underlying, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_position_optimizer_signals_underlying_ts
    ON position_optimizer_signals(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_position_optimizer_signals_signal_ts
    ON position_optimizer_signals(signal_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_position_optimizer_signals_strategy
    ON position_optimizer_signals(top_strategy_type, timestamp DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_position_optimizer_underlying') THEN
        ALTER TABLE position_optimizer_signals
        ADD CONSTRAINT fk_position_optimizer_underlying
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- position_optimizer_accuracy
-- One row per (underlying, trade_date, signal_direction, strategy_type).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS position_optimizer_accuracy (
    underlying                VARCHAR(10)   NOT NULL,
    trade_date                DATE          NOT NULL,
    signal_direction          VARCHAR(10)   NOT NULL CHECK (signal_direction IN ('bullish', 'bearish', 'neutral')),
    strategy_type             VARCHAR(40)   NOT NULL,
    total_signals             INTEGER       NOT NULL DEFAULT 0,
    profitable_signals        INTEGER       NOT NULL DEFAULT 0,
    avg_realized_return_pct   NUMERIC(10, 4),
    avg_expected_value        NUMERIC(12, 4),
    avg_predicted_pop         NUMERIC(6, 4),
    avg_realized_move_pct     NUMERIC(8, 4),
    updated_at                TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (underlying, trade_date, signal_direction, strategy_type)
);

CREATE INDEX IF NOT EXISTS idx_position_optimizer_accuracy_underlying_date
    ON position_optimizer_accuracy(underlying, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_position_optimizer_accuracy_strategy
    ON position_optimizer_accuracy(strategy_type, trade_date DESC);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_position_optimizer_accuracy_underlying') THEN
        ALTER TABLE position_optimizer_accuracy
        ADD CONSTRAINT fk_position_optimizer_accuracy_underlying
        FOREIGN KEY (underlying) REFERENCES symbols(symbol) ON DELETE CASCADE;
    END IF;
END $$;

-- signal_engine_trade_ideas
-- Proprietary trade lifecycle records managed by standalone Signal Engine service.

CREATE TABLE IF NOT EXISTS signal_engine_trade_ideas (
    id                  BIGSERIAL PRIMARY KEY,
    underlying          VARCHAR(20)   NOT NULL,
    signal_timestamp    TIMESTAMPTZ   NOT NULL,
    timestamp           TIMESTAMPTZ   NOT NULL,
    status              VARCHAR(40)   NOT NULL CHECK (
        status IN (
            'ready_to_trigger',
            'position_open',
            'partial_take_profit',
            'stopped_out',
            'target_fully_hit',
            'closed'
        )
    ),
    signal_timeframe    VARCHAR(20)   NOT NULL CHECK (signal_timeframe IN ('intraday', 'swing', 'multi_day')),
    signal_direction    VARCHAR(10)   NOT NULL CHECK (signal_direction IN ('bullish', 'bearish', 'neutral')),
    strategy_type       VARCHAR(40)   NOT NULL,
    expiry              DATE          NOT NULL,
    strikes             VARCHAR(120)  NOT NULL,
    contracts           INTEGER       NOT NULL,
    entry_price         NUMERIC(18, 6) NOT NULL,
    current_mark        NUMERIC(18, 6) NOT NULL,
    stop_price          NUMERIC(18, 6) NOT NULL,
    target_1            NUMERIC(18, 6) NOT NULL,
    target_2            NUMERIC(18, 6) NOT NULL,
    realized_pnl        NUMERIC(18, 6) NOT NULL DEFAULT 0,
    unrealized_pnl      NUMERIC(18, 6) NOT NULL DEFAULT 0,
    total_pnl           NUMERIC(18, 6) NOT NULL DEFAULT 0,
    trade_cost          NUMERIC(18, 6) NOT NULL,
    time_opened         TIMESTAMPTZ   NOT NULL,
    time_closed         TIMESTAMPTZ,
    notes               TEXT          NOT NULL DEFAULT '',
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='signal_engine_trade_ideas' AND column_name='time_opened'
    ) THEN
        ALTER TABLE signal_engine_trade_ideas ADD COLUMN time_opened TIMESTAMPTZ;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='signal_engine_trade_ideas' AND column_name='time_closed'
    ) THEN
        ALTER TABLE signal_engine_trade_ideas ADD COLUMN time_closed TIMESTAMPTZ;
    END IF;

    UPDATE signal_engine_trade_ideas
    SET time_opened = timestamp
    WHERE time_opened IS NULL;

    ALTER TABLE signal_engine_trade_ideas
    ALTER COLUMN time_opened SET NOT NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_signal_engine_trade_ideas_underlying_ts
    ON signal_engine_trade_ideas(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_signal_engine_trade_ideas_status
    ON signal_engine_trade_ideas(status, timestamp DESC);

-- consolidated_trade_signals
-- Single unified signal stream combining trade, volatility expansion, and optimizer logic.

CREATE TABLE IF NOT EXISTS consolidated_trade_signals (
    underlying              VARCHAR(20)   NOT NULL,
    timestamp               TIMESTAMPTZ   NOT NULL,
    timeframe               VARCHAR(20)   NOT NULL CHECK (timeframe IN ('intraday', 'swing', 'multi_day')),
    composite_score         NUMERIC(12, 4) NOT NULL,
    normalized_score        NUMERIC(8, 4)  NOT NULL,
    direction               VARCHAR(10)   NOT NULL CHECK (direction IN ('bullish', 'bearish', 'neutral')),
    strength                VARCHAR(10)   NOT NULL CHECK (strength IN ('high', 'medium', 'low')),
    estimated_win_pct       NUMERIC(6, 4),
    trade_type              VARCHAR(40)   NOT NULL,
    trade_rationale         TEXT          NOT NULL,
    target_expiry           VARCHAR(20),
    suggested_strikes       VARCHAR(120),
    current_price           NUMERIC(18, 6) NOT NULL,
    net_gex                 NUMERIC(18, 2),
    gamma_flip              NUMERIC(18, 6),
    put_call_ratio          NUMERIC(12, 6),
    dealer_net_delta        NUMERIC(18, 2),
    vwap_deviation_pct      NUMERIC(10, 4),
    move_probability        NUMERIC(8, 4),
    expected_magnitude_pct  NUMERIC(8, 4),
    top_strategy_type       VARCHAR(40),
    top_candidate           JSONB         NOT NULL DEFAULT '{}'::jsonb,
    components              JSONB         NOT NULL DEFAULT '{}'::jsonb,
    updated_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (underlying, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_consolidated_trade_signals_underlying_ts
    ON consolidated_trade_signals(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_consolidated_trade_signals_tf
    ON consolidated_trade_signals(timeframe, timestamp DESC);

CREATE TABLE IF NOT EXISTS consolidated_signal_accuracy (
    underlying       VARCHAR(20)    NOT NULL,
    trade_date       DATE           NOT NULL,
    timeframe        VARCHAR(20)    NOT NULL CHECK (timeframe IN ('intraday', 'swing', 'multi_day')),
    strength_bucket  VARCHAR(10)    NOT NULL CHECK (strength_bucket IN ('high', 'medium', 'low')),
    total_signals    INTEGER        NOT NULL DEFAULT 0,
    correct_signals  INTEGER        NOT NULL DEFAULT 0,
    win_pct          NUMERIC(6, 4),
    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (underlying, trade_date, timeframe, strength_bucket)
);

CREATE INDEX IF NOT EXISTS idx_consolidated_signal_accuracy_underlying_date
    ON consolidated_signal_accuracy(underlying, trade_date DESC);

CREATE TABLE IF NOT EXISTS consolidated_position_accuracy (
    underlying                VARCHAR(20)   NOT NULL,
    trade_date                DATE          NOT NULL,
    signal_direction          VARCHAR(10)   NOT NULL CHECK (signal_direction IN ('bullish', 'bearish', 'neutral')),
    strategy_type             VARCHAR(40)   NOT NULL,
    total_signals             INTEGER       NOT NULL DEFAULT 0,
    profitable_signals        INTEGER       NOT NULL DEFAULT 0,
    avg_realized_return_pct   NUMERIC(8, 4),
    avg_expected_value        NUMERIC(12, 4),
    avg_predicted_pop         NUMERIC(8, 4),
    avg_realized_move_pct     NUMERIC(8, 4),
    updated_at                TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    created_at                TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (underlying, trade_date, signal_direction, strategy_type)
);

CREATE INDEX IF NOT EXISTS idx_consolidated_position_accuracy_underlying_date
    ON consolidated_position_accuracy(underlying, trade_date DESC);

-- =============================================================================
-- Unified signal engine (v2)
-- =============================================================================

-- Fresh start for legacy signaling objects.
DROP TABLE IF EXISTS trade_signals CASCADE;
DROP TABLE IF EXISTS signal_accuracy CASCADE;
DROP TABLE IF EXISTS position_optimizer_signals CASCADE;
DROP TABLE IF EXISTS position_optimizer_accuracy CASCADE;
DROP TABLE IF EXISTS signal_engine_trade_ideas CASCADE;
DROP TABLE IF EXISTS consolidated_trade_signals CASCADE;
DROP TABLE IF EXISTS consolidated_signal_accuracy CASCADE;
DROP TABLE IF EXISTS consolidated_position_accuracy CASCADE;

-- Authoritative definition. Column migrations below if needed.
CREATE TABLE IF NOT EXISTS signal_scores (
    underlying VARCHAR(10) NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    composite_score DOUBLE PRECISION NOT NULL,
    normalized_score DOUBLE PRECISION NOT NULL,
    direction VARCHAR(25) NOT NULL CHECK (direction IN ('trend_expansion', 'controlled_trend', 'chop_range', 'high_risk_reversal')),
    components JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (underlying, timestamp)
);

-- Migration: widen direction column and replace CHECK constraint to match MSI regime labels.
DO $$
BEGIN
    -- Drop old constraint (may contain stale bullish/bearish/neutral values).
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'signal_scores_direction_check'
          AND conrelid = 'signal_scores'::regclass
    ) THEN
        ALTER TABLE signal_scores DROP CONSTRAINT signal_scores_direction_check;
    END IF;

    -- Widen to VARCHAR(25) if currently narrower (covers any partially-migrated state).
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'signal_scores'
          AND column_name = 'direction'
          AND character_maximum_length IS NOT NULL
          AND character_maximum_length < 25
    ) THEN
        ALTER TABLE signal_scores ALTER COLUMN direction TYPE VARCHAR(25);
    END IF;

    -- Remap any rows written with the old bullish/bearish/neutral labels.
    UPDATE signal_scores
    SET direction = CASE direction
        WHEN 'bullish'  THEN 'trend_expansion'
        WHEN 'bearish'  THEN 'high_risk_reversal'
        WHEN 'neutral'  THEN 'chop_range'
        ELSE 'chop_range'
    END
    WHERE direction NOT IN ('trend_expansion', 'controlled_trend', 'chop_range', 'high_risk_reversal');

    -- Add updated constraint.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'signal_scores_direction_check'
          AND conrelid = 'signal_scores'::regclass
    ) THEN
        ALTER TABLE signal_scores
            ADD CONSTRAINT signal_scores_direction_check
            CHECK (direction IN ('trend_expansion', 'controlled_trend', 'chop_range', 'high_risk_reversal'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_signal_scores_underlying_ts
    ON signal_scores(underlying, timestamp DESC);

-- Authoritative definition. Column migrations below if needed.
CREATE TABLE IF NOT EXISTS signal_trades (
    id BIGSERIAL PRIMARY KEY,
    underlying VARCHAR(10) NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    signal_timestamp TIMESTAMPTZ NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    status VARCHAR(12) NOT NULL CHECK (status IN ('open', 'closed')),
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('bullish', 'bearish')),
    score_at_entry DOUBLE PRECISION NOT NULL,
    score_latest DOUBLE PRECISION,
    option_symbol VARCHAR(50) NOT NULL,
    option_type CHAR(1) NOT NULL CHECK (option_type IN ('C','P')),
    expiration DATE NOT NULL,
    strike NUMERIC(12,4) NOT NULL,
    entry_price NUMERIC(12,6) NOT NULL,
    current_price NUMERIC(12,6) NOT NULL,
    quantity_initial INTEGER NOT NULL,
    quantity_open INTEGER NOT NULL,
    realized_pnl NUMERIC(14,4) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(14,4) NOT NULL DEFAULT 0,
    total_pnl NUMERIC(14,4) NOT NULL DEFAULT 0,
    pnl_percent NUMERIC(12,4) NOT NULL DEFAULT 0,
    components_at_entry JSONB NOT NULL DEFAULT '{}'::jsonb,
    components_latest JSONB
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'signal_trades_status_check'
          AND conrelid = 'signal_trades'::regclass
    ) THEN
        ALTER TABLE signal_trades
            ADD CONSTRAINT signal_trades_status_check
            CHECK (status IN ('open', 'closed'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'signal_trades_direction_check'
          AND conrelid = 'signal_trades'::regclass
    ) THEN
        ALTER TABLE signal_trades
            ADD CONSTRAINT signal_trades_direction_check
            CHECK (direction IN ('bullish', 'bearish'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'signal_trades_option_type_check'
          AND conrelid = 'signal_trades'::regclass
    ) THEN
        ALTER TABLE signal_trades
            ADD CONSTRAINT signal_trades_option_type_check
            CHECK (option_type IN ('C','P'));
    END IF;

    -- Drop legacy unique constraint: trades are now independent (never add to existing).
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_signal_trades_unique_signal'
          AND conrelid = 'signal_trades'::regclass
    ) THEN
        ALTER TABLE signal_trades
            DROP CONSTRAINT uq_signal_trades_unique_signal;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_signal_trades_underlying_open
    ON signal_trades(underlying, status, opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_trades_underlying_closed
    ON signal_trades(underlying, closed_at DESC)
    WHERE status = 'closed';

DROP TRIGGER IF EXISTS update_signal_scores_updated_at ON signal_scores;
CREATE TRIGGER update_signal_scores_updated_at
    BEFORE UPDATE ON signal_scores
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_signal_trades_updated_at ON signal_trades;
CREATE TRIGGER update_signal_trades_updated_at
    BEFORE UPDATE ON signal_trades
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Closed trades are immutable.
CREATE OR REPLACE FUNCTION prevent_closed_signal_trade_updates()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status = 'closed' THEN
        RAISE EXCEPTION 'signal_trades row % is immutable after close', OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS lock_closed_signal_trade ON signal_trades;
CREATE TRIGGER lock_closed_signal_trade
    BEFORE UPDATE ON signal_trades
    FOR EACH ROW
    WHEN (OLD.status = 'closed')
    EXECUTE FUNCTION prevent_closed_signal_trade_updates();

-- ---------------------------------------------------------------------------
-- signal_component_scores
-- Stores each scoring component's individual score every cycle.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_component_scores (
    underlying      VARCHAR(10)   NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    timestamp       TIMESTAMPTZ   NOT NULL,
    component_name  VARCHAR(50)   NOT NULL,
    -- Component output after clamping to [-1, +1].  Named `clamped_score`
    -- rather than `raw_score` because the scoring engine clamps before
    -- persisting; the pre-clamp "raw" value is never stored.
    clamped_score   DOUBLE PRECISION NOT NULL,
    weighted_score  DOUBLE PRECISION NOT NULL,  -- clamped_score * weight
    weight          DOUBLE PRECISION NOT NULL,
    context_values  JSONB         NOT NULL DEFAULT '{}'::jsonb,  -- inputs used
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (underlying, timestamp, component_name)
);

-- Migration helper: existing deployments still have the old `raw_score`
-- column.  Rename in place if we detect it.  Safe to re-run.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_component_scores'
          AND column_name = 'raw_score'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_component_scores'
          AND column_name = 'clamped_score'
    ) THEN
        ALTER TABLE signal_component_scores RENAME COLUMN raw_score TO clamped_score;
    END IF;
END
$$;

-- Migration helper: trap_detection used to label its level fields
-- `resistance_level` / `support_level`, but the values it stores are
-- the *recently broken* levels (broken resistance sits below close,
-- broken support sits above).  Rename the JSON keys to make the
-- post-breakout convention explicit.  Idempotent: skips rows already
-- carrying the new keys.
UPDATE signal_component_scores
SET context_values = jsonb_set(
        context_values - 'resistance_level',
        '{broken_resistance_level}',
        context_values -> 'resistance_level',
        true
    )
WHERE component_name = 'trap_detection'
  AND context_values ? 'resistance_level';

UPDATE signal_component_scores
SET context_values = jsonb_set(
        context_values - 'support_level',
        '{broken_support_level}',
        context_values -> 'support_level',
        true
    )
WHERE component_name = 'trap_detection'
  AND context_values ? 'support_level';

CREATE INDEX IF NOT EXISTS idx_signal_component_scores_underlying_ts
    ON signal_component_scores(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_signal_component_scores_component
    ON signal_component_scores(component_name, timestamp DESC);

-- ---------------------------------------------------------------------------
-- signal_action_cards
-- Persists Action Cards emitted by the Playbook Engine.  One row per
-- non-STAND_DOWN Card.  Used for hysteresis (suppress re-emission of the
-- same pattern within its dwell window) and audit / backtesting.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_action_cards (
    id                BIGSERIAL PRIMARY KEY,
    underlying        VARCHAR(10) NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    timestamp         TIMESTAMPTZ NOT NULL,
    pattern           VARCHAR(64) NOT NULL,
    action            VARCHAR(32) NOT NULL,
    tier              VARCHAR(8)  NOT NULL,
    direction         VARCHAR(20) NOT NULL,
    confidence        DOUBLE PRECISION NOT NULL,
    payload           JSONB       NOT NULL,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_action_cards_underlying_ts
    ON signal_action_cards(underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_signal_action_cards_underlying_pattern_ts
    ON signal_action_cards(underlying, pattern, timestamp DESC);

-- ---------------------------------------------------------------------------
-- playbook_pattern_stats
-- Read-only output of the Playbook backtest harness (PR-14).  Each row
-- summarizes one (pattern, underlying, window) — hit rate, sample size,
-- MFE/MAE, and a proposed pattern_base derived from the empirical hit
-- rate.  Live patterns continue to use their hard-coded priors until a
-- follow-up PR explicitly promotes these numbers.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS playbook_pattern_stats (
    pattern           VARCHAR(64)   NOT NULL,
    underlying        VARCHAR(10)   NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    window_start      DATE          NOT NULL,
    window_end        DATE          NOT NULL,
    n_emitted         INTEGER       NOT NULL DEFAULT 0,
    n_resolved        INTEGER       NOT NULL DEFAULT 0,
    n_target_hit      INTEGER       NOT NULL DEFAULT 0,
    n_stop_hit        INTEGER       NOT NULL DEFAULT 0,
    n_time_exit       INTEGER       NOT NULL DEFAULT 0,
    hit_rate          DOUBLE PRECISION,
    avg_confidence    DOUBLE PRECISION,
    avg_mfe_pct       DOUBLE PRECISION,
    avg_mae_pct       DOUBLE PRECISION,
    proposed_base     DOUBLE PRECISION,
    computed_at       TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (pattern, underlying, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_playbook_pattern_stats_underlying_window
    ON playbook_pattern_stats(underlying, window_end DESC);

-- ---------------------------------------------------------------------------
-- portfolio_snapshots (schema only — used in Part 2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    underlying          VARCHAR(10)   NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    timestamp           TIMESTAMPTZ   NOT NULL,
    composite_score     DOUBLE PRECISION NOT NULL,
    normalized_score    DOUBLE PRECISION NOT NULL,
    direction           VARCHAR(25)   NOT NULL,
    target_contracts    INTEGER       NOT NULL DEFAULT 0,
    target_direction    VARCHAR(25)   NOT NULL DEFAULT 'neutral',
    target_strategy     VARCHAR(40),
    actual_contracts    INTEGER       NOT NULL DEFAULT 0,
    actual_direction    VARCHAR(25)   NOT NULL DEFAULT 'neutral',
    heat_pct            DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    action_taken        VARCHAR(40),   -- 'opened', 'closed', 'resized', 'held', 'cash'
    action_detail       JSONB         NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (underlying, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_underlying_ts
    ON portfolio_snapshots(underlying, timestamp DESC);

-- Migration: widen direction columns to hold MSI regime labels (up to 18 chars).
DO $$
DECLARE
    col TEXT;
BEGIN
    FOREACH col IN ARRAY ARRAY['direction', 'target_direction', 'actual_direction'] LOOP
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'portfolio_snapshots'
              AND column_name = col
              AND character_maximum_length IS NOT NULL
              AND character_maximum_length < 25
        ) THEN
            EXECUTE format('ALTER TABLE portfolio_snapshots ALTER COLUMN %I TYPE VARCHAR(25)', col);
        END IF;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- signal_calibration
-- Walk-forward component weight & normalization calibration storage.
-- Rows here are optional — the scoring engine will fall back to env/defaults
-- when no calibration is present. All additions are idempotent so
-- redeploys are safe.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_calibration (
    underlying      VARCHAR(10)   NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    component_name  VARCHAR(50)   NOT NULL,
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    weight          DOUBLE PRECISION,
    norm_constant   DOUBLE PRECISION,
    hit_rate        DOUBLE PRECISION,
    sharpe          DOUBLE PRECISION,
    sample_size     INTEGER,
    metadata        JSONB         NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (underlying, component_name, window_start)
);

CREATE INDEX IF NOT EXISTS idx_signal_calibration_underlying_component
    ON signal_calibration(underlying, component_name, window_end DESC);

DROP TRIGGER IF EXISTS update_signal_calibration_updated_at ON signal_calibration;
CREATE TRIGGER update_signal_calibration_updated_at
    BEFORE UPDATE ON signal_calibration
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Supplemental index: speeds up per-component lookback queries used by the
-- new gex_gradient, dealer_delta_pressure, and vanna_charm_flow components.
CREATE INDEX IF NOT EXISTS idx_signal_component_scores_component_underlying_ts
    ON signal_component_scores(component_name, underlying, timestamp DESC);

-- ---------------------------------------------------------------------------
-- signal_events
-- Discrete edge-triggered events emitted when an independent signal's
-- ``triggered`` flag transitions false -> true (hysteresis-protected).
-- Consumers poll this table (or subscribe via Postgres LISTEN/NOTIFY or a
-- Redis mirror) instead of scraping the latest row from signal_component_scores.
-- Each event also receives a placeholder PNL column that a follow-up job
-- backfills at +30min / +60min / +120min horizons for hit-rate computation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_events (
    id              BIGSERIAL PRIMARY KEY,
    underlying      VARCHAR(10)   NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    timestamp       TIMESTAMPTZ   NOT NULL,
    signal_name     VARCHAR(64)   NOT NULL,
    direction       VARCHAR(16)   NOT NULL,  -- bullish / bearish / neutral / <signal_value>
    score           DOUBLE PRECISION NOT NULL,
    context_values  JSONB         NOT NULL DEFAULT '{}'::jsonb,
    close_at_emit   DOUBLE PRECISION,
    -- Forward-looking diagnostics populated by an offline job.
    close_30m       DOUBLE PRECISION,
    close_60m       DOUBLE PRECISION,
    close_120m      DOUBLE PRECISION,
    outcome_30m     VARCHAR(8),   -- 'win' / 'loss' / NULL
    outcome_60m     VARCHAR(8),
    outcome_120m    VARCHAR(8),
    created_at      TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_events_name_ts
    ON signal_events(signal_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_signal_events_underlying_ts
    ON signal_events(underlying, timestamp DESC);

-- ---------------------------------------------------------------------------
-- component_normalizer_cache
-- Per-symbol, per-field rolling normalization constants.  The signal engine
-- refreshes the rows it consumes roughly every engine cycle (see
-- src/signals/unified_signal_engine.py).  Falls back to env-var defaults
-- on cold start or when a row is missing.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS component_normalizer_cache (
    underlying      VARCHAR(10)   NOT NULL REFERENCES symbols(symbol) ON DELETE CASCADE,
    field_name      VARCHAR(64)   NOT NULL,
    window_days     INTEGER       NOT NULL DEFAULT 20,
    p05             DOUBLE PRECISION,
    p50             DOUBLE PRECISION,
    p95             DOUBLE PRECISION,
    std             DOUBLE PRECISION,
    sample_size     INTEGER,
    updated_at      TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (underlying, field_name)
);

-- =============================================================================
-- Per-user API keys
-- =============================================================================
-- Long-lived API keys for individual users.  Keys are stored as SHA-256
-- hashes (the raw secret is shown to the operator exactly once at issuance
-- time); the short ``prefix`` is kept in clear so a key can be referenced in
-- logs and the admin CLI without exposing the secret.
--
-- ``user_id`` is a free-form identifier owned by the operator (email,
-- username, integration name, ...).  No FK; we deliberately don't model a
-- ``users`` table here — the keys table *is* the source of truth.
--
-- Provision keys with: ``python -m src.api.admin_keys create <user_id> --name <label>``
-- =============================================================================
CREATE TABLE IF NOT EXISTS api_keys (
    id              BIGSERIAL     PRIMARY KEY,
    user_id         VARCHAR(128)  NOT NULL,
    name            VARCHAR(128)  NOT NULL,
    key_hash        CHAR(64)      NOT NULL UNIQUE,
    prefix          VARCHAR(16)   NOT NULL,
    scopes          TEXT[]        NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_used_at    TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_active_hash
    ON api_keys(key_hash) WHERE revoked_at IS NULL;
