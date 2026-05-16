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

-- Backfill columns for option_chains tables that pre-date them.  Fresh
-- installs already get all columns from the CREATE TABLE above; this
-- block only runs ALTER on existing deployments.  ``ADD COLUMN IF NOT
-- EXISTS`` (PostgreSQL >= 9.6) collapses the previous nine separate
-- ``IF NOT EXISTS (SELECT ... information_schema.columns ...)`` DO
-- blocks into a single statement per column.
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS mid NUMERIC(12, 4);
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS ask_volume BIGINT DEFAULT 0;
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS mid_volume BIGINT DEFAULT 0;
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS bid_volume BIGINT DEFAULT 0;
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS implied_volatility NUMERIC(8, 6);
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS delta NUMERIC(8, 6);
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS gamma NUMERIC(10, 8);
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS theta NUMERIC(10, 6);
ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS vega NUMERIC(10, 6);

-- Column semantics (COMMENT ON is idempotent; safe to re-run).
-- ``volume`` is the vendor's SESSION-CUMULATIVE contract volume (resets
-- to 0 at the cash open and only ever increases intraday) — it is NOT a
-- per-minute figure. Period (per-bucket) volume must be derived from
-- ``flow_contract_facts`` (canonical: day-partitioned LAG deltas with
-- buy/sell classification), never by reading this column as if it were
-- the minute's traded volume. ``ask_volume`` / ``mid_volume`` /
-- ``bid_volume`` are the OPPOSITE: per-bucket classified deltas the
-- ingestion upsert ACCUMULATES additively, so they do not reconcile
-- against ``volume`` directly.
COMMENT ON COLUMN option_chains.volume IS
    'Session-cumulative raw contract volume (resets at cash open, monotonic intraday). NOT per-minute. Use flow_contract_facts for period volume.';
COMMENT ON COLUMN option_chains.ask_volume IS
    'Per-bucket Lee-Ready ask-side (buyer-initiated) classified volume; upsert-accumulated. Does not reconcile against the cumulative volume column.';
COMMENT ON COLUMN option_chains.mid_volume IS
    'Per-bucket mid/indeterminate classified volume; upsert-accumulated.';
COMMENT ON COLUMN option_chains.bid_volume IS
    'Per-bucket Lee-Ready bid-side (seller-initiated) classified volume; upsert-accumulated.';

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
-- Partial covering index for per-contract snapshot lookups (LATERAL
-- joins, src/api/database.py:_do_refresh_flow_cache).  NOT picked by
-- the planner for analytics _get_snapshot() despite being designed
-- for it -- bitmap-heap-scan wins at every lookback width.  Don't
-- drop without first migrating per-contract lookups to an alternate
-- plan.  Build in production via ``make db-add-distinct-on-index``
-- (CREATE INDEX CONCURRENTLY).  Full incident history in
-- docs/runbooks/option_chains_indexing.md.
CREATE INDEX IF NOT EXISTS idx_option_chains_underlying_option_symbol_ts_gamma_covering
    ON option_chains(underlying, option_symbol, timestamp DESC)
    INCLUDE (
        strike, option_type, expiration,
        last, bid, ask,
        volume, open_interest,
        delta, gamma, theta, vega, implied_volatility
    )
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

-- Backfill columns for gex_summary tables that pre-date them.  The
-- canonical schema is the CREATE TABLE above; these statements only
-- run ALTER on existing deployments.  ``call_wall`` / ``put_wall`` are
-- populated by the Analytics Engine on each cycle (see
-- src/analytics/walls.py).  ``max_pain_by_expiration`` carries the
-- per-expiration breakdown that pairs with the scalar ``max_pain``
-- column (front-month value); see
-- src/analytics/main_engine.py:_calculate_max_pain.
ALTER TABLE gex_summary ADD COLUMN IF NOT EXISTS flip_distance DOUBLE PRECISION;
ALTER TABLE gex_summary ADD COLUMN IF NOT EXISTS local_gex DOUBLE PRECISION;
ALTER TABLE gex_summary ADD COLUMN IF NOT EXISTS convexity_risk DOUBLE PRECISION;
ALTER TABLE gex_summary ADD COLUMN IF NOT EXISTS call_wall NUMERIC(12, 4);
ALTER TABLE gex_summary ADD COLUMN IF NOT EXISTS put_wall NUMERIC(12, 4);
ALTER TABLE gex_summary ADD COLUMN IF NOT EXISTS max_pain_by_expiration JSONB;

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

-- Backfill split-by-type and dealer-sign columns on existing
-- gex_by_strike deployments.  Dealers are net short the book, so
-- dealer_charm_exposure / dealer_vanna_exposure are the negative of
-- the market-aggregate charm_exposure / vanna_exposure columns.
-- Downstream signals that want tradable "dealer hedging flow" should
-- read the dealer_* columns.
ALTER TABLE gex_by_strike ADD COLUMN IF NOT EXISTS call_charm_exposure DOUBLE PRECISION;
ALTER TABLE gex_by_strike ADD COLUMN IF NOT EXISTS put_charm_exposure DOUBLE PRECISION;
ALTER TABLE gex_by_strike ADD COLUMN IF NOT EXISTS call_vanna_exposure DOUBLE PRECISION;
ALTER TABLE gex_by_strike ADD COLUMN IF NOT EXISTS put_vanna_exposure DOUBLE PRECISION;
ALTER TABLE gex_by_strike ADD COLUMN IF NOT EXISTS dealer_charm_exposure DOUBLE PRECISION;
ALTER TABLE gex_by_strike ADD COLUMN IF NOT EXISTS dealer_vanna_exposure DOUBLE PRECISION;
ALTER TABLE gex_by_strike ADD COLUMN IF NOT EXISTS expiration_bucket VARCHAR(16);

-- Vanna/charm exposure unit bases (COMMENT ON is idempotent). Each is
-- the per-unit-perturbation dollar analog of GEX (γ·OI·100·S²·0.01,
-- "$ per 1% spot move"): vanna is "$ delta-hedge notional per 1
-- volatility point" (∂Δ/∂σ·OI·100·S·0.01 — one S only, since the vol
-- bump is an absolute 0.01, not ∝S), charm is "$ delta-hedge notional
-- drift per day" (∂Δ/∂t_perday·OI·100·S). They are DIFFERENT axes
-- (vol vs time) and must NOT be summed as raw dollars — consumers
-- normalize each field independently (see vanna_charm_flow).
COMMENT ON COLUMN gex_by_strike.vanna_exposure IS
    'Market-aggregate vanna: $ dealer delta-hedge notional change per 1 volatility point (vanna*OI*100*S*0.01). Different axis from charm; normalize per-field.';
COMMENT ON COLUMN gex_by_strike.charm_exposure IS
    'Market-aggregate charm: $ dealer delta-hedge notional drift per DAY (charm_per_day*OI*100*S). Different axis from vanna; normalize per-field.';
COMMENT ON COLUMN gex_by_strike.dealer_vanna_exposure IS
    'Dealer-sign vanna ($/vol-point), = -vanna_exposure. + => dealers buy underlying as IV rises.';
COMMENT ON COLUMN gex_by_strike.dealer_charm_exposure IS
    'Dealer-sign charm ($/day), = -charm_exposure. + => dealers buy underlying as time passes.';

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
-- ``option_chains_with_deltas`` is retained as a backward-compat view for
-- ad-hoc queries / dashboards.  ``volume_delta`` here is recomputed on
-- read via LAG() and is not the canonical flow value -- the engine writes
-- the buy/sell-classified flow facts to ``flow_contract_facts`` (defined
-- below).  New queries should read flow numbers from ``flow_contract_facts``
-- and only fall back to this view for raw OI delta or per-row enrichment
-- not stored elsewhere.
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
-- DECOMMISSIONED: idx_flow_by_contract_symbol_ts_series_covering.
--
-- This covering index was the tactical fix for the unfiltered
-- /api/flow/series heap scan (carried the `filtered` CTE columns so the
-- planner could do an index-only scan instead of a ~3,000-page
-- bitmap-heap-scan). It is superseded by the flow_series_5min snapshot
-- below: unfiltered reads now bypass flow_by_contract entirely, and
-- strike/expiration-filtered reads are served by flow_by_contract_pkey
-- + idx_flow_by_contract_symbol_ts{,_exp,_type}.
--
-- Intentionally NOT (re)created: a fresh database never builds it. On an
-- existing database the ~200 MB index is physically removed, once and
-- after the production verification gate, via:
--     make flow-series-drop-covering-index CONFIRM=yes
-- (DROP INDEX CONCURRENTLY — see the Makefile target for the gate
-- checklist). Keeping schema.sql free of the CREATE is what stops
-- `make schema-apply` from resurrecting it on the next deploy.
CREATE INDEX IF NOT EXISTS idx_flow_smart_money_symbol_ts
    ON flow_smart_money(symbol, timestamp DESC);

-- Pre-aggregated /api/flow/series snapshot. One row per (symbol,
-- bar_start) 5-minute bucket holding exactly what the get_flow_series
-- outer SELECT produces. The Analytics Engine UPSERTs the current
-- session's rows every cycle (mirrors how gex_summary is written) and
-- src/tools/flow_series_5min_backfill.py bootstraps history; the
-- unfiltered /api/flow/series read then SELECTs straight from here
-- instead of running the 8-CTE pipeline. Column types match the CTE's
-- emitted types so asyncpg decodes them identically (NUMERIC -> Decimal,
-- BIGINT -> int, float8 -> float). underlying_price mirrors
-- underlying_quotes.close (NUMERIC(12,4)); it is NULL-able because the
-- CTE's carry-forward yields NULL for bars before the first quote of a
-- session. is_synthetic flags carry-forward (no-flow) bars.
CREATE TABLE IF NOT EXISTS flow_series_5min (
    symbol            VARCHAR(10)  NOT NULL,
    bar_start         TIMESTAMPTZ  NOT NULL,
    call_premium_cum  NUMERIC,
    put_premium_cum   NUMERIC,
    call_volume_cum   BIGINT,
    put_volume_cum    BIGINT,
    net_volume_cum    BIGINT,
    raw_volume_cum    BIGINT,
    call_position_cum BIGINT,
    put_position_cum  BIGINT,
    net_premium_cum   NUMERIC,
    put_call_ratio    DOUBLE PRECISION,
    underlying_price  NUMERIC(12, 4),
    contract_count    INTEGER,
    is_synthetic      BOOLEAN,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, bar_start)
);
CREATE INDEX IF NOT EXISTS idx_flow_series_5min_symbol_bar
    ON flow_series_5min(symbol, bar_start DESC);

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
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_flow_series_5min_symbol') THEN
        ALTER TABLE flow_series_5min
        ADD CONSTRAINT fk_flow_series_5min_symbol
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

-- Rebuild the technicals/flow views atomically.  Each view is a
-- DROP + CREATE pair; without a surrounding transaction there is a
-- sub-second window during `make schema-apply` where the view does
-- not exist, and any concurrent reader (signals/API service mid-cycle)
-- gets `relation "..." does not exist`.  PostgreSQL has transactional
-- DDL, so wrapping the whole block makes the swap atomic: readers see
-- either the old or the new view, never a missing one.  ON_ERROR_STOP
-- semantics are preserved -- an error inside the block aborts the
-- transaction and psql still exits non-zero.
BEGIN;

DROP VIEW IF EXISTS underlying_daily_volume CASCADE;
CREATE VIEW underlying_daily_volume AS
SELECT
    symbol,
    DATE(timestamp AT TIME ZONE 'America/New_York') AS trade_date_et,
    SUM(COALESCE(up_volume, 0) + COALESCE(down_volume, 0))::bigint AS cumulative_daily_volume
FROM underlying_quotes
GROUP BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York');

DROP VIEW IF EXISTS underlying_buying_pressure CASCADE;
-- Tape-classified uptick/downtick volume from TradeStation's bar
-- stream (Lee-Ready-style classification on consolidated NBBO).
-- This is a TICK-TEST CLASSIFICATION, not actual trade-side
-- attribution: a 1k-share print between exchanges with NBBO churn
-- can land on either side depending on the order of bookkeeping
-- events.  Column and label naming reflects what's actually
-- measured -- the previous "Buying" / "Selling" labels overstated
-- the precision of the underlying TS feed.
CREATE VIEW underlying_buying_pressure AS
SELECT
    q.timestamp AT TIME ZONE 'America/New_York' AS time_et,
    q.timestamp,
    q.symbol,
    q.close AS price,
    (q.up_volume - q.down_volume)::bigint AS uptick_minus_downtick_vol,
    ROUND(COALESCE((q.up_volume::numeric / NULLIF((q.up_volume + q.down_volume), 0)) * 100, 50), 2) AS uptick_vol_pct,
    CASE
        WHEN (q.up_volume - q.down_volume) >= 50000 THEN '🟢 Strong Uptick Bias'
        WHEN (q.up_volume - q.down_volume) > 0 THEN '✅ Uptick Bias'
        WHEN (q.up_volume - q.down_volume) <= -50000 THEN '❌ Downtick Bias'
        ELSE '⚪ Neutral'
    END AS tick_bias,
    -- Backwards-compat aliases so existing Makefile / dashboard SQL
    -- keeps working.  New code should read the canonical names above.
    (q.up_volume - q.down_volume)::bigint AS vol,
    ROUND(COALESCE((q.up_volume::numeric / NULLIF((q.up_volume + q.down_volume), 0)) * 100, 50), 2) AS buy_pct,
    CASE
        WHEN (q.up_volume - q.down_volume) >= 50000 THEN '🟢 Strong Uptick Bias'
        WHEN (q.up_volume - q.down_volume) > 0 THEN '✅ Uptick Bias'
        WHEN (q.up_volume - q.down_volume) <= -50000 THEN '❌ Downtick Bias'
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
        -- No volume yet today (pre-open / halt): VWAP is undefined, so
        -- every price comparison below is NULL and would otherwise fall
        -- through to the ELSE and report a spurious bearish "Below VWAP".
        WHEN cum_vol IS NULL OR cum_vol = 0 THEN '⚪ No Volume'
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
        -- Rolling baseline must NOT span the trading-day boundary: the
        -- opening 30 minutes are structurally 5-20x midday volume, so a
        -- window that reaches back into the prior session's close/after-
        -- hours bars makes volume_sigma fire "Extreme Spike" on every
        -- routine open. Partition by ET trading day (same convention as
        -- the cumulative VWAP window in underlying_vwap_deviation).
        AVG(up_volume + down_volume) OVER (
            PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
            ORDER BY timestamp
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS avg_volume,
        STDDEV_SAMP(up_volume + down_volume) OVER (
            PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
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
        -- Partition the LAG by ET trading day so the first bar of a
        -- session yields NULL (no prior intraday bar) instead of
        -- "today's open minus the prior session's close" -- an overnight
        -- gap mislabeled as a per-minute price change.
        close - LAG(close) OVER (
            PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
            ORDER BY timestamp
        ) AS price_change
    FROM underlying_quotes
    ORDER BY symbol, timestamp DESC
),
latest_delta AS (
    -- ``expected_hedge_shares`` is the dealer's required delta-hedge
    -- position in shares: dealers are short the customer book, so dealer
    -- option delta = -SUM(customer_delta * OI * 100), and the share-hedge
    -- that flattens that delta is +SUM(customer_delta * OI * 100).
    -- A POSITIVE value => dealer is currently long shares as a hedge.
    -- A NEGATIVE value => dealer is currently short shares as a hedge.
    -- This is the static position level.  The dynamic hedge flow on a
    -- price move is governed by gamma, not delta -- see gex_summary.
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
),
notional_scale AS (
    -- Per-symbol "what counts as a meaningful share count" baseline:
    -- ``current_price * 100`` is the dollar value of one share-hedge
    -- contract.  Using 10000 contracts ($1M notional on SPY at $100,
    -- $5.5M on SPX at $5500) as the meaningful threshold makes the
    -- label calibration scale across underlyings instead of
    -- saturating SPX while undercounting SPY.
    SELECT symbol, current_price * 100 * 10000 AS hedge_notional_threshold
    FROM latest_price
)
SELECT
    p.timestamp AT TIME ZONE 'America/New_York' AS time_et,
    p.timestamp,
    p.symbol,
    p.current_price,
    p.price_change,
    COALESCE(d.expected_hedge_shares, 0) AS expected_hedge_shares,
    -- Honest labeling: this column reports the dealer's CURRENT static
    -- hedge position direction, not "risk of forced hedging" (that's a
    -- function of gamma, not delta).  Threshold is symbol-aware via
    -- notional_scale so SPX/SPY use comparable bars.
    CASE
        WHEN COALESCE(d.expected_hedge_shares, 0) > ns.hedge_notional_threshold / NULLIF(p.current_price, 0)
            THEN '🟢 Dealer Long Hedge'
        WHEN COALESCE(d.expected_hedge_shares, 0) < -ns.hedge_notional_threshold / NULLIF(p.current_price, 0)
            THEN '🔴 Dealer Short Hedge'
        ELSE '⚪ Dealer Balanced Hedge'
    END AS hedge_pressure
FROM latest_price p
LEFT JOIN latest_delta d ON d.symbol::text = p.symbol::text
LEFT JOIN notional_scale ns ON ns.symbol::text = p.symbol::text;

DROP VIEW IF EXISTS gamma_exposure_levels CASCADE;
-- Per-strike dealer GEX in industry-standard "dollar gamma per 1% move"
-- units: γ × OI × 100 × S² × 0.01.  This matches the canonical formula
-- used by the analytics engine (src/analytics/main_engine.py:439) and
-- src/analytics/walls.py.  The previous version of this view used the
-- per-share-equivalent ``γ × OI × 100`` form, which differs from the
-- canonical persisted ``gex_by_strike.net_gex`` by a factor of S²/100
-- (~2,000x on SPY at $450, ~300,000x on SPX at $5500), and combined
-- that with a hardcoded ±$1M threshold that was uncalibrated for either
-- form.  This rewrite makes the view consistent with the rest of the
-- codebase and uses a per-symbol threshold derived from spot.
CREATE VIEW gamma_exposure_levels AS
WITH latest_spot AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        close::numeric AS spot
    FROM underlying_quotes
    ORDER BY symbol, timestamp DESC
),
latest_options AS (
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
    o.underlying,
    o.strike,
    -- Canonical formula: γ × OI × 100 × S² × 0.01.  Calls contribute
    -- positively, puts contribute negatively (dealer convention: dealers
    -- are short calls, long puts).
    SUM(
        CASE
            WHEN o.option_type = 'C' THEN o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01
            ELSE -o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01
        END
    ) AS net_gex,
    SUM(ABS(o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01)) AS total_gex,
    SUM(o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01) FILTER (WHERE o.option_type = 'C') AS call_gex,
    SUM(o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01) FILTER (WHERE o.option_type = 'P') AS put_gex,
    COUNT(*) AS num_contracts,
    SUM(o.open_interest) AS total_oi,
    -- Per-symbol threshold: 0.1% of spot's notional × 1M-share scale.
    -- For SPY at $450, threshold is ~$2M GEX.  For SPX at $5500,
    -- threshold is ~$300M GEX.  Both correspond to "meaningful at this
    -- underlying's typical OI scale" rather than a single dollar number
    -- that saturates SPX and undercounts SPY.
    CASE
        WHEN SUM(
            CASE
                WHEN o.option_type = 'C' THEN o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01
                ELSE -o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01
            END
        ) > s.spot * s.spot * 0.1 THEN '🟢 Strong +GEX'
        WHEN SUM(
            CASE
                WHEN o.option_type = 'C' THEN o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01
                ELSE -o.gamma * o.open_interest::numeric * 100 * s.spot * s.spot * 0.01
            END
        ) < -(s.spot * s.spot * 0.1) THEN '🔴 Strong -GEX'
        ELSE '⚪ Neutral GEX'
    END AS gex_level
FROM latest_options o
JOIN latest_spot s ON s.symbol::text = o.underlying::text
GROUP BY o.underlying, o.strike, s.spot;

-- End of the atomic technicals/flow view rebuild (see BEGIN above).
COMMIT;

-- =============================================================================
-- Unified signal engine (v2)
-- =============================================================================

-- Clean up legacy v1 signaling tables on existing deployments.  Fresh
-- installs skip these (the tables never existed).  All production code
-- that wrote/read these was removed alongside this drop block; the
-- replacement is signal_scores / signal_trades / signal_component_scores
-- defined below.
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

-- Covering index for the confluence-matrix outer read and any other
-- "latest N signal_scores rows" query.  signal_scores rows carry a fat
-- ``components`` JSONB column and only fit ~4 tuples per heap page; with
-- ``composite_score`` carried in the INCLUDE list the planner can
-- satisfy LIMIT-N scans with an Index Only Scan that never touches the
-- JSONB heap pages.  Build with ``CREATE INDEX CONCURRENTLY`` in
-- production via ``make db-add-signal-scores-composite-index``.
CREATE INDEX IF NOT EXISTS idx_signal_scores_underlying_ts_composite_covering
    ON signal_scores(underlying, timestamp DESC)
    INCLUDE (composite_score);

-- Aggressive autovacuum settings on the two confluence-matrix-feeding
-- tables: both are appended every scoring cycle, so the visibility map
-- ages out of date quickly and Index Only Scans degrade into heap
-- fetches on the newest rows (exactly the rows we read).  Lowering the
-- vacuum/analyze scale_factors keeps the VM current enough for IOS to
-- skip the heap and the planner's stats fresh enough that LIMIT
-- estimates are accurate.  Applied here for fresh installs; existing
-- deployments should apply via ``make db-tune-signal-tables-autovacuum``.
ALTER TABLE signal_scores SET (
    autovacuum_vacuum_scale_factor   = 0.02,
    autovacuum_vacuum_threshold      = 500,
    autovacuum_analyze_scale_factor  = 0.02,
    autovacuum_analyze_threshold     = 500
);

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

-- Covering index for the confluence-matrix read path.  The /api/signals/
-- {basic,advanced}/confluence-matrix endpoints scan recent rows for a
-- specific underlying, filter to N component names, and only need
-- ``clamped_score``.  With ``clamped_score`` carried in the INCLUDE list
-- and the filter columns in the key, the planner can satisfy the read
-- with an Index Only Scan and zero heap fetches — the JSONB
-- ``context_values`` column on the heap rows makes those fetches
-- disproportionately expensive otherwise.
--
-- Build with ``CREATE INDEX CONCURRENTLY`` in production to avoid locking
-- the writers; this schema entry serves fresh installs and idempotent
-- retries.
CREATE INDEX IF NOT EXISTS idx_signal_component_scores_underlying_ts_comp_clamped_covering
    ON signal_component_scores(underlying, timestamp DESC, component_name)
    INCLUDE (clamped_score);

-- Match the autovacuum tuning on signal_scores — same write profile,
-- same need to keep the visibility map current for IOS.
ALTER TABLE signal_component_scores SET (
    autovacuum_vacuum_scale_factor   = 0.02,
    autovacuum_vacuum_threshold      = 1000,
    autovacuum_analyze_scale_factor  = 0.02,
    autovacuum_analyze_threshold     = 1000
);

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
