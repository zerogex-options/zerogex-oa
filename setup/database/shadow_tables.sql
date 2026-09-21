-- =============================================================================
-- Shadow tables for the market data vendor migration.
--
-- Used by ``python -m src.tools.feed_compare --persist`` (step 14 of
-- docs/compliance/market-data-remediation-runbook.md) to capture what each
-- feed reported at the same instant, so a divergence found weeks later can
-- still be explained from the record.
--
-- These are deliberately SEPARATE tables rather than a ``provider`` column
-- added to option_chains / underlying_quotes. The live tables are read by
-- the analytics engine, every API route and every signal; a candidate feed
-- under evaluation must have no path into any of that. A shared table with
-- a discriminator column is one forgotten WHERE clause away from serving a
-- subscriber a number computed from an unvetted vendor.
--
-- Apply with:
--     psql -d zerogex -f setup/database/shadow_tables.sql
-- Drop when the migration is done:
--     DROP TABLE option_chains_shadow, underlying_quotes_shadow, feed_comparisons;
-- =============================================================================

CREATE TABLE IF NOT EXISTS option_chains_shadow (
    provider        VARCHAR(32)   NOT NULL,
    option_symbol   VARCHAR(50)   NOT NULL,
    captured_at     TIMESTAMPTZ   NOT NULL,
    underlying      VARCHAR(10)   NOT NULL,
    strike          NUMERIC(12, 4) NOT NULL,
    expiration      DATE          NOT NULL,
    option_type     CHAR(1)       NOT NULL,
    -- Every price column is NULLABLE on purpose. A feed that reports no
    -- bid is a fact worth capturing; storing 0 would make an absent quote
    -- indistinguishable from a genuine zero and would quietly flatter a
    -- candidate's coverage numbers.
    bid             NUMERIC(12, 4),
    ask             NUMERIC(12, 4),
    last            NUMERIC(12, 4),
    mid             NUMERIC(12, 4),
    bid_size        BIGINT,
    ask_size        BIGINT,
    volume          BIGINT,
    open_interest   BIGINT,
    implied_volatility NUMERIC(8, 6),
    quote_timestamp TIMESTAMPTZ,
    PRIMARY KEY (provider, option_symbol, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_ocs_capture
    ON option_chains_shadow (underlying, captured_at DESC);

CREATE TABLE IF NOT EXISTS underlying_quotes_shadow (
    provider     VARCHAR(32)    NOT NULL,
    symbol       VARCHAR(10)    NOT NULL,
    captured_at  TIMESTAMPTZ    NOT NULL,
    bar_timestamp TIMESTAMPTZ,
    open         NUMERIC(12, 4),
    high         NUMERIC(12, 4),
    low          NUMERIC(12, 4),
    close        NUMERIC(12, 4),
    volume       BIGINT,
    -- NULL means "this feed cannot report a signed split", which is the
    -- normal case for every vendor except TradeStation. Distinct from 0,
    -- which means "no signed volume in this bar".
    up_volume    BIGINT,
    down_volume  BIGINT,
    PRIMARY KEY (provider, symbol, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_uqs_capture
    ON underlying_quotes_shadow (symbol, captured_at DESC);

-- One row per metric per comparison run, so the "differences are written
-- down and explained" bar the runbook sets can be met from SQL rather than
-- from scrollback.
CREATE TABLE IF NOT EXISTS feed_comparisons (
    id               BIGSERIAL PRIMARY KEY,
    captured_at      TIMESTAMPTZ NOT NULL,
    underlying       VARCHAR(10) NOT NULL,
    incumbent        VARCHAR(32) NOT NULL,
    candidate        VARCHAR(32) NOT NULL,
    metric           VARCHAR(32) NOT NULL,
    incumbent_value  DOUBLE PRECISION,
    candidate_value  DOUBLE PRECISION,
    pct_diff         DOUBLE PRECISION,
    within_tolerance BOOLEAN,
    note             TEXT,
    -- Coverage context. A metric can agree while the candidate quotes half
    -- the chain, which is a finding the metric alone would hide.
    incumbent_contracts INTEGER,
    candidate_contracts INTEGER,
    incumbent_oi_contracts INTEGER,
    candidate_oi_contracts INTEGER
);

CREATE INDEX IF NOT EXISTS idx_feed_comparisons_run
    ON feed_comparisons (underlying, captured_at DESC);
