-- =====================================================================
-- ZeroGEX — Pin Strike vs GEX King study export
--
--   make db-pin-vs-king-export
--
-- (or directly: psql "$DATABASE_URL" -f this-file)
--
-- Read-only: SELECTs and client-side \copy only. Writes three CSVs into
-- the directory you run it from. No server filesystem access needed, so
-- it works as any ordinary DB user.
--
-- Scope is SPX + QQQ over the 90-day retention window. To change it, edit
-- the IN (...) lists and the INTERVAL in all three queries.
--
-- Deliberately metric-agnostic: it exports the raw per-minute levels and
-- the raw per-minute bars rather than a pre-computed verdict, so touch
-- rate, direction-from-open, and hold-rate-after-touch can all be scored
-- from the same pull without going back to the database.
-- =====================================================================
\set ON_ERROR_STOP on
\timing on

\echo ''
\echo '>>> 1/3  session inventory (coverage sanity check)'
\copy (SELECT gs.underlying, (gs.timestamp AT TIME ZONE 'America/New_York')::date AS trading_date_et, COUNT(*) AS n_rows, MIN(gs.timestamp AT TIME ZONE 'America/New_York')::time AS first_et, MAX(gs.timestamp AT TIME ZONE 'America/New_York')::time AS last_et, COUNT(*) FILTER (WHERE gs.pin_strike IS NOT NULL) AS rows_with_pin, COUNT(*) FILTER (WHERE gs.max_gamma_strike IS NOT NULL) AS rows_with_king FROM gex_summary gs WHERE gs.underlying IN ('SPX','QQQ') AND gs.timestamp >= NOW() - INTERVAL '90 days' GROUP BY 1,2 ORDER BY 1,2) TO 'zerogex_sessions.csv' WITH (FORMAT csv, HEADER)

\echo '>>> 2/3  per-minute dealer levels'
\copy (SELECT gs.underlying, gs.timestamp AS ts_utc, (gs.timestamp AT TIME ZONE 'America/New_York')::date AS trading_date_et, to_char(gs.timestamp AT TIME ZONE 'America/New_York','HH24:MI:SS') AS time_et, gs.pin_strike, gs.pin_confidence, gs.pin_score, gs.pin_strike_reason, gs.max_gamma_strike, gs.max_gamma_value, gs.gamma_flip_point, gs.gamma_flip_raw, gs.call_wall, gs.put_wall, gs.max_pain, gs.net_gex_at_spot, gs.total_net_gex FROM gex_summary gs WHERE gs.underlying IN ('SPX','QQQ') AND gs.timestamp >= NOW() - INTERVAL '90 days' ORDER BY gs.underlying, gs.timestamp) TO 'zerogex_levels.csv' WITH (FORMAT csv, HEADER)

\echo '>>> 3/3  per-minute underlying bars'
\copy (SELECT uq.symbol, uq.timestamp AS ts_utc, (uq.timestamp AT TIME ZONE 'America/New_York')::date AS trading_date_et, to_char(uq.timestamp AT TIME ZONE 'America/New_York','HH24:MI:SS') AS time_et, uq.open, uq.high, uq.low, uq.close FROM underlying_quotes uq WHERE uq.symbol IN ('SPX','QQQ') AND uq.timestamp >= NOW() - INTERVAL '90 days' ORDER BY uq.symbol, uq.timestamp) TO 'zerogex_bars.csv' WITH (FORMAT csv, HEADER)

\echo ''
\echo 'Done. Send back: zerogex_sessions.csv, zerogex_levels.csv, zerogex_bars.csv'
