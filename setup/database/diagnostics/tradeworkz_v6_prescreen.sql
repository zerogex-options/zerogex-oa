-- TradeWorkz v6 pre-screen: the cheap questions to answer BEFORE a bot screen.
--
-- The v6 candidates (docs/design/tradeworkz-v6-strategies.md) sell or buy the
-- 0DTE move on this platform's confirmed MAGNITUDE signals, and one of them
-- trades the last-hour momentum the hedging-demand literature documents. Each
-- rests on a base rate that the existing tables can report in seconds, without
-- running `make tradeworkz-backtest` (~1 hour per 90-day screen). Run this
-- first; a section that comes back thin or null says the dependent bot will
-- read "insufficient" or "underpowered", and which gate to look at.
--
-- Every statement is a SELECT. Read-only; writes nothing.
-- Runs on-demand via `make tradeworkz-v6-prescreen`.

\pset pager off
\pset border 2

\echo
\echo ============================================================
\echo  A. Option-chain coverage per symbol (the screen window)
\echo ============================================================
\echo  The replay is clamped to hot UNION archive coverage. The archive is
\echo  read with one equality predicate per symbol so the probe uses the
\echo  (underlying, timestamp) index instead of scanning the whole table.
\echo

SELECT 'option_chains' AS tbl, underlying, MIN(timestamp) AS first_ts, MAX(timestamp) AS last_ts
FROM option_chains
WHERE underlying IN ('SPY', 'QQQ', 'SPX')
GROUP BY underlying
UNION ALL
SELECT 'option_chains_archive', 'SPY',
       (SELECT MIN(timestamp) FROM option_chains_archive WHERE underlying = 'SPY'),
       (SELECT MAX(timestamp) FROM option_chains_archive WHERE underlying = 'SPY')
UNION ALL
SELECT 'option_chains_archive', 'QQQ',
       (SELECT MIN(timestamp) FROM option_chains_archive WHERE underlying = 'QQQ'),
       (SELECT MAX(timestamp) FROM option_chains_archive WHERE underlying = 'QQQ')
UNION ALL
SELECT 'option_chains_archive', 'SPX',
       (SELECT MIN(timestamp) FROM option_chains_archive WHERE underlying = 'SPX'),
       (SELECT MAX(timestamp) FROM option_chains_archive WHERE underlying = 'SPX')
ORDER BY 1, 2;

\echo
\echo ============================================================
\echo  B. Does the morning forecast's expected_vol_ratio predict the day?
\echo ============================================================
\echo  The compression-forecast gate assumes the graded daily_forecast rows
\echo  carry skill. Spearman rho between the committed expected_vol_ratio and
\echo  the receipt's realized_vol_ratio, per symbol, plus the bucket hit rate
\echo  and mean realized ratio on committed-compression days vs the rest.
\echo  rho near 0 => the forecast gate is noise; run only the ungated control.
\echo  A symbol with no rows here has no forecast => its forecast-gated bots
\echo  abstain on that symbol (the writer defaults to SPY only).
\echo

WITH graded AS (
    SELECT symbol, date, expected_vol_ratio, realized_vol_ratio, expected_vol_state,
           CASE WHEN realized_vol_ratio <= 0.85 THEN 'compression'
                WHEN realized_vol_ratio >= 1.15 THEN 'expansion'
                ELSE 'normal' END AS realized_state
    FROM daily_forecast
    WHERE receipt_ts IS NOT NULL
      AND expected_vol_ratio IS NOT NULL
      AND realized_vol_ratio IS NOT NULL
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY symbol ORDER BY expected_vol_ratio) AS r_expected,
           RANK() OVER (PARTITION BY symbol ORDER BY realized_vol_ratio) AS r_realized
    FROM graded
)
SELECT symbol,
       COUNT(*) AS graded_days,
       MIN(date) AS first_day,
       MAX(date) AS last_day,
       ROUND(CORR(r_expected, r_realized)::numeric, 3) AS spearman_rho,
       ROUND(100.0 * AVG((expected_vol_state = realized_state)::int), 1) AS state_hit_pct,
       COUNT(*) FILTER (WHERE expected_vol_state = 'compression') AS compression_days,
       ROUND(AVG(realized_vol_ratio) FILTER (WHERE expected_vol_state = 'compression')::numeric, 3)
           AS realized_on_compression_days,
       ROUND(AVG(realized_vol_ratio) FILTER (WHERE expected_vol_state <> 'compression')::numeric, 3)
           AS realized_on_other_days
FROM ranked
GROUP BY symbol
ORDER BY symbol;

\echo
\echo ============================================================
\echo  C. ATM IV history per symbol (the 60-day percentile)
\echo ============================================================
\echo  atm_iv_pctile_60d needs >= 20 prior sessions; below that the gate is
\echo  permissive by default (require_iv_pctile=false) and the funnel shows it.
\echo

SELECT underlying, COUNT(*) AS sessions, MIN(trading_date) AS first_day, MAX(trading_date) AS last_day,
       ROUND(AVG(atm_call_iv)::numeric, 4) AS mean_atm_iv
FROM daily_atm_iv
WHERE underlying IN ('SPY', 'QQQ', 'SPX')
GROUP BY underlying
ORDER BY underlying;

\echo
\echo ============================================================
\echo  D. Dealer-gamma sign base rates, RTH, last 90 days
\echo ============================================================
\echo  The fly needs positive gamma midday (then the top quartile by
\echo  gex_historical_stats, which this probe cannot rank cheaply); the
\echo  momentum bot needs NEGATIVE gamma at 15:00-15:20 ET. Shares of minutes.
\echo

WITH g AS (
    SELECT underlying, net_gex_at_spot,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM gex_summary
    WHERE underlying IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= NOW() - INTERVAL '90 days'
      AND net_gex_at_spot IS NOT NULL
)
SELECT underlying,
       COUNT(*) FILTER (WHERE et BETWEEN '09:30' AND '16:00') AS rth_minutes,
       ROUND(100.0 * AVG((net_gex_at_spot > 0)::int) FILTER (WHERE et BETWEEN '10:30' AND '13:30'), 1)
           AS pct_positive_1030_1330,
       ROUND(100.0 * AVG((net_gex_at_spot < 0)::int) FILTER (WHERE et BETWEEN '15:00' AND '15:20'), 1)
           AS pct_negative_1500_1520
FROM g
GROUP BY underlying
ORDER BY underlying;

\echo
\echo ============================================================
\echo  E. Last-hour momentum, raw, from the cash tape (no options)
\echo ============================================================
\echo  The hedging_demand_close_momentum hypothesis before any option is
\echo  priced: on sessions where the 09:30 -> 15:00 cash return is at least
\echo  half the VIX-implied daily sigma, how often does 15:00 -> 15:55 go the
\echo  same way, and how many bps does it travel in the day's direction? Split
\echo  by the dealer-gamma sign at 15:00. Baltussen et al. say the short-gamma
\echo  row should carry the effect and the long-gamma row should not; if the
\echo  short-gamma continuation share is not clearly above 50% here, the bot
\echo  screen cannot rescue it. Index symbols open on their 09:31 bar (the
\echo  09:30 print is stale). QQQ is scaled with VIX rather than VXN here,
\echo  which only shifts how many of its sessions qualify.
\echo

WITH bars AS (
    SELECT symbol, timestamp, close,
           (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM underlying_quotes
    WHERE symbol IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= NOW() - INTERVAL '95 days'
),
opens AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_open
    FROM bars
    WHERE et >= CASE WHEN symbol IN ('SPX', 'NDX') THEN '09:31'::time ELSE '09:30'::time END
      AND et < '09:40'
    ORDER BY symbol, sess, timestamp
),
at_1500 AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_1500
    FROM bars WHERE et BETWEEN '14:45' AND '15:00'
    ORDER BY symbol, sess, timestamp DESC
),
at_1555 AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_1555
    FROM bars WHERE et BETWEEN '15:40' AND '15:55'
    ORDER BY symbol, sess, timestamp DESC
),
gamma_1500 AS (
    SELECT DISTINCT ON (underlying, sess) underlying AS symbol, sess, net_gex_at_spot
    FROM (SELECT underlying, net_gex_at_spot, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM gex_summary
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= NOW() - INTERVAL '95 days') g
    WHERE et BETWEEN '14:00' AND '15:00' AND net_gex_at_spot IS NOT NULL
    ORDER BY underlying, sess, timestamp DESC
),
vix_1500 AS (
    SELECT DISTINCT ON (sess) sess, close AS vix
    FROM (SELECT close, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM vix_bars WHERE timestamp >= NOW() - INTERVAL '95 days') v
    WHERE et BETWEEN '14:00' AND '15:00'
    ORDER BY sess, timestamp DESC
),
sessions AS (
    SELECT o.symbol, o.sess,
           a.px_1500 / o.px_open - 1 AS day_ret,
           c.px_1555 / a.px_1500 - 1 AS last_ret,
           v.vix / 100.0 / SQRT(252) AS implied_daily,
           g.net_gex_at_spot
    FROM opens o
    JOIN at_1500 a USING (symbol, sess)
    JOIN at_1555 c USING (symbol, sess)
    JOIN gamma_1500 g USING (symbol, sess)
    JOIN vix_1500 v USING (sess)
),
qualifying AS (
    SELECT *, SIGN(day_ret) * last_ret AS last_ret_in_day_direction
    FROM sessions
    WHERE ABS(day_ret) >= 0.5 * implied_daily AND ABS(day_ret) <= 3.0 * implied_daily
)
SELECT symbol,
       CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END AS gamma_at_1500,
       COUNT(*) AS qualifying_sessions,
       ROUND(100.0 * AVG((SIGN(day_ret) = SIGN(last_ret))::int), 1) AS continuation_pct,
       ROUND((10000 * AVG(last_ret_in_day_direction))::numeric, 1) AS mean_last_55m_bps_in_day_dir,
       ROUND((10000 * PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY last_ret_in_day_direction::float8))::numeric, 1)
           AS median_last_55m_bps_in_day_dir
FROM qualifying
GROUP BY ROLLUP (symbol, CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END)
ORDER BY symbol NULLS LAST, 2 NULLS LAST;

\echo
\echo ============================================================
\echo  F. Does positive gamma compress the realized day, on this tape?
\echo ============================================================
\echo  The fly's thesis in one table: the cash session's high-low range as a
\echo  multiple of a NORMAL day's range (sqrt(8/pi) x the VIX-implied 1-sigma
\echo  move at the open), split by the dealer-gamma sign at 12:00 ET. Barbon
\echo  and Buraschi and this platform's own excursion studies say the
\echo  long-gamma rows should sit lower. The compression share (<= 0.85) is
\echo  roughly the fraction of sessions on which an ATM straddle sold at the
\echo  open would have been paid; midday entries see less of the range.
\echo

WITH bars AS (
    SELECT symbol, timestamp, high, low, close,
           (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM underlying_quotes
    WHERE symbol IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= NOW() - INTERVAL '95 days'
),
opens AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_open
    FROM bars
    WHERE et >= CASE WHEN symbol IN ('SPX', 'NDX') THEN '09:31'::time ELSE '09:30'::time END
      AND et < '09:40'
    ORDER BY symbol, sess, timestamp
),
ranges AS (
    SELECT symbol, sess, MAX(high) - MIN(low) AS day_range
    FROM bars WHERE et BETWEEN '09:30' AND '16:00'
    GROUP BY symbol, sess
),
gamma_1200 AS (
    SELECT DISTINCT ON (underlying, sess) underlying AS symbol, sess, net_gex_at_spot
    FROM (SELECT underlying, net_gex_at_spot, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM gex_summary
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= NOW() - INTERVAL '95 days') g
    WHERE et BETWEEN '11:00' AND '12:00' AND net_gex_at_spot IS NOT NULL
    ORDER BY underlying, sess, timestamp DESC
),
vix_open AS (
    SELECT DISTINCT ON (sess) sess, close AS vix
    FROM (SELECT close, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM vix_bars WHERE timestamp >= NOW() - INTERVAL '95 days') v
    WHERE et BETWEEN '09:30' AND '09:45'
    ORDER BY sess, timestamp
),
ratios AS (
    SELECT o.symbol, o.sess, g.net_gex_at_spot,
           r.day_range / (o.px_open * v.vix / 100.0 / SQRT(252) * SQRT(8.0 / PI())) AS realized_over_normal
    FROM opens o
    JOIN ranges r USING (symbol, sess)
    JOIN gamma_1200 g USING (symbol, sess)
    JOIN vix_open v USING (sess)
)
SELECT symbol,
       CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END AS gamma_at_1200,
       COUNT(*) AS sessions,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY realized_over_normal::float8)::numeric, 3) AS median_ratio,
       ROUND(100.0 * AVG((realized_over_normal <= 0.85)::int), 1) AS compression_pct,
       ROUND(100.0 * AVG((realized_over_normal >= 1.15)::int), 1) AS expansion_pct
FROM ratios
GROUP BY ROLLUP (symbol, CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END)
ORDER BY symbol NULLS LAST, 2 NULLS LAST;

\echo
\echo  Done. Next: python -m src.tools.tw_edge_field_probe --days 90 for the
\echo  new snapshot fields, then make tradeworkz-backtest for the v6 bots.
\echo
